package com.messagepipeline.transport.ble

import android.Manifest
import android.annotation.SuppressLint
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothProfile
import android.bluetooth.BluetoothStatusCodes
import android.content.Context
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.content.ContextCompat
import com.messagepipeline.Frame
import com.messagepipeline.Transport
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runInterruptible
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/** Android 作为 BLE GATT Client 时使用的连接参数。 */
data class BleGattConfig(
    val serviceUuid: UUID,
    val writeCharacteristicUuid: UUID,
    val notifyCharacteristicUuid: UUID,
    val requestedMtu: Int = 517,
    val connectionTimeoutMillis: Long = 15_000,
    val operationTimeoutMillis: Long = 5_000,
    val autoConnect: Boolean = false,
    val writeType: Int = BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT,
) {
    init {
        require(requestedMtu in DEFAULT_GATT_MTU..MAX_GATT_MTU) {
            "requestedMtu must be in $DEFAULT_GATT_MTU..$MAX_GATT_MTU"
        }
        require(connectionTimeoutMillis > 0) { "connectionTimeoutMillis must be > 0" }
        require(operationTimeoutMillis > 0) { "operationTimeoutMillis must be > 0" }
        require(
            writeType == BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT ||
                writeType == BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE,
        ) { "writeType must be WRITE_TYPE_DEFAULT or WRITE_TYPE_NO_RESPONSE" }
    }

    companion object {
        const val DEFAULT_GATT_MTU = 23
        const val MAX_GATT_MTU = 517
    }
}

class BleTransportException(message: String, cause: Throwable? = null) :
    IllegalStateException(message, cause)

/**
 * Android BLE GATT Client Transport。
 *
 * [connect] 返回时，连接、服务发现、MTU 请求和通知订阅均已完成。宿主应用负责扫描设备并在
 * Android 12+ 动态授予 `BLUETOOTH_CONNECT`。每个实例独占一个 [BluetoothGatt] 连接。
 */
class BleGattTransport private constructor(
    private val session: GattSession,
) : Transport {

    /** 已协商 ATT MTU 扣除 3 字节 ATT 头，并受单个 characteristic value 512 字节上限约束。 */
    override val mtu: Int
        get() = minOf(session.negotiatedMtu - ATT_HEADER_BYTES, MAX_ATTRIBUTE_VALUE_BYTES)
            .coerceAtLeast(1)

    override fun send(frame: Frame) {
        session.send(frame, mtu)
    }

    override fun onReceive(listener: (Frame) -> Unit) {
        session.checkOpen()
        session.listener.set(listener)
    }

    override fun close() {
        session.close()
    }

    companion object {
        private const val ATT_HEADER_BYTES = 3
        private const val MAX_ATTRIBUTE_VALUE_BYTES = 512

        /** 在 IO dispatcher 中完成连接；这是 Android/Kotlin 调用方的推荐入口。 */
        suspend fun connect(
            context: Context,
            device: BluetoothDevice,
            config: BleGattConfig,
            onError: (Throwable) -> Unit = {},
        ): BleGattTransport = runInterruptible(Dispatchers.IO) {
            connectBlocking(context, device, config, onError)
        }

        /**
         * 阻塞直到 GATT 完全就绪。不能在 Android 主线程调用；主要供非协程代码和测试工具使用。
         */
        fun connectBlocking(
            context: Context,
            device: BluetoothDevice,
            config: BleGattConfig,
            onError: (Throwable) -> Unit = {},
        ): BleGattTransport {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S &&
                ContextCompat.checkSelfPermission(context, Manifest.permission.BLUETOOTH_CONNECT) !=
                PackageManager.PERMISSION_GRANTED
            ) {
                throw BleTransportException("BLUETOOTH_CONNECT permission has not been granted")
            }
            val session = GattSession(config, onError)
            val gatt = try {
                connectGattWithPermission(context, device, config, session.callback)
            } catch (error: Throwable) {
                session.fail(BleTransportException("failed to start GATT connection", error))
                throw BleTransportException("failed to start GATT connection", error)
            }
            if (gatt == null) {
                session.fail(BleTransportException("BluetoothDevice.connectGatt returned null"))
                throw BleTransportException("BluetoothDevice.connectGatt returned null")
            }
            session.gatt.compareAndSet(null, gatt)

            val ready = try {
                session.ready.await(config.connectionTimeoutMillis, TimeUnit.MILLISECONDS)
            } catch (interrupted: InterruptedException) {
                Thread.currentThread().interrupt()
                session.fail(BleTransportException("interrupted while connecting GATT", interrupted))
                throw BleTransportException("interrupted while connecting GATT", interrupted)
            }
            if (!ready) {
                val error = BleTransportException(
                    "GATT connection was not ready within ${config.connectionTimeoutMillis} ms",
                )
                session.fail(error)
                throw error
            }
            session.failure.get()?.let { throw BleTransportException("GATT connection failed", it) }
            session.checkOpen()
            return BleGattTransport(session)
        }

        @SuppressLint("MissingPermission")
        private fun connectGattWithPermission(
            context: Context,
            device: BluetoothDevice,
            config: BleGattConfig,
            callback: BluetoothGattCallback,
        ): BluetoothGatt? = device.connectGatt(
            context.applicationContext,
            config.autoConnect,
            callback,
            BluetoothDevice.TRANSPORT_LE,
        )
    }
}

@SuppressLint("MissingPermission")
private class GattSession(
    private val config: BleGattConfig,
    private val onError: (Throwable) -> Unit,
) {
    val ready = CountDownLatch(1)
    val failure = AtomicReference<Throwable?>(null)
    val gatt = AtomicReference<BluetoothGatt?>(null)
    val listener = AtomicReference<((Frame) -> Unit)?>(null)
    @Volatile var negotiatedMtu: Int = BleGattConfig.DEFAULT_GATT_MTU

    private val closed = AtomicBoolean(false)
    private val notificationSetupStarted = AtomicBoolean(false)
    private val writeLock = Any()
    private val pendingWrite = AtomicReference<PendingWrite?>(null)
    private val receiveExecutor = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "MessagePipeline-BleReceive").apply { isDaemon = true }
    }
    private var writeCharacteristic: BluetoothGattCharacteristic? = null
    private var notifyCharacteristic: BluetoothGattCharacteristic? = null

    val callback = object : BluetoothGattCallback() {
        override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
            if (closed.get()) return
            this@GattSession.gatt.compareAndSet(null, gatt)
            if (status != BluetoothGatt.GATT_SUCCESS) {
                fail(BleTransportException("GATT connection status=$status state=$newState"))
                return
            }
            when (newState) {
                BluetoothProfile.STATE_CONNECTED -> {
                    val started = runCatching { gatt.discoverServices() }
                        .getOrElse { error ->
                            fail(BleTransportException("service discovery failed to start", error))
                            return
                        }
                    if (!started) fail(BleTransportException("BluetoothGatt.discoverServices returned false"))
                }

                BluetoothProfile.STATE_DISCONNECTED ->
                    fail(BleTransportException("GATT disconnected"))
            }
        }

        override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
            if (status != BluetoothGatt.GATT_SUCCESS) {
                fail(BleTransportException("service discovery failed with status=$status"))
                return
            }
            val service = gatt.getService(config.serviceUuid)
            if (service == null) {
                fail(BleTransportException("service ${config.serviceUuid} not found"))
                return
            }
            val write = service.getCharacteristic(config.writeCharacteristicUuid)
            val notify = service.getCharacteristic(config.notifyCharacteristicUuid)
            if (write == null || notify == null) {
                fail(
                    BleTransportException(
                        "required write/notify characteristic not found in ${config.serviceUuid}",
                    ),
                )
                return
            }
            if (!supportsConfiguredWriteType(write)) {
                fail(BleTransportException("write characteristic does not support configured write type"))
                return
            }
            val notifyProperties = notify.properties
            if (notifyProperties and (BluetoothGattCharacteristic.PROPERTY_NOTIFY or
                    BluetoothGattCharacteristic.PROPERTY_INDICATE) == 0
            ) {
                fail(BleTransportException("notify characteristic is neither NOTIFY nor INDICATE"))
                return
            }
            writeCharacteristic = write
            notifyCharacteristic = notify

            val mtuStarted = if (config.requestedMtu == BleGattConfig.DEFAULT_GATT_MTU) {
                false
            } else {
                runCatching { gatt.requestMtu(config.requestedMtu) }
                    .getOrElse { error ->
                        fail(BleTransportException("MTU request failed to start", error))
                        return
                    }
            }
            if (!mtuStarted) {
                enableNotifications(gatt)
            }
        }

        override fun onMtuChanged(gatt: BluetoothGatt, mtu: Int, status: Int) {
            if (status == BluetoothGatt.GATT_SUCCESS && mtu >= BleGattConfig.DEFAULT_GATT_MTU) {
                negotiatedMtu = mtu
            }
            enableNotifications(gatt)
        }

        override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
            if (descriptor.uuid != CLIENT_CHARACTERISTIC_CONFIG_UUID) return
            if (status == BluetoothGatt.GATT_SUCCESS) {
                ready.countDown()
            } else {
                fail(BleTransportException("enabling GATT notifications failed with status=$status"))
            }
        }

        override fun onCharacteristicWrite(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            status: Int,
        ) {
            if (characteristic.uuid != config.writeCharacteristicUuid) return
            pendingWrite.get()?.complete(status)
        }

        @Deprecated("Deprecated in Android API 33")
        override fun onCharacteristicChanged(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
        ) {
            @Suppress("DEPRECATION")
            dispatchNotification(characteristic, characteristic.value?.copyOf() ?: ByteArray(0))
        }

        override fun onCharacteristicChanged(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            value: ByteArray,
        ) {
            dispatchNotification(characteristic, value.copyOf())
        }
    }

    fun send(frame: Frame, mtu: Int) {
        checkOpen()
        require(frame.bytes.size <= mtu) { "frame is ${frame.bytes.size} bytes, BLE mtu is $mtu" }
        val characteristic = writeCharacteristic
            ?: throw BleTransportException("write characteristic is not ready")
        val bluetoothGatt = gatt.get() ?: throw BleTransportException("BluetoothGatt is unavailable")

        synchronized(writeLock) {
            checkOpen()
            val operation = PendingWrite()
            check(pendingWrite.compareAndSet(null, operation)) { "another GATT write is already pending" }
            try {
                val started = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                    bluetoothGatt.writeCharacteristic(characteristic, frame.bytes, config.writeType) ==
                        BluetoothStatusCodes.SUCCESS
                } else {
                    @Suppress("DEPRECATION")
                    characteristic.writeType = config.writeType
                    @Suppress("DEPRECATION")
                    characteristic.value = frame.bytes
                    @Suppress("DEPRECATION")
                    bluetoothGatt.writeCharacteristic(characteristic)
                }
                if (!started) throw BleTransportException("BluetoothGatt.writeCharacteristic did not start")

                val completed = try {
                    operation.latch.await(config.operationTimeoutMillis, TimeUnit.MILLISECONDS)
                } catch (interrupted: InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw BleTransportException("interrupted while waiting for GATT write", interrupted)
                }
                if (!completed) {
                    throw BleTransportException(
                        "GATT write timed out after ${config.operationTimeoutMillis} ms",
                    )
                }
                if (operation.status.get() != BluetoothGatt.GATT_SUCCESS) {
                    throw BleTransportException("GATT write failed with status=${operation.status.get()}")
                }
            } catch (error: Throwable) {
                val transportError = if (error is BleTransportException) {
                    error
                } else {
                    BleTransportException("GATT write failed", error)
                }
                fail(transportError)
                throw transportError
            } finally {
                pendingWrite.compareAndSet(operation, null)
            }
        }
    }

    fun checkOpen() {
        failure.get()?.let { throw BleTransportException("BLE transport failed", it) }
        if (closed.get()) throw BleTransportException("BLE transport closed")
    }

    fun fail(error: Throwable) {
        if (!failure.compareAndSet(null, error)) return
        pendingWrite.get()?.complete(FAILED_STATUS)
        ready.countDown()
        runCatching { onError(error) }
        closeResources()
    }

    fun close() {
        if (!closed.compareAndSet(false, true)) return
        pendingWrite.get()?.complete(FAILED_STATUS)
        listener.set(null)
        ready.countDown()
        closeResources()
    }

    private fun enableNotifications(gatt: BluetoothGatt) {
        if (!notificationSetupStarted.compareAndSet(false, true)) return
        try {
            val characteristic = notifyCharacteristic
            if (characteristic == null) {
                fail(BleTransportException("notify characteristic is not ready"))
                return
            }
            if (!gatt.setCharacteristicNotification(characteristic, true)) {
                fail(BleTransportException("setCharacteristicNotification returned false"))
                return
            }
            val descriptor = characteristic.getDescriptor(CLIENT_CHARACTERISTIC_CONFIG_UUID)
            if (descriptor == null) {
                fail(BleTransportException("notification characteristic has no CCCD descriptor"))
                return
            }
            val descriptorValue = if (
                characteristic.properties and BluetoothGattCharacteristic.PROPERTY_INDICATE != 0 &&
                characteristic.properties and BluetoothGattCharacteristic.PROPERTY_NOTIFY == 0
            ) {
                BluetoothGattDescriptor.ENABLE_INDICATION_VALUE
            } else {
                BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
            }
            val started = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                gatt.writeDescriptor(descriptor, descriptorValue) == BluetoothStatusCodes.SUCCESS
            } else {
                @Suppress("DEPRECATION")
                descriptor.value = descriptorValue
                @Suppress("DEPRECATION")
                gatt.writeDescriptor(descriptor)
            }
            if (!started) fail(BleTransportException("writing CCCD descriptor did not start"))
        } catch (error: Throwable) {
            fail(BleTransportException("enabling GATT notifications failed", error))
        }
    }

    private fun supportsConfiguredWriteType(characteristic: BluetoothGattCharacteristic): Boolean {
        val requiredProperty = if (config.writeType == BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE) {
            BluetoothGattCharacteristic.PROPERTY_WRITE_NO_RESPONSE
        } else {
            BluetoothGattCharacteristic.PROPERTY_WRITE
        }
        return characteristic.properties and requiredProperty != 0
    }

    private fun dispatchNotification(characteristic: BluetoothGattCharacteristic, value: ByteArray) {
        if (characteristic.uuid != config.notifyCharacteristicUuid || closed.get()) return
        try {
            receiveExecutor.execute {
                if (!closed.get()) {
                    try {
                        listener.get()?.invoke(Frame(value))
                    } catch (callbackError: Throwable) {
                        runCatching { onError(callbackError) }
                    }
                }
            }
        } catch (_: Throwable) {
            // close() 与通知并发时 executor 可能已停止；此时直接丢弃关闭后的通知。
        }
    }

    private fun closeResources() {
        closed.set(true)
        listener.set(null)
        receiveExecutor.shutdownNow()
        gatt.getAndSet(null)?.let { bluetoothGatt ->
            runCatching { bluetoothGatt.disconnect() }
            runCatching { bluetoothGatt.close() }
        }
    }

    private data class PendingWrite(
        val latch: CountDownLatch = CountDownLatch(1),
        val status: AtomicInteger = AtomicInteger(PENDING_STATUS),
    ) {
        fun complete(result: Int) {
            if (status.compareAndSet(PENDING_STATUS, result)) latch.countDown()
        }
    }

    companion object {
        private const val PENDING_STATUS = Int.MIN_VALUE
        private const val FAILED_STATUS = -1
        private val CLIENT_CHARACTERISTIC_CONFIG_UUID: UUID =
            UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")
    }
}
