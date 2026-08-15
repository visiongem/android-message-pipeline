package com.messagepipeline.transport.usb

import android.hardware.usb.UsbConstants
import android.hardware.usb.UsbDevice
import android.hardware.usb.UsbDeviceConnection
import android.hardware.usb.UsbEndpoint
import android.hardware.usb.UsbInterface
import android.hardware.usb.UsbManager
import com.messagepipeline.Frame
import com.messagepipeline.Transport
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/** Android USB Host Bulk Transport 参数。 */
data class UsbBulkConfig(
    val interfaceIndex: Int? = null,
    val maxFrameSize: Int = 16_380,
    val readBufferSize: Int = 16_384,
    val transferTimeoutMillis: Int = 1_000,
    val forceClaimInterface: Boolean = true,
) {
    init {
        require(interfaceIndex == null || interfaceIndex >= 0) { "interfaceIndex must be >= 0" }
        require(maxFrameSize in 1..MAX_ALLOWED_FRAME_SIZE) {
            "maxFrameSize must be in 1..$MAX_ALLOWED_FRAME_SIZE"
        }
        require(readBufferSize in 1..MAX_BULK_TRANSACTION_SIZE) {
            "readBufferSize must be in 1..$MAX_BULK_TRANSACTION_SIZE"
        }
        require(transferTimeoutMillis > 0) { "transferTimeoutMillis must be > 0" }
    }

    companion object {
        const val MAX_ALLOWED_FRAME_SIZE = 4 * 1024 * 1024
        const val MAX_BULK_TRANSACTION_SIZE = 16 * 1024
    }
}

class UsbTransportException(message: String, cause: Throwable? = null) :
    IllegalStateException(message, cause)

/**
 * Android USB Host 模式下，基于一对 Bulk IN/OUT endpoint 的双向 Transport。
 *
 * 调用 [open] 前，宿主应用必须已通过 [UsbManager.requestPermission] 获得设备权限。USB Bulk
 * 不保证一次读对应一次写，本实现在线路上增加 4 字节大端长度前缀来恢复 [Frame] 边界。
 */
class UsbBulkTransport private constructor(
    private val connection: UsbDeviceConnection,
    private val usbInterface: UsbInterface,
    private val inputEndpoint: UsbEndpoint,
    private val outputEndpoint: UsbEndpoint,
    private val config: UsbBulkConfig,
    private val onError: (Throwable) -> Unit,
) : Transport {

    override val mtu: Int = config.maxFrameSize

    private val closed = AtomicBoolean(false)
    private val listener = AtomicReference<((Frame) -> Unit)?>(null)
    private val terminalError = AtomicReference<Throwable?>(null)
    private val readStarted = AtomicBoolean(false)
    private val writeLock = Any()
    private val resourceLock = Any()
    private val decoder = LengthPrefixedFrameDecoder(config.maxFrameSize)
    private var readThread: Thread? = null

    override fun send(frame: Frame) {
        checkOpen()
        require(frame.bytes.size <= mtu) { "frame is ${frame.bytes.size} bytes, usb mtu is $mtu" }
        val packet = encodeLengthPrefixedFrame(frame)

        synchronized(writeLock) {
            var offset = 0
            while (offset < packet.size) {
                checkOpen()
                val length = minOf(
                    packet.size - offset,
                    UsbBulkConfig.MAX_BULK_TRANSACTION_SIZE,
                )
                val transferred = try {
                    connection.bulkTransfer(
                        outputEndpoint,
                        packet,
                        offset,
                        length,
                        config.transferTimeoutMillis,
                    )
                } catch (error: Throwable) {
                    fail(UsbTransportException("USB bulk write failed", error))
                    throw terminalException()
                }
                if (transferred <= 0) {
                    val error = UsbTransportException(
                        "USB bulk write returned $transferred at offset $offset",
                    )
                    fail(error)
                    throw error
                }
                offset += transferred
            }
        }
    }

    override fun onReceive(listener: (Frame) -> Unit) {
        checkOpen()
        this.listener.set(listener)
        if (readStarted.compareAndSet(false, true)) {
            readThread = Thread(::readLoop, "MessagePipeline-UsbBulkReader").also { it.start() }
        }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        listener.set(null)
        readThread?.interrupt()
        releaseResources()
    }

    private fun readLoop() {
        val buffer = ByteArray(config.readBufferSize)
        try {
            while (!closed.get()) {
                val count = connection.bulkTransfer(
                    inputEndpoint,
                    buffer,
                    buffer.size,
                    config.transferTimeoutMillis,
                )
                if (count < 0) {
                    // API 不区分超时和普通负值；短暂退避可避免设备拔出后形成忙循环。
                    try {
                        Thread.sleep(10)
                    } catch (_: InterruptedException) {
                        Thread.currentThread().interrupt()
                        return
                    }
                    continue
                }
                if (count == 0) continue
                val frames = decoder.accept(buffer, count)
                val currentListener = listener.get()
                if (currentListener != null) {
                    for (frame in frames) {
                        try {
                            currentListener(frame)
                        } catch (callbackError: Throwable) {
                            runCatching { onError(callbackError) }
                        }
                    }
                }
            }
        } catch (error: Throwable) {
            if (!closed.get()) fail(UsbTransportException("USB bulk read failed", error))
        }
    }

    private fun checkOpen() {
        terminalError.get()?.let { throw UsbTransportException("USB transport failed", it) }
        if (closed.get()) throw UsbTransportException("USB transport closed")
    }

    private fun terminalException(): UsbTransportException =
        UsbTransportException("USB transport failed", terminalError.get())

    private fun fail(error: Throwable) {
        terminalError.compareAndSet(null, error)
        if (closed.compareAndSet(false, true)) {
            listener.set(null)
            runCatching { onError(error) }
            releaseResources()
        }
    }

    private fun releaseResources() {
        synchronized(resourceLock) {
            runCatching { connection.releaseInterface(usbInterface) }
            runCatching { connection.close() }
        }
    }

    companion object {
        /**
         * 打开第一个同时包含 Bulk IN 和 Bulk OUT endpoint 的接口，或 [UsbBulkConfig.interfaceIndex]
         * 指定的接口。权限申请和 USB attach/detach 监听由宿主应用负责。
         */
        fun open(
            usbManager: UsbManager,
            device: UsbDevice,
            config: UsbBulkConfig = UsbBulkConfig(),
            onError: (Throwable) -> Unit = {},
        ): UsbBulkTransport {
            if (!usbManager.hasPermission(device)) {
                throw UsbTransportException("USB permission has not been granted")
            }
            val endpoints = findBulkEndpoints(device, config.interfaceIndex)
                ?: throw UsbTransportException("no interface with Bulk IN and Bulk OUT endpoints")
            val connection = usbManager.openDevice(device)
                ?: throw UsbTransportException("UsbManager.openDevice returned null")
            if (!connection.claimInterface(endpoints.usbInterface, config.forceClaimInterface)) {
                connection.close()
                throw UsbTransportException("failed to claim USB interface ${endpoints.usbInterface.id}")
            }
            return UsbBulkTransport(
                connection = connection,
                usbInterface = endpoints.usbInterface,
                inputEndpoint = endpoints.input,
                outputEndpoint = endpoints.output,
                config = config,
                onError = onError,
            )
        }

        private fun findBulkEndpoints(device: UsbDevice, interfaceIndex: Int?): BulkEndpoints? {
            val indices = interfaceIndex?.let(::listOf) ?: (0 until device.interfaceCount).toList()
            for (index in indices) {
                if (index !in 0 until device.interfaceCount) continue
                val usbInterface = device.getInterface(index)
                var input: UsbEndpoint? = null
                var output: UsbEndpoint? = null
                for (endpointIndex in 0 until usbInterface.endpointCount) {
                    val endpoint = usbInterface.getEndpoint(endpointIndex)
                    if (endpoint.type != UsbConstants.USB_ENDPOINT_XFER_BULK) continue
                    when (endpoint.direction) {
                        UsbConstants.USB_DIR_IN -> input = endpoint
                        UsbConstants.USB_DIR_OUT -> output = endpoint
                    }
                }
                if (input != null && output != null) {
                    return BulkEndpoints(usbInterface, input, output)
                }
            }
            return null
        }
    }

    private data class BulkEndpoints(
        val usbInterface: UsbInterface,
        val input: UsbEndpoint,
        val output: UsbEndpoint,
    )
}

internal fun encodeLengthPrefixedFrame(frame: Frame): ByteArray =
    ByteBuffer.allocate(Int.SIZE_BYTES + frame.bytes.size)
        .putInt(frame.bytes.size)
        .put(frame.bytes)
        .array()

internal class LengthPrefixedFrameDecoder(private val maxFrameSize: Int) {
    private val lengthBytes = ByteArray(Int.SIZE_BYTES)
    private var lengthByteCount = 0
    private var payload: ByteArray? = null
    private var payloadByteCount = 0

    fun accept(bytes: ByteArray, count: Int = bytes.size): List<Frame> {
        require(count in 0..bytes.size) { "invalid byte count $count" }
        val frames = mutableListOf<Frame>()
        var offset = 0

        while (offset < count) {
            if (payload == null) {
                val copied = minOf(Int.SIZE_BYTES - lengthByteCount, count - offset)
                bytes.copyInto(lengthBytes, lengthByteCount, offset, offset + copied)
                lengthByteCount += copied
                offset += copied
                if (lengthByteCount < Int.SIZE_BYTES) continue

                val size = ByteBuffer.wrap(lengthBytes).int
                lengthByteCount = 0
                if (size !in 0..maxFrameSize) {
                    reset()
                    throw UsbTransportException("invalid USB frame length $size")
                }
                if (size == 0) {
                    frames += Frame(ByteArray(0))
                    continue
                }
                payload = ByteArray(size)
                payloadByteCount = 0
            }

            val target = payload ?: continue
            val copied = minOf(target.size - payloadByteCount, count - offset)
            bytes.copyInto(target, payloadByteCount, offset, offset + copied)
            payloadByteCount += copied
            offset += copied
            if (payloadByteCount == target.size) {
                frames += Frame(target)
                payload = null
                payloadByteCount = 0
            }
        }
        return frames
    }

    private fun reset() {
        lengthByteCount = 0
        payload = null
        payloadByteCount = 0
    }
}
