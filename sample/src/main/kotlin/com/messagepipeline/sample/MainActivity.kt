package com.messagepipeline.sample

import android.Manifest
import android.annotation.SuppressLint
import android.app.Activity
import android.app.PendingIntent
import android.bluetooth.BluetoothManager
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.PackageManager
import android.hardware.usb.UsbDevice
import android.hardware.usb.UsbManager
import android.os.Build
import android.os.Bundle
import android.text.InputType
import android.view.ViewGroup
import android.widget.Button
import android.widget.CheckBox
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import androidx.core.content.ContextCompat
import com.messagepipeline.Transport
import com.messagepipeline.chunker.BinaryChunker
import com.messagepipeline.codec.StringCodec
import com.messagepipeline.dispatcher.PipelineDispatcher
import com.messagepipeline.transport.ReliableTransport
import com.messagepipeline.transport.ble.BleGattConfig
import com.messagepipeline.transport.ble.BleGattTransport
import com.messagepipeline.transport.usb.UsbBulkTransport
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import java.util.UUID

/** 手工真机联调入口；UUID、MAC 和 USB 设备均由测试人员在运行时选择。 */
@SuppressLint("SetTextI18n")
class MainActivity : Activity() {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main)
    private lateinit var statusView: TextView
    private lateinit var macInput: EditText
    private lateinit var serviceUuidInput: EditText
    private lateinit var writeUuidInput: EditText
    private lateinit var notifyUuidInput: EditText
    private lateinit var messageInput: EditText
    private lateinit var reliableCheck: CheckBox

    @Volatile private var pipeline: PipelineDispatcher<String>? = null
    @Volatile private var activeTransport: Transport? = null
    private var pendingUsbDevice: UsbDevice? = null

    private val usbReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context, intent: Intent) {
            when (intent.action) {
                ACTION_USB_PERMISSION -> {
                    val granted = intent.getBooleanExtra(UsbManager.EXTRA_PERMISSION_GRANTED, false)
                    val device = pendingUsbDevice
                    pendingUsbDevice = null
                    if (granted && device != null) connectUsb(device)
                    else showStatus("USB 权限被拒绝")
                }

                UsbManager.ACTION_USB_DEVICE_DETACHED -> {
                    closePipeline()
                    showStatus("USB 设备已拔出")
                }
            }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(buildContent())
        registerUsbReceiver()
    }

    override fun onDestroy() {
        closePipeline()
        runCatching { unregisterReceiver(usbReceiver) }
        scope.cancel()
        super.onDestroy()
    }

    private fun buildContent(): ScrollView {
        val padding = (16 * resources.displayMetrics.density).toInt()
        val column = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(padding, padding, padding, padding)
        }

        column.addView(TextView(this).apply {
            text = "BLE GATT Client"
            textSize = 20f
        })
        macInput = column.addInput("BLE MAC，例如 AA:BB:CC:DD:EE:FF")
        serviceUuidInput = column.addInput("Service UUID")
        writeUuidInput = column.addInput("Write Characteristic UUID")
        notifyUuidInput = column.addInput("Notify Characteristic UUID")
        column.addView(Button(this).apply {
            text = "连接 BLE"
            setOnClickListener { connectBle() }
        })

        column.addView(Button(this).apply {
            text = "连接首个 USB Bulk 设备"
            setOnClickListener { requestUsbConnection() }
        })

        reliableCheck = CheckBox(this).apply {
            text = "启用 ReliableTransport（对端必须支持）"
        }
        column.addView(reliableCheck)

        messageInput = column.addInput("要发送的 UTF-8 消息").apply {
            inputType = InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_FLAG_MULTI_LINE
            minLines = 3
        }
        column.addView(Button(this).apply {
            text = "发送"
            setOnClickListener { sendMessage() }
        })
        column.addView(Button(this).apply {
            text = "断开"
            setOnClickListener {
                closePipeline()
                showStatus("已断开")
            }
        })

        statusView = TextView(this).apply {
            text = "等待连接"
            setPadding(0, padding, 0, 0)
            setTextIsSelectable(true)
        }
        column.addView(statusView)
        return ScrollView(this).apply { addView(column) }
    }

    private fun connectBle() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S &&
            ContextCompat.checkSelfPermission(this, Manifest.permission.BLUETOOTH_CONNECT) !=
            PackageManager.PERMISSION_GRANTED
        ) {
            requestPermissions(arrayOf(Manifest.permission.BLUETOOTH_CONNECT), REQUEST_BLUETOOTH_CONNECT)
            showStatus("请授予蓝牙连接权限，然后再次点击连接")
            return
        }

        val mac = macInput.text.toString().trim()
        val config = runCatching {
            BleGattConfig(
                serviceUuid = UUID.fromString(serviceUuidInput.text.toString().trim()),
                writeCharacteristicUuid = UUID.fromString(writeUuidInput.text.toString().trim()),
                notifyCharacteristicUuid = UUID.fromString(notifyUuidInput.text.toString().trim()),
            )
        }.getOrElse {
            showStatus("UUID 格式不正确：${it.message}")
            return
        }
        val device = runCatching {
            val manager = getSystemService(BluetoothManager::class.java)
            manager.adapter.getRemoteDevice(mac)
        }.getOrElse {
            showStatus("BLE MAC 不正确或蓝牙不可用：${it.message}")
            return
        }
        val useReliable = reliableCheck.isChecked
        showStatus("正在连接 BLE…")
        scope.launch {
            runCatching {
                BleGattTransport.connect(this@MainActivity, device, config, ::showStatusError)
            }.onSuccess { raw ->
                startPipeline(raw, useReliable)
                showStatus("BLE 已连接，物理 payload MTU=${raw.mtu}")
            }.onFailure(::showStatusError)
        }
    }

    private fun requestUsbConnection() {
        val manager = getSystemService(UsbManager::class.java)
        val device = manager.deviceList.values.firstOrNull()
        if (device == null) {
            showStatus("没有发现 USB 设备")
            return
        }
        if (manager.hasPermission(device)) {
            connectUsb(device)
            return
        }
        pendingUsbDevice = device
        val intent = Intent(ACTION_USB_PERMISSION).setPackage(packageName)
        val pendingIntent = PendingIntent.getBroadcast(
            this,
            0,
            intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_MUTABLE,
        )
        manager.requestPermission(device, pendingIntent)
        showStatus("正在请求 USB 权限…")
    }

    private fun connectUsb(device: UsbDevice) {
        val useReliable = reliableCheck.isChecked
        showStatus("正在打开 USB Bulk 接口…")
        scope.launch(Dispatchers.IO) {
            runCatching {
                UsbBulkTransport.open(
                    usbManager = getSystemService(UsbManager::class.java),
                    device = device,
                    onError = ::showStatusError,
                )
            }.onSuccess { raw ->
                startPipeline(raw, useReliable)
                showStatus("USB 已连接，逻辑 MTU=${raw.mtu}")
            }.onFailure(::showStatusError)
        }
    }

    private fun startPipeline(rawTransport: Transport, useReliable: Boolean) {
        closePipeline()
        val transport = if (useReliable) ReliableTransport(rawTransport) else rawTransport
        activeTransport = transport
        pipeline = PipelineDispatcher(
            codec = StringCodec,
            chunker = BinaryChunker(),
            transport = transport,
            onMessage = { message -> showStatus("收到：$message") },
            onError = ::showStatusError,
        ).also { it.start() }
    }

    private fun sendMessage() {
        val current = pipeline
        if (current == null) {
            showStatus("请先连接 BLE 或 USB")
            return
        }
        val message = messageInput.text.toString()
        scope.launch(Dispatchers.IO) {
            runCatching { current.send(message) }
                .onSuccess { showStatus("发送完成：${message.toByteArray().size} bytes") }
                .onFailure(::showStatusError)
        }
    }

    @Synchronized
    private fun closePipeline() {
        pipeline?.stop()
        pipeline = null
        activeTransport?.close()
        activeTransport = null
    }

    private fun registerUsbReceiver() {
        val filter = IntentFilter().apply {
            addAction(ACTION_USB_PERMISSION)
            addAction(UsbManager.ACTION_USB_DEVICE_DETACHED)
        }
        ContextCompat.registerReceiver(
            this,
            usbReceiver,
            filter,
            ContextCompat.RECEIVER_NOT_EXPORTED,
        )
    }

    private fun showStatusError(error: Throwable) {
        showStatus("错误：${error.message ?: error::class.java.simpleName}")
    }

    private fun showStatus(message: String) {
        runOnUiThread { statusView.text = message }
    }

    private fun LinearLayout.addInput(hintText: String): EditText = EditText(context).also { input ->
        input.hint = hintText
        input.layoutParams = LinearLayout.LayoutParams(
            ViewGroup.LayoutParams.MATCH_PARENT,
            ViewGroup.LayoutParams.WRAP_CONTENT,
        )
        addView(input)
    }

    companion object {
        private const val ACTION_USB_PERMISSION = "com.messagepipeline.sample.USB_PERMISSION"
        private const val REQUEST_BLUETOOTH_CONNECT = 100
    }
}
