# android-message-pipeline

> “小通道里跑大消息”的统一抽象。BLE GATT、USB Bulk、串口、二维码、Socket 等受 MTU
> 限制的通道，都可以复用同一套编解码、分片重组和异步分发模型。

## 4 个核心抽象

```kotlin
interface Codec<T>          // 业务消息 ↔ 字节
interface Chunker           // 字节 ↔ 受 MTU 限制的 Frame
interface Transport         // 实际 IO
class PipelineDispatcher<T> // IO 与业务回调解耦
```

仓库提供以下实现：

- `BytesCodec` / `StringCodec`
- `DefaultChunker`：可读的 ASCII/hex 帧，适合文本和二维码通道
- `BinaryChunker`：11 字节二进制头，适合 BLE/USB 等低 MTU 字节通道
- `LoopbackTransport`：内存测试替身
- `ReliableTransport`：包装其他 Transport，提供 ACK、重传和重复帧抑制
- `BleGattTransport`：Android BLE GATT Client/Central
- `UsbBulkTransport`：Android USB Host Bulk IN/OUT

## 快速开始

```kotlin
val (alice, bob) = LoopbackTransport.pair(mtu = 80)

val bobPipeline = PipelineDispatcher(
    codec = StringCodec,
    chunker = DefaultChunker(),
    transport = bob,
    onMessage = { message -> println("收到: $message") },
    onError = { error -> error.printStackTrace() },
).also { it.start() }

val alicePipeline = PipelineDispatcher(
    codec = StringCodec,
    chunker = DefaultChunker(),
    transport = alice,
    onMessage = {},
).also { it.start() }

alicePipeline.send("x".repeat(5000))

alicePipeline.stop()
bobPipeline.stop()
alice.close()
bob.close()
```

## 选择 Chunker

`DefaultChunker` 的线路格式为：

```text
[index:total:hash:group] payload_in_hex
```

它便于抓包和文本通道传输，但 hex 会把 payload 扩大一倍。现在每个输出帧都会严格满足
`Frame.bytes.size <= mtu`。

`BinaryChunker` 使用 11 字节头和原始 payload，包含 group、index、total 和完整消息 CRC32。
BLE、USB 和 Socket 默认应优先使用它。两种实现都支持乱序、重复帧、多 group 交错、超时清理、
最大 pending group 数和最大 pending 字节数。

超时清理是惰性的：新帧到达或调用 `cleanupExpired()` 时执行；断线重连可调用 `reset()`。

## ACK 和重传

`ReliableTransport` 可以包装任意双向 Transport：

```kotlin
val reliable = ReliableTransport(
    delegate = physicalTransport,
    config = ReliabilityConfig(
        ackTimeoutMillis = 500,
        maxRetries = 3,
        retryBackoffMillis = 50,
    ),
)

val pipeline = PipelineDispatcher(
    codec = BytesCodec,
    chunker = BinaryChunker(),
    transport = reliable,
    onMessage = ::handleMessage,
    onError = ::handleError,
)
```

可靠层使用 8 字节二进制头，`reliable.mtu` 会自动扣除该开销。ACK 丢失时 DATA 会重发，接收端
按 session + sequence 去重并重新 ACK。重试耗尽会抛出 `ReliableTransportException`。

通信两端必须都实现该可靠层协议。它确认的是“对端可靠层已接收 Frame”，不是业务处理完成。

## BLE GATT Client

宿主应用负责扫描 `BluetoothDevice`，并在 Android 12+ 动态申请 `BLUETOOTH_CONNECT`。连接入口
是挂起函数，返回时已完成服务发现、MTU 请求和通知订阅：

```kotlin
val ble = BleGattTransport.connect(
    context = context,
    device = device,
    config = BleGattConfig(
        serviceUuid = SERVICE_UUID,
        writeCharacteristicUuid = WRITE_UUID,
        notifyCharacteristicUuid = NOTIFY_UUID,
    ),
    onError = ::handleError,
)

val transport = ReliableTransport(ble) // 可选；对端必须支持
val pipeline = PipelineDispatcher(
    codec = BytesCodec,
    chunker = BinaryChunker(),
    transport = transport,
    onMessage = ::handleMessage,
)
```

BLE Transport 的 `mtu` 是协商后的 ATT MTU 减 3，而不是请求值。GATT 写操作会串行执行并等待
`onCharacteristicWrite`；通知会转交独立线程，避免业务或 ACK 阻塞系统蓝牙回调。

当前 BLE 实现只负责 Android GATT Client/Central，不包含扫描 UI、GATT Server/Peripheral 或
自动重连。

## USB Host Bulk

宿主应用先枚举设备并调用 `UsbManager.requestPermission()`。权限授予后：

```kotlin
val usb = UsbBulkTransport.open(
    usbManager = usbManager,
    device = device,
    config = UsbBulkConfig(
        interfaceIndex = null, // 自动寻找同时包含 Bulk IN/OUT 的接口
    ),
    onError = ::handleError,
)

val pipeline = PipelineDispatcher(
    codec = BytesCodec,
    chunker = BinaryChunker(),
    transport = usb,
    onMessage = ::handleMessage,
)
```

USB Bulk 是字节流，本实现在线路上为每个 Frame 增加 4 字节大端长度前缀；对端也必须实现该
分帧规则。写入会处理部分传输，读取线程能处理拆包和粘包。宿主应用仍负责 USB attach/detach
监听并在拔出时调用 `close()`。

当前 USB 实现仅支持 USB Host Bulk endpoint，不支持 USB Accessory、CDC ACM 串口初始化或
厂商私有 control transfer。

## 真机联调 Sample

`sample` 模块是不绑定厂商协议的手工联调应用：BLE 可输入设备 MAC、Service/Write/Notify UUID，
USB 会选择首个已连接设备并申请权限；两者都可以选择是否启用可靠层并发送 UTF-8 消息。

```bash
./gradlew :sample:installDebug
```

对端需要实现 [PROTOCOL.md](PROTOCOL.md) 中对应的 BinaryChunker、可靠层和 USB 长度前缀协议。

## 测试

```bash
./gradlew :lib:testDebugUnitTest
```

测试覆盖：

- ASCII 与二进制分片的 MTU、顺序/乱序、重复帧、并发 group、空消息和超时
- ACK/DATA 丢失、重传耗尽和去重
- USB 长度前缀的拆包、粘包和非法长度
- 5KB 消息在模拟 BLE 默认 20 字节 ATT payload、再扣除可靠层头后的端到端传输

BLE/USB 真机行为仍需要匹配 UUID 或 endpoint 的外设进行设备测试。

## License

Apache 2.0。
