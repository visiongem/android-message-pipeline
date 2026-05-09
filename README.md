# android-message-pipeline

> "小通道里跑大消息"的统一抽象。BLE GATT (MTU 23) / USB Bulk / 串口 / 二维码 / Socket —— 任何受 MTU 限制的通道，都能用同一套分片 + 重组 + 异步分发协议。**自用为主**：作者过往工作里反复重写过这套，整理成可复用代码。

## 4 个核心抽象

```kotlin
interface Codec<T>          // 业务消息 ↔ 字节
interface Chunker           // 字节 ↔ 受 MTU 限制的物理帧
interface Transport         // 实际 IO（BLE / USB / Socket / ...）
class PipelineDispatcher    // 双线程异步管道（IO 与业务解耦）
```

## 快速开始：发送一条 5KB 消息

```kotlin
import com.messagepipeline.codec.StringCodec
import com.messagepipeline.chunker.DefaultChunker
import com.messagepipeline.transport.LoopbackTransport
import com.messagepipeline.dispatcher.PipelineDispatcher

// 1. 拿一对互通的 transport（实际项目里换成 BLE / USB transport 实现）
val (alice, bob) = LoopbackTransport.pair(mtu = 80)

// 2. Bob 端：监听
val bobPipeline = PipelineDispatcher(
    codec = StringCodec,
    chunker = DefaultChunker(),
    transport = bob,
    onMessage = { msg -> println("收到: $msg") }
).also { it.start() }

// 3. Alice 端：发
val alicePipeline = PipelineDispatcher<String>(
    codec = StringCodec,
    chunker = DefaultChunker(),
    transport = alice,
    onMessage = {}  // alice 只发不收
).also { it.start() }

alicePipeline.send("x".repeat(5000))   // 自动切成多帧 + 顺序无关地组装回去

// 关闭
alicePipeline.stop(); bobPipeline.stop()
alice.close(); bob.close()
```

## 分片协议（DefaultChunker）

```
[index:total:hash:group] payload_in_hex

例:
[0:5:a3f1c8d9:7] 1f4a3b...
[1:5:a3f1c8d9:7] 8c9d2e...
...
```

- `hash`: MD5 前 8 字符，用于完整性校验 + 区分不同消息
- `group`: 本机递增 id，支持多组消息**并发组装**（顺序无关）
- 整帧 ASCII（payload 也 hex 编码），便于在二维码 / 文本通道中传输；BLE / USB 等字节通道可自行实现 raw 版省去 hex 开销

## 接入新 Transport

任何"能发字节、能收字节"的通道都行。实现 [`Transport`](lib/src/main/kotlin/com/messagepipeline/Pipeline.kt) 接口的 4 个方法：

```kotlin
class MyBleTransport(private val gatt: BluetoothGatt) : Transport {
    override val mtu: Int = 20  // BLE 减去 GATT 头
    override fun send(frame: Frame) { /* 写到 GATT Characteristic */ }
    override fun onReceive(listener: (Frame) -> Unit) { /* 注册 BLE callback */ }
    override fun close() { gatt.close() }
}
```

## 测试

```bash
./gradlew :lib:testDebugUnitTest
```

包含：
- `DefaultChunkerTest`：分片协议正确性（顺序到达 / 乱序到达 / 多组并发）
- `PipelineEndToEndTest`：5KB 消息从 Alice 到 Bob 的端到端

## 当前不实现 / 留给调用方

- 真实 BLE Transport（用 androidx.bluetooth 或 Nordic 库自己实现）
- USB Accessory Transport
- 重传 / ACK 机制（DefaultChunker 假设 transport 可靠）
- 超时清理（chunker 长期未收齐的 group 会泄漏；调用方需在合适时机调用 `chunker.reset()`）

## License

Apache 2.0。
