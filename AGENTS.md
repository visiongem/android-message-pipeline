# AGENTS.md

用中文回复。

## 项目概览

这是一个 Android/Kotlin 消息管道库，用于在 BLE、USB 等受 MTU 限制的通道中传输大消息。

核心链路是：

```text
Codec → Chunker → Transport → PipelineDispatcher
```

- `DefaultChunker`：ASCII/hex 协议，适合文本通道。
- `BinaryChunker`：紧凑二进制协议，适合 BLE/USB。
- `ReliableTransport`：提供 ACK、重传和去重。
- `BleGattTransport` / `UsbBulkTransport`：真实 Android 硬件通道。
- `sample`：BLE/USB 真机手工联调应用。
- `PROTOCOL.md`：跨端线路协议；修改帧格式前必须阅读并同步更新。

## 开发约束

- 只写 Kotlin；使用 Gradle KTS、Version Catalog、JVM 17、minSdk 24。
- 保持现有 4 个核心抽象，不轻易破坏公开 API。
- BLE/USB、可靠层和分片代码要严格遵守 MTU，并处理关闭、超时、重复帧和损坏帧。
- 行为变更必须补测试；公开 API 或协议变化同时更新 KDoc、`README.md` 和 `PROTOCOL.md`。
- BLE/USB 真机协议要求通信对端实现 `PROTOCOL.md` 中对应格式。

## 验证

```bash
./gradlew :lib:testDebugUnitTest :lib:lintDebug
./gradlew :sample:assembleDebug :sample:lintDebug
```
