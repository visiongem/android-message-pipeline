# CLAUDE.md

## 项目身份
- 名字：android-message-pipeline
- 用途：作者自用 Android 通用消息管道库；针对"小通道传大消息"问题
- 包前缀：`com.messagepipeline`
- License：Apache 2.0

## 技术栈硬约束
- Kotlin 2.0.21（不接受 Java 源文件）
- Gradle KTS + Version Catalog
- 协程（kotlinx.coroutines），但当前主线程模型用 BlockingQueue + Thread；将来重构到 Flow 也行
- JVM 17 toolchain
- minSdk 24

## 4 个核心抽象（不要轻易改 API）
- `Codec<T>`：业务消息 ↔ 字节
- `Chunker`：字节 ↔ 受 MTU 限制的物理帧
- `Transport`：实际 IO 通道
- `PipelineDispatcher`：双线程把以上三者粘起来

## 当前实现的范围
- `BytesCodec` / `StringCodec`：默认 codec
- `DefaultChunker`：`[index:total:hash:group]` 头格式
- `LoopbackTransport`：内存 transport（仅测试用）
- `PipelineDispatcher`：双线程管道

## 当前**不**实现
- BLE Transport：留给调用方（用 androidx.bluetooth / Nordic 自己接）
- USB Accessory Transport：同上
- 重传 / ACK：假设 transport 可靠
- 超时清理：调用方需在合适时机 `chunker.reset()`

## 设计原则
- **不引入 Hilt/Koin/RxJava**
- **抽象 4 个就够**，不要再加第 5 个抽象
- **每个核心组件必须有单元测试**——`DefaultChunkerTest` / `PipelineEndToEndTest` 是底线
- **API 简洁优先**，宁可让调用方多写一行也不暴露内部细节

## TODO
- [ ] 真实 BLE Transport 示例（独立 sample 模块）
- [ ] 错误回调（当前 dispatcher 内部异常被吞）
- [ ] 重传 / ACK（如果有真实场景需要）

## 与 secure-toolkit / kotlin-utils 的关系
三个仓库互不依赖。但理论上可组合：
- 用 `kotlin-utils` 的 hex 编解码替换默认 `DefaultChunker` 的 hex 实现
- 用 `secure-toolkit` 的 AES-GCM 包装 codec，做端到端加密管道
- 不强制——保持每个仓库独立工作的能力
