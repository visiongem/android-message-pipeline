package com.messagepipeline

/**
 * 4 个核心抽象组合起来 = 一个能在 BLE / USB / 串口 / 二维码 / Socket 上传"小通道里跑大消息"的管道。
 * 调用方只需要：
 *   1. 选一个 [Codec]（业务消息 ↔ 字节）
 *   2. 选一个 [Chunker]（字节 ↔ 受 MTU 限制的物理帧）
 *   3. 接一个 [Transport]（实际的 IO 通道）
 *   4. 用 [com.messagepipeline.dispatcher.PipelineDispatcher] 串起来
 */

/**
 * 业务消息与字节流之间的双向转换。
 *
 * 实现示例：
 * - [com.messagepipeline.codec.BytesCodec]: 直通（业务消息本身就是字节）
 * - [com.messagepipeline.codec.StringCodec]: UTF-8 字符串 ↔ 字节
 * - 业务自己实现：Protobuf / Gson 等
 */
interface Codec<T> {
    fun encode(value: T): ByteArray
    fun decode(bytes: ByteArray): T
}

/**
 * 把整段字节切成多个不超过 [mtu] 的 [Frame]，以及反向重组。
 *
 * 默认实现 [com.messagepipeline.chunker.DefaultChunker] 使用
 *   `[index:total:hash:groupId] payload`
 * 头格式，支持乱序到达 + 多组并发。
 * 字节通道可使用 [com.messagepipeline.chunker.BinaryChunker] 降低帧头与 hex 编码开销。
 */
interface Chunker {
    /** 把 [bytes] 切片。返回的 Frame 序列**包含完整头**，可直接送到 [Transport]。 */
    fun split(bytes: ByteArray, mtu: Int): Sequence<Frame>

    /**
     * 累积接收到的 Frame；一旦某个 group 收齐，返回该 group 的完整字节并从内部状态删除。
     * 还没收齐时返回 null。
     */
    fun assemble(frame: Frame): ByteArray?
}

/**
 * 一个分片（chunker 的输出 / 输入单元）。一般直接 [bytes] 写到 transport。
 */
data class Frame(val bytes: ByteArray) {
    override fun equals(other: Any?): Boolean =
        other is Frame && bytes.contentEquals(other.bytes)
    override fun hashCode(): Int = bytes.contentHashCode()
}

/**
 * 实际 IO 通道。任何"能发字节、能收字节"的通道都可作为 Transport：
 * BLE GATT Characteristic / USB Bulk Transfer / Socket / 串口 / 二维码动图...
 *
 * 实现示例：[com.messagepipeline.transport.LoopbackTransport]（用于测试）
 */
interface Transport {
    /** 通道可承载的 [Frame.bytes] 字节数。BLE 实现返回的是 ATT MTU 扣除协议头后的值。 */
    val mtu: Int

    /** 发送一帧。**阻塞**直到完成或抛异常。 */
    fun send(frame: Frame)

    /** 注册一帧到达回调。回调可能在任意线程。 */
    fun onReceive(listener: (Frame) -> Unit)

    /** 关闭通道，释放底层资源。 */
    fun close()
}
