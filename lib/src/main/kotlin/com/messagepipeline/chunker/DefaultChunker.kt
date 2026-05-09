package com.messagepipeline.chunker

import com.messagepipeline.Chunker
import com.messagepipeline.Frame
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger

/**
 * 默认分片协议。
 *
 * ## 头格式（ASCII）
 *
 * ```
 * [index:total:hash:group] payload
 * ```
 *
 * - `index`：当前帧序号，从 0 开始
 * - `total`：本组总帧数
 * - `hash` ：完整数据的 MD5 前 8 个 hex 字符（用于完整性校验 + 区分不同消息）
 * - `group`：本机生成的 group id（递增整数），用于支持多组并发组装
 * - `payload`：原始字节（**注意**：当前实现把 payload 也用 hex 编码以便和 ASCII 头共存）
 *
 * 例：`[2:5:a3f1c8d9:7] 1f4a3b...`
 *
 * ## 设计取舍
 *
 * - **MD5 不是用作密码学校验**——只是粗略指纹，性能与冲突率折中
 * - **payload 用 hex 编码**：让整帧是 ASCII，方便在二维码 / 文本通道里传输；如果你的
 *   transport 本身就是字节通道（BLE / USB），可以自己实现一个 raw 版省掉编码开销
 * - **assemble 不做超时清理**：调用方负责调用 [reset] 在合适时机清理；否则长期不来的半截
 *   group 会泄漏内存
 */
class DefaultChunker : Chunker {

    private val groupCounter = AtomicInteger(0)
    private val pending = mutableMapOf<Int, MutableMap<Int, ByteArray>>()  // groupId → (idx → payload)
    private val pendingMeta = mutableMapOf<Int, GroupMeta>()
    private val lock = Any()

    override fun split(bytes: ByteArray, mtu: Int): Sequence<Frame> {
        require(mtu > MIN_MTU) { "mtu must be > $MIN_MTU to fit header" }
        val payloadBudget = mtu - MAX_HEADER_SIZE
        val hash = bytes.md5Prefix8()
        val groupId = groupCounter.getAndIncrement()
        val total = (bytes.size + payloadBudget - 1) / payloadBudget.coerceAtLeast(1)
        return sequence {
            for (i in 0 until total) {
                val from = i * payloadBudget
                val to = minOf(from + payloadBudget, bytes.size)
                val payload = bytes.copyOfRange(from, to)
                val header = "[$i:$total:$hash:$groupId] "
                val payloadHex = payload.toHex()
                yield(Frame((header + payloadHex).toByteArray(Charsets.US_ASCII)))
            }
        }
    }

    override fun assemble(frame: Frame): ByteArray? {
        val text = String(frame.bytes, Charsets.US_ASCII)
        val match = HEADER_RE.find(text) ?: return null
        val idx = match.groupValues[1].toInt()
        val total = match.groupValues[2].toInt()
        val hash = match.groupValues[3]
        val groupId = match.groupValues[4].toInt()
        val payloadHex = text.substring(match.range.last + 1)
        val payload = payloadHex.hexDecode() ?: return null

        synchronized(lock) {
            val map = pending.getOrPut(groupId) { mutableMapOf() }
            map[idx] = payload
            pendingMeta.getOrPut(groupId) { GroupMeta(total, hash) }
            if (map.size < total) return null

            // 收齐了：拼接 + 校验 hash
            val assembled = ByteArray(map.values.sumOf { it.size })
            var cursor = 0
            for (i in 0 until total) {
                val piece = map[i] ?: return null
                System.arraycopy(piece, 0, assembled, cursor, piece.size)
                cursor += piece.size
            }
            pending.remove(groupId)
            pendingMeta.remove(groupId)
            return if (assembled.md5Prefix8() == hash) assembled else null
        }
    }

    /** 主动清空中间状态（比如 transport 重连时）。 */
    fun reset() {
        synchronized(lock) {
            pending.clear()
            pendingMeta.clear()
        }
    }

    private data class GroupMeta(val total: Int, val hash: String)

    companion object {
        // 头最大 32 字节（[i:total:hash:group] + 空格），保险起见保留 40
        private const val MAX_HEADER_SIZE = 40
        private const val MIN_MTU = MAX_HEADER_SIZE
        private val HEADER_RE = Regex("""\[(\d+):(\d+):([0-9a-f]{8}):(\d+)\] """)
    }
}

private fun ByteArray.md5Prefix8(): String {
    val md = MessageDigest.getInstance("MD5").digest(this)
    return md.joinToString("") { "%02x".format(it) }.substring(0, 8)
}

private fun ByteArray.toHex(): String =
    joinToString("") { "%02x".format(it) }

private fun String.hexDecode(): ByteArray? {
    if (length % 2 != 0) return null
    val out = ByteArray(length / 2)
    for (i in out.indices) {
        val hi = Character.digit(this[i * 2], 16)
        val lo = Character.digit(this[i * 2 + 1], 16)
        if (hi < 0 || lo < 0) return null
        out[i] = ((hi shl 4) or lo).toByte()
    }
    return out
}
