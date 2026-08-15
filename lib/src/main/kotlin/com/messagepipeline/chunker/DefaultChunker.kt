package com.messagepipeline.chunker

import com.messagepipeline.Chunker
import com.messagepipeline.Frame
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong

/**
 * 默认 ASCII/hex 分片协议。
 *
 * 帧格式为 `[index:total:hash:group] payload`。它支持乱序、重复帧和多组交错重组，并对
 * 未完成消息施加超时及内存上限。MD5 前 8 位只用作传输完整性指纹，不提供密码学安全性。
 *
 * 过期 group 会在下一次 [assemble] 或显式调用 [cleanupExpired] 时清理；这种惰性清理不会
 * 创建后台线程。连接重建时可调用 [reset] 立即释放全部中间状态。
 */
class DefaultChunker(
    private val groupTimeoutMillis: Long = DEFAULT_GROUP_TIMEOUT_MILLIS,
    private val maxPendingGroups: Int = DEFAULT_MAX_PENDING_GROUPS,
    private val maxPendingBytes: Long = DEFAULT_MAX_PENDING_BYTES,
    private val maxMessageBytes: Int = DEFAULT_MAX_MESSAGE_BYTES,
    private val maxFramesPerGroup: Int = DEFAULT_MAX_FRAMES_PER_GROUP,
    private val timeSourceMillis: () -> Long = { System.nanoTime() / 1_000_000L },
) : Chunker {

    init {
        require(groupTimeoutMillis > 0) { "groupTimeoutMillis must be > 0" }
        require(maxPendingGroups > 0) { "maxPendingGroups must be > 0" }
        require(maxPendingBytes > 0) { "maxPendingBytes must be > 0" }
        require(maxMessageBytes > 0) { "maxMessageBytes must be > 0" }
        require(maxFramesPerGroup > 0) { "maxFramesPerGroup must be > 0" }
    }

    private val groupCounter = AtomicLong(0)
    private val pending = mutableMapOf<Long, GroupState>()
    private val lock = Any()
    private var pendingBytes = 0L

    override fun split(bytes: ByteArray, mtu: Int): Sequence<Frame> {
        require(bytes.size <= maxMessageBytes) {
            "message is ${bytes.size} bytes, max is $maxMessageBytes"
        }

        val hash = bytes.md5Prefix8()
        val groupId = nextGroupId()
        val layout = calculateLayout(bytes.size, mtu, hash, groupId)

        return sequence {
            if (bytes.isEmpty()) {
                yield(Frame(header(0, 1, hash, groupId).toByteArray(Charsets.US_ASCII)))
                return@sequence
            }

            for (index in 0 until layout.total) {
                val from = index * layout.payloadBytes
                val to = minOf(from + layout.payloadBytes, bytes.size)
                val text = header(index, layout.total, hash, groupId) +
                    bytes.copyOfRange(from, to).toHex()
                val frame = Frame(text.toByteArray(Charsets.US_ASCII))
                check(frame.bytes.size <= mtu) { "internal error: encoded frame exceeds mtu" }
                yield(frame)
            }
        }
    }

    override fun assemble(frame: Frame): ByteArray? {
        val text = frame.bytes.toString(Charsets.US_ASCII)
        val match = FRAME_RE.matchEntire(text) ?: return null
        val index = match.groupValues[1].toIntOrNull() ?: return null
        val total = match.groupValues[2].toIntOrNull() ?: return null
        val hash = match.groupValues[3]
        val groupId = match.groupValues[4].toLongOrNull() ?: return null
        val payload = match.groupValues[5].hexDecode() ?: return null

        if (groupId < 0 || total !in 1..maxFramesPerGroup || index !in 0 until total) return null
        if (payload.size > maxMessageBytes) return null

        synchronized(lock) {
            val now = timeSourceMillis()
            cleanupExpiredLocked(now)

            var state = pending[groupId]
            if (state != null && (state.total != total || state.hash != hash)) {
                removeGroupLocked(groupId)
                return null
            }

            if (state == null) {
                makeRoomForGroupLocked()
                state = GroupState(total = total, hash = hash, lastUpdatedMillis = now)
                pending[groupId] = state
            }

            val previous = state.pieces[index]
            if (previous != null) {
                if (!previous.contentEquals(payload)) {
                    removeGroupLocked(groupId)
                    return null
                }
                state.lastUpdatedMillis = now
                return null
            }

            if (state.receivedBytes + payload.size > maxMessageBytes ||
                !makeRoomForBytesLocked(payload.size.toLong(), excludingGroupId = groupId)
            ) {
                removeGroupLocked(groupId)
                return null
            }

            state.pieces[index] = payload
            state.receivedBytes += payload.size
            state.lastUpdatedMillis = now
            pendingBytes += payload.size

            if (state.pieces.size < total) return null

            val assembled = ByteArray(state.receivedBytes)
            var cursor = 0
            for (pieceIndex in 0 until total) {
                val piece = state.pieces[pieceIndex] ?: return null
                piece.copyInto(assembled, destinationOffset = cursor)
                cursor += piece.size
            }
            removeGroupLocked(groupId)
            return assembled.takeIf { it.md5Prefix8() == hash }
        }
    }

    /** 清理已超过 [groupTimeoutMillis] 的未完成 group，返回清理数量。 */
    fun cleanupExpired(): Int = synchronized(lock) {
        cleanupExpiredLocked(timeSourceMillis())
    }

    /** 主动清空全部中间状态，通常在 transport 断开或重连时调用。 */
    fun reset() {
        synchronized(lock) {
            pending.clear()
            pendingBytes = 0
        }
    }

    private fun calculateLayout(
        messageSize: Int,
        mtu: Int,
        hash: String,
        groupId: Long,
    ): Layout {
        require(mtu > 0) { "mtu must be > 0" }
        var total = 1

        while (true) {
            val largestHeaderBytes = header(total - 1, total, hash, groupId)
                .toByteArray(Charsets.US_ASCII).size
            val payloadBytes = (mtu - largestHeaderBytes) / HEX_CHARS_PER_BYTE
            if (messageSize == 0) {
                require(largestHeaderBytes <= mtu) { "mtu $mtu is too small for protocol header" }
                return Layout(total = 1, payloadBytes = 0)
            }
            require(payloadBytes > 0) { "mtu $mtu is too small for protocol header and payload" }

            val calculatedTotal = ((messageSize.toLong() + payloadBytes - 1) / payloadBytes).toInt()
            require(calculatedTotal <= maxFramesPerGroup) {
                "message requires $calculatedTotal frames, max is $maxFramesPerGroup"
            }
            if (calculatedTotal == total) return Layout(total, payloadBytes)
            total = calculatedTotal
        }
    }

    private fun nextGroupId(): Long = groupCounter.getAndUpdate { current ->
        if (current == Long.MAX_VALUE) 0 else current + 1
    }

    private fun makeRoomForGroupLocked() {
        while (pending.size >= maxPendingGroups) removeOldestGroupLocked()
    }

    private fun makeRoomForBytesLocked(bytes: Long, excludingGroupId: Long): Boolean {
        if (bytes > maxPendingBytes) return false
        while (pendingBytes + bytes > maxPendingBytes) {
            val oldest = pending.entries
                .asSequence()
                .filter { it.key != excludingGroupId }
                .minByOrNull { it.value.lastUpdatedMillis }
                ?.key ?: return false
            removeGroupLocked(oldest)
        }
        return true
    }

    private fun cleanupExpiredLocked(nowMillis: Long): Int {
        val expired = pending
            .filterValues { nowMillis - it.lastUpdatedMillis >= groupTimeoutMillis }
            .keys
            .toList()
        expired.forEach(::removeGroupLocked)
        return expired.size
    }

    private fun removeOldestGroupLocked() {
        pending.minByOrNull { it.value.lastUpdatedMillis }?.key?.let(::removeGroupLocked)
    }

    private fun removeGroupLocked(groupId: Long) {
        val removed = pending.remove(groupId) ?: return
        pendingBytes -= removed.receivedBytes
    }

    private fun header(index: Int, total: Int, hash: String, groupId: Long): String =
        "[$index:$total:$hash:$groupId] "

    private data class Layout(val total: Int, val payloadBytes: Int)

    private data class GroupState(
        val total: Int,
        val hash: String,
        val pieces: MutableMap<Int, ByteArray> = mutableMapOf(),
        var receivedBytes: Int = 0,
        var lastUpdatedMillis: Long,
    )

    companion object {
        const val DEFAULT_GROUP_TIMEOUT_MILLIS = 30_000L
        const val DEFAULT_MAX_PENDING_GROUPS = 64
        const val DEFAULT_MAX_PENDING_BYTES = 8L * 1024 * 1024
        const val DEFAULT_MAX_MESSAGE_BYTES = 4 * 1024 * 1024
        const val DEFAULT_MAX_FRAMES_PER_GROUP = 100_000

        private const val HEX_CHARS_PER_BYTE = 2
        private val FRAME_RE = Regex("""\[(\d+):(\d+):([0-9a-f]{8}):(\d+)\] ([0-9a-f]*)""")
    }
}

private fun ByteArray.md5Prefix8(): String =
    MessageDigest.getInstance("MD5")
        .digest(this)
        .joinToString("") { "%02x".format(it) }
        .substring(0, 8)

private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }

private fun String.hexDecode(): ByteArray? {
    if (length % 2 != 0) return null
    val out = ByteArray(length / 2)
    for (index in out.indices) {
        val high = Character.digit(this[index * 2], 16)
        val low = Character.digit(this[index * 2 + 1], 16)
        if (high < 0 || low < 0) return null
        out[index] = ((high shl 4) or low).toByte()
    }
    return out
}
