package com.messagepipeline.chunker

import com.messagepipeline.Chunker
import com.messagepipeline.Frame
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.CRC32

/**
 * 面向 BLE/USB 等字节通道的紧凑分片协议。
 *
 * 11 字节二进制头依次为 magic/version(1)、group(2)、index(2)、total(2)、CRC32(4)，其余
 * 均为原始 payload。相比 [DefaultChunker] 的 ASCII/hex 格式，它适合 BLE 默认 ATT payload
 * 很小的场景。最多支持单条消息 65,535 帧。
 */
class BinaryChunker(
    private val groupTimeoutMillis: Long = DefaultChunker.DEFAULT_GROUP_TIMEOUT_MILLIS,
    private val maxPendingGroups: Int = DefaultChunker.DEFAULT_MAX_PENDING_GROUPS,
    private val maxPendingBytes: Long = DefaultChunker.DEFAULT_MAX_PENDING_BYTES,
    private val maxMessageBytes: Int = DefaultChunker.DEFAULT_MAX_MESSAGE_BYTES,
    private val timeSourceMillis: () -> Long = { System.nanoTime() / 1_000_000L },
) : Chunker {

    init {
        require(groupTimeoutMillis > 0) { "groupTimeoutMillis must be > 0" }
        require(maxPendingGroups > 0) { "maxPendingGroups must be > 0" }
        require(maxPendingBytes > 0) { "maxPendingBytes must be > 0" }
        require(maxMessageBytes > 0) { "maxMessageBytes must be > 0" }
    }

    private val nextGroup = AtomicInteger(0)
    private val pending = mutableMapOf<Int, GroupState>()
    private val lock = Any()
    private var pendingBytes = 0L

    override fun split(bytes: ByteArray, mtu: Int): Sequence<Frame> {
        require(bytes.size <= maxMessageBytes) {
            "message is ${bytes.size} bytes, max is $maxMessageBytes"
        }
        val payloadBytes = mtu - HEADER_SIZE
        if (bytes.isNotEmpty()) require(payloadBytes > 0) { "mtu $mtu must be > $HEADER_SIZE" }
        if (bytes.isEmpty()) require(mtu >= HEADER_SIZE) { "mtu $mtu must be >= $HEADER_SIZE" }

        val total = if (bytes.isEmpty()) 1 else
            ((bytes.size.toLong() + payloadBytes - 1) / payloadBytes).toInt()
        require(total in 1..MAX_FRAMES) { "message requires $total frames, max is $MAX_FRAMES" }
        val group = nextGroup.getAndIncrement() and 0xffff
        val checksum = bytes.crc32()

        return sequence {
            for (index in 0 until total) {
                val from = if (bytes.isEmpty()) 0 else index * payloadBytes
                val to = minOf(from + payloadBytes.coerceAtLeast(0), bytes.size)
                val frameBytes = ByteBuffer.allocate(HEADER_SIZE + to - from)
                    .put(MAGIC_AND_VERSION)
                    .putShort(group.toShort())
                    .putShort(index.toShort())
                    .putShort(total.toShort())
                    .putInt(checksum)
                    .put(bytes, from, to - from)
                    .array()
                check(frameBytes.size <= mtu)
                yield(Frame(frameBytes))
            }
        }
    }

    override fun assemble(frame: Frame): ByteArray? {
        if (frame.bytes.size < HEADER_SIZE) return null
        val buffer = ByteBuffer.wrap(frame.bytes)
        if (buffer.get() != MAGIC_AND_VERSION) return null
        val group = buffer.short.toInt() and 0xffff
        val index = buffer.short.toInt() and 0xffff
        val total = buffer.short.toInt() and 0xffff
        val checksum = buffer.int
        if (total !in 1..MAX_FRAMES || index !in 0 until total) return null
        val payload = ByteArray(buffer.remaining()).also(buffer::get)

        synchronized(lock) {
            val now = timeSourceMillis()
            cleanupExpiredLocked(now)
            var state = pending[group]
            if (state != null && (state.total != total || state.checksum != checksum)) {
                removeLocked(group)
                return null
            }
            if (state == null) {
                while (pending.size >= maxPendingGroups) removeOldestLocked()
                state = GroupState(total, checksum, lastUpdatedMillis = now)
                pending[group] = state
            }

            val previous = state.pieces[index]
            if (previous != null) {
                if (!previous.contentEquals(payload)) removeLocked(group)
                else state.lastUpdatedMillis = now
                return null
            }
            if (state.receivedBytes + payload.size > maxMessageBytes ||
                !makeRoomForBytesLocked(payload.size.toLong(), group)
            ) {
                removeLocked(group)
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
                piece.copyInto(assembled, cursor)
                cursor += piece.size
            }
            removeLocked(group)
            return assembled.takeIf { it.crc32() == checksum }
        }
    }

    fun cleanupExpired(): Int = synchronized(lock) {
        cleanupExpiredLocked(timeSourceMillis())
    }

    fun reset() {
        synchronized(lock) {
            pending.clear()
            pendingBytes = 0
        }
    }

    private fun cleanupExpiredLocked(now: Long): Int {
        val expired = pending.filterValues { now - it.lastUpdatedMillis >= groupTimeoutMillis }.keys.toList()
        expired.forEach(::removeLocked)
        return expired.size
    }

    private fun makeRoomForBytesLocked(bytes: Long, currentGroup: Int): Boolean {
        if (bytes > maxPendingBytes) return false
        while (pendingBytes + bytes > maxPendingBytes) {
            val oldest = pending.entries
                .filter { it.key != currentGroup }
                .minByOrNull { it.value.lastUpdatedMillis }
                ?.key ?: return false
            removeLocked(oldest)
        }
        return true
    }

    private fun removeOldestLocked() {
        pending.minByOrNull { it.value.lastUpdatedMillis }?.key?.let(::removeLocked)
    }

    private fun removeLocked(group: Int) {
        val removed = pending.remove(group) ?: return
        pendingBytes -= removed.receivedBytes
    }

    private data class GroupState(
        val total: Int,
        val checksum: Int,
        val pieces: MutableMap<Int, ByteArray> = mutableMapOf(),
        var receivedBytes: Int = 0,
        var lastUpdatedMillis: Long,
    )

    companion object {
        const val HEADER_SIZE = 11
        const val MAX_FRAMES = 65_535
        private const val MAGIC_AND_VERSION: Byte = 0x51
    }
}

private fun ByteArray.crc32(): Int = CRC32().also { it.update(this) }.value.toInt()
