package com.messagepipeline.transport

import com.messagepipeline.Frame
import com.messagepipeline.Transport
import java.nio.ByteBuffer
import java.util.LinkedHashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/** ACK/重传参数。一次初始发送失败后，最多再尝试 [maxRetries] 次。 */
data class ReliabilityConfig(
    val ackTimeoutMillis: Long = 500,
    val maxRetries: Int = 3,
    val retryBackoffMillis: Long = 50,
    val deduplicationWindowMillis: Long = 60_000,
    val maxRememberedFrames: Int = 2_048,
) {
    init {
        require(ackTimeoutMillis > 0) { "ackTimeoutMillis must be > 0" }
        require(maxRetries >= 0) { "maxRetries must be >= 0" }
        require(retryBackoffMillis >= 0) { "retryBackoffMillis must be >= 0" }
        require(deduplicationWindowMillis > 0) { "deduplicationWindowMillis must be > 0" }
        require(maxRememberedFrames > 0) { "maxRememberedFrames must be > 0" }
    }
}

/** 可靠发送失败。底层 IO 异常可通过 [cause] 获取。 */
class ReliableTransportException(message: String, cause: Throwable? = null) :
    IllegalStateException(message, cause)

/**
 * 为任意双向 [Transport] 增加逐帧 ACK、超时重传和重复投递抑制。
 *
 * 该类本身仍实现 [Transport]，因此不会改变 Pipeline 的 4 个核心抽象。它独占底层 transport
 * 的接收监听器；通信两端都必须使用相同版本的 ReliableTransport 协议。
 */
class ReliableTransport(
    private val delegate: Transport,
    private val config: ReliabilityConfig = ReliabilityConfig(),
    private val sessionId: Int = ThreadLocalRandom.current().nextInt(SESSION_ID_LIMIT),
    private val timeSourceMillis: () -> Long = { System.nanoTime() / 1_000_000L },
) : Transport {

    init {
        require(sessionId in 0 until SESSION_ID_LIMIT) { "sessionId must fit an unsigned short" }
        require(delegate.mtu > HEADER_SIZE) {
            "delegate mtu ${delegate.mtu} must be > reliable header size $HEADER_SIZE"
        }
        delegate.onReceive(::handleIncoming)
    }

    override val mtu: Int = delegate.mtu - HEADER_SIZE

    private val closed = AtomicBoolean(false)
    private val nextSequence = AtomicLong(0)
    private val listener = AtomicReference<((Frame) -> Unit)?>(null)
    private val pendingAcks = ConcurrentHashMap<Int, PendingAck>()
    private val receivedLock = Any()
    private val receivedFrames = LinkedHashMap<MessageId, Long>()

    override fun send(frame: Frame) {
        checkOpen()
        require(frame.bytes.size <= mtu) {
            "frame is ${frame.bytes.size} bytes, reliable mtu is $mtu"
        }

        val sequence = nextSequence.getAndIncrement().toInt()
        val pending = PendingAck()
        check(pendingAcks.putIfAbsent(sequence, pending) == null) {
            "sequence $sequence is already pending"
        }

        try {
            val packet = encodePacket(TYPE_DATA, sessionId, sequence, frame.bytes)
            for (attempt in 0..config.maxRetries) {
                checkOpen()
                try {
                    delegate.send(packet)
                } catch (error: Throwable) {
                    if (attempt == config.maxRetries) {
                        throw ReliableTransportException(
                            "frame $sequence failed after ${attempt + 1} attempts",
                            error,
                        )
                    }
                }

                try {
                    pending.latch.await(config.ackTimeoutMillis, TimeUnit.MILLISECONDS)
                } catch (interrupted: InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw ReliableTransportException("interrupted while waiting for ACK $sequence", interrupted)
                }
                if (pending.acknowledged.get()) return
                checkOpen()

                if (attempt < config.maxRetries && config.retryBackoffMillis > 0) {
                    try {
                        Thread.sleep(backoffFor(attempt))
                    } catch (interrupted: InterruptedException) {
                        Thread.currentThread().interrupt()
                        throw ReliableTransportException("interrupted before retrying frame $sequence", interrupted)
                    }
                }
            }
            throw ReliableTransportException(
                "ACK timeout for frame $sequence after ${config.maxRetries + 1} attempts",
            )
        } finally {
            pendingAcks.remove(sequence, pending)
        }
    }

    override fun onReceive(listener: (Frame) -> Unit) {
        checkOpen()
        this.listener.set(listener)
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        pendingAcks.values.forEach { it.latch.countDown() }
        pendingAcks.clear()
        listener.set(null)
        synchronized(receivedLock) { receivedFrames.clear() }
        delegate.close()
    }

    private fun handleIncoming(frame: Frame) {
        if (closed.get()) return
        val packet = decodePacket(frame) ?: return
        when (packet.type) {
            TYPE_ACK -> {
                if (packet.sessionId == sessionId) {
                    pendingAcks[packet.sequence]?.let { pending ->
                        pending.acknowledged.set(true)
                        pending.latch.countDown()
                    }
                }
            }

            TYPE_DATA -> receiveData(packet)
        }
    }

    private fun receiveData(packet: Packet) {
        val currentListener = listener.get() ?: return
        val id = MessageId(packet.sessionId, packet.sequence)
        val firstDelivery = synchronized(receivedLock) {
            val now = timeSourceMillis()
            cleanupReceivedLocked(now)
            val isNew = receivedFrames.put(id, now) == null
            while (receivedFrames.size > config.maxRememberedFrames) {
                val oldest = receivedFrames.entries.iterator()
                if (!oldest.hasNext()) break
                oldest.next()
                oldest.remove()
            }
            isNew
        }

        try {
            delegate.send(encodePacket(TYPE_ACK, packet.sessionId, packet.sequence, ByteArray(0)))
        } catch (_: Throwable) {
            // 对端会因 ACK 超时而重发；收到重复 DATA 时仍会再次尝试 ACK。
        }

        if (firstDelivery && !closed.get()) currentListener(Frame(packet.payload))
    }

    private fun cleanupReceivedLocked(nowMillis: Long) {
        val iterator = receivedFrames.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (nowMillis - entry.value >= config.deduplicationWindowMillis) iterator.remove()
        }
    }

    private fun checkOpen() {
        if (closed.get()) throw ReliableTransportException("transport closed")
    }

    private fun backoffFor(attempt: Int): Long {
        val multiplier = 1L shl attempt.coerceAtMost(20)
        if (config.retryBackoffMillis >= MAX_BACKOFF_MILLIS / multiplier) return MAX_BACKOFF_MILLIS
        return config.retryBackoffMillis * multiplier
    }

    private data class PendingAck(
        val latch: CountDownLatch = CountDownLatch(1),
        val acknowledged: AtomicBoolean = AtomicBoolean(false),
    )

    private data class MessageId(val sessionId: Int, val sequence: Int)

    private data class Packet(
        val type: Byte,
        val sessionId: Int,
        val sequence: Int,
        val payload: ByteArray,
    )

    companion object {
        private const val MAGIC: Byte = 0x6d
        private const val VERSION_AND_DATA: Byte = 0x10
        private const val VERSION_AND_ACK: Byte = 0x11
        private const val TYPE_DATA: Byte = 0
        private const val TYPE_ACK: Byte = 1
        private const val HEADER_SIZE = 8
        private const val SESSION_ID_LIMIT = 65_536
        private const val MAX_BACKOFF_MILLIS = 30_000L

        private fun encodePacket(type: Byte, sessionId: Int, sequence: Int, payload: ByteArray): Frame {
            val bytes = ByteBuffer.allocate(HEADER_SIZE + payload.size)
                .put(MAGIC)
                .put(if (type == TYPE_DATA) VERSION_AND_DATA else VERSION_AND_ACK)
                .putShort(sessionId.toShort())
                .putInt(sequence)
                .put(payload)
                .array()
            return Frame(bytes)
        }

        private fun decodePacket(frame: Frame): Packet? {
            if (frame.bytes.size < HEADER_SIZE) return null
            val buffer = ByteBuffer.wrap(frame.bytes)
            if (buffer.get() != MAGIC) return null
            val type = when (buffer.get()) {
                VERSION_AND_DATA -> TYPE_DATA
                VERSION_AND_ACK -> TYPE_ACK
                else -> return null
            }
            val packetSessionId = buffer.short.toInt() and 0xffff
            val sequence = buffer.int
            val payloadSize = buffer.remaining()
            if (type == TYPE_ACK && payloadSize != 0) return null
            val payload = ByteArray(payloadSize).also(buffer::get)
            return Packet(type, packetSessionId, sequence, payload)
        }
    }
}
