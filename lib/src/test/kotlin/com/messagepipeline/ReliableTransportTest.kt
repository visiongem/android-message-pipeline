package com.messagepipeline

import com.messagepipeline.transport.ReliabilityConfig
import com.messagepipeline.transport.ReliableTransport
import com.messagepipeline.transport.ReliableTransportException
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class ReliableTransportTest {

    private val fastConfig = ReliabilityConfig(
        ackTimeoutMillis = 10,
        maxRetries = 2,
        retryBackoffMillis = 0,
    )

    @Test fun `delivers frame and acknowledges it`() {
        val (rawA, rawB) = TestTransport.pair(mtu = 128)
        val a = ReliableTransport(rawA, fastConfig, sessionId = 1)
        val b = ReliableTransport(rawB, fastConfig, sessionId = 2)
        val received = AtomicReference<Frame>()
        b.onReceive(received::set)

        val payload = Frame("hello".toByteArray())
        a.send(payload)

        assertArrayEquals(payload.bytes, received.get().bytes)
        assertEquals(128 - 8, a.mtu)
    }

    @Test fun `retries a dropped data packet`() {
        val (rawA, rawB) = TestTransport.pair(mtu = 128)
        val dataWrites = AtomicInteger()
        rawA.drop = { frame -> isData(frame) && dataWrites.getAndIncrement() == 0 }
        val a = ReliableTransport(rawA, fastConfig, sessionId = 1)
        val b = ReliableTransport(rawB, fastConfig, sessionId = 2)
        val deliveries = AtomicInteger()
        b.onReceive { deliveries.incrementAndGet() }

        a.send(Frame(byteArrayOf(1, 2, 3)))

        assertEquals(2, dataWrites.get())
        assertEquals(1, deliveries.get())
    }

    @Test fun `lost ack causes retry without duplicate delivery`() {
        val (rawA, rawB) = TestTransport.pair(mtu = 128)
        val ackWrites = AtomicInteger()
        rawB.drop = { frame -> isAck(frame) && ackWrites.getAndIncrement() == 0 }
        val a = ReliableTransport(rawA, fastConfig, sessionId = 1)
        val b = ReliableTransport(rawB, fastConfig, sessionId = 2)
        val deliveries = AtomicInteger()
        b.onReceive { deliveries.incrementAndGet() }

        a.send(Frame(byteArrayOf(9)))

        assertEquals(2, ackWrites.get())
        assertEquals(1, deliveries.get())
    }

    @Test fun `throws after ack retry budget is exhausted`() {
        val (rawA, rawB) = TestTransport.pair(mtu = 128)
        rawB.drop = ::isAck
        val a = ReliableTransport(rawA, fastConfig, sessionId = 1)
        val b = ReliableTransport(rawB, fastConfig, sessionId = 2)
        b.onReceive { }

        assertThrows(ReliableTransportException::class.java) {
            a.send(Frame(byteArrayOf(4)))
        }
        assertEquals(3, rawA.sent.get())
    }

    private class TestTransport(override val mtu: Int) : Transport {
        val sent = AtomicInteger()
        var drop: (Frame) -> Boolean = { false }
        private val listener = AtomicReference<((Frame) -> Unit)?>(null)
        private lateinit var peer: TestTransport
        private var closed = false

        override fun send(frame: Frame) {
            check(!closed)
            sent.incrementAndGet()
            if (!drop(frame)) peer.listener.get()?.invoke(frame)
        }

        override fun onReceive(listener: (Frame) -> Unit) {
            this.listener.set(listener)
        }

        override fun close() {
            closed = true
            listener.set(null)
        }

        companion object {
            fun pair(mtu: Int): Pair<TestTransport, TestTransport> {
                val a = TestTransport(mtu)
                val b = TestTransport(mtu)
                a.peer = b
                b.peer = a
                return a to b
            }
        }
    }

    companion object {
        private fun isData(frame: Frame): Boolean = frame.bytes.size >= 2 && frame.bytes[1].toInt() == 0x10
        private fun isAck(frame: Frame): Boolean = frame.bytes.size >= 2 && frame.bytes[1].toInt() == 0x11
    }
}
