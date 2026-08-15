package com.messagepipeline

import com.messagepipeline.chunker.BinaryChunker
import com.messagepipeline.codec.BytesCodec
import com.messagepipeline.dispatcher.PipelineDispatcher
import com.messagepipeline.transport.LoopbackTransport
import com.messagepipeline.transport.ReliabilityConfig
import com.messagepipeline.transport.ReliableTransport
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.random.Random

class LowMtuReliablePipelineTest {

    @Test fun `5KB survives BLE minimum payload plus reliable overhead`() {
        val (rawAlice, rawBob) = LoopbackTransport.pair(mtu = 20)
        val reliability = ReliabilityConfig(ackTimeoutMillis = 100, retryBackoffMillis = 0)
        val aliceTransport = ReliableTransport(rawAlice, reliability, sessionId = 1)
        val bobTransport = ReliableTransport(rawBob, reliability, sessionId = 2)
        val received = AtomicReference<ByteArray>()
        val latch = CountDownLatch(1)

        val bob = PipelineDispatcher(
            codec = BytesCodec,
            chunker = BinaryChunker(maxMessageBytes = 10_000),
            transport = bobTransport,
            onMessage = { received.set(it); latch.countDown() },
        ).also { it.start() }
        val alice = PipelineDispatcher(
            codec = BytesCodec,
            chunker = BinaryChunker(maxMessageBytes = 10_000),
            transport = aliceTransport,
            onMessage = {},
        ).also { it.start() }

        try {
            val payload = Random(99).nextBytes(5_000)
            alice.send(payload)

            assertTrue(latch.await(5, TimeUnit.SECONDS))
            assertArrayEquals(payload, received.get())
        } finally {
            alice.stop()
            bob.stop()
            aliceTransport.close()
            bobTransport.close()
        }
    }
}
