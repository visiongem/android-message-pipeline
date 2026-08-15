package com.messagepipeline

import com.messagepipeline.chunker.DefaultChunker
import com.messagepipeline.codec.StringCodec
import com.messagepipeline.dispatcher.PipelineDispatcher
import com.messagepipeline.transport.LoopbackTransport
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class PipelineEndToEndTest {

    @Test fun `dispatcher cannot be restarted after stop`() {
        val transport = LoopbackTransport()
        val pipeline = PipelineDispatcher(
            codec = StringCodec,
            chunker = DefaultChunker(),
            transport = transport,
            onMessage = {},
        )

        pipeline.start()
        pipeline.stop()

        assertThrows(IllegalStateException::class.java) { pipeline.start() }
        transport.close()
    }

    @Test fun `alice sends 5KB string, bob receives intact`() {
        val (alice, bob) = LoopbackTransport.pair(mtu = 80)

        val received = mutableListOf<String>()
        val latch = CountDownLatch(1)

        val bobReceiver = PipelineDispatcher(
            codec = StringCodec,
            chunker = DefaultChunker(),
            transport = bob,
            onMessage = { msg ->
                synchronized(received) { received.add(msg) }
                latch.countDown()
            }
        ).also { it.start() }

        val aliceSender = PipelineDispatcher<String>(
            codec = StringCodec,
            chunker = DefaultChunker(),
            transport = alice,
            onMessage = { /* alice 只发不收 */ }
        ).also { it.start() }

        try {
            val payload = "x".repeat(5000)
            aliceSender.send(payload)

            val arrived = latch.await(5, TimeUnit.SECONDS)
            assertEquals(true, arrived)
            assertEquals(1, received.size)
            assertEquals(payload, received[0])
        } finally {
            aliceSender.stop()
            bobReceiver.stop()
            alice.close()
            bob.close()
        }
    }

    @Test fun `short message single frame`() {
        val (alice, bob) = LoopbackTransport.pair(mtu = 200)
        val received = mutableListOf<String>()
        val latch = CountDownLatch(1)

        val bobPipeline = PipelineDispatcher(
            codec = StringCodec,
            chunker = DefaultChunker(),
            transport = bob,
            onMessage = { msg -> synchronized(received) { received.add(msg) }; latch.countDown() }
        ).also { it.start() }

        val alicePipeline = PipelineDispatcher<String>(
            codec = StringCodec,
            chunker = DefaultChunker(),
            transport = alice,
            onMessage = {}
        ).also { it.start() }

        try {
            alicePipeline.send("hello")
            assertEquals(true, latch.await(3, TimeUnit.SECONDS))
            assertEquals("hello", received[0])
        } finally {
            alicePipeline.stop(); bobPipeline.stop()
            alice.close(); bob.close()
        }
    }
}
