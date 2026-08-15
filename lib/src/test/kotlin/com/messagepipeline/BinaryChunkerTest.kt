package com.messagepipeline

import com.messagepipeline.chunker.BinaryChunker
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.random.Random

class BinaryChunkerTest {

    @Test fun `roundtrips large payload at BLE minimum with reliable overhead`() {
        val chunker = BinaryChunker(maxMessageBytes = 10_000)
        val data = Random(42).nextBytes(5_000)
        val frames = chunker.split(data, mtu = 12).toList()
        var result: ByteArray? = null

        for (frame in frames.shuffled(Random(7))) {
            chunker.assemble(frame)?.let { result = it }
        }

        assertTrue(frames.all { it.bytes.size <= 12 })
        assertArrayEquals(data, result)
    }

    @Test fun `empty binary payload roundtrips`() {
        val chunker = BinaryChunker()
        val frames = chunker.split(ByteArray(0), mtu = BinaryChunker.HEADER_SIZE).toList()

        assertEquals(1, frames.size)
        assertArrayEquals(ByteArray(0), chunker.assemble(frames.single()))
    }

    @Test fun `duplicate frame does not duplicate output`() {
        val chunker = BinaryChunker()
        val data = Random(1).nextBytes(100)
        val frames = chunker.split(data, mtu = 20).toList()
        chunker.assemble(frames.first())
        chunker.assemble(frames.first())
        var result: ByteArray? = null
        for (frame in frames.drop(1)) chunker.assemble(frame)?.let { result = it }

        assertArrayEquals(data, result)
    }
}
