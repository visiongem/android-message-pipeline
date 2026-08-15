package com.messagepipeline

import com.messagepipeline.chunker.DefaultChunker
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.random.Random

class DefaultChunkerTest {

    @Test fun `roundtrip small payload single frame`() {
        val chunker = DefaultChunker()
        val data = "hello".toByteArray()
        val frames = chunker.split(data, mtu = 200).toList()
        assertTrue("应只切成 1 帧", frames.size == 1)
        assertArrayEquals(data, chunker.assemble(frames[0]))
    }

    @Test fun `every encoded frame respects mtu after hex expansion`() {
        val chunker = DefaultChunker()
        val data = Random(7).nextBytes(2048)

        for (mtu in listOf(48, 64, 80, 185)) {
            val frames = chunker.split(data, mtu).toList()
            assertTrue("mtu=$mtu", frames.all { it.bytes.size <= mtu })
        }
    }

    @Test fun `empty payload produces one frame and roundtrips`() {
        val chunker = DefaultChunker()
        val frames = chunker.split(ByteArray(0), mtu = 64).toList()

        assertEquals(1, frames.size)
        assertArrayEquals(ByteArray(0), chunker.assemble(frames.single()))
    }

    @Test fun `roundtrip multi frame in order`() {
        val chunker = DefaultChunker()
        val data = Random(0xC0FFEE).nextBytes(1024)
        val frames = chunker.split(data, mtu = 64).toList()
        assertTrue("应切多帧", frames.size > 1)

        var assembled: ByteArray? = null
        for (frame in frames) {
            assembled = chunker.assemble(frame)
        }
        assertArrayEquals(data, assembled)
    }

    @Test fun `roundtrip multi frame out of order`() {
        val chunker = DefaultChunker()
        val data = Random(0xBEEF).nextBytes(2000)
        val frames = chunker.split(data, mtu = 64).toList().shuffled(Random(42))

        var assembled: ByteArray? = null
        for (frame in frames) {
            val r = chunker.assemble(frame)
            if (r != null) assembled = r
        }
        assertArrayEquals(data, assembled)
    }

    @Test fun `assemble returns null until last frame`() {
        val chunker = DefaultChunker()
        val data = Random(1).nextBytes(500)
        val frames = chunker.split(data, mtu = 64).toList()
        for (i in 0 until frames.lastIndex) {
            assertNull("第 $i 帧不该返回结果", chunker.assemble(frames[i]))
        }
        assertNotNull("最后一帧应该返回完整数据", chunker.assemble(frames.last()))
    }

    @Test fun `concurrent groups isolated`() {
        val chunker = DefaultChunker()
        val a = Random(11).nextBytes(800)
        val b = Random(22).nextBytes(600)
        val framesA = chunker.split(a, mtu = 64).toList()
        val framesB = chunker.split(b, mtu = 64).toList()

        // 交错喂入
        val interleaved = mutableListOf<Frame>()
        var i = 0; var j = 0
        while (i < framesA.size || j < framesB.size) {
            if (i < framesA.size) interleaved.add(framesA[i++])
            if (j < framesB.size) interleaved.add(framesB[j++])
        }

        val results = mutableListOf<ByteArray>()
        for (f in interleaved) {
            chunker.assemble(f)?.let { results.add(it) }
        }

        assertTrue("应组装出 2 个完整消息", results.size == 2)
        // 排序后比对（哪个先到不一定）
        val expected = listOf(a, b).sortedBy { it.contentHashCode() }
        val got = results.sortedBy { it.contentHashCode() }
        assertArrayEquals(expected[0], got[0])
        assertArrayEquals(expected[1], got[1])
    }

    @Test fun `expired incomplete group is discarded`() {
        var now = 0L
        val chunker = DefaultChunker(
            groupTimeoutMillis = 100,
            timeSourceMillis = { now },
        )
        val frames = chunker.split(Random(9).nextBytes(500), mtu = 64).toList()
        assertNull(chunker.assemble(frames.first()))

        now = 100
        assertEquals(1, chunker.cleanupExpired())
        for (frame in frames.drop(1)) assertNull(chunker.assemble(frame))
    }

    @Test fun `conflicting duplicate invalidates group`() {
        val chunker = DefaultChunker()
        val frames = chunker.split(Random(3).nextBytes(200), mtu = 64).toList()
        assertNull(chunker.assemble(frames.first()))

        val corrupted = frames.first().bytes.copyOf().also { bytes ->
            bytes[bytes.lastIndex] = if (bytes.last() == '0'.code.toByte()) '1'.code.toByte() else '0'.code.toByte()
        }
        assertNull(chunker.assemble(Frame(corrupted)))
        for (frame in frames.drop(1)) assertNull(chunker.assemble(frame))
    }

    @Test fun `rejects malformed metadata and trailing data`() {
        val chunker = DefaultChunker()
        assertNull(chunker.assemble(Frame("[1:1:d41d8cd9:0] ".toByteArray())))
        assertNull(chunker.assemble(Frame("[0:0:d41d8cd9:0] ".toByteArray())))
        assertNull(chunker.assemble(Frame("junk[0:1:d41d8cd9:0] ".toByteArray())))
        assertNull(chunker.assemble(Frame("[0:1:d41d8cd9:0] 00!".toByteArray())))
    }
}
