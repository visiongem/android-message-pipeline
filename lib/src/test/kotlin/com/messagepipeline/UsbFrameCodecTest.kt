package com.messagepipeline

import com.messagepipeline.transport.usb.LengthPrefixedFrameDecoder
import com.messagepipeline.transport.usb.UsbTransportException
import com.messagepipeline.transport.usb.encodeLengthPrefixedFrame
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import java.nio.ByteBuffer

class UsbFrameCodecTest {

    @Test fun `decodes header and payload split across arbitrary reads`() {
        val expected = Frame("fragmented usb frame".toByteArray())
        val packet = encodeLengthPrefixedFrame(expected)
        val decoder = LengthPrefixedFrameDecoder(maxFrameSize = 100)
        val decoded = mutableListOf<Frame>()

        for (byte in packet) decoded += decoder.accept(byteArrayOf(byte))

        assertEquals(1, decoded.size)
        assertArrayEquals(expected.bytes, decoded.single().bytes)
    }

    @Test fun `decodes multiple frames from one usb read`() {
        val first = Frame(byteArrayOf(1, 2))
        val second = Frame(byteArrayOf(3, 4, 5))
        val packet = encodeLengthPrefixedFrame(first) + encodeLengthPrefixedFrame(second)

        val decoded = LengthPrefixedFrameDecoder(100).accept(packet)

        assertEquals(listOf(first, second), decoded)
    }

    @Test fun `supports empty frame`() {
        val decoded = LengthPrefixedFrameDecoder(100)
            .accept(encodeLengthPrefixedFrame(Frame(ByteArray(0))))

        assertEquals(1, decoded.size)
        assertArrayEquals(ByteArray(0), decoded.single().bytes)
    }

    @Test fun `rejects oversized and negative lengths`() {
        val decoder = LengthPrefixedFrameDecoder(maxFrameSize = 10)
        assertThrows(UsbTransportException::class.java) {
            decoder.accept(ByteBuffer.allocate(4).putInt(11).array())
        }
        assertThrows(UsbTransportException::class.java) {
            decoder.accept(ByteBuffer.allocate(4).putInt(-1).array())
        }
    }
}
