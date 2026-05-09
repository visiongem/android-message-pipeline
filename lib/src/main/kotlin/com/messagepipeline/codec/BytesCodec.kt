package com.messagepipeline.codec

import com.messagepipeline.Codec

/** 直通 Codec：消息本身就是字节。 */
object BytesCodec : Codec<ByteArray> {
    override fun encode(value: ByteArray): ByteArray = value
    override fun decode(bytes: ByteArray): ByteArray = bytes
}

/** UTF-8 字符串 ↔ 字节。 */
object StringCodec : Codec<String> {
    override fun encode(value: String): ByteArray = value.toByteArray(Charsets.UTF_8)
    override fun decode(bytes: ByteArray): String = String(bytes, Charsets.UTF_8)
}
