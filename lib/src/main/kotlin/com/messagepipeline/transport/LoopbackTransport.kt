package com.messagepipeline.transport

import com.messagepipeline.Frame
import com.messagepipeline.Transport
import java.util.concurrent.atomic.AtomicReference

/**
 * 内存 loopback transport：发出去的帧直接进入接收回调。
 * 用于：
 * - 单元测试
 * - 双进程同机演示（拿两个 LoopbackTransport 然后用 [pair] 串起来）
 *
 * @param mtu 模拟通道的 MTU 限制
 */
class LoopbackTransport(override val mtu: Int = 64) : Transport {

    private val listener = AtomicReference<((Frame) -> Unit)?>(null)
    private var peer: LoopbackTransport? = null
    @Volatile private var closed = false

    override fun send(frame: Frame) {
        check(!closed) { "transport closed" }
        val target = peer ?: this   // 没配对就回环到自己
        target.listener.get()?.invoke(frame)
    }

    override fun onReceive(listener: (Frame) -> Unit) {
        this.listener.set(listener)
    }

    override fun close() {
        closed = true
        listener.set(null)
    }

    companion object {
        /** 创建一对互通的 transport：a.send → b 收，b.send → a 收。 */
        fun pair(mtu: Int = 64): Pair<LoopbackTransport, LoopbackTransport> {
            val a = LoopbackTransport(mtu)
            val b = LoopbackTransport(mtu)
            a.peer = b
            b.peer = a
            return a to b
        }
    }
}
