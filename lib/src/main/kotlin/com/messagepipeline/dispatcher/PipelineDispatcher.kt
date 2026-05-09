package com.messagepipeline.dispatcher

import com.messagepipeline.Chunker
import com.messagepipeline.Codec
import com.messagepipeline.Frame
import com.messagepipeline.Transport
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 双线程消息分发：
 *
 * - **Reader 线程** 持续从 transport 拿帧 → 经 chunker.assemble 累积 → 收齐后 codec.decode
 *   → 把整条消息塞入 incomingQueue
 * - **Dispatcher 线程** 从 incomingQueue 取消息 → 调用 [onMessage] 回调
 *
 * 这样 IO（拿帧 + 校验完整性）和业务（解码 + 处理）解耦，IO 线程不会被慢业务阻塞。
 *
 * 关闭：调用 [stop] 优雅停止两条线程。
 */
class PipelineDispatcher<T>(
    private val codec: Codec<T>,
    private val chunker: Chunker,
    private val transport: Transport,
    private val onMessage: (T) -> Unit,
) {

    private val incomingFrames: BlockingQueue<Frame> = LinkedBlockingQueue()
    private val running = AtomicBoolean(false)
    private var readerThread: Thread? = null
    private var dispatcherThread: Thread? = null

    fun start() {
        check(running.compareAndSet(false, true)) { "already started" }

        // transport 收到的帧放入队列
        transport.onReceive { frame -> incomingFrames.offer(frame) }

        readerThread = Thread({ readerLoop() }, "MessagePipeline-Reader").also { it.start() }
        dispatcherThread = Thread({ dispatcherLoop() }, "MessagePipeline-Dispatcher").also { it.start() }
    }

    fun stop() {
        if (!running.compareAndSet(true, false)) return
        readerThread?.interrupt()
        dispatcherThread?.interrupt()
    }

    /** 调用方主动发送一条消息。 */
    fun send(message: T) {
        val bytes = codec.encode(message)
        for (frame in chunker.split(bytes, transport.mtu)) {
            transport.send(frame)
        }
    }

    private val outgoingMessages: BlockingQueue<T> = LinkedBlockingQueue()

    private fun readerLoop() {
        while (running.get()) {
            try {
                val frame = incomingFrames.take()
                val assembled = chunker.assemble(frame) ?: continue   // 还没收齐
                val message = codec.decode(assembled)
                outgoingMessages.offer(message)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt(); return
            } catch (t: Throwable) {
                // TODO: 暴露错误回调；当前简化实现忽略
            }
        }
    }

    private fun dispatcherLoop() {
        while (running.get()) {
            try {
                val message = outgoingMessages.take()
                onMessage(message)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt(); return
            }
        }
    }
}
