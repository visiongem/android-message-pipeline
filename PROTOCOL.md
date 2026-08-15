# Wire protocols

本文档供 Android、MCU 和桌面端实现互操作。除 `DefaultChunker` 外，所有多字节整数均使用网络
字节序（big-endian）。

## 协议分层

典型的可靠字节通道按以下顺序封装：

```text
业务消息 → Codec → BinaryChunker → ReliableTransport → BLE/USB Transport
```

每一层的 `mtu` 都表示它能从上一层接收的最大 `Frame.bytes` 长度。可靠层会从底层 MTU 中扣除
8 字节；BLE Transport 返回 ATT MTU 减 3；USB Transport 的逻辑 MTU 默认是 16,380 字节。

## DefaultChunker v1

完整帧是 ASCII：

```text
[index:total:hash:group] payload_in_lowercase_hex
```

- `index` 从 0 开始。
- `total` 是完整消息的帧数。
- `hash` 是完整消息 MD5 的前 8 个小写 hex 字符，只作完整性指纹。
- `group` 是非负十进制消息组 id。
- 空消息有一帧，payload 为空。

## BinaryChunker v1

头长度固定为 11 字节：

| Offset | Size | 字段 | 说明 |
|---:|---:|---|---|
| 0 | 1 | magic/version | 固定 `0x51` |
| 1 | 2 | group | unsigned 16-bit |
| 3 | 2 | index | unsigned 16-bit，从 0 开始 |
| 5 | 2 | total | unsigned 16-bit，范围 1～65,535 |
| 7 | 4 | CRC32 | 完整消息 CRC32，保留原始 32-bit 位模式 |
| 11 | N | payload | 原始字节，不编码 |

接收端按 `(group, total, CRC32)` 识别一组消息，按 index 拼接并校验完整消息 CRC32。重复 index
只有在 payload 完全相同时才可接受。

## ReliableTransport v1

可靠层头长度固定为 8 字节：

| Offset | Size | 字段 | 说明 |
|---:|---:|---|---|
| 0 | 1 | magic | 固定 `0x6d` |
| 1 | 1 | version/type | DATA=`0x10`，ACK=`0x11` |
| 2 | 2 | session | unsigned 16-bit，连接实例随机生成 |
| 4 | 4 | sequence | 32-bit 序号，按位回传 |
| 8 | N | payload | DATA 为上层 Frame；ACK 必须为空 |

接收 DATA 后，以完全相同的 session 和 sequence 回复 ACK。接收端应在去重窗口内记住
`(session, sequence)`：重复 DATA 需要再次回复 ACK，但不能再次向上层投递。

发送端在 ACK 超时后重发完全相同的 DATA。默认配置为 500ms 超时、最多 3 次重试，并在重试
之间指数退避。ACK 只表示对端可靠层已接收 Frame，不表示业务处理成功。

## UsbBulkTransport v1

USB Bulk 本身按字节流处理。每个可靠层或 chunker Frame 在线路上增加 4 字节 unsigned 长度前缀：

| Offset | Size | 字段 |
|---:|---:|---|
| 0 | 4 | 非负 signed 32-bit payload 长度，大端序；不得超过双方约定的 maxFrameSize |
| 4 | N | Frame payload |

一次 Bulk read 可能只包含部分头、部分 payload，或同时包含多个 Frame。接收实现不能依赖 USB
transfer 边界。Android 9 以前单次 `bulkTransfer` 不应超过 16,384 字节。
