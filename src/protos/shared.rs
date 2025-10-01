// 该文件由 `prost-build` 自动生成，请勿手动修改

/// Header 消息结构
/// 通常用于封装通用元数据，比如时间戳
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Header {
    /// ts 表示时间戳（Timestamp 类型，可选字段）
    /// 对应 Protobuf 中的 message Timestamp
    #[prost(message, optional, tag = "1")]
    pub ts: ::core::option::Option<::prost_types::Timestamp>,
}

/// Heartbeat 消息结构
/// 用于心跳检测，记录发送次数或序号
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Heartbeat {
    /// count 表示心跳计数（uint64 类型）
    #[prost(uint64, tag = "1")]
    pub count: u64,
}

/// Socket 消息结构
/// 用于表示网络套接字信息（IP + 端口）
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Socket {
    /// ip 表示 IPv4 或 IPv6 地址
    #[prost(string, tag = "1")]
    pub ip: ::prost::alloc::string::String,

    /// port 表示端口号（int64 类型）
    #[prost(int64, tag = "2")]
    pub port: i64,
}

