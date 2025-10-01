use super::constants::*;

/// 流量控制策略（Backpressure Handling Strategy）
///
/// 用于在消息处理速度跟不上生产速度时，决定如何处理过载情况。
#[derive(Debug, Clone, Copy)]
pub enum BackpressureStrategy {
    /// 阻塞并等待（默认策略）
    ///
    /// 当消息队列已满时，生产者会被阻塞，直到有空间可用。
    /// 优点：保证消息不丢失。
    /// 缺点：可能导致生产者等待，降低吞吐量。
    Block,

    /// 丢弃消息
    ///
    /// 当消息队列已满时，直接丢弃新到来的消息。
    /// 优点：不会阻塞生产者，保证系统流畅。
    /// 缺点：可能丢失数据，不保证消息完整性。
    Drop,
}

/// 为 `BackpressureStrategy` 实现默认值
impl Default for BackpressureStrategy {
    /// 默认策略为 `Block`
    ///
    /// 也就是说，如果没有显式指定流量控制策略，
    /// 系统将选择阻塞等待（Block）方式处理过载消息。
    fn default() -> Self {
        Self::Block
    }
}

/// 流量控制配置（Backpressure Configuration）
///
/// 用于配置消息队列的容量和超载时的处理策略。
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// 队列容量（Channel size），默认值：1000
    ///
    /// 表示允许同时存在的消息数量上限。
    /// 超过此数量时，将根据 `strategy` 决定如何处理。
    pub permits: usize,

    /// 流量控制策略（Backpressure handling strategy），默认值：Block
    ///
    /// 指定当队列已满时的处理方式：
    /// - `Block` 阻塞并等待队列有空位
    /// - `Drop` 丢弃新到来的消息
    pub strategy: BackpressureStrategy,
}
/// 为 `BackpressureConfig` 实现默认值
impl Default for BackpressureConfig {
    /// 默认配置：
    /// - 队列容量（permits）: 3000
    /// - 流量控制策略（strategy）: 默认 Block（阻塞等待）
    fn default() -> Self {
        Self {
            permits: 3000,
            strategy: BackpressureStrategy::default()
        }
    }
}

/// 连接配置（Connection Configuration）
///
/// 用于配置 gRPC 或消息流连接的超时与消息大小限制。
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// 连接超时时间（秒），默认值：10
    ///
    /// 如果在指定时间内无法建立连接，将返回超时错误。
    pub connect_timeout: u64,

    /// 请求超时时间（秒），默认值：60
    ///
    /// 单次请求的最大等待时间，超过此时间未收到响应将超时。
    pub request_timeout: u64,

    /// 最大解码消息大小（字节），默认值：10MB
    ///
    /// 用于限制接收消息的大小，避免内存占用过高。
    pub max_decoding_message_size: usize,
}

/// 为 `ConnectionConfig` 提供默认值
impl Default for ConnectionConfig {
    /// 默认配置：
    /// - 连接超时（connect_timeout）：DEFAULT_CONNECT_TIMEOUT（默认 10 秒）
    /// - 请求超时（request_timeout）：DEFAULT_REQUEST_TIMEOUT（默认 60 秒）
    /// - 最大解码消息大小（max_decoding_message_size）：DEFAULT_MAX_DECODING_MESSAGE_SIZE（默认 10MB）
    fn default() -> Self {
        Self {
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_decoding_message_size: DEFAULT_MAX_DECODING_MESSAGE_SIZE,
        }
    }
}

/// 通用客户端配置（Stream Client Configuration）
///
/// 用于配置消息流客户端的连接、流量控制及性能监控。
#[derive(Debug, Clone)]
pub struct StreamClientConfig {
    /// 连接配置
    ///
    /// 包含连接超时、请求超时、最大解码消息大小等。
    pub connection: ConnectionConfig,

    /// 流量控制配置
    ///
    /// 包含队列容量和超载处理策略（Block / Drop）。
    pub backpressure: BackpressureConfig,

    /// 是否启用性能监控（默认: false）
    ///
    /// 如果为 true，将收集请求/消息的性能指标，如吞吐量、延迟等。
    pub enable_metrics: bool,
}

/// 为 `StreamClientConfig` 提供默认值
impl Default for StreamClientConfig {
    /// 默认配置：
    /// - connection: ConnectionConfig 的默认值
    ///   （连接超时 10 秒、请求超时 60 秒、最大解码消息大小 10MB）
    /// - backpressure: BackpressureConfig 的默认值
    ///   （队列容量 3000，策略 Block）
    /// - enable_metrics: false（默认不启用性能监控）
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            backpressure: BackpressureConfig::default(),
            enable_metrics: false,
        }
    }
}

impl StreamClientConfig {
    /// 创建高吞吐量配置（High-Throughput Configuration）
    ///
    /// 优化目标：高并发场景下优先保证吞吐量，而非延迟。
    ///
    /// 特点：
    /// - 流量控制策略采用 Drop，不阻塞生产者，允许丢弃部分消息
    /// - 队列容量（permits）设为 20,000，可应对突发流量
    ///
    /// 适用场景：
    /// - 处理大量数据
    /// - 可以容忍高峰期偶尔丢失消息
    pub fn high_throughput() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            backpressure: BackpressureConfig {
                permits: 20000,
                strategy: BackpressureStrategy::Drop,
            },
            enable_metrics: false,
        }
    }

    /// 创建低延迟配置（Low-Latency Configuration）
    ///
    /// 优化目标：实时场景下优先保证低延迟，不丢失任何消息。
    ///
    /// 特点：
    /// - 事件立即处理，不进行大量缓冲
    /// - 流量控制策略采用 Block，确保消息不丢失
    /// - 队列容量（permits）设为 4,000，实现吞吐量与延迟的平衡
    ///
    /// 适用场景：
    /// - 每毫秒都很关键的应用，如交易系统或实时监控
    /// - 不能容忍事件丢失
    pub fn low_latency() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            backpressure: BackpressureConfig {
                permits: 4000,
                strategy: BackpressureStrategy::Block
            },
            enable_metrics: false,
        }
    }
}
