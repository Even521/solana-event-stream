// ===============================
// 流处理相关的常量定义
// ===============================

// ---------- 默认配置常量 ----------

/// 默认连接超时时间（秒）
/// 控制客户端与服务端建立连接的最大等待时间
pub const DEFAULT_CONNECT_TIMEOUT: u64 = 10;

/// 默认请求超时时间（秒）
/// 控制单个 gRPC 请求的最大等待时间
pub const DEFAULT_REQUEST_TIMEOUT: u64 = 60;

/// 默认通道（队列）大小
/// 表示缓冲区可同时容纳的最大消息数
pub const DEFAULT_CHANNEL_SIZE: usize = 1000;

/// 默认最大解码消息大小（字节）
/// 防止过大消息导致 OOM，默认 10MB
pub const DEFAULT_MAX_DECODING_MESSAGE_SIZE: usize = 1024 * 1024 * 10;


// ---------- 性能监控相关常量 ----------

/// 性能监控窗口（秒）
/// 表示统计性能指标时的滑动窗口大小
pub const DEFAULT_METRICS_WINDOW_SECONDS: u64 = 5;

/// 性能监控打印间隔（秒）
/// 控制指标日志打印的频率
pub const DEFAULT_METRICS_PRINT_INTERVAL_SECONDS: u64 = 10;

/// 慢处理阈值（微秒）
/// 超过该处理耗时将被标记为“慢请求”
pub const SLOW_PROCESSING_THRESHOLD_US: f64 = 3000.0;