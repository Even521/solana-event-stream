use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::common::AnyResult;
use crate::protos::shredstream::shredstream_proxy_client::ShredstreamProxyClient;
use crate::streaming::common::{
    MetricsManager, PerformanceMetrics, StreamClientConfig, SubscriptionHandle,
};

/// ShredStream gRPC 客户端封装
///
/// 用于与 ShredStream 服务进行 gRPC 通信，支持事件订阅、背压控制和性能监控。
#[derive(Clone)]
pub struct ShredStreamGrpc {
    /// gRPC 客户端实例
    ///
    /// 封装了 tonic 的 gRPC 客户端，用于与 ShredStream 服务交互。
    pub shredstream_client: Arc<ShredstreamProxyClient<Channel>>,

    /// 客户端配置
    ///
    /// 包含连接超时、请求超时、背压策略和是否启用性能监控等配置。
    pub config: StreamClientConfig,

    /// 性能指标存储
    ///
    /// 使用 RwLock 包裹的共享性能指标对象，用于统计和查询处理性能。
    pub metrics: Arc<RwLock<PerformanceMetrics>>,

    /// 性能指标管理器
    ///
    /// 提供启动自动监控、打印指标等功能。
    pub metrics_manager: MetricsManager,

    /// 当前的订阅句柄
    ///
    /// 使用 Arc<Mutex<>> 包裹，支持异步安全更新。
    /// 包含异步任务句柄以及性能监控任务句柄。
    pub subscription_handle: Arc<Mutex<Option<SubscriptionHandle>>>,
}

impl ShredStreamGrpc {
    /// 创建客户端，使用默认配置
    pub async fn new(endpoint: String) -> AnyResult<Self> {
        Self::new_with_config(endpoint, StreamClientConfig::default()).await
    }

    /// 使用自定义配置创建 ShredStream 客户端实例
    ///
    /// # 参数
    /// - `endpoint`: gRPC 服务端地址，例如 "http://127.0.0.1:9999"
    /// - `config`: 客户端配置，包含连接参数、背压策略、性能监控开关等
    ///
    /// # 返回值
    /// - `AnyResult<Self>`：成功返回 `ShredStreamGrpc` 实例，失败返回错误
    ///
    /// # 功能说明
    /// 1️⃣ 连接 ShredStream gRPC 服务：
    ///    - 使用 `ShredstreamProxyClient::connect` 建立异步 gRPC 连接
    ///
    /// 2️⃣ 初始化性能监控对象：
    ///    - `PerformanceMetrics` 用于统计延迟、吞吐量等指标
    ///    - `metrics_manager` 包装了是否启用监控的逻辑
    ///
    /// 3️⃣ 构建客户端实例：
    ///    - 使用 `Arc` 包装 `shredstream_client`，支持多线程共享
    ///    - `subscription_handle` 使用 `Mutex<Option<_>>`，后续用于管理订阅生命周期
    ///
    /// # 异步特性
    /// - 使用 `async` / `.await`，在连接建立时不会阻塞 tokio 运行时
    pub async fn new_with_config(
        endpoint: String,
        config: StreamClientConfig,
    ) -> AnyResult<Self> {
        // 建立 gRPC 客户端连接
        let shredstream_client = ShredstreamProxyClient::connect(endpoint.clone()).await?;

        // 创建性能指标对象
        let metrics = Arc::new(RwLock::new(PerformanceMetrics::new()));

        // 创建性能监控管理器
        let metrics_manager = MetricsManager::new(config.enable_metrics, "ShredStream".to_string());

        // 返回封装好的客户端实例
        Ok(Self {
            shredstream_client: Arc::new(shredstream_client),
            config,
            metrics: metrics.clone(),
            metrics_manager,
            subscription_handle: Arc::new(Mutex::new(None)),
        })
    }

    /// Creates a new ShredStreamClient with high-throughput configuration.
    ///
    /// This is a convenience method that creates a client optimized for high-concurrency scenarios
    /// where throughput is prioritized over latency. See `StreamClientConfig::high_throughput()`
    /// for detailed configuration information.
    pub async fn new_high_throughput(endpoint: String) -> AnyResult<Self> {
        Self::new_with_config(endpoint, StreamClientConfig::high_throughput()).await
    }

    /// Creates a new ShredStreamClient with low-latency configuration.
    ///
    /// This is a convenience method that creates a client optimized for real-time scenarios
    /// where latency is prioritized over throughput. See `StreamClientConfig::low_latency()`
    /// for detailed configuration information.
    pub async fn new_low_latency(endpoint: String) -> AnyResult<Self> {
        Self::new_with_config(endpoint, StreamClientConfig::low_latency()).await
    }


    /// 获取当前配置
    pub fn get_config(&self) -> &StreamClientConfig {
        &self.config
    }

    /// 更新配置
    pub fn update_config(&mut self, config: StreamClientConfig) {
        self.config = config;
    }

    /// 获取性能指标
    pub fn get_metrics(&self) -> PerformanceMetrics {
        self.metrics_manager.get_metrics()
    }

    /// 启用或禁用性能监控
    pub fn set_enable_metrics(&mut self, enabled: bool) {
        self.config.enable_metrics = enabled;
    }

    /// 打印性能指标
    pub fn print_metrics(&self) {
        self.metrics_manager.print_metrics();
    }

    /// 启动自动性能监控任务
    pub async fn start_auto_metrics_monitoring(&self) {
        self.metrics_manager.start_auto_monitoring().await;
    }

    /// 停止当前订阅
    pub async fn stop(&self) {
        let mut handle_guard = self.subscription_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            handle.stop();
        }
    }
}
