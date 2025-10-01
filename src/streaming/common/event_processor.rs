use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_queue::SegQueue;
use solana_sdk::pubkey::Pubkey;

use crate::common::AnyResult;
use crate::streaming::common::BackpressureStrategy;
use crate::streaming::common::{
    MetricsEventType, MetricsManager, StreamClientConfig as ClientConfig,
};
use crate::streaming::event_parser::common::filter::EventTypeFilter;
use crate::streaming::event_parser::core::account_event_parser::AccountEventParser;
use crate::streaming::event_parser::core::common_event_parser::CommonEventParser;

use crate::streaming::event_parser::core::event_parser::EventParser;
use crate::streaming::event_parser::{core::traits::UnifiedEvent, Protocol};
use crate::streaming::grpc::{BackpressureConfig, EventPretty};
use crate::streaming::shred::TransactionWithSlot;
use once_cell::sync::OnceCell;

pub enum EventSource {
    Grpc,
    Shred,
}

/// High-performance Event processor using SegQueue for all strategies
/// 事件处理器
///
/// 负责管理事件流的接收、解析、队列管理、背压控制以及回调分发。
/// 这是整个流式处理框架的核心调度器。
pub struct EventProcessor {
    /// 性能指标管理器
    /// 用于收集和监控吞吐量、延迟、慢事件统计等信息。
    pub(crate) metrics_manager: MetricsManager,

    /// 客户端配置
    /// 包含连接、订阅、超时等参数设置。
    pub(crate) config: ClientConfig,

    /// 事件解析器缓存
    /// 使用 `OnceCell` 延迟初始化并缓存事件解析器，
    /// 将原始数据转换为统一的事件类型 `UnifiedEvent`。
    pub(crate) parser_cache: OnceCell<Arc<EventParser>>,

    /// 支持的协议列表
    /// 保存需要处理的协议集合（如不同的交易协议/事件协议）。
    pub(crate) protocols: Vec<Protocol>,

    /// 事件类型过滤器
    /// 可选字段，用于过滤和订阅特定类型的事件，
    /// 避免处理无关数据。
    pub(crate) event_type_filter: Option<EventTypeFilter>,

    /// 回调函数
    /// 在事件被成功解析为 `UnifiedEvent` 后触发的回调逻辑。
    /// 使用 `Arc<Fn>` 保证在多线程环境下安全调用。
    pub(crate) callback: Option<Arc<dyn Fn(Box<dyn UnifiedEvent>) + Send + Sync>>,

    /// 背压配置
    /// 控制事件队列满载时的处理策略（阻塞/丢弃）及队列容量大小。
    pub(crate) backpressure_config: BackpressureConfig,

    /// gRPC 事件队列
    /// 存放从 gRPC 流接收的事件，
    /// 每个事件带有美化后的内容和可选的过滤键 (Pubkey)。
    pub(crate) grpc_queue: Arc<SegQueue<(EventPretty, Option<Pubkey>)>>,

    /// Shred 队列
    /// 存放从 Shred 数据源接收的事件，
    /// 包含交易信息和对应的 Slot。
    pub(crate) shred_queue: Arc<SegQueue<(TransactionWithSlot, Option<Pubkey>)>>,

    /// gRPC 队列挂起事件数
    /// 使用原子计数器跟踪当前待处理的 gRPC 事件数量，
    /// 用于限流和性能监控。
    pub(crate) grpc_pending_count: Arc<AtomicUsize>,

    /// Shred 队列挂起事件数
    /// 跟踪待处理的 Shred 事件数量。
    pub(crate) shred_pending_count: Arc<AtomicUsize>,

    /// 处理关闭标记
    /// 使用原子布尔值标记系统是否进入关闭状态，
    /// 用于优雅地停止事件处理。
    pub(crate) processing_shutdown: Arc<AtomicBool>,
}

impl EventProcessor {
    pub fn new(metrics_manager: MetricsManager, config: ClientConfig) -> Self {
        let backpressure_config = config.backpressure.clone();
        let grpc_queue = Arc::new(SegQueue::new());
        let shred_queue = Arc::new(SegQueue::new());
        let grpc_pending_count = Arc::new(AtomicUsize::new(0));
        let shred_pending_count = Arc::new(AtomicUsize::new(0));
        let processing_shutdown = Arc::new(AtomicBool::new(false));

        Self {
            metrics_manager,
            config,
            parser_cache: OnceCell::new(),
            protocols: vec![],
            event_type_filter: None,
            backpressure_config,
            callback: None,
            grpc_queue,
            shred_queue,
            grpc_pending_count,
            shred_pending_count,
            processing_shutdown,
        }
    }

    /// 配置事件处理器的协议列表、事件类型过滤器、背压策略和回调函数
    ///
    /// # 参数说明
    /// - `source: EventSource`
    ///     - 指定事件来源，如 ShredStream、Geyser 等
    /// - `protocols: Vec<Protocol>`
    ///     - 需要订阅的协议列表，用于筛选特定类型的事件
    /// - `event_type_filter: Option<EventTypeFilter>`
    ///     - 可选的事件类型过滤器，用于进一步筛选事件
    /// - `backpressure_config: BackpressureConfig`
    ///     - 背压（Backpressure）策略配置，用于控制事件处理的速率
    /// - `callback: Option<Arc<dyn Fn(Box<dyn UnifiedEvent>) + Send + Sync>>`
    ///     - 可选的事件回调函数，每当事件被处理时调用
    ///
    /// # 功能说明
    /// 1️⃣ 更新内部状态：
    ///    - 保存协议列表、事件类型过滤器、背压策略和回调函数
    /// 2️⃣ 初始化事件解析器（`EventParser`）缓存：
    ///    - 使用 `get_or_init` 确保只初始化一次
    ///    - 解析器会根据协议列表和事件类型过滤器处理原始事件
    /// 3️⃣ 根据背压策略决定是否启动阻塞处理线程：
    ///    - 如果 `strategy` 是 `Block`，调用 `start_block_processing_thread`
    ///    - 阻塞策略会在事件处理队列满时阻塞，保证不丢失事件
    ///
    /// # 注意
    /// - 如果背压策略是 `Drop`，则不会启动阻塞线程，事件可能会被丢弃
    pub fn set_protocols_and_event_type_filter(
        &mut self,
        source: EventSource,
        protocols: Vec<Protocol>,
        event_type_filter: Option<EventTypeFilter>,
        backpressure_config: BackpressureConfig,
        callback: Option<Arc<dyn Fn(Box<dyn UnifiedEvent>) + Send + Sync>>,
    ) {
        // 更新协议列表和事件类型过滤器
        self.protocols = protocols;
        self.event_type_filter = event_type_filter;

        // 更新背压策略和回调函数
        self.backpressure_config = backpressure_config;
        self.callback = callback;

        // 初始化事件解析器缓存（只初始化一次）
        let protocols_ref = &self.protocols;
        let event_type_filter_ref = self.event_type_filter.as_ref();
        self.parser_cache.get_or_init(|| {
            Arc::new(EventParser::new(protocols_ref.clone(), event_type_filter_ref.cloned()))
        });

        // 如果背压策略是阻塞，启动阻塞处理线程
        if matches!(self.backpressure_config.strategy, BackpressureStrategy::Block) {
            self.start_block_processing_thread(source);
        }
    }

    pub fn get_parser(&self) -> Arc<EventParser> {
        self.parser_cache.get().unwrap().clone()
    }

    fn create_adapter_callback(&self) -> Arc<dyn Fn(Box<dyn UnifiedEvent>) + Send + Sync> {
        let callback = self.callback.clone().unwrap();
        let metrics_manager = self.metrics_manager.clone();

        Arc::new(move |event: Box<dyn UnifiedEvent>| {
            let processing_time_us = event.handle_us() as f64;
            callback(event);
            metrics_manager.update_metrics(MetricsEventType::Transaction, 1, processing_time_us);
        })
    }

    pub async fn process_grpc_event_transaction_with_metrics(
        &self,
        event_pretty: EventPretty,
        bot_wallet: Option<Pubkey>,
    ) -> AnyResult<()> {
        self.apply_backpressure_control(event_pretty, bot_wallet).await
    }

    async fn apply_backpressure_control(
        &self,
        event_pretty: EventPretty,
        bot_wallet: Option<Pubkey>,
    ) -> AnyResult<()> {
        match self.backpressure_config.strategy {
            BackpressureStrategy::Block => {
                loop {
                    let current_pending = self.grpc_pending_count.load(Ordering::Relaxed);
                    if current_pending < self.backpressure_config.permits {
                        self.grpc_queue.push((event_pretty, bot_wallet));
                        self.grpc_pending_count.fetch_add(1, Ordering::Relaxed);
                        break;
                    }

                    tokio::task::yield_now().await;
                }
                Ok(())
            }
            BackpressureStrategy::Drop => {
                let current_pending = self.grpc_pending_count.load(Ordering::Relaxed);
                if current_pending >= self.backpressure_config.permits {
                    self.metrics_manager.increment_dropped_events();
                    Ok(())
                } else {
                    self.grpc_pending_count.fetch_add(1, Ordering::Relaxed);
                    let processor = self.clone();
                    tokio::spawn(async move {
                        match processor
                            .process_grpc_event_transaction(event_pretty, bot_wallet)
                            .await
                        {
                            Ok(_) => {
                                processor.grpc_pending_count.fetch_sub(1, Ordering::Relaxed);
                            }
                            Err(e) => {
                                log::error!("Error in async gRPC processing: {}", e);
                            }
                        }
                    });
                    Ok(())
                }
            }
        }
    }

    async fn process_grpc_event_transaction(
        &self,
        event_pretty: EventPretty,
        bot_wallet: Option<Pubkey>,
    ) -> AnyResult<()> {
        if self.callback.is_none() {
            return Ok(());
        }
        match event_pretty {
            EventPretty::Account(account_pretty) => {
                self.metrics_manager.add_account_process_count();
                let account_event = AccountEventParser::parse_account_event(
                    &self.protocols,
                    account_pretty,
                    self.event_type_filter.as_ref(),
                );
                if let Some(event) = account_event {
                    let processing_time_us = event.handle_us() as f64;
                    self.invoke_callback(event);
                    self.update_metrics(MetricsEventType::Account, 1, processing_time_us);
                }
            }
            EventPretty::Transaction(transaction_pretty) => {
                self.metrics_manager.add_tx_process_count();
                let slot = transaction_pretty.slot;
                let signature = transaction_pretty.signature;
                let block_time = transaction_pretty.block_time;
                let recv_us = transaction_pretty.recv_us;
                let transaction_index = transaction_pretty.transaction_index;
                let grpc_tx = transaction_pretty.grpc_tx;

                let parser = self.get_parser();
                let adapter_callback = self.create_adapter_callback();
                parser
                    .parse_grpc_transaction_owned(
                        grpc_tx,
                        signature,
                        Some(slot),
                        block_time,
                        recv_us,
                        bot_wallet,
                        transaction_index,
                        adapter_callback,
                    )
                    .await?;
            }
            EventPretty::BlockMeta(block_meta_pretty) => {
                self.metrics_manager.add_block_meta_process_count();
                let block_time_ms = block_meta_pretty
                    .block_time
                    .map(|ts| ts.seconds * 1000 + ts.nanos as i64 / 1_000_000)
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                let block_meta_event = CommonEventParser::generate_block_meta_event(
                    block_meta_pretty.slot,
                    block_meta_pretty.block_hash,
                    block_time_ms,
                    block_meta_pretty.recv_us,
                );
                let processing_time_us = block_meta_event.handle_us() as f64;
                self.invoke_callback(block_meta_event);
                self.update_metrics(MetricsEventType::BlockMeta, 1, processing_time_us);
            }
        }

        Ok(())
    }

    pub fn invoke_callback(&self, event: Box<dyn UnifiedEvent>) {
        if let Some(callback) = self.callback.as_ref() {
            callback(event);
        }
    }

    pub async fn process_shred_transaction_immediate(
        &self,
        transaction_with_slot: TransactionWithSlot,
        bot_wallet: Option<Pubkey>,
    ) -> AnyResult<()> {
        self.process_shred_transaction(transaction_with_slot, bot_wallet).await
    }

    /// 处理单条 ShredStream 交易并应用性能监控
    ///
    /// # 参数
    /// - `transaction_with_slot`: 封装了原始交易及其 slot 信息的对象
    /// - `bot_wallet`: 可选参数，用于指定需要关注的机器人钱包地址
    ///
    /// # 返回值
    /// - `AnyResult<()>`：成功返回 `Ok(())`，失败返回错误
    ///
    /// # 功能说明
    /// 1. 将交易交给 `apply_shred_backpressure_control` 方法处理
    /// 2. 在处理过程中会应用背压策略，防止高并发下队列阻塞或溢出
    /// 3. 可以结合内部性能监控统计处理延迟和吞吐量
    pub async fn process_shred_transaction_with_metrics(
        &self,
        transaction_with_slot: TransactionWithSlot,
        bot_wallet: Option<Pubkey>,
    ) -> AnyResult<()> {
        // 异步调用内部方法进行背压控制和交易处理
        self.apply_shred_backpressure_control(transaction_with_slot, bot_wallet).await
    }

    /// 根据背压策略处理单条 ShredStream 交易
    ///
    /// # 参数
    /// - `transaction_with_slot`: 封装了原始交易及 slot 的对象
    /// - `bot_wallet`: 可选的机器人钱包地址，用于交易筛选或标记
    ///
    /// # 返回值
    /// - `AnyResult<()>`：成功返回 `Ok(())`，失败返回错误
    ///
    /// # 功能说明
    /// 该方法根据 `backpressure_config.strategy` 选择不同处理逻辑：
    ///
    /// 1️⃣ **Block 模式**
    ///    - 如果当前队列 `shred_pending_count` 超过 `permits` 限制，则循环等待（yield）
    ///    - 一旦队列有空间，将交易推入 `shred_queue` 并增加计数器
    ///    - 保证所有交易都会被处理，不会丢失
    ///
    /// 2️⃣ **Drop 模式**
    ///    - 如果当前队列 `shred_pending_count` 超过 `permits` 限制，则直接丢弃交易，并在 `metrics_manager` 中记录
    ///    - 否则异步启动一个任务处理交易，处理完成后减少计数器
    ///    - 提高吞吐量，允许丢弃部分交易以避免阻塞
    ///
    /// # 异步处理
    /// - 使用 `tokio::spawn` 在 Drop 模式下异步执行交易处理，避免阻塞主任务
    /// - 使用 `tokio::task::yield_now().await` 在 Block 模式下释放当前任务，让出调度权
    ///
    /// # 性能与安全
    /// - 计数器 `shred_pending_count` 使用 `Relaxed` 内存序保证性能
    /// - `shred_queue` 采用多线程安全队列 `SegQueue` 存放待处理交易
    async fn apply_shred_backpressure_control(
        &self,
        transaction_with_slot: TransactionWithSlot,
        bot_wallet: Option<Pubkey>,
    ) -> AnyResult<()> {
        match self.backpressure_config.strategy {
            BackpressureStrategy::Block => {
                // 阻塞模式：队列满时等待
                loop {
                    let current_pending = self.shred_pending_count.load(Ordering::Relaxed);
                    if current_pending < self.backpressure_config.permits {
                        self.shred_queue.push((transaction_with_slot, bot_wallet));
                        self.shred_pending_count.fetch_add(1, Ordering::Relaxed);
                        break;
                    }

                    tokio::task::yield_now().await;
                }
                Ok(())
            }
            BackpressureStrategy::Drop => {
                // 丢弃模式：队列满时丢弃交易
                let current_pending = self.shred_pending_count.load(Ordering::Relaxed);
                if current_pending >= self.backpressure_config.permits {
                    self.metrics_manager.increment_dropped_events();
                    Ok(())
                } else {
                    self.shred_pending_count.fetch_add(1, Ordering::Relaxed);
                    let processor = self.clone();
                    // 异步处理交易
                    tokio::spawn(async move {
                        match processor
                            .process_shred_transaction(transaction_with_slot, bot_wallet)
                            .await
                        {
                            Ok(_) => {
                                processor.shred_pending_count.fetch_sub(1, Ordering::Relaxed);
                            }
                            Err(e) => {
                                log::error!("Error in async shred processing: {}", e);
                            }
                        }
                    });
                    Ok(())
                }
            }
        }
    }

    /// 处理单条带 Slot 信息的交易（TransactionWithSlot）
    ///
    /// 该方法负责将接收到的交易交给事件解析器（EventParser）解析， — 并调用用户注册的回调进行处理，同时更新性能指标。
    ///
    /// # 参数
    /// - `transaction_with_slot`: 包含交易（VersionedTransaction）、slot 和接收时间等信息
    /// - `bot_wallet`: 可选的 Bot 钱包地址，用于过滤或标记特定交易
    ///
    /// # 返回值
    /// - `AnyResult<()>`：处理成功返回 Ok(()), 出现解析错误时返回 Err
    ///
    pub async fn process_shred_transaction(
        &self,
        transaction_with_slot: TransactionWithSlot,
        bot_wallet: Option<Pubkey>,
    ) -> AnyResult<()> {
        // 如果没有注册回调，则直接返回
        if self.callback.is_none() {
            return Ok(());
        }

        // 增加已处理交易计数
        self.metrics_manager.add_tx_process_count();

        let tx = transaction_with_slot.transaction;
        let slot = transaction_with_slot.slot;

        // 如果交易没有签名，则跳过
        if tx.signatures.is_empty() {
            return Ok(());
        }

        // 获取交易签名（第一个签名）
        let signature = tx.signatures[0];

        // 获取交易接收时间（微秒）
        let recv_us = transaction_with_slot.recv_us;

        // 获取事件解析器实例
        let parser = self.get_parser();

        // 创建适配回调，将解析出的事件传给用户注册的回调
        let adapter_callback = self.create_adapter_callback();

        // 调用解析器解析 VersionedTransaction
        // 传入交易、签名、slot、接收时间、bot_wallet 等信息
        parser
            .parse_versioned_transaction_owned(
                tx,
                signature,
                Some(slot),
                None,
                recv_us,
                bot_wallet,
                None,
                &[],
                adapter_callback,
            )
            .await?;

        Ok(())
    }


    fn update_metrics(&self, ty: MetricsEventType, count: u64, time_us: f64) {
        self.metrics_manager.update_metrics(ty, count, time_us);
    }

    fn start_block_processing_thread(&self, source: EventSource) {
        self.processing_shutdown.store(false, Ordering::Relaxed);

        let grpc_queue = Arc::clone(&self.grpc_queue);
        let shred_queue = Arc::clone(&self.shred_queue);
        let grpc_pending_count = Arc::clone(&self.grpc_pending_count);
        let shred_pending_count = Arc::clone(&self.shred_pending_count);
        let shutdown_flag = Arc::clone(&self.processing_shutdown);
        let shutdown_flag_clone = Arc::clone(&self.processing_shutdown);
        let processor = self.clone();
        let processor_clone = self.clone();
        // Dedicated thread with busy-wait and lock-free processing
        match source {
            EventSource::Grpc => {
                std::thread::spawn(move || {
                    let mut worker_threads =
                        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4); // 如果获取失败则回退到4个线程

                    let rt = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(worker_threads)
                        .enable_all()
                        .build()
                        .unwrap();

                    while !shutdown_flag.load(Ordering::Relaxed) {
                        if let Some((event_pretty, bot_wallet)) = grpc_queue.pop() {
                            grpc_pending_count.fetch_sub(1, Ordering::Relaxed);
                            if let Err(e) = rt.block_on(
                                processor.process_grpc_event_transaction(event_pretty, bot_wallet),
                            ) {
                                println!("Error processing gRPC event: {}", e);
                            }
                        } else {
                            // 待测试替换方案： lock-free queue + spin + batch
                            std::thread::sleep(std::time::Duration::from_micros(500));
                        }
                    }
                });
            }
            EventSource::Shred => {
                // Shred processing with same low-latency optimization
                std::thread::spawn(move || {
                    let worker_threads =
                        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4); // 如果获取失败则回退到4个线程

                    let rt = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(worker_threads)
                        .enable_all()
                        .build()
                        .unwrap();

                    while !shutdown_flag_clone.load(Ordering::Relaxed) {
                        if let Some((transaction_with_slot, bot_wallet)) = shred_queue.pop() {
                            shred_pending_count.fetch_sub(1, Ordering::Relaxed);
                            if let Err(e) = rt.block_on(
                                processor_clone
                                    .process_shred_transaction(transaction_with_slot, bot_wallet),
                            ) {
                                log::error!("Error processing shred transaction: {}", e);
                            }
                        } else {
                            // 待测试替换方案： lock-free queue + spin + batch
                            std::thread::sleep(std::time::Duration::from_micros(500));
                        }
                    }
                });
            }
        }
    }

    pub fn stop_processing(&self) {
        self.processing_shutdown.store(true, Ordering::Relaxed);
    }
}

impl Clone for EventProcessor {
    fn clone(&self) -> Self {
        Self {
            metrics_manager: self.metrics_manager.clone(),
            config: self.config.clone(),
            parser_cache: self.parser_cache.clone(),
            protocols: self.protocols.clone(),
            event_type_filter: self.event_type_filter.clone(),
            backpressure_config: self.backpressure_config.clone(),
            callback: self.callback.clone(),
            grpc_queue: self.grpc_queue.clone(),
            shred_queue: self.shred_queue.clone(),
            grpc_pending_count: self.grpc_pending_count.clone(),
            shred_pending_count: self.shred_pending_count.clone(),
            processing_shutdown: self.processing_shutdown.clone(),
        }
    }
}
