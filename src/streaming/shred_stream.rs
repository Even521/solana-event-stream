use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use futures::StreamExt;
use solana_sdk::pubkey::Pubkey;

use crate::common::AnyResult;
use crate::protos::shredstream::SubscribeEntriesRequest;
use crate::streaming::common::{EventProcessor, SubscriptionHandle};
use crate::streaming::event_parser::common::filter::EventTypeFilter;
use crate::streaming::event_parser::common::high_performance_clock::get_high_perf_clock;
use crate::streaming::event_parser::{Protocol, UnifiedEvent};
use crate::streaming::shred::pool::factory;
use log::error;
use solana_entry::entry::Entry;

use super::ShredStreamGrpc;

impl ShredStreamGrpc {

    /// 订阅 ShredStream 数据流，并将接收到的事件通过回调函数处理
    ///
    /// # 泛型参数
    /// - `F`：事件回调闭包类型，实现 `Fn(Box<dyn UnifiedEvent>) + Send + Sync + 'static`
    ///
    /// # 参数
    /// - `protocols`：需要订阅的协议类型列表
    /// - `bot_wallet`：可选的钱包地址，用于事件处理上下文
    /// - `event_type_filter`：可选的事件类型过滤器
    /// - `callback`：事件回调函数，接收到事件后会调用
    ///
    /// # 返回值
    /// - `AnyResult<()>`：成功返回 `Ok(())`，失败返回错误
    ///
    /// # 功能说明
    /// 1️⃣ **停止已有订阅**：
    ///    - 如果已有活跃订阅，先调用 `self.stop().await` 停止它，避免重复订阅
    ///
    /// 2️⃣ **启动性能监控**：
    ///    - 如果配置开启了 `enable_metrics`，使用 `metrics_manager.start_auto_monitoring()` 启动自动监控
    ///
    /// 3️⃣ **创建事件处理器**：
    ///    - 构建 `EventProcessor` 并配置协议、事件过滤器、回调函数及背压策略
    ///
    /// 4️⃣ **启动流处理异步任务**：
    ///    - 使用 gRPC 客户端订阅 `SubscribeEntriesRequest` 流
    ///    - 异步循环读取 `stream.next().await`
    ///    - 对每条消息：
    ///        a. 打印接收时间（纳秒）、slot 和原始 entries 字节长度
    ///        b. 尝试用 `bincode` 反序列化 entries
    ///        c. 遍历每个 `Entry` 的交易
    ///        d. 封装为 `TransactionWithSlot`
    ///        e. 调用 `EventProcessor::process_shred_transaction_with_metrics` 处理
    ///        f. 处理失败打印错误
    ///
    /// 5️⃣ **错误处理**：
    ///    - 如果反序列化失败，打印 slot 对应错误
    ///    - 如果 stream 出现错误（如网络中断），打印错误并退出循环
    ///
    /// 6️⃣ **保存订阅句柄**：
    ///    - 将异步任务句柄、性能监控句柄封装为 `SubscriptionHandle`
    ///    - 保存到 `self.subscription_handle`，用于后续停止订阅或管理生命周期
    ///
    /// # 异步特性
    /// - 函数为 `async`，内部使用 `tokio::spawn` 启动流处理任务
    /// - 支持高性能异步流处理及背压控制
    pub async fn shredstream_subscribe<F>(
        &self,
        protocols: Vec<Protocol>,
        bot_wallet: Option<Pubkey>,
        event_type_filter: Option<EventTypeFilter>,
        callback: F,
    ) -> AnyResult<()>
    where
        F: Fn(Box<dyn UnifiedEvent>) + Send + Sync + 'static,
    {
        // 如果已有活跃订阅，先停止它
        self.stop().await;

        let mut metrics_handle = None;
        // 启动自动性能监控（如果启用）
        if self.config.enable_metrics {
            metrics_handle = self.metrics_manager.start_auto_monitoring().await;
        }

        // 创建事件处理器
        let mut event_processor =
            EventProcessor::new(self.metrics_manager.clone(), self.config.clone());
        event_processor.set_protocols_and_event_type_filter(
            super::common::EventSource::Shred,
            protocols,
            event_type_filter,
            self.config.backpressure.clone(),
            Some(Arc::new(callback)),
        );

        // 启动流处理
        let mut client = (*self.shredstream_client).clone();
        let request = tonic::Request::new(SubscribeEntriesRequest {});
        let mut stream = client.subscribe_entries(request).await?.into_inner();
        let event_processor_clone = event_processor.clone();

        // 启动异步任务来处理 ShredStream 流
        let stream_task = tokio::spawn(async move {
            // 循环从 stream 中接收消息，直到流结束或发生错误
            while let Some(message) = stream.next().await {
                match message {
                    Ok(msg) => {
                        //My print
                        print_slot_entries_info(msg.slot, msg.entries.len());
                        // 尝试将原始 entries 字节反序列化为 Vec<Entry>
                        if let Ok(entries) = bincode::deserialize::<Vec<Entry>>(&msg.entries) {
                            for entry in entries {
                                for transaction in entry.transactions {
                                    // 封装为带 slot 信息的 TransactionWithSlot 对象
                                    let transaction_with_slot =
                                        factory::create_transaction_with_slot_pooled(
                                            transaction.clone(),
                                            msg.slot,
                                            get_high_perf_clock(),
                                        );

                                    // 使用 EventProcessor 处理交易（包含背压控制）
                                    if let Err(e) = event_processor_clone
                                        .process_shred_transaction_with_metrics(
                                            transaction_with_slot,
                                            bot_wallet,
                                        )
                                        .await
                                    {
                                        error!("Error handling message: {e:?}");
                                    }
                                }
                            }
                        } else {
                            error!("Failed to deserialize entries for slot {}", msg.slot);
                        }
                    }
                    Err(error) => {
                        error!("Stream error: {error:?}");
                        break;
                    }
                }
            }
        });

        // 保存订阅句柄，便于后续停止或管理生命周期
        let subscription_handle = SubscriptionHandle::new(stream_task, None, metrics_handle);
        let mut handle_guard = self.subscription_handle.lock().await;
        *handle_guard = Some(subscription_handle);

        Ok(())
    }

}

/// 打印接收到的 Slot 信息和原始 entries 字节长度
/// 同时记录接收时间（纳秒）
///
/// # 参数
/// - `slot`: 当前 slot
/// - `entries_len`: entries 的字节长度
pub fn print_slot_entries_info(slot: u64, entries_len: usize) {
    let recv_time_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_nanos();

    println!(
        "Time: {} ns | Slot {} | Raw entries bytes: {}",
        recv_time_ns, slot, entries_len
    );
}