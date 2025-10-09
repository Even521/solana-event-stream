use solana_event_stream::{
    match_event,
    streaming::{
        event_parser::{
            protocols::{
                pumpfun::{
                    PumpFunCreateTokenEvent, PumpFunMigrateEvent,
                    PumpFunTradeEvent,
                },
                pumpswap::{
                    PumpSwapBuyEvent, PumpSwapCreatePoolEvent,
                    PumpSwapDepositEvent
                    , PumpSwapSellEvent, PumpSwapWithdrawEvent,
                }


                ,
                BlockMetaEvent,
            },
            Protocol, UnifiedEvent,
        },
        shred::StreamClientConfig,
        ShredStreamGrpc,
    },
};
use solana_event_stream::streaming::common::EventType;
use solana_event_stream::streaming::event_parser::common::filter::EventTypeFilter;
use solana_event_stream::streaming::event_parser::protocols::pumpfun::PumpFunMigrateEvent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting ShredStream Streamer...");
    test_shreds().await?;
    Ok(())
}

async fn test_shreds() -> Result<(), Box<dyn std::error::Error>> {
    println!("Subscribing to ShredStream events...");

    // 创建一个低延迟客户端配置实例
    let mut config = StreamClientConfig::low_latency();
    //消息的性能指标
    config.enable_metrics = true;
    //创建并初始化一个 ShredStream gRPC 客户端实例
    let shred_stream =
        ShredStreamGrpc::new_with_config("http://rich-us.zan.top:9999".to_string(), config).await?;

    let callback = create_event_callback();
    let protocols = vec![
        Protocol::PumpFun,
        Protocol::PumpSwap,
    ];

    // Event filtering
    // No event filtering, includes all events
   // let event_type_filter = None;
    // Only include PumpSwapBuy events and PumpSwapSell events
     let event_type_filter =
         EventTypeFilter { include: vec![EventType::PumpFunMigrate] };

    println!("Listening for events, press Ctrl+C to stop...");
    shred_stream.shredstream_subscribe(protocols, None, event_type_filter, callback).await?;



    tokio::signal::ctrl_c().await?;

    Ok(())
}

/// 创建一个事件处理回调函数
///
/// # 返回值
/// - 返回一个闭包 `impl Fn(Box<dyn UnifiedEvent>)`
///   可作为 EventProcessor 或 ShredStream 流处理器的事件回调
///
/// # 功能说明
/// 1️⃣ 接收一个 `Box<dyn UnifiedEvent>` 类型的事件
///    - 事件是统一接口 `UnifiedEvent`，可动态派发不同类型事件
///
/// 2️⃣ 使用 `match_event!` 宏匹配具体事件类型
///    - 当前示例只处理 `PumpFunMigrateEvent`
///    - 对匹配到的事件执行自定义逻辑（这里是打印事件内容）
///
/// 3️⃣ 可扩展
///    - 可以在宏中增加更多事件类型及处理逻辑
pub fn create_event_callback() -> impl Fn(Box<dyn UnifiedEvent>) {
    |event: Box<dyn UnifiedEvent>| {
        // 匹配不同事件类型
        match_event!(event, {
            PumpFunMigrateEvent => |e: PumpFunMigrateEvent| {
                // 打印 PumpFunMigrateEvent 事件内容
                println!("PumpFunMigrateEvent: {e:?}");
            },
            // 未来可继续添加更多事件类型处理逻辑
        });
    }
}