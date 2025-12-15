use solana_event_stream::streaming::{
    event_parser::{Protocol, DexEvent},
    shred::StreamClientConfig,
    ShredStreamGrpc,
};
use solana_event_stream::streaming::event_parser::common::EventType;
use solana_event_stream::streaming::event_parser::common::filter::EventTypeFilter;
use solana_event_stream::streaming::event_parser::DexEvent::PumpFunCreateTokenEvent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting ShredStream Streamer...");
    test_shreds().await?;
    Ok(())
}

async fn test_shreds() -> Result<(), Box<dyn std::error::Error>> {
    println!("Subscribing to ShredStream events...");

    // Create low-latency configuration
    let mut config = StreamClientConfig::default();
    // Enable performance monitoring, has performance overhead, disabled by default
    config.enable_metrics = false;
    let shred_stream =
        ShredStreamGrpc::new_with_config(" ".to_string(), config).await?;

    let callback = create_event_callback();
    let protocols = vec![
        Protocol::PumpFun,
    ];

    let protocols = vec![Protocol::PumpFun];
    let event_type_filter = EventTypeFilter {
        include: vec![
            EventType::PumpFunCreateToken,
            EventType::PumpFunBuy,
            EventType::PumpFunSell,
            EventType::PumpFunMigrate,
        ],
    };

    shred_stream.shredstream_subscribe(protocols, None, event_type_filter, callback).await?;

   //shred_stream.shredstream_subscribe(protocols, None, event_type_filter, callback).await?;

    // // 支持 stop 方法，测试代码 - 异步1000秒之后停止
    // let shred_clone = shred_stream.clone();
    // tokio::spawn(async move {
    //     tokio::time::sleep(std::time::Duration::from_secs(1000)).await;
    //     shred_clone.stop().await;
    // });

    println!("Waiting for Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;

    Ok(())
}

fn create_event_callback() -> impl Fn(DexEvent) {
    |event: DexEvent| {
        match event {
            DexEvent::PumpFunMigrateEvent(e) => {
                println!("PumpFunMigrateEvent: {:?} mint:{:?}", e.metadata.handle_us,e.user);
            }
            DexEvent::PumpFunCreateTokenEvent(e) =>{
                println!("PumpFunCreateTokenEvent: {:?} event:{:?}", e.metadata.handle_us,e);
            }
            _ => {}
        }
    }
}