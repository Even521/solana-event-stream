use solana_event_stream::streaming::{
    event_parser::{
        protocols::{
            bonk::parser::BONK_PROGRAM_ID, meteora_damm_v2::parser::METEORA_DAMM_V2_PROGRAM_ID,
            pumpfun::parser::PUMPFUN_PROGRAM_ID, pumpswap::parser::PUMPSWAP_PROGRAM_ID,
            raydium_amm_v4::parser::RAYDIUM_AMM_V4_PROGRAM_ID,
            raydium_clmm::parser::RAYDIUM_CLMM_PROGRAM_ID,
            raydium_cpmm::parser::RAYDIUM_CPMM_PROGRAM_ID,
        },
        DexEvent, Protocol,
    },
    grpc::ClientConfig,
    yellowstone_grpc::{AccountFilter, TransactionFilter},
    YellowstoneGrpc,
};
use solana_event_stream::streaming::event_parser::common::EventType;
use solana_event_stream::streaming::event_parser::common::filter::EventTypeFilter;
use solana_event_stream::streaming::event_parser::DexEvent::PumpFunCreateTokenEvent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Yellowstone gRPC Streamer...");
    test_grpc().await?;
    Ok(())
}

async fn test_grpc() -> Result<(), Box<dyn std::error::Error>> {
    println!("Subscribing to Yellowstone gRPC events...");

    // Create low-latency configuration
    let mut config: ClientConfig = ClientConfig::default();
    // Enable performance monitoring, has performance overhead, disabled by default
    config.enable_metrics = true;
    let grpc = YellowstoneGrpc::new_with_config(
        " ".to_string(),
        None,
        config,
    )?;

    println!("GRPC client created successfully");

    let callback = create_event_callback();

    // Will try to parse corresponding protocol events from transactions
    let protocols = vec![
        Protocol::PumpFun
    ];

    println!("Protocols to monitor: {:?}", protocols);

    // Filter accounts
    let account_include = vec![
        PUMPFUN_PROGRAM_ID.to_string(),         // Listen to pumpfun program ID
        // PUMPSWAP_PROGRAM_ID.to_string(),        // Listen to pumpswap program ID
        // BONK_PROGRAM_ID.to_string(),            // Listen to bonk program ID
        // RAYDIUM_CPMM_PROGRAM_ID.to_string(),    // Listen to raydium_cpmm program ID
        // RAYDIUM_CLMM_PROGRAM_ID.to_string(),    // Listen to raydium_clmm program ID
        // RAYDIUM_AMM_V4_PROGRAM_ID.to_string(),  // Listen to raydium_amm_v4 program ID
        // METEORA_DAMM_V2_PROGRAM_ID.to_string(), // Listen to meteora_damm_v2 program ID
    ];
    let account_exclude = vec![];
    let account_required = vec![];

    // Listen to transaction data
    let transaction_filter = TransactionFilter {
        account_include: account_include.clone(),
        account_exclude,
        account_required,
    };

    // Listen to account data belonging to owner programs -> account event monitoring
    let account_filter =
        AccountFilter { account: vec![], owner: account_include.clone(), filters: vec![] };

    // Event filtering
    // No event filtering, includes all events
    //let event_type_filter = None;
    // Only include PumpSwapBuy events and PumpSwapSell events
    // let event_type_filter = Some(EventTypeFilter { include: vec![EventType::PumpFunTrade] });
    let event_type_filter = Some(EventTypeFilter { include: vec![EventType::PumpFunCreateToken,EventType::PumpFunCreateV2Token,EventType::PumpFunBuy,EventType::PumpFunSell] });

    println!("Starting to listen for events, press Ctrl+C to stop...");
    println!("Monitoring programs: {:?}", account_include);

    println!("Starting subscription...");

    grpc.subscribe_events_immediate(
        protocols,
        None,
        vec![transaction_filter],
        vec![account_filter],
        event_type_filter,
        None,
        callback,
    )
    .await?;

    // 支持 stop 方法，测试代码 -  异步1000秒之后停止
    let grpc_clone = grpc.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(1000)).await;
        grpc_clone.stop().await;
    });

    println!("Waiting for Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;

    Ok(())
}

fn create_event_callback() -> impl Fn(DexEvent) {
    |event: DexEvent| {

        match event {
            DexEvent::PumpFunCreateTokenEvent(e) => {
                println!("{:?}", e);
            }
            DexEvent::PumpFunCreateV2TokenEvent(e) => {
                println!("{:?}", e);
            }
            DexEvent::PumpFunTradeEvent(e)=>{
               // println!("{:?}", e);
            }
            // .... other events
            _ => {
              //  println!("{:?}", event);
            }
        }
    }
}