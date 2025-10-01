/// `SolanaRpcClient` 是 Solana 非阻塞异步 RPC 客户端的类型别名
///
/// 实际类型为 `solana_client::nonblocking::rpc_client::RpcClient`
/// 使用该别名可以简化代码书写，并统一客户端类型
pub type SolanaRpcClient = solana_client::nonblocking::rpc_client::RpcClient;

/// `AnyResult<T>` 是通用的结果类型别名
///
/// 实际类型为 `anyhow::Result<T>`
/// 使用 `anyhow` 可以方便地处理各种错误，支持 `?` 链式错误传播
pub type AnyResult<T> = anyhow::Result<T>;
