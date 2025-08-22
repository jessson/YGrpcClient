# YGrpc 客户端

一个对 Yellowstone gRPC 协议简单封装的 Rust 客户端，方便使用。

## 使用
源码依赖
根据自己的项目配置调整Cargo.toml中的依赖
较新的yellow stone grpc可能需要修改部分源码

## 使用方法
```rust
let client =
    YGrpcClient::new("https://solana-yellowstone-grpc.publicnode.com:443", None).await;
let level = SubLevel::Processed;

// Test transaction subscription cancellation
let tx_id = "tx_cancel_test".to_string();
let accounts = vec!["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string()]; // System program
let mut transaction_rx = client
    .simple_sub_transactions(tx_id.clone(), accounts, false, None, level)
    .await
    .unwrap();

let tx_client = client.clone();
let tx_id_clone = tx_id.clone();
tokio::spawn(async move {
    let mut cnt = 0;
    while let Some(tx) = transaction_rx.recv().await {
        let tx = tx.transaction.unwrap();
        cnt += 1;
        log::info!(
            "cancel test - cnt: {}, tx: {:?}",
            cnt,
            bs58::encode(tx.signature).into_string()
        );
        if cnt >= 3 {
            log::info!("Requesting cancellation of transaction subscription");
            tx_client.cancel_subscribe(&tx_id_clone).await;
            break;
        }
    }
    log::info!("tx cancel test loop exit");
});
```

## 许可证

本项目采用 MIT 许可证。

# YGrpcClient
