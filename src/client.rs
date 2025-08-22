use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Error;
use futures::{SinkExt, StreamExt as _};
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::transport::ClientTlsConfig;
use tonic_health::pb::health_client::HealthClient;
use yellowstone_grpc_client::{GeyserGrpcClient, InterceptorXToken};
use yellowstone_grpc_proto::geyser::{
    geyser_client::GeyserClient, subscribe_request_filter_accounts_filter,
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    SubscribeUpdateAccount, SubscribeUpdateSlot, SubscribeUpdateTransaction,
};

const CHANNEL_SIZE: usize = 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(i32)]
pub enum SubLevel {
    Processed,
    Confirmed,
    Finalized,
}

async fn create_grpc_client(
    url: &str,
    x_token: Option<String>,
) -> Result<GeyserGrpcClient<InterceptorXToken>, Error> {
    let domain = url
        .split("://")
        .nth(1)
        .unwrap_or(url)
        .split(':')
        .next()
        .unwrap_or(url);
    log::info!("grpc tls domain: {} | {}", domain, url);

    let tls_config = ClientTlsConfig::new().domain_name(domain); // 确保域名正确

    let builder = GeyserGrpcClient::build_from_shared(url.to_string())?
        .tls_config(tls_config)?
        .x_token(x_token)?
        .max_decoding_message_size(1024 * 1024 * 100)
        .connect_timeout(Duration::from_secs(10))
        .buffer_size(1024 * 1024 * 100)
        .http2_adaptive_window(true)
        .http2_keep_alive_interval(Duration::from_secs(10))
        .initial_connection_window_size(1024 * 1024 * 100)
        .initial_stream_window_size(1024 * 1024 * 100)
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .tcp_nodelay(true);

    let channel = builder.endpoint.connect().await?;

    let interceptor = InterceptorXToken {
        x_token: builder.x_token.clone(),
        x_request_snapshot: builder.x_request_snapshot,
    };

    let mut geyser = GeyserClient::with_interceptor(channel.clone(), interceptor.clone());
    if let Some(encoding) = builder.send_compressed {
        geyser = geyser.send_compressed(encoding);
    }
    if let Some(encoding) = builder.accept_compressed {
        geyser = geyser.accept_compressed(encoding);
    }
    if let Some(limit) = builder.max_decoding_message_size {
        geyser = geyser.max_decoding_message_size(limit);
    }
    if let Some(limit) = builder.max_encoding_message_size {
        geyser = geyser.max_encoding_message_size(limit);
    }

    let client =
        GeyserGrpcClient::new(HealthClient::with_interceptor(channel, interceptor), geyser);
    Ok(client)
}

#[derive(Clone)]
pub enum SenderChannelType {
    Transaction(mpsc::Sender<SubscribeUpdateTransaction>),
    Account(mpsc::Sender<SubscribeUpdateAccount>),
    Slot(mpsc::Sender<SubscribeUpdateSlot>),
}

#[derive(Clone)]
struct UserSubInfo {
    request: SubscribeRequest,
    channel: SenderChannelType,
    cancel_tx: Option<mpsc::Sender<bool>>,
}

struct InnerClient {
    url: String,
    x_token: Option<String>,
    grpc: GeyserGrpcClient<InterceptorXToken>,
}

pub struct YGrpcClient {
    client: Arc<Mutex<InnerClient>>,
    sub_infos: Arc<RwLock<HashMap<String, UserSubInfo>>>,
}

impl YGrpcClient {
    pub async fn new(url: &str, x_token: Option<String>) -> Arc<Self> {
        let grpc = create_grpc_client(url, x_token.clone()).await.unwrap();
        let instance = Self {
            client: Arc::new(Mutex::new(InnerClient {
                url: url.to_string(),
                x_token,
                grpc,
            })),
            sub_infos: Arc::new(RwLock::new(HashMap::new())),
        };
        let instance = Arc::new(instance);
        let instance_clone = instance.clone();

        tokio::spawn(async move {
            let mut skip_ping = false;
            loop {
                let err = instance_clone.process_reconnect(skip_ping).await;
                if let Err(e) = err {
                    log::error!("reconnect error: {:?}, delay for 10s", e);
                    skip_ping = true;
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
        });

        instance
    }

    async fn process_reconnect(&self, skip_ping: bool) -> Result<(), Error> {
        if !skip_ping {
            let mut client = self.client.lock().await;
            let mut rx = Self::process_ping(&mut client.grpc).await?;
            drop(client);
            while let Some(_) = rx.recv().await {
                log::warn!("grpc prepare to reconnect...");
                break;
            }
        }

        let mut client = self.client.lock().await;
        client.grpc = create_grpc_client(&client.url, client.x_token.clone()).await?;
        let mut sub_info_clone = self.sub_infos.read().await.clone();

        for (id, sub_info) in sub_info_clone.iter_mut() {
            log::info!("reconnect sub: {}", id);
            match &sub_info.channel {
                SenderChannelType::Transaction(channel) => {
                    let cancel_tx = Self::_subscribe_transactions(
                        &mut client.grpc,
                        id.clone(),
                        &sub_info.request,
                        channel.clone(),
                        self.sub_infos.clone(),
                    )
                    .await?;
                    sub_info.cancel_tx = Some(cancel_tx);
                    self.sub_infos
                        .write()
                        .await
                        .insert(id.clone(), sub_info.clone());
                }
                SenderChannelType::Account(channel) => {
                    let cancel_tx = Self::_subscribe_accounts(
                        &mut client.grpc,
                        id.clone(),
                        &sub_info.request,
                        channel.clone(),
                        self.sub_infos.clone(),
                    )
                    .await?;
                    sub_info.cancel_tx = Some(cancel_tx);
                    self.sub_infos
                        .write()
                        .await
                        .insert(id.clone(), sub_info.clone());
                }
                SenderChannelType::Slot(channel) => {
                    let cancel_tx = Self::_subscribe_slots(
                        &mut client.grpc,
                        id.clone(),
                        &sub_info.request,
                        channel.clone(),
                        self.sub_infos.clone(),
                    )
                    .await?;
                    sub_info.cancel_tx = Some(cancel_tx);
                    self.sub_infos
                        .write()
                        .await
                        .insert(id.clone(), sub_info.clone());
                }
            }
        }

        Ok(())
    }

    async fn process_ping(
        client: &mut GeyserGrpcClient<InterceptorXToken>,
    ) -> Result<mpsc::Receiver<bool>, Error> {
        let req = Some(SubscribeRequest {
            slots: HashMap::new(),
            accounts: HashMap::new(),
            transactions: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: Some(CommitmentLevel::Confirmed as i32),
            accounts_data_slice: Vec::new(),
            ping: Some(SubscribeRequestPing { id: 1 }),
            transactions_status: HashMap::new(),
        });

        let (mut sub, mut stream) = client.subscribe_with_request(req).await?;
        let (tx, rx) = mpsc::channel::<bool>(1);

        tokio::spawn(async move {
            while let Some(message) = stream.next().await {
                match message {
                    Ok(update) => {
                        if let Some(update_oneof) = update.update_oneof {
                            match update_oneof {
                                UpdateOneof::Ping(_) => {
                                    log::debug!("grpc recv ping");
                                    let _ = sub.send(SubscribeRequest {
                                        ping: Some(SubscribeRequestPing { id: 1 }),
                                        ..Default::default()
                                    });
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("ping error: {:?}", e);
                        break;
                    }
                }
            }
            let _ = tx.try_send(true);
        });

        Ok(rx)
    }

    async fn _subscribe_transactions(
        grpc: &mut GeyserGrpcClient<InterceptorXToken>,
        id: String,
        req: &SubscribeRequest,
        tx_channel: mpsc::Sender<SubscribeUpdateTransaction>,
        sub_infos: Arc<RwLock<HashMap<String, UserSubInfo>>>,
    ) -> Result<mpsc::Sender<bool>, Error> {
        let (cancel_tx, mut cancel_rx) = mpsc::channel::<bool>(1);
        let (_, mut stream) = grpc.subscribe_with_request(Some(req.clone())).await?;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    message = stream.next() => {
                        match message {
                            Some(Ok(update)) => {
                                if let Some(update_oneof) = update.update_oneof {
                                    match update_oneof {
                                        UpdateOneof::Transaction(transaction) => {
                                            if transaction.transaction.is_some() {
                                                let res = tx_channel.try_send(transaction);
                                                if let Err(e) = res {
                                                    log::warn!("ID: {:?}, tx sub send error: {:?}", id, e);
                                                    break;
                                                }
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                log::error!("ID: {:?}, tx sub recv error: {:?}", id, e);
                                break;
                            }
                            None => {
                                log::info!("ID: {:?}, stream ended", id);
                                break;
                            }
                        }
                    }
                    _ = cancel_rx.recv() => {
                        log::info!("ID: {:?}, tx sub cancelled", id);
                        break;
                    }
                }
            }
            sub_infos.write().await.remove(&id);
        });

        Ok(cancel_tx)
    }

    async fn _subscribe_accounts(
        grpc: &mut GeyserGrpcClient<InterceptorXToken>,
        id: String,
        req: &SubscribeRequest,
        tx_channel: mpsc::Sender<SubscribeUpdateAccount>,
        sub_infos: Arc<RwLock<HashMap<String, UserSubInfo>>>,
    ) -> Result<mpsc::Sender<bool>, Error> {
        let (cancel_tx, mut cancel_rx) = mpsc::channel::<bool>(1);
        let (_, mut stream) = grpc.subscribe_with_request(Some(req.clone())).await?;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    message = stream.next() => {
                        match message {
                            Some(Ok(update)) => {
                                if let Some(update_oneof) = update.update_oneof {
                                    match update_oneof {
                                        UpdateOneof::Account(account) => {
                                            if account.account.is_some() {
                                                let res = tx_channel.try_send(account);
                                                if let Err(e) = res {
                                                    log::warn!("ID: {:?}, account sub send error: {:?}", id, e);
                                                    break;
                                                }
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                log::error!("ID: {:?}, acc sub recv error: {:?}", id, e);
                                break;
                            }
                            None => {
                                log::info!("ID: {:?}, stream ended", id);
                                break;
                            }
                        }
                    }
                    _ = cancel_rx.recv() => {
                        log::info!("ID: {:?}, account sub cancelled", id);
                        break;
                    }
                }
            }
            sub_infos.write().await.remove(&id);
        });

        Ok(cancel_tx)
    }

    pub async fn simple_sub_transactions(
        &self,
        id: String,
        accounts: Vec<String>,
        required: bool,
        failed: Option<bool>,
        level: SubLevel,
    ) -> Result<mpsc::Receiver<SubscribeUpdateTransaction>, Error> {
        let mut filters = HashMap::new();
        let mut filter = SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed,
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
        };
        if required {
            filter.account_required = accounts;
        } else {
            filter.account_include = accounts;
        }
        filters.insert(id.clone(), filter);
        self.subscribe_transactions(id, filters, level).await
    }

    pub async fn subscribe_transactions(
        &self,
        id: String,
        tx_filters: HashMap<String, SubscribeRequestFilterTransactions>,
        level: SubLevel,
    ) -> Result<mpsc::Receiver<SubscribeUpdateTransaction>, Error> {
        let req = SubscribeRequest {
            slots: HashMap::new(),
            accounts: HashMap::new(),
            transactions: tx_filters.clone(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: Some(level as i32),
            accounts_data_slice: Vec::new(),
            ping: None,
            transactions_status: HashMap::new(),
        };
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        log::debug!("subscribe_transactions id: {:?}", id.clone());
        let mut client = self.client.lock().await;
        let cancel_tx = Self::_subscribe_transactions(
            &mut client.grpc,
            id.clone(),
            &req,
            tx.clone(),
            self.sub_infos.clone(),
        )
        .await?;

        let sub_info = UserSubInfo {
            request: req,
            channel: SenderChannelType::Transaction(tx.clone()),
            cancel_tx: Some(cancel_tx),
        };

        self.sub_infos.write().await.insert(id.clone(), sub_info);

        Ok(rx)
    }

    pub async fn simple_sub_programs(
        &self,
        id: String,
        programs: Vec<String>,
        size: Option<u32>,
        level: SubLevel,
    ) -> Result<mpsc::Receiver<SubscribeUpdateAccount>, Error> {
        let mut filters = HashMap::new();
        let size_filter = size.map_or(vec![], |size| {
            vec![SubscribeRequestFilterAccountsFilter {
                filter: Some(subscribe_request_filter_accounts_filter::Filter::Datasize(
                    size as u64,
                )),
            }]
        });
        let program_filter = SubscribeRequestFilterAccounts {
            account: vec![],
            owner: programs.clone(),
            filters: size_filter,
        };
        log::info!(
            "subscribe_programs id: {:?}, programs: {:?}",
            id.clone(),
            program_filter
        );
        filters.insert(id.clone(), program_filter);
        self.subscribe_accounts(id, filters, level).await
    }

    pub async fn subscribe_accounts(
        &self,
        id: String,
        account_filters: HashMap<String, SubscribeRequestFilterAccounts>,
        level: SubLevel,
    ) -> Result<mpsc::Receiver<SubscribeUpdateAccount>, Error> {
        let req = SubscribeRequest {
            slots: HashMap::new(),
            accounts: account_filters.clone(),
            transactions: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: Some(level as i32),
            accounts_data_slice: Vec::new(),
            ping: None,
            transactions_status: HashMap::new(),
        };

        log::debug!("subscribe_accounts id: {:?}", id.clone());
        let mut inner_client = self.client.lock().await;
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        let cancel_tx = Self::_subscribe_accounts(
            &mut inner_client.grpc,
            id.clone(),
            &req,
            tx.clone(),
            self.sub_infos.clone(),
        )
        .await?;

        let sub_info = UserSubInfo {
            request: req,
            channel: SenderChannelType::Account(tx.clone()),
            cancel_tx: Some(cancel_tx),
        };

        self.sub_infos.write().await.insert(id.clone(), sub_info);

        Ok(rx)
    }

    async fn _subscribe_slots(
        grpc: &mut GeyserGrpcClient<InterceptorXToken>,
        id: String,
        req: &SubscribeRequest,
        tx_channel: mpsc::Sender<SubscribeUpdateSlot>,
        sub_infos: Arc<RwLock<HashMap<String, UserSubInfo>>>,
    ) -> Result<mpsc::Sender<bool>, Error> {
        let (cancel_tx, mut cancel_rx) = mpsc::channel::<bool>(1);
        let (_, mut stream) = grpc.subscribe_with_request(Some(req.clone())).await?;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    message = stream.next() => {
                        match message {
                            Some(Ok(update)) => {
                                if let Some(update_oneof) = update.update_oneof {
                                    match update_oneof {
                                        UpdateOneof::Slot(slot) => {
                                            let res = tx_channel.try_send(slot);
                                            if let Err(e) = res {
                                                log::error!("ID: {:?}, slot sub send error: {:?}", id, e);
                                                break;
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                log::error!("ID: {:?}, slot sub recv error: {:?}", id, e);
                                break;
                            }
                            None => {
                                log::info!("ID: {:?}, stream ended", id);
                                break;
                            }
                        }
                    }
                    _ = cancel_rx.recv() => {
                        log::info!("ID: {:?}, slot sub cancelled", id);
                        break;
                    }
                }
            }
            sub_infos.write().await.remove(&id);
        });

        Ok(cancel_tx)
    }

    pub async fn subscribe_slots(
        &self,
        id: String,
        filter_by_commitment: Option<bool>,
        level: SubLevel,
    ) -> Result<mpsc::Receiver<SubscribeUpdateSlot>, Error> {
        let mut filter = HashMap::new();
        filter.insert(
            id.clone(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: filter_by_commitment,
            },
        );

        let req = SubscribeRequest {
            slots: filter,
            accounts: HashMap::new(),
            transactions: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: Some(level as i32),
            accounts_data_slice: Vec::new(),
            ping: None,
            transactions_status: HashMap::new(),
        };

        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        let mut inner_client = self.client.lock().await;
        let cancel_tx = Self::_subscribe_slots(
            &mut inner_client.grpc,
            id.clone(),
            &req,
            tx.clone(),
            self.sub_infos.clone(),
        )
        .await?;

        let sub_info = UserSubInfo {
            request: req,
            channel: SenderChannelType::Slot(tx.clone()),
            cancel_tx: Some(cancel_tx),
        };

        self.sub_infos.write().await.insert(id.clone(), sub_info);

        Ok(rx)
    }

    pub async fn cancel_subscribe(&self, id: &str) {
        let sub_infos = self.sub_infos.read().await;
        if let Some(sub_info) = sub_infos.get(id) {
            if let Some(cancel_tx) = &sub_info.cancel_tx {
                let _ = cancel_tx.send(true).await;
                log::info!("Cancel subscription: {}", id);
            }
        }
    }
}

mod tests {
    use super::*;
    fn init_test() {
        // 配置日志输出
        env_logger::Builder::from_default_env()
            .format_timestamp(Some(env_logger::fmt::TimestampPrecision::Millis)) // 不显示时间戳
            .format_target(false) // 不显示目标
            .format_level(true) // 显示日志级别
            .target(env_logger::Target::Stdout) // 输出到标准输出
            .init();
    }

    #[tokio::test]
    async fn test_cancel_subscribe() {
        init_test();
        // RUST_LOG=info cargo test --package ygrpc --lib -- client::tests::test_cancel_subscribe --exact --show-output --nocapture
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

        // Test account subscription cancellation
        let acc_id = "acc_cancel_test".to_string();
        let programs = vec!["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string()];
        let mut account_rx = client
            .simple_sub_programs(acc_id.clone(), programs, None, level)
            .await
            .unwrap();

        let acc_client = client.clone();
        let acc_id_clone = acc_id.clone();
        tokio::spawn(async move {
            let mut cnt = 0;
            while let Some(acc) = account_rx.recv().await {
                let acc = acc.account.unwrap();
                cnt += 1;
                log::info!(
                    "cancel test - acc cnt: {}, owner: {:?}, data len: {}",
                    cnt,
                    bs58::encode(acc.owner).into_string(),
                    acc.data.len()
                );
                if cnt >= 2 {
                    log::info!("Requesting cancellation of account subscription");
                    acc_client.cancel_subscribe(&acc_id_clone).await;
                    break;
                }
            }
            log::info!("acc cancel test loop exit");
        });

        // Test slot subscription cancellation
        let slot_id = "slot_cancel_test".to_string();
        let mut slot_rx = client
            .subscribe_slots(slot_id.clone(), Some(false), level)
            .await
            .unwrap();

        let slot_client = client.clone();
        let slot_id_clone = slot_id.clone();
        tokio::spawn(async move {
            let mut cnt = 0;
            while let Some(slot) = slot_rx.recv().await {
                cnt += 1;
                log::info!("cancel test - slot cnt: {}, slot: {:?}", cnt, slot.slot);
                if cnt >= 2 {
                    log::info!("Requesting cancellation of slot subscription");
                    slot_client.cancel_subscribe(&slot_id_clone).await;
                    break;
                }
            }
            log::info!("slot cancel test loop exit");
        });

        // Wait for some activity then test cancellations
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        // Cancel any remaining subscriptions manually
        log::info!("Manually cancelling remaining subscriptions");
        let _ = client.cancel_subscribe(&tx_id).await;
        let _ = client.cancel_subscribe(&acc_id).await;
        let _ = client.cancel_subscribe(&slot_id).await;

        // Verify subscriptions are cleaned up
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        let sub_count = client.sub_infos.read().await.len();
        log::info!("Remaining subscriptions after cancel: {}", sub_count);

        assert_eq!(
            sub_count, 0,
            "All subscriptions should be cancelled and cleaned up"
        );
    }
}
