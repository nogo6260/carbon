use ::{
    async_trait::async_trait,
    carbon_core::{
        datasource::{Datasource, TransactionUpdate, Update, UpdateType},
        error::CarbonResult,
        metrics::MetricsCollection,
    },
    futures::{stream::try_unfold, TryStreamExt},
    jito_protos::shredstream::{
        shredstream_proxy_client::ShredstreamProxyClient, SubscribeEntriesRequest,
    },
    scc::HashCache,
    solana_client::rpc_client::SerializableTransaction,
    solana_entry::entry::Entry,
    solana_sdk::{message::v0::LoadedAddresses, pubkey::Pubkey},
    solana_transaction_status::TransactionStatusMeta,
    std::{
        collections::HashMap,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    },
    tokio::sync::{mpsc::Sender, RwLock},
    tokio_util::sync::CancellationToken,
};

type LocalAddresseTables = Arc<RwLock<HashMap<Pubkey, [Pubkey; 255]>>>;

#[derive(Debug)]
pub struct JitoShredstreamGrpcClient {
    endpoint: String,
    local_address_table_loopups: Option<LocalAddresseTables>,
    include_vote: bool,
}

impl JitoShredstreamGrpcClient {
    pub fn new(endpoint: String) -> Self {
        JitoShredstreamGrpcClient {
            endpoint,
            local_address_table_loopups: None,
            include_vote: false,
        }
    }

    pub fn with_local_alts(mut self, alts: LocalAddresseTables) -> Self {
        self.local_address_table_loopups = Some(alts);
        self
    }

    pub fn with_vote(mut self) -> Self {
        self.include_vote = true;
        self
    }
}

#[async_trait]
impl Datasource for JitoShredstreamGrpcClient {
    async fn consume(
        &self,
        sender: &Sender<Update>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> CarbonResult<()> {
        let sender = sender.clone();

        let mut client = ShredstreamProxyClient::connect(self.endpoint.clone())
            .await
            .map_err(|err| carbon_core::error::Error::FailedToConsumeDatasource(err.to_string()))?;

        let include_vote = self.include_vote;
        let local_address_table_loopups = self.local_address_table_loopups.clone();
        tokio::spawn(async move {
            let result = tokio::select! {
                _ = cancellation_token.cancelled() => {
                    log::info!("Cancelling Jito Shreadstream gRPC subscription.");
                    return;
                }

                result = client.subscribe_entries(SubscribeEntriesRequest {}) =>
                    result
            };

            let stream = match result {
                Ok(r) => r.into_inner(),
                Err(e) => {
                    log::error!("Failed to subscribe: {:?}", e);
                    return;
                }
            };

            let stream = try_unfold(
                (stream, cancellation_token),
                |(mut stream, cancellation_token)| async move {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            log::info!("Cancelling Jito Shreadstream gRPC subscription.");
                            Ok(None)
                        },
                        v = stream.message() => match v {
                            Ok(Some(v)) => Ok(Some((v, (stream, cancellation_token)))),
                            Ok(None) => Ok(None),
                            Err(e) => Err(e),
                        },
                    }
                },
            );

            let dedup_cache = Arc::new(HashCache::with_capacity(1024, 4096));

            if
                let Err(e) = stream.try_for_each_concurrent(None, |message| {
                    let metrics = metrics.clone();
                    let sender = sender.clone();
                    let local_atls = local_address_table_loopups.clone();
                    let dedup_cache = dedup_cache.clone();

                    async move {
                        let start_time = SystemTime::now();
                        let block_time = Some(
                            start_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
                        );

                        let entries: Vec<Entry> = match bincode::deserialize(&message.entries) {
                            Ok(e) => e,
                            Err(e) => {
                                log::error!("Failed to deserialize entries: {:?}", e);
                                return Ok(());
                            }
                        };

                        let total_entries = entries.len();
                        let mut duplicate_entries = 0;

                        for entry in entries {
                            if dedup_cache.contains(&entry.hash) {
                                duplicate_entries += 1;
                                continue;
                            }
                            let _ = dedup_cache.put(entry.hash, ());

                            for transaction in entry.transactions {
                                let accounts = transaction.message.static_account_keys();
                                let is_vote = accounts.len() == 3 && solana_sdk::vote::program::check_id(&accounts[2]);
                                if !include_vote && is_vote {
                                    continue;
                                }

                                let signature = *transaction.get_signature();
                                let mut loaded_addresses = LoadedAddresses::default();

                                if
                                    let (Some(local_atls), Some(tables)) = (
                                        &local_atls,
                                        transaction.message.address_table_lookups(),
                                    )
                                {
                                    for table in tables.iter() {
                                        let keys = local_atls
                                            .read().await
                                            .get(&table.account_key)
                                            .cloned();

                                        if let Some(keys) = keys {
                                            let writable: Vec<_> = table.writable_indexes
                                                .iter()
                                                .filter_map(|&i| keys.get(i as usize))
                                                .collect();
                                            let readonly: Vec<_> = table.readonly_indexes
                                                .iter()
                                                .filter_map(|&i| keys.get(i as usize))
                                                .collect();

                                            loaded_addresses.writable.extend(writable);
                                            loaded_addresses.readonly.extend(readonly);
                                        }
                                    }
                                }

                                let update = Update::Transaction(
                                    Box::new(TransactionUpdate {
                                        signature,
                                        is_vote,
                                        transaction,
                                        meta: TransactionStatusMeta {
                                            status: Ok(()),
                                            loaded_addresses,
                                            ..Default::default()
                                        },
                                        slot: message.slot,
                                        block_time,
                                    })
                                );

                                if let Err(e) = sender.try_send(update) {
                                    log::error!(
                                        "Failed to send transaction update with signature {:?} at slot {}: {:?}",
                                        signature,
                                        message.slot,
                                        e
                                    );
                                    return Ok(());
                                }
                            }
                        }
                        
                        metrics
                            .record_histogram(
                                "jito_shredstream_grpc_entry_process_time_nanoseconds",
                                start_time.elapsed().unwrap().as_nanos() as f64
                            ).await
                            .unwrap();

                        metrics
                            .increment_counter(
                                "jito_shredstream_grpc_entry_updates_received",
                                total_entries as u64
                            ).await
                            .unwrap_or_else(|value| {
                                log::error!("Error recording metric: {}", value)
                            });

                        metrics
                            .increment_counter(
                                "jito_shredstream_grpc_duplicate_entries",
                                duplicate_entries
                            ).await
                            .unwrap_or_else(|value| {
                                log::error!("Error recording metric: {}", value)
                            });

                        Ok(())
                    }
                }).await
            {
                log::error!("Grpc stream error: {e:?}");
            }
        });

        Ok(())
    }

    fn update_types(&self) -> Vec<UpdateType> {
        vec![UpdateType::Transaction]
    }
}
