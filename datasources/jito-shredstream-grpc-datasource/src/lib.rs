use ::{
    async_trait::async_trait,
    carbon_core::{
        datasource::{Datasource, TransactionUpdate, Update, UpdateType},
        error::CarbonResult,
        metrics::MetricsCollection,
    },
    carbon_jito_protos::shredstream::{
        shredstream_proxy_client::ShredstreamProxyClient, SubscribeEntriesRequest,
    },
    futures::{stream::try_unfold, TryStreamExt},
    scc::{HashCache, HashMap, HashSet},
    shredstream_lazy_deserialize::VecLazyEntry,
    solana_message::{
        legacy::Message as LegacyMessage,
        v0::{LoadedAddresses, Message},
        VersionedMessage,
    },
    solana_pubkey::Pubkey,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_status::TransactionStatusMeta,
    std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    },
    tokio::sync::mpsc::Sender,
    tokio_util::sync::CancellationToken,
};
const VOTE_ID: Pubkey = solana_pubkey::pubkey!("Vote111111111111111111111111111111111111111");

pub type LocalAddresseTables = Arc<HashMap<Pubkey, Vec<Pubkey>>>;
pub type TargetAccounts = Arc<HashSet<Pubkey>>;
pub type TargetPrograms = Arc<HashSet<Pubkey>>;

#[derive(Debug)]
pub struct JitoShredstreamGrpcClient {
    endpoint: String,
    local_address_table_loopups: Option<LocalAddresseTables>,
    target_accounts: Option<TargetAccounts>,
    target_programs: Option<TargetPrograms>,
    include_vote: bool,
}

impl JitoShredstreamGrpcClient {
    pub fn new(endpoint: String) -> Self {
        JitoShredstreamGrpcClient {
            endpoint,
            local_address_table_loopups: None,
            target_accounts: None,
            target_programs: None,
            include_vote: false,
        }
    }

    pub fn with_local_address_table(mut self, alts: LocalAddresseTables) -> Self {
        self.local_address_table_loopups = Some(alts);
        self
    }

    pub fn with_target_accounts(mut self, accounts: TargetAccounts) -> Self {
        self.target_accounts = Some(accounts);
        self
    }

    pub fn with_target_programs(mut self, programs: TargetPrograms) -> Self {
        self.target_programs = Some(programs);
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
        let target_accounts = self.target_accounts.clone();
        let target_programs = self.target_programs.clone();
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
                    let target_accounts = target_accounts.clone();
                    let target_programs = target_programs.clone();
                    let dedup_cache = dedup_cache.clone();

                    async move {
                        let start_time = SystemTime::now();
                        let block_time = Some(
                            start_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
                        );

                        let entries = match VecLazyEntry::new(&message.entries) {
                            Ok(e) => e,
                            Err(e) => {
                                log::error!("Failed to deserialize entries: {:?}", e);
                                return Ok(());
                            }
                        };

                        let total_entries = entries.len();
                        let mut duplicate_entries = 0;

                        'next_entry: for entry in entries.iter() {
                            let Ok(hash) = entry.to_hash() else {
                                continue;
                            };
                            if dedup_cache.contains(&hash) {
                                duplicate_entries += 1;
                                continue;
                            }
                            let _ = dedup_cache.put(hash, ());

                            for transaction in entry.transactions.iter() {
                                let Ok(account_keys) =
                                    transaction.message.static_account_keys() else {
                                    continue 'next_entry;
                                };

                                let Ok(mut instructions) = transaction.message.instructions() else {
                                    continue 'next_entry;
                                };

                                if let Some(programs) = &target_programs {
                                    instructions = instructions
                                        .into_iter()
                                        .filter_map(|ix|
                                            programs
                                                .contains(ix.program_id(&account_keys))
                                                .then_some(ix)
                                        )
                                        .collect::<Vec<_>>();

                                    if instructions.is_empty() {
                                        continue 'next_entry;
                                    }
                                }

                                let is_vote =
                                    account_keys.len() == 3 &&
                                    VOTE_ID.eq(&account_keys[2]);
                                if !include_vote && is_vote {
                                    continue;
                                }

                                let Ok(signature) = transaction.to_signature() else {
                                    continue 'next_entry;
                                };

                                let Ok(atls) = transaction.message.address_table_lookups() else {
                                    continue 'next_entry;
                                };

                                let mut loaded_addresses = LoadedAddresses::default();

                                if let (Some(local_atls), Some(tables)) = (&local_atls, &atls) {
                                    for table in tables.iter() {
                                        let keys = local_atls.get(&table.account_key);

                                        if let Some(keys) = keys {
                                            let writable: Vec<_> = table.writable_indexes
                                                .iter()
                                                .filter_map(|&i| keys.get().get(i as usize))
                                                .collect();
                                            let readonly: Vec<_> = table.readonly_indexes
                                                .iter()
                                                .filter_map(|&i| keys.get().get(i as usize))
                                                .collect();

                                            loaded_addresses.writable.extend(writable);
                                            loaded_addresses.readonly.extend(readonly);
                                        }
                                    }
                                }

                                if let Some(target_accounts) = &target_accounts {
                                    if
                                        !account_keys
                                            .iter()
                                            .any(|acc| target_accounts.contains(acc)) &&
                                        !loaded_addresses.writable
                                            .iter()
                                            .any(|acc| target_accounts.contains(acc)) &&
                                        !loaded_addresses.readonly
                                            .iter()
                                            .any(|acc| target_accounts.contains(acc))
                                    {
                                        continue 'next_entry;
                                    }
                                }

                                let Ok(header) = transaction.message.header() else {
                                    continue 'next_entry;
                                };

                                let Ok(recent_blockhash) =
                                    transaction.message.recent_blockhash() else {
                                    continue 'next_entry;
                                };

                                let versioned_message = match atls {
                                    Some(address_table_lookups) =>
                                        VersionedMessage::V0(Message {
                                            header,
                                            account_keys,
                                            recent_blockhash,
                                            instructions,
                                            address_table_lookups,
                                        }),
                                    None =>
                                        VersionedMessage::Legacy(LegacyMessage {
                                            header,
                                            account_keys,
                                            recent_blockhash,
                                            instructions,
                                        }),
                                };

                                let update = Update::Transaction(
                                    Box::new(TransactionUpdate {
                                        signature,
                                        is_vote,
                                        transaction: VersionedTransaction {
                                            signatures: vec![signature],
                                            message: versioned_message,
                                        },
                                        meta: TransactionStatusMeta {
                                            status: Ok(()),
                                            loaded_addresses,
                                            ..Default::default()
                                        },
                                        slot: message.slot,
                                        block_time,
                                        block_hash: None,
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
