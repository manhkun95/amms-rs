pub mod cache;
pub mod discovery;
pub mod error;
pub mod filters;

use crate::amms::amm::AutomatedMarketMaker;
use crate::amms::amm::AMM;
use crate::amms::error::AMMError;
use crate::amms::factory::Factory;

use alloy::consensus::BlockHeader;
use alloy::eips::BlockId;
use alloy::rpc::types::{Block, Filter, FilterSet, Log};
use alloy::{
    network::Network,
    primitives::{Address, FixedBytes},
    providers::Provider,
};
use async_stream::stream;
use cache::StateChange;
use cache::StateChangeCache;

use error::StateSpaceError;
use filters::AMMFilter;
use filters::PoolFilter;
use futures::stream::FuturesUnordered;
use futures::Stream;
use futures::StreamExt;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};
use tokio::sync::RwLock;
use tracing::debug;
use tracing::info;
use tracing::warn;

pub const CACHE_SIZE: usize = 30;

#[derive(Clone)]
pub struct StateSpaceManager<N, P> {
    pub state: Arc<RwLock<StateSpace>>,
    pub latest_block: Arc<AtomicU64>,
    // discovery_manager: Option<DiscoveryManager>,
    pub block_filter: Filter,
    pub provider: P,
    pub factories: Vec<Factory>,
    phantom: PhantomData<N>,
    // TODO: add support for caching
}

impl<N, P> StateSpaceManager<N, P> {
    pub async fn subscribe(
        &self,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Vec<Address>, StateSpaceError>> + Send + '_>>,
        StateSpaceError,
    >
    where
        P: Provider<N> + Clone + 'static,
        N: Network<BlockResponse = Block>,
    {
        let provider = self.provider.clone();
        let latest_block = self.latest_block.clone();
        let state = self.state.clone();
        let mut block_filter = self.block_filter.clone();

        let block_stream = provider.subscribe_blocks().await?.into_stream();

        Ok(Box::pin(stream! {
            tokio::pin!(block_stream);

            while let Some(block) = block_stream.next().await {
                let block_number = block.number();
                block_filter = block_filter.select(block_number);


                let logs = provider.get_logs(&block_filter).await?;

                let affected_amms = state.write().await.sync_v2(&logs, &self.factories, &self.provider).await?;
                latest_block.store(block_number, Ordering::Relaxed);

                yield Ok(affected_amms);
            }
        }))
    }
}

// TODO: Drop impl, create a checkpoint
#[derive(Debug, Default)]
pub struct StateSpaceBuilder<N, P> {
    pub provider: P,
    pub latest_block: u64,
    pub factories: Vec<Factory>,
    pub amms: Vec<AMM>,
    pub filters: Vec<PoolFilter>,
    phantom: PhantomData<N>,
    // TODO: add support for caching
}

impl<N, P> StateSpaceBuilder<N, P>
where
    N: Network,
    P: Provider<N> + Clone + 'static,
{
    pub fn new(provider: P) -> StateSpaceBuilder<N, P> {
        Self {
            provider,
            latest_block: 0,
            factories: vec![],
            amms: vec![],
            filters: vec![],
            // discovery: false,
            phantom: PhantomData,
        }
    }

    pub fn block(self, latest_block: u64) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder {
            latest_block,
            ..self
        }
    }

    pub fn with_factories(self, factories: Vec<Factory>) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder { factories, ..self }
    }

    pub fn with_amms(self, amms: Vec<AMM>) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder { amms, ..self }
    }

    pub fn with_filters(self, filters: Vec<PoolFilter>) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder { filters, ..self }
    }

    pub async fn sync(self) -> Result<StateSpaceManager<N, P>, AMMError> {
        let chain_tip = BlockId::from(self.provider.get_block_number().await?);
        let factories = self.factories.clone();
        let mut futures = FuturesUnordered::new();

        let mut filter_set = HashSet::new();
        for factory in &self.factories {
            // Add pool creation events to track new pools
            filter_set.insert(factory.discovery_event());
            
            // Add existing pool sync events
            for event in factory.pool_events() {
                filter_set.insert(event);
            }
        }

        for amm in self.amms.iter() {
            for event in amm.sync_events() {
                filter_set.insert(event);
            }
        }

        let block_filter = Filter::new().event_signature(FilterSet::from(
            filter_set.into_iter().collect::<Vec<FixedBytes<32>>>(),
        ));
        let mut amm_variants = HashMap::new();
        for amm in self.amms.into_iter() {
            amm_variants
                .entry(amm.variant())
                .or_insert_with(Vec::new)
                .push(amm);
        }

        for factory in factories {
            let provider = self.provider.clone();
            let filters = self.filters.clone();

            let extension = amm_variants.remove(&factory.variant());
            futures.push(tokio::spawn(async move {
                let mut discovered_amms = factory.discover(chain_tip, provider.clone()).await?;

                if let Some(amms) = extension {
                    discovered_amms.extend(amms);
                }

                // Apply discovery filters
                for filter in filters.iter() {
                    if filter.stage() == filters::FilterStage::Discovery {
                        let pre_filter_len = discovered_amms.len();
                        discovered_amms = filter.filter(discovered_amms).await?;

                        info!(
                            target: "state_space::sync",
                            factory = %factory.address(),
                            pre_filter_len,
                            post_filter_len = discovered_amms.len(),
                            filter = ?filter,
                            "Discovery filter"
                        );
                    }
                }

                discovered_amms = factory.sync(discovered_amms, chain_tip, provider).await?;

                // Apply sync filters
                for filter in filters.iter() {
                    if filter.stage() == filters::FilterStage::Sync {
                        let pre_filter_len = discovered_amms.len();
                        discovered_amms = filter.filter(discovered_amms).await?;

                        info!(
                            target: "state_space::sync",
                            factory = %factory.address(),
                            pre_filter_len,
                            post_filter_len = discovered_amms.len(),
                            filter = ?filter,
                            "Sync filter"
                        );
                    }
                }

                Ok::<Vec<AMM>, AMMError>(discovered_amms)
            }));
        }

        let mut state_space = StateSpace::default();
        while let Some(res) = futures.next().await {
            let synced_amms = res??;

            for amm in synced_amms {
                state_space.state.insert(amm.address(), amm);
            }
        }

        // Sync remaining AMM variants
        for (_, remaining_amms) in amm_variants.drain() {
            for mut amm in remaining_amms {
                let address = amm.address();
                amm = amm.init(chain_tip, self.provider.clone()).await?;
                state_space.state.insert(address, amm);
            }
        }

        Ok(StateSpaceManager {
            latest_block: Arc::new(AtomicU64::new(self.latest_block)),
            state: Arc::new(RwLock::new(state_space)),
            block_filter,
            provider: self.provider,
            factories: self.factories,
            phantom: PhantomData,
        })
    }

    pub async fn no_sync(self) -> Result<StateSpaceManager<N, P>, AMMError> {
        let factories = self.factories.clone();

        let mut filter_set = HashSet::new();
        for factory in &self.factories {
            // Add pool creation events to track new pools
            filter_set.insert(factory.discovery_event());
            
            // Add existing pool sync events
            for event in factory.pool_events() {
                filter_set.insert(event);
            }
        }

        for amm in self.amms.iter() {
            for event in amm.sync_events() {
                filter_set.insert(event);
            }
        }

        let block_filter = Filter::new().event_signature(FilterSet::from(
            filter_set.into_iter().collect::<Vec<FixedBytes<32>>>(),
        ));
        let mut amm_variants = HashMap::new();
        for amm in self.amms.into_iter() {
            amm_variants
                .entry(amm.variant())
                .or_insert_with(Vec::new)
                .push(amm);
        }
        let mut state_space = StateSpace::default();

        // Sync remaining AMM variants
        for (_, remaining_amms) in amm_variants.drain() {
            for mut amm in remaining_amms {
                let address = amm.address();
                state_space.state.insert(address, amm);
            }
        }

        Ok(StateSpaceManager {
            latest_block: Arc::new(AtomicU64::new(self.latest_block)),
            state: Arc::new(RwLock::new(state_space)),
            block_filter,
            provider: self.provider,
            factories: self.factories,
            phantom: PhantomData,
        })
    }
}

#[derive(Debug, Default)]
pub struct StateSpace {
    pub state: HashMap<Address, AMM>,
    pub latest_block: Arc<AtomicU64>,
    cache: StateChangeCache<CACHE_SIZE>,
}

impl StateSpace {
    pub fn get(&self, address: &Address) -> Option<&AMM> {
        self.state.get(address)
    }

    pub fn get_mut(&mut self, address: &Address) -> Option<&mut AMM> {
        self.state.get_mut(address)
    }

    pub async fn sync<N, P>(
        &mut self, 
        logs: &[Log], 
        factories: &[Factory], 
        provider: P
    ) -> Result<Vec<Address>, StateSpaceError> 
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        let latest = self.latest_block.load(Ordering::Relaxed);
        let Some(mut block_number) = logs
            .first()
            .map(|log| log.block_number.ok_or(StateSpaceError::MissingBlockNumber))
            .transpose()?
        else {
            return Ok(vec![]);
        };

        // Check if there is a reorg and unwind to state before block_number
        if latest >= block_number {
            info!(
                target: "state_space::sync",
                from = %latest,
                to = %block_number - 1,
                "Unwinding state changes"
            );

            let cached_state = self.cache.unwind_state_changes(block_number);
            for amm in cached_state {
                debug!(target: "state_space::sync", ?amm, "Reverting AMM state");
                self.state.insert(amm.address(), amm);
            }
        }

        let mut cached_amms = HashSet::new();
        let mut affected_amms = HashSet::new();
        for log in logs {
            // If the block number is updated, cache the current block state changes
            let log_block_number = log
                .block_number
                .ok_or(StateSpaceError::MissingBlockNumber)?;
            if log_block_number != block_number {
                let amms = cached_amms.drain().collect::<Vec<AMM>>();
                affected_amms.extend(amms.iter().map(|amm| amm.address()));
                let state_change = StateChange::new(amms, block_number);

                debug!(
                    target: "state_space::sync",
                    state_change = ?state_change,
                    "Caching state change"
                );

                self.cache.push(state_change);
                block_number = log_block_number;
            }

            // Check if this is a pool creation event
            if let Some(factory) = Self::is_pool_creation_event(log, factories) {
                match factory.create_pool(log.clone()) {
                    Ok(mut new_amm) => {
                        let pool_address = new_amm.address();
                        
                        // Initialize the new pool
                        match new_amm.init(alloy::eips::BlockId::Number(alloy::eips::BlockNumberOrTag::Number(log_block_number)), provider.clone()).await {
                            Ok(initialized_amm) => {
                                info!(
                                    target: "state_space::sync",
                                    pool_address = ?pool_address,
                                    factory = ?factory.address(),
                                    "New pool discovered and initialized"
                                );
                                
                                self.state.insert(pool_address, initialized_amm);
                                affected_amms.insert(pool_address);

                                println!("Inserted new pool: {:?}", pool_address);
                            }
                            Err(e) => {
                                debug!(
                                    target: "state_space::sync",
                                    pool_address = ?pool_address,
                                    error = ?e,
                                    "Failed to initialize new pool"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        debug!(
                            target: "state_space::sync",
                            error = ?e,
                            "Failed to create pool from log"
                        );
                    }
                }
            }
            // If the AMM is in the state space add the current state to cache and sync from log
            else if let Some(amm) = self.state.get_mut(&log.address()) {
                cached_amms.insert(amm.clone());
                amm.sync(log)?;

                info!(
                    target: "state_space::sync",
                    ?amm,
                    "Synced AMM"
                );
            }
        }

        if !cached_amms.is_empty() {
            let amms = cached_amms.drain().collect::<Vec<AMM>>();
            affected_amms.extend(amms.iter().map(|amm| amm.address()));
            let state_change = StateChange::new(amms, block_number);

            debug!(
                target: "state_space::sync",
                state_change = ?state_change,
                "Caching state change"
            );

            self.cache.push(state_change);
        }

        Ok(affected_amms.into_iter().collect())
    }

    /// sync_v2 with underflow error handling for UniswapV3 pools
    pub async fn sync_v2<N, P>(
        &mut self, 
        logs: &[Log], 
        factories: &[Factory], 
        provider: P
    ) -> Result<Vec<Address>, StateSpaceError> 
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        let latest = self.latest_block.load(Ordering::Relaxed);
        let Some(mut block_number) = logs
            .first()
            .map(|log| log.block_number.ok_or(StateSpaceError::MissingBlockNumber))
            .transpose()?
        else {
            return Ok(vec![]);
        };

        // Check if there is a reorg and unwind to state before block_number
        if latest >= block_number {
            info!(
                target: "state_space::sync_v2",
                from = %latest,
                to = %block_number - 1,
                "Unwinding state changes"
            );

            let cached_state = self.cache.unwind_state_changes(block_number);
            for amm in cached_state {
                debug!(target: "state_space::sync_v2", ?amm, "Reverting AMM state");
                self.state.insert(amm.address(), amm);
            }
        }

        let mut cached_amms = HashSet::new();
        let mut affected_amms = HashSet::new();
        let mut recovered_pools = Vec::new();
        
        for log in logs {
            // If the block number is updated, cache the current block state changes
            let log_block_number = log
                .block_number
                .ok_or(StateSpaceError::MissingBlockNumber)?;
            if log_block_number != block_number {
                let amms = cached_amms.drain().collect::<Vec<AMM>>();
                affected_amms.extend(amms.iter().map(|amm| amm.address()));
                let state_change = StateChange::new(amms, block_number);

                debug!(
                    target: "state_space::sync_v2",
                    state_change = ?state_change,
                    "Caching state change"
                );

                self.cache.push(state_change);
                block_number = log_block_number;
            }

            // Check if this is a pool creation event
            if let Some(factory) = Self::is_pool_creation_event(log, factories) {
                match factory.create_pool(log.clone()) {
                    Ok(new_amm) => {
                        let pool_address = new_amm.address();
                        
                        // Initialize the new pool
                        match new_amm.init(alloy::eips::BlockId::Number(alloy::eips::BlockNumberOrTag::Number(log_block_number)), provider.clone()).await {
                            Ok(initialized_amm) => {
                                info!(
                                    target: "state_space::sync_v2",
                                    pool_address = ?pool_address,
                                    factory = ?factory.address(),
                                    "New pool discovered and initialized"
                                );
                                
                                self.state.insert(pool_address, initialized_amm);
                                affected_amms.insert(pool_address);

                                println!("Inserted new pool: {:?}", pool_address);
                            }
                            Err(e) => {
                                debug!(
                                    target: "state_space::sync_v2",
                                    pool_address = ?pool_address,
                                    error = ?e,
                                    "Failed to initialize new pool"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        debug!(
                            target: "state_space::sync_v2",
                            error = ?e,
                            "Failed to create pool from log"
                        );
                    }
                }
            }
            // If the AMM is in the state space add the current state to cache and sync from log
            else if let Some(amm) = self.state.get_mut(&log.address()) {
                cached_amms.insert(amm.clone());
                
                // Try to sync with underflow protection using panic catching
                let sync_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    amm.sync(log)
                }));
                
                match sync_result {
                    Ok(Ok(_)) => {
                        info!(
                            target: "state_space::sync_v2",
                            ?amm,
                            "Synced AMM successfully"
                        );
                    }
                    Ok(Err(e)) => {
                        // Regular error from sync method
                        return Err(e.into());
                    }
                    Err(panic_info) => {
                        // Panic occurred during sync
                        let panic_message = if let Some(s) = panic_info.downcast_ref::<String>() {
                            s.clone()
                        } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                            s.to_string()
                        } else {
                            "Unknown panic during sync".to_string()
                        };
                        
                        warn!(
                            target: "state_space::sync_v2",
                            pool_address = ?log.address(),
                            panic_msg = %panic_message,
                            "Panic caught during AMM sync"
                        );
                        
                        // Check if this is an underflow panic
                        if panic_message.contains("attempt to subtract with overflow") || 
                           panic_message.contains("LiquidityUnderflow") {
                            warn!(
                                target: "state_space::sync_v2",
                                pool_address = ?log.address(),
                                "UniswapV3 underflow panic detected, resetting pool to initial state"
                            );
                            
                            // Reset the pool to a clean state 
                            match self.reset_pool_to_initial_state(log.address(), log_block_number, &provider).await {
                                Ok(reset_amm) => {
                                    info!(
                                        target: "state_space::sync_v2", 
                                        pool_address = ?log.address(),
                                        "Successfully reset pool to initial state after panic"
                                    );
                                    self.state.insert(log.address(), reset_amm);
                                    recovered_pools.push(log.address());
                                    affected_amms.insert(log.address());
                                }
                                Err(reset_err) => {
                                    warn!(
                                        target: "state_space::sync_v2",
                                        pool_address = ?log.address(),
                                        error = ?reset_err,
                                        "Failed to reset pool after panic, removing from state"
                                    );
                                    self.state.remove(&log.address());
                                }
                            }
                        } else {
                            // Non-underflow panic, re-panic to maintain original behavior
                            warn!(
                                target: "state_space::sync_v2",
                                pool_address = ?log.address(),
                                panic_msg = %panic_message,
                                "Non-underflow panic during sync, removing pool from state"
                            );
                            self.state.remove(&log.address());
                        }
                    }
                }
            }
        }

        if !cached_amms.is_empty() {
            let amms = cached_amms.drain().collect::<Vec<AMM>>();
            affected_amms.extend(amms.iter().map(|amm| amm.address()));
            let state_change = StateChange::new(amms, block_number);

            debug!(
                target: "state_space::sync_v2",
                state_change = ?state_change,
                "Caching state change"
            );

            self.cache.push(state_change);
        }

        if !recovered_pools.is_empty() {
            info!(
                target: "state_space::sync_v2",
                recovered_count = recovered_pools.len(),
                "Recovered pools from underflow errors"
            );
        }

        Ok(affected_amms.into_iter().collect())
    }

    /// Reset a pool to its initial state at a specific block
    async fn reset_pool_to_initial_state<N, P>(
        &self,
        pool_address: Address,
        block_number: u64,
        provider: &P,
    ) -> Result<AMM, StateSpaceError>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        // Create a fresh pool instance based on the pool type
        use crate::amms::amm::AMM;
        
        // Try to create a fresh UniswapV3 pool since that's where underflow occurs
        let fresh_pool = crate::amms::uniswap_v3::UniswapV3Pool::new(pool_address, Address::default());
        
        // Initialize it at the specific block to get clean state
        match fresh_pool.init(
            alloy::eips::BlockId::Number(alloy::eips::BlockNumberOrTag::Number(block_number)), 
            provider.clone()
        ).await {
            Ok(initialized_pool) => {
                info!(
                    target: "state_space::sync_v2",
                    pool_address = ?pool_address,
                    block = block_number,
                    "Successfully created fresh pool state"
                );
                Ok(AMM::UniswapV3Pool(initialized_pool))
            }
            Err(e) => {
                warn!(
                    target: "state_space::sync_v2",
                    pool_address = ?pool_address,
                    block = block_number,
                    error = ?e,
                    "Failed to initialize fresh pool"
                );
                Err(StateSpaceError::AMMError(e))
            }
        }
    }

    // Helper method to check if a log is a pool creation event
    fn is_pool_creation_event<'a>(log: &Log, factories: &'a [Factory]) -> Option<&'a Factory> {
        let signature = log.topics()[0];
        let log_address = log.address();
        
        factories.iter().find(|factory| {
            factory.address() == log_address && factory.discovery_event() == signature
        })
    }
}

#[macro_export]
macro_rules! sync {
    // Sync factories with provider
    ($factories:expr, $provider:expr) => {{
        StateSpaceBuilder::new($provider.clone())
            .with_factories($factories)
            .sync()
            .await?
    }};

    // Sync factories with filters
    ($factories:expr, $filters:expr, $provider:expr) => {{
        StateSpaceBuilder::new($provider.clone())
            .with_factories($factories)
            .with_filters($filters)
            .sync()
            .await?
    }};

    ($factories:expr, $amms:expr, $filters:expr, $provider:expr) => {{
        StateSpaceBuilder::new($provider.clone())
            .with_factories($factories)
            .with_amms($amms)
            .with_filters($filters)
            .sync()
            .await?
    }};
}
