use alloy::eips::BlockId;
use alloy::primitives::{Address, U256};
use alloy::transports::layers::ThrottleLayer;
use alloy::{
    primitives::address, providers::ProviderBuilder, rpc::client::ClientBuilder,
    transports::layers::RetryBackoffLayer,
};
use amms::amms::amm::AutomatedMarketMaker;
use amms::amms::uniswap_v3::UniswapV3Pool;
use amms::amms::uniswap_v2::UniswapV2Pool;
use amms::amms::uniswap_v2_variant::UniswapV2Pool as UniswapV2VariantPool;
use amms::amms::uniswap_v3_variant::UniswapV3Pool as UniswapV3VariantPool;
use amms::amms::uniswap_v2_variant::IUniswapV2Pair::IUniswapV2PairInstance;
use std::sync::Arc;
use std::str::FromStr;

async fn check_pool_stability<N, P>(provider: P) -> eyre::Result<()>
where
    N: alloy::network::Network,
    P: alloy::providers::Provider<N> + Clone,
{
    // List of pool addresses to check
    let pool_addresses = vec![
        "0x6c9408de11f3ad1388de1c4fe011520c757d93b5",
        "0x1a497bcf84c57e7647391a377f9849dee98111ca",
        "0xf9280daaf6215ea6c69ea81c66293a5c87f224fd",
        "0xff8b8f31f4027c245a5e521fab0d7c50da5cddfe",
        "0x5c06758c2d322dadb9b2e2e708f49cd5fa75877c",
        "0x50effa7395ef2daa52ec25f6dd0e2030f529591e",
        "0x7f4ed92f7b7d121eae6234a3d2cc96c2edf192c0",
        "0xeddd36cabd1d6bce2e9f6d40162943b2346c7e7b",
        "0xdbc9cf4e8d3741112ebc2970152e8d9b9684989a",
        "0x0ecf8720508dc2f81e3fc694bfb7a8a1052d889f",
        "0xeb01c8e4f5f4527516a676cea7d8603f22789ec8",
        "0x0c544f4f1078190f6d76badee0fea0a8236ee682",
        "0x9b14b3e8eaeb2f2791c48c22e4ae9dcb21908c60",
        "0x6d2c7bc0598af494c5bbf9ce25e02faa944ed673",
        "0x0872fb0ec90001a0e770e93ed54ce9cfc2b27482",
        "0x3c1f6d843af17d0d87c6924b633cb500047b67bf",
        "0x8494f537dd2a6b2bb1cf60632473ed3473204b56",
        "0x37e019e0981f2a4b40ae9609d062053f2b5f072b",
        "0x3479ace74c60099c0ee03fb1f4d7fcb734eab64b",
        "0xb44a07a32ea7b18642e93fd73d27d7021ca4d653",
        "0x7f5d1f93232702a951759a47a27de08d35e9bbe8",
        "0x31c4f84e382a1d84ca70dc7a5e1c0cf722cbe727",
        "0x85f92601718e406ff5ef39bd71eafd02468e5cfd",
        "0x3eb5b7bb006a3430ce5988d47d3e20f0834f9d01",
        "0x340e025f6f9a49aca4499443dc4486244866852d",
        "0x4f4f955d22b3a7fb489941df46b656af945804e5",
        "0xdfc16b0e5cc1e2395887c7e912aa026ca0742cb5",
        "0x3a9a876da842195b6e9166a6ba9d261d188daed4",
        "0xd64c68f7a93455b2064c0b4ae7bf65db3bd9ce57",
        "0x69616a0b92eee0cba6eec95f9a9dfa036697bc28",
        "0xabe999189509ee6a5f016e123b5c56a23f5e3a96",
        "0x3aa10f15f88c1e2fd78d5ae61bb7cb6629e92ef3",
        "0x8c8a92242e17531b20811af758bff407d840e0d1",
        "0x75fe0d9c582c66bbe2f783bdb25fc8e37e706500",
        "0xeefe3171d46d16682a083ac98560421c6131e20f",
        "0x825d251096ed504d6728eb73835d0213fc85abf4",
        "0x6c1c41dc697787ecce649025be0dd476ff940c9e",
        "0x7363d66b3dd3f963da905e1313dd89f50c7cabfd",
        "0xf75f0ab2d029864f71a81d47085301da700b59ee",
        "0x5e8066f887caf515bf899880d76dec908b742996",
        "0x37082651632939d04d700329c76031f4bfe41dff",
        "0x467dd5403417d8e4c390f64b4c097026879c9c59",
        "0x8bd750cb647e31ee71614204e6ddd284acd2419c",
        "0x9ae49027d6d969fee6534fb8245a68bc781b08ed",
        "0x1db54918fc8177e854781b71ebbaeb91919d5ed9",
        "0x48a768ea683a6d203ff05c129e748b69d5e8b678",
        "0x2120c69ec8d289b959baf2c7fbec2d1a56c00384",
        "0x12eb84b4d09a0a28ae48eee5f6df59944399027f",
        "0x11f5711da425d38b459388032744b1ceffcb682f",
        "0x167694a7f08a88d4ccc531215cf3883081cc9e0a",
        "0xa19c3b27e3522b83d65b50fc3df6c238d5dace2b",
        "0xce6be3f8b0281a1d7cccb140485ff51d4d6b212b",
    ];

    println!("\n🔍 Checking stability of UniswapV2VariantPool pools (via on-chain calls):");
    println!("{:=<80}", "");

    let mut stable_count = 0;
    let mut not_stable_count = 0;
    let mut error_count = 0;

    for (index, pool_address_str) in pool_addresses.iter().enumerate() {
        let pool_address = Address::from_str(pool_address_str)?;
        
        // Create contract instance for direct on-chain call
        let pair_contract = IUniswapV2PairInstance::new(pool_address, provider.clone());
        
        // Call the stable() function directly on-chain
        let stable_result = pair_contract.stable().call().block(BlockId::latest()).await;

        match stable_result {
            Ok(is_stable) => {
                let stable_status = if is_stable { 
                    stable_count += 1;
                    "✓ STABLE" 
                } else { 
                    not_stable_count += 1;
                    "✗ NOT STABLE" 
                };
                println!(
                    "{:2}. {} | {} (on-chain call)",
                    index + 1,
                    pool_address_str,
                    stable_status
                );
            },
            Err(e) => {
                error_count += 1;
                println!(
                    "{:2}. {} | ❌ ERROR: {}",
                    index + 1,
                    pool_address_str,
                    e
                );
            }
        }
    }

    println!("{:=<80}", "");
    println!("📊 Summary (Direct On-Chain Calls):");
    println!("   ✓ Stable pools: {}", stable_count);
    println!("   ✗ Not stable pools: {}", not_stable_count);
    println!("   ❌ Errors: {}", error_count);
    println!("   📋 Total checked: {}", pool_addresses.len());
    println!();

    Ok(())
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let rpc_endpoint = std::env::var("ETHEREUM_PROVIDER")?;
    let client = ClientBuilder::default()
        .layer(ThrottleLayer::new(50))
        .layer(RetryBackoffLayer::new(5, 200, 330))
        .http(rpc_endpoint.parse()?);

    let provider = Arc::new(ProviderBuilder::new().connect_client(client));

    // Original swap simulation code
    println!("🔄 Running original swap simulation...");
    // let pool = UniswapV2VariantPool::new(address!("0x3c1f6d843af17d0d87c6924b633cb500047b67bf"), address!("0xDa12F450580A4cc485C3b501BAB7b0B3cbc3B31B"), 300, false)
    //     .init(BlockId::latest(), provider.clone())
    //     .await?;
    // let pool = UniswapV2Pool::new(address!("0xc2293ced8aef390ea518413b94d8cbdaf4b9690b"), address!("0x0c278010bf21bae386b6ce4440535970a4ea5238"), 300)
    //     .init(BlockId::latest(), provider)
    //     .await?;

    
    // let pool = UniswapV3VariantPool::new(address!("0xa5b155d7ccbb6eed8f50f53131c96465317e0ecb"), address!("0x2E08F5Ff603E4343864B14599CAeDb19918BDCaF"))
    //     .init(BlockId::latest(), provider.clone())
    //     .await?;

    let pool = UniswapV3Pool::new(address!("0xc64dd384c6c76526b1f721d61077e0e981a3a53f"), address!("0x2E08F5Ff603E4343864B14599CAeDb19918BDCaF"))
        .init(BlockId::latest(), provider)
        .await?;

    // Note that the token out does not need to be specified when
    // simulating a swap for pools with only two tokens.
    let amount_out = pool.simulate_swap(
        pool.token_a.address,
        Address::default(),
        U256::from_str("100000000000").unwrap(),
    )?;
    println!("Amount out: {:?}", amount_out);
    println!("Fee: {:?}", pool.fee);
    println!("bit map: {:?}", pool.tick_bitmap);
    println!("ticks: {:?}", pool.ticks);

    // New pool stability checking functionality
    // check_pool_stability(provider.clone()).await?;

    Ok(())
}
