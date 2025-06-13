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
use std::sync::Arc;
use std::str::FromStr;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let rpc_endpoint = std::env::var("ETHEREUM_PROVIDER")?;
    let client = ClientBuilder::default()
        .layer(ThrottleLayer::new(50))
        .layer(RetryBackoffLayer::new(5, 200, 330))
        .http(rpc_endpoint.parse()?);

    let provider = Arc::new(ProviderBuilder::new().connect_client(client));

    let pool = UniswapV2Pool::new(address!("0xd64c68f7a93455b2064c0b4ae7bf65db3bd9ce57"), address!("0xDa12F450580A4cc485C3b501BAB7b0B3cbc3B31B"), 300)
        .init(BlockId::latest(), provider)
        .await?;
    // let pool = UniswapV2Pool::new(address!("0xc2293ced8aef390ea518413b94d8cbdaf4b9690b"), address!("0x0c278010bf21bae386b6ce4440535970a4ea5238"), 300)
    //     .init(BlockId::latest(), provider)
    //     .await?;


    
    // let pool = UniswapV3VariantPool::new(address!("0x11f6ad647c331bcf927ea9df6bd0f777e0e25fec"), address!("0x2E08F5Ff603E4343864B14599CAeDb19918BDCaF"))
    //     .init(BlockId::latest(), provider)
    //     .await?;

    // let pool = UniswapV3Pool::new(address!("0x56abfaf40f5b7464e9cc8cff1af13863d6914508"), address!("0x2E08F5Ff603E4343864B14599CAeDb19918BDCaF"))
    //     .init(BlockId::latest(), provider)
    //     .await?;

    // Note that the token out does not need to be specified when
    // simulating a swap for pools with only two tokens.
    let amount_out = pool.simulate_swap(
        pool.token_b.address,
        Address::default(),
        U256::from_str("2901260121115161600").unwrap(),
    )?;
    println!("Amount out: {:?}", amount_out);

    println!("Fee: {:?}", pool.fee);

    Ok(())
}
