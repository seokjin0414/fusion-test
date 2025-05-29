use std::collections::HashMap;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use aws_config::{
    meta::region::RegionProviderChain,
    BehaviorVersion, 
    SdkConfig
};
// use aws_sdk_s3tables;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_rust::spec::identifier::Identifier;
use iceberg_s3tables_catalog::S3TablesCatalog;

async fn init_table() -> Result<Tabular> {
    // AWS config
    let region_provider = RegionProviderChain::default_provider()
        .or_else("ap-northeast-2");
    
    let sdk_config: SdkConfig = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    
    // let client = aws_sdk_s3tables::Client::new(&sdk_config);
    
    // Catalog 생성
    let catalog_arn = "";
    let catalog = S3TablesCatalog::new(&sdk_config, catalog_arn, ObjectStoreBuilder::s3())?;
    let arc_catalog: Arc<dyn Catalog> = Arc::new(catalog);

    // 테이블 로딩
    let table_identifier=  Identifier::new(&["v1".to_string()], "test");
    let table = arc_catalog.load_tabular(&table_identifier).await?;
    
    Ok(table)
}


#[tokio::main]
async fn main() -> Result<()> {
    println!("Hello, world!");

    Ok(())
}