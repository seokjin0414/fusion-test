use std::{
    sync::Arc,
    env::var,
};
use anyhow::{anyhow, Result};
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region, SdkConfig};
// use aws_sdk_s3tables;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_rust::spec::identifier::Identifier;
use iceberg_s3tables_catalog::S3TablesCatalog;
use iceberg_rust::table::Table;


async fn init_table() -> Result<Arc<Table>> {
    let default_region = var("AWS_DEFAULT_REGION")
        .map_err(|e| anyhow!("Could not find var AWS_DEFAULT_REGION: {:?}", e))?;
    let region_provider = RegionProviderChain::default_provider()
        .or_else(Region::new(default_region));
    let sdk_config: SdkConfig = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    
    // let client = aws_sdk_s3tables::Client::new(&sdk_config);
    
    let catalog_arn = var("S3_TABLES_CATALOG_ARN")
        .map_err(|e| anyhow!("Could not find var S3_TABLES_CATALOG_ARN: {:?}", e))?;
    let catalog = 
        S3TablesCatalog::new(&sdk_config, &catalog_arn, ObjectStoreBuilder::s3())?;
    let arc_catalog: Arc<dyn Catalog> = Arc::new(catalog);

    // 테이블 로딩
    let namespace = var("NAMESPACE")
        .map_err(|e| anyhow!("Could not find var NAMESPACE: {:?}", e))?;
    let test_table = var("TEST_TABLE")
        .map_err(|e| anyhow!("Could not find var TEST_TABLE: {:?}", e))?;
    let identifier=  Identifier::new(&[namespace], &test_table);
    let tabular = arc_catalog
        .load_tabular(&identifier)
        .await?;

    match tabular {
        Tabular::Table(t) => Ok(Arc::new(t.clone())),
        _ => Err(anyhow!("Loaded tabular is not a Table")),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Hello, world!");

    
    
    
    
    
    Ok(())
}