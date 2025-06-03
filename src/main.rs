use std::sync::Arc;
use anyhow::{anyhow, Result};
use aws_config::{
    meta::region::RegionProviderChain,
    BehaviorVersion, 
    SdkConfig
};
use chrono::{DateTime, Utc};
use datafusion::arrow::array::{Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_iceberg::DataFusionTable;
use dotenvy::var;
// use aws_sdk_s3tables;
use iceberg_rust::catalog::{tabular, Catalog};
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_rust::spec::identifier::Identifier;
use iceberg_s3tables_catalog::S3TablesCatalog;
use serde_derive::Deserialize;
use uuid::Uuid;

#[derive(Deserialize, Debug, Clone)]
struct DataSet {
    pub measurement_point_id: Uuid,
    pub value: Option<f64>,
    pub recorded_at: DateTime<Utc>,
}

impl DataSet {
    pub fn test_set() -> Result<Vec<DataSet>> {
        let mut rdr = csv::Reader::from_path("src/test_set.csv")
            .map_err(|e| anyhow!("fail to read test_set.csv: {:?}", e))?;

        let mut vec: Vec<DataSet> = Vec::new();
        for result in rdr.deserialize() {
            let record: DataSet = result
                .map_err(|e| anyhow!("fail to deserialize: {:?}", e))?;
            vec.push(record);
        }

        Ok(vec)
    }
}

fn data_set_to_record_batch(data: &[DataSet]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("measurement_point_id", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
        // Field::new("recorded_at", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
    ]));

    let id_array = StringArray::from_iter(
        data.iter().map(|d|  Some(d.measurement_point_id.to_string()))
    );
    let value_array = Float64Array::from_iter(
        data.iter().map(|d| d.value)
    );
    let recorded_array = TimestampNanosecondArray::from_iter(
        data.iter().map(|d| d.recorded_at.timestamp_nanos_opt())
    );

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(value_array),
            // Arc::new(recorded_array),
        ],
    ).map_err(|e| anyhow!("fail to create record batch: {:?}", e))?;
    Ok(batch)
}

async fn record_batch_to_dataframe(
    batch: RecordBatch,
    ctx: &SessionContext,
) -> Result<DataFrame> {
    let schema = batch.schema();
    let table = MemTable::try_new(schema, vec![vec![batch]])?;

    ctx.register_table("tmp_mem_table", Arc::new(table))
        .map_err(|e| anyhow!("fail to register table: {:?}", e))?;

    let df = ctx.table("tmp_mem_table")
        .await
        .map_err(|e| anyhow!("fail to get table: {:?}", e))?;

    Ok(df)
}

async fn init_table() -> Result<Tabular> {
    // AWS config
    let region_provider = RegionProviderChain::default_provider()
        .or_else("ap-northeast-2");
    
    let sdk_config: SdkConfig = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    
    // let client = aws_sdk_s3tables::Client::new(&sdk_config);
    
    // Catalog
    let catalog_arn = var("S3_TABLES_CATALOG_ARN")
        .map_err(|e| anyhow!("fail to get S3_TABLES_CATALOG_ARN: {:?}", e))?;

    let catalog = S3TablesCatalog::new(&sdk_config, &catalog_arn, ObjectStoreBuilder::s3())?;
    let arc_catalog: Arc<dyn Catalog> = Arc::new(catalog);

    // Table
    let namespace = var("NAMESPACE")
        .map_err(|e| anyhow!("fail to get NAMESPACE: {:?}", e))?;
    let test_table = var("TEST_TABLE")
        .map_err(|e| anyhow!("fail to get TEST_TABLE: {:?}", e))?;
    let identifier=  Identifier::new(&[namespace], &test_table);
    let tabular = arc_catalog.load_tabular(&identifier).await?;

    Ok(tabular)
}


#[tokio::main]
async fn main() -> Result<()> {
    let tabular = init_table().await?;
    let df_table = Arc::new(DataFusionTable::from(tabular));

    let test_data_set = DataSet::test_set()?;
    let batch = data_set_to_record_batch(&test_data_set)?;
    
    let ctx = SessionContext::new();
    ctx.register_table("test", df_table)?;

    let df = record_batch_to_dataframe(batch, &ctx).await?;
    println!("#### 3");
    
    
    df.write_table("test", DataFrameWriteOptions::default()).await
        .map_err(|e| anyhow!("fail to write table: {:?}", e))?;
    println!("#### 4");

    Ok(())
}