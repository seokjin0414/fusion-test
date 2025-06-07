# Direct Insert to S3Tables Iceberg Table with Rust DataFusion (Experimental)

This project demonstrates an experimental implementation for directly inserting data into an Apache Iceberg table using Rust’s DataFusion and Iceberg libraries with the AWS S3Tables Catalog.

> **Note:**  
> As of now, the S3TablesCatalog is not officially supported for direct batch insert outside the Spark ecosystem.  
> Many features are missing or incomplete, so this project is a low-level PoC/test for direct Rust-based data ingestion.

---

## Purpose

- Prototype and validate **batch data insertion to S3Tables-backed Iceberg tables using DataFusion**, without relying on Spark.
- **Goal:**  
  Directly insert data into an Iceberg table configured with  
  `"spark.sql.catalog.my_catalog.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog"`  
  **(i.e., S3Tables, not legacy S3 or Hive catalogs)** —  
  realizing end-to-end Rust-based ingestion:  
  1. Load CSV data
  2. Convert to Arrow RecordBatch
  3. Register as DataFusion DataFrame
  4. Commit data to Iceberg via S3TablesCatalog

---

## Project Structure & Data Flow

### 1. Data Preparation: `RecordBatch` Creation

- CSV data is read into a vector of `DataSet` structs (see `DataSet::test_set()`).
- The function `data_set_to_record_batch` converts the vector of Rust structs into a **RecordBatch**  
  (an Apache Arrow in-memory columnar representation), suitable for analytics workloads.

### 2. RecordBatch → DataFrame:  
The `record_batch_to_dataframe` function registers the `RecordBatch` into an in-memory table within a DataFusion `SessionContext`,  
then loads it as a **DataFrame** for further transformation or writing.

### 3. S3Tables Iceberg Table Initialization: `init_table`

- `init_table()` is responsible for:
  - Initializing the AWS SDK and region.
  - Loading the S3TablesCatalog (using the ARN from environment variables).
  - Resolving the Iceberg table identifier (`NAMESPACE` and `TEST_TABLE` from `.env`).
  - Loading the corresponding table via the catalog.
- This encapsulates all steps needed to **connect to and operate on a target S3Tables-backed Iceberg table**.

### 4. Data Commit

- The DataFrame (registered with DataFusion) is written to the Iceberg table via `.write_table()`.

---

## Example Flow (Key Functions)

```rust
// 1. Prepare test data
let test_data_set = DataSet::test_set()?;

// 2. Convert to Arrow RecordBatch
let batch = data_set_to_record_batch(&test_data_set)?;

// 3. Create a new DataFusion session
let ctx = SessionContext::new();

// 4. Load (init) the S3Tables Iceberg table
let tabular = init_table().await?;
let df_table = Arc::new(DataFusionTable::from(tabular));
ctx.register_table("test", df_table)?;

// 5. Register RecordBatch as DataFrame
let df = record_batch_to_dataframe(batch, &ctx).await?;

// 6. Commit data to Iceberg table
let commit_result = df.write_table("test", DataFrameWriteOptions::default()).await?;
```

---

## Warnings (⚠️ Experimental)

This code is for **experimental, proof-of-concept, or testing purposes only**.

When inserting into an Iceberg table via S3TablesCatalog and DataFusion, you may encounter:

- Iceberg snapshot time conflict errors
- Temporary unavailability of the table
- Manual recovery required due to snapshot/version conflicts

These issues may occur repeatedly, so **do not use in production** without thorough validation.

> **Note:**  
> Using S3TablesCatalog differs from legacy S3/Hive catalogs in both configuration and operational behavior.

---

## Contact

If you have questions, need further support, or want to collaborate,  
please reach out via:

- **Email:** sars21@hanmail.net  
- **LinkedIn:** [https://www.linkedin.com/in/seokjin-shin/](https://www.linkedin.com/in/seokjin-shin/)

Feel free to connect!
