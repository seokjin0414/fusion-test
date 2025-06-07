# Direct Insert to S3Tables Iceberg Table with Rust DataFusion (Experimental)

This project demonstrates a test implementation for **directly inserting data into an Iceberg table using Rust’s DataFusion and Iceberg libraries** with the **AWS S3Tables Catalog**.

---

## Purpose

- Prototype and validate batch data insertion to S3Tables Iceberg tables using DataFusion
- End-to-end test: loading CSV data, converting to Arrow RecordBatch, and committing to Iceberg via AWS S3TablesCatalog

---

## Warnings (⚠️ Experimental)

- This code is for **experimental/PoC/testing purposes only**.
- When inserting into an Iceberg table via S3TablesCatalog and DataFusion, you may encounter:
    - **Iceberg snapshot time conflict errors**
    - **Temporary unavailability of the table**
    - **Manual recovery required due to snapshot/version conflicts**
- These issues may occur repeatedly, so **do not use in production without thorough validation**.
- Note that using S3TablesCatalog differs from legacy S3/Hive catalogs in both configuration and operational behavior.

---

## Contact

If you have questions, need further support, or want to collaborate,  
please reach out via:

- **Email:** sars21@hanmail.net  
- **LinkedIn:** [https://www.linkedin.com/in/seokjin-shin/](https://www.linkedin.com/in/seokjin-shin/)

Feel free to connect!
