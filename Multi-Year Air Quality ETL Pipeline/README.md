## Async ETL Pipeline: API to BigQuery (Incremental Load with Staging)

This project implements an asynchronous ETL pipeline that:
- Extracts paginated data from a public API
- Transforms and engineers additional metrics
- Loads data into Google BigQuery using staging table logic
- Performs incremental loading with deduplication
