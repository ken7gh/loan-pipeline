# Snowflake Loan Monthly Pipeline

## Steps

1. Run the DDL scripts once to create schemas, tables, views, sequences and one time DMLs.
2. Run the ingestion script (**10_load_raw_loan_monthly.sql**) to load the file from the external stage into `RAW.RAW_LOAN_MONTHLY` and write the run details to `RAW.LOAD_HISTORY`.
3. Run the merge script (**20_merge_target_loan_monthly.sql**) to upsert the current batch into `TARGET.TARGET_LOAN_MONTHLY`.
4. Run the QC script (**30_qc_checks.sql**) to write validation results into `TARGET.QC_RESULTS`.

## Operational Notes

### How do you prevent duplicate loading if the same file arrives twice?

The ingestion step checks `RAW.LOAD_HISTORY` before loading. If the same file was already processed successfully, the load is skipped. As a second layer of protection, the `MERGE` step is idempotent, so rerunning the same batch does not create duplicate target rows.

### How do you handle schema evolution if the file adds new columns?

The RAW layer stores source fields separately from the business-ready target layer, so schema changes can be handled in stages. I would add the new column to the RAW table first, then update the clean view to cast and expose it, and finally add it to the target table only when it is needed downstream.

### What would you do if COPY INTO partially loads a file and then fails?

I use ON_ERROR = 'ABORT_STATEMENT' so COPY INTO fails as a unit. If an error occurs, Snowflake aborts the load rather than partially continuing, and I record the failed run in RAW.LOAD_HISTORY so it can be retried safely.

### What warehouse sizing / file sizing guidance would you give for performance?

For this monthly pipeline, I would start with a small or medium warehouse and scale only if load times require it. Snowflake notes that for data loading, warehouse size should match the number of files and the amount of data in each file, and that simply increasing warehouse size does not always improve load performance because loading is heavily influenced by file count and file size.
For file sizing, I would use compressed files and avoid many tiny files. Snowflake recommends aiming for files roughly 100–250 MB compressed for a strong cost/performance balance, with at least 10 MB on average to avoid excessive per-file overhead.
