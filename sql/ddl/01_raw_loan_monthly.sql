-- =========================================================
-- Store source columns as strings to preserve raw input.
-- Metadata columns added auditing and reruns.
-- =========================================================
CREATE OR REPLACE TABLE RAW.RAW_LOAN_MONTHLY (
    reporting_month   STRING COMMENT 'Field from LOAN_MONTHLY_YYYYMM.gz file',
    loan_id           STRING COMMENT 'Field from LOAN_MONTHLY_YYYYMM.gz file',
    servicer_name     STRING COMMENT 'Field from LOAN_MONTHLY_YYYYMM.gz file',
    balance           STRING COMMENT 'Field from LOAN_MONTHLY_YYYYMM.gz file',
    interest_rate     STRING COMMENT 'Field from LOAN_MONTHLY_YYYYMM.gz file',
    state             STRING COMMENT 'Field from LOAN_MONTHLY_YYYYMM.gz file',
    updated_at        STRING COMMENT 'Field from LOAN_MONTHLY_YYYYMM.gz file',
    file_name         STRING COMMENT 'Metadata File name',
    load_time         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Metadata Load time',
    run_id            NUMBER COMMENT 'Metadata RUN ID'
) COMMENT = 'Historical table for monthly data from LOAN_MONTHLY_YYYYMM.gz files';
