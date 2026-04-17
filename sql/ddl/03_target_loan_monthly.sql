-- =========================================================
-- Final analytics-ready table with typed fields for monthly loan data.
-- Includes operational metadata for traceability.
-- Business key: loan_id + reporting_month
-- =========================================================
CREATE OR REPLACE TABLE TARGET.TARGET_LOAN_MONTHLY (
    reporting_month   STRING        NOT NULL,
    loan_id           STRING        NOT NULL,
    servicer_name     STRING,
    balance           NUMBER(18,2),
    interest_rate     NUMBER(8,4),
    state             STRING,
    updated_at        TIMESTAMP_NTZ,
    created_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    last_modified_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    last_run_id       NUMBER,
    last_file_name    STRING
    ,CONSTRAINT PK_TARGET_LOAN_MONTHLY PRIMARY KEY (loan_id, reporting_month)
) COMMENT = 'Current data table for monthly loan data';
