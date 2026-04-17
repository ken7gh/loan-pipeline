-- =========================================================
-- Apply basic cleansing and type casting.
-- TRY_* functions prevent transformation failures on bad data
-- =========================================================
CREATE OR REPLACE VIEW VW_LOAN_MONTHLY_CLEAN AS
SELECT
    NULLIF(TRIM(reporting_month), '')                              AS reporting_month,
    NULLIF(TRIM(loan_id), '')                                      AS loan_id,
    NULLIF(TRIM(servicer_name), '')                                AS servicer_name,
    TRY_TO_DECIMAL(NULLIF(TRIM(balance), ''), 18, 2)               AS balance,
    TRY_TO_DECIMAL(NULLIF(TRIM(interest_rate), ''), 8, 4)          AS interest_rate,
    UPPER(NULLIF(TRIM(state), ''))                                 AS state,
    TRY_TO_TIMESTAMP_NTZ(NULLIF(TRIM(updated_at), ''))             AS updated_at,
    file_name,
    load_time,
    run_id
FROM RAW_LOAN_MONTHLY;