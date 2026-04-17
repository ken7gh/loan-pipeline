-- =========================================================
-- TASK 3: Incremental Merge into TARGET
-- =========================================================
MERGE INTO TARGET.TARGET_LOAN_MONTHLY AS tgt
USING (
    WITH CURRENT_RUN AS (
        SELECT
            reporting_month,
            loan_id,
            servicer_name,
            balance,
            interest_rate,
            state,
            updated_at,
            file_name,
            load_time,
            run_id
        FROM RAW.VW_LOAN_MONTHLY_CLEAN
        WHERE run_id = (SELECT value 
		                  FROM RAW.PIPELINE_CONTROL_STATE 
						 WHERE process = "load_raw_loan_monthly"
                           AND key = "sequence_id")
          AND loan_id IS NOT NULL
          AND reporting_month IS NOT NULL
    ),
    DEDUPED AS (
        SELECT
            reporting_month,
            loan_id,
            servicer_name,
            balance,
            interest_rate,
            state,
            updated_at,
            file_name,
            load_time,
            run_id
        FROM CURRENT_RUN
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY loan_id, reporting_month
            ORDER BY updated_at DESC NULLS LAST, load_time DESC
        ) = 1
    )
    SELECT *
    FROM DEDUPED
) AS src
ON tgt.loan_id = src.loan_id
AND tgt.reporting_month = src.reporting_month

WHEN MATCHED AND (
       NVL(tgt.servicer_name, '##NULL##') <> NVL(src.servicer_name, '##NULL##')
    OR NVL(tgt.balance, -999999999.99) <> NVL(src.balance, -999999999.99)
    OR NVL(tgt.interest_rate, -999999.9999) <> NVL(src.interest_rate, -999999.9999)
    OR NVL(tgt.state, '##NULL##') <> NVL(src.state, '##NULL##')
    OR NVL(tgt.updated_at, '1900-01-01 00:00:00'::TIMESTAMP_NTZ)
       <> NVL(src.updated_at, '1900-01-01 00:00:00'::TIMESTAMP_NTZ)
)
THEN UPDATE SET
    servicer_name    = src.servicer_name,
    balance          = src.balance,
    interest_rate    = src.interest_rate,
    state            = src.state,
    updated_at       = src.updated_at,
    last_modified_at = CURRENT_TIMESTAMP(),
    last_run_id      = src.run_id,
    last_file_name   = src.file_name

WHEN NOT MATCHED THEN
    INSERT (
        reporting_month,
        loan_id,
        servicer_name,
        balance,
        interest_rate,
        state,
        updated_at,
        created_at,
        last_modified_at,
        last_run_id,
        last_file_name
    )
    VALUES (
        src.reporting_month,
        src.loan_id,
        src.servicer_name,
        src.balance,
        src.interest_rate,
        src.state,
        src.updated_at,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        src.run_id,
        src.file_name
    );