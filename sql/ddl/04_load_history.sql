-- 4) LOAD HISTORY / AUDIT TABLE
-- One row per ingestion run / file load attempt.
-- =========================================================
CREATE OR REPLACE TABLE TARGET.LOAD_HISTORY (
    run_id            NUMBER        NOT NULL,
    file_name         STRING        NOT NULL,
    start_time        TIMESTAMP_NTZ NOT NULL,
    end_time          TIMESTAMP_NTZ,
    rows_parsed       NUMBER,
    rows_loaded       NUMBER,
    errors_seen       NUMBER,
    status            STRING,
    error_message     STRING,
    created_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)COMMENT = 'Tracking table for file ingestion runs';
