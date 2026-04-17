-- =========================================================
-- 5) QC RESULTS TABLE
-- One row per QC check per run.
-- =========================================================
CREATE OR REPLACE TABLE TARGET.QC_RESULTS (
    run_id            STRING        NOT NULL,
    check_name        STRING        NOT NULL,
    expected_value    STRING,
    actual_value      STRING,
    status            STRING,
    severity          STRING,
    sample_query      STRING,
    check_time        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'QC Table';
