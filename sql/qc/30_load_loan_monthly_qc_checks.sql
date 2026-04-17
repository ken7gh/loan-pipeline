-- =========================================================
-- Purpose:
--   - Run QC checks for the latest successful ingestion run
--   - Write one result row per QC check into TARGET.QC_RESULTS
-- =========================================================

-- ---------------------------------------------------------
-- Get current successful run_id
-- ---------------------------------------------------------
SET RUN_ID = (
    SELECT value 
	  FROM RAW.PIPELINE_CONTROL_STATE 
	 WHERE process = "load_raw_loan_monthly"
       AND key = "sequence_id"
);

-- =========================================================
-- 1) ROW COUNT CHECK
-- Compare valid deduplicated source rows for this run
-- against matching rows present in target after merge.
-- =========================================================
INSERT INTO TARGET.QC_RESULTS (
    run_id,
    check_name,
    expected_value,
    actual_value,
    status,
    severity,
    sample_query
)
WITH SRC AS (
    SELECT
        reporting_month,
        loan_id,
        updated_at,
        load_time
    FROM RAW.VW_LOAN_MONTHLY_CLEAN
    WHERE run_id = $RUN_ID
      AND loan_id IS NOT NULL
      AND reporting_month IS NOT NULL
),
DEDUP AS (
    SELECT
        reporting_month,
        loan_id
    FROM SRC
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY loan_id, reporting_month
        ORDER BY updated_at DESC NULLS LAST, load_time DESC
    ) = 1
),
TARGET_MATCH AS (
    SELECT COUNT(*) AS cnt
    FROM TARGET.TARGET_LOAN_MONTHLY t
    INNER JOIN DEDUP d
        ON t.loan_id = d.loan_id
       AND t.reporting_month = d.reporting_month
),
SRC_COUNT AS (
    SELECT COUNT(*) AS cnt
    FROM DEDUP
)
SELECT
    $RUN_ID AS run_id,
    'ROW_COUNT_CHECK' AS check_name,
    TO_VARCHAR((SELECT cnt FROM SRC_COUNT)) AS expected_value,
    TO_VARCHAR((SELECT cnt FROM TARGET_MATCH)) AS actual_value,
    CASE
        WHEN (SELECT cnt FROM SRC_COUNT) = (SELECT cnt FROM TARGET_MATCH)
            THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    'HIGH' AS severity,
    'Compare deduplicated valid source rows for run_id = ' || $RUN_ID ||
    ' to matching business keys in TARGET.TARGET_LOAN_MONTHLY' AS sample_query
;

-- =========================================================
-- 2) DUPLICATE CHECK
-- Count duplicate business keys in raw/clean data for run
-- =========================================================
INSERT INTO TARGET.QC_RESULTS (
    run_id,
    check_name,
    expected_value,
    actual_value,
    status,
    severity,
    sample_query
)
WITH DUPES AS (
    SELECT
        loan_id,
        reporting_month,
        COUNT(*) AS dup_count
    FROM RAW.VW_LOAN_MONTHLY_CLEAN
    WHERE run_id = $RUN_ID
      AND loan_id IS NOT NULL
      AND reporting_month IS NOT NULL
    GROUP BY loan_id, reporting_month
    HAVING COUNT(*) > 1
),
DUPE_COUNT AS (
    SELECT COUNT(*) AS cnt
    FROM DUPES
)
SELECT
    $RUN_ID AS run_id,
    'DUPLICATE_CHECK' AS check_name,
    '0' AS expected_value,
    TO_VARCHAR((SELECT cnt FROM DUPE_COUNT)) AS actual_value,
    CASE
        WHEN (SELECT cnt FROM DUPE_COUNT) = 0
            THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    'HIGH' AS severity,
    'SELECT loan_id, reporting_month, COUNT(*) FROM RAW.VW_LOAN_MONTHLY_CLEAN WHERE run_id = ' || $RUN_ID ||
    ' GROUP BY loan_id, reporting_month HAVING COUNT(*) > 1' AS sample_query
;

-- =========================================================
-- 3) NULL CHECK
-- loan_id and reporting_month must not be null
-- =========================================================
INSERT INTO TARGET.QC_RESULTS (
    run_id,
    check_name,
    expected_value,
    actual_value,
    status,
    severity,
    sample_query
)
WITH NULLS_FOUND AS (
    SELECT COUNT(*) AS cnt
    FROM RAW.VW_LOAN_MONTHLY_CLEAN
    WHERE run_id = $RUN_ID
      AND (
            loan_id IS NULL
         OR reporting_month IS NULL
      )
)
SELECT
    $RUN_ID AS run_id,
    'NULL_CHECK' AS check_name,
    '0' AS expected_value,
    TO_VARCHAR((SELECT cnt FROM NULLS_FOUND)) AS actual_value,
    CASE
        WHEN (SELECT cnt FROM NULLS_FOUND) = 0
            THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    'HIGH' AS severity,
    'SELECT * FROM RAW.VW_LOAN_MONTHLY_CLEAN WHERE run_id = ' || $RUN_ID ||
    ' AND (loan_id IS NULL OR reporting_month IS NULL)' AS sample_query
;

-- =========================================================
-- 4) ANOMALY CHECK
-- balance < 0 OR interest_rate < 0 OR interest_rate > 25
-- =========================================================
INSERT INTO TARGET.QC_RESULTS (
    run_id,
    check_name,
    expected_value,
    actual_value,
    status,
    severity,
    sample_query
)
WITH ANOMALIES AS (
    SELECT COUNT(*) AS cnt
    FROM RAW.VW_LOAN_MONTHLY_CLEAN
    WHERE run_id = $RUN_ID
      AND (
            balance < 0
         OR interest_rate < 0
         OR interest_rate > 25
      )
)
SELECT
    $RUN_ID AS run_id,
    'ANOMALY_CHECK' AS check_name,
    '0' AS expected_value,
    TO_VARCHAR((SELECT cnt FROM ANOMALIES)) AS actual_value,
    CASE
        WHEN (SELECT cnt FROM ANOMALIES) = 0
            THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    'MEDIUM' AS severity,
    'SELECT * FROM RAW.VW_LOAN_MONTHLY_CLEAN WHERE run_id = ' || $RUN_ID ||
    ' AND (balance < 0 OR interest_rate < 0 OR interest_rate > 25)' AS sample_query
;