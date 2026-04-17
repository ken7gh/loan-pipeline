CREATE OR REPLACE FILE FORMAT RAW.FF_LOAN_MONTHLY_PIPE_GZ
    TYPE = CSV
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1
    COMPRESSION = AUTO
    FIELD_OPTIONALLY_ENCLOSED_BY = NONE
    TRIM_SPACE = TRUE
    EMPTY_FIELD_AS_NULL = TRUE
    NULL_IF = ('NULL', 'null', '');

BEGIN
    LET v_run_id STRING := := NEXTVAL(TARGET.RUN_ID_SEQ);
    LET v_file_name STRING := 'LOAN_MONTHLY_202601.gz';
    LET v_start_time TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    LET v_rows_loaded NUMBER := 0;
    LET v_errors_seen NUMBER := 0;
    LET v_rows_parsed NUMBER := 0;

    INSERT INTO RAW.LOAD_HISTORY (
        run_id,
        file_name,
        start_time,
        status
    )
    VALUES (
        v_run_id,
        v_file_name,
        v_start_time,
        'STARTED'
    );

    COPY INTO RAW.RAW_LOAN_MONTHLY (
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
    )
    FROM (
        SELECT
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            METADATA$FILENAME,
            CURRENT_TIMESTAMP(),
            v_run_id
        FROM @MY_EXT_STAGE/path/LOAN_MONTHLY_202601.gz
    )
    FILE_FORMAT = (FORMAT_NAME = RAW.FF_LOAN_MONTHLY_PIPE_GZ)
    ON_ERROR = 'ABORT_STATEMENT';

    SELECT COUNT(*)
      INTO :v_rows_loaded
    FROM RAW.RAW_LOAN_MONTHLY
    WHERE run_id = v_run_id;

    v_rows_parsed := v_rows_loaded;
    v_errors_seen := 0;

    UPDATE RAW.LOAD_HISTORY
    SET
        end_time = CURRENT_TIMESTAMP(),
        rows_parsed = v_rows_parsed,
        rows_loaded = v_rows_loaded,
        errors_seen = v_errors_seen,
        status = 'SUCCESS',
        error_message = NULL
    WHERE run_id = v_run_id;

    UPDATE RAW.PIPELINE_CONTROL_STATE
      SET value = v_run_id
    WHERE process = "load_raw_loan_monthly"
      AND key = "sequence_id";

EXCEPTION
    WHEN OTHER THEN
        UPDATE RAW.LOAD_HISTORY
        SET
            end_time = CURRENT_TIMESTAMP(),
            rows_parsed = v_rows_parsed,
            rows_loaded = v_rows_loaded,
            errors_seen = 1,
            status = 'FAILED',
            error_message = SQLERRM
        WHERE run_id = v_run_id;

        RAISE;
END;
