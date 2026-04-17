-- Run ID Seq
CREATE SEQUENCE IF NOT EXISTS RAW.RUN_ID_SEQ
    START = 1
    INCREMENT = 1
    COMMENT = 'Sequence used to generate unique run_id for each ingestion run';