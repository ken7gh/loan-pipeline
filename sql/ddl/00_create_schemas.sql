-- =========================================================
-- Create RAW schema
-- Purpose:
--   - Landing zone for ingested files from external stage and related data cleansing views
--   - Stores data in append-only, historical format
--   - Preserves original structure and ingestion metadata
-- =========================================================
CREATE SCHEMA IF NOT EXISTS RAW
COMMENT = 'Raw layer: append-only landing tables storing source data with ingestion metadata (file_name, load_time, run_id) for auditability and reprocessing.';


-- =========================================================
-- Create TARGET schema
-- Purpose:
--   - Stores analytics-ready, business-consumable tables
--   - Maintains one record per business key
--   - Populated via incremental merge from cleaned data
-- =========================================================
CREATE SCHEMA IF NOT EXISTS TARGET
COMMENT = 'Target layer: analytics-ready tables maintained via incremental merges. Data is deduplicated and updated based on business keys.';