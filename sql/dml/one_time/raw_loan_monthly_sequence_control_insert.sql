-- First insert for raw_loan_monthly sequence control
INSERT INTO RAW.PIPELINE_CONTROL_STATE (
	process,
    key,
    value
)
VALUES (
    "load_raw_loan_monthly"
    "sequence_id",
	"0"
 );