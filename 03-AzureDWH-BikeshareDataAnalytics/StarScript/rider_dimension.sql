CREATE TABLE rider_dimension (
    rider_id INT,	
	account_start_date DATE,
    account_end_date DATE,
    is_member BIT
);

-- Load data into rider_dimension
INSERT INTO rider_dimension (rider_id, account_start_date, account_end_date, is_member)
SELECT DISTINCT
    rider_id,
    CAST(account_start_date AS DATE) AS account_start_date,
    CAST(account_end_date AS DATE) AS account_end_date,
    is_member
FROM dbo.stage_rider;