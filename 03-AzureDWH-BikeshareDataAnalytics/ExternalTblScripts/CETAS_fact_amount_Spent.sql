-- Use CETAS to export select statement
IF OBJECT_ID('dbo.fact_payment ') IS NOT NULL
BEGIN
  DROP EXTERNAL TABLE [dbo].[fact_payment ];
END

CREATE EXTERNAL TABLE dbo.fact_payment 

WITH (
    LOCATION    = 'fact_payment ',
    DATA_SOURCE = [adls2filesystemuhk_adls2uhk_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
)  
AS
SELECT DISTINCT
	p.payment_id,
	r.rider_id,
    p.payment_date,
    p.amount
FROM dbo.stage_payment p 
JOIN dbo.rider_dimension r
on p.rider_id = r.rider_id;
GO

-- Query the newly created CETAS external table
SELECT TOP 100 * FROM dbo.fact_payment 
GO