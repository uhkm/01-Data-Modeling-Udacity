-- Use CETAS to export select statement
IF OBJECT_ID('dbo.rider_dimension') IS NOT NULL
BEGIN
  DROP EXTERNAL TABLE [dbo].[rider_dimension];
END

CREATE EXTERNAL TABLE dbo.rider_dimension

WITH (
    LOCATION    = 'rider_dimension',
    DATA_SOURCE = [adls2filesystemuhk_adls2uhk_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
)  
AS
SELECT DISTINCT
    rider_id,
    CAST(account_start_date AS DATE) AS account_start_date,
    CAST(account_end_date AS DATE) AS account_end_date,
    is_member
FROM dbo.stage_rider;
GO

-- Query the newly created CETAS external table
SELECT TOP 100 * FROM dbo.rider_dimension
GO