-- E2Eテスト用テストデータ挿入スクリプト
-- 実行順序: 03_insert_test_data.sql

USE TGMATestDB;
GO

-- Clear existing test data (with existence checks)
IF OBJECT_ID('[dbo].[point_grant_email]', 'U') IS NOT NULL
    DELETE FROM [dbo].[point_grant_email];

IF OBJECT_ID('[dbo].[marketing_client_dm]', 'U') IS NOT NULL
    DELETE FROM [dbo].[marketing_client_dm];

IF OBJECT_ID('[dbo].[ClientDmBx]', 'U') IS NOT NULL
    DELETE FROM [dbo].[ClientDmBx];

IF OBJECT_ID('[dbo].[client_dm]', 'U') IS NOT NULL
    DELETE FROM [dbo].[client_dm];

-- Clear etl.e2e_test_execution_log if it exists
IF OBJECT_ID('[etl].[e2e_test_execution_log]', 'U') IS NOT NULL
    DELETE FROM [etl].[e2e_test_execution_log];
ELSE
    PRINT 'etl.e2e_test_execution_log table does not exist - skipping delete';

GO

-- Insert test client data
INSERT INTO [dbo].[client_dm]
    (
    [client_id], [client_name], [email], [phone], [address], [registration_date], [status]
    )
VALUES
    ('CLIENT001', '田中太郎', 'tanaka@example.com', '090-1234-5678', '東京都渋谷区1-1-1', '2023-01-15', 'ACTIVE'),
    ('CLIENT002', '佐藤花子', 'sato@example.com', '090-2345-6789', '大阪府大阪市2-2-2', '2023-02-20', 'ACTIVE'),
    ('CLIENT003', '鈴木一郎', 'suzuki@example.com', '090-3456-7890', '愛知県名古屋市3-3-3', '2023-03-25', 'INACTIVE'),
    ('CLIENT004', '高橋美咲', 'takahashi@example.com', '090-4567-8901', '福岡県福岡市4-4-4', '2023-04-30', 'ACTIVE'),
    ('CLIENT005', '山田健太', 'yamada@example.com', '090-5678-9012', '北海道札幌市5-5-5', '2023-05-15', 'ACTIVE');
GO

-- Insert test ClientDmBx data
INSERT INTO [dbo].[ClientDmBx]
    (
    [client_id], [segment], [score], [last_transaction_date], [total_amount], [data_source]
    )
VALUES
    ('CLIENT001', 'PREMIUM', 85.5, '2023-12-15', 125000.00, 'ETL_PIPELINE'),
    ('CLIENT002', 'STANDARD', 72.3, '2023-12-10', 85000.00, 'ETL_PIPELINE'),
    ('CLIENT003', 'BASIC', 45.8, '2023-11-25', 15000.00, 'ETL_PIPELINE'),
    ('CLIENT004', 'PREMIUM', 92.1, '2023-12-18', 200000.00, 'ETL_PIPELINE'),
    ('CLIENT005', 'STANDARD', 68.7, '2023-12-12', 65000.00, 'ETL_PIPELINE');
GO

-- Insert test point grant email data
INSERT INTO [dbo].[point_grant_email]
    (
    [client_id], [email], [points_granted], [email_sent_date], [campaign_id], [status]
    )
VALUES
    ('CLIENT001', 'tanaka@example.com', 1000, '2023-12-20', 'CAMPAIGN_001', 'SENT'),
    ('CLIENT002', 'sato@example.com', 500, '2023-12-20', 'CAMPAIGN_001', 'SENT'),
    ('CLIENT004', 'takahashi@example.com', 1500, '2023-12-20', 'CAMPAIGN_001', 'SENT'),
    ('CLIENT005', 'yamada@example.com', 750, '2023-12-20', 'CAMPAIGN_001', 'PENDING');
GO

-- Insert test marketing client data
INSERT INTO [dbo].[marketing_client_dm]
    (
    [client_id], [marketing_segment], [preference_category], [engagement_score],
    [last_campaign_response], [opt_in_email], [opt_in_sms]
    )
VALUES
    ('CLIENT001', 'HIGH_VALUE', 'ELECTRONICS', 9.2, '2023-12-15', 1, 1),
    ('CLIENT002', 'MEDIUM_VALUE', 'FASHION', 7.5, '2023-12-10', 1, 0),
    ('CLIENT003', 'LOW_VALUE', 'BOOKS', 3.8, '2023-11-20', 0, 0),
    ('CLIENT004', 'HIGH_VALUE', 'TRAVEL', 9.8, '2023-12-18', 1, 1),
    ('CLIENT005', 'MEDIUM_VALUE', 'FOOD', 6.9, '2023-12-12', 1, 0);
GO

-- Insert staging test data
INSERT INTO [staging].[client_raw]
    (
    [source_system], [raw_data], [file_name], [processed]
    )
VALUES
    ('CRM_SYSTEM', '{"client_id":"CLIENT006","name":"新規顧客","email":"new@example.com"}', 'daily_import_20231220.json', 0),
    ('SALES_SYSTEM', '{"client_id":"CLIENT001","transaction_amount":5000,"date":"2023-12-20"}', 'sales_20231220.json', 0);
GO

-- Insert ETL execution log data
INSERT INTO [etl].[pipeline_execution_log]
    (
    [pipeline_name], [activity_name], [start_time], [end_time], [status], [input_rows], [output_rows]
    )
VALUES
    ('pi_Copy_marketing_client_dm', 'CopyActivity', '2023-12-20 10:00:00', '2023-12-20 10:05:00', 'SUCCESS', 100, 95),
    ('pi_Insert_ClientDmBx', 'InsertActivity', '2023-12-20 10:10:00', '2023-12-20 10:15:00', 'SUCCESS', 50, 50),
    ('pi_PointGrantEmail', 'EmailActivity', '2023-12-20 10:20:00', '2023-12-20 10:25:00', 'SUCCESS', 25, 24);
GO

-- Create test data integrity check procedures
CREATE OR ALTER PROCEDURE [dbo].[sp_validate_pipeline_data_integrity]
    @pipeline_name NVARCHAR(100),
    @expected_input_rows INT,
    @expected_output_rows INT
AS
BEGIN
    DECLARE @actual_input_rows INT;
    DECLARE @actual_output_rows INT;
    DECLARE @validation_result NVARCHAR(100);

    SELECT
        @actual_input_rows = input_rows,
        @actual_output_rows = output_rows
    FROM [etl].[pipeline_execution_log]
    WHERE pipeline_name = @pipeline_name
        AND created_at >= DATEADD(HOUR, -1, GETDATE())
    ORDER BY created_at DESC;

    IF @actual_input_rows = @expected_input_rows AND @actual_output_rows = @expected_output_rows
        SET @validation_result = 'PASS';
    ELSE
        SET @validation_result = 'FAIL';

    SELECT
        @pipeline_name as pipeline_name,
        @expected_input_rows as expected_input,
        @actual_input_rows as actual_input,
        @expected_output_rows as expected_output,
        @actual_output_rows as actual_output,
        @validation_result as validation_result;
END
GO

PRINT 'Test data inserted successfully';
GO

-- Display data summary for verification
SELECT *
FROM [dbo].[v_test_data_summary];
GO
