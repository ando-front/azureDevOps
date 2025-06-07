----------------------------------------------------------------------
-- お客さま番号登録完了のお知らせ 外部テーブル作成・データ処理
-- Source: Externalized from ARM template
-- Purpose: お客さま番号登録完了のお知らせ用外部テーブル作成とデータ処理
-- 
-- Main Features:
-- - 外部テーブルの動的作成・削除処理
-- - TSVファイルロード用外部テーブル定義
-- - リスト連携MA向け処理
-- - mTGinfo関連データ処理
-- 
-- Target Output: お客さま番号登録完了のお知らせCSV
-- Data Sources: myTG会員情報、外部TSVファイル
-- 
-- History:
-- 2024/12/31 ARMテンプレートから外部化
----------------------------------------------------------------------

-- 実行日時の設定
DECLARE @today_jst DATETIME;
SET @today_jst = CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time');

-- 作成する外部テーブルが存在する場合は削除する
DECLARE @isAllEventsTemp int;
SELECT @isAllEventsTemp = count(*)
FROM sys.objects
WHERE schema_id = schema_id('omni')
    AND name = 'omni_ods_mytginfo_trn_customer_number_add_completed_ext_temp';

IF @isAllEventsTemp = 1
    DROP EXTERNAL TABLE [omni].[omni_ods_mytginfo_trn_customer_number_add_completed_ext_temp];

-- ADFによりTSVをロードする外部テーブルを作成する
CREATE EXTERNAL TABLE [omni].[omni_ods_mytginfo_trn_customer_number_add_completed_ext_temp]
(
    [CUSTOMER_NUMBER] NVARCHAR(50),
    [REGISTRATION_STATUS] NVARCHAR(10),
    [COMPLETION_DATE] NVARCHAR(8),
    [MTG_ID] NVARCHAR(100)
)
WITH (
    LOCATION = '/customer_number_registration/',
    DATA_SOURCE = [ExternalDataSource],
    FILE_FORMAT = [TsvFileFormat]
);

-- お客さま番号登録完了情報を処理テーブルに挿入
TRUNCATE TABLE [omni].[omni_ods_mytginfo_trn_customer_number_add_completed_temp];

INSERT INTO [omni].[omni_ods_mytginfo_trn_customer_number_add_completed_temp]
SELECT
    [CUSTOMER_NUMBER],
    [REGISTRATION_STATUS],
    [COMPLETION_DATE],
    [MTG_ID],
    FORMAT(@today_jst, 'yyyy/MM/dd HH:mm:ss') AS PROCESS_DATETIME
FROM [omni].[omni_ods_mytginfo_trn_customer_number_add_completed_ext_temp];
