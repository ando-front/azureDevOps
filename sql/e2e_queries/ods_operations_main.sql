----------------------------------------------------------------------
-- ODS（Operational Data Store）操作クエリ
-- Source: Externalized from ARM template queries
-- Purpose: ODS層でのデータ統合・変換処理
-- 
-- ODS層での以下の処理を実行：
-- - omni_ods_marketing データ統合
-- - omni_ods_cloak データ処理
-- - 中間テーブル操作
-- - データ品質管理
-- 
-- Features:
-- - Marketing ODS データ統合
-- - Cloak ODS サービス処理
-- - 中間テーブル管理
-- - データ整合性チェック
-- 
-- History:
-- 2024/12/31 ARMテンプレートから外部化
----------------------------------------------------------------------

-- Marketing ODS データ処理
-- @name: marketing_ods_processing
INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
SELECT [顧客キー_Ax] AS CLIENT_KEY_AX
      , [LIV0EU_ガスメータ設置場所番号＿１ｘ] AS LIV0EU_1X
      , [LIV0EU_カスタマ番号＿８ｘ] AS LIV0EU_8X
-- Additional columns for comprehensive ODS processing
FROM [Marketing].[顧客DM]
WHERE [処理日] = @processing_date;

-- Cloak ODS サービス処理
-- @name: cloak_ods_service_processing  
UPDATE [omni].[omni_ods_cloak_trn_usageservice]
SET TRANSFER_TYPE = '02'
   ,OUTPUT_DATE = @output_date
WHERE TRANSFER_YMD = @transfer_ymd
    AND SERVICE_TYPE IN ('001', '006', '010');

-- 中間テーブルクリーンアップ
-- @name: intermediate_table_cleanup
TRUNCATE TABLE [omni].[omni_ods_temp_processing];

-- データ整合性チェック
-- @name: data_integrity_check
SELECT COUNT(*) as record_count
      , COUNT(CASE WHEN CLIENT_KEY_AX IS NULL THEN 1 END) as null_client_keys
      , COUNT(CASE WHEN LIV0EU_1X IS NULL THEN 1 END) as null_gas_meters
FROM [omni].[omni_ods_marketing_trn_client_dm]
WHERE REC_REG_YMD = @processing_date;
