----------------------------------------------------------------------
-- ガス機器・水まわり修理 MA向けリスト連携
-- Source: Externalized from ARM template  
-- Purpose: ガス機器・水まわり修理対象者リスト作成処理
-- 
-- Main Features:
-- - 実行日-21日時点の修理対象データ抽出
-- - ガスメータ設置場所番号ベースの処理
-- - 作業種別・修理種別による分類
-- - 中間テーブル経由の段階的処理
-- - 決済内訳修理情報の管理
-- 
-- Target Output: ガス機器・水まわり修理対象者リスト
-- Processing: 日次バッチ処理（実行日-21日分）
-- 
-- History:
-- 2024/12/31 ARMテンプレートから外部化
----------------------------------------------------------------------

-- 実行日時の設定
DECLARE @today_jst DATETIME;
SET @today_jst = CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time');

-- 抽出対象日=実行日-21日
DECLARE @targetDate varchar(8);
SET @targetDate = format(DATEADD(DAY,-21,@today_jst),'yyyyMMdd');

----------------------------------------------------------------------
-- ① 中間テーブルに修理対象データを出力
TRUNCATE TABLE [omni].[omni_ods_ma_trn_lim_settlement_breakdown_repair_temp];

INSERT INTO [omni].[omni_ods_ma_trn_lim_settlement_breakdown_repair_temp]
SELECT
    GMT_SET_NO, -- ガスメータ設置場所番号
    WORK_TYPE_CODE, -- 作業種別コード
    REPAIR_TYPE_CODE, -- 修理種別コード
    REPAIR_REQUEST_DATE, -- 修理依頼日
    REPAIR_COMPLETION_DATE, -- 修理完了日
    CUSTOMER_ID, -- 顧客ID
    REPAIR_AMOUNT, -- 修理金額
    SETTLEMENT_STATUS, -- 決済ステータス
    BREAKDOWN_CATEGORY, -- 故障カテゴリ
    URGENT_FLAG, -- 緊急フラグ
    FORMAT(@today_jst, 'yyyy/MM/dd HH:mm:ss') AS PROCESS_DATETIME
-- 処理日時
FROM [omni].[gas_appliance_repair_data] gar
WHERE gar.REPAIR_REQUEST_DATE = @targetDate
    AND gar.REPAIR_STATUS IN ('完了', '対応中')
    AND gar.REPAIR_TYPE_CODE IN ('ガス機器', '水まわり');

----------------------------------------------------------------------
-- ② 最終出力用データの準備
SELECT
    GMT_SET_NO,
    WORK_TYPE_CODE,
    REPAIR_TYPE_CODE,
    REPAIR_REQUEST_DATE,
    REPAIR_COMPLETION_DATE,
    CUSTOMER_ID,
    REPAIR_AMOUNT,
    SETTLEMENT_STATUS,
    BREAKDOWN_CATEGORY,
    URGENT_FLAG,
    PROCESS_DATETIME
FROM [omni].[omni_ods_ma_trn_lim_settlement_breakdown_repair_temp]
ORDER BY GMT_SET_NO, REPAIR_REQUEST_DATE;
