----------------------------------------------------------------------
-- 電気契約Thanksシナリオ MA向けリスト連携
-- Source: Externalized from ARM template
-- Purpose: 電気契約完了時のThanksシナリオ用リスト作成
-- 
-- Main Features:
-- - 電気契約Thanksシナリオ対象者抽出
-- - 実行日-1日の対象データ抽出
-- - 契約タイプ情報付与
-- - mTGID取得（本人特定契約ベース）
-- - 中間テーブル経由の段階的処理
-- 
-- Target Output: 電気契約Thanksシナリオ対象者リスト
-- Processing: 日次バッチ処理（実行日-1日分）
-- 
-- History:
-- 2023/07 新規作成
-- 2024/10 出力カラム「契約タイプ」追加、抽出条件変更、mTGID取得「本人特定契約」に変更
-- 2024/12/31 ARMテンプレートから外部化
----------------------------------------------------------------------

-- 実行日時の設定
DECLARE @today_jst DATETIME;
SET @today_jst = CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time');

-- 抽出対象日=実行日-1日
DECLARE @targetDate varchar(8);
SET @targetDate = format(DATEADD(DAY,-1,@today_jst),'yyyyMMdd');

----------------------------------------------------------------------
-- ① 中間テーブルに電気契約Thanksシナリオ対象データを出力
TRUNCATE TABLE [omni].[omni_ods_ma_trn_electric_contract_thanks_temp];

INSERT INTO [omni].[omni_ods_ma_trn_electric_contract_thanks_temp]
SELECT DISTINCT
    ecdata.CONTRACT_ID, -- 契約ID
    ecdata.CUSTOMER_ID, -- 顧客ID  
    ecdata.CONTRACT_START_DATE, -- 契約開始日
    ecdata.CONTRACT_TYPE, -- 契約タイプ
    ecdata.ELECTRIC_PLAN_CODE, -- 電気プランコード
    ecdata.REGISTRATION_DATE, -- 登録日
    mig.MTG_ID, -- mTGID（本人特定契約から取得）
    FORMAT(@today_jst, 'yyyy/MM/dd HH:mm:ss') AS PROCESS_DATETIME
-- 処理日時
FROM [omni].[electric_contract_data] ecdata
    INNER JOIN [omni].[本人特定契約] mig
    ON ecdata.CUSTOMER_ID = mig.CUSTOMER_ID
WHERE ecdata.REGISTRATION_DATE = @targetDate
    AND ecdata.CONTRACT_STATUS = '完了'
    AND ecdata.CONTRACT_TYPE IN ('新規', '切替')
    AND mig.STATUS = '有効';

----------------------------------------------------------------------
-- ② 最終出力用データの準備
SELECT
    CONTRACT_ID,
    CUSTOMER_ID,
    CONTRACT_START_DATE,
    CONTRACT_TYPE,
    ELECTRIC_PLAN_CODE,
    MTG_ID,
    PROCESS_DATETIME
FROM [omni].[omni_ods_ma_trn_electric_contract_thanks_temp]
ORDER BY CONTRACT_ID;
