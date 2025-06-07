----------------------------------------------------------------------
-- KARTE統合クエリ
-- Source: Externalized from ARM template queries
-- Purpose: KARTE連携用契約有無情報の作成・管理
-- 
-- KARTE連携用の以下の処理を実行：
-- - 契約有無テンプテーブル作成
-- - 域内ガス契約フラグ設定
-- - 域外ガス契約フラグ設定
-- - 電気契約フラグ設定
-- - サービス種別による契約判定
-- 
-- Features:
-- - 利用サービスマスタとの連携
-- - 契約種別の自動判定
-- - フラグ値の集約処理
-- - KARTE システム連携準備
-- 
-- History:
-- 2024/12/31 ARMテンプレートから外部化
----------------------------------------------------------------------

-- KARTE連携用契約有無tempを作成
-- @name: karte_contract_temp_setup
TRUNCATE TABLE [omni].[omni_ods_cloak_trn_karte_contract_temp];

INSERT INTO [omni].[omni_ods_cloak_trn_karte_contract_temp]
SELECT [MTGID]
      , MAX(CASE WHEN SERVICE_TYPE = '001' THEN '1'
                ELSE '0'
       END) AS INTERNAL_AREA_GAS                           -- 域内ガス契約フラグ
      , MAX(CASE WHEN SERVICE_TYPE = '010' THEN '1'
                ELSE '0'
       END) AS EXTERNAL_AREA_GAS                           -- 域外ガス契約フラグ
      , MAX(CASE WHEN SERVICE_TYPE = '006' THEN '1'
                ELSE '0'
       END) AS POWER
-- 電気契約フラグ
FROM [omni].[omni_ods_cloak_trn_usageservice]
-- 利用サービス
WHERE TRANSFER_TYPE = '02'
-- 異動種別=02(提供中)
GROUP BY [MTGID];

-- KARTE契約データ検証
-- @name: karte_contract_validation
SELECT COUNT(*) as total_records
      , COUNT(CASE WHEN INTERNAL_AREA_GAS = '1' THEN 1 END) as internal_gas_contracts
      , COUNT(CASE WHEN EXTERNAL_AREA_GAS = '1' THEN 1 END) as external_gas_contracts  
      , COUNT(CASE WHEN POWER = '1' THEN 1 END) as power_contracts
      , COUNT(CASE WHEN INTERNAL_AREA_GAS = '1' AND POWER = '1' THEN 1 END) as gas_power_combined
FROM [omni].[omni_ods_cloak_trn_karte_contract_temp];

-- KARTE連携データ出力準備
-- @name: karte_export_preparation
SELECT [MTGID]
      , [INTERNAL_AREA_GAS]
      , [EXTERNAL_AREA_GAS] 
      , [POWER]
      , CASE 
           WHEN INTERNAL_AREA_GAS = '1' AND POWER = '1' THEN 'GAS_POWER'
           WHEN INTERNAL_AREA_GAS = '1' THEN 'GAS_ONLY'
           WHEN POWER = '1' THEN 'POWER_ONLY'
           ELSE 'NO_CONTRACT'
       END AS CONTRACT_TYPE
FROM [omni].[omni_ods_cloak_trn_karte_contract_temp]
WHERE [MTGID] IS NOT NULL
ORDER BY [MTGID];
