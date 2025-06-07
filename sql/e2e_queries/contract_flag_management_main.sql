----------------------------------------------------------------------
-- 契約フラグ管理クエリ
-- Source: Externalized from ARM template queries  
-- Purpose: mTG会員契約フラグの管理・更新処理
-- 
-- 以下の契約フラグ管理を実行：
-- - 全量契約中フラグ管理
-- - 域内ガス契約フラグ
-- - 域外ガス契約フラグ  
-- - 電気契約フラグ
-- - ガス電気合算契約フラグ
-- 
-- Features:
-- - 本人特定契約との連携
-- - 電力CIS契約情報との統合
-- - ガス契約日次情報との統合
-- - フラグ集約処理
-- 
-- History:
-- 2025/03/04 新規作成
-- 2024/12/31 ARMテンプレートから外部化
----------------------------------------------------------------------

-- mTG会員_全量契約中フラグ_tempの全レコード削除
-- @name: full_contract_flag_temp_truncate
TRUNCATE TABLE [omni].[omni_odm_mytginfo_trn_mtgmaster_full_under_contract_flag_temp];

-- 域内ガス契約フラグ付与
-- @name: internal_area_gas_flag_insert
INSERT INTO [omni].[omni_odm_mytginfo_trn_mtgmaster_full_under_contract_flag_temp]
SELECT cpkiyk.MTGID                                              -- mTG会員ID
      , cpkiyk.EDA_NO                                             -- 枝番
      , 1    AS INTERNAL_AREA_GAS_FLAG                            -- ガス契約中フラグ (0:契約中ではない 1:契約中)
      , NULL AS EXTERNAL_AREA_GAS_FLAG                            -- 域外ガス契約中フラグ
      , NULL AS INTERNAL_AREA_GAS_AND_POWER_FLAG                  -- 域内ガス電気合算契約中フラグ
      , NULL AS EXTERNAL_AREA_GAS_AND_POWER_FLAG                  -- 域外ガス電気合算契約中フラグ
      , NULL AS POWER_FLAG
-- 電気契約中フラグ
FROM [omni].[omni_ods_cloak_trn_personal_identification_contract] cpkiyk -- 本人特定契約
    INNER JOIN [omni].[omni_odm_gascstmr_trn_gaskiy] gaskiy -- ガス契約（契約中）日次お客さま情報
    ON cpkiyk.KEY_3X = gaskiy.KEY_3X
-- お客さま登録番号
;

-- 域外ガス契約フラグ付与（未定のため空処理）
-- @name: external_area_gas_flag_placeholder
SELECT TOP 0
    *
FROM [omni].[omni_odm_mytginfo_trn_mtgmaster_full_under_contract_flag_temp];

-- 域内ガス電気合算契約フラグ付与
-- @name: internal_gas_power_combined_flag_insert
INSERT INTO [omni].[omni_odm_mytginfo_trn_mtgmaster_full_under_contract_flag_temp]
SELECT cpkiyk.MTGID                                        -- mTG会員ID
      , cpkiyk.EDA_NO                                       -- 枝番
      , NULL AS INTERNAL_AREA_GAS_FLAG                      -- ガス契約中フラグ
      , NULL AS EXTERNAL_AREA_GAS_FLAG                      -- 域外ガス契約中フラグ
      , 1    AS INTERNAL_AREA_GAS_AND_POWER_FLAG            -- 域内ガス電気合算契約中フラグ (0:契約中ではない 1:契約中)
      , NULL AS EXTERNAL_AREA_GAS_AND_POWER_FLAG            -- 域外ガス電気合算契約中フラグ
      , NULL AS POWER_FLAG
-- 電気契約中フラグ
FROM [omni].[omni_ods_cloak_trn_personal_identification_contract] cpkiyk -- 本人特定契約
    INNER JOIN [omni].[omni_ods_epcis_trn_contract] ct -- 電力ＣＩＳ契約情報
    ON cpkiyk.KEY_3X = ct.KEY_3X -- お客さま登録番号
    INNER JOIN [omni].[omni_odm_gascstmr_trn_gaskiy] gaskiy -- ガス契約（契約中）日次お客さま情報
    ON cpkiyk.KEY_3X = gaskiy.KEY_3X
-- お客さま登録番号
WHERE ct.SA_TYPE_CD in('ESLVY','ESOLY')
-- 契約タイプ=合算
;

-- フラグ集約処理
-- @name: contract_flag_aggregation
TRUNCATE TABLE [omni].[omni_odm_mytginfo_trn_mtgmaster_under_contract_flag_temp];

INSERT INTO [omni].[omni_odm_mytginfo_trn_mtgmaster_under_contract_flag_temp]
SELECT MTGID                                                                              -- mTG会員ID
      , EDA_NO                                                                             -- 枝番
      , MAX(INTERNAL_AREA_GAS_FLAG) AS INTERNAL_AREA_GAS_FLAG                              -- ガス契約中フラグ
      , MAX(EXTERNAL_AREA_GAS_FLAG) AS EXTERNAL_AREA_GAS_FLAG                              -- 域外ガス契約中フラグ
      , MAX(INTERNAL_AREA_GAS_AND_POWER_FLAG) AS INTERNAL_AREA_GAS_AND_POWER_FLAG          -- 域内ガス電気合算契約中フラグ
      , MAX(EXTERNAL_AREA_GAS_AND_POWER_FLAG) AS EXTERNAL_AREA_GAS_AND_POWER_FLAG          -- 域外ガス電気合算契約中フラグ
      , MAX(POWER_FLAG) AS POWER_FLAG
-- 電気契約中フラグ
FROM [omni].[omni_odm_mytginfo_trn_mtgmaster_full_under_contract_flag_temp]
GROUP BY MTGID, EDA_NO;
