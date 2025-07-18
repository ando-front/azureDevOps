----------------------------------------------------------------------
-- omni.顧客DM(Marketingスキーマ)のODS化
-- 
-- 2024/02/02　新規作成
-- 2024/04/22　レイアウト変更に伴う、カラム追加（391行目～446行目）
-- 2024/06/03　6月の削除対象カラムをNULLに置き換えとする暫定対応
-- 2024/08/07　Maketingスキーマ.顧客DMからomni.顧客DMに参照先を変更
-- 2024/09/05　2カラムWEB履歴_1062_SS_Web見積_直近1か月フラグとWEB履歴_1062_SS_Web見積_直近1年フラグのカラム名変更対応で暫定的にNULLを設定
-- 2024/12/XX　巨大SQLクエリの外部化対応（ARMテンプレートから分離）
----------------------------------------------------------------------

-- Maketingスキーマのomni.顧客DMから、omniスキーマの顧客DM_tempにデータを全量コピーする。
SELECT [顧客キー_Ax] AS CLIENT_KEY_AX
      ,[LIV0EU_ガスメータ設置場所番号＿１ｘ] AS LIV0EU_1X
      ,[LIV0EU_カスタマ番号＿８ｘ] AS LIV0EU_8X
      ,[LIV0EU_受持ＮＷ箇所コード] AS LIV0EU_UKMT_NW_PART_CD
      ,[LIV0EU_受持ＮＷ箇所_法人名] AS LIV0EU_UKMT_NW_PART_CORP_NAME
      ,[LIV0EU_新社集計箇所コード] AS LIV0EU_NEW_CORP_AGG_CD
      ,[LIV0EU_適用開始年月日] AS LIV0EU_APPLY_START_YMD
      ,[LIV0EU_適用終了年月日] AS LIV0EU_APPLY_END_YMD
      ,[LIV0EU_使用契約番号＿４ｘ] AS LIV0EU_4X
      ,[LIV0EU_支払契約番号＿２ｘ] AS LIV0EU_2X
      ,[LIV0EU_郵便番号] AS LIV0EU_ZIP_CD
      ,[LIV0EU_都道府県名漢字] AS LIV0EU_PERF_KJ
      ,[LIV0EU_行政区画コード] AS LIV0EU_GYOK_CD
      ,[LIV0EU_行政区名] AS LIV0EU_GYOK_NAME
      ,[LIV0EU_町コード] AS LIV0EU_STREET_CD
      ,[LIV0EU_町名] AS LIV0EU_STREET_MAME
      ,[LIV0EU_丁目＿字コード] AS LIV0EU_CITY_BLOCK_CD
      ,[LIV0EU_建物番号] AS LIV0EU_BLD_NO
      ,[LIV0EU_新設年月] AS LIV0EU_SHINSETSU_YM
      ,[LIV0EU_供内管設備状態コード] AS LIV0EU_INNER_TUBE_EQP_STATE_CD
      ,[LIV0EU_ガスメータ開閉栓状態コード] AS LIV0EU_GAS_METER_CLOSING_CD
      ,[LIV0EU_用途コード] AS LIV0EU_USAGE_CD
      ,[LIV0EU_ガス用途_集合・戸建分類] AS LIV0EU_GAS_USAGE_AP_DT_CLASS
      ,[LIV0EU_ガス用途_大分類] AS LIV0EU_GAS_USAGE_MAJOR_CLASS
      ,[LIV0EU_ガス用途_中分類] AS LIV0EU_GAS_USAGE_MIDIUM_CLASS
      ,[LIV0EU_ガス用途_小分類] AS LIV0EU_GAS_USAGE_MINOR_CLASS
      ,[LIV0EU_ガス用途_家庭用詳細] AS LIV0EU_GAS_USAGE_HOME_USE_DETAIL
      ,[LIV0EU_ガス用途_業務用詳細] AS LIV0EU_GAS_USAGE_BIZ_USE_DETAIL
      ,[LIV0EU_ガス用途_12セグメント分類] AS LIV0EU_GAS_USAGE_12_SEGMENT_CLASS
      ,[LIV0EU_ガス用途_都市エネ大分類] AS LIV0EU_GAS_USAGE_TOSHI_ENE_MAJOR_CLASS
      ,[LIV0EU_ガス用途_都市エネ小分類] AS LIV0EU_GAS_USAGE_TOSHI_ENE_MINOR_CLASS
      ,[LIV0EU_ガス用途_都市エネ官公庁フラグ] AS LIV0EU_GAS_USAGE_TOSHI_ENE_PUBOFF_FLG
      ,[LIV0EU_メータ号数流量] AS LIV0EU_METER_NUMBER_FLOW_RATE
      ,[LIV0EU_検針方法名称] AS LIV0EU_READING_METER_METHOD_NAME
      ,[LIV0EU_ガス料金支払方法コード] AS LIV0EU_GAS_PAY_METHOD_CD
    bi.LIV0EU_GAS_PAY_METHOD,
    bi.LIV0EU_CHARGE_G_CD,
    bi.LIV0EU_CHARGE_G_AREA_TYPE_CD,
    bi.LIV0EU_BLOCK_NUMBER,
    bi.LIV0EU_GAS_SHRI_CRT_SHBT_CD,
    bi.LIV0EU_GAS_SHRI_CRT_SHBT_CD_NAME,
    bi.LIV0EU_GAS_SHRI_CRT_SHBT_CD_CRT_DETAILS,
    bi.LIV0EU_KSY_NAME,
    bi.LIV0EU_TEIHO_METRO_SIGN,
    bi.LIV0EU_TEIHO_METRO_SIGN_NAME,
    bi.LIV0EU_TEIHO_HOUSING_SITUATION_NAME,
    bi.LIV0EU_OPENING_REASON_NAME,
    bi.LIV0EU_HOME_OWNER_TYPE_NAME,
    bi.LIV0EU_HOME_OWNERSHIP_TYPE_NAME,
    bi.LIV0EU_CIS_DM_REFUAL_SING,
    bi.LIV0EU_CIS_SERVICE_UM_CD,
    bi.LIV0EU_LSYR_TOTAL_USE,
    bi.LIV0EU_HEAVY_USE_JYUYOUKA_FLG,
    bi.LIV0EU_INCHANGE_NW_LIFEVAL_ENESUTA_BRI,
    bi.LIV0EU_INCHANGE_NW_LIFEVAL_ENESUTA_NAME,
    bi.LIV0EU_OPENING_MONTH_PASSED,
    bi.LIV0EU_OPENING_MONTH_PASSED_CATEGORY,
    bi.TEIHO_TYPE,

    -- Equipment & Service columns
    es.LIV0SPD_KONRO_SHBT,
    es.LIV0SPD_KONRO_SHBT_NAME,
    es.LIV0SPD_KONRO_POSSESSION_EQUIPMENT_NO,
    es.LIV0SPD_KONRO_SL_DEST_CD,
    es.LIV0SPD_KONRO_DT_MANUFACTURE,
    es.LIV0SPD_KONRO_NUM_YEAR,
    es.LIV0SPD_KONRO_NUM_YEAR_CATEGORY,
    es.LIV0SPD_KYTK_SHBT,
    es.LIV0SPD_KYTK_NAME,
    es.LIV0SPD_KYTK_POSSESSION_DEVICE_NO,
    es.LIV0SPD_KYTK_SL_DESTINATION_CD,
    es.LIV0SPD_KYTK_DT_MANUFACTURE,
    es.LIV0SPD_KYTK_NUM_YEAR,
    es.LIV0SPD_KYTK_NUM_YEAR_CATEGORY,
    es.LIV0SPD_210_RICE_COOKER,
    es.LIV0SPD_220_MICROWAVE,
    es.LIV0SPD_230_OVEN,
    es.LIV0SPD_240_CONVECTION_OVEN,
    es.LIV0SPD_250_COMBINATION_MICROWAVE,
    es.LIV0SPD_300_DRYING_MACHINE,
    es.LIV0SPD_310_DRIER,
    es.LIV0SPD_410_WIRE_MESH_STOVE,
    es.LIV0SPD_420_OPEN_STOVE,
    es.LIV0SPD_430_FH_STOVE,
    es.LIV0SPD_440_BF_STOVE,
    es.LIV0SPD_450_FF_STOVE,
    es.LIV0SPD_460_FE_STOVE,
    es.TESHSMC_SERIAL_NUMBER,
    es.TESHSMC_SYSTEM_SHBT,
    es.TESHSEQ_SYSTEM_COMPLETION_DATE,
    es.TESHSEQ_MAINTENANCE_TYPE_CD,
    es.TESHSEQ_OWNER_TYPE_CD,
    es.TESHSEQ_ATTACHMENT_YMD,
    es.TESHSEQ_REMOVAL_YMD,
    es.TESHSEQ_TES_MAINT_ELIGIBILITY,
    es.TESHRDTR_UNDER_FLOOR_HEATING,
    es.TESHRDTR_BATH_HEATING,
    es.TESSV_MAINTENANCE_JOIN,
    es.TESSV_SERVICE_ID,
    es.TESSV_SERVICE_EXPIRATION_YM,
    es.TESSV_SERVICE_SHBT,
    es.ARMC_COUNT,
    es.ARMC_REMOVED_FLG,
    es.ARMC_UNDER_LEASE_FLG,
    es.ARMC_LEASE_END_FLG,
    es.EPCISCRT_3X,
    es.EPCISCRT_LIGHTING_SA_ID,
    es.EPCISCRT_LIGHTING_START_USE_DATE,
    es.EPCISCRT_LIGHTING_END_USE_DATE,
    es.EPCISCRT_LIGHTING_CRT_STATUS,
    es.EPCISCRT_LIGHTING_RESN_DISCONTION_USE,
    es.EPCISCRT_LIGHTING_CRT_TYPE,
    es.EPCISCRT_LIGHTING_CRT_TYPE_NAME,
    es.EPCISCRT_LIGHTING_CRT_TYPE_COMBIN_SINGLE,
    es.EPCISCRT_LIGHTING_SHRI_METHOD,
    es.EPCISCRT_LIGHTING_SHRI_METHOD_NAME,
    es.EPCISCRT_LIGHTING_PRICE_MENU,
    es.EPCISCRT_LIGHTING_PRICE_MENU_NAME,
    es.EPCISCRT_LIGHTING_CRT_ELECT_CURRENT,
    es.EPCISCRT_LIGHTING_CRT_CAPACITY,
    es.EPCISCRT_LIGHTING_CRT_ELECT_POWER,
    es.EPCISCRT_LIGHTING_CRT_ELECT_POWER_KW,
    es.EPCISCRT_LIGHTING_USAGE_CD,
    es.EPCISCRT_LIGHTING_ELECT_USAGE_NAME,
    es.EPCISCRT_LIGHTING_ELECT_USAGE_BRI,
    es.EPCISCRT_LIGHTING_USAGE_FAMILY_DETAIL,
    es.EPCISCRT_POWER_SA_ID,
    es.EPCISCRT_POWER_START_USE_DATE,
    es.EPCISCRT_POWER_END_USE_DATE,
    es.EPCISCRT_POWER_CRT_STATUS,
    es.EPCISCRT_POWER_RESN_DISCONTION_USE,
    es.EPCISCRT_POWER_CRT_TYPE,
    es.EPCISCRT_POWER_CRT_TYPE_NAME,
    es.EPCISCRT_POWER_CRT_TYPE_COMBIN_SINGLE,
    es.EPCISCRT_POWER_SHRI_METHOD,
    es.EPCISCRT_POWER_SHRI_METHOD_NAME,
    es.EPCISCRT_POWER_PRICE_MENU,
    es.EPCISCRT_POWER_PRICE_MENU_NAME,
    es.EPCISCRT_POWER_CRT_ELECT_CURRENT,
    es.EPCISCRT_POWER_CRT_CAPACITY,
    es.EPCISCRT_POWER_CRT_ELECT_POWER,
    es.EPCISCRT_POWER_CRT_ELECT_POWER_KW,
    es.EPCISCRT_POWER_USAGE_CD,
    es.EPCISCRT_POWER_ELECT_USAGE_NAME,
    es.EPCISCRT_POWER_ELECT_USAGE_BRI,
    es.EPCISCRT_POWER_ELECT_USAGE_FAMILY_DETAIL,
    es.EPCISCRT_ELECT_UNDER_CRT_FLG,
    es.EPCISCRT_ELECT_CANCELLED_FLG,
    es.EPCISCRT_LIGHTING_CONTRACT_COUNT,
    es.EPCISCRT_POWER_CONTRACT_CRT_COUNT,
    es.EPCISCRT_LIGHTING_TOTAL_KW,
    es.EPCISCRT_POWER_CONTRACT_TOTAL_KW,

    -- Yearly Billing columns
    yb.EPCISCLM_2018_DNRKSIYO,
    yb.EPCISCLM_2018_DNRKRYO,
    yb.EPCISCLM_2019_DNRKSIYO,
    yb.EPCISCLM_2019_DNRKRYO,
    yb.EPCISCLM_2020_DNRKSIYO,
    yb.EPCISCLM_2020_DNRKRYO,
    yb.EPCISCLM_2021_DNRKSIYO,
    yb.EPCISCLM_2021_DNRKRYO,
    yb.CISDWH_DISTRICT_CD,
    yb.CISDWH_CALORIE_CORRECTION_USAGE_1YEAR,
    yb.CISDWH_SALES_AMOUNT_1YEAR,
    yb.CISDWH_2018_GASSIYO,
    yb.CISDWH_2018_GASRYO,
    yb.CISDWH_2019_GASSIYO,
    yb.CISDWH_2019_GASRYO,
    yb.CISDWH_2020_GASSIYO,
    yb.CISDWH_2020_GASRYO,
    yb.CISDWH_2021_GASSIYO,
    yb.CISDWH_2021_GASRYO,
    yb.EPCISRPN_START_APPLI_FLG,
    yb.EPCISRPN_START_ELECT_RECEPT_DBRI,
    yb.EPCISRPN_START_ELECT_RECEPT_SBRI,
    yb.EPCISRPN_START_RECEPT_DT,
    yb.EPCISRPN_SUSPNTIN_RECEPT_FLG,
    yb.EPCISRPN_SUSPNTIN_ELECT_RECEPT_MAJOR_CLS,
    yb.EPCISRPN_SUSPNTIN_ELECT_RECEPT_MINOR_CLS,
    yb.EPCISRPN_SUSPNTIN_RECEPT_DT,

    -- Analysis & Service columns (抜粋、実際は全カラム)
    as1.LIV1CSWK_OPENING_FLG_1YEAR,
    as1.LIV1CSWK_CLOSING_FLG_1YEAR,
    as1.LIV1CSWK_SL_FLG_1YEAR,
    as1.WEBHIS_REFERENCE_DATE,
    as1.MATHPROC_YEAR_BUILT,
    as1.DEMOGRAPHIC_ESTIMATED_NUM_HOUSEHOLDS,
    as1.MYTG_MYTG_FLG,
    as1.HEAD_HOUSEHOLD_AGE,
    as1.STRATEGIC_SEGMENT,
    as1.CUSTOMER_STAGE,
    as1.REC_REG_YMD,
    as1.REC_UPD_YMD

FROM basic_info bi
    INNER JOIN equipment_service es ON bi.CLIENT_KEY_AX = es.CLIENT_KEY_AX
    INNER JOIN yearly_billing yb ON bi.CLIENT_KEY_AX = yb.CLIENT_KEY_AX
    INNER JOIN analysis_service as1 ON bi.CLIENT_KEY_AX = as1.CLIENT_KEY_AX
;
