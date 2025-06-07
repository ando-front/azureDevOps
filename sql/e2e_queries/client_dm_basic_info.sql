----------------------------------------------------------------------
-- 顧客DM基本情報部分
-- LIV0EU系、定保区分、機器情報など基本的な顧客情報
----------------------------------------------------------------------

SELECT [顧客キー_Ax] AS CLIENT_KEY_AX
      , [LIV0EU_ガスメータ設置場所番号＿１ｘ] AS LIV0EU_1X
      , [LIV0EU_カスタマ番号＿８ｘ] AS LIV0EU_8X
      , [LIV0EU_受持ＮＷ箇所コード] AS LIV0EU_UKMT_NW_PART_CD
      , [LIV0EU_受持ＮＷ箇所_法人名] AS LIV0EU_UKMT_NW_PART_CORP_NAME
      , [LIV0EU_新社集計箇所コード] AS LIV0EU_NEW_CORP_AGG_CD
      , [LIV0EU_適用開始年月日] AS LIV0EU_APPLY_START_YMD
      , [LIV0EU_適用終了年月日] AS LIV0EU_APPLY_END_YMD
      , [LIV0EU_使用契約番号＿４ｘ] AS LIV0EU_4X
      , [LIV0EU_支払契約番号＿２ｘ] AS LIV0EU_2X
      , [LIV0EU_郵便番号] AS LIV0EU_ZIP_CD
      , [LIV0EU_都道府県名漢字] AS LIV0EU_PERF_KJ
      , [LIV0EU_行政区画コード] AS LIV0EU_GYOK_CD
      , [LIV0EU_行政区名] AS LIV0EU_GYOK_NAME
      , [LIV0EU_町コード] AS LIV0EU_STREET_CD
      , [LIV0EU_町名] AS LIV0EU_STREET_MAME
      , [LIV0EU_丁目＿字コード] AS LIV0EU_CITY_BLOCK_CD
      , [LIV0EU_建物番号] AS LIV0EU_BLD_NO
      , [LIV0EU_新設年月] AS LIV0EU_SHINSETSU_YM
      , [LIV0EU_供内管設備状態コード] AS LIV0EU_INNER_TUBE_EQP_STATE_CD
      , [LIV0EU_ガスメータ開閉栓状態コード] AS LIV0EU_GAS_METER_CLOSING_CD
      , [LIV0EU_用途コード] AS LIV0EU_USAGE_CD
      , [LIV0EU_ガス用途_集合・戸建分類] AS LIV0EU_GAS_USAGE_AP_DT_CLASS
      , [LIV0EU_ガス用途_大分類] AS LIV0EU_GAS_USAGE_MAJOR_CLASS
      , [LIV0EU_ガス用途_中分類] AS LIV0EU_GAS_USAGE_MIDIUM_CLASS
      , [LIV0EU_ガス用途_小分類] AS LIV0EU_GAS_USAGE_MINOR_CLASS
      , [LIV0EU_ガス用途_家庭用詳細] AS LIV0EU_GAS_USAGE_HOME_USE_DETAIL
      , [LIV0EU_ガス用途_業務用詳細] AS LIV0EU_GAS_USAGE_BIZ_USE_DETAIL
      , [LIV0EU_ガス用途_12セグメント分類] AS LIV0EU_GAS_USAGE_12_SEGMENT_CLASS
      , [LIV0EU_ガス用途_都市エネ大分類] AS LIV0EU_GAS_USAGE_TOSHI_ENE_MAJOR_CLASS
      , [LIV0EU_ガス用途_都市エネ小分類] AS LIV0EU_GAS_USAGE_TOSHI_ENE_MINOR_CLASS
      , [LIV0EU_ガス用途_都市エネ官公庁フラグ] AS LIV0EU_GAS_USAGE_TOSHI_ENE_PUBOFF_FLG
      , [LIV0EU_メータ号数流量] AS LIV0EU_METER_NUMBER_FLOW_RATE
      , [LIV0EU_検針方法名称] AS LIV0EU_READING_METER_METHOD_NAME
      , [LIV0EU_ガス料金支払方法コード] AS LIV0EU_GAS_PAY_METHOD_CD
      , [LIV0EU_ガス料金支払方法] AS LIV0EU_GAS_PAY_METHOD
      , [LIV0EU_料金Ｇコード] AS LIV0EU_CHARGE_G_CD
      , [LIV0EU_料金Ｇエリア区分コード] AS LIV0EU_CHARGE_G_AREA_TYPE_CD
      , [LIV0EU_ブロック番号] AS LIV0EU_BLOCK_NUMBER
      , [LIV0EU_ガス料金契約種別コード] AS LIV0EU_GAS_SHRI_CRT_SHBT_CD
      , [LIV0EU_ガス料金契約種別コード契約種名] AS LIV0EU_GAS_SHRI_CRT_SHBT_CD_NAME
      , [LIV0EU_ガス料金契約種別コード契約種細目名] AS LIV0EU_GAS_SHRI_CRT_SHBT_CD_CRT_DETAILS
      , [LIV0EU_管理課所名称] AS LIV0EU_KSY_NAME
      , [LIV0EU_定保メトロサイン] AS LIV0EU_TEIHO_METRO_SIGN
      , [LIV0EU_定保メトロサイン名称] AS LIV0EU_TEIHO_METRO_SIGN_NAME
      , [LIV0EU_定保在宅状況名称] AS LIV0EU_TEIHO_HOUSING_SITUATION_NAME
      , [LIV0EU_開栓事由名称] AS LIV0EU_OPENING_REASON_NAME
      , [LIV0EU_住宅施主区分名称] AS LIV0EU_HOME_OWNER_TYPE_NAME
      , [LIV0EU_住宅所有区分名称] AS LIV0EU_HOME_OWNERSHIP_TYPE_NAME
      , [LIV0EU_ＣＩＳ＿ＤＭ拒否サイン] AS LIV0EU_CIS_DM_REFUAL_SING
      , [LIV0EU_ＣＩＳ＿サービス情報有無コード] AS LIV0EU_CIS_SERVICE_UM_CD
      , [LIV0EU_前年総使用量] AS LIV0EU_LSYR_TOTAL_USE
      , [LIV0EU_多使用需要家フラグ] AS LIV0EU_HEAVY_USE_JYUYOUKA_FLG
      , [LIV0EU_受持ＮＷ箇所_ライフバル・エネスタ分類] AS LIV0EU_INCHANGE_NW_LIFEVAL_ENESUTA_BRI
      , [LIV0EU_受持ＮＷ箇所_ライフバル・エネスタ名] AS LIV0EU_INCHANGE_NW_LIFEVAL_ENESUTA_NAME
      , [LIV0EU_開栓後経過月] AS LIV0EU_OPENING_MONTH_PASSED
      , [LIV0EU_開栓後経過月カテゴリ] AS LIV0EU_OPENING_MONTH_PASSED_CATEGORY
      , [定保区分] AS TEIHO_TYPE
FROM [omni].[顧客DM]
