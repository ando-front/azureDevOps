----------------------------------------------------------------------
-- omni.顧客DM(Marketingスキーマ)のODS化
-- 
-- 2024/02/02　新規作成
-- 2024/04/22　レイアウト変更に伴う、カラム追加（391行目～446行目）
-- 2024/06/03　6月の削除対象カラムをNULLに置き換えとする暫定対応
-- 2024/08/07　Maketingスキーマ.顧客DMからomni.顧客DMに参照先を変更
-- 2024/09/05　2カラムWEB履歴_1062_SS_Web見積_直近1か月フラグとWEB履歴_1062_SS_Web見積_直近1年フラグのカラム名変更対応で暫定的にNULLを設定
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
      ,[LIV0EU_ガス料金支払方法] AS LIV0EU_GAS_PAY_METHOD
      ,[LIV0EU_料金Ｇコード] AS LIV0EU_CHARGE_G_CD
      ,[LIV0EU_料金Ｇエリア区分コード] AS LIV0EU_CHARGE_G_AREA_TYPE_CD
      ,[LIV0EU_ブロック番号] AS LIV0EU_BLOCK_NUMBER
      ,[LIV0EU_ガス料金契約種別コード] AS LIV0EU_GAS_SHRI_CRT_SHBT_CD
      ,[LIV0EU_ガス料金契約種別コード契約種名] AS LIV0EU_GAS_SHRI_CRT_SHBT_CD_NAME
      ,[LIV0EU_ガス料金契約種別コード契約種細目名] AS LIV0EU_GAS_SHRI_CRT_SHBT_CD_CRT_DETAILS
      ,[LIV0EU_管理課所名称] AS LIV0EU_KSY_NAME
      ,[LIV0EU_定保メトロサイン] AS LIV0EU_TEIHO_METRO_SIGN
      ,[LIV0EU_定保メトロサイン名称] AS LIV0EU_TEIHO_METRO_SIGN_NAME
      ,[LIV0EU_定保在宅状況名称] AS LIV0EU_TEIHO_HOUSING_SITUATION_NAME
      ,[LIV0EU_開栓事由名称] AS LIV0EU_OPENING_REASON_NAME
      ,[LIV0EU_住宅施主区分名称] AS LIV0EU_HOME_OWNER_TYPE_NAME
      ,[LIV0EU_住宅所有区分名称] AS LIV0EU_HOME_OWNERSHIP_TYPE_NAME
      ,[LIV0EU_ＣＩＳ＿ＤＭ拒否サイン] AS LIV0EU_CIS_DM_REFUAL_SING
      ,[LIV0EU_ＣＩＳ＿サービス情報有無コード] AS LIV0EU_CIS_SERVICE_UM_CD
      ,[LIV0EU_前年総使用量] AS LIV0EU_LSYR_TOTAL_USE
      ,[LIV0EU_多使用需要家フラグ] AS LIV0EU_HEAVY_USE_JYUYOUKA_FLG
      ,[LIV0EU_受持ＮＷ箇所_ライフバル・エネスタ分類] AS LIV0EU_INCHANGE_NW_LIFEVAL_ENESUTA_BRI
      ,[LIV0EU_受持ＮＷ箇所_ライフバル・エネスタ名] AS LIV0EU_INCHANGE_NW_LIFEVAL_ENESUTA_NAME
      ,[LIV0EU_開栓後経過月] AS LIV0EU_OPENING_MONTH_PASSED
      ,[LIV0EU_開栓後経過月カテゴリ] AS LIV0EU_OPENING_MONTH_PASSED_CATEGORY
      ,[定保区分] AS TEIHO_TYPE
      ,[LIV0共有所有機器_コンロ種別] AS LIV0SPD_KONRO_SHBT
      ,[LIV0共有所有機器_コンロ種別名] AS LIV0SPD_KONRO_SHBT_NAME
      ,[LIV0共有所有機器_コンロ_所有機器番号] AS LIV0SPD_KONRO_POSSESSION_EQUIPMENT_NO
      ,[LIV0共有所有機器_コンロ_販売先コード] AS LIV0SPD_KONRO_SL_DEST_CD
      ,[LIV0共有所有機器_コンロ_製造年月] AS LIV0SPD_KONRO_DT_MANUFACTURE
      ,[LIV0共有所有機器_コンロ_経年数] AS LIV0SPD_KONRO_NUM_YEAR
      ,[LIV0共有所有機器_コンロ_経年数カテゴリ] AS LIV0SPD_KONRO_NUM_YEAR_CATEGORY
      ,[LIV0共有所有機器_給湯機種別] AS LIV0SPD_KYTK_SHBT
      ,[LIV0共有所有機器_給湯機種名称] AS LIV0SPD_KYTK_NAME
      ,[LIV0共有所有機器_給湯器_所有機器番号] AS LIV0SPD_KYTK_POSSESSION_DEVICE_NO
      ,[LIV0共有所有機器_給湯器_販売先コード] AS LIV0SPD_KYTK_SL_DESTINATION_CD
      ,[LIV0共有所有機器_給湯器_製造年月] AS LIV0SPD_KYTK_DT_MANUFACTURE
      ,[LIV0共有所有機器_給湯器_経年数] AS LIV0SPD_KYTK_NUM_YEAR
      ,[LIV0共有所有機器_給湯器_経年数カテゴリ] AS LIV0SPD_KYTK_NUM_YEAR_CATEGORY
      ,[LIV0共有所有機器_210_炊飯器] AS LIV0SPD_210_RICE_COOKER
      ,[LIV0共有所有機器_220_レンジ] AS LIV0SPD_220_MICROWAVE
      ,[LIV0共有所有機器_230_オーブン] AS LIV0SPD_230_OVEN
      ,[LIV0共有所有機器_240_コンベック] AS LIV0SPD_240_CONVECTION_OVEN
      ,[LIV0共有所有機器_250_コンビネーションレンジ] AS LIV0SPD_250_COMBINATION_MICROWAVE
      ,[LIV0共有所有機器_300_乾燥機ほか] AS LIV0SPD_300_DRYING_MACHINE
      ,[LIV0共有所有機器_310_ドライヤー（衣類乾燥機）] AS LIV0SPD_310_DRIER
      ,[LIV0共有所有機器_410_金網ストーブ] AS LIV0SPD_410_WIRE_MESH_STOVE
      ,[LIV0共有所有機器_420_開放型ストーブ] AS LIV0SPD_420_OPEN_STOVE
      ,[LIV0共有所有機器_430_ＦＨストーブ] AS LIV0SPD_430_FH_STOVE
      ,[LIV0共有所有機器_440_ＢＦストーブ] AS LIV0SPD_440_BF_STOVE
      ,[LIV0共有所有機器_450_ＦＦストーブ] AS LIV0SPD_450_FF_STOVE
      ,[LIV0共有所有機器_460_ＦＥストーブ] AS LIV0SPD_460_FE_STOVE
--      ,[TES熱源機情報_製造番号] AS TESHSMC_SERIAL_NUMBER
      ,NULL AS TESHSMC_SERIAL_NUMBER -- TES熱源機情報_製造番号　カンマを含むためNULLに設定
      ,[TES熱源機情報_システム種別] AS TESHSMC_SYSTEM_SHBT
      ,[TES熱源機情報_システム落成日] AS TESHSEQ_SYSTEM_COMPLETION_DATE
      ,[TES熱源機情報_メンテ区分コード] AS TESHSEQ_MAINTENANCE_TYPE_CD
      ,[TES熱源機情報_所有形態コード] AS TESHSEQ_OWNER_TYPE_CD
      ,[TES熱源機情報_取付年月日] AS TESHSEQ_ATTACHMENT_YMD
      ,[TES熱源機情報_取外年月日] AS TESHSEQ_REMOVAL_YMD
      ,[TES熱源機情報_TESメンテ加入資格有無] AS TESHSEQ_TES_MAINT_ELIGIBILITY
      ,[TES放熱器情報_床暖房有無] AS TESHRDTR_UNDER_FLOOR_HEATING
      ,[TES放熱器情報_バス暖房有無] AS TESHRDTR_BATH_HEATING
      ,[TESサービス情報_TESメンテ加入有無] AS TESSV_MAINTENANCE_JOIN
      ,[TESサービス情報_サービス番号] AS TESSV_SERVICE_ID
      ,[TESサービス情報_サービス満了年月] AS TESSV_SERVICE_EXPIRATION_YM
      ,[TESサービス情報_サービス種別] AS TESSV_SERVICE_SHBT
      ,[警報機情報_警報器台数] AS ARMC_COUNT
      ,[警報機情報_警報器取外し済みフラグ] AS ARMC_REMOVED_FLG
      ,[警報機情報_警報器リース中フラグ] AS ARMC_UNDER_LEASE_FLG
      ,[警報機情報_警報器リース終了フラグ] AS ARMC_LEASE_END_FLG
      ,[電力CIS契約情報_お客様登録番号_3x] AS EPCISCRT_3X
      ,[電力CIS契約情報_電灯_電力契約番号] AS EPCISCRT_LIGHTING_SA_ID
      ,[電力CIS契約情報_電灯_使用開始日] AS EPCISCRT_LIGHTING_START_USE_DATE
      ,[電力CIS契約情報_電灯_使用中止日] AS EPCISCRT_LIGHTING_END_USE_DATE
      ,[電力CIS契約情報_電灯_契約ステータス] AS EPCISCRT_LIGHTING_CRT_STATUS
      ,[電力CIS契約情報_電灯_使用中止理由] AS EPCISCRT_LIGHTING_RESN_DISCONTION_USE
      ,[電力CIS契約情報_電灯_契約タイプ] AS EPCISCRT_LIGHTING_CRT_TYPE
      ,[電力CIS契約情報_電灯_契約タイプ_名称] AS EPCISCRT_LIGHTING_CRT_TYPE_NAME
      ,[電力CIS契約情報_電灯_契約タイプ_合算・単独分類] AS EPCISCRT_LIGHTING_CRT_TYPE_COMBIN_SINGLE
      ,[電力CIS契約情報_電灯_支払方法] AS EPCISCRT_LIGHTING_SHRI_METHOD
      ,[電力CIS契約情報_電灯_支払方法名] AS EPCISCRT_LIGHTING_SHRI_METHOD_NAME
      ,[電力CIS契約情報_電灯_料金メニュー] AS EPCISCRT_LIGHTING_PRICE_MENU
      ,[電力CIS契約情報_電灯_料金メニュー名] AS EPCISCRT_LIGHTING_PRICE_MENU_NAME
      ,[電力CIS契約情報_電灯_契約電流] AS EPCISCRT_LIGHTING_CRT_ELECT_CURRENT
      ,[電力CIS契約情報_電灯_契約容量] AS EPCISCRT_LIGHTING_CRT_CAPACITY
      ,[電力CIS契約情報_電灯_契約電力] AS EPCISCRT_LIGHTING_CRT_ELECT_POWER
      ,[電力CIS契約情報_電灯_契約電力kW換算値] AS EPCISCRT_LIGHTING_CRT_ELECT_POWER_KW
      ,[電力CIS契約情報_電灯_用途コード] AS EPCISCRT_LIGHTING_USAGE_CD
      ,[電力CIS契約情報_電灯_電気用途名] AS EPCISCRT_LIGHTING_ELECT_USAGE_NAME
      ,[電力CIS契約情報_電灯_電気用途分類] AS EPCISCRT_LIGHTING_ELECT_USAGE_BRI
      ,[電力CIS契約情報_電灯_電気用途家庭用詳細] AS EPCISCRT_LIGHTING_USAGE_FAMILY_DETAIL
      ,[電力CIS契約情報_動力_電力契約番号] AS EPCISCRT_POWER_SA_ID
      ,[電力CIS契約情報_動力_使用開始日] AS EPCISCRT_POWER_START_USE_DATE
      ,[電力CIS契約情報_動力_使用中止日] AS EPCISCRT_POWER_END_USE_DATE
      ,[電力CIS契約情報_動力_契約ステータス] AS EPCISCRT_POWER_CRT_STATUS
      ,[電力CIS契約情報_動力_使用中止理由] AS EPCISCRT_POWER_RESN_DISCONTION_USE
      ,[電力CIS契約情報_動力_契約タイプ] AS EPCISCRT_POWER_CRT_TYPE
      ,[電力CIS契約情報_動力_契約タイプ_名称] AS EPCISCRT_POWER_CRT_TYPE_NAME
      ,[電力CIS契約情報_動力_契約タイプ_合算・単独分類] AS EPCISCRT_POWER_CRT_TYPE_COMBIN_SINGLE
      ,[電力CIS契約情報_動力_支払方法] AS EPCISCRT_POWER_SHRI_METHOD
      ,[電力CIS契約情報_動力_支払方法名] AS EPCISCRT_POWER_SHRI_METHOD_NAME
      ,[電力CIS契約情報_動力_料金メニュー] AS EPCISCRT_POWER_PRICE_MENU
      ,[電力CIS契約情報_動力_料金メニュー名] AS EPCISCRT_POWER_PRICE_MENU_NAME
      ,[電力CIS契約情報_動力_契約電流] AS EPCISCRT_POWER_CRT_ELECT_CURRENT
      ,[電力CIS契約情報_動力_契約容量] AS EPCISCRT_POWER_CRT_CAPACITY
      ,[電力CIS契約情報_動力_契約電力] AS EPCISCRT_POWER_CRT_ELECT_POWER
      ,[電力CIS契約情報_動力_契約電力kW換算値] AS EPCISCRT_POWER_CRT_ELECT_POWER_KW
      ,[電力CIS契約情報_動力_用途コード] AS EPCISCRT_POWER_USAGE_CD
      ,[電力CIS契約情報_動力_電気用途名] AS EPCISCRT_POWER_ELECT_USAGE_NAME
      ,[電力CIS契約情報_動力_電気用途分類] AS EPCISCRT_POWER_ELECT_USAGE_BRI
      ,[電力CIS契約情報_動力_電気用途家庭用詳細] AS EPCISCRT_POWER_ELECT_USAGE_FAMILY_DETAIL
      ,[電力CIS契約情報_電力契約中フラグ] AS EPCISCRT_ELECT_UNDER_CRT_FLG
      ,[電力CIS契約情報_電力解約済みフラグ] AS EPCISCRT_ELECT_CANCELLED_FLG
      ,[電力CIS契約情報_電灯契約数] AS EPCISCRT_LIGHTING_CONTRACT_COUNT
      ,[電力CIS契約情報_動力契約数] AS EPCISCRT_POWER_CONTRACT_CRT_COUNT
      ,[電力CIS契約情報_電灯合計kW] AS EPCISCRT_LIGHTING_TOTAL_KW
      ,[電力CIS契約情報_動力合計kW] AS EPCISCRT_POWER_CONTRACT_TOTAL_KW
      
--      ,[電力CIS請求情報_2018年度電気使用量] AS EPCISCLM_2018_DNRKSIYO
--      ,[電力CIS請求情報_2018年度電気料金] AS EPCISCLM_2018_DNRKRYO
--      ,[電力CIS請求情報_2019年度電気使用量] AS EPCISCLM_2019_DNRKSIYO
--      ,[電力CIS請求情報_2019年度電気料金] AS EPCISCLM_2019_DNRKRYO
--      ,[電力CIS請求情報_2020年度電気使用量] AS EPCISCLM_2020_DNRKSIYO
--      ,[電力CIS請求情報_2020年度電気料金] AS EPCISCLM_2020_DNRKRYO
      ,NULL AS EPCISCLM_2018_DNRKSIYO   -- カラムが削除のためNULLとする
      ,NULL AS EPCISCLM_2018_DNRKRYO    -- カラムが削除のためNULLとする
      ,NULL AS EPCISCLM_2019_DNRKSIYO   -- カラムが削除のためNULLとする
      ,NULL AS EPCISCLM_2019_DNRKRYO    -- カラムが削除のためNULLとする
      ,NULL AS EPCISCLM_2020_DNRKSIYO   -- カラムが削除のためNULLとする
      ,NULL AS EPCISCLM_2020_DNRKRYO    -- カラムが削除のためNULLとする
      
      ,[電力CIS請求情報_2021年度電気使用量] AS EPCISCLM_2021_DNRKSIYO
      ,[電力CIS請求情報_2021年度電気料金] AS EPCISCLM_2021_DNRKRYO
      ,[CISDWH_地区コード] AS CISDWH_DISTRICT_CD
      ,[CISDWH_熱量補正使用量_直近1年] AS CISDWH_CALORIE_CORRECTION_USAGE_1YEAR
      ,[CISDWH_販売金額_直近1年] AS CISDWH_SALES_AMOUNT_1YEAR
      
--      ,[CISDWH_2018年度ガス使用量] AS CISDWH_2018_GASSIYO
--      ,[CISDWH_2018年度ガス料金] AS CISDWH_2018_GASRYO
--      ,[CISDWH_2019年度ガス使用量] AS CISDWH_2019_GASSIYO
--      ,[CISDWH_2019年度ガス料金] AS CISDWH_2019_GASRYO
--      ,[CISDWH_2020年度ガス使用量] AS CISDWH_2020_GASSIYO
--      ,[CISDWH_2020年度ガス料金] AS CISDWH_2020_GASRYO
      ,NULL AS CISDWH_2018_GASSIYO   -- カラムが削除のためNULLとする
      ,NULL AS CISDWH_2018_GASRYO    -- カラムが削除のためNULLとする
      ,NULL AS CISDWH_2019_GASSIYO   -- カラムが削除のためNULLとする
      ,NULL AS CISDWH_2019_GASRYO    -- カラムが削除のためNULLとする
      ,NULL AS CISDWH_2020_GASSIYO   -- カラムが削除のためNULLとする
      ,NULL AS CISDWH_2020_GASRYO    -- カラムが削除のためNULLとする
      
      ,[CISDWH_2021年度ガス使用量] AS CISDWH_2021_GASSIYO
      ,[CISDWH_2021年度ガス料金] AS CISDWH_2021_GASRYO
      ,[電力CIS受付情報_開始申込フラグ] AS EPCISRPN_START_APPLI_FLG
      ,[電力CIS受付情報_開始電力受付方法（大分類）] AS EPCISRPN_START_ELECT_RECEPT_DBRI
      ,[電力CIS受付情報_開始電力受付方法（小分類）] AS EPCISRPN_START_ELECT_RECEPT_SBRI
      ,[電力CIS受付情報_開始受付日] AS EPCISRPN_START_RECEPT_DT
      ,[電力CIS受付情報_中止申込フラグ] AS EPCISRPN_SUSPNTIN_RECEPT_FLG
      ,[電力CIS受付情報_中止電力受付方法（大分類）] AS EPCISRPN_SUSPNTIN_ELECT_RECEPT_MAJOR_CLS
      ,[電力CIS受付情報_中止電力受付方法（小分類）] AS EPCISRPN_SUSPNTIN_ELECT_RECEPT_MINOR_CLS
      ,[電力CIS受付情報_中止受付日] AS EPCISRPN_SUSPNTIN_RECEPT_DT
      ,[LIV1案件業務_開栓フラグ_1年以内] AS LIV1CSWK_OPENING_FLG_1YEAR
      ,[LIV1案件業務_閉栓フラグ_1年以内] AS LIV1CSWK_CLOSING_FLG_1YEAR
      ,[LIV1案件業務_販売フラグ_1年以内] AS LIV1CSWK_SL_FLG_1YEAR
      ,[LIV1案件業務_機器修理・点検フラグ_1年以内] AS LIV1CSWK_KKSR_FLG_1YEAR
      ,[LIV1案件業務_能動的接点活動フラグ_1年以内] AS LIV1CSWK_ACA_FLG_1YEAR
      ,[LIV1案件業務_アフターフォローフラグ_1年以内] AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_1YEAR
      ,[LIV1案件業務_その他フラグ_1年以内] AS LIV1CSWK_OTHER_FLG_1YEAR
      ,[LIV1案件業務_案件全般フラグ_1年以内] AS LIV1CSWK_CASE_ALL_ASPECT_FLG_1YEAR
      ,[LIV1案件業務_開栓フラグ_2年以内] AS LIV1CSWK_OPENING_FLG_2YEAR
      ,[LIV1案件業務_閉栓フラグ_2年以内] AS LIV1CSWK_CLOSED_FLG_2YEAR
      ,[LIV1案件業務_販売フラグ_2年以内] AS LIV1CSWK_SL_FLG_2YEAR
      ,[LIV1案件業務_機器修理・点検フラグ_2年以内] AS LIV1CSWK_KKSR_FLG_2YEAR
      ,[LIV1案件業務_能動的接点活動フラグ_2年以内] AS LIV1CSWK_ACA_FLG_2YEAR
      ,[LIV1案件業務_アフターフォローフラグ_2年以内] AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2YEAR
      ,[LIV1案件業務_その他フラグ_2年以内] AS LIV1CSWK_OTHER_FLG_2YEAR
      ,[LIV1案件業務_案件全般フラグ_2年以内] AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2YEAR
      
--      ,[LIV1案件業務_開栓フラグ_2018年度] AS LIV1CSWK_OPENING_FLG_2018
--      ,[LIV1案件業務_閉栓フラグ_2018年度] AS LIV1CSWK_CLOSED_FLG_2018
--      ,[LIV1案件業務_販売フラグ_2018年度] AS LIV1CSWK_SL_FLG_2018
--      ,[LIV1案件業務_機器修理・点検フラグ_2018年度] AS LIV1CSWK_KKSR_FLG_2018
--      ,[LIV1案件業務_能動的接点活動フラグ_2018年度] AS LIV1CSWK_ACA_FLG_2018
--      ,[LIV1案件業務_アフターフォローフラグ_2018年度] AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2018
--      ,[LIV1案件業務_その他フラグ_2018年度] AS LIV1CSWK_OTHER_FLG_2018
--      ,[LIV1案件業務_案件全般フラグ_2018年度] AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2018
--      ,[LIV1案件業務_開栓フラグ_2019年度] AS LIV1CSWK_OPENING_FLG_2019
--      ,[LIV1案件業務_閉栓フラグ_2019年度] AS LIV1CSWK_CLOSED_FLG_2019
--      ,[LIV1案件業務_販売フラグ_2019年度] AS LIV1CSWK_SL_FLG_2019
--      ,[LIV1案件業務_機器修理・点検フラグ_2019年度] AS LIV1CSWK_KKSR_FLG_2019
--      ,[LIV1案件業務_能動的接点活動フラグ_2019年度] AS LIV1CSWK_ACA_FLG_2019
--      ,[LIV1案件業務_アフターフォローフラグ_2019年度] AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2019
--      ,[LIV1案件業務_その他フラグ_2019年度] AS LIV1CSWK_OTHER_FLG_2019
--      ,[LIV1案件業務_案件全般フラグ_2019年度] AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2019
--      ,[LIV1案件業務_開栓フラグ_2020年度] AS LIV1CSWK_OPENING_FLG_2020
--      ,[LIV1案件業務_閉栓フラグ_2020年度] AS LIV1CSWK_CLOSED_FLG_2020
--      ,[LIV1案件業務_販売フラグ_2020年度] AS LIV1CSWK_SL_FLG_2020
--      ,[LIV1案件業務_機器修理・点検フラグ_2020年度] AS LIV1CSWK_KKSR_FLG_2020
--      ,[LIV1案件業務_能動的接点活動フラグ_2020年度] AS LIV1CSWK_ACA_FLG_2020
--      ,[LIV1案件業務_アフターフォローフラグ_2020年度] AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2020
--      ,[LIV1案件業務_その他フラグ_2020年度] AS LIV1CSWK_OTHER_FLG_2020
--      ,[LIV1案件業務_案件全般フラグ_2020年度] AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2020
      ,NULL AS LIV1CSWK_OPENING_FLG_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_CLOSED_FLG_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_SL_FLG_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_KKSR_FLG_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_ACA_FLG_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_OTHER_FLG_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_OPENING_FLG_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_CLOSED_FLG_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_SL_FLG_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_KKSR_FLG_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_ACA_FLG_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_OTHER_FLG_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_OPENING_FLG_2020   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_CLOSED_FLG_2020   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_SL_FLG_2020   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_KKSR_FLG_2020   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_ACA_FLG_2020   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2020   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_OTHER_FLG_2020   -- カラムが削除のためNULLとする
      ,NULL AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2020   -- カラムが削除のためNULLとする
      
      ,[LIV1案件業務_開栓フラグ_2021年度] AS LIV1CSWK_OPENING_FLG_2021
      ,[LIV1案件業務_閉栓フラグ_2021年度] AS LIV1CSWK_CLOSED_FLG_2021
      ,[LIV1案件業務_販売フラグ_2021年度] AS LIV1CSWK_SL_FLG_2021
      ,[LIV1案件業務_機器修理・点検フラグ_2021年度] AS LIV1CSWK_KKSR_FLG_2021
      ,[LIV1案件業務_能動的接点活動フラグ_2021年度] AS LIV1CSWK_ACTIVE_CONTACT_ACTIVIT_FLG_2021
      ,[LIV1案件業務_アフターフォローフラグ_2021年度] AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2021
      ,[LIV1案件業務_その他フラグ_2021年度] AS LIV1CSWK_OTHER_FLG_2021
      ,[LIV1案件業務_案件全般フラグ_2021年度] AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2021
      ,[LIV1案件業務_開栓フラグ_全期間] AS LIV1CSWK_OPENING_FLG
      ,[LIV1案件業務_閉栓フラグ_全期間] AS LIV1CSWK_CLOSED_FLG
      ,[LIV1案件業務_販売フラグ_全期間] AS LIV1CSWK_SL_FLG
      ,[LIV1案件業務_機器修理・点検フラグ_全期間] AS LIV1CSWK_KKSR_FLG
      ,[LIV1案件業務_能動的接点活動フラグ_全期間] AS LIV1CSWK_ACA_FLG
      ,[LIV1案件業務_アフターフォローフラグ_全期間] AS LIV1CSWK_AF_FLG
      ,[LIV1案件業務_その他フラグ_全期間] AS LIV1CSWK_OTHER_FLG
      ,[LIV1案件業務_案件全般フラグ_全期間] AS LIV1CSWK_CASE_ALL_ASPECT_FLG
      ,[LIV2落成実績販売_ガス機器全般販売フラグ_1年以内] AS LIV2ACMTSL_GASKIKI_ALL_SL_FLG_1YEAR
      ,[LIV2落成実績販売_機器全般販売フラグ_1年以内] AS LIV2ACMTSL_EQUIPMENT_ALL_SL_FLG_1YEAR
      ,[LIV2落成実績販売_住設機器・リフォーム全般販売フラグ_1年以内] AS LIV2ACMTSL_HOUSE_FCLTS_ALL_SL_FLG_1YEAR
      ,[LIV2落成実績販売_オール販売フラグ_1年以内] AS LIV2ACMTSL_ALLSL_FLG_1YEAR
      ,[LIV2落成実績販売_ガス機器全般販売フラグ_2年以内] AS LIV2ACMTSL_GASKIKI_ALL_SL_FLG_2YEAR
      ,[LIV2落成実績販売_機器全般販売フラグ_2年以内] AS LIV2ACMTSL_EQUIPMENT_ALL_SL_FLG_2YEAR
      ,[LIV2落成実績販売_住設機器・リフォーム全般販売フラグ_2年以内] AS LIV2ACMTSL_HOUSE_FCLTS_ALL_SL_FLG_2YEAR
      ,[LIV2落成実績販売_オール販売フラグ_2年以内] AS LIV2ACMTSL_ALLSL_FLG_2YEAR
      
--      ,[LIV2落成実績販売_ガス機器全般販売フラグ_2018年度] AS LIV2ACMTSL_GASKIKI_ALL_SL_FLG_2018
--      ,[LIV2落成実績販売_機器全般販売フラグ_2018年度] AS LIV2ACMTSL_EQUIPMENT_ALL_SL_FLG_2018
--      ,[LIV2落成実績販売_住設機器・リフォーム全般販売フラグ_2018年度] AS LIV2ACMTSL_HOUSE_FCLTS_ALL_SL_FLG_2018
--      ,[LIV2落成実績販売_オール販売フラグ_2018年度] AS LIV2ACMTSL_ALLSL_FLG_2018
--      ,[LIV2落成実績販売_ガス機器全般販売フラグ_2019年度] AS LIV2ACMTSL_GASKIKI_ALL_SL_FLG_2019
--      ,[LIV2落成実績販売_機器全般販売フラグ_2019年度] AS LIV2ACMTSL_EQUIPMENT_ALL_SL_FLG_2019
--      ,[LIV2落成実績販売_住設機器・リフォーム全般販売フラグ_2019年度] AS LIV2ACMTSL_HOUSE_FCLTS_ALL_SL_FLG_2019
--      ,[LIV2落成実績販売_オール販売フラグ_2019年度] AS LIV2ACMTSL_ALLSL_FLG_2019
--      ,[LIV2落成実績販売_ガス機器全般販売フラグ_2020年度] AS LIV2ACMTSL_GASKIKI_ALL_SL_FLG_2020
--      ,[LIV2落成実績販売_機器全般販売フラグ_2020年度] AS LIV2ACMTSL_EQUIPMENT_ALL_SL_FLG_2020
--      ,[LIV2落成実績販売_住設機器・リフォーム全般販売フラグ_2020年度] AS LIV2ACMTSL_HOUSE_FCLTS_ALL_SL_FLG_2020
--      ,[LIV2落成実績販売_オール販売フラグ_2020年度] AS LIV2ACMTSL_ALLSL_FLG_2020
      ,NULL AS LIV2ACMTSL_GASKIKI_ALL_SL_FLG_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV2ACMTSL_EQUIPMENT_ALL_SL_FLG_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV2ACMTSL_HOUSE_FCLTS_ALL_SL_FLG_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV2ACMTSL_ALLSL_FLG_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV2ACMTSL_GASKIKI_ALL_SL_FLG_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV2ACMTSL_EQUIPMENT_ALL_SL_FLG_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV2ACMTSL_HOUSE_FCLTS_ALL_SL_FLG_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV2ACMTSL_ALLSL_FLG_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV2ACMTSL_GASKIKI_ALL_SL_FLG_2020   -- カラムが削除のためNULLとする
      ,NULL AS LIV2ACMTSL_EQUIPMENT_ALL_SL_FLG_2020   -- カラムが削除のためNULLとする
      ,NULL AS LIV2ACMTSL_HOUSE_FCLTS_ALL_SL_FLG_2020   -- カラムが削除のためNULLとする
      ,NULL AS LIV2ACMTSL_ALLSL_FLG_2020   -- カラムが削除のためNULLとする
      
      ,[LIV2落成実績販売_ガス機器全般販売フラグ_2021年度] AS LIV2ACMTSL_GASKIKI_ALL_SL_FLG_2021
      ,[LIV2落成実績販売_機器全般販売フラグ_2021年度] AS LIV2ACMTSL_EQUIPMENT_ALL_SL_FLG_2021
      ,[LIV2落成実績販売_住設機器・リフォーム全般販売フラグ_2021年度] AS LIV2ACMTSL_HOUSE_FCLTS_ALL_SL_FLG_2021
      ,[LIV2落成実績販売_オール販売フラグ_2021年度] AS LIV2ACMTSL_ALLSL_FLG_2021
      ,[LIV2落成実績販売_TES_EJ_販売フラグ_全期間] AS LIV2ACMTSL_TES_EJ_SL_FLG
      ,[LIV2落成実績販売_風呂給_EJ_販売フラグ_全期間] AS LIV2ACMTSL_BATH_EJ_SL_FLG
      ,[LIV2落成実績販売_大湯_EJ_販売フラグ_全期間] AS LIV2ACMTSL_OOYU_EJ_SL_FLG
      ,[LIV2落成実績販売_TES_標準_販売フラグ_全期間] AS LIV2ACMTSL_TES_STD_SL_FLG
      ,[LIV2落成実績販売_風呂給_標準_販売フラグ_全期間] AS LIV2ACMTSL_BATH_STD_SL_FLG
      ,[LIV2落成実績販売_大湯_標準_販売フラグ_全期間] AS LIV2ACMTSL_OOYU_STD_SL_FLG
      ,[LIV2落成実績販売_その他風呂販売フラグ_全期間] AS LIV2ACMTSL_OTHER_BATH_SL_FLG
      ,[LIV2落成実績販売_小型湯沸器販売フラグ_全期間] AS LIV2ACMTSL_SMALL_WATER_HEATER_SL_FLG
      ,[LIV2落成実績販売_衣類乾燥機販売フラグ_全期間] AS LIV2ACMTSL_CLOTHES_DRYER_SL_FLG
      ,[LIV2落成実績販売_ビルトインコンロ_ピピッと_販売フラグ_全期間] AS LIV2ACMTSL_BUILD_IN_KONRO_PPT_SL_FLG
      ,[LIV2落成実績販売_ビルトインコンロ_ピピッと以外_販売フラグ_全期間] AS LIV2ACMTSL_BUILD_IN_KONRO_NPPT_SL_FLG
      ,[LIV2落成実績販売_テーブルコンロ_ピピッと_販売フラグ_全期間] AS LIV2ACMTSL_TABLE_KONRO_PPT_SL_FLG
      ,[LIV2落成実績販売_テーブルコンロ_ピピッと以外_販売フラグ_全期間] AS LIV2ACMTSL_TABLE_KONRO_NPPT_SL_FLG
      ,[LIV2落成実績販売_ファンヒータ販売フラグ_全期間] AS LIV2ACMTSL_FAN_HEATER_SL_FLG
      ,[LIV2落成実績販売_床暖房販売フラグ_全期間] AS LIV2ACMTSL_FLOOR_HEATER_SL_FLG
      ,[LIV2落成実績販売_ミストサウナ販売フラグ_全期間] AS LIV2ACMTSL_MIST_SAUNA_SL_FLG
      ,[LIV2落成実績販売_浴室暖房乾燥機販売フラグ_全期間] AS LIV2ACMTSL_BATHROOM_HEATRE_SL_FLG
      ,[LIV2落成実績販売_警報器販売フラグ_全期間] AS LIV2ACMTSL_ALARM_SL_FLG
      ,[LIV2落成実績販売_炊飯器販売フラグ_全期間] AS LIV2ACMTSL_RICE_COOKER_SL_FLG
      ,[LIV2落成実績販売_FF暖房機販売フラグ_全期間] AS LIV2ACMTSL_FFHU_SL_FLG
      ,[LIV2落成実績販売_一般ストーブ販売フラグ_全期間] AS LIV2ACMTSL_STD_STOVE_SL_FLG
      ,[LIV2落成実績販売_ビルトインレンジ販売フラグ_全期間] AS LIV2ACMTSL_BUILD_IN_MICROWAVE_SL_FLG
      ,[LIV2落成実績販売_高速オーブン販売フラグ_全期間] AS LIV2ACMTSL_HIGH_SPEED_OVEN_SL_FLG
      ,[LIV2落成実績販売_食器洗い乾燥機販売フラグ_全期間] AS LIV2ACMTSL_DISHWASHER_SL_FLG
      ,[LIV2落成実績販売_エコウイル販売フラグ_全期間] AS LIV2ACMTSL_ECO_WILL_SL_FLG
      ,[LIV2落成実績販売_エネファーム販売フラグ_全期間] AS LIV2ACMTSL_ENEFARM_SL_FLG
      ,[LIV2落成実績販売_暖房専用放熱器販売フラグ_全期間] AS LIV2ACMTSL_HEATING_ONLY_RADIATOR_SL_FLG
      ,[LIV2落成実績販売_TESエアコン室内機販売フラグ_全期間] AS LIV2ACMTSL_TES_AIR_CON_INDOOR_UN_SL_FLG
      ,[LIV2落成実績販売_SOLAMO販売フラグ_全期間] AS LIV2ACMTSL_SOLAMO_SL_FLG
      ,[LIV2落成実績販売_ガス機器全般販売フラグ_全期間] AS LIV2ACMTSL_GASKIKI_GENERAL_SL_FLG
      ,[LIV2落成実績販売_機器全般販売フラグ_全期間] AS LIV2ACMTSL_EQUIPMENT_GENERAL_SL_FLG
      ,[LIV2落成実績販売_ユニットバス販売フラグ_全期間] AS LIV2ACMTSL_UNIT_SL_FLG
      ,[LIV2落成実績販売_システムキッチン販売フラグ_全期間] AS LIV2ACMTSL_SYSETM_KITCHEN_SL_FLG
      ,[LIV2落成実績販売_換気扇フード販売フラグ_全期間] AS LIV2ACMTSL_RANGE_HOOD_SL_FLG
      ,[LIV2落成実績販売_洗面化粧台販売フラグ_全期間] AS LIV2ACMTSL_WASHBASIN_SL_FLG
      ,[LIV2落成実績販売_トイレ販売フラグ_全期間] AS LIV2ACMTSL_TOILET_SL_FLG
      ,[LIV2落成実績販売_エアコン販売フラグ_全期間] AS LIV2ACMTSL_AIR_CON_SL_FLG
      ,[LIV2落成実績販売_太陽光システム販売フラグ_全期間] AS LIV2ACMTSL_SUN_LIGHT_SYSTEM_SL_FLG
      ,[LIV2落成実績販売_住設機器・リフォーム全般販売フラグ_全期間] AS LIV2ACMTSL_HOUSE_FCLTS_RNV_ALL_SL_FLG
      ,[LIV2落成実績販売_オール販売フラグ_全期間] AS LIV2ACMTSL_ALL_SL_FLG
      ,[LIV2落成実績税抜実販売金額_ガス機器全般販売_税抜実販売価格合算_1年以内] AS LIV2AMTAECSL_GASKIKI_ALL_SL_PRICE_1YEAR
      ,[LIV2落成実績税抜実販売金額_機器全般販売_税抜実販売価格合算_1年以内] AS LIV2AMTAECSL_EQUIPMENT_ALL_SLPRICE_1YEAR
      
--      ,[LIV2落成実績税抜実販売金額_ガス機器全般販売_税抜実販売価格合算_2018年度] AS LIV2AMTAECSL_GASKIKI_ALL_SL_PRICE_2018
--      ,[LIV2落成実績税抜実販売金額_機器全般販売_税抜実販売価格合算_2018年度] AS LIV2AMTAECSL_EQUIPMENT_ALL_SL_PRICE_2018
--      ,[LIV2落成実績税抜実販売金額_ガス機器全般販売_税抜実販売価格合算_2019年度] AS LIV2AMTAECSL_GASKIKI_ALL_SL_PRICE_2019
--      ,[LIV2落成実績税抜実販売金額_機器全般販売_税抜実販売価格合算_2019年度] AS LIV2AMTAECSL_EQUIPMENT_ALL_SL_PRICE_2019
--      ,[LIV2落成実績税抜実販売金額_ガス機器全般販売_税抜実販売価格合算_2020年度] AS LIV2AMTAECSL_GASKIKI_ALL_SL_PRICE_2020
--      ,[LIV2落成実績税抜実販売金額_機器全般販売_税抜実販売価格合算_2020年度] AS LIV2AMTAECSL_EQUIPMENT_ALL_SL_PRICE_2020
      ,NULL AS LIV2AMTAECSL_GASKIKI_ALL_SL_PRICE_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV2AMTAECSL_EQUIPMENT_ALL_SL_PRICE_2018   -- カラムが削除のためNULLとする
      ,NULL AS LIV2AMTAECSL_GASKIKI_ALL_SL_PRICE_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV2AMTAECSL_EQUIPMENT_ALL_SL_PRICE_2019   -- カラムが削除のためNULLとする
      ,NULL AS LIV2AMTAECSL_GASKIKI_ALL_SL_PRICE_2020   -- カラムが削除のためNULLとする
      ,NULL AS LIV2AMTAECSL_EQUIPMENT_ALL_SL_PRICE_2020   -- カラムが削除のためNULLとする
      
      ,[LIV2落成実績税抜実販売金額_ガス機器全般販売_税抜実販売価格合算_2021年度] AS LIV2AMTAECSL_GASKIKI_ALL_SL_PRICE_2021
      ,[LIV2落成実績税抜実販売金額_機器全般販売_税抜実販売価格合算_2021年度] AS LIV2AMTAECSL_EQUIPMENT_ALL_SL_PRICE_2021
      ,[LIV2落成実績税抜実販売金額_ガス機器全般販売_税抜実販売価格合算_全期間] AS LIV2AMTAECSL_GASKIKI_ALL_SL_PRICE
      ,[LIV2落成実績税抜実販売金額_機器全般販売_税抜実販売価格合算_全期間] AS LIV2AMTAECSL_EQUIPMENT_ALL_SL_PRICE_EXC

--      ,[COSMOS設備_支払契約番号] AS COSMOS_PAYMENT_CNTRACT_ID
--      ,[COSMOS設備_契約種別] AS COSMOS_CNTRACT_SHBT
--      ,[COSMOS設備_契約設定理由] AS COSMOS_CNTRACT_SETTING_REASON
--      ,[COSMOS設備_契約箇所] AS COSMOS_CNTRACT_PART
--      ,[COSMOS設備_マイツーホー契約フラグ] AS COSMOS_MY24_CNTRACT_FLG
--      ,[COSMOS設備_マイツーホー契約日] AS COSMOS_MY24_CNTRACT_DT
--      ,[COSMOS設備_くらし見守り契約フラグ] AS COSMOS_WATCH_OVER_CNTRACT_FLG
--      ,[COSMOS設備_くらし見守り契約日] AS COSMOS_WATCH_OVER_CNTRACT_DT
--      ,[COSMOS設備_サービス開始年月日] AS COSMOS_SERVICE_START_YMD
--      ,[COSMOS設備_マイツーホー解約フラグ] AS COSMOS_MY24_CANCELL_CNTRACT_FLG
--      ,[COSMOS設備_マイツーホー解約日] AS COSMOS_MY24_CANCELL_CNTRACT_DT
--      ,[COSMOS設備_くらし見守り解約フラグ] AS COSMOS_WATCH_OVER_CANCELL_CRT_FLG
--      ,[COSMOS設備_くらし見守り解約日] AS COSMOS_WATCH_OVER_CANCELL_CRT_DT
      ,NULL AS COSMOS_PAYMENT_CNTRACT_ID   -- カラムが削除のためNULLとする
      ,NULL AS COSMOS_CNTRACT_SHBT   -- カラムが削除のためNULLとする
      ,NULL AS COSMOS_CNTRACT_SETTING_REASON   -- カラムが削除のためNULLとする
      ,NULL AS COSMOS_CNTRACT_PART   -- カラムが削除のためNULLとする
      ,NULL AS COSMOS_MY24_CNTRACT_FLG   -- カラムが削除のためNULLとする
      ,convert(date,NULL) AS COSMOS_MY24_CNTRACT_DT   -- カラムが削除のためNULLとする
      ,NULL AS COSMOS_WATCH_OVER_CNTRACT_FLG   -- カラムが削除のためNULLとする
      ,convert(date,NULL) AS COSMOS_WATCH_OVER_CNTRACT_DT   -- カラムが削除のためNULLとする
      ,convert(date,NULL) AS COSMOS_SERVICE_START_YMD   -- カラムが削除のためNULLとする
      ,NULL AS COSMOS_MY24_CANCELL_CNTRACT_FLG   -- カラムが削除のためNULLとする
      ,convert(date,NULL) AS COSMOS_MY24_CANCELL_CNTRACT_DT   -- カラムが削除のためNULLとする
      ,NULL AS COSMOS_WATCH_OVER_CANCELL_CRT_FLG   -- カラムが削除のためNULLとする
      ,convert(date,NULL) AS COSMOS_WATCH_OVER_CANCELL_CRT_DT   -- カラムが削除のためNULLとする
      
      ,[数理加工情報_築年数] AS MATHPROC_YEAR_BUILT
      ,[数理加工情報_築年グループ名] AS MATHPROC_YEAR_BUILT_GROUP_NAME
      ,[数理加工情報_分譲賃貸分類] AS MATHPROC_SALE_RENT_CLASS
      ,[数理加工情報_年間ガス使用量] AS MATHPROC_ANN_GAS_USAGE
      ,[数理加工情報_年間ガス使用量CATG] AS MATHPROC_ANN_GAS_USAGE_CATG
      ,[数理加工情報_年間ガス使用量_直近1年_50m刻み] AS MATHPROC_ANN_GAS_USAGE_1YEAR_50M_INC
      ,[数理加工情報_年間ガス使用量_直近1年_業務用区分] AS MATHPROC_ANN_GAS_USAGE_1YEAR_GYOMU_TYPE
      ,[数理加工情報_推定電力使用量（ガス使用量などからの推計）] AS MATHPROC_ESTI_DNRK_USAGE
      ,[数理加工情報_推定電力使用量CATG] AS MATHPROC_ESTI_DNRK_USAGE_CATG
      ,[電力使用量（最終的な推計値）] AS ELECT_USAGE
      ,[電力使用量（最終的な推計値）_500kWh刻み] AS ELECT_USAGE_500KW_INC
      ,[myTG会員情報_myTGフラグ] AS MYTG_MYTG_FLG
      ,[myTG会員情報_myTG会員登録日] AS MYTG_MEMBER_REGIST_DT
      ,[myTG会員情報_myTG会員ID] AS MYTG_MTGID
      ,[myTG会員情報_myTG性別] AS MYTG_SEX
      ,[myTG会員情報_myTG年代] AS MYTG_AGE
      ,[ずっともガス総メリット額（セットポイント込）] AS ZUTOMO_GAS_TOTAL_ADVANTAGES_AMOUNT
      ,[ずっともガス総メリットフラグ（セットポイント込）] AS ZUTOMO_GAS_TOTAL_ADVANTAGES_FLG
      ,[ずっとも電気総メリット額（ポイント込）] AS ZUTOMO_ELECT_TOTAL_ADVANTAGES_AMOUNT
      ,[ずっとも電気総メリットフラグ（ポイント込)] AS ZUTOMO_ELECT_TOTAL_ADVANTAGES_FLG
      ,[燃焼確認_実施フラグ] AS CMBSTNCK_ENFCMNT_FLG
      
--      ,[燃焼確認_2018年度実施フラグ] AS CMBSTNCK_ENFCMNT_FLG_2018
--      ,[燃焼確認_2019年度実施フラグ] AS CMBSTNCK_ENFCMNT_FLG_2019
--      ,[燃焼確認_2020年度実施フラグ] AS CMBSTNCK_ENFCMNT_FLG_2020
      ,NULL AS CMBSTNCK_ENFCMNT_FLG_2018   -- カラムが削除のためNULLとする
      ,NULL AS CMBSTNCK_ENFCMNT_FLG_2019   -- カラムが削除のためNULLとする
      ,NULL AS CMBSTNCK_ENFCMNT_FLG_2020   -- カラムが削除のためNULLとする
      
      ,[燃焼確認_2021年度実施フラグ] AS CMBSTNCK_ENFCMNT_FLG_2021
      ,[燃焼確認_直近実施日] AS CMBSTNCK_MOST_RECENT_ENFCMNT_DT
      ,[トラブルサポート利用フラグ] AS TROUBLE_SUPPORT_USE_FLG
      ,[駆けつけサービス利用フラグ] AS KAKETSUKE_SERVICE_USE_FLG
      ,[住まサポ分析_受付フラグ] AS SUMASUPP_RECEPT_FLG
      ,[住まサポ分析_最新受付チャネル] AS SUMASUPP_LATEST_RECEPT_CHANNELS
      ,[住まサポ分析_落成フラグ（案件番号）] AS SUMASUPP_CASE_ID
      ,[住まサポ分析_落成フラグ] AS SUMASUPP_COMPLETION_FLG
      ,[ガス機器SS_加入申込フラグ] AS GASDVCSS_SUB_APPLICATE_FLG
      ,[ガス機器SS_契約中フラグ] AS GASDVCSS_UNDER_CONTRUCT_FLG
      ,[ガス機器SS_解約済フラグ] AS GASDVCSS_CANCELLED_FLG
      ,[ガス機器SS_OP_電気エアコン_契約中フラグ] AS GASDVCSS_OP_ELECT_AIRCON_UNDER_CRT_FLG
      ,[ガス機器SS_OP_水まわり_契約中フラグ] AS GASDVCSS_OP_WATER_UNDER_CRT_FLG
      ,[ガス機器SS_OP_電気設備_契約中フラグ] AS GASDVCSS_OP_ELECT_EQP_UNDER_CRT_FLG
      ,[MDBデータ_管理会社名] AS MDBDATA_MGMT_CORP_NAME
      ,[MDBデータ_建物構造] AS MDBDATA_BLDG_STRUCT
      ,[MDBデータ_総戸数] AS MDBDATA_TOTAL_NUMBER_HOUSES
      ,[公共建物リスト_公共物件区分] AS PUBBLDGLS_PUB_BLDG_TYPE
      ,[取次店ガス加入者フラグ] AS AGENCY_GAS_MEMBER_FLG
      ,[取次店名] AS AGENCY_NAME
      ,[ハウスクリーニング_ハウスクリーニング利用実績フラグ] AS HSCLNG_USE_FLG
      ,[ハウスクリーニング_ハウスクリーニング利用実績フラグ_直近1年] AS HSCLNG_USE_FLG_1YEAR
      ,[ハウスクリーニング_リピーターフラグ_全期間] AS HSCLNG_REPEATER_FLG
      ,[ハウスクリーニング_リピーターフラグ_直近1年] AS HSCLNG_REPEATER_FLG_1YEAR
      ,[ハウスクリーニング_新規顧客フラグ] AS HSCLNG_NEW_CUSTOMER_FLG
      ,[ハウスクリーニング_対象エリアフラグ] AS HSCLNG_TARGET_AREA_FLG
      ,[TGCRM_世帯主年代] AS TGCRM_HEAD_HOUSEHOLD_AGE
      ,[世帯主年代（実績値または推定値）] AS HEAD_HOUSEHOLD_AGE
      ,[世帯主年代実績値フラグ] AS HEAD_HOUSEHOLD_AGE_FLG
      ,[WebShop_注文実績フラグ_全期間] AS WEBSHOP_ORDER_RESULT_FLG
      ,[WebShop_注文実績フラグ_直近1年] AS WEBSHOP_ORDER_RESULT_FLG_1YEAR
      ,[WebShop_リピーターフラグ_全期間] AS WEBSHOP_REPEATER_FLG
      ,[WebShop_リピーターフラグ_直近1年] AS WEBSHOP_REPEATER_FLG_1YEAR
      ,[WebShop_新規顧客フラグ] AS WEBSHOP_NEW_CUSTOMER_FLG
      ,[WebShop_累積注文回数_全期間] AS WEBSHOP_ACCUMLATE_ORDER_COUNT
      ,[WebShop_累積注文回数_直近1年間] AS WEBSHOP_ACCUMLATE_ORDER_COUNT_1YEAR
      ,[WEB履歴_基準日] AS WEBHIS_REFERENCE_DATE
      ,[WEB履歴_1003_GP_各種手続き_引越し_直近1か月フラグ] AS WEBHIS_1003_GP_VARPROC_MOVE_1MO_FLG
      ,[WEB履歴_1003_GP_各種手続き_引越し_直近1年フラグ] AS WEBHIS_1003_GP_VARPROC_MOVE_1YR_FLG
      ,[WEB履歴_1005_GP_各種手続き_申込切替_直近1か月フラグ] AS WEBHIS_1005_GP_VARPROC_SWAPP_1MO_FLG
      ,[WEB履歴_1005_GP_各種手続き_申込切替_直近1年フラグ] AS WEBHIS_1005_GP_VARPROC_SWAPP_1YR_FLG
      ,[WEB履歴_1011_GP_ガス・電気_さすてな_直近1か月フラグ] AS WEBHIS_1011_GP_GAS_ELECT_SASTENA_1MO_FLG
      ,[WEB履歴_1011_GP_ガス・電気_さすてな_直近1年フラグ] AS WEBHIS_1011_GP_GAS_ELECT_SASTENA_1YR_FLG
      ,[WEB履歴_1012_GP_ガス・電気_時間帯別_直近1か月フラグ] AS WEBHIS_1012_GP_GAS_ELECT_TMZONE_1MO_FLG
      ,[WEB履歴_1012_GP_ガス・電気_時間帯別_直近1年フラグ] AS WEBHIS_1012_GP_GAS_ELECT_TMZONE_1YR_FLG
      ,[WEB履歴_1019_SS_修理サービス_ガス機器_直近1か月フラグ] AS WEBHIS_1019_SS_REPSERV_GASKIKI_1MO_FLG
      ,[WEB履歴_1019_SS_修理サービス_ガス機器_直近1年フラグ] AS WEBHIS_1019_SS_REPSERV_GASKIKI_1YR_FLG
      ,[WEB履歴_1020_SS_修理サービス_水まわり_直近1か月フラグ] AS WEBHIS_1020_SS_REPSERV_WARTER_1MO_FLG
      ,[WEB履歴_1020_SS_修理サービス_水まわり_直近1年フラグ] AS WEBHIS_1020_SS_REPSERV_WARTER_1YR_FLG
      ,[WEB履歴_1021_SS_修理サービス_スペサポ_直近1か月フラグ] AS WEBHIS_1021_SS_REPSERV_SPESAPO_1MO_FLG
      ,[WEB履歴_1021_SS_修理サービス_スペサポ_直近1年フラグ] AS WEBHIS_1021_SS_REPSERV_SPESAPO_1YR_FLG
      ,[WEB履歴_1024_SS_見守りサービス_直近1か月フラグ] AS WEBHIS_1024_SS_WATCHOVERSERV_1MO_FLG
      ,[WEB履歴_1024_SS_見守りサービス_直近1年フラグ] AS WEBHIS_1024_SS_WATCHOVERSERV_1YR_FLG
      ,[WEB履歴_1031_SS_家事サービス_WS_直近1か月フラグ] AS WEBHIS_1031_SS_HOUSEKEEPSERV_WS_1MO_FLG
      ,[WEB履歴_1031_SS_家事サービス_WS_直近1年フラグ] AS WEBHIS_1031_SS_HOUSEKEEPSERV_WS_1YR_FLG
      ,[WEB履歴_1036_SS_エネルギー機器_エネファ_直近1か月フラグ] AS WEBHIS_1036_SS_ENEKIKI_ENEFA_1MO_FLG
      ,[WEB履歴_1036_SS_エネルギー機器_エネファ_直近1年フラグ] AS WEBHIS_1036_SS_ENEKIKI_ENEFA_1YR_FLG
      ,[WEB履歴_1037_SS_エネルギー機器_BTM_直近1か月フラグ] AS WEBHIS_1037_SS_ENEKIKI_BTM_1MO_FLG
      ,[WEB履歴_1037_SS_エネルギー機器_BTM_直近1年フラグ] AS WEBHIS_1037_SS_ENEKIKI_BTM_1YR_FLG
      ,[WEB履歴_1038_SS_エネルギー機器_BTM_広告LP_直近1か月フラグ] AS WEBHIS_1038_SS_ENEKIKI_BTM_ADVLP_1MO_FLG
      ,[WEB履歴_1038_SS_エネルギー機器_BTM_広告LP_直近1年フラグ] AS WEBHIS_1038_SS_ENEKIKI_BTM_ADVLP_1YR_FLG
     
--      ,[WEB履歴_1062_SS_Web見積_直近1か月フラグ] AS WEBHIS_1062_SS_WEBQUOTE_1MO_FLG
--      ,[WEB履歴_1062_SS_Web見積_直近1年フラグ] AS WEBHIS_1062_SS_WEBQUOTE_1YR_FLG
      ,NULL AS WEBHIS_1062_SS_WEBQUOTE_1MO_FLG -- 上流のカラム名が変更 暫定的にNULLを設定
      ,NULL AS WEBHIS_1062_SS_WEBQUOTE_1YR_FLG -- 上流のカラム名が変更 暫定的にNULLを設定
      
      ,[WEB履歴_1064_SS_Web見積_広告LP_直近1か月フラグ] AS WEBHIS_1064_SS_WEBQUOTE_ADVLP_1MO_FLG
      ,[WEB履歴_1064_SS_Web見積_広告LP_直近1年フラグ] AS WEBHIS_1064_SS_WEBQUOTE_ADVLP_1YR_FLG
      ,[WEB履歴_9999_全般_直近1か月フラグ] AS WEBHIS_9999_GENERAL_1MO_FLG
      ,[WEB履歴_9999_全般_直近1年フラグ] AS WEBHIS_9999_GENERAL_1YR_FLG
      ,[暮らしES基盤_MDK_顧客単位_契約中フラグ] AS LIVBASE_MDK_CUSTUNIT_UNDCONT_FLG
      ,[暮らしES基盤_MDK_顧客単位_解約済フラグ] AS LIVBASE_MDK_CUSTUNIT_CANCEL_FLG
      ,[暮らしES基盤_MDK_顧客単位_解約日] AS LIVBASE_MDK_CUSTUNIT_CANCEL_DATE
      ,[暮らしES基盤_MDK_顧客単位_申込区分] AS LIVBASE_MDK_CUSTUNIT_APP_TYPE
      ,[暮らしES基盤_MDK_顧客単位_申込日] AS LIVBASE_MDK_CUSTUNIT_APP_DATE
      ,[暮らしES基盤_EF安心定額サポート_顧客単位_契約中フラグ] AS LIVBASE_EFSECLFLATSP_CUSTUNT_UNDCONT_FLG
      ,[暮らしES基盤_EF安心定額サポート_顧客単位_解約済フラグ] AS LIVBASE_EFSECLFLATSP_CUSTUNT_CANCEL_FLG
      ,[暮らしES基盤_EF安心定額サポート_顧客単位_解約日] AS LIVBASE_EFSECLFLATSP_CUSTUNT_CANCEL_DATE
      ,[暮らしES基盤_EF安心定額サポート_顧客単位_申込区分] AS LIVBASE_EFSECLFLATSP_CUSTUNT_APP_TYPE
      ,[暮らしES基盤_EF安心定額サポート_顧客単位_申込日] AS LIVBASE_EFSECLFLATSP_CUSTUNT_APP_DATE
      ,[暮らしES基盤_くらし見守り_顧客単位_契約中フラグ] AS LIVBASE_WATCHOVERLIF_CUSTUNT_UNDCONT_FLG
      ,[暮らしES基盤_くらし見守り_顧客単位_解約済フラグ] AS LIVBASE_WATCHOVERLIF_CUSTUNT_CANCEL_FLG
      ,[暮らしES基盤_くらし見守り_顧客単位_解約日] AS LIVBASE_WATCHOVERLIF_CUSTUNT_CANCEL_DATE
      ,[暮らしES基盤_くらし見守り_顧客単位_申込区分] AS LIVBASE_WATCHOVERLIF_CUSTUNT_APP_TYPE
      ,[暮らしES基盤_くらし見守り_顧客単位_申込日] AS LIVBASE_WATCHOVERLIF_CUSTUNT_APP_DATE
      ,[暮らしES基盤_もしものたより_顧客単位_契約中フラグ] AS LIVBASE_IFSTORY_CUSTUNT_UNDCONT_FLG
      ,[暮らしES基盤_もしものたより_顧客単位_解約済フラグ] AS LIVBASE_IFSTORY_CUSTUNT_CANCEL_FLG
      ,[暮らしES基盤_もしものたより_顧客単位_解約日] AS LIVBASE_IFSTORY_CUSTUNT_CANCEL_DATE
      ,[暮らしES基盤_もしものたより_顧客単位_申込区分] AS LIVBASE_IFSTORY_CUSTUNT_APP_TYPE
      ,[暮らしES基盤_もしものたより_顧客単位_申込日] AS LIVBASE_IFSTORY_CUSTUNT_APP_DATE
      ,[暮らしES基盤_シニアケア_顧客単位_契約中フラグ] AS LIVBASE_SENCARE_CUSTUNT_UNDCONT_FLG
      ,[暮らしES基盤_シニアケア_顧客単位_解約済フラグ] AS LIVBASE_SENCARE_CUSTUNT_CANCEL_FLG
      ,[暮らしES基盤_シニアケア_顧客単位_解約日] AS LIVBASE_SENCARE_CUSTUNT_CANCEL_DATE
      ,[暮らしES基盤_シニアケア_顧客単位_申込区分] AS LIVBASE_SENCARE_CUSTUNT_APP_TYPE
      ,[暮らしES基盤_シニアケア_顧客単位_申込日] AS LIVBASE_SENCARE_CUSTUNT_APP_DATE
      ,[東京ガスの修理サービス_水まわり修理_利用フラグ_全期間] AS TGRPSV_WARTER_REPAIR_USE_FLG
      ,[東京ガスの修理サービス_水まわり修理_利用フラグ_直近1年] AS TGRPSV_WARTER_REPAIR_USE_FLG_1YEAR
      ,[東京ガスの修理サービス_水まわり修理_累積利用回数_全期間] AS TGRPSV_WARTER_REPAIR_ACU_USE_COUNT
      ,[東京ガスの修理サービス_水まわり修理_累積利用回数_直近1年以内] AS TGRPSV_WARTER_REPAIR_ACU_USE_COUNT_1YEAR
      ,[東京ガスの修理サービス_ガス機器修理_利用フラグ_全期間] AS TGRPSV_GASKIKI_REPAIR_USE_FLG
      ,[東京ガスの修理サービス_ガス機器修理_利用フラグ_直近1年] AS TGRPSV_GASKIKI_REPAIR_USE_FLG_1YEAR
      ,[東京ガスの修理サービス_ガス機器修理_累積利用回数_全期間] AS TGRPSV_GASKIKI_REPAIR_ACU_USE_COUNT
      ,[東京ガスの修理サービス_ガス機器修理_累積利用回数_直近1年以内] AS TGRPSV_GASKIKI_REPAIR_USE_COUNT_1YEAR
      ,[ｍyTG会員情報_メール開封率_全期間] AS MYTG_MAIL_OPEN_RATE
      ,[ｍyTG会員情報_メール開封率_直近1年] AS MYTG_MAIL_OPEN_RATE_1YEAR
      ,[ｍyTG会員情報_メール添付URLクリック率_全期間] AS MYTG_MAIL_URL_CLICK_RATE
      ,[ｍyTG会員情報_メール添付URLクリック率_直近1年] AS MYTG_MAIL_URL_CLICK_RATE_1YEAR
      ,[ｍyTG会員情報_定期利用フラグ_直近1年] AS MYTG_SUBSCRIBE_FLG_1YEAR
      ,[ｍyTG会員情報_定期利用フラグ_直近6ヶ月] AS MYTG_SUBSCRIBE_FLG_6MONTH
      ,[ｍyTG会員情報_アクティブ率] AS MYTG_ACTIVE_RATE
      ,[直近開栓者フラグ] AS MOST_RECENT_OPENER_FLG
      ,[戦略セグメント] AS STRATEGIC_SEGMENT
      ,[顧客ステージ] AS CUSTOMER_STAGE
      ,[推定入居区分_新築中古入居分類] AS ESTIMVINTP_NEW_USED_MOVEIN_TYPE
      ,[デモグラフィック情報_推定世帯人数] AS DEMOGRAPHIC_ESTIMATED_NUM_HOUSEHOLDS
      ,[レコード作成日時] AS REC_REG_YMD
      ,[レコード更新日時] AS REC_UPD_YMD

FROM [omni].[顧客DM]
;