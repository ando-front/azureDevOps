----------------------------------------------------------------------
-- 顧客DM年度データ・請求情報部分
-- 電力CIS請求情報、CISDWH情報、各年度データ（削除対象カラムのNULL対応含む）
----------------------------------------------------------------------

SELECT [顧客キー_Ax] AS CLIENT_KEY_AX
      -- 2018-2020年度の削除対象カラム（NULL設定）
      , NULL AS EPCISCLM_2018_DNRKSIYO   -- カラムが削除のためNULLとする
      , NULL AS EPCISCLM_2018_DNRKRYO    -- カラムが削除のためNULLとする
      , NULL AS EPCISCLM_2019_DNRKSIYO   -- カラムが削除のためNULLとする
      , NULL AS EPCISCLM_2019_DNRKRYO    -- カラムが削除のためNULLとする
      , NULL AS EPCISCLM_2020_DNRKSIYO   -- カラムが削除のためNULLとする
      , NULL AS EPCISCLM_2020_DNRKRYO    -- カラムが削除のためNULLとする
      
      -- 2021年度データ（有効）
      , [電力CIS請求情報_2021年度電気使用量] AS EPCISCLM_2021_DNRKSIYO
      , [電力CIS請求情報_2021年度電気料金] AS EPCISCLM_2021_DNRKRYO
      , [CISDWH_地区コード] AS CISDWH_DISTRICT_CD
      , [CISDWH_熱量補正使用量_直近1年] AS CISDWH_CALORIE_CORRECTION_USAGE_1YEAR
      , [CISDWH_販売金額_直近1年] AS CISDWH_SALES_AMOUNT_1YEAR
      
      -- CISDWH 2018-2020年度の削除対象カラム（NULL設定）
      , NULL AS CISDWH_2018_GASSIYO   -- カラムが削除のためNULLとする
      , NULL AS CISDWH_2018_GASRYO    -- カラムが削除のためNULLとする
      , NULL AS CISDWH_2019_GASSIYO   -- カラムが削除のためNULLとする
      , NULL AS CISDWH_2019_GASRYO    -- カラムが削除のためNULLとする
      , NULL AS CISDWH_2020_GASSIYO   -- カラムが削除のためNULLとする
      , NULL AS CISDWH_2020_GASRYO    -- カラムが削除のためNULLとする
      
      -- CISDWH 2021年度データ（有効）
      , [CISDWH_2021年度ガス使用量] AS CISDWH_2021_GASSIYO
      , [CISDWH_2021年度ガス料金] AS CISDWH_2021_GASRYO
      
      -- 電力CIS受付情報
      , [電力CIS受付情報_開始申込フラグ] AS EPCISRPN_START_APPLI_FLG
      , [電力CIS受付情報_開始電力受付方法（大分類）] AS EPCISRPN_START_ELECT_RECEPT_DBRI
      , [電力CIS受付情報_開始電力受付方法（小分類）] AS EPCISRPN_START_ELECT_RECEPT_SBRI
      , [電力CIS受付情報_開始受付日] AS EPCISRPN_START_RECEPT_DT
      , [電力CIS受付情報_中止申込フラグ] AS EPCISRPN_SUSPNTIN_RECEPT_FLG
      , [電力CIS受付情報_中止電力受付方法（大分類）] AS EPCISRPN_SUSPNTIN_ELECT_RECEPT_MAJOR_CLS
      , [電力CIS受付情報_中止電力受付方法（小分類）] AS EPCISRPN_SUSPNTIN_ELECT_RECEPT_MINOR_CLS
      , [電力CIS受付情報_中止受付日] AS EPCISRPN_SUSPNTIN_RECEPT_DT
FROM [omni].[顧客DM]
