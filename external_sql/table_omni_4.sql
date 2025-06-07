---------------------------------------------------------
-- at_CreateCSV_MovingPromotionList
-- 引越し予測2か月以内リスト.csv.gz出力用データ抽出
---------------------------------------------------------

-- 出力日時
DECLARE @today_jst datetime;
SET @today_jst=CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time');

DECLARE @outDT varchar(20);
SET @outDT=format(@today_jst,'yyyy/MM/dd HH:mm:ss');


-- 出力用データ抽出
SELECT distinct
     cdna.LIV0EU_1X as GASMETER_INSTALLATION_LOCATION_NUMBER_1X    -- LIV0EU_ガスメータ設置場所番号＿１ｘ
    ,cdna.LIV0EU_4X as USAGE_CONTRACT_NUMBER_4X                    -- LIV0EU_使用契約番号＿４ｘ
    ,cdna.TNKYSK_2MONTH as MOVING_FORECAST_WITHIN_TWO_MONTHS       -- 転居予測_2か月以内
    ,@outDT as OUTPUT_DATETIME                                     -- IF出力日時
FROM [omni].[omni_ods_marketing_trn_client_dna] cdna          -- 顧客DNA

INNER JOIN [omni].[omni_odm_ma_trn_under_contract_temp] tmp   -- ガス契約中お客さま情報_temp
      ON     cdna.LIV0EU_1X=tmp.MTST_NO
         and cdna.LIV0EU_4X=tmp.SVKY_NO

WHERE   cdna.LIV0EU_1X is not NULL
    and cdna.LIV0EU_4X is not NULL
    and cdna.TNKYSK_2MONTH is not NULL
;
