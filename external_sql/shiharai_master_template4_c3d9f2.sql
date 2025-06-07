----------------------------------------------------------------------------
-- MA向けリスト連携 支払マスタ 出力 at_CreateCSV_PaymentMethodMaster
----------------------------------------------------------------------------

DECLARE @today VARCHAR(8);
SET @today=FORMAT(DATEADD(HOUR,9,GETDATE()),'yyyyMMdd ');

DECLARE @today_jst datetime;
SET @today_jst=CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time');
-- 最新の日時
DECLARE @outDT varchar(20);
SET @outDT=format(@today_jst, 'yyyy/MM/dd HH:mm:ss')

----------------------------------------------------------------------------

SELECT
    Bx
    , INDEX_ID
    , SIH_KIY_NO         -- 支払契約番号
    , SHIK_SIH_KIY_JTKB  -- 支払契約状態区分
    , SIH_HUHU_SHBT      -- 支払方法種別
    , SIK_SVC_SHBT       -- 請求サービス種別

    , @outDT as OUTPUT_DATETIME
-- 出力日付
FROM (
    SELECT usag.Bx                -- Bx
          , usag.INDEX_ID          -- インデックス番号
          , gas.SIH_KIY_NO         -- 支払契約番号
          , gas.SHIK_SIH_KIY_JTKB  -- 支払契約状態区分
          , gas.SIH_HUHU_SHBT      -- 支払方法種別
          , gas.SIK_SVC_SHBT       -- 請求サービス種別
          , ROW_NUMBER() OVER(PARTITION BY Bx,SIH_KIY_NO ORDER BY OUTPUT_DATE desc) as rown
    FROM [omni].[omni_ods_cloak_trn_usageservice] usag -- BxリストA
        INNER JOIN [omni].[omni_odm_gascstmr_trn_gaskiy] gas -- ODM.ガス契約（契約中）日次お客さま情報
        ON  usag.USER_KEY = CONVERT(varchar,gas.SYO_KYO_TRNO) --使用者契約お客さま登録番号
            AND usag.USER_KEY_TYPE = '003' -- 003：お客さま番号3x
            AND usag.SERVICE_TYPE =  '001' -- 001：ガス
            AND usag.TRANSFER_TYPE = '02'                         -- 02:提供中
)tmp
WHERE rown=1
;
