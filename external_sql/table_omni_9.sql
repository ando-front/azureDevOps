--------------------------------------------------------------------------------------------------------------
-- MA向けリスト連携 開栓後の支払方法のご案内（開栓者全量連携） 出力 at_CreateCSV_OpeningPaymentGuide
--------------------------------------------------------------------------------------------------------------

DECLARE @today_jst datetime;
SET @today_jst=CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time');

DECLARE @from20days varchar(8);
SET @from20days=format(dateadd(day,-20, @today_jst),'yyyyMMdd'); -- 実行日JSTから20日前

DECLARE @to5days varchar(8);
SET @to5days=format(dateadd(day,-5,@today_jst),'yyyyMMdd');      -- 実行日JSTから5日前


-- ①作業テーブル作成
TRUNCATE TABLE [omni].[omni_ods_ma_trn_opened1x_temp];

INSERT INTO [omni].[omni_ods_ma_trn_opened1x_temp]
SELECT
     ANKEN_NO                -- 案件番号
    ,GASMETER_SETTI_BASYO_NO -- ガスメータ設置場所番号
    ,SAGYO_YMD               -- 作業年月日
FROM [omni].[omni_ods_livalit_trn_liv5_opening_basics]
WHERE TENKEN_JUTAKU_KBN='1'                  -- TG小売り
      AND KAISEN_JIYU_CD in ('11','12')      -- 11:代替,12新設
      AND SAGYO_YMD>=@from20days
      AND SAGYO_YMD<@to5days
      AND ERROR_FOLLOW_JOTAI_CD in('0','2')  -- 0：正常,2:フォロー済
;


-- ②作業テーブル作成
TRUNCATE TABLE [omni].[omni_ods_ma_trn_opening_target_temp];

INSERT INTO [omni].[omni_ods_ma_trn_opening_target_temp]
SELECT
     temp1.*
    ,CONVERT(VARCHAR(11), A.SYO_KYO_TRNO)  as SYO_KYO_TRNO      -- 使用者契約お客さま登録番号
    ,CONVERT(VARCHAR(11), A.SHSY_KYO_TRNO) as SHSY_KYO_TRNO     -- 支払者契約お客さま登録番号
    ,NULL as SIH_HUHU_SHBT    -- A.SIH_HUHU_SHBT as SIH_HUHU_SHBT    -- 支払方法種別　...出力から無くし固定のNULLとする
FROM [omni].[omni_ods_ma_trn_opened1x_temp] temp1    -- ①作業テーブル
INNER JOIN [omni].[omni_odm_gascstmr_trn_gaskiy] A   -- ガス契約（契約中）日次お客さま情報
      ON temp1.GASMETER_SETTI_BASYO_NO=CONVERT(VARCHAR(11),A.MTST_NO)
--WHERE A.SHIK_SIH_KIY_JTKB<>'00'   -- 無効ではない
--      AND A.SIK_SVC_SHBT not in ('11','14','15','21','13')   -- (11,14,15(一括),21(分割),13(郵送一括:オーナー))ではない　...条件削除
;


-- ③結果出力 => 開栓後の支払方法のご案内
SET @today_jst=CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time');  -- 最新の日時

SELECT
     Bx
    ,INDEX_ID
    ,ANKEN_NO                   -- 案件番号
    ,GASMETER_SETTI_BASYO_NO    -- ガスメータ設置場所番号
    ,SAGYO_YMD                  -- 作業年月日
    ,SYO_KYO_TRNO               -- 使用者契約お客さま登録番号
    ,SHSY_KYO_TRNO              -- 支払者契約お客さま登録番号
    --,SIH_HUHU_SHBT              -- 支払方法種別　...出力から無くす
    ,format(@today_jst, 'yyyy/MM/dd HH:mm:ss') as OUTPUT_DATETIME  -- IF出力日時
FROM(
    SELECT
         A.Bx
        ,A.INDEX_ID
        ,temp2.*
        ,ROW_NUMBER() OVER(PARTITION BY Bx,ANKEN_NO,GASMETER_SETTI_BASYO_NO,SAGYO_YMD ORDER BY OUTPUT_DATE desc) as rown -- INDEX_IDを除くカラムごとのOUTPUT_DATEの降順
    FROM [omni].[omni_ods_cloak_trn_usageservice] A               -- 利用サービス
    INNER JOIN [omni].[omni_ods_ma_trn_opening_target_temp] temp2 -- ②作業テーブル
          ON A.USER_KEY=temp2.SYO_KYO_TRNO      -- 使用者契約お客さま登録番号
            AND A.USER_KEY_TYPE='003'   -- お客さま番号3x
            AND A.SERVICE_TYPE ='001'   -- ガス
            AND A.TRANSFER_TYPE='02'    -- 提供中
)tmp
WHERE rown=1
;
