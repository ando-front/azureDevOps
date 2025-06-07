--------------------------------------------------------------------------
-- 「支払アラート」作成
-- Source: src/dev/arm_template/ARMTemplateForFactory.json
-- Activity: at_CreateCSV_PaymentAlert (pi_Send_PaymentAlert)
-- Created: 2024-12-31
-- 
-- Ver 1.0 初版 (2022/09)
-- Ver 1.1 「利用サービス」の代わりに「本人特定契約」を使用する (2024/11)
--
-- Purpose: 支払期限を過ぎた未収請求のアラート情報を抽出・履歴管理・MA向け出力
--
-- Column Structure (14 columns):
-- 1. MTGID - 会員ID (2024/11 追加)
-- 2. IRIM_SQSB_NO - 依頼元請求識別番号
-- 3. SIK_NO - 請求番号
-- 4. SIK_KKT_YMD - 請求確定年月日
-- 5. RKN_MBN - 料金月分
-- 6. SQBT_SSYT_YMD - 請求媒体作成予定年月日
-- 7. SHKG_YMD - 支払期限年月日
-- 8. SHKD_HNKU_FLG - 支払期限日変更フラグ
-- 9. ETR_YYKG_YMD - 延滞利息猶予期限年月日
-- 10. CST_TUCH_NO - お客さま通知番号(3x)
-- 11. GSN_SIK_FLG - 合算請求フラグ
-- 12. SYO_KYO_TRNO - 使用者契約お客さま登録番号（4xに対する3x）
-- 13. SIH_HUHU_SHBT - 支払方法種別
-- 14. SHSY_KYO_TRNO - 支払者契約お客さま登録番号
-- 15. OUTPUT_DATETIME - 出力日付
--
-- Business Logic:
-- ①未収テーブルから支払期限過ぎた請求を抽出
-- ②ガス契約情報と結合
-- ③本人特定契約から会員ID取得 (2024/11変更)
-- ④履歴テーブルに蓄積・管理
-- ⑤請求番号ユニークでMA向け出力
--------------------------------------------------------------------------

-- ①MA向けリスト連携（支払アラート）支払アラート(全体)_temp作成
DECLARE @today VARCHAR(8);
SET @today=FORMAT(DATEADD(HOUR,9,GETDATE()),'yyyyMMdd ');

TRUNCATE TABLE [omni].[omni_ods_ma_trn_paymentalert_target_temp];

INSERT [omni].[omni_ods_ma_trn_paymentalert_target_temp]
SELECT DISTINCT IRIM_SQSB_NO           -- 依頼元請求識別番号
               , SIK_SIK_NO             -- 請求_請求番号
               , SIK_SIK_KKT_YMD        -- 請求_請求確定年月日
               , SIK_RKN_MBN            -- 請求_料金月分
               , SIK_SQBT_SSYT_YMD      -- 請求_請求媒体作成予定年月日
               , SIK_SHKG_YMD           -- 請求_支払期限年月日
               , SIK_SHKD_HNKU_FLG      -- 請求_支払期限日変更フラグ
               , SIK_ETR_YYKG_YMD       -- 請求_延滞利息猶予期限年月日
               , SIK_CST_TUCH_NO        -- 請求_お客さま通知番号(3x)
               , SIK_GSN_SIK_FLG        -- 請求_合算請求フラグ
               , SIK_KRNO               -- 請求管理番号
               , SIK_SIK_TSNO           -- 請求_請求通し番号
               , SIK_SIK_TTNO
-- 請求_請求特定番号
FROM [omni].[omni_ods_ciriuscf_trn_uncollected]
-- 未収
WHERE CNS_FLG = '0' -- 0：入金なし
    AND MTH_UM_FLG = '0' -- 0：返金なし
    AND SIK_SSHN_SQJK_KBN = '3' -- 3：請求中
    AND SIK_SHKG_YMD < @today -- 支払期限年月日  ※2022/05/11 条件を変更
    AND SIK_SIK_KNGK > 0 -- 請求金額
    AND SIK_SIH_HUHU_SHBT = '2' -- 2：払込み
    AND SIK_SIK_SVC_SHBT not in('11','14','15','21','13')
-- (11,14,15(一括),21(分割),13(郵送一括))ではない
;


-- ②MA向けリスト連携（支払アラート）ガス契約未収_temp作成
TRUNCATE TABLE [omni].[omni_ods_ma_trn_uncollected_gas_temp];

INSERT [omni].[omni_ods_ma_trn_uncollected_gas_temp]
-- ②ガス契約未収
SELECT temp.*                                             -- ①の全項目
       , CONVERT(varchar,gas.SYO_KYO_TRNO)                  -- A.使用者契約お客さま登録番号（4xに対する3x）
       , gas.SIH_HUHU_SHBT                                  -- A.支払方法種別 
       , CONVERT(varchar,gas.SHSY_KYO_TRNO)
-- A.支払者契約お客さま登録番号（2xに対する3x）
FROM [omni].[omni_ods_ma_trn_paymentalert_target_temp] temp -- ①未収_対象のみ
    INNER JOIN [omni].[omni_odm_gascstmr_trn_gaskiy] gas -- ODM.ガス契約（契約中）日次お客さま情報 A
    ON temp.IRIM_SQSB_NO = CONVERT(varchar,gas.SIH_KIY_NO)
WHERE gas.SHIK_SIH_KIY_JTKB <> '00' -- 00：無効ではない
    AND gas.SIK_SVC_SHBT NOT IN('11','14','15','21','13')
-- (11,14,15(一括),21(分割),13(郵送一括:オーナー))ではない
;


-- ③MA向けリスト連携（支払アラート）をテーブルに出力
DECLARE @today_jst datetime;
SET @today_jst=CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time');
-- 最新の日時

TRUNCATE TABLE [omni].[omni_ods_ma_trn_paymentalert];

INSERT INTO [omni].[omni_ods_ma_trn_paymentalert]
SELECT
    MTGID            -- 会員ID　追加 2024/11
    , IRIM_SQSB_NO     -- 依頼元請求識別番号
    , SIK_NO           -- 請求_請求番号
    , SIK_KKT_YMD      -- 請求_請求確定年月日
    , RKN_MBN          -- 請求_料金月分
    , SQBT_SSYT_YMD    -- 請求_請求媒体作成予定年月日
    , SHKG_YMD         -- 請求_支払期限年月日
    , SHKD_HNKU_FLG    -- 請求_支払期限日変更フラグ
    , ETR_YYKG_YMD     -- 請求_延滞利息猶予期限年月日
    , CST_TUCH_NO      -- 請求_お客さま通知番号(3x)
    , GSN_SIK_FLG      -- 請求_合算請求フラグ
    , SYO_KYO_TRNO     -- 使用者契約お客さま登録番号（4xに対する3x）
    , SIH_HUHU_SHBT    -- 支払方法種別
    , SHSY_KYO_TRNO    -- 支払者契約お客さま登録番号
    , SIK_KRNO         -- 請求管理番号
    , SIK_TSNO         -- 請求通し番号
    , SIK_TTNO         -- 請求特定番号

    , format(@today_jst, 'yyyy/MM/dd HH:mm:ss') as OUTPUT_DATETIME -- 出力日付
    , 0
-- 既存フラグ
FROM (
    SELECT cpkiyk.MTGID         -- 本人特定契約.会員ID
          , gas_temp.*           -- ②の全項目
          , ROW_NUMBER() OVER(PARTITION BY [MTGID],[IRIM_SQSB_NO],[RKN_MBN],[SIK_NO] ORDER BY [CSV_YMD] desc) as rown
    FROM [omni].[omni_ods_mytginfo_trn_cpkiyk] cpkiyk -- 本人特定契約　変更 2024/11
        INNER JOIN [omni].[omni_ods_ma_trn_uncollected_gas_temp] gas_temp -- ②ガス契約未収_temp
        ON cpkiyk.SHRKY_NO = gas_temp.IRIM_SQSB_NO -- 本人特定契約.支払契約番号 = ②.依頼元請求識別番号
            AND cpkiyk.TAIKAI_FLAG = 0                                     -- 0：現会員
)tmp
WHERE rown=1
;


--------------------------------------------------------------------------
-- ④支払アラート→支払アラート履歴
--   ダッシュボード用DM整備のため、テーブル「支払アラート履歴」に出力する

-- 「請求番号」が「支払アラート履歴」「支払アラート」に存在→既存フラグ=1
UPDATE pa
    SET EXISTING_FLAG=1    -- 既存フラグを更新
FROM [omni].[omni_ods_ma_trn_paymentalert] pa -- 支払アラート
WHERE EXISTS(SELECT 1
FROM [omni].[omni_ods_ma_trn_paymentalert_history] pah
WHERE pah.SIK_NO=pa.SIK_NO and pah.SIK_KRNO=pa.SIK_KRNO)
;

-- 「請求番号」が「支払アラート」に存在し、「支払アラート履歴」に存在しないので挿入
INSERT INTO [omni].[omni_ods_ma_trn_paymentalert_history]
SELECT
    '' as Bx             -- Bx　　　　　　　　文字長0に変更 2024/11
    , '' as INDEX_ID       -- インデックス番号　文字長0に変更 2024/11
    , IRIM_SQSB_NO         -- 依頼元請求識別番号
    , SIK_NO               -- 請求番号
    , SIK_KKT_YMD          -- 請求確定年月日
    , RKN_MBN              -- 料金月分
    , SQBT_SSYT_YMD        -- 請求媒体作成予定年月日
    , SHKG_YMD             -- 支払期限年月日
    , SHKD_HNKU_FLG        -- 支払期限日変更フラグ
    , ETR_YYKG_YMD         -- 延滞利息猶予期限年月日
    , CST_TUCH_NO          -- お客さま通知番号(3x)
    , GSN_SIK_FLG          -- 合算請求フラグ
    , SYO_KYO_TRNO         -- 使用者契約お客さま登録番号（4xに対する3x）
    , SIH_HUHU_SHBT        -- 支払方法種別
    , SHSY_KYO_TRNO        -- 支払者契約お客さま登録番号
    , SIK_KRNO             -- 請求管理番号
    , SIK_TSNO             -- 請求通し番号
    , SIK_TTNO             -- 請求特定番号
    , left(OUTPUT_DATETIME,10) -- 最古のIF出力日付
    , left(OUTPUT_DATETIME,10) -- 最新のIF出力日付
    , MTGID
-- 会員ID　追加 2024/11
FROM [omni].[omni_ods_ma_trn_paymentalert] pa
-- 支払アラート
WHERE EXISTING_FLAG=0
-- 既存フラグ
;

-- 「請求番号」が「支払アラート」と「支払アラート履歴」に存在するので「最新のIF出力日付」更新
UPDATE pah
    SET pah.LATEST_OUTPUT_DATE=left(pa.OUTPUT_DATETIME,10)    -- 最新のIF出力日付を更新
FROM [omni].[omni_ods_ma_trn_paymentalert_history] pah -- 支払アラート履歴
    INNER JOIN [omni].[omni_ods_ma_trn_paymentalert] pa -- 支払アラート
    ON pah.SIK_NO=pa.SIK_NO and pah.SIK_KRNO=pa.SIK_KRNO
WHERE pa.EXISTING_FLAG=1
-- 既存フラグ=1について更新する
;


--------------------------------------------------------------------------
-- ⑤MA向けに改めてSELECTする
SELECT
    MTGID                -- 会員ID　追加 2024/11
    , IRIM_SQSB_NO         -- 依頼元請求識別番号
    , SIK_NO               -- 請求番号
    , SIK_KKT_YMD          -- 請求確定年月日
    , RKN_MBN              -- 料金月分
    , SQBT_SSYT_YMD        -- 請求媒体作成予定年月日
    , SHKG_YMD             -- 支払期限年月日
    , SHKD_HNKU_FLG        -- 支払期限日変更フラグ
    , ETR_YYKG_YMD         -- 延滞利息猶予期限年月日
    , CST_TUCH_NO          -- お客さま通知番号(3x)
    , GSN_SIK_FLG          -- 合算請求フラグ
    , SYO_KYO_TRNO         -- 使用者契約お客さま登録番号（4xに対する3x）
    , SIH_HUHU_SHBT        -- 支払方法種別
    , SHSY_KYO_TRNO        -- 支払者契約お客さま登録番号
    , OUTPUT_DATETIME
-- 出力日付
FROM (
    SELECT
        MTGID            -- 会員ID　追加 2024/11
        , IRIM_SQSB_NO     -- 依頼元請求識別番号
        , SIK_NO           -- 請求番号
        , SIK_KKT_YMD      -- 請求確定年月日
        , RKN_MBN          -- 料金月分
        , SQBT_SSYT_YMD    -- 請求媒体作成予定年月日
        , SHKG_YMD         -- 支払期限年月日
        , SHKD_HNKU_FLG    -- 支払期限日変更フラグ
        , ETR_YYKG_YMD     -- 延滞利息猶予期限年月日
        , CST_TUCH_NO      -- お客さま通知番号(3x)
        , GSN_SIK_FLG      -- 合算請求フラグ
        , SYO_KYO_TRNO     -- 使用者契約お客さま登録番号（4xに対する3x）
        , SIH_HUHU_SHBT    -- 支払方法種別
        , SHSY_KYO_TRNO    -- 支払者契約お客さま登録番号
        , OUTPUT_DATETIME  -- 出力日付
        , ROW_NUMBER() OVER(PARTITION BY [SIK_NO] ORDER BY [SHKG_YMD] desc) as rown
    FROM [omni].[omni_ods_ma_trn_paymentalert]
)tmp
WHERE rown=1   -- 請求番号で一意とする
;
