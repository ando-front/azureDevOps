--------------------------------------------------------------------------
-- 支払方法変更リスト作成
-- 
-- 【データ概要】
-- 前日分と現在のガス契約情報を比較して、支払方法が変更された顧客を抽出し、
-- MA向けに出力する。払込（支払方法コード='2'）から他の支払方法に変更された
-- お客様を対象とする。
-- 
-- 【出力カラム構成】 4カラム
-- Bx              : 利用サービスマスタのBx
-- INDEX_ID        : インデックス番号
-- SYO_KYO_TRNO    : 使用契約番号
-- OUTPUT_DATETIME : 出力日付
-- 
-- 【ビジネスロジック】
-- 1. 前日分ガス契約情報とODMガス契約情報の登録日を比較
-- 2. データが変化している場合のみ、支払方法変更顧客を抽出
-- 3. 前日分：支払方法='2'(払込) → 現在：支払方法<>'2' の変更を検出
-- 4. 利用サービステーブルとJOINしてBx、INDEX_IDを取得
-- 5. 同一Bxで複数レコードがある場合は、OUTPUT_DATE降順で最新を採用
-- 
-- 【作成者】: 自動外部化処理
-- 【作成日】: 2025/01/16
-- 【外部参照】: src/dev/arm_template/linkedTemplates/ArmTemplate_3.json (line:2061)
--------------------------------------------------------------------------

-- 「支払方法変更」作成 at_CreateCSV_PaymentMethodChanged

DECLARE @isChanged int;

DECLARE @recRecDT_pre varchar(8);
-- 前日分ガス契約（契約中）日次お客さま情報_tempの登録日
DECLARE @recRecDT     varchar(8);
-- ODM.ガス契約（契約中）日次お客さま情報の登録日

SELECT top 1
    @recRecDT_pre=[REC_REG_YMD2]
FROM [omni].[omni_odm_gascstmr_trn_previousday_gaskiy_temp]
SELECT top 1
    @recRecDT=[REC_REG_YMD2]
FROM [omni].[omni_odm_gascstmr_trn_gaskiy]

SELECT @isChanged=count(*)
FROM [omni].[omni_ods_ma_trn_changed_payment_temp]
WHERE [REC_REG_YMD_PRE]=@recRecDT_pre and [REC_REG_YMD]=@recRecDT;

IF @isChanged=0  -- 以前に「支払い方法が変更された顧客_temp」を作成したときに使用した前日分とODMが変化したとき
    BEGIN
    -- ①作業テーブル作成

    TRUNCATE TABLE [omni].[omni_ods_ma_trn_changed_payment_temp];
    -- 支払い方法が変更された顧客_temp

    INSERT [omni].[omni_ods_ma_trn_changed_payment_temp]
    SELECT
        B.[SYO_KYO_TRNO]
            , A.REC_REG_YMD2
            , B.REC_REG_YMD2
    FROM [omni].[omni_odm_gascstmr_trn_previousday_gaskiy_temp] A -- 前日分ガス契約（契約中）日次お客さま情報_temp
        INNER JOIN [omni].[omni_odm_gascstmr_trn_gaskiy] B -- ODM.ガス契約（契約中）日次お客さま情報
        ON A.[SVKY_NO]=B.[SVKY_NO]
            AND A.[SIH_KIY_NO]=B.[SIH_KIY_NO]
    WHERE A.[SIH_HUHU_SHBT]='2' -- 2:払込
        and B.[SIH_HUHU_SHBT]<>'2'
        and B.[SIK_SVC_SHBT] not in ('11','12','13','21')
    ;


    -- 「支払方法変更」出力
    DECLARE @today_jst varchar(20);
    -- 実行日
    SET @today_jst=format(CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time'),'yyyy/MM/dd HH:mm:ss');

    SELECT Bx
              , INDEX_ID
              , SYO_KYO_TRNO
              , @today_jst as OUTPUT_DATETIME
    FROM (
            SELECT
            A.Bx
                , A.INDEX_ID
                , t.SYO_KYO_TRNO
                , ROW_NUMBER() OVER(PARTITION BY [Bx] ORDER BY [OUTPUT_DATE] desc) as rown
        -- 同一Bx中のOUTPUT_DATEの降順
        FROM [omni].[omni_ods_cloak_trn_usageservice] A -- 利用サービス
            INNER JOIN [omni].[omni_ods_ma_trn_changed_payment_temp] t -- 支払い方法が変更された顧客_temp
            ON A.USER_KEY=t.SYO_KYO_TRNO
                AND A.USER_KEY_TYPE='003'
                AND A.SERVICE_TYPE='001' -- 001：ガス
                AND A.TRANSFER_TYPE='02'     -- 2:提供中
        )tmp
    WHERE rown=1
;
END
ELSE
    BEGIN
    -- 前日分とODMが変化ないため、カラムだけ出力する
    SELECT top 0
        NULL as Bx
            , NULL as INDEX_ID
            , NULL as SYO_KYO_TRNO
            , NULL as OUTPUT_DATETIME
;
END
;
