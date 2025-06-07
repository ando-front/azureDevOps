--------------------------------------------------------------------------
-- 本人特定契約_IF連携＞ADF整備
-- 
-- 【データ概要】
-- テーブル「本人特定契約」からMA向けの項目を抽出する
-- 
-- 【出力カラム構成】 20カラム
-- MTGID          : 会員ID
-- EDA_NO         : 枝番
-- GMT_SET_NO     : ガスメータ設置場所番号
-- SYOKY_NO       : 使用契約番号
-- CST_REG_NO     : お客さま登録番号
-- SHRKY_NO       : 支払契約番号
-- HJ_CST_NAME    : 表示名称
-- YUSEN_JNJ_NO   : 優先順位
-- TKTIYMD        : 特定年月日
-- TRKSBTCD       : 種別コード
-- CST_NO         : 表示用お客さま番号
-- INTRA_TRK_ID   : イントラ登録ID
-- SND_UM_CD      : ホスト送信有無コード
-- TRK_SBT_CD     : 登録種別コード
-- REC_REG_YMD    : レコード登録年月日
-- REC_REG_JKK    : レコード登録時刻
-- REC_UPD_YMD    : レコード更新年月日
-- REC_UPD_JKK    : レコード更新時刻
-- TAIKAI_FLAG    : 退会フラグ
-- OUTPUT_DATETIME: 出力日付
-- 
-- 【作成者】: 自動外部化処理
-- 【作成日】: 2025/01/16
-- 【外部参照】: src/dev/arm_template/linkedTemplates/ArmTemplate_3.json (line:441)
--------------------------------------------------------------------------

-- テーブル「本人特定契約」からMA向けの項目を抽出する
DECLARE @today_jst datetime;
SET @today_jst=CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time');
-- 最新の日時
DECLARE @outDT varchar(20);
SET @outDT=format(@today_jst, 'yyyy/MM/dd HH:mm:ss')

SELECT
    MTGID          -- 会員ID
    , EDA_NO         -- 枝番
    , GMT_SET_NO     -- ガスメータ設置場所番号
    , SYOKY_NO       -- 使用契約番号
    , CST_REG_NO     -- お客さま登録番号
    , SHRKY_NO       -- 支払契約番号
    , HJ_CST_NAME    -- 表示名称
    , YUSEN_JNJ_NO   -- 優先順位
    , TKTIYMD        -- 特定年月日
    , TRKSBTCD       -- 種別コード
    , CST_NO         -- 表示用お客さま番号
    , INTRA_TRK_ID   -- イントラ登録ID
    , SND_UM_CD      -- ホスト送信有無コード
    , TRK_SBT_CD     -- 登録種別コード
    , REC_REG_YMD    -- レコード登録年月日
    , REC_REG_JKK    -- レコード登録時刻
    , REC_UPD_YMD    -- レコード更新年月日
    , REC_UPD_JKK    -- レコード更新時刻
    , TAIKAI_FLAG    -- 退会フラグ

    , @outDT as OUTPUT_DATETIME
-- 出力日付
FROM [omni].[omni_ods_mytginfo_trn_cpkiyk] -- 本人特定契約
;
