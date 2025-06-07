/*
メール許諾出力クエリ
パイプライン: pi_mTGMailPermission
目的: 会員基本情報とメールサービス情報を結合してメール許諾データを抽出

カラム構成:
- 会員基本情報（cpinfo）: 20カラム
- メールサービス情報（mailsvc）: 7カラム
- OUTPUT_DATETIME: 出力日時（JST）

外部化前の場所: ARMTemplateForFactory.json line 4485
作成日: 2024-12-19
*/

-- @name: mail_permission_main
-- @description: メール許諾データの抽出とCSV出力
DECLARE @today_jst varchar(20);
SET @today_jst=format(CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time'), 'yyyy/MM/dd HH:mm:ss');

SELECT
    cpinfo.ID_NO                     -- 会員ID
    , cpinfo.MAIL_ADR                  -- メールアドレス
    , cpinfo.SUBMAIL_ADR               -- サブメールアドレス
    , cpinfo.TEL_NO                    -- 電話番号
    , cpinfo.SUBTEL_NO                 -- 日中のご連絡先
    , mailsvc.SVC_CD                   -- サービスコード
    , mailsvc.START_YMD                -- 適用開始年月日
    , REPLACE(mailsvc.END_YMD, '99999999', '99991231') as END_YMD      -- 適用終了年月日
    , mailsvc.LOCK_FLG_CD              -- ロックフラグコード
    , mailsvc.REC_REG_YMD              -- レコード登録年月日
    , mailsvc.REC_REG_JKK              -- レコード登録時刻
    , mailsvc.REC_UPD_YMD              -- レコード更新年月日
    , mailsvc.REC_UPD_JKK              -- レコード更新時刻
    , @today_jst as OUTPUT_DATETIME    -- 出力日付
    , cpinfo.TAIKAI_FLAG               -- 退会フラグ
    , cpinfo.WEB_NO                    -- 8x（Web）
    , cpinfo.TNP_CD                    -- 店舗コード
    , cpinfo.THK_NWKSY_CD              -- THANKSネットワーク箇所コード
    , cpinfo.ID_KBN                    -- 会員区分
    , cpinfo.REG_CD                    -- 会員登録経路
    , cpinfo.REG_COMP_FLG              -- 会員登録完了フラグ
    , cpinfo.FIRST_MIGRATION_FLG       -- TGCRM初期移行フラグ
    , cpinfo.FREE_ITEM01               -- 予備1
    , cpinfo.FREE_ITEM02               -- 予備2
    , cpinfo.FREE_ITEM03               -- 予備3
    , cpinfo.FREE_ITEM04               -- 予備4
    , cpinfo.FREE_ITEM05               -- 予備5
    , cpinfo.FREE_ITEM06               -- 予備6
    , cpinfo.FREE_ITEM07               -- 予備7
    , cpinfo.FREE_ITEM08               -- 予備8
    , cpinfo.FREE_ITEM09               -- 予備9
    , cpinfo.FREE_ITEM10
-- 予備10
FROM [omni].[omni_ods_mytginfo_trn_cpinfo] cpinfo -- 会員基本情報
    LEFT OUTER JOIN [omni].[omni_ods_mytginfo_trn_mailsvc] mailsvc -- メールサービス
    ON cpinfo.ID_NO = mailsvc.ID_NO;                        -- 会員基本情報.会員ID=メールサービス.会員ID
