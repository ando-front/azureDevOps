----------------------------------------------------------------------
--  MA向けリスト連携 メール許諾 出力 at_CreateCSV_MailPermission
----------------------------------------------------------------------

-- 結果出力 => メール許諾

DECLARE @today_jst varchar(20);
SET @today_jst=format(CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time'), 'yyyy/MM/dd HH:mm:ss');

SELECT
     cpinfo.ID_NO                     -- 会員ID
    ,cpinfo.MAIL_ADR                  -- メールアドレス
    ,cpinfo.SUBMAIL_ADR               -- サブメールアドレス
    ,cpinfo.TEL_NO                    -- 電話番号
    ,cpinfo.SUBTEL_NO                 -- 日中のご連絡先
    ,mailsvc.SVC_CD                   -- サービスコード
    ,mailsvc.START_YMD                -- 適用開始年月日
    ,REPLACE(mailsvc.END_YMD, '99999999', '99991231') as END_YMD      -- 適用終了年月日
    ,mailsvc.LOCK_FLG_CD              -- ロックフラグコード
    ,mailsvc.REC_REG_YMD              -- レコード登録年月日
    ,mailsvc.REC_REG_JKK              -- レコード登録時刻
    ,mailsvc.REC_UPD_YMD              -- レコード更新年月日
    ,mailsvc.REC_UPD_JKK              -- レコード更新時刻
    ,@today_jst as OUTPUT_DATETIME    -- 出力日付
    ,cpinfo.TAIKAI_FLAG               -- 退会フラグ　…追加
    ,cpinfo.WEB_NO                    -- 8x（Web） …追加
    ,cpinfo.TNP_CD                    -- 店舗コード …追加
    ,cpinfo.THK_NWKSY_CD              -- THANKSネットワーク箇所コード …追加
    ,cpinfo.ID_KBN                    -- 会員区分 …追加
    ,cpinfo.REG_CD                    -- 会員登録経路 …追加
    ,cpinfo.REG_COMP_FLG              -- 会員登録完了フラグ …追加
    ,cpinfo.FIRST_MIGRATION_FLG       -- TGCRM初期移行フラグ …追加
    ,cpinfo.FREE_ITEM01               -- 予備1 …追加
    ,cpinfo.FREE_ITEM02               -- 予備2 …追加
    ,cpinfo.FREE_ITEM03               -- 予備3 …追加
    ,cpinfo.FREE_ITEM04               -- 予備4 …追加
    ,cpinfo.FREE_ITEM05               -- 予備5 …追加
    ,cpinfo.FREE_ITEM06               -- 予備6 …追加
    ,cpinfo.FREE_ITEM07               -- 予備7 …追加
    ,cpinfo.FREE_ITEM08               -- 予備8 …追加
    ,cpinfo.FREE_ITEM09               -- 予備9 …追加
    ,cpinfo.FREE_ITEM10               -- 予備10 …追加
FROM [omni].[omni_ods_mytginfo_trn_cpinfo] cpinfo                     -- 会員基本情報
LEFT OUTER JOIN [omni].[omni_ods_mytginfo_trn_mailsvc] mailsvc        -- メールサービス
     ON cpinfo.ID_NO = mailsvc.ID_NO;                        -- 会員基本情報.会員ID=メールサービス.会員ID
;
