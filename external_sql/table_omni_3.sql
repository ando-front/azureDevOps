--------------------------------------------------------------------------
-- アクションポイント当月エントリーリスト＞ADF整備（SFTP送信）
-- 
-- 2025/03/17  新規作成
--------------------------------------------------------------------------

-- テーブル「アクションポイントエントリーevent」からMA向けの項目を抽出する
-- 最新の日時の宣言
DECLARE @today_jst datetime;
-- 最新の日時を取得
SET @today_jst=CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time');  

-- IF出力日時の宣言
DECLARE @outputDT varchar(20);
-- 取得した最新の日時をyyyy/MM/dd HH:mm:ssに変換する
SET @outputDT = FORMAT(@today_jst, 'yyyy/MM/dd HH:mm:ss');

-- 実行年月の月初日と月末日を宣言
DECLARE @month_startDT varchar(10);  -- 実行日の月初日
DECLARE @month_endDT varchar(10);    -- 実行日の月末日
-- 実行年月の月初日を取得
SET @month_startDT = FORMAT(@today_jst, 'yyyy-MM') + '-01';
-- 実行年月の月末日を取得
SET @month_endDT = EOMONTH(@today_jst);

SELECT
     MTGID                                           -- mTG会員ID
    ,ACTIONPOINT_TYPE                                -- アクションポイント種別
    ,FORMAT(ENTRY_DATE, 'yyyy/MM/dd') AS ENTRY_DATE  -- エントリー年月日
    ,@outputDT AS OUTPUT_DATETIME                    -- IF出力日時
FROM [omni].[omni_odm_actionpoint_trn_entry_event]   -- アクションポイントエントリーevent
WHERE ENTRY_DATE BETWEEN @month_startDT AND @month_endDT  -- 「エントリー年月日」の年月が実行日の年月と一致するレコード
;
