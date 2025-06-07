------------------------------------------------------------------------
-- MA向けリスト連携 LINEID連携情報 出力 at_CreateCSV_LINEIDLinkInfo
-- 外部化されたSQLクエリファイル
-- 元ファイル: ArmTemplate_4.json (LINEIDLinkInfo pipeline)
-- 出力列数: 7列 (ID_NO, LINE_U_ID, IDCS_U_ID, LINE_RNK_DTTM, KJ_FLG, LINE_RNK_KJ_DTTM, OUTPUT_DATETIME)
-- 重複削除条件: なし（全件出力）
-- 外部化日: 2024-01-16
------------------------------------------------------------------------

DECLARE @today_jst datetime;
SET @today_jst=CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time');
-- 最新の日時

DECLARE @outDT varchar(20);
SET @outDT=format(@today_jst, 'yyyy/MM/dd HH:mm:ss');

----------------------------------------------------------------------------
SELECT [ID_NO]                   -- 会員ID
      , [LINE_U_ID]               -- LINE_uID
      , [IDCS_U_ID]               -- IDCS_uID
      , format([LINE_RNK_DTTM], 'yyyy/MM/dd HH:mm:ss') as LINE_RNK_DTTM    -- LINE連携日時
      , [KJ_FLG]                  -- 解除フラグ
      , format([LINE_RNK_KJ_DTTM],'yyyy/MM/dd') as LINE_RNK_KJ_DTTM        -- LINE連携解除日時
      , @outDT as OUTPUT_DATETIME
FROM [mytg].[mytg_ods_line_mst_line_id_all] -- LINEID連携累積情報
;
