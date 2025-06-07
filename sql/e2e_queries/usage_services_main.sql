/*
利用サービス出力クエリ
パイプライン: pi_UsageServices
目的: omni.omni_ods_cloak_trn_usageserviceテーブルから利用サービスデータを抽出してCSV出力

カラム構成:
- 全てのソーステーブルカラム
- OUTPUT_DATETIME: 出力日時（JST）

外部化前の場所: ARMTemplateForFactory.json line 4326
作成日: 2024-12-19
*/

-- @name: usage_services_main
-- @description: 利用サービスデータの抽出とCSV出力
DECLARE @today_jst varchar(20);
SET @today_jst=format(CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time'), 'yyyy/MM/dd HH:mm:ss');

SELECT
    *
    , @today_jst as OUTPUT_DATETIME
-- 出力日付
FROM [omni].[omni_ods_cloak_trn_usageservice]
;
