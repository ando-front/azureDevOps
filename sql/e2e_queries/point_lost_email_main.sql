----------------------------------------------------------------------------
-- MA向けリスト連携 ポイント失効メール 出力 at_CreateCSV_PointLostEmail
-- 外部化されたSQLクエリファイル
-- 元ファイル: ArmTemplate_3.json (pi_PointLostEmail pipeline)
-- 出力列数: 4列 (ID_NO, MAIL_ADR, CSV_YMD, OUTPUT_DATETIME)
-- 重複削除条件: ID_NO単位でメールアドレス降順
-- 外部化日: 2024-01-16
----------------------------------------------------------------------------

-- 作成する外部テーブル(ポイント失効メール_ext_temp)が存在する場合は削除する
DECLARE @isAllEventsTemp int;
SELECT @isAllEventsTemp=count(*)
FROM sys.objects
WHERE schema_id = schema_id('omni') AND name='omni_ods_mytginfo_trn_point_delete_ext_temp';
IF @isAllEventsTemp = 1
    DROP EXTERNAL TABLE [omni].[omni_ods_mytginfo_trn_point_delete_ext_temp];


-- ADFによりTSVをロードする外部テーブルを作成する

-- 外部テーブル作成 omni_ods_mytginfo_trn_point_delete_ext_temp(ポイント失効メール_ext_temp)
CREATE EXTERNAL TABLE [omni].[omni_ods_mytginfo_trn_point_delete_ext_temp]
(
     ID_NO	NVARCHAR(20)	NULL	 -- 会員ID
    ,MAIL_ADR	NVARCHAR(50)	NULL	 -- メールアドレス
)
WITH (
    DATA_SOURCE=[mytokyogas],
    LOCATION='{DYNAMIC_LOCATION}',
    FILE_FORMAT=[TSV2],
    REJECT_TYPE=VALUE,
    REJECT_VALUE=0
)
;

-- 会員IDの重複を削除する。重複削除するレコードはメールアドレスが最大のもの以外に行う
SELECT ID_NO, MAIL_ADR, '{DYNAMIC_CSV_YMD}'as CSV_YMD, FORMAT((GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time'),'yyyy/MM/dd HH:mm:ss')as OUTPUT_DATETIME
FROM (
SELECT *
, DupRank = ROW_NUMBER() OVER (
              PARTITION BY ID_NO
              ORDER BY MAIL_ADR DESC
            )
    FROM omni.omni_ods_mytginfo_trn_point_delete_ext_temp
) as table_pointLostEmail
WHERE DupRank = 1


-- 作成した外部テーブルが存在する場合は削除行う
SELECT @isAllEventsTemp=count(*)
FROM sys.objects
WHERE schema_id = schema_id('omni') AND name='omni_ods_mytginfo_trn_point_delete_ext_temp';
IF @isAllEventsTemp = 1
    DROP EXTERNAL TABLE [omni].[omni_ods_mytginfo_trn_point_delete_ext_temp];
