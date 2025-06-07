-- ポイント付与メールパイプライン メインSQLクエリ
-- ファイル: point_grant_email_main.sql
-- 対象パイプライン: pi_PointGrantEmail
-- 抽出元: pi_PointGrantEmail.json (at_CreateCSV_PointGrantEmail)
-- 作成日: 2025-05-31
-- 説明: ポイント付与メール連携用データ抽出・重複削除クエリ

-- @name: point_grant_email_main_query
-- @description: ポイント付与メール メインデータ抽出クエリ（外部テーブル作成・データ抽出・重複削除）
-- @parameters: executionStartDateTimeUTC (パイプライン実行開始日時)
-- @returns: ID_NO, PNT_TYPE_CD, MAIL_ADR, PICTURE_MM, CSV_YMD, OUTPUT_DATETIME

----------------------------------------------------------------------------
-- MA向けリスト連携 ポイント付与メール 出力 at_CreateCSV_PointGrantEmail
-- 2023/02/28　初版
-- 2023/10/05　重複削除条件修正
-- 2025/05/31　SQL外部化対応
----------------------------------------------------------------------------

-- 作成する外部テーブル(ポイント付与メール_ext_temp)が存在する場合は削除する
DECLARE @isAllEventsTemp int;
SELECT @isAllEventsTemp=count(*)
FROM sys.objects
WHERE schema_id = schema_id('omni') AND name='omni_ods_mytginfo_trn_point_add_ext_temp';
IF @isAllEventsTemp = 1
    DROP EXTERNAL TABLE [omni].[omni_ods_mytginfo_trn_point_add_ext_temp];

-- ADFによりTSVをロードする外部テーブルを作成する

-- 外部テーブル作成 omni_ods_mytginfo_trn_point_add_ext_temp(ポイント付与メール_ext_temp)
CREATE EXTERNAL TABLE [omni].[omni_ods_mytginfo_trn_point_add_ext_temp]
(
     ID_NO			NVARCHAR(20)	NULL	 -- 会員ID
    ,PNT_TYPE_CD	NVARCHAR(4)		NULL	 -- ポイント種別
    ,MAIL_ADR		NVARCHAR(50)	NULL	 -- メールアドレス
    ,PICTURE_MM		NVARCHAR(2)		NULL	 -- 画像月分
)
WITH (
    DATA_SOURCE=[mytokyogas],
    LOCATION='{DYNAMIC_LOCATION}', -- パイプライン実行時に置換される
    FILE_FORMAT=[TSV2],
    REJECT_TYPE=VALUE,
    REJECT_VALUE=0
)
;

-- 会員IDと画像月分の組み合わせで重複削除する。重複削除するレコードはメールアドレスが最大のもの以外に行う
SELECT
    ID_NO,
    PNT_TYPE_CD,
    MAIL_ADR,
    PICTURE_MM,
    '{DYNAMIC_CSV_YMD}' as CSV_YMD, -- パイプライン実行時に置換される
    FORMAT((GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time'),'yyyy/MM/dd HH:mm:ss') as OUTPUT_DATETIME
FROM (
    SELECT *,
        DupRank = ROW_NUMBER() OVER (
                  PARTITION BY ID_NO,PNT_TYPE_CD,PICTURE_MM
                  ORDER BY MAIL_ADR DESC
                )
    FROM omni.omni_ods_mytginfo_trn_point_add_ext_temp
) as table_pointGrantEmail
WHERE DupRank = 1

-- 作成した外部テーブルが存在する場合は削除行う
SELECT @isAllEventsTemp=count(*)
FROM sys.objects
WHERE schema_id = schema_id('omni') AND name='omni_ods_mytginfo_trn_point_add_ext_temp';
IF @isAllEventsTemp = 1
    DROP EXTERNAL TABLE [omni].[omni_ods_mytginfo_trn_point_add_ext_temp];
