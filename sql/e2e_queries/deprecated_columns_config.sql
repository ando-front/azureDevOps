----------------------------------------------------------------------
-- 削除対象カラム設定ファイル
-- 
-- Purpose: システム全体で削除されたカラムを一元管理
-- Usage: client_dm_analysis_service.sql等で参照してNULLカラムを動的生成
-- 
-- History:
-- 2025/05/31 リファクタリングで作成 - ハードコードされたNULLカラムを一元管理
----------------------------------------------------------------------

-- 削除対象年度とカラムのマッピングテーブル作成
-- @name: deprecated_columns_config_table_create
IF NOT EXISTS (SELECT *
FROM sys.tables
WHERE name = 'deprecated_columns_config')
CREATE TABLE deprecated_columns_config
(
    config_id int IDENTITY(1,1) PRIMARY KEY,
    column_category varchar(100) NOT NULL,
    -- カラムカテゴリ（LIV1CSWK、WEBHIS等）
    deprecated_year varchar(10) NOT NULL,
    -- 削除対象年度（2018、2019、2020等）
    column_suffix varchar(100) NOT NULL,
    -- カラム末尾（_OPENING_FLG、_CLOSED_FLG等）
    alias_name varchar(200) NOT NULL,
    -- 出力用エイリアス名
    deprecation_reason varchar(500),
    -- 削除理由
    deprecated_date date DEFAULT GETDATE(),
    -- 削除日
    created_at datetime2 DEFAULT GETDATE()
);

-- 削除対象カラム設定データ投入
-- @name: deprecated_columns_data_insert
INSERT INTO deprecated_columns_config
    (column_category, deprecated_year, column_suffix, alias_name, deprecation_reason)
VALUES
    -- LIV1案件業務 2018年度削除対象
    ('LIV1CSWK', '2018', '_OPENING_FLG', 'LIV1CSWK_OPENING_FLG_2018', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2018', '_CLOSED_FLG', 'LIV1CSWK_CLOSED_FLG_2018', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2018', '_SL_FLG', 'LIV1CSWK_SL_FLG_2018', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2018', '_KKSR_FLG', 'LIV1CSWK_KKSR_FLG_2018', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2018', '_ACA_FLG', 'LIV1CSWK_ACA_FLG_2018', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2018', '_FOLLOWUP_SERVICE_FLG', 'LIV1CSWK_FOLLOWUP_SERVICE_FLG_2018', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2018', '_OTHER_FLG', 'LIV1CSWK_OTHER_FLG_2018', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2018', '_CASE_ALL_ASPECT_FLG', 'LIV1CSWK_CASE_ALL_ASPECT_FLG_2018', 'データ保持期間終了のため削除'),

    -- LIV1案件業務 2019年度削除対象
    ('LIV1CSWK', '2019', '_OPENING_FLG', 'LIV1CSWK_OPENING_FLG_2019', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2019', '_CLOSED_FLG', 'LIV1CSWK_CLOSED_FLG_2019', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2019', '_SL_FLG', 'LIV1CSWK_SL_FLG_2019', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2019', '_KKSR_FLG', 'LIV1CSWK_KKSR_FLG_2019', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2019', '_ACA_FLG', 'LIV1CSWK_ACA_FLG_2019', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2019', '_FOLLOWUP_SERVICE_FLG', 'LIV1CSWK_FOLLOWUP_SERVICE_FLG_2019', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2019', '_OTHER_FLG', 'LIV1CSWK_OTHER_FLG_2019', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2019', '_CASE_ALL_ASPECT_FLG', 'LIV1CSWK_CASE_ALL_ASPECT_FLG_2019', 'データ保持期間終了のため削除'),

    -- LIV1案件業務 2020年度削除対象
    ('LIV1CSWK', '2020', '_OPENING_FLG', 'LIV1CSWK_OPENING_FLG_2020', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2020', '_CLOSED_FLG', 'LIV1CSWK_CLOSED_FLG_2020', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2020', '_SL_FLG', 'LIV1CSWK_SL_FLG_2020', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2020', '_KKSR_FLG', 'LIV1CSWK_KKSR_FLG_2020', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2020', '_ACA_FLG', 'LIV1CSWK_ACA_FLG_2020', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2020', '_FOLLOWUP_SERVICE_FLG', 'LIV1CSWK_FOLLOWUP_SERVICE_FLG_2020', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2020', '_OTHER_FLG', 'LIV1CSWK_OTHER_FLG_2020', 'データ保持期間終了のため削除'),
    ('LIV1CSWK', '2020', '_CASE_ALL_ASPECT_FLG', 'LIV1CSWK_CASE_ALL_ASPECT_FLG_2020', 'データ保持期間終了のため削除'),

    -- WEB履歴 削除対象
    ('WEBHIS', 'CURRENT', '_1062_SS_WEBQUOTE_1MO_FLG', 'WEBHIS_1062_SS_WEBQUOTE_1MO_FLG', '上流のカラム名が変更のため暫定的にNULLを設定'),
    ('WEBHIS', 'CURRENT', '_1062_SS_WEBQUOTE_1YR_FLG', 'WEBHIS_1062_SS_WEBQUOTE_1YR_FLG', '上流のカラム名が変更のため暫定的にNULLを設定');

-- 削除対象カラム一覧取得用クエリ
-- @name: get_deprecated_columns_list
SELECT
    column_category,
    deprecated_year,
    column_suffix,
    alias_name,
    deprecation_reason,
    'NULL AS ' + alias_name + '   -- ' + deprecation_reason as sql_column_definition
FROM deprecated_columns_config
ORDER BY column_category, deprecated_year, column_suffix;

-- カテゴリ別削除対象カラム数取得
-- @name: deprecated_columns_count_by_category
SELECT
    column_category,
    COUNT(*) as deprecated_count,
    STRING_AGG(deprecated_year, ', ') as affected_years
FROM deprecated_columns_config
GROUP BY column_category
ORDER BY column_category;

-- 動的SQL生成用：削除対象カラムのSELECT句生成
-- @name: generate_deprecated_columns_select
SELECT
    '      , NULL AS ' + alias_name + '   -- ' + deprecation_reason as select_clause
FROM deprecated_columns_config
WHERE column_category = 'LIV1CSWK'
-- 特定カテゴリのみ
ORDER BY deprecated_year, column_suffix;

-- 設定クリーンアップ（テスト・初期化用）
-- @name: cleanup_deprecated_columns_config
-- DELETE FROM deprecated_columns_config;
-- DROP TABLE IF EXISTS deprecated_columns_config;
