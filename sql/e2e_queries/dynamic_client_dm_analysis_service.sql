----------------------------------------------------------------------
-- 動的SQLクエリ生成用プロシージャ
-- 
-- Purpose: 削除対象カラムを動的に管理し、SQLクエリを自動生成
-- Usage: client_dm_analysis_service.sqlの代替として使用
-- 
-- Benefits:
-- - ハードコードされたNULLカラムの一元管理
-- - 新しい削除対象カラムの簡単追加
-- - メンテナンス性の向上
-- 
-- History:
-- 2025/05/31 リファクタリングで作成
----------------------------------------------------------------------

-- 動的顧客DM分析サービス情報取得プロシージャ
-- @name: dynamic_client_dm_analysis_service_proc
CREATE OR ALTER PROCEDURE sp_get_client_dm_analysis_service_dynamic
    @include_deprecated BIT = 1,
    -- 削除対象カラムを含めるかどうか
    @debug_mode BIT = 0
-- デバッグモード（SQL文を出力）
AS
BEGIN
    SET NOCOUNT ON;

    -- 基本SELECT句
    DECLARE @base_sql NVARCHAR(MAX) = N'
SELECT [顧客キー_Ax] AS CLIENT_KEY_AX
      -- LIV1案件業務（直近期間）
      , [LIV1案件業務_開栓フラグ_1年以内] AS LIV1CSWK_OPENING_FLG_1YEAR
      , [LIV1案件業務_閉栓フラグ_1年以内] AS LIV1CSWK_CLOSING_FLG_1YEAR
      , [LIV1案件業務_販売フラグ_1年以内] AS LIV1CSWK_SL_FLG_1YEAR
      , [LIV1案件業務_機器修理・点検フラグ_1年以内] AS LIV1CSWK_KKSR_FLG_1YEAR
      , [LIV1案件業務_能動的接点活動フラグ_1年以内] AS LIV1CSWK_ACA_FLG_1YEAR
      , [LIV1案件業務_アフターフォローフラグ_1年以内] AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_1YEAR
      , [LIV1案件業務_その他フラグ_1年以内] AS LIV1CSWK_OTHER_FLG_1YEAR
      , [LIV1案件業務_案件全般フラグ_1年以内] AS LIV1CSWK_CASE_ALL_ASPECT_FLG_1YEAR
      , [LIV1案件業務_開栓フラグ_2年以内] AS LIV1CSWK_OPENING_FLG_2YEAR
      , [LIV1案件業務_閉栓フラグ_2年以内] AS LIV1CSWK_CLOSED_FLG_2YEAR
      , [LIV1案件業務_販売フラグ_2年以内] AS LIV1CSWK_SL_FLG_2YEAR
      , [LIV1案件業務_機器修理・点検フラグ_2年以内] AS LIV1CSWK_KKSR_FLG_2YEAR
      , [LIV1案件業務_能動的接点活動フラグ_2年以内] AS LIV1CSWK_ACA_FLG_2YEAR
      , [LIV1案件業務_アフターフォローフラグ_2年以内] AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2YEAR
      , [LIV1案件業務_その他フラグ_2年以内] AS LIV1CSWK_OTHER_FLG_2YEAR
      , [LIV1案件業務_案件全般フラグ_2年以内] AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2YEAR';

    DECLARE @deprecated_columns NVARCHAR(MAX) = N'';
    DECLARE @active_columns NVARCHAR(MAX) = N'
      -- 2021年度データ（有効）
      , [LIV1案件業務_開栓フラグ_2021年度] AS LIV1CSWK_OPENING_FLG_2021
      , [LIV1案件業務_閉栓フラグ_2021年度] AS LIV1CSWK_CLOSED_FLG_2021
      , [LIV1案件業務_販売フラグ_2021年度] AS LIV1CSWK_SL_FLG_2021
      , [LIV1案件業務_機器修理・点検フラグ_2021年度] AS LIV1CSWK_KKSR_FLG_2021
      , [LIV1案件業務_能動的接点活動フラグ_2021年度] AS LIV1CSWK_ACTIVE_CONTACT_ACTIVIT_FLG_2021
      , [LIV1案件業務_アフターフォローフラグ_2021年度] AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2021
      , [LIV1案件業務_その他フラグ_2021年度] AS LIV1CSWK_OTHER_FLG_2021
      , [LIV1案件業務_案件全般フラグ_2021年度] AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2021
      
      -- 全期間データ
      , [LIV1案件業務_開栓フラグ_全期間] AS LIV1CSWK_OPENING_FLG
      , [LIV1案件業務_閉栓フラグ_全期間] AS LIV1CSWK_CLOSED_FLG
      , [LIV1案件業務_販売フラグ_全期間] AS LIV1CSWK_SL_FLG
      , [LIV1案件業務_機器修理・点検フラグ_全期間] AS LIV1CSWK_KKSR_FLG
      , [LIV1案件業務_能動的接点活動フラグ_全期間] AS LIV1CSWK_ACA_FLG
      , [LIV1案件業務_アフターフォローフラグ_全期間] AS LIV1CSWK_AF_FLG
      , [LIV1案件業務_その他フラグ_全期間] AS LIV1CSWK_OTHER_FLG
      , [LIV1案件業務_案件全般フラグ_全期間] AS LIV1CSWK_CASE_ALL_ASPECT_FLG
      
      -- WEB履歴情報
      , [WEB履歴_基準日] AS WEBHIS_REFERENCE_DATE
      , [WEB履歴_1003_GP_各種手続き_引越し_直近1か月フラグ] AS WEBHIS_1003_GP_VARPROC_MOVE_1MO_FLG
      , [WEB履歴_1003_GP_各種手続き_引越し_直近1年フラグ] AS WEBHIS_1003_GP_VARPROC_MOVE_1YR_FLG
      , [WEB履歴_1005_GP_各種手続き_申込切替_直近1か月フラグ] AS WEBHIS_1005_GP_VARPROC_SWAPP_1MO_FLG
      , [WEB履歴_1005_GP_各種手続き_申込切替_直近1年フラグ] AS WEBHIS_1005_GP_VARPROC_SWAPP_1YR_FLG
      , [WEB履歴_1011_GP_ガス・電気_さすてな_直近1か月フラグ] AS WEBHIS_1011_GP_GAS_ELECT_SASTENA_1MO_FLG
      , [WEB履歴_1011_GP_ガス・電気_さすてな_直近1年フラグ] AS WEBHIS_1011_GP_GAS_ELECT_SASTENA_1YR_FLG
      , [WEB履歴_1012_GP_ガス・電気_時間帯別_直近1か月フラグ] AS WEBHIS_1012_GP_GAS_ELECT_TMZONE_1MO_FLG
      , [WEB履歴_1012_GP_ガス・電気_時間帯別_直近1年フラグ] AS WEBHIS_1012_GP_GAS_ELECT_TMZONE_1YR_FLG';

    DECLARE @remaining_columns NVARCHAR(MAX) = N'
      -- 数理加工情報・デモグラフィック情報
      , [数理加工情報_築年数] AS MATHPROC_YEAR_BUILT
      , [数理加工情報_築年グループ名] AS MATHPROC_YEAR_BUILT_GROUP_NAME
      , [数理加工情報_分譲賃貸分類] AS MATHPROC_SALE_RENT_CLASS
      , [数理加工情報_年間ガス使用量] AS MATHPROC_ANN_GAS_USAGE
      , [数理加工情報_年間ガス使用量CATG] AS MATHPROC_ANN_GAS_USAGE_CATG
      , [数理加工情報_年間ガス使用量_直近1年_50m刻み] AS MATHPROC_ANN_GAS_USAGE_1YEAR_50M_INC
      , [数理加工情報_年間ガス使用量_直近1年_業務用区分] AS MATHPROC_ANN_GAS_USAGE_1YEAR_GYOMU_TYPE
      , [数理加工情報_推定電力使用量（ガス使用量などからの推計）] AS MATHPROC_ESTI_DNRK_USAGE
      , [数理加工情報_推定電力使用量CATG] AS MATHPROC_ESTI_DNRK_USAGE_CATG
      , [電力使用量（最終的な推計値）] AS ELECT_USAGE
      , [電力使用量（最終的な推計値）_500kWh刻み] AS ELECT_USAGE_500KW_INC
      , [デモグラフィック情報_推定世帯人数] AS DEMOGRAPHIC_ESTIMATED_NUM_HOUSEHOLDS
      
      -- myTG会員情報
      , [myTG会員情報_myTGフラグ] AS MYTG_MYTG_FLG
      , [myTG会員情報_myTG会員登録日] AS MYTG_MEMBER_REGIST_DT
      , [myTG会員情報_myTG会員ID] AS MYTG_MTGID
      , [myTG会員情報_myTG性別] AS MYTG_SEX
      , [myTG会員情報_myTG年代] AS MYTG_AGE
      
      -- その他分析情報
      , [世帯主年代（実績値または推定値）] AS HEAD_HOUSEHOLD_AGE
      , [世帯主年代実績値フラグ] AS HEAD_HOUSEHOLD_AGE_FLG
      , [直近開栓者フラグ] AS MOST_RECENT_OPENER_FLG
      , [戦略セグメント] AS STRATEGIC_SEGMENT
      , [顧客ステージ] AS CUSTOMER_STAGE
      , [推定入居区分_新築中古入居分類] AS ESTIMVINTP_NEW_USED_MOVEIN_TYPE
      
      -- レコード管理情報
      , [レコード作成日時] AS REC_REG_YMD
      , [レコード更新日時] AS REC_UPD_YMD
FROM [omni].[顧客DM]';

    -- 削除対象カラムを動的生成
    IF @include_deprecated = 1
    BEGIN
        -- deprecated_columns_configテーブルから削除対象カラムを取得
        IF EXISTS (SELECT 1
        FROM sys.tables
        WHERE name = 'deprecated_columns_config')
        BEGIN
            SELECT @deprecated_columns = @deprecated_columns + 
                CHAR(13) + CHAR(10) + '      , NULL AS ' + alias_name + '   -- ' + deprecation_reason
            FROM deprecated_columns_config
            ORDER BY column_category, deprecated_year, column_suffix;
        END
        ELSE
        BEGIN
            -- テーブルが存在しない場合はハードコードされた削除対象カラムを使用
            SET @deprecated_columns = N'
      -- 削除対象年度のLIV1案件業務データ（NULL設定）
      , NULL AS LIV1CSWK_OPENING_FLG_2018   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_CLOSED_FLG_2018   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_SL_FLG_2018   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_KKSR_FLG_2018   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_ACA_FLG_2018   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2018   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_OTHER_FLG_2018   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2018   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_OPENING_FLG_2019   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_CLOSED_FLG_2019   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_SL_FLG_2019   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_KKSR_FLG_2019   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_ACA_FLG_2019   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2019   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_OTHER_FLG_2019   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2019   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_OPENING_FLG_2020   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_CLOSED_FLG_2020   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_SL_FLG_2020   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_KKSR_FLG_2020   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_ACA_FLG_2020   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2020   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_OTHER_FLG_2020   -- カラムが削除のためNULLとする
      , NULL AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2020   -- カラムが削除のためNULLとする
      , NULL AS WEBHIS_1062_SS_WEBQUOTE_1MO_FLG -- 上流のカラム名が変更 暫定的にNULLを設定
      , NULL AS WEBHIS_1062_SS_WEBQUOTE_1YR_FLG -- 上流のカラム名が変更 暫定的にNULLを設定';
        END
    END

    -- 最終的なSQL文を構築
    DECLARE @final_sql NVARCHAR(MAX) = @base_sql + @deprecated_columns + @active_columns + @remaining_columns;

    -- デバッグモードの場合はSQL文を出力
    IF @debug_mode = 1
    BEGIN
        PRINT 'Generated SQL:';
        PRINT @final_sql;
        RETURN;
    END

    -- SQL実行
    EXEC sp_executesql @final_sql;
END;

-- 使用例とテスト用クエリ
-- @name: dynamic_client_dm_usage_examples
/*
-- 通常使用（削除対象カラム込み）
EXEC sp_get_client_dm_analysis_service_dynamic @include_deprecated = 1, @debug_mode = 0;

-- 削除対象カラム無し
EXEC sp_get_client_dm_analysis_service_dynamic @include_deprecated = 0, @debug_mode = 0;

-- SQL生成内容をデバッグ確認
EXEC sp_get_client_dm_analysis_service_dynamic @include_deprecated = 1, @debug_mode = 1;
*/

-- 従来のclient_dm_analysis_service.sqlとの互換性のためのビュー作成
-- @name: client_dm_analysis_service_view_create
CREATE OR ALTER VIEW vw_client_dm_analysis_service
AS
    SELECT [顧客キー_Ax] AS CLIENT_KEY_AX
          -- LIV1案件業務（直近期間）
          , [LIV1案件業務_開栓フラグ_1年以内] AS LIV1CSWK_OPENING_FLG_1YEAR
          , [LIV1案件業務_閉栓フラグ_1年以内] AS LIV1CSWK_CLOSING_FLG_1YEAR
          , [LIV1案件業務_販売フラグ_1年以内] AS LIV1CSWK_SL_FLG_1YEAR
          , [LIV1案件業務_機器修理・点検フラグ_1年以内] AS LIV1CSWK_KKSR_FLG_1YEAR
          , [LIV1案件業務_能動的接点活動フラグ_1年以内] AS LIV1CSWK_ACA_FLG_1YEAR
          , [LIV1案件業務_アフターフォローフラグ_1年以内] AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_1YEAR
          , [LIV1案件業務_その他フラグ_1年以内] AS LIV1CSWK_OTHER_FLG_1YEAR
          , [LIV1案件業務_案件全般フラグ_1年以内] AS LIV1CSWK_CASE_ALL_ASPECT_FLG_1YEAR
          , [LIV1案件業務_開栓フラグ_2年以内] AS LIV1CSWK_OPENING_FLG_2YEAR
          , [LIV1案件業務_閉栓フラグ_2年以内] AS LIV1CSWK_CLOSED_FLG_2YEAR
          , [LIV1案件業務_販売フラグ_2年以内] AS LIV1CSWK_SL_FLG_2YEAR
          , [LIV1案件業務_機器修理・点検フラグ_2年以内] AS LIV1CSWK_KKSR_FLG_2YEAR
          , [LIV1案件業務_能動的接点活動フラグ_2年以内] AS LIV1CSWK_ACA_FLG_2YEAR
          , [LIV1案件業務_アフターフォローフラグ_2年以内] AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2YEAR
          , [LIV1案件業務_その他フラグ_2年以内] AS LIV1CSWK_OTHER_FLG_2YEAR
          , [LIV1案件業務_案件全般フラグ_2年以内] AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2YEAR
          
          -- 削除対象年度のLIV1案件業務データ（NULL設定）
          , NULL AS LIV1CSWK_OPENING_FLG_2018
          , NULL AS LIV1CSWK_CLOSED_FLG_2018
          , NULL AS LIV1CSWK_SL_FLG_2018
          , NULL AS LIV1CSWK_KKSR_FLG_2018
          , NULL AS LIV1CSWK_ACA_FLG_2018
          , NULL AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2018
          , NULL AS LIV1CSWK_OTHER_FLG_2018
          , NULL AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2018
          , NULL AS LIV1CSWK_OPENING_FLG_2019
          , NULL AS LIV1CSWK_CLOSED_FLG_2019
          , NULL AS LIV1CSWK_SL_FLG_2019
          , NULL AS LIV1CSWK_KKSR_FLG_2019
          , NULL AS LIV1CSWK_ACA_FLG_2019
          , NULL AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2019
          , NULL AS LIV1CSWK_OTHER_FLG_2019
          , NULL AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2019
          , NULL AS LIV1CSWK_OPENING_FLG_2020
          , NULL AS LIV1CSWK_CLOSED_FLG_2020
          , NULL AS LIV1CSWK_SL_FLG_2020
          , NULL AS LIV1CSWK_KKSR_FLG_2020
          , NULL AS LIV1CSWK_ACA_FLG_2020
          , NULL AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2020
          , NULL AS LIV1CSWK_OTHER_FLG_2020
          , NULL AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2020
          
          -- 2021年度データ（有効）
          , [LIV1案件業務_開栓フラグ_2021年度] AS LIV1CSWK_OPENING_FLG_2021
          , [LIV1案件業務_閉栓フラグ_2021年度] AS LIV1CSWK_CLOSED_FLG_2021
          , [LIV1案件業務_販売フラグ_2021年度] AS LIV1CSWK_SL_FLG_2021
          , [LIV1案件業務_機器修理・点検フラグ_2021年度] AS LIV1CSWK_KKSR_FLG_2021
          , [LIV1案件業務_能動的接点活動フラグ_2021年度] AS LIV1CSWK_ACTIVE_CONTACT_ACTIVIT_FLG_2021
          , [LIV1案件業務_アフターフォローフラグ_2021年度] AS LIV1CSWK_FOLLOWUP_SERVICE_FLG_2021
          , [LIV1案件業務_その他フラグ_2021年度] AS LIV1CSWK_OTHER_FLG_2021
          , [LIV1案件業務_案件全般フラグ_2021年度] AS LIV1CSWK_CASE_ALL_ASPECT_FLG_2021
          
          -- 全期間データ
          , [LIV1案件業務_開栓フラグ_全期間] AS LIV1CSWK_OPENING_FLG
          , [LIV1案件業務_閉栓フラグ_全期間] AS LIV1CSWK_CLOSED_FLG
          , [LIV1案件業務_販売フラグ_全期間] AS LIV1CSWK_SL_FLG
          , [LIV1案件業務_機器修理・点検フラグ_全期間] AS LIV1CSWK_KKSR_FLG
          , [LIV1案件業務_能動的接点活動フラグ_全期間] AS LIV1CSWK_ACA_FLG
          , [LIV1案件業務_アフターフォローフラグ_全期間] AS LIV1CSWK_AF_FLG
          , [LIV1案件業務_その他フラグ_全期間] AS LIV1CSWK_OTHER_FLG
          , [LIV1案件業務_案件全般フラグ_全期間] AS LIV1CSWK_CASE_ALL_ASPECT_FLG
          
          -- WEB履歴情報
          , [WEB履歴_基準日] AS WEBHIS_REFERENCE_DATE
          , [WEB履歴_1003_GP_各種手続き_引越し_直近1か月フラグ] AS WEBHIS_1003_GP_VARPROC_MOVE_1MO_FLG
          , [WEB履歴_1003_GP_各種手続き_引越し_直近1年フラグ] AS WEBHIS_1003_GP_VARPROC_MOVE_1YR_FLG
          , [WEB履歴_1005_GP_各種手続き_申込切替_直近1か月フラグ] AS WEBHIS_1005_GP_VARPROC_SWAPP_1MO_FLG
          , [WEB履歴_1005_GP_各種手続き_申込切替_直近1年フラグ] AS WEBHIS_1005_GP_VARPROC_SWAPP_1YR_FLG
          , [WEB履歴_1011_GP_ガス・電気_さすてな_直近1か月フラグ] AS WEBHIS_1011_GP_GAS_ELECT_SASTENA_1MO_FLG
          , [WEB履歴_1011_GP_ガス・電気_さすてな_直近1年フラグ] AS WEBHIS_1011_GP_GAS_ELECT_SASTENA_1YR_FLG
          , [WEB履歴_1012_GP_ガス・電気_時間帯別_直近1か月フラグ] AS WEBHIS_1012_GP_GAS_ELECT_TMZONE_1MO_FLG
          , [WEB履歴_1012_GP_ガス・電気_時間帯別_直近1年フラグ] AS WEBHIS_1012_GP_GAS_ELECT_TMZONE_1YR_FLG
          , NULL AS WEBHIS_1062_SS_WEBQUOTE_1MO_FLG -- 上流のカラム名が変更 暫定的にNULLを設定
          , NULL AS WEBHIS_1062_SS_WEBQUOTE_1YR_FLG -- 上流のカラム名が変更 暫定的にNULLを設定
          
          -- 数理加工情報・デモグラフィック情報
          , [数理加工情報_築年数] AS MATHPROC_YEAR_BUILT
          , [数理加工情報_築年グループ名] AS MATHPROC_YEAR_BUILT_GROUP_NAME
          , [数理加工情報_分譲賃貸分類] AS MATHPROC_SALE_RENT_CLASS
          , [数理加工情報_年間ガス使用量] AS MATHPROC_ANN_GAS_USAGE
          , [数理加工情報_年間ガス使用量CATG] AS MATHPROC_ANN_GAS_USAGE_CATG
          , [数理加工情報_年間ガス使用量_直近1年_50m刻み] AS MATHPROC_ANN_GAS_USAGE_1YEAR_50M_INC
          , [数理加工情報_年間ガス使用量_直近1年_業務用区分] AS MATHPROC_ANN_GAS_USAGE_1YEAR_GYOMU_TYPE
          , [数理加工情報_推定電力使用量（ガス使用量などからの推計）] AS MATHPROC_ESTI_DNRK_USAGE
          , [数理加工情報_推定電力使用量CATG] AS MATHPROC_ESTI_DNRK_USAGE_CATG
          , [電力使用量（最終的な推計値）] AS ELECT_USAGE
          , [電力使用量（最終的な推計値）_500kWh刻み] AS ELECT_USAGE_500KW_INC
          , [デモグラフィック情報_推定世帯人数] AS DEMOGRAPHIC_ESTIMATED_NUM_HOUSEHOLDS
          
          -- myTG会員情報
          , [myTG会員情報_myTGフラグ] AS MYTG_MYTG_FLG
          , [myTG会員情報_myTG会員登録日] AS MYTG_MEMBER_REGIST_DT
          , [myTG会員情報_myTG会員ID] AS MYTG_MTGID
          , [myTG会員情報_myTG性別] AS MYTG_SEX
          , [myTG会員情報_myTG年代] AS MYTG_AGE
          
          -- その他分析情報
          , [世帯主年代（実績値または推定値）] AS HEAD_HOUSEHOLD_AGE
          , [世帯主年代実績値フラグ] AS HEAD_HOUSEHOLD_AGE_FLG
          , [直近開栓者フラグ] AS MOST_RECENT_OPENER_FLG
          , [戦略セグメント] AS STRATEGIC_SEGMENT
          , [顧客ステージ] AS CUSTOMER_STAGE
          , [推定入居区分_新築中古入居分類] AS ESTIMVINTP_NEW_USED_MOVEIN_TYPE
          
          -- レコード管理情報
          , [レコード作成日時] AS REC_REG_YMD
          , [レコード更新日時] AS REC_UPD_YMD
    FROM [omni].[顧客DM];
