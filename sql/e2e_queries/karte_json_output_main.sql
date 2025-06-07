----------------------------------------------------------------------
-- KARTE システム JSON出力用データ処理
-- Source: Externalized from ARM template
-- Purpose: KARTE連携用のJSON形式データ出力処理
-- 
-- Main Features:
-- - HASHED_MTGID をuser_idとして出力
-- - 契約情報（ガス・電気）のbit変換処理
-- - スコア情報の数値データ出力
-- - JSON形式での構造化出力対応
-- - 年齢層別・サービス別の分析データ
-- 
-- Target Output: KARTE システム連携用JSONデータ
-- Format: arrayOfObjects JSON形式
-- 
-- History:
-- 2024/12/31 ARMテンプレートから外部化
----------------------------------------------------------------------

SELECT TOP (1000)
    [HASHED_MTGID] as user_id,

    -- contract information
    convert(bit,[internal_area_gas]) as internal_area_gas,
    [internal_area_gas_menu],
    [external_area_gas],
    [external_area_gas_menu],
    [power],
    [power_menu],

    -- score information
    [pv_siti_scr],
    [dg_stn_age_0_29],
    [dg_stn_age_30_39],
    [dg_stn_age_40_49],
    [dg_stn_age_50_59],
    [dg_stn_age_60_69],
    [dg_stn_age_70_99],
    [dg_stns_4mlon_less],
    [dg_stns_10mlon_over],
    [tnkysk_1year],
    [syzi_gs_dtrkscr_month03],
    [syzi_gs_dtrkscr_year01],
    [syzi_el_dtrkscr_month03],
    [syzi_el_dtrkscr_year01],
    [web_history_flag_1],
    [web_history_flag_2],
    [contact_history_score],
    [usage_pattern_score],
    [loyalty_score]

FROM [omni].[karte_user_analytics_data]
WHERE [status] = 'active'
    AND [HASHED_MTGID] IS NOT NULL
ORDER BY [last_update_date] DESC;
