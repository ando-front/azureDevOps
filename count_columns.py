#!/usr/bin/env python3
"""
パイプラインのSQLからカラム数を数えるスクリプト
"""

import re
import json


def count_columns_in_select_query(sql_query):
    """SELECT句のカラム数をカウント"""
    # ASキーワードを使ったカラム定義を検索
    as_pattern = r'AS\s+([A-Z_0-9]+)'
    as_matches = re.findall(as_pattern, sql_query, re.IGNORECASE)
    
    # SQLのコメント行とNULL指定を除外してより正確にカウント
    lines = sql_query.split('\n')
    actual_columns = []
    
    for line in lines:
        # コメント行をスキップ
        if line.strip().startswith('--'):
            continue
            
        # AS句を含む行を検索
        as_match = re.search(r'AS\s+([A-Z_0-9]+)', line, re.IGNORECASE)
        if as_match:
            column_name = as_match.group(1)
            actual_columns.append(column_name)
    
    return len(actual_columns), actual_columns


def count_columns_in_insert_query(sql_query):
    """INSERT句のカラム数をカウント"""
    # INSERT文のSELECT部分を抽出
    select_part_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_query, re.DOTALL | re.IGNORECASE)
    if not select_part_match:
        return 0, []
        
    select_part = select_part_match.group(1)
    
    # カンマで分割してカラムを抽出
    columns = []
    for line in select_part.split('\n'):
        line = line.strip()
        if not line or line.startswith('--'):
            continue
            
        # カンマで終わる場合、カラム名を抽出
        if ',' in line or line == select_part.split('\n')[-1].strip():
            # コメントを除去
            column_line = re.sub(r'--.*$', '', line).strip()
            if column_line:
                # カンマを除去してカラム名を取得
                column_name = column_line.rstrip(',').strip()
                if column_name and not column_name.startswith('('):
                    columns.append(column_name)
    
    return len(columns), columns


def analyze_pipeline_columns():
    """パイプラインのカラム数を分析"""
    
    # プロダクションコードのSQLを読み込み（ARM Templateから抽出）
    production_select_sql = '''----------------------------------------------------------------------
-- omni.顧客DM(Marketingスキーマ)のODS化
-- 
-- 2024/02/02　新規作成
-- 2024/04/22　レイアウト変更に伴う、カラム追加（391行目～446行目）
-- 2024/06/03　6月の削除対象カラムをNULLに置き換えとする暫定対応
-- 2024/08/07　Maketingスキーマ.顧客DMからomni.顧客DMに参照先を変更
-- 2024/09/05　2カラムWEB履歴_1062_SS_Web見積_直近1か月フラグとWEB履歴_1062_SS_Web見積_直近1年フラグのカラム名変更対応で暫定的にNULLを設定
----------------------------------------------------------------------

-- Maketingスキーマのomni.顧客DMから、omniスキーマの顧客DM_tempにデータを全量コピーする。
SELECT [顧客キー_Ax] AS CLIENT_KEY_AX
      ,[LIV0EU_ガスメータ設置場所番号＿１ｘ] AS LIV0EU_1X
      ,[LIV0EU_カスタマ番号＿８ｘ] AS LIV0EU_8X
      ,[LIV0EU_受持ＮＷ箇所コード] AS LIV0EU_UKMT_NW_PART_CD
      ,[LIV0EU_受持ＮＷ箇所_法人名] AS LIV0EU_UKMT_NW_PART_CORP_NAME
      ,[LIV0EU_新社集計箇所コード] AS LIV0EU_NEW_CORP_AGG_CD
      ,[LIV0EU_適用開始年月日] AS LIV0EU_APPLY_START_YMD
      ,[LIV0EU_適用終了年月日] AS LIV0EU_APPLY_END_YMD
      ,[LIV0EU_使用契約番号＿４ｘ] AS LIV0EU_4X
      ,[LIV0EU_支払契約番号＿２ｘ] AS LIV0EU_2X
      ,[LIV0EU_郵便番号] AS LIV0EU_ZIP_CD
      ,[LIV0EU_都道府県名漢字] AS LIV0EU_PERF_KJ
      ,[LIV0EU_行政区画コード] AS LIV0EU_GYOK_CD
      ,[LIV0EU_行政区名] AS LIV0EU_GYOK_NAME
      ,[LIV0EU_町コード] AS LIV0EU_STREET_CD
      ,[LIV0EU_町名] AS LIV0EU_STREET_MAME
      ,[LIV0EU_丁目＿字コード] AS LIV0EU_CITY_BLOCK_CD
      ,[LIV0EU_建物番号] AS LIV0EU_BLD_NO
      ,[LIV0EU_新設年月] AS LIV0EU_SHINSETSU_YM
      ,[LIV0EU_供内管設備状態コード] AS LIV0EU_INNER_TUBE_EQP_STATE_CD
      ,[LIV0EU_ガスメータ開閉栓状態コード] AS LIV0EU_GAS_METER_CLOSING_CD
      ,[LIV0EU_用途コード] AS LIV0EU_USAGE_CD
      ,[LIV0EU_ガス用途_集合・戸建分類] AS LIV0EU_GAS_USAGE_AP_DT_CLASS
      ,[LIV0EU_ガス用途_大分類] AS LIV0EU_GAS_USAGE_MAJOR_CLASS
      ,[LIV0EU_ガス用途_中分類] AS LIV0EU_GAS_USAGE_MIDIUM_CLASS
      ,[LIV0EU_ガス用途_小分類] AS LIV0EU_GAS_USAGE_MINOR_CLASS
      ,[LIV0EU_ガス用途_家庭用詳細] AS LIV0EU_GAS_USAGE_HOME_USE_DETAIL
      ,[LIV0EU_ガス用途_業務用詳細] AS LIV0EU_GAS_USAGE_BIZ_USE_DETAIL
      ,[LIV0EU_ガス用途_12セグメント分類] AS LIV0EU_GAS_USAGE_12_SEGMENT_CLASS
      ,[LIV0EU_ガス用途_都市エネ大分類] AS LIV0EU_GAS_USAGE_TOSHI_ENE_MAJOR_CLASS
      ,[LIV0EU_ガス用途_都市エネ小分類] AS LIV0EU_GAS_USAGE_TOSHI_ENE_MINOR_CLASS
      ,[LIV0EU_ガス用途_都市エネ官公庁フラグ] AS LIV0EU_GAS_USAGE_TOSHI_ENE_PUBOFF_FLG
      ,[LIV0EU_メータ号数流量] AS LIV0EU_METER_NUMBER_FLOW_RATE
      ,[LIV0EU_検針方法名称] AS LIV0EU_READING_METER_METHOD_NAME
      ,[LIV0EU_ガス料金支払方法コード] AS LIV0EU_GAS_PAY_METHOD_CD
      ,[LIV0EU_ガス料金支払方法] AS LIV0EU_GAS_PAY_METHOD
      ,[LIV0EU_料金Ｇコード] AS LIV0EU_CHARGE_G_CD
      ,[LIV0EU_料金Ｇエリア区分コード] AS LIV0EU_CHARGE_G_AREA_TYPE_CD
      ,[LIV0EU_ブロック番号] AS LIV0EU_BLOCK_NUMBER
      ,[LIV0EU_ガス料金契約種別コード] AS LIV0EU_GAS_SHRI_CRT_SHBT_CD
      ,[LIV0EU_ガス料金契約種別コード契約種名] AS LIV0EU_GAS_SHRI_CRT_SHBT_CD_NAME
      ,[LIV0EU_ガス料金契約種別コード契約種細目名] AS LIV0EU_GAS_SHRI_CRT_SHBT_CD_CRT_DETAILS
      ,[LIV0EU_管理課所名称] AS LIV0EU_KSY_NAME
      ,[LIV0EU_定保メトロサイン] AS LIV0EU_TEIHO_METRO_SIGN
      ,[LIV0EU_定保メトロサイン名称] AS LIV0EU_TEIHO_METRO_SIGN_NAME
      ,[LIV0EU_定保在宅状況名称] AS LIV0EU_TEIHO_HOUSING_SITUATION_NAME
      ,[LIV0EU_開栓事由名称] AS LIV0EU_OPENING_REASON_NAME
      ,[LIV0EU_住宅施主区分名称] AS LIV0EU_HOME_OWNER_TYPE_NAME
      ,[LIV0EU_住宅所有区分名称] AS LIV0EU_HOME_OWNERSHIP_TYPE_NAME
      ,[LIV0EU_ＣＩＳ＿ＤＭ拒否サイン] AS LIV0EU_CIS_DM_REFUAL_SING
      ,[LIV0EU_ＣＩＳ＿サービス情報有無コード] AS LIV0EU_CIS_SERVICE_UM_CD
      ,[LIV0EU_前年総使用量] AS LIV0EU_LSYR_TOTAL_USE
      ,[LIV0EU_多使用需要家フラグ] AS LIV0EU_HEAVY_USE_JYUYOUKA_FLG
      ,[LIV0EU_受持ＮＷ箇所_ライフバル・エネスタ分類] AS LIV0EU_INCHANGE_NW_LIFEVAL_ENESUTA_BRI
      ,[LIV0EU_受持ＮＷ箇所_ライフバル・エネスタ名] AS LIV0EU_INCHANGE_NW_LIFEVAL_ENESUTA_NAME
      ,[LIV0EU_開栓後経過月] AS LIV0EU_OPENING_MONTH_PASSED
      ,[LIV0EU_開栓後経過月カテゴリ] AS LIV0EU_OPENING_MONTH_PASSED_CATEGORY
      ,[定保区分] AS TEIHO_TYPE
      ,[LIV0共有所有機器_コンロ種別] AS LIV0SPD_KONRO_SHBT
      ,[LIV0共有所有機器_コンロ種別名] AS LIV0SPD_KONRO_SHBT_NAME
      ,[LIV0共有所有機器_コンロ_所有機器番号] AS LIV0SPD_KONRO_POSSESSION_EQUIPMENT_NO
      ,[LIV0共有所有機器_コンロ_販売先コード] AS LIV0SPD_KONRO_SL_DEST_CD
      ,[LIV0共有所有機器_コンロ_製造年月] AS LIV0SPD_KONRO_DT_MANUFACTURE
      ,[LIV0共有所有機器_コンロ_経年数] AS LIV0SPD_KONRO_NUM_YEAR
      ,[LIV0共有所有機器_コンロ_経年数カテゴリ] AS LIV0SPD_KONRO_NUM_YEAR_CATEGORY
      ,[LIV0共有所有機器_給湯機種別] AS LIV0SPD_KYTK_SHBT
      ,[LIV0共有所有機器_給湯機種名称] AS LIV0SPD_KYTK_NAME
      ,[LIV0共有所有機器_給湯器_所有機器番号] AS LIV0SPD_KYTK_POSSESSION_DEVICE_NO
      ,[LIV0共有所有機器_給湯器_販売先コード] AS LIV0SPD_KYTK_SL_DESTINATION_CD
      ,[LIV0共有所有機器_給湯器_製造年月] AS LIV0SPD_KYTK_DT_MANUFACTURE
      ,[LIV0共有所有機器_給湯器_経年数] AS LIV0SPD_KYTK_NUM_YEAR
      ,[LIV0共有所有機器_給湯器_経年数カテゴリ] AS LIV0SPD_KYTK_NUM_YEAR_CATEGORY
      ,[LIV0共有所有機器_210_炊飯器] AS LIV0SPD_210_RICE_COOKER
      ,[LIV0共有所有機器_220_レンジ] AS LIV0SPD_220_MICROWAVE
      ,[LIV0共有所有機器_230_オーブン] AS LIV0SPD_230_OVEN
      ,[LIV0共有所有機器_240_コンベック] AS LIV0SPD_240_CONVECTION_OVEN
      ,[LIV0共有所有機器_250_コンビネーションレンジ] AS LIV0SPD_250_COMBINATION_MICROWAVE
      ,[LIV0共有所有機器_300_乾燥機ほか] AS LIV0SPD_300_DRYING_MACHINE
      ,[LIV0共有所有機器_310_ドライヤー（衣類乾燥機）] AS LIV0SPD_310_DRIER
      ,[LIV0共有所有機器_410_金網ストーブ] AS LIV0SPD_410_WIRE_MESH_STOVE
      ,[LIV0共有所有機器_420_開放型ストーブ] AS LIV0SPD_420_OPEN_STOVE
      ,[LIV0共有所有機器_430_ＦＨストーブ] AS LIV0SPD_430_FH_STOVE
      ,[LIV0共有所有機器_440_ＢＦストーブ] AS LIV0SPD_440_BF_STOVE
      ,[LIV0共有所有機器_450_ＦＦストーブ] AS LIV0SPD_450_FF_STOVE
      ,[LIV0共有所有機器_460_ＦＥストーブ] AS LIV0SPD_460_FE_STOVE
--      ,[TES熱源機情報_製造番号] AS TESHSMC_SERIAL_NUMBER
      ,NULL AS TESHSMC_SERIAL_NUMBER -- TES熱源機情報_製造番号　カンマを含むためNULLに設定
      ,[TES熱源機情報_システム種別] AS TESHSMC_SYSTEM_SHBT'''
    
    production_insert_sql = '''INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
SELECT CLIENT_KEY_AX
      , LIV0EU_1X
      , LIV0EU_8X
      , LIV0EU_UKMT_NW_PART_CD
      , LIV0EU_UKMT_NW_PART_CORP_NAME
      , LIV0EU_NEW_CORP_AGG_CD
      , LIV0EU_APPLY_START_YMD
      , LIV0EU_APPLY_END_YMD
      , LIV0EU_4X
      , LIV0EU_2X
      , LIV0EU_ZIP_CD
      , LIV0EU_PERF_KJ
      , LIV0EU_GYOK_CD
      , LIV0EU_GYOK_NAME
      , LIV0EU_STREET_CD
      , LIV0EU_STREET_MAME
      , LIV0EU_CITY_BLOCK_CD
      , LIV0EU_BLD_NO
      , LIV0EU_SHINSETSU_YM
      , LIV0EU_INNER_TUBE_EQP_STATE_CD
      , LIV0EU_GAS_METER_CLOSING_CD
      , LIV0EU_USAGE_CD
      , LIV0EU_GAS_USAGE_AP_DT_CLASS
      , LIV0EU_GAS_USAGE_MAJOR_CLASS
      , LIV0EU_GAS_USAGE_MIDIUM_CLASS
      , LIV0EU_GAS_USAGE_MINOR_CLASS
      , LIV0EU_GAS_USAGE_HOME_USE_DETAIL
      , LIV0EU_GAS_USAGE_BIZ_USE_DETAIL
      , LIV0EU_GAS_USAGE_12_SEGMENT_CLASS
      , LIV0EU_GAS_USAGE_TOSHI_ENE_MAJOR_CLASS
      , LIV0EU_GAS_USAGE_TOSHI_ENE_MINOR_CLASS
      , LIV0EU_GAS_USAGE_TOSHI_ENE_PUBOFF_FLG
      , LIV0EU_METER_NUMBER_FLOW_RATE
      , LIV0EU_READING_METER_METHOD_NAME
      , LIV0EU_GAS_PAY_METHOD_CD
      , LIV0EU_GAS_PAY_METHOD
      , LIV0EU_CHARGE_G_CD
      , LIV0EU_CHARGE_G_AREA_TYPE_CD
      , LIV0EU_BLOCK_NUMBER
      , LIV0EU_GAS_SHRI_CRT_SHBT_CD
      , LIV0EU_GAS_SHRI_CRT_SHBT_CD_NAME
      , LIV0EU_GAS_SHRI_CRT_SHBT_CD_CRT_DETAILS
      , LIV0EU_KSY_NAME
      , LIV0EU_TEIHO_METRO_SIGN
      , LIV0EU_TEIHO_METRO_SIGN_NAME
      , LIV0EU_TEIHO_HOUSING_SITUATION_NAME
      , LIV0EU_OPENING_REASON_NAME
      , LIV0EU_HOME_OWNER_TYPE_NAME
      , LIV0EU_HOME_OWNERSHIP_TYPE_NAME
      , LIV0EU_CIS_DM_REFUAL_SING
      , LIV0EU_CIS_SERVICE_UM_CD
      , LIV0EU_LSYR_TOTAL_USE
      , LIV0EU_HEAVY_USE_JYUYOUKA_FLG
      , LIV0EU_INCHANGE_NW_LIFEVAL_ENESUTA_BRI
      , LIV0EU_INCHANGE_NW_LIFEVAL_ENESUTA_NAME
      , LIV0EU_OPENING_MONTH_PASSED
      , LIV0EU_OPENING_MONTH_PASSED_CATEGORY
      , TEIHO_TYPE
      , LIV0SPD_KONRO_SHBT
      , LIV0SPD_KONRO_SHBT_NAME
      , LIV0SPD_KONRO_POSSESSION_EQUIPMENT_NO
      , LIV0SPD_KONRO_SL_DEST_CD
      , LIV0SPD_KONRO_DT_MANUFACTURE
      , LIV0SPD_KONRO_NUM_YEAR
      , LIV0SPD_KONRO_NUM_YEAR_CATEGORY
      , LIV0SPD_KYTK_SHBT
      , LIV0SPD_KYTK_NAME
      , LIV0SPD_KYTK_POSSESSION_DEVICE_NO
      , LIV0SPD_KYTK_SL_DESTINATION_CD
      , LIV0SPD_KYTK_DT_MANUFACTURE
      , LIV0SPD_KYTK_NUM_YEAR
      , LIV0SPD_KYTK_NUM_YEAR_CATEGORY
      , LIV0SPD_210_RICE_COOKER
      , LIV0SPD_220_MICROWAVE
      , LIV0SPD_230_OVEN
      , LIV0SPD_240_CONVECTION_OVEN
      , LIV0SPD_250_COMBINATION_MICROWAVE
      , LIV0SPD_300_DRYING_MACHINE
      , LIV0SPD_310_DRIER
      , LIV0SPD_410_WIRE_MESH_STOVE
      , LIV0SPD_420_OPEN_STOVE
      , LIV0SPD_430_FH_STOVE
      , LIV0SPD_440_BF_STOVE
      , LIV0SPD_450_FF_STOVE
      , LIV0SPD_460_FE_STOVE
      , TESHSMC_SERIAL_NUMBER
      , TESHSMC_SYSTEM_SHBT'''
    
    # テストコードで想定している列
    test_critical_columns = ["CLIENT_KEY_AX", "KNUMBER_AX", "ADDRESS_KEY_AX"]
    test_expected_column_count = 533
    
    print("=" * 80)
    print("pi_Copy_marketing_client_dm パイプライン カラム数分析")
    print("=" * 80)
    
    # SELECT句のカラム数分析
    select_count, select_columns = count_columns_in_select_query(production_select_sql)
    print(f"\n📊 SELECT句分析結果:")
    print(f"   カラム数: {select_count}")
    print(f"   最初の10カラム: {select_columns[:10]}")
    print(f"   最後の10カラム: {select_columns[-10:]}")
    
    # INSERT句のカラム数分析
    insert_count, insert_columns = count_columns_in_insert_query(production_insert_sql)
    print(f"\n📊 INSERT句分析結果:")
    print(f"   カラム数: {insert_count}")
    print(f"   最初の10カラム: {insert_columns[:10]}")
    print(f"   最後の10カラム: {insert_columns[-10:]}")
    
    # テストケースとの比較
    print(f"\n🧪 テストコードとの比較:")
    print(f"   テストが期待するカラム数: {test_expected_column_count}")
    print(f"   実際のプロダクションカラム数: {select_count}")
    print(f"   差異: {test_expected_column_count - select_count}")
    
    # テストの重要カラムが存在するかチェック
    print(f"\n🔍 テスト用重要カラムの存在確認:")
    for col in test_critical_columns:
        exists = col in select_columns
        status = "✅ 存在" if exists else "❌ 不存在"
        print(f"   {col}: {status}")
    
    # SELECT句とINSERT句の整合性チェック
    print(f"\n🔧 SELECT句とINSERT句の整合性:")
    print(f"   SELECT句カラム数: {select_count}")
    print(f"   INSERT句カラム数: {insert_count}")
    match_status = "✅ 一致" if select_count == insert_count else "❌ 不一致"
    print(f"   整合性: {match_status}")
    
    # カラムグループ分析
    column_groups = {
        "LIV0EU": [col for col in select_columns if col.startswith("LIV0EU_")],
        "LIV0SPD": [col for col in select_columns if col.startswith("LIV0SPD_")],
        "TESHSMC": [col for col in select_columns if col.startswith("TESHSMC_")],
        "TESHSEQ": [col for col in select_columns if col.startswith("TESHSEQ_")],
        "TESHRDTR": [col for col in select_columns if col.startswith("TESHRDTR_")],
        "TESSV": [col for col in select_columns if col.startswith("TESSV_")],
        "EPCISCRT": [col for col in select_columns if col.startswith("EPCISCRT_")],
        "WEBHIS": [col for col in select_columns if col.startswith("WEBHIS_")]
    }
    
    print(f"\n📈 カラムグループ別分析:")
    total_group_columns = 0
    for group_name, group_columns in column_groups.items():
        count = len(group_columns)
        total_group_columns += count
        print(f"   {group_name}*: {count}カラム")
    
    other_columns = select_count - total_group_columns
    print(f"   その他: {other_columns}カラム")
    
    # 結論
    print(f"\n📋 E2Eテストの妥当性評価:")
    if select_count >= 500:
        coverage = "✅ 十分" if test_expected_column_count >= select_count * 0.9 else "⚠️ 部分的"
        print(f"   プロダクションコードは大規模({select_count}カラム)")
        print(f"   テストカバレッジ: {coverage}")
        print(f"   テスト対象カラム率: {(test_expected_column_count/select_count)*100:.1f}%")
    else:
        print(f"   プロダクションコードは中規模({select_count}カラム)")
        print(f"   テストは過大評価の可能性")
    
    if select_count == insert_count:
        print("   ✅ SELECT句とINSERT句の整合性は保たれている")
    else:
        print("   ❌ SELECT句とINSERT句にカラム数の不整合がある")
    
    return {
        "select_count": select_count,
        "insert_count": insert_count,
        "test_expected": test_expected_column_count,
        "column_groups": column_groups,
        "is_consistent": select_count == insert_count
    }


if __name__ == "__main__":
    analyze_pipeline_columns()
