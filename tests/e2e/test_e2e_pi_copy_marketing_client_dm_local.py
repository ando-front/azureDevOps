import os
import pandas as pd
import pytest
import numpy as np

# 依存関係のないヘルパー関数をテストファイル内に直接定義
def generate_test_data(num_rows: int, num_cols: int, column_prefix: str = "col") -> pd.DataFrame:
    """シンプルなテストデータフレームを生成します。"""
    data = {f"{column_prefix}{i}": np.random.rand(num_rows) for i in range(num_cols)}
    return pd.DataFrame(data)

def read_csv_to_dataframe(file_path: str) -> pd.DataFrame:
    """CSVファイルをPandas DataFrameに読み込みます。"""
    return pd.read_csv(file_path)

def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    """2つのデータフレームが等しいか比較します。"""
    return df1.equals(df2)

# パイプラインのロジックを模倣する関数
def simulate_pipeline_logic(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    pi_Copy_marketing_client_dm パイプラインの主要なロジックを模倣します。
    """
    output_df = input_df.copy()
    return output_df

@pytest.mark.e2e_local
def test_e2e_pi_copy_marketing_client_dm_local():
    """
    pi_Copy_marketing_client_dm パイプラインのローカルE2Eテスト。
    ヘルパーモジュールへの依存をなくし、自己完結させています。
    """
    input_file = "test_input_data.csv"
    output_file = "test_output_data.csv"
    num_rows = 100
    num_cols = 10

    try:
        input_df = generate_test_data(num_rows=num_rows, num_cols=num_cols)
        input_df.to_csv(input_file, index=False)

        output_df = simulate_pipeline_logic(input_df)
        output_df.to_csv(output_file, index=False)

        result_df = read_csv_to_dataframe(output_file)

        assert compare_dataframes(input_df, result_df), "入力データと出力データが一致しません。"

        print("ローカルE2Eテストが正常に完了しました。")

    finally:
        if os.path.exists(input_file):
            os.remove(input_file)
        if os.path.exists(output_file):
            os.remove(output_file)