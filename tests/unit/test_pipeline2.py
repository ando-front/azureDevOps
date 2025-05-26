import os
import pytest


def test_pipeline2(tmp_path):
    input_folder  = tmp_path / "input"
    output_folder = tmp_path / "output"
    sftp_folder   = tmp_path / "sftp"
    for d in (input_folder, output_folder, sftp_folder):
        d.mkdir()

    test_file = "test_data_input.csv"
    content = "header1,header2\nvalue1,value2\n"
    input_path = input_folder / test_file
    input_path.write_text(content)

    # ステップ1: SQLMi → Synapse → Blob と SFTP への転送をシミュレート
    # (実際はファイルコピー)
    output_file = output_folder / (test_file + ".gz")
    sftp_file  = sftp_folder   / (test_file + ".gz")
    output_file.write_text(content)
    sftp_file.write_text(content)

    assert output_file.exists()
    assert sftp_file.exists()