"""
SQLのSELECT/INSERT文からカラム名を抽出・正規化するユーティリティ。
"""

import re
from tests.unit.normalize_column import normalize_column_name
from typing import List


def extract_normalized_columns(sql: str) -> List[str]:
    """
    SQLのSELECT句またはINSERT SELECT句からカラムリストを抽出し、正規化して返す
    """
    # SELECT句を抽出（複数行にまたがる場合も考慮）
    m = re.search(r"SELECT\s+(.+?)\s+FROM", sql, re.DOTALL | re.IGNORECASE)
    if not m:
        raise AssertionError("SELECT句が見つかりません")
    
    select_body = m.group(1).strip()
    cols: List[str] = []
    
    # カンマで分割する前に、角括弧内のカンマを一時的に置換
    temp_body = select_body
    bracket_pattern = r'\[([^\]]+)\]'
    bracket_matches = re.findall(bracket_pattern, temp_body)
    for i, match in enumerate(bracket_matches):
        placeholder = f"__BRACKET_PLACEHOLDER_{i}__"
        temp_body = temp_body.replace(f'[{match}]', placeholder)
    
    # カンマで分割
    column_parts = temp_body.split(',')
    
    for part in column_parts:
        part = part.strip()
        if not part or part.startswith('--'):
            continue
            
        # プレースホルダーを元に戻す
        for i, match in enumerate(bracket_matches):
            placeholder = f"__BRACKET_PLACEHOLDER_{i}__"
            part = part.replace(placeholder, f'[{match}]')
        
        # NULL AS ... は除外
        if re.match(r"NULL\s+AS", part, re.IGNORECASE):
            continue
            
        # AS句がある場合、AS句の後の名前を使用
        as_match = re.search(r"\s+AS\s+(.+?)$", part, re.IGNORECASE)
        if as_match:
            col_name = as_match.group(1).strip()
        else:
            # AS句がない場合、列名を抽出
            # 1. 角括弧で囲まれた名前を優先
            bracket_match = re.search(r'\[([^\]]+)\]', part)
            if bracket_match:
                col_name = bracket_match.group(1).strip()
            else:
                # 2. テーブル名.カラム名形式の場合、カラム名部分を取得
                dot_match = re.search(r'(?:\w+\.)?(\w+)', part)
                if dot_match:
                    col_name = dot_match.group(1).strip()
                else:
                    # 3. その他の場合、最初の単語を取得
                    words = part.split()
                    if words:
                        col_name = words[0].strip()
                    else:
                        continue
        
        # 引用符を除去
        col_name = col_name.strip('\'"')
        
        # 角括弧を除去（正規化前に）
        col_name = col_name.strip('[]')
        
        # 正規化して追加
        normalized = normalize_column_name(col_name)
        if normalized:
            cols.append(normalized)
    
    return cols


def extract_table_columns_from_insert(sql: str) -> List[str]:
    """
    INSERT文のテーブル名とカラムリストを抽出する
    例: INSERT INTO table_name (col1, col2, ...) VALUES ...
    """
    # INSERT INTO table_name (columns) パターンを抽出
    pattern = r"INSERT\s+INTO\s+\[?(\w+)\]?\s*\(\s*(.+?)\s*\)"
    match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
    
    if not match:
        raise AssertionError("INSERT文のカラムリストが見つかりません")
    
    table_name = match.group(1)
    columns_str = match.group(2)
    
    # カンマで分割してカラム名を取得
    columns = []
    for col in columns_str.split(','):
        col = col.strip()
        # 角括弧を除去
        col = col.strip('[]')
        # 引用符を除去
        col = col.strip('\'"')
        
        # 正規化して追加
        normalized = normalize_column_name(col)
        if normalized:
            columns.append(normalized)
    
    return columns
