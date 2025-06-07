"""
SQL文から英語カラム名（AS句エイリアス等）を抽出・比較するユーティリティ。
"""

import re
from typing import List, Set


def extract_english_column_names(sql: str) -> Set[str]:
    """
    SQLのSELECT句またはINSERT SELECT句から英語のAS句（エイリアス）部分のみを抽出する
    日本語・英語・全角・半角混在のカラム名では英語名部分のみで突合することで
    エラーを減らすアプローチ
    """
    # JSONエスケープ文字を実際の改行に変換
    sql = sql.replace('\\r\\n', '\n').replace('\\r', '\n').replace('\\n', '\n')
    
    # SELECT句を抽出（複数行にまたがる場合も考慮）
    # FROMがある場合とない場合の両方に対応
    m = re.search(r"SELECT\s+(.+?)(?:\s+FROM|$)", sql, re.DOTALL | re.IGNORECASE)
    if not m:
        raise AssertionError("SELECT句が見つかりません")
    
    select_body = m.group(1).strip()
    english_columns: Set[str] = set()
    
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
            
        # AS句がある場合、AS句の後の英語名を使用
        as_match = re.search(r"\s+AS\s+([A-Z_0-9]+)", part, re.IGNORECASE)
        if as_match:
            english_name = as_match.group(1).strip().upper()
            # 英語名パターンのみを受け入れる（アルファベット、数字、アンダースコア）
            if re.match(r'^[A-Z_0-9]+$', english_name):
                english_columns.add(english_name)
        else:
            # AS句がない場合は、カラム名そのものが英語かチェック
            # 角括弧や引用符を除去
            clean_part = part.strip('[]').strip('\'"').strip()
            # 改行文字も除去
            clean_part = clean_part.replace('\n', '').replace('\r', '').strip()
            if clean_part and re.match(r'^[A-Z_0-9]+$', clean_part.upper()):
                english_columns.add(clean_part.upper())

    return english_columns


def extract_english_table_columns_from_insert(sql: str) -> Set[str]:
    """
    INSERT文から英語名部分のみを抽出する
    形式1: INSERT INTO table_name (col1, col2, ...) VALUES ...
    形式2: INSERT INTO table_name SELECT col1, col2, ... FROM ...
    """
    # まず形式1: INSERT INTO table_name (columns) パターンを試す
    pattern1 = r"INSERT\s+INTO\s+\[?[\w.]+\]?\s*\(\s*(.+?)\s*\)"
    match1 = re.search(pattern1, sql, re.IGNORECASE | re.DOTALL)
    
    if match1:
        columns_str = match1.group(1)
        # カンマで分割してカラム名を取得
        english_columns: Set[str] = set()
        for col in columns_str.split(','):
            col = col.strip()
            # 角括弧を除去
            col = col.strip('[]')
            # 引用符を除去
            col = col.strip('\'"')
            
            # 英語名パターンのみを受け入れる（アルファベット、数字、アンダースコア）
            col_upper = col.upper()
            if re.match(r'^[A-Z_0-9]+$', col_upper):
                english_columns.add(col_upper)
        
        return english_columns
    
    # 形式2: INSERT INTO table_name SELECT columns FROM table
    # より汎用的なパターンマッチングで対応
    # \r\n などのエスケープ文字も含めて処理
    insert_select_pattern = r"INSERT\s+INTO\s+\[?[^\]]+\]?\.?\[?[^\]]*\]?\s*[\r\n]*\s*SELECT\s+(.*?)\s*FROM"
    match_insert_select = re.search(insert_select_pattern, sql, re.IGNORECASE | re.DOTALL)
    
    if match_insert_select:
        select_part = match_insert_select.group(1)
        # \r\n を改行に変換
        select_part = select_part.replace('\\r\\n', '\n').replace('\\r', '\n').replace('\\n', '\n')
        # SELECT句から英語カラム名を抽出（既存の関数を再利用）
        select_sql = f"SELECT {select_part} FROM dummy"  # FROMを追加してパターンマッチングを安定化
        return extract_english_column_names(select_sql)
    
    raise AssertionError("INSERT文のカラムリストが見つかりません")


def compare_english_columns(select_sql: str, insert_sql: str) -> tuple[bool, Set[str], Set[str]]:
    """
    SELECT句とINSERT句から英語カラム名のみを抽出して比較する
    
    Returns:
        tuple: (一致するかどうか, SELECT側の英語カラム名, INSERT側の英語カラム名)
    """
    select_columns = extract_english_column_names(select_sql)
    insert_columns = extract_english_table_columns_from_insert(insert_sql)
    
    return select_columns == insert_columns, select_columns, insert_columns
