import re
from tests.unit.normalize_column import normalize_column_name
from typing import List


def extract_normalized_columns(sql: str) -> List[str]:
    """
    SQLのSELECT句またはINSERT SELECT句からカラムリストを抽出し、正規化して返す
    """
    m = re.search(r"SELECT(.+?)FROM", sql, re.DOTALL | re.IGNORECASE)
    if not m:
        raise AssertionError("SELECT句が見つかりません")
    body = m.group(1)
    cols: List[str] = []
    # 1行ずつ処理し、カンマ区切りで分割して各カラムを抽出
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith('--'):
            continue
        # NULL AS ... は除外
        if re.match(r"NULL\s+AS", line, re.IGNORECASE):
            continue
        # カンマで分割し、各要素を個別に処理
        for colpart in line.split(','):
            colpart = colpart.strip()
            if not colpart:
                continue
            m_as = re.match(r".+? AS ([A-Z0-9_]+)", colpart, re.IGNORECASE)
            if m_as:
                col = m_as.group(1).strip()
            else:
                col = colpart.split()[0].strip(',').strip()
            if col:
                cols.append(normalize_column_name(col))
    return cols
