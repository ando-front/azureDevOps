import re
import unicodedata

def normalize_column_name(col: str) -> str:
    """
    SQLカラム名の表記揺れ（角括弧・関数・AS句・全角/半角・大文字小文字・コメント・クォート・空白）を吸収して正規化
    """
    # 角括弧除去
    col = re.sub(r"^\[|\]$", "", col.strip())
    # コメント除去
    col = re.sub(r"--.*$", "", col)
    # AS句でエイリアスのみ抽出
    m_as = re.match(r"(.+?)\s+AS\s+([\w@]+)", col, re.IGNORECASE)
    if m_as:
        col = m_as.group(2)
    # 関数名除去（FORMAT(ENTRY_DATE,...) → ENTRY_DATE）
    m_func = re.match(r"([A-Z_@][A-Z0-9_@]*)\(([^,\)]+)", col, re.IGNORECASE)
    if m_func:
        col = m_func.group(2).strip()
    # クォート除去
    col = col.replace("'", "")
    # 全角→半角
    col = unicodedata.normalize('NFKC', col)
    # 大文字化
    col = col.upper()
    # 前後空白
    col = col.strip()
    return col
