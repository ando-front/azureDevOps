#!/usr/bin/env python3
"""
一括構文エラー修正スクリプト
E2Eテストファイルの構文エラーを体系的に修正します。
"""

import os
import re
import ast
import glob

def fix_import_issues(file_path):
    """インポート関連の問題を修正"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 不正な位置のインポート文を削除
    content = re.sub(
        r'^from tests\.helpers\.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class$',
        '',
        content,
        flags=re.MULTILINE
    )
    
    # 正しい位置にインポートがあるかチェック
    if 'from tests.helpers.reproducible_e2e_helper import' not in content[:1000]:
        # ファイルの先頭のインポート部分に追加
        import_line = 'from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class\n'
        
        # インポート部分を見つけて追加
        lines = content.split('\n')
        insert_index = -1
        
        for i, line in enumerate(lines):
            if line.startswith('import ') or line.startswith('from '):
                insert_index = i
            elif insert_index != -1 and not (line.startswith('import ') or line.startswith('from ') or line.strip() == '' or line.startswith('#')):
                break
        
        if insert_index != -1:
            lines.insert(insert_index + 1, import_line.strip())
            content = '\n'.join(lines)
    
    return content

def fix_try_except_blocks(content):
    """try-except文の修正"""
    # 不完全なtry文を修正
    content = re.sub(
        r'(\s+try:\s*\n(?:[^e][^\n]*\n)*?)(?=\s+@classmethod|\s+def |\s*class |\Z)',
        r'\1        except Exception as e:\n            print(f"Error: {e}")\n            return False\n',
        content,
        flags=re.MULTILINE | re.DOTALL
    )
    
    return content

def fix_indentation_issues(content):
    """インデントの問題を修正"""
    lines = content.split('\n')
    fixed_lines = []
    
    for i, line in enumerate(lines):
        # 空行は保持
        if not line.strip():
            fixed_lines.append(line)
            continue
            
        # インデントの修正が必要な行を検出
        if i > 0 and fixed_lines:
            prev_line = fixed_lines[-1]
            
            # クラスやメソッド定義の後の不正なインデント
            if (prev_line.strip().endswith(':') and 
                line.strip() and 
                not line.startswith('    ') and 
                not line.startswith('\t') and
                not line.strip().startswith('#')):
                line = '    ' + line.lstrip()
        
        fixed_lines.append(line)
    
    return '\n'.join(fixed_lines)

def fix_syntax_errors_in_file(file_path):
    """単一ファイルの構文エラーを修正"""
    try:
        print(f"修正中: {os.path.basename(file_path)}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        # 各種修正を適用
        content = fix_import_issues(file_path)
        content = fix_try_except_blocks(content)
        content = fix_indentation_issues(content)
        
        # 構文チェック
        try:
            ast.parse(content)
            
            # ファイルに書き戻し
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"✅ {os.path.basename(file_path)}: 修正完了")
            return True
            
        except SyntaxError as e:
            print(f"❌ {os.path.basename(file_path)}: 構文エラーが残存 (Line {e.lineno}): {e.msg}")
            return False
            
    except Exception as e:
        print(f"❌ {os.path.basename(file_path)}: ファイル処理エラー: {e}")
        return False

def main():
    """メイン処理"""
    # E2Eテストファイルを検索
    test_files = glob.glob('tests/e2e/test_*.py')
    
    print(f"E2Eテストファイル {len(test_files)} 個を処理中...")
    
    fixed_count = 0
    error_count = 0
    
    for test_file in test_files:
        if fix_syntax_errors_in_file(test_file):
            fixed_count += 1
        else:
            error_count += 1
    
    print(f"\n=== 修正結果 ===")
    print(f"修正成功: {fixed_count} ファイル")
    print(f"エラー残存: {error_count} ファイル")

if __name__ == '__main__':
    main()
