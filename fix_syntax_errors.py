#!/usr/bin/env python3
"""
E2Eテストファイルの構文エラーをチェックして自動修正するスクリプト
"""

import os
import glob
import ast
import re

def check_and_fix_syntax_errors():
    """E2Eテストファイルの構文エラーをチェックして修正"""
    workspace_root = "/Users/andokenji/Documents/書類 - 安藤賢二のMac mini/GitHub/azureDevOps"
    e2e_test_dir = os.path.join(workspace_root, "tests", "e2e")
    
    pattern = os.path.join(e2e_test_dir, "test_*.py")
    test_files = glob.glob(pattern)
    
    syntax_errors = []
    fixed_files = []
    
    for file_path in test_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Python構文チェック
            try:
                ast.parse(content)
                # print(f"✅ {os.path.basename(file_path)}: 構文OK")
            except SyntaxError as e:
                print(f"❌ {os.path.basename(file_path)}: 構文エラー (Line {e.lineno}): {e.msg}")
                syntax_errors.append((file_path, e))
                
                # 一般的な構文エラーパターンを修正
                fixed_content = fix_common_syntax_errors(content, file_path)
                
                if fixed_content != content:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(fixed_content)
                    fixed_files.append(file_path)
                    print(f"  → 修正を試行しました")
                    
        except Exception as e:
            print(f"⚠️ {os.path.basename(file_path)}: ファイル読み込みエラー: {e}")
    
    print(f"\n=== 構文チェック結果 ===")
    print(f"チェック済み: {len(test_files)}ファイル")
    print(f"構文エラー: {len(syntax_errors)}ファイル")
    print(f"修正試行: {len(fixed_files)}ファイル")
    
    return syntax_errors, fixed_files

def fix_common_syntax_errors(content, file_path):
    """一般的な構文エラーパターンを修正"""
    original_content = content
    
    # パターン1: "class TestName:    @classmethod" を修正
    pattern1 = r'(class\s+\w+[^:]*:)\s*(@classmethod)'
    replacement1 = r'\1\n    \n    \2'
    content = re.sub(pattern1, replacement1, content)
    
    # パターン2: メソッド定義の前に適切な改行を追加
    pattern2 = r'([^:\n])(\s*@classmethod\s*\n\s*def\s+\w+)'
    replacement2 = r'\1\n    \2'
    content = re.sub(pattern2, replacement2, content)
    
    # パターン3: インデント修正
    lines = content.split('\n')
    fixed_lines = []
    in_class = False
    
    for i, line in enumerate(lines):
        if re.match(r'^class\s+\w+', line):
            in_class = True
            fixed_lines.append(line)
        elif in_class and line.strip().startswith('@classmethod'):
            # @classmethodの前に適切なインデントを確保
            if fixed_lines and not fixed_lines[-1].strip() == '':
                fixed_lines.append('')  # 空行を追加
            fixed_lines.append('    ' + line.strip())
        elif in_class and line.strip().startswith('def ') and i > 0 and lines[i-1].strip().startswith('@classmethod'):
            # @classmethodの後のdefにも適切なインデント
            fixed_lines.append('    ' + line.strip())
        else:
            fixed_lines.append(line)
    
    content = '\n'.join(fixed_lines)
    
    if content != original_content:
        print(f"  → パターン修正を適用: {os.path.basename(file_path)}")
    
    return content

if __name__ == "__main__":
    check_and_fix_syntax_errors()
