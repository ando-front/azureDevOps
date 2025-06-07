#!/usr/bin/env python3
"""
E2Eテストファイルを再現可能フレームワークに一括移行するスクリプト

Usage:
    python migrate_e2e_tests.py
"""

import os
import glob
import re
from pathlib import Path

class E2ETestMigrator:
    """E2Eテストファイルを再現可能フレームワークに移行"""
    
    def __init__(self):
        self.workspace_root = "/Users/andokenji/Documents/書類 - 安藤賢二のMac mini/GitHub/azureDevOps"
        self.e2e_test_dir = os.path.join(self.workspace_root, "tests", "e2e")
        
    def find_all_e2e_tests(self):
        """全てのE2Eテストファイルを検索"""
        pattern = os.path.join(self.e2e_test_dir, "test_*.py")
        test_files = glob.glob(pattern)
        
        # 既に移行済みのファイルを除外
        excluded_files = [
            "test_final_integration.py"  # 既に移行済み
        ]
        
        filtered_files = []
        for file in test_files:
            basename = os.path.basename(file)
            if basename not in excluded_files:
                filtered_files.append(file)
                
        return filtered_files
    
    def get_import_statements(self):
        """再現可能フレームワークのインポート文"""
        return """from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class
"""
    
    def get_setup_class_method(self):
        """setup_classメソッドの再現可能フレームワーク対応版"""
        return '''    @classmethod
    def setup_class(cls):
        """再現可能テスト環境のセットアップ"""
        setup_reproducible_test_class()
        
        # Disable proxy settings for tests
        for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if var in os.environ:
                del os.environ[var]
'''
    
    def get_teardown_class_method(self):
        """teardown_classメソッドの再現可能フレームワーク対応版"""
        return '''    @classmethod 
    def teardown_class(cls):
        """再現可能テスト環境のクリーンアップ"""
        cleanup_reproducible_test_class()
'''
    
    def migrate_test_file(self, file_path):
        """単一のテストファイルを移行"""
        print(f"移行中: {os.path.basename(file_path)}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 既に移行済みかチェック
        if 'setup_reproducible_test_class' in content:
            print(f"  → スキップ（既に移行済み）")
            return False
            
        # インポート文を追加（ファイルの先頭部分で安全な場所に）
        import_stmt = self.get_import_statements()
        
        # DocStringの後、最初のimport文の前に追加
        import_pattern = r'(""".*?"""\s*\n)(.*?)(import [^\n]+\n|from [^\n]+\n)'
        match = re.search(import_pattern, content, re.DOTALL)
        
        if match:
            # DocStringの後にインポートを追加
            pre_docstring = match.group(1)
            middle_content = match.group(2)
            first_import = match.group(3)
            insertion_point = len(pre_docstring + middle_content)
            content = content[:insertion_point] + import_stmt + content[insertion_point:]
        else:
            # 通常のimport文の後に追加
            import_pattern = r'(import [^\n]+\n|from [^\n]+\n)'
            matches = list(re.finditer(import_pattern, content))
            
            if matches:
                # 最後のimport文の後に追加
                last_import = matches[-1]
                insertion_point = last_import.end()
                content = content[:insertion_point] + import_stmt + content[insertion_point:]
            else:
                # import文が見つからない場合、ファイルの先頭に追加
                lines = content.split('\n')
                # DocStringを探す
                docstring_end = 0
                in_docstring = False
                for i, line in enumerate(lines):
                    if '"""' in line:
                        if not in_docstring:
                            in_docstring = True
                        else:
                            docstring_end = i + 1
                            break
                
                if docstring_end > 0:
                    lines.insert(docstring_end, import_stmt.strip())
                else:
                    lines.insert(0, import_stmt.strip())
                content = '\n'.join(lines)
        
        # setup_classメソッドを処理（クラス内にのみ追加）
        class_matches = list(re.finditer(r'class\s+(\w+)[^:]*:\s*\n', content))
        
        for class_match in class_matches:
            class_name = class_match.group(1)
            if 'Test' in class_name:  # テストクラスのみ処理
                # このクラス内にsetup_classがあるかチェック
                class_start = class_match.end()
                # 次のクラスまたはファイル終端を見つける
                next_class = re.search(r'\nclass\s+\w+[^:]*:\s*\n', content[class_start:])
                class_end = class_start + next_class.start() if next_class else len(content)
                class_content = content[class_start:class_end]
                
                if '@classmethod' in class_content and 'def setup_class' in class_content:
                    # 既存のsetup_classを置換
                    setup_pattern = r'(\s*)@classmethod\s*\n\s*def setup_class\(cls\):[^}]*?(?=\n\s*(?:@|def|\Z))'
                    new_setup = self.get_setup_class_method()
                    class_content = re.sub(setup_pattern, new_setup, class_content, flags=re.MULTILINE | re.DOTALL)
                else:
                    # setup_classが存在しない場合、クラス定義の直後に追加
                    class_content = self.get_setup_class_method() + '\n' + class_content
                
                # teardown_classを追加（存在しない場合）
                if '@classmethod' not in class_content or 'def teardown_class' not in class_content:
                    teardown_method = self.get_teardown_class_method()
                    class_content = class_content + '\n' + teardown_method
                
                # 元のコンテンツに反映
                content = content[:class_start] + class_content + content[class_end:]
        
        # ファイルを更新
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"  → 移行完了")
            return True
        except Exception as e:
            print(f"  → エラー: {str(e)}")
            return False
    
    def migrate_all_tests(self):
        """全てのE2Eテストファイルを移行"""
        test_files = self.find_all_e2e_tests()
        
        print(f"=== E2Eテストファイル一括移行開始 ===")
        print(f"対象ファイル数: {len(test_files)}")
        print()
        
        migrated_count = 0
        skipped_count = 0
        
        for file_path in test_files:
            try:
                if self.migrate_test_file(file_path):
                    migrated_count += 1
                else:
                    skipped_count += 1
            except Exception as e:
                print(f"  → エラー: {str(e)}")
        
        print()
        print(f"=== 移行完了 ===")
        print(f"移行済み: {migrated_count}ファイル")
        print(f"スキップ: {skipped_count}ファイル")
        print(f"合計: {len(test_files)}ファイル")
        

def main():
    migrator = E2ETestMigrator()
    migrator.migrate_all_tests()

if __name__ == "__main__":
    main()
