# SQL外部化ファイル名改善スクリプト
import json
import os
import re
from pathlib import Path

def extract_pipeline_info_from_query(query_content):
    """SQLクエリから意味のある名前を抽出"""
    # コメントから処理名を抽出
    comment_patterns = [
        r'--\s*([^\r\n]+)',
        r'/\*\s*([^*]+)\*/',
    ]
    
    for pattern in comment_patterns:
        matches = re.findall(pattern, query_content, re.MULTILINE)
        if matches:
            first_comment = matches[0].strip()
            # 日本語の処理名を検索
            if any(char in first_comment for char in ['顧客', 'マスタ', 'データ', '処理', '作成', 'コピー']):
                return first_comment[:50]  # 最初の50文字
    
    # テーブル名から推測
    table_patterns = [
        r'FROM\s+\[?([^\[\]\s]+)\]?',
        r'INSERT\s+INTO\s+\[?([^\[\]\s]+)\]?',
        r'UPDATE\s+\[?([^\[\]\s]+)\]?',
    ]
    
    for pattern in table_patterns:
        matches = re.findall(pattern, query_content, re.IGNORECASE)
        if matches:
            table_name = matches[0].replace('[', '').replace(']', '')
            return f"table_{table_name}"
    
    return None

def improve_sql_file_names():
    """SQLファイル名を改善"""
    print("=== SQLファイル名改善処理 ===")
    
    sql_dir = "external_sql"
    arm_template = "ARMTemplateForFactory_External.json"
    
    if not os.path.exists(sql_dir):
        print(f"エラー: SQLディレクトリが見つかりません: {sql_dir}")
        return 1
    
    if not os.path.exists(arm_template):
        print(f"エラー: ARMテンプレートが見つかりません: {arm_template}")
        return 1
    
    # ARMテンプレート読み込み
    with open(arm_template, 'r', encoding='utf-8') as f:
        arm_content = f.read()
    
    # SQLファイルの名前変更マッピング
    file_mappings = {}
    
    for sql_file in sorted(os.listdir(sql_dir)):
        if sql_file.endswith('.sql'):
            file_path = os.path.join(sql_dir, sql_file)
            
            with open(file_path, 'r', encoding='utf-8') as f:
                query_content = f.read()
            
            # 意味のある名前を抽出
            meaningful_name = extract_pipeline_info_from_query(query_content)
            
            if meaningful_name:
                # ファイル名として適切な文字に変換
                safe_name = re.sub(r'[^\w\-_]', '_', meaningful_name)
                safe_name = re.sub(r'_+', '_', safe_name).strip('_')
                new_filename = f"{safe_name}.sql"
                
                # 重複回避
                counter = 1
                original_new_filename = new_filename
                while new_filename in file_mappings.values():
                    new_filename = f"{safe_name}_{counter}.sql"
                    counter += 1
                
                file_mappings[sql_file] = new_filename
                print(f"変更予定: {sql_file} -> {new_filename}")
            else:
                # 意味のある名前が見つからない場合はそのまま
                file_mappings[sql_file] = sql_file
                print(f"維持: {sql_file}")
    
    # ファイル名変更実行
    updated_arm_content = arm_content
    
    for old_name, new_name in file_mappings.items():
        if old_name != new_name:
            # SQLファイルをリネーム
            old_path = os.path.join(sql_dir, old_name)
            new_path = os.path.join(sql_dir, new_name)
            
            try:
                os.rename(old_path, new_path)
                print(f"リネーム完了: {old_name} -> {new_name}")
                
                # ARMテンプレート内の参照を更新
                old_ref = f"{{{{EXTERNAL_SQL:{old_name}}}}}"
                new_ref = f"{{{{EXTERNAL_SQL:{new_name}}}}}"
                updated_arm_content = updated_arm_content.replace(old_ref, new_ref)
                
            except Exception as e:
                print(f"リネームエラー: {old_name} -> {new_name}: {e}")
    
    # 更新されたARMテンプレートを保存
    with open(arm_template, 'w', encoding='utf-8') as f:
        f.write(updated_arm_content)
    
    print("=== ファイル名改善完了 ===")
    print("最終的なSQLファイル:")
    
    for sql_file in sorted(os.listdir(sql_dir)):
        if sql_file.endswith('.sql'):
            file_path = os.path.join(sql_dir, sql_file)
            size_kb = os.path.getsize(file_path) / 1024
            print(f"  - {sql_file} ({size_kb:.1f} KB)")
    
    return 0

if __name__ == "__main__":
    improve_sql_file_names()
