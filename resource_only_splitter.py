#!/usr/bin/env python3
"""
ARMテンプレートからADFリソースのみを抽出して分割するスクリプト
テストで期待される形式（リソースオブジェクトのみ）で出力する
"""

print("スクリプトが読み込まれました")

import json
import shutil
from pathlib import Path
from datetime import datetime

class ResourceOnlySplitter:
    def __init__(self, source_dir="src/dev", arm_template_dir="src/dev/arm_template"):
        self.source_dir = Path(source_dir)
        self.arm_template_dir = Path(arm_template_dir)
        
    def backup_existing_files(self):
        """既存のファイルをバックアップ"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = Path(f"backups/src_dev_backup_{timestamp}")
        
        if self.source_dir.exists():
            backup_dir.parent.mkdir(exist_ok=True)
            shutil.copytree(self.source_dir, backup_dir)
            print(f"バックアップを作成しました: {backup_dir}")
            return backup_dir
        return None
    
    def load_arm_template(self):
        """ARM テンプレートファイルを読み込み"""
        template_files = list(self.arm_template_dir.glob("*.json"))
        
        if not template_files:
            print(f"ARM テンプレートファイルが見つかりません: {self.arm_template_dir}")
            return None
            
        template_file = template_files[0]  # 最初のファイルを使用
        print(f"ARM テンプレートを読み込み中: {template_file}")
        
        with open(template_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def categorize_resource(self, resource):
        """リソースのタイプに基づいてカテゴリを決定"""
        resource_type = resource.get("type", "")
        
        if "pipelines" in resource_type:
            return "pipeline"
        elif "datasets" in resource_type:
            return "dataset"
        elif "linkedServices" in resource_type:
            return "linkedService"
        elif "triggers" in resource_type:
            return "trigger"
        elif "dataflows" in resource_type:
            return "dataflow"
        elif "integrationRuntimes" in resource_type:
            return "integrationRuntime"
        elif "managedVirtualNetworks" in resource_type:
            return "managedVirtualNetwork"
        elif "managedPrivateEndpoints" in resource_type:
            return "managedPrivateEndpoint"
        else:
            return "other"
    
    def extract_resource_name(self, resource):
        """リソース名を抽出"""
        name = resource.get("name", "")
        
        # ARM テンプレートの動的名前からベース名を抽出
        if "[concat" in name:
            # [concat(parameters('factoryName'), '/リソース名')] の形式から抽出
            parts = name.split("'")
            if len(parts) >= 4:
                resource_name = parts[3]  # リソース名部分
                # '/' で始まる場合は削除
                return resource_name.lstrip('/')
        elif "[parameters" in name:
            # [parameters('factoryName')] の場合はスキップ
            return None
        
        # 通常の名前の場合
        if name and not name.startswith('['):
            return name.lstrip('/')
        
        return None
    
    def save_resource(self, resource, category, resource_name):
        """リソースを適切なディレクトリに保存"""
        if not resource_name:
            return
            
        # ディレクトリを作成
        category_dir = self.source_dir / category
        category_dir.mkdir(parents=True, exist_ok=True)
        
        # デバッグ用出力
        print(f"カテゴリディレクトリ: {category_dir}")
        print(f"リソース名: {resource_name}")
        
        # managedPrivateEndpointの場合は階層構造を維持
        if category == "managedPrivateEndpoint":
            # リソース名から階層構造を抽出 (例: vnet/endpoint)
            name_parts = resource_name.split('/')
            if len(name_parts) > 1:
                sub_dir = category_dir / name_parts[0]
                sub_dir.mkdir(exist_ok=True)
                file_path = sub_dir / f"{name_parts[1]}.json"
            else:
                file_path = category_dir / f"{resource_name}.json"
        else:
            file_path = category_dir / f"{resource_name}.json"
        
        # リソースオブジェクトのみを保存（ARM テンプレート構造ではなく）
        resource_only = {
            "name": resource_name,
            "type": resource.get("type"),
            "properties": resource.get("properties", {}),
            "dependsOn": resource.get("dependsOn", [])        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(resource_only, f, indent=2, ensure_ascii=False)
        
        return file_path
    
    def split_resources(self):
        """リソースを分割"""
        print("=== リソース分割開始 ===")
        
        # 既存ファイルをバックアップ
        backup_dir = self.backup_existing_files()
        
        # ARM テンプレートを読み込み
        template = self.load_arm_template()
        if not template:
            print("ARM テンプレートの読み込みに失敗しました")
            return False
        
        resources = template.get("resources", [])
        print(f"処理対象リソース数: {len(resources)}")
        
        # 統計用カウンター
        stats = {}
        saved_files = []
        
        for resource in resources:
            category = self.categorize_resource(resource)
            resource_name = self.extract_resource_name(resource)
            
            if resource_name:
                file_path = self.save_resource(resource, category, resource_name)
                if file_path:
                    saved_files.append(file_path)
                    stats[category] = stats.get(category, 0) + 1
          # 結果を出力
        print("\n=== 分割結果 ===")
        for category, count in stats.items():
            print(f"{category}: {count} ファイル")
        
        print(f"\n総計: {len(saved_files)} ファイルを作成しました")
        print(f"バックアップ: {backup_dir}")
        return True

def main():
    """メイン実行関数"""
    print("=== ResourceOnlySplitter 開始 ===")
    splitter = ResourceOnlySplitter()
    success = splitter.split_resources()
    
    if success:
        print("\nリソース分割が完了しました！")
        print("次のステップ: python -m pytest tests/unit/ -v")
    else:
        print("リソース分割に失敗しました。")

if __name__ == "__main__":
    print("スクリプトが実行されました")
    main()
