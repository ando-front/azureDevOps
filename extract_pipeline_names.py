import json
import re

def extract_pipeline_names():
    """ARMテンプレートからパイプライン名を抽出する"""
    
    arm_template_path = r"src\dev\arm_template\ARMTemplateForFactory.json"
    
    try:
        with open(arm_template_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # パイプライン名のパターンを検索
        pattern = r'"name":\s*"\[concat\(parameters\(\'factoryName\'\),\s*\'\/([^\']+)\'\)\]",\s*"type":\s*"Microsoft\.DataFactory\/factories\/pipelines"'
        matches = re.findall(pattern, content)
        
        print(f"パイプライン総数: {len(matches)}")
        print("\nパイプライン一覧:")
        for i, pipeline_name in enumerate(matches, 1):
            print(f"{i:2d}. {pipeline_name}")
            
        return matches
        
    except FileNotFoundError:
        print(f"ファイルが見つかりません: {arm_template_path}")
        return []
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        return []

if __name__ == "__main__":
    pipeline_names = extract_pipeline_names()
