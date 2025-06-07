#!/usr/bin/env python3
"""
LinkedTemplates SQL外部化スクリプト
個別パイプライン定義ファイル（linkedTemplatesディレクトリ）内の長いSQLクエリを外部ファイルに分離

機能:
- linkedTemplatesディレクトリ内の全ARMテンプレートファイルを処理
- 1000文字以上の長いSQLクエリを自動検出
- 外部SQLファイルを作成し、適切な参照に置換
- ファイルサイズ削減による運用効率向上
- 既存SQLファイルとの重複を避けるための重複検知

作成日: 2024年12月
目的: CI/CDパイプライン最適化
"""

import json
import os
import re
import hashlib
from typing import Dict, List, Tuple, Optional
from pathlib import Path

class LinkedTemplatesSqlExternalizer:
    def __init__(self, templates_dir: str, external_sql_dir: str):
        self.templates_dir = Path(templates_dir)
        self.external_sql_dir = Path(external_sql_dir)
        self.external_sql_dir.mkdir(exist_ok=True)
        
        # 処理統計
        self.stats = {
            "processed_files": 0,
            "total_queries_found": 0,
            "queries_externalized": 0,
            "bytes_saved": 0,
            "existing_sql_files": 0
        }
        
        # 既存のSQLファイルマップ（重複検知用）
        self.existing_sql_files = self._load_existing_sql_files()
        
    def _load_existing_sql_files(self) -> Dict[str, str]:
        """既存のSQLファイルを読み込み、ハッシュマップを作成"""
        sql_files = {}
        if self.external_sql_dir.exists():
            for sql_file in self.external_sql_dir.glob("*.sql"):
                try:
                    with open(sql_file, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()[:8]
                        sql_files[content_hash] = sql_file.name
                        self.stats["existing_sql_files"] += 1
                except Exception as e:
                    print(f"⚠️ Warning: SQLファイル読み込みエラー {sql_file}: {e}")
        
        print(f"📁 既存SQLファイル: {len(sql_files)}個読み込み完了")
        return sql_files
    
    def _extract_sql_queries(self, content: str) -> List[Tuple[str, int, int]]:
        """ARMテンプレートからsqlReaderQueryを抽出"""
        queries = []
        
        # sqlReaderQueryパターンを検索
        pattern = r'"sqlReaderQuery":\s*"([^"\\]*(\\.[^"\\]*)*)"'
        
        for match in re.finditer(pattern, content, re.DOTALL):
            full_match = match.group(0)
            sql_content = match.group(1)
            
            # エスケープされた文字を復元
            sql_content = sql_content.replace('\\"', '"')
            sql_content = sql_content.replace('\\n', '\n')
            sql_content = sql_content.replace('\\r', '\r')
            sql_content = sql_content.replace('\\t', '\t')
            sql_content = sql_content.replace('\\\\', '\\')
            
            # クエリの長さをチェック（1000文字以上）
            if len(sql_content) >= 1000:
                queries.append((full_match, match.start(), match.end()))
                
        return queries
    
    def _generate_sql_filename(self, sql_content: str, file_context: str) -> str:
        """SQLクエリ内容に基づいて適切なファイル名を生成"""
        
        # クエリ内容から意味のあるキーワードを抽出
        content_lower = sql_content.lower()
        
        # テーブル名の抽出パターン
        table_patterns = [
            r'from\s+\[?(\w+)\]?\.\[?(\w+)\]?\.\[?([^[\]\s,;]+)',  # [schema].[table_name]
            r'from\s+\[?([^[\]\s,;]+)\]?',  # table_name
            r'truncate\s+table\s+\[?(\w+)\]?\.\[?(\w+)\]?\.\[?([^[\]\s,;]+)',
            r'insert\s+(?:into\s+)?\[?(\w+)\]?\.\[?(\w+)\]?\.\[?([^[\]\s,;]+)'
        ]
        
        table_name = None
        for pattern in table_patterns:
            match = re.search(pattern, content_lower)
            if match:
                # 最後のグループ（テーブル名）を取得
                table_name = match.groups()[-1] if match.groups() else None
                break
        
        # 処理タイプの特定
        if "支払アラート" in sql_content or "paymentalert" in content_lower:
            base_name = "支払アラート_作成"
        elif "顧客dm" in content_lower and "marketing" in content_lower:
            base_name = "table_marketing"
        elif "顧客dna" in content_lower:
            base_name = "table_dna"
        elif "lineid" in content_lower and "連携" in sql_content:
            base_name = "lineid_連携情報"
        elif "引越し予測" in sql_content or "moving" in content_lower:
            base_name = "引越し予測_リスト"
        elif "電気契約" in sql_content and "thanks" in content_lower:
            base_name = "電気契約_thanks"
        elif "支払方法変更" in sql_content:
            base_name = "支払方法変更"
        elif "開栓" in sql_content and "支払方法" in sql_content:
            base_name = "開栓_支払方法案内"
        elif "本人特定契約" in sql_content:
            base_name = "本人特定契約_if連携"
        elif "利用サービス" in sql_content:
            base_name = "利用サービス_出力"
        elif "ガス機器" in sql_content and "修理" in sql_content:
            base_name = "ガス機器_修理"
        elif table_name:
            base_name = f"table_{table_name}"
        else:
            # ファイルコンテキストから推測
            if "template_3" in file_context.lower():
                base_name = "template3_query"
            elif "template_4" in file_context.lower():
                base_name = "template4_query"
            else:
                base_name = "linked_template_query"
        
        # ハッシュを追加してユニーク性を保証
        content_hash = hashlib.sha256(sql_content.encode('utf-8')).hexdigest()[:6]
        
        return f"{base_name}_{content_hash}.sql"
    
    def _check_existing_sql_file(self, sql_content: str) -> Optional[str]:
        """既存のSQLファイルに同じ内容があるかチェック"""
        content_hash = hashlib.sha256(sql_content.strip().encode('utf-8')).hexdigest()[:8]
        return self.existing_sql_files.get(content_hash)
    
    def _save_sql_file(self, sql_content: str, filename: str) -> str:
        """SQLファイルを保存"""
        sql_path = self.external_sql_dir / filename
        
        with open(sql_path, 'w', encoding='utf-8') as f:
            f.write(sql_content)
        
        # ハッシュマップを更新
        content_hash = hashlib.sha256(sql_content.strip().encode('utf-8')).hexdigest()[:8]
        self.existing_sql_files[content_hash] = filename
        
        return filename
    
    def process_template_file(self, template_path: Path) -> Dict:
        """個別テンプレートファイルを処理"""
        print(f"\n🔄 処理中: {template_path.name}")
        
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            print(f"❌ ファイル読み込みエラー: {e}")
            return {"success": False, "error": str(e)}
        
        original_size = len(content)
        queries = self._extract_sql_queries(content)
        
        if not queries:
            print(f"   📊 長いSQLクエリは見つかりませんでした")
            return {"success": True, "queries_processed": 0, "size_reduction": 0}
        
        print(f"   📊 {len(queries)}個の長いSQLクエリを発見")
        
        # 後ろから処理して位置がずれないようにする
        modified_content = content
        queries_processed = 0
        
        for i, (full_match, start_pos, end_pos) in enumerate(reversed(queries)):
            self.stats["total_queries_found"] += 1
            
            # SQLクエリ内容を抽出
            sql_match = re.search(r'"sqlReaderQuery":\s*"([^"\\]*(\\.[^"\\]*)*)"', full_match)
            if not sql_match:
                continue
                
            sql_content = sql_match.group(1)
            # エスケープ解除
            sql_content = sql_content.replace('\\"', '"')
            sql_content = sql_content.replace('\\n', '\n')
            sql_content = sql_content.replace('\\r', '\r')
            sql_content = sql_content.replace('\\t', '\t')
            sql_content = sql_content.replace('\\\\', '\\')
            
            # 既存ファイルをチェック
            existing_file = self._check_existing_sql_file(sql_content)
            if existing_file:
                filename = existing_file
                print(f"   ♻️ 既存SQLファイルを再利用: {filename}")
            else:
                # 新しいSQLファイルを作成
                filename = self._generate_sql_filename(sql_content, template_path.name)
                self._save_sql_file(sql_content, filename)
                print(f"   📄 新規SQLファイル作成: {filename}")
                self.stats["queries_externalized"] += 1
            
            # 参照に置換
            replacement = f'"sqlReaderQuery": "{{{{EXTERNAL_SQL:{filename}}}}}"'
            
            # 文字列置換（後ろから処理しているため位置は正確）
            modified_content = modified_content[:start_pos] + replacement + modified_content[end_pos:]
            queries_processed += 1
        
        # 外部化されたテンプレートファイルを保存
        external_template_path = template_path.parent / f"{template_path.stem}_External.json"
        try:
            with open(external_template_path, 'w', encoding='utf-8') as f:
                f.write(modified_content)
                
            size_reduction = original_size - len(modified_content)
            self.stats["bytes_saved"] += size_reduction
            
            print(f"   ✅ 外部化完了: {external_template_path.name}")
            print(f"   📉 サイズ削減: {size_reduction:,} bytes ({size_reduction/1024:.1f}KB)")
            
            return {
                "success": True,
                "queries_processed": queries_processed,
                "size_reduction": size_reduction,
                "output_file": external_template_path.name
            }
            
        except Exception as e:
            print(f"   ❌ ファイル保存エラー: {e}")
            return {"success": False, "error": str(e)}
    
    def process_all_templates(self) -> Dict:
        """linkedTemplatesディレクトリ内の全テンプレートファイルを処理"""
        print(f"🚀 LinkedTemplates SQL外部化処理開始")
        print(f"📁 対象ディレクトリ: {self.templates_dir}")
        print(f"📁 出力ディレクトリ: {self.external_sql_dir}")
        
        template_files = list(self.templates_dir.glob("ArmTemplate_*.json"))
        
        if not template_files:
            print("❌ 処理対象のARMテンプレートファイルが見つかりません")
            return {"success": False, "error": "No template files found"}
        
        print(f"📊 処理対象ファイル: {len(template_files)}個")
        
        results = {}
        
        for template_file in template_files:
            self.stats["processed_files"] += 1
            result = self.process_template_file(template_file)
            results[template_file.name] = result
        
        # 統計情報を表示
        print(f"\n📊 =========================")
        print(f"📊 処理完了サマリー")
        print(f"📊 =========================")
        print(f"📁 処理ファイル数: {self.stats['processed_files']}")
        print(f"🔍 発見SQLクエリ数: {self.stats['total_queries_found']}")
        print(f"📄 外部化SQLクエリ数: {self.stats['queries_externalized']}")
        print(f"♻️ 既存SQLファイル再利用: {self.stats['existing_sql_files']}")
        print(f"📉 総サイズ削減: {self.stats['bytes_saved']:,} bytes ({self.stats['bytes_saved']/1024:.1f}KB)")
        
        return {
            "success": True,
            "stats": self.stats,
            "results": results
        }

def main():
    """メイン実行関数"""
    base_dir = Path("c:/Users/0190402/git/tg-ma-MA-ADF-TEST")
    templates_dir = base_dir / "src/dev/arm_template/linkedTemplates"
    external_sql_dir = base_dir / "external_sql"
    
    # 処理実行
    externalizer = LinkedTemplatesSqlExternalizer(
        str(templates_dir),
        str(external_sql_dir)
    )
    
    result = externalizer.process_all_templates()
    
    if result["success"]:
        print(f"\n✅ LinkedTemplates SQL外部化処理が正常に完了しました！")
        return 0
    else:
        print(f"\n❌ 処理中にエラーが発生しました: {result.get('error', 'Unknown error')}")
        return 1

if __name__ == "__main__":
    exit(main())
