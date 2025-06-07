"""
パイプライン単位E2Eテストスイート実行用設定

各パイプラインを独立してテストするためのテスト実行設定とユーティリティを提供します。
テストスイートは以下の機能を含みます：

1. パイプライン依存関係の管理
2. 並列実行可能なテストの特定
3. テスト結果の集約とレポート
4. エラー発生時の分析とデバッグ支援
"""

import pytest
import logging
import asyncio
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class PipelineCategory(Enum):
    """パイプラインカテゴリ分類"""
    DATA_COPY = "data_copy"           # データコピー系
    DATA_PROCESSING = "data_processing"  # データ処理系
    DATA_INTEGRATION = "data_integration"  # データ統合系
    REPORTING = "reporting"           # レポート系
    MAINTENANCE = "maintenance"       # メンテナンス系


class PipelinePriority(Enum):
    """パイプライン優先度"""
    CRITICAL = 1      # 最重要（他のパイプラインの前提条件）
    HIGH = 2         # 高優先度（ビジネス上重要）
    MEDIUM = 3       # 中優先度（定期実行）
    LOW = 4          # 低優先度（補助的）


@dataclass
class PipelineTestConfig:
    """パイプラインテスト設定"""
    name: str
    test_file: str
    category: PipelineCategory
    priority: PipelinePriority
    dependencies: List[str]
    expected_duration_minutes: int
    parallel_safe: bool
    description: str


class PipelineTestRegistry:
    """パイプラインテスト登録管理"""
    
    def __init__(self):
        self.pipelines = {}
        self._register_default_pipelines()
    
    def _register_default_pipelines(self):
        """デフォルトパイプライン登録"""
        
        # 1. Marketing Client DM Pipeline
        self.register_pipeline(PipelineTestConfig(
            name="pi_Copy_marketing_client_dm",
            test_file="test_e2e_pipeline_marketing_client_dm.py",
            category=PipelineCategory.DATA_COPY,
            priority=PipelinePriority.CRITICAL,
            dependencies=[],
            expected_duration_minutes=30,
            parallel_safe=True,
            description="Marketingスキーマの顧客DMを、omniスキーマの顧客ODSに全量コピーする"
        ))
        
        # 2. Client DM Bx Insert Pipeline
        self.register_pipeline(PipelineTestConfig(
            name="pi_Insert_ClientDmBx",
            test_file="test_e2e_pipeline_insert_clientdm_bx.py",
            category=PipelineCategory.DATA_PROCESSING,
            priority=PipelinePriority.HIGH,
            dependencies=["pi_Copy_marketing_client_dm"],
            expected_duration_minutes=25,
            parallel_safe=False,  # 顧客DMテーブルに依存
            description="顧客DMにBxを付与し作業テーブルに出力する"
        ))
          # 3. Point Grant Email Pipeline
        self.register_pipeline(PipelineTestConfig(
            name="pi_PointGrantEmail",
            test_file="test_e2e_pipeline_point_grant_email.py",
            category=PipelineCategory.REPORTING,
            priority=PipelinePriority.MEDIUM,
            dependencies=[],
            expected_duration_minutes=15,
            parallel_safe=True,
            description="ポイント付与メール送信処理"
        ))
          # 4. Action Point Current Month Entry List Pipeline
        self.register_pipeline(PipelineTestConfig(
            name="pi_Send_ActionPointCurrentMonthEntryList",
            test_file="test_e2e_pipeline_action_point_current_month_entry.py",
            category=PipelineCategory.DATA_INTEGRATION,
            priority=PipelinePriority.MEDIUM,
            dependencies=[],
            expected_duration_minutes=10,
            parallel_safe=True,
            description="アクションポイント当月エントリーリストSFTP連携"
        ))
        
        # 5. Client DM Send Pipeline
        self.register_pipeline(PipelineTestConfig(
            name="pi_Send_ClientDM",
            test_file="test_e2e_pipeline_client_dm.py",
            category=PipelineCategory.DATA_INTEGRATION,
            priority=PipelinePriority.HIGH,
            dependencies=[],
            expected_duration_minutes=20,
            parallel_safe=True,
            description="顧客DMデータのCSV出力およびSFMC SFTP連携"
        ))
    
    def register_pipeline(self, config: PipelineTestConfig):
        """パイプライン登録"""
        self.pipelines[config.name] = config
        logger.info(f"パイプライン登録: {config.name}")
    
    def get_pipeline(self, name: str) -> Optional[PipelineTestConfig]:
        """パイプライン設定取得"""
        return self.pipelines.get(name)
    
    def get_pipelines_by_category(self, category: PipelineCategory) -> List[PipelineTestConfig]:
        """カテゴリ別パイプライン取得"""
        return [p for p in self.pipelines.values() if p.category == category]
    
    def get_pipelines_by_priority(self, priority: PipelinePriority) -> List[PipelineTestConfig]:
        """優先度別パイプライン取得"""
        return [p for p in self.pipelines.values() if p.priority == priority]
    
    def get_executable_pipelines(self, completed_pipelines: List[str]) -> List[PipelineTestConfig]:
        """実行可能パイプライン取得（依存関係考慮）"""
        executable = []
        for pipeline in self.pipelines.values():
            if all(dep in completed_pipelines for dep in pipeline.dependencies):
                executable.append(pipeline)
        return executable
    
    def get_parallel_safe_pipelines(self) -> List[PipelineTestConfig]:
        """並列実行可能パイプライン取得"""
        return [p for p in self.pipelines.values() if p.parallel_safe]


class PipelineTestExecutor:
    """パイプラインテスト実行管理"""
    
    def __init__(self, registry: PipelineTestRegistry):
        self.registry = registry
        self.test_results = {}
        self.execution_order = []
        
    def execute_all_tests(self, 
                         parallel: bool = False, 
                         category_filter: Optional[PipelineCategory] = None,
                         priority_filter: Optional[PipelinePriority] = None) -> Dict[str, Any]:
        """全テスト実行"""
        logger.info("パイプライン単位E2Eテスト実行開始")
        start_time = datetime.now()
        
        # フィルタ適用
        target_pipelines = list(self.registry.pipelines.values())
        
        if category_filter:
            target_pipelines = [p for p in target_pipelines if p.category == category_filter]
        
        if priority_filter:
            target_pipelines = [p for p in target_pipelines if p.priority == priority_filter]
        
        # 実行順序決定
        execution_plan = self._create_execution_plan(target_pipelines, parallel)
        
        # テスト実行
        if parallel:
            results = self._execute_parallel_tests(execution_plan)
        else:
            results = self._execute_sequential_tests(execution_plan)
        
        end_time = datetime.now()
        total_duration = end_time - start_time
        
        # 結果集約
        summary = self._create_test_summary(results, total_duration)
        
        logger.info(f"パイプライン単位E2Eテスト実行完了: 総実行時間={total_duration}")
        
        return summary
    
    def _create_execution_plan(self, 
                             pipelines: List[PipelineTestConfig], 
                             parallel: bool) -> Dict[str, Any]:
        """実行計画作成"""
        logger.info("テスト実行計画作成開始")
        
        # 依存関係解決によるトポロジカルソート
        sorted_pipelines = self._topological_sort(pipelines)
        
        if parallel:
            # 並列実行グループ化
            execution_groups = self._create_parallel_groups(sorted_pipelines)
        else:
            # 順次実行
            execution_groups = [[p] for p in sorted_pipelines]
        
        plan = {
            "execution_groups": execution_groups,
            "total_pipelines": len(pipelines),
            "estimated_duration": sum(p.expected_duration_minutes for p in pipelines),
            "parallel_execution": parallel
        }
        
        logger.info(f"実行計画作成完了: {len(execution_groups)}グループ, {len(pipelines)}パイプライン")
        
        return plan
    
    def _topological_sort(self, pipelines: List[PipelineTestConfig]) -> List[PipelineTestConfig]:
        """トポロジカルソート（依存関係順序）"""
        pipeline_map = {p.name: p for p in pipelines}
        visited = set()
        temp_visited = set()
        result = []
        
        def visit(pipeline_name: str):
            if pipeline_name in temp_visited:
                raise ValueError(f"循環依存が検出されました: {pipeline_name}")
            
            if pipeline_name in visited:
                return
            
            temp_visited.add(pipeline_name)
            
            pipeline = pipeline_map.get(pipeline_name)
            if pipeline:
                for dep in pipeline.dependencies:
                    if dep in pipeline_map:
                        visit(dep)
                
                visited.add(pipeline_name)
                result.append(pipeline)
            
            temp_visited.remove(pipeline_name)
        
        for pipeline in pipelines:
            if pipeline.name not in visited:
                visit(pipeline.name)
        
        return result
    
    def _create_parallel_groups(self, sorted_pipelines: List[PipelineTestConfig]) -> List[List[PipelineTestConfig]]:
        """並列実行グループ作成"""
        groups = []
        remaining = sorted_pipelines.copy()
        completed = set()
        
        while remaining:
            current_group = []
            
            # 現在実行可能なパイプラインを抽出
            for pipeline in remaining[:]:
                # 依存関係確認
                can_execute = all(dep in completed for dep in pipeline.dependencies)
                
                # 並列実行安全性確認
                parallel_safe = pipeline.parallel_safe
                
                if can_execute and parallel_safe:
                    current_group.append(pipeline)
                    remaining.remove(pipeline)
                elif can_execute and not current_group:
                    # 並列不安全だが、他に実行可能なものがない場合は単独実行
                    current_group.append(pipeline)
                    remaining.remove(pipeline)
                    break
            
            if current_group:
                groups.append(current_group)
                completed.update(p.name for p in current_group)
            else:
                # デッドロック検出
                break
        
        return groups
    
    def _execute_sequential_tests(self, execution_plan: Dict[str, Any]) -> Dict[str, Any]:
        """順次テスト実行"""
        logger.info("順次テスト実行開始")
        results = {}
        
        for group_idx, group in enumerate(execution_plan["execution_groups"]):
            logger.info(f"実行グループ {group_idx + 1}/{len(execution_plan['execution_groups'])} 開始")
            
            for pipeline in group:
                logger.info(f"パイプラインテスト実行: {pipeline.name}")
                
                try:
                    result = self._execute_single_test(pipeline)
                    results[pipeline.name] = result
                    
                    if result["status"] == "FAILED":
                        logger.error(f"パイプラインテスト失敗: {pipeline.name}")
                        # 失敗時の継続ポリシーは設定可能
                        
                except Exception as e:
                    logger.error(f"パイプラインテスト実行エラー: {pipeline.name} - {str(e)}")
                    results[pipeline.name] = {
                        "status": "ERROR",
                        "error": str(e),
                        "duration_seconds": 0
                    }
        
        logger.info("順次テスト実行完了")
        return results
    
    def _execute_parallel_tests(self, execution_plan: Dict[str, Any]) -> Dict[str, Any]:
        """並列テスト実行"""
        logger.info("並列テスト実行開始")
        results = {}
        
        for group_idx, group in enumerate(execution_plan["execution_groups"]):
            logger.info(f"並列実行グループ {group_idx + 1}/{len(execution_plan['execution_groups'])} 開始")
            
            # 並列実行
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(group)) as executor:
                future_to_pipeline = {
                    executor.submit(self._execute_single_test, pipeline): pipeline 
                    for pipeline in group
                }
                
                for future in concurrent.futures.as_completed(future_to_pipeline):
                    pipeline = future_to_pipeline[future]
                    try:
                        result = future.result()
                        results[pipeline.name] = result
                        logger.info(f"パイプラインテスト完了: {pipeline.name}")
                    except Exception as e:
                        logger.error(f"パイプラインテスト実行エラー: {pipeline.name} - {str(e)}")
                        results[pipeline.name] = {
                            "status": "ERROR",
                            "error": str(e),
                            "duration_seconds": 0
                        }
        
        logger.info("並列テスト実行完了")
        return results
    
    def _execute_single_test(self, pipeline: PipelineTestConfig) -> Dict[str, Any]:
        """単一テスト実行"""
        start_time = datetime.now()
        
        try:
            # pytest実行
            test_file_path = Path(__file__).parent / pipeline.test_file
            
            if not test_file_path.exists():
                return {
                    "status": "SKIPPED",
                    "reason": f"テストファイルが存在しません: {pipeline.test_file}",
                    "duration_seconds": 0
                }
            
            # pytest実行（実際の実装では subprocess.run などを使用）
            exit_code = pytest.main([str(test_file_path), "-v", "--tb=short"])
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            status = "PASSED" if exit_code == 0 else "FAILED"
            
            return {
                "status": status,
                "duration_seconds": duration,
                "expected_duration_minutes": pipeline.expected_duration_minutes,
                "category": pipeline.category.value,
                "priority": pipeline.priority.value
            }
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return {
                "status": "ERROR",
                "error": str(e),
                "duration_seconds": duration
            }
    
    def _create_test_summary(self, results: Dict[str, Any], total_duration) -> Dict[str, Any]:
        """テスト結果サマリ作成"""
        passed_count = sum(1 for r in results.values() if r["status"] == "PASSED")
        failed_count = sum(1 for r in results.values() if r["status"] == "FAILED")
        error_count = sum(1 for r in results.values() if r["status"] == "ERROR")
        skipped_count = sum(1 for r in results.values() if r["status"] == "SKIPPED")
        
        total_tests = len(results)
        success_rate = (passed_count / total_tests * 100) if total_tests > 0 else 0
        
        summary = {
            "execution_summary": {
                "total_tests": total_tests,
                "passed": passed_count,
                "failed": failed_count,
                "errors": error_count,
                "skipped": skipped_count,
                "success_rate": success_rate,
                "total_duration_seconds": total_duration.total_seconds()
            },
            "test_results": results,
            "performance_analysis": self._analyze_performance(results),
            "failure_analysis": self._analyze_failures(results)
        }
        
        return summary
    
    def _analyze_performance(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """パフォーマンス分析"""
        durations = [r["duration_seconds"] for r in results.values() if "duration_seconds" in r]
        
        if not durations:
            return {}
        
        return {
            "average_duration": sum(durations) / len(durations),
            "max_duration": max(durations),
            "min_duration": min(durations),
            "total_duration": sum(durations)
        }
    
    def _analyze_failures(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """失敗分析"""
        failures = {
            name: result for name, result in results.items() 
            if result["status"] in ["FAILED", "ERROR"]
        }
        
        return {
            "failed_pipelines": list(failures.keys()),
            "failure_details": failures
        }


# テスト設定のグローバルインスタンス
pipeline_registry = PipelineTestRegistry()
test_executor = PipelineTestExecutor(pipeline_registry)


def execute_pipeline_test_suite(parallel: bool = False, 
                               category: Optional[str] = None,
                               priority: Optional[str] = None) -> Dict[str, Any]:
    """パイプラインテストスイート実行エントリーポイント"""
    
    # フィルタ変換
    category_filter = None
    if category:
        try:
            category_filter = PipelineCategory(category)
        except ValueError:
            logger.warning(f"無効なカテゴリ: {category}")
    
    priority_filter = None
    if priority:
        try:
            priority_filter = PipelinePriority(int(priority))
        except (ValueError, TypeError):
            logger.warning(f"無効な優先度: {priority}")
    
    # テスト実行
    return test_executor.execute_all_tests(
        parallel=parallel,
        category_filter=category_filter,
        priority_filter=priority_filter
    )


if __name__ == "__main__":
    # コマンドライン実行例
    import argparse
    
    parser = argparse.ArgumentParser(description="パイプライン単位E2Eテスト実行")
    parser.add_argument("--parallel", action="store_true", help="並列実行")
    parser.add_argument("--category", type=str, help="カテゴリフィルタ")
    parser.add_argument("--priority", type=str, help="優先度フィルタ")
    
    args = parser.parse_args()
    
    results = execute_pipeline_test_suite(
        parallel=args.parallel,
        category=args.category,
        priority=args.priority
    )
    
    print(f"テスト実行結果: {results['execution_summary']}")
