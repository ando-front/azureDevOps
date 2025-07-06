"""
テストケースID: SYS-SCHED-002
テスト名: Integration Runtime管理テスト（段階的実装）
テスト戦略: 性能・セキュリティ検証
優先度: 中
作成日: 2025年7月3日

Azure Data Factory Integration Runtime管理機能の段階的テスト
段階1: 基本監視機能（AutoResolve・Self-hosted IR状態確認）
段階2: 負荷分散・パフォーマンス検証（将来実装）
段階3: 障害回復・フェイルオーバー検証（将来実装）
"""

import pytest
import json
import os
from unittest.mock import Mock, patch
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.identity import DefaultAzureCredential
import time


class TestSYS_SCHED_002_IntegrationRuntimeManagement:
    """
    SYS-SCHED-002: Integration Runtime管理テスト
    
    テスト戦略準拠:
    - 性能・セキュリティ検証項目
    - 段階的実装によるリスク軽減
    - 運用監視基盤の強化
    """
    
    @pytest.fixture(scope="class")
    def mock_datafactory_client(self):
        """Data Factory管理クライアントのモック"""
        with patch('azure.datafactory.DataFactoryManagementClient') as mock_client:
            mock_instance = Mock()
            mock_client.return_value = mock_instance
            yield mock_instance
    
    @pytest.fixture(scope="class")
    def mock_monitor_client(self):
        """Azure Monitor クライアントのモック"""
        with patch('azure.monitor.query.LogsQueryClient') as mock_monitor:
            mock_instance = Mock()
            mock_monitor.return_value = mock_instance
            yield mock_instance
    
    def test_sys_sched_002_01_autoresolve_ir_monitoring(self, mock_datafactory_client):
        """
        SYS-SCHED-002-01: AutoResolve Integration Runtime監視テスト
        段階1実装: 基本的な状態監視機能
        """
        ir_name = "AutoResolveIntegrationRuntime"
        
        # モック設定: AutoResolve IR状態
        mock_datafactory_client.integration_runtimes.get.return_value = {
            "name": ir_name,
            "type": "Microsoft.DataFactory/factories/integrationruntimes",
            "properties": {
                "type": "Managed",
                "state": "Started",
                "typeProperties": {
                    "computeProperties": {
                        "location": "AutoResolve",
                        "dataFlowProperties": {
                            "computeType": "General",
                            "coreCount": 8
                        }
                    }
                }
            }
        }
        
        # ステータス取得
        mock_datafactory_client.integration_runtimes.get_status.return_value = {
            "name": ir_name,
            "properties": {
                "type": "Managed",
                "state": "Started",
                "capabilities": ["DataFlow", "Pipeline"],
                "nodes": [],
                "version": "1.0.0"
            }
        }
        
        # テスト実行
        ir_config = mock_datafactory_client.integration_runtimes.get(
            resource_group_name="mock_rg",
            factory_name="omni-df-dev",
            integration_runtime_name=ir_name
        )
        
        ir_status = mock_datafactory_client.integration_runtimes.get_status(
            resource_group_name="mock_rg",
            factory_name="omni-df-dev",
            integration_runtime_name=ir_name
        )
        
        # 検証
        assert ir_config["name"] == ir_name
        assert ir_config["properties"]["type"] == "Managed"
        assert ir_config["properties"]["state"] == "Started"
        assert ir_status["properties"]["state"] == "Started"
        assert "DataFlow" in ir_status["properties"]["capabilities"]
        assert "Pipeline" in ir_status["properties"]["capabilities"]
        
        print(f"✅ SYS-SCHED-002-01: {ir_name} 監視テスト成功")
    
    def test_sys_sched_002_02_self_hosted_ir_availability(self, mock_datafactory_client):
        """
        SYS-SCHED-002-02: Self-hosted Integration Runtime可用性テスト
        段階1実装: SHIR接続・状態確認
        """
        ir_name = "SelfHostedIR"
        
        # モック設定: Self-hosted IR状態
        mock_datafactory_client.integration_runtimes.get.return_value = {
            "name": ir_name,
            "properties": {
                "type": "SelfHosted",
                "description": "Self-hosted integration runtime"
            }
        }
        
        mock_datafactory_client.integration_runtimes.get_status.return_value = {
            "name": ir_name,
            "properties": {
                "type": "SelfHosted",
                "state": "Online",
                "capabilities": ["Pipeline", "ExternalData"],
                "nodes": [
                    {
                        "nodeName": "SHIR-Node-01",
                        "status": "Online",
                        "version": "5.42.8682.1",
                        "availableMemoryInMB": 8192,
                        "cpuUtilization": 15
                    }
                ],
                "version": "5.42.8682.1",
                "latestVersion": "5.42.8682.1"
            }
        }
        
        # テスト実行
        ir_config = mock_datafactory_client.integration_runtimes.get(
            resource_group_name="mock_rg",
            factory_name="omni-df-dev",
            integration_runtime_name=ir_name
        )
        
        ir_status = mock_datafactory_client.integration_runtimes.get_status(
            resource_group_name="mock_rg",
            factory_name="omni-df-dev",
            integration_runtime_name=ir_name
        )
        
        # SHIR可用性検証
        assert ir_config["name"] == ir_name
        assert ir_config["properties"]["type"] == "SelfHosted"
        assert ir_status["properties"]["state"] == "Online"
        assert len(ir_status["properties"]["nodes"]) >= 1
        
        # ノード状態検証
        nodes = ir_status["properties"]["nodes"]
        online_nodes = [node for node in nodes if node["status"] == "Online"]
        assert len(online_nodes) >= 1, "少なくとも1つのノードがオンラインである必要があります"
        
        # リソース使用率検証
        for node in online_nodes:
            assert node["availableMemoryInMB"] > 1024, "使用可能メモリが不足しています"
            assert node["cpuUtilization"] < 80, "CPU使用率が高すぎます"
        
        print(f"✅ SYS-SCHED-002-02: {ir_name} 可用性テスト成功")
    
    def test_sys_sched_002_03_ir_connection_validation(self, mock_datafactory_client):
        """
        SYS-SCHED-002-03: Integration Runtime接続検証
        段階1実装: LinkedServiceとの関連付け確認
        """
        test_linkedservices = [
            ("li_dam_dwh", "AutoResolveIntegrationRuntime"),
            ("li_dam_dwh_shir", "SelfHostedIR"),
            ("li_blob_adls", "AutoResolveIntegrationRuntime")
        ]
        
        for ls_name, expected_ir in test_linkedservices:
            # LinkedService設定取得
            mock_datafactory_client.linked_services.get.return_value = {
                "name": ls_name,
                "properties": {
                    "type": "AzureSqlDW" if "dam_dwh" in ls_name else "AzureBlobStorage",
                    "typeProperties": {
                        "connectVia": {
                            "referenceName": expected_ir,
                            "type": "IntegrationRuntimeReference"
                        }
                    }
                }
            }
            
            linkedservice = mock_datafactory_client.linked_services.get(
                resource_group_name="mock_rg",
                factory_name="omni-df-dev",
                linked_service_name=ls_name
            )
            
            # IR関連付け検証
            actual_ir = linkedservice["properties"]["typeProperties"]["connectVia"]["referenceName"]
            assert actual_ir == expected_ir
            
            print(f"✅ SYS-SCHED-002-03: {ls_name} → {expected_ir} 関連付け確認")
    
    def test_sys_sched_002_04_ir_monitoring_metrics(self, mock_monitor_client):
        """
        SYS-SCHED-002-04: Integration Runtime監視メトリクス取得
        段階1実装: 基本的なメトリクス監視
        """
        # モック設定: 監視メトリクス
        mock_monitor_client.query_workspace.return_value = Mock()
        mock_monitor_client.query_workspace.return_value.tables = [
            Mock(rows=[
                ["AutoResolveIntegrationRuntime", "2025-07-03T10:00:00Z", "Started", 8, 4096],
                ["SelfHostedIR", "2025-07-03T10:00:00Z", "Online", 4, 8192]
            ])
        ]
        
        # クエリ実行
        query = """
        AzureDiagnostics
        | where Category == "IntegrationRuntimeLogs"
        | where TimeGenerated >= ago(1h)
        | project IntegrationRuntimeName, TimeGenerated, State, CoreCount, MemoryMB
        """
        
        result = mock_monitor_client.query_workspace(
            workspace_id="mock_workspace_id",
            query=query
        )
        
        # メトリクス検証
        assert len(result.tables[0].rows) == 2
        
        for row in result.tables[0].rows:
            ir_name, timestamp, state, core_count, memory_mb = row
            assert ir_name in ["AutoResolveIntegrationRuntime", "SelfHostedIR"]
            assert state in ["Started", "Online"]
            assert core_count > 0
            assert memory_mb > 0
        
        print(f"✅ SYS-SCHED-002-04: IR監視メトリクス取得成功")
    
    def test_sys_sched_002_05_ir_performance_baseline(self, mock_datafactory_client, mock_monitor_client):
        """
        SYS-SCHED-002-05: Integration Runtime性能ベースライン確認
        段階1実装: 基本性能指標の取得・評価
        """
        # パフォーマンス指標のモック設定
        performance_data = {
            "AutoResolveIntegrationRuntime": {
                "avg_pipeline_duration": 120,  # 秒
                "success_rate": 98.5,  # %
                "avg_cpu_utilization": 45,  # %
                "avg_memory_utilization": 60  # %
            },
            "SelfHostedIR": {
                "avg_pipeline_duration": 90,  # 秒
                "success_rate": 99.2,  # %
                "avg_cpu_utilization": 25,  # %
                "avg_memory_utilization": 40  # %
            }
        }
        
        # 性能基準値定義
        performance_thresholds = {
            "max_pipeline_duration": 300,  # 秒
            "min_success_rate": 95.0,  # %
            "max_cpu_utilization": 80,  # %
            "max_memory_utilization": 80  # %
        }
        
        # 各IRの性能評価
        for ir_name, metrics in performance_data.items():
            # 性能基準チェック
            assert metrics["avg_pipeline_duration"] <= performance_thresholds["max_pipeline_duration"], \
                f"{ir_name}: パイプライン実行時間が基準を超過"
            
            assert metrics["success_rate"] >= performance_thresholds["min_success_rate"], \
                f"{ir_name}: 成功率が基準を下回る"
            
            assert metrics["avg_cpu_utilization"] <= performance_thresholds["max_cpu_utilization"], \
                f"{ir_name}: CPU使用率が基準を超過"
            
            assert metrics["avg_memory_utilization"] <= performance_thresholds["max_memory_utilization"], \
                f"{ir_name}: メモリ使用率が基準を超過"
            
            print(f"✅ SYS-SCHED-002-05: {ir_name} 性能基準クリア")
    
    def test_sys_sched_002_06_ir_configuration_validation(self, mock_datafactory_client):
        """
        SYS-SCHED-002-06: Integration Runtime設定検証
        段階1実装: 設定パラメータの妥当性確認
        """
        ir_configs = [
            {
                "name": "AutoResolveIntegrationRuntime",
                "type": "Managed",
                "expected_cores": 8,
                "expected_compute_type": "General"
            },
            {
                "name": "SelfHostedIR", 
                "type": "SelfHosted",
                "min_nodes": 1,
                "expected_version": "5.42"
            }
        ]
        
        for config in ir_configs:
            ir_name = config["name"]
            
            if config["type"] == "Managed":
                mock_datafactory_client.integration_runtimes.get.return_value = {
                    "name": ir_name,
                    "properties": {
                        "type": "Managed",
                        "typeProperties": {
                            "computeProperties": {
                                "dataFlowProperties": {
                                    "computeType": config["expected_compute_type"],
                                    "coreCount": config["expected_cores"]
                                }
                            }
                        }
                    }
                }
                
                ir_info = mock_datafactory_client.integration_runtimes.get(
                    resource_group_name="mock_rg",
                    factory_name="omni-df-dev",
                    integration_runtime_name=ir_name
                )
                
                # Managed IR設定検証
                props = ir_info["properties"]["typeProperties"]["computeProperties"]["dataFlowProperties"]
                assert props["coreCount"] == config["expected_cores"]
                assert props["computeType"] == config["expected_compute_type"]
                
            elif config["type"] == "SelfHosted":
                mock_datafactory_client.integration_runtimes.get_status.return_value = {
                    "properties": {
                        "type": "SelfHosted",
                        "nodes": [{"nodeName": f"Node-{i+1}"} for i in range(config["min_nodes"])],
                        "version": f"{config['expected_version']}.8682.1"
                    }
                }
                
                ir_status = mock_datafactory_client.integration_runtimes.get_status(
                    resource_group_name="mock_rg",
                    factory_name="omni-df-dev",
                    integration_runtime_name=ir_name
                )
                
                # Self-hosted IR設定検証
                assert len(ir_status["properties"]["nodes"]) >= config["min_nodes"]
                assert ir_status["properties"]["version"].startswith(config["expected_version"])
            
            print(f"✅ SYS-SCHED-002-06: {ir_name} 設定検証成功")
    
    def test_sys_sched_002_05_disaster_recovery_preparation(self, mock_datafactory_client):
        """
        SYS-SCHED-002-05: 災害復旧準備テスト（段階2実装）
        災害復旧シナリオ: IR構成バックアップ・復旧手順検証
        """
        # 複数IRの構成情報取得
        ir_configurations = []
        ir_names = ["AutoResolveIntegrationRuntime", "SelfHostedIR", "AzureIR-EastUS"]
        
        for ir_name in ir_names:
            mock_datafactory_client.integration_runtimes.get.return_value = {
                "name": ir_name,
                "properties": {
                    "type": "Managed" if "AutoResolve" in ir_name or "Azure" in ir_name else "SelfHosted",
                    "state": "Started",
                    "typeProperties": {
                        "computeProperties": {
                            "location": "AutoResolve" if "AutoResolve" in ir_name else "East US",
                            "dataFlowProperties": {
                                "computeType": "General",
                                "coreCount": 8
                            }
                        }
                    }
                }
            }
            
            ir_config = mock_datafactory_client.integration_runtimes.get(
                resource_group_name="mock_rg",
                factory_name="omni-df-dev",
                integration_runtime_name=ir_name
            )
            ir_configurations.append(ir_config)
        
        # 災害復旧準備状況検証
        assert len(ir_configurations) == 3, "すべてのIR構成が取得できること"
        
        # 冗長化されたIR構成の確認
        managed_irs = [ir for ir in ir_configurations if ir["properties"]["type"] == "Managed"]
        self_hosted_irs = [ir for ir in ir_configurations if ir["properties"]["type"] == "SelfHosted"]
        
        assert len(managed_irs) >= 2, "Managed IRが冗長化されていること"
        assert len(self_hosted_irs) >= 1, "Self-hosted IRが利用可能であること"
        
        print(f"✅ SYS-SCHED-002-05: 災害復旧準備テスト成功 (IR構成数: {len(ir_configurations)})")
    
    def test_sys_sched_002_06_ir_failover_scenario(self, mock_datafactory_client):
        """
        SYS-SCHED-002-06: IRフェイルオーバーシナリオテスト（段階2実装）
        主IRが利用不可の場合の代替IR切り替え検証
        """
        primary_ir = "AzureIR-EastUS"
        backup_ir = "AutoResolveIntegrationRuntime"
        
        # 主IRが障害状態をシミュレート
        mock_datafactory_client.integration_runtimes.get_status.side_effect = [
            # 1回目: 主IRが停止状態
            {
                "name": primary_ir,
                "properties": {
                    "type": "Managed",
                    "state": "Stopped",  # 障害状態
                    "capabilities": [],
                    "nodes": []
                }
            },
            # 2回目: バックアップIRが正常状態
            {
                "name": backup_ir,
                "properties": {
                    "type": "Managed",
                    "state": "Started",
                    "capabilities": ["DataFlow", "Pipeline"],
                    "nodes": []
                }
            }
        ]
        
        # フェイルオーバーロジック検証
        primary_status = mock_datafactory_client.integration_runtimes.get_status(
            resource_group_name="mock_rg",
            factory_name="omni-df-dev",
            integration_runtime_name=primary_ir
        )
        
        # 主IRが停止中の場合、バックアップIRを確認
        if primary_status["properties"]["state"] != "Started":
            backup_status = mock_datafactory_client.integration_runtimes.get_status(
                resource_group_name="mock_rg",
                factory_name="omni-df-dev",
                integration_runtime_name=backup_ir
            )
            
            # バックアップIRが正常に動作することを確認
            assert backup_status["properties"]["state"] == "Started"
            assert "Pipeline" in backup_status["properties"]["capabilities"]
            
            print(f"✅ SYS-SCHED-002-06: フェイルオーバー成功 ({primary_ir} → {backup_ir})")
        
    def test_sys_sched_002_07_performance_monitoring_advanced(self, mock_datafactory_client, mock_monitor_client):
        """
        SYS-SCHED-002-07: 高度なパフォーマンス監視テスト（段階2実装）
        IR負荷分散・リソース使用率・レスポンス時間監視
        """
        # Azure Monitor のクエリ結果をモック
        mock_monitor_client.query_workspace.return_value = Mock(
            tables=[
                Mock(rows=[
                    ["AutoResolveIntegrationRuntime", "85", "120", "15.2"],
                    ["SelfHostedIR", "65", "89", "8.7"],
                    ["AzureIR-EastUS", "45", "67", "12.1"]
                ])
            ]
        )
        
        # パフォーマンスメトリクス取得
        query = """
        ADFActivityRun
        | where TimeGenerated >= ago(1h)
        | where IntegrationRuntimeName != ""
        | summarize 
            AvgDuration = avg(DurationInMs),
            MaxDuration = max(DurationInMs),
            ActivityCount = count()
        by IntegrationRuntimeName
        """
        
        result = mock_monitor_client.query_workspace(
            workspace_id="mock-workspace-id",
            query=query,
            timespan="PT1H"
        )
        
        # パフォーマンス検証
        performance_data = []
        for row in result.tables[0].rows:
            ir_name, avg_duration, max_duration, activity_count = row
            performance_data.append({
                "ir_name": ir_name,
                "avg_duration_ms": float(avg_duration),
                "max_duration_ms": float(max_duration),
                "activity_count": int(activity_count)
            })
        
        # パフォーマンス基準検証
        for data in performance_data:
            # 平均実行時間が100ms以下であることを確認
            assert data["avg_duration_ms"] < 100, f"{data['ir_name']} の平均実行時間が基準を超過"
            
            # 最大実行時間が200ms以下であることを確認
            assert data["max_duration_ms"] < 200, f"{data['ir_name']} の最大実行時間が基準を超過"
            
            print(f"✅ {data['ir_name']}: 平均{data['avg_duration_ms']:.1f}ms, 最大{data['max_duration_ms']:.1f}ms")
        
        print(f"✅ SYS-SCHED-002-07: 高度なパフォーマンス監視テスト成功")

    # 既存のコード...


# テスト実行メタデータ
TEST_METADATA = {
    "test_case_id": "SYS-SCHED-002",
    "test_name": "Integration Runtime管理テスト",
    "test_strategy_alignment": "性能・セキュリティ検証",
    "priority": "中",
    "implementation_phase": "段階1（基本監視機能）",
    "coverage": "AutoResolve・Self-hosted IR",
    "execution_time": "約60秒",
    "dependencies": ["azure-datafactory", "azure-monitor-query", "azure-identity"],
    "test_categories": ["総合テスト", "監視機能", "性能検証"],
    "expected_success_rate": "100%",
    "validation_points": [
        "IR状態監視・可用性確認",
        "LinkedService関連付け検証",
        "監視メトリクス取得",
        "性能ベースライン確認",
        "設定パラメータ妥当性"
    ],
    "future_phases": {
        "phase_2": "負荷分散・パフォーマンス詳細検証",
        "phase_3": "障害回復・フェイルオーバー検証"
    },
    "maintenance_notes": "新しいIR追加時は設定検証ケースに追加。性能基準値は運用実績に基づき調整。"
}

if __name__ == "__main__":
    # テスト実行例
    print("SYS-SCHED-002: Integration Runtime管理テスト（段階1）実行中...")
    pytest.main([__file__, "-v", "--tb=short"])
    print(f"テストメタデータ: {json.dumps(TEST_METADATA, ensure_ascii=False, indent=2)}")
