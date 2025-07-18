# E2Eテスト戦略の振り返り

## 概要

本ドキュメントは、現在のE2Eテスト環境におけるSQL Server接続およびテスト実行の課題を踏まえ、テスト戦略の妥当性を再検討した記録です。特に、SQL Serverへの接続がE2Eテストの必須要件であるか、および代替手段の可能性について議論しました。

## 検討の背景にある課題

- `results.xml` および `pytest_results.txt` から、`ClientDmBx` テーブルが見つからないエラーや、`TGMATestDB` へのログイン失敗エラーが確認されています。
- これらのエラーは、SQL Serverへの接続自体は確立されているものの、テスト実行時に必要なデータベースやテーブルが完全に準備できていないことに起因している可能性が高いです。
- `pytest-html` プラグインのオプションが認識されないという、テストレポート生成に関する技術的な問題も発生しています。

## 検討事項

### 1. E2EテストでSQL Serverを構築してテストを実施することがMUSTであるか？

**妥当性の検討:**

- **テスト仕様書との整合性:** プロジェクトの `docs/adf_pipeline_test_specification.md` や `docs/TEST_DESIGN_SPECIFICATION.md` などのテスト仕様書を確認する必要があります。もしこれらのドキュメントが、データパイプラインのEnd-to-Endの動作（データソースからターゲットデータベースへのデータフロー、変換、ロード）を検証することをE2Eテストの主要な目的としている場合、実際のSQL Server（またはそれに準ずる環境）との統合は必須であると考えられます。
- **既存テストコードの依存性:** 現在のE2Eテストコード (`tests/e2e/` 配下) は `pyodbc` を使用してSQL Serverに直接接続し、特定のテーブル (`ClientDmBx` など) に対してクエリを実行しています。これは、テストが実際のデータベース環境に強く依存して設計されていることを示しています。これらのテストをモックやインメモリDBに置き換えることは、テストコードの大幅な改修を伴い、E2Eテストとしての「実際のシステムに近い環境での検証」という価値を損なう可能性があります。

**現時点での結論（仮説）:**

既存のテストコードの設計とエラーの内容から判断すると、E2EテストにおいてSQL Serverへの接続と特定のテーブルの利用は必須である可能性が高いです。データパイプラインのEnd-to-Endの動作検証には、実際のデータベース環境が不可欠であると考えられます。

### 2. 代替手段の検討（テスト環境の見直し）

SQL Serverの構築・接続が継続的に問題となる場合、以下の代替手段が検討可能です。

#### 2.1. テストデータの固定化（データベース初期化プロセスの簡素化）

**具体的にどのような変更になるか：**

- **現状:** `docker-compose.e2e.yml` の `sqlserver-test` サービスは、複数のSQLスクリプト（`00_create_synapse_db_fixed.sql`, `01_init_database_fixed.sql`, `02_create_test_tables.sql`, `02_create_staging_tables.sql`, `07_additional_schema_objects.sql`, `03_insert_test_data.sql`, `08_create_raw_data_source_table.sql`）を実行し、複数のデータベース（`SynapseTestDB`, `TGMATestDB`）と様々なテーブルをサンプルデータと共に作成しています。
- **提案する変更:**
    - E2Eテストで**必須となる最小限のスキーマとデータ**のみを作成する、新しい簡素化されたSQL初期化スクリプト（例: `docker/sql/init/00_minimal_e2e_init.sql`）を作成します。このスクリプトは、`TGMATestDB` と `ClientDmBx` テーブル、およびテストに必要な最小限のデータのみを含みます。
    - `docker-compose.e2e.yml` を修正し、`sqlserver-test` サービスがこの新しい最小限のスクリプトのみを実行するように変更します。

**カバレッジの妥当性:**

- **利点:** セットアップ時間の短縮、リソース消費の削減、デバッグの容易化が期待できます。
- **欠点:** 大規模なデータセットや複雑なスキーマに依存するエッジケースやパフォーマンスの問題を見逃す可能性があります。E2Eテストの主要な目的が、実際のデータ量や構造におけるデータパイプラインの機能性やパフォーマンス検証である場合、カバレッジが低下する可能性があります。
- **妥当性:** E2Eテストの主な目的がパイプラインの**ロジックの機能的正確性**の検証であり、パフォーマンスやスケーラビリティの検証が別のテストフェーズ（例: 性能テスト）でカバーされるのであれば、このアプローチは妥当です。

#### 2.2. 統合テストの範囲限定（E2Eテストの範囲を狭める）

**具体的にどのような変更になるか：**

- **現状:** E2Eテストは、SQL Server、Azurite、IR Simulatorを含む完全なDocker Compose環境に対して実行されています。`ClientDmBx` エラーが発生した `test_advanced_database_operations.py` のようなテストは、E2Eスイートの一部として実際のデータベースと対話しています。
- **提案する変更:**
    - **E2Eテストのスコープ再定義:** E2Eテストは、アプリケーションコンポーネント間の連携（例: PythonスクリプトとIR Simulator間の連携）に焦点を当て、データベースとの直接的なやり取りは**モック化またはスタブ化**します。これにより、E2Eテストの実行速度を向上させ、環境の複雑さを軽減します。
    - **`tests/e2e/test_advanced_database_operations.py` の修正:** データベースアクセス層を抽象化し、`unittest.mock` などのモックフレームワークを使用して、`ClientDmBx` へのクエリに対するデータベースの応答をシミュレートするようにテストコードをリファクタリングします。
    - **新しい統合テストスイートの作成:** データベースとの実際の統合を検証するために、新しいテストファイル（例: `tests/integration/test_database_integration.py`）を作成します。この統合テストは、実際のSQL Serverインスタンス（または専用の統合テスト用DBインスタンス）に接続し、`ClientDmBx` テーブルの作成、データ挿入、クエリの正確性などを検証します。このテストは、E2EテストのDocker Compose環境とは別に実行される可能性があります。

**カバレッジの妥当性:**

- **利点:** E2Eテストの実行が高速化され、テスト失敗時の原因特定が容易になります。E2E環境の複雑性も軽減されます。
- **欠点:** 「End-to-End」テストの定義が変更され、システム全体（アプリケーションコンポーネントから実際のデータベースまで）の完全なEnd-to-Endの動作検証が、E2Eテストと統合テストの2つのフェーズに分割されます。これにより、テストの管理が複雑になる可能性や、テストのギャップが生じないように注意深い設計が必要になります。
- **妥当性:** 大規模なシステムや複雑なデータパイプラインにおいて、テストの階層化は一般的なプラクティスです。E2Eテストの目的を「主要なビジネスフローの検証」に限定し、データベースとの詳細な連携は専用の統合テストでカバーするという戦略は妥当です。ただし、テストの網羅性を確保するためには、統合テストの設計と実行が非常に重要になります。

## 今後の方向性

現在の `pytest-html` のオプション認識問題は、テスト戦略の議論とは別の技術的な問題です。まずはこの技術的な問題を解決し、テストレポートが正常に生成される状態を目指します。その上で、上記のテスト戦略の代替案について、プロジェクトの具体的な要件やリソース、長期的なメンテナンス性を考慮して、より詳細な議論を行う必要があります。

**直近の行動:**

`pytest --trace-config` の出力を取得し、`pytest-html` のロードに関する問題を特定します。
