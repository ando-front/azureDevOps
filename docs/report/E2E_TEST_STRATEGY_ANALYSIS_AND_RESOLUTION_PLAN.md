# E2Eテスト戦略分析と解決方針レポート

**日付:** 2025年7月1日  
**対象プロジェクト:** Azure Data Factory E2E テストプラットフォーム  
**作成者:** E2E テスト環境改善チーム

## 📋 現状の課題整理

### 主要な技術的課題

1. **SQL Server接続問題（SQLSTATE: 42000）**
   - `pyodbc`を使用したSQL Server（`adf-sql-server-test`）への接続が継続的に失敗
   - `sqlcmd`での接続は成功するが、Python ODBCドライバでの接続が不安定
   - OpenSSLセキュリティレベルと自己署名証明書の競合が根本原因と推定

2. **テスト発見の範囲限定問題**
   - 約400件のテストケースのうち58件しか実行されない
   - `pytest.ini`の`python_files`設定により、`*_fixed.py`等のファイルが除外されていた

3. **データベース初期化の競合状態**
   - `ClientDmBx`テーブル等の必要なテーブルが初期化完了前にテストが開始される

## 🎯 テスト仕様書との照合分析

### E2Eテストの定義と要件

**ドキュメント確認結果:**

1. **`docs/TEST_DESIGN_SPECIFICATION.md`より:**
   - **総テストケース数**: 691ケース
   - **E2Eテスト成功率**: 100% (691/691) ← 目標値
   - **実行環境**: Docker + SQL Server 2022 ← **必須要件**
   - **テスト対象**: 37+パイプラインの完全なデータフロー検証

2. **`docs/E2E_TESTING.md`より:**
   - Docker環境でのSQL Server (`TGMATestDB`) 接続が前提
   - 実際のデータベース環境での「エンドツーエンド業務フロー」検証が主目的
   - `ir-simulator-e2e`, `sqlserver-e2e-test`, `azurite-e2e-test`の3層アーキテクチャ

3. **`docs/adf_pipeline_test_specification.md`より:**
   - SQLMiからSynapse Analytics経由でBlobストレージへのデータ抽出フロー
   - 実際のデータベース接続による統合検証が必須

### **結論: SQL Server接続は E2E テストの MUST 要件**

ドキュメント分析の結果、以下の理由により、SQL Server環境の構築・接続はE2Eテストにおいて**必須要件**であることが確認されました：

- **テスト仕様書で明示的に定義**: 「Docker + SQL Server 2022」環境での691ケーステスト実行が成功基準
- **アーキテクチャ設計の中核**: 3層アーキテクチャのうち`sqlserver-e2e-test`が中心的役割
- **ビジネス要件**: ADF パイプラインの「SQLMi → Synapse → Blob」データフローの完全検証

## 🔧 技術的解決方針

### 方針1: SQL Server接続問題の根本解決（推奨）

**実装方法:**

1. **OpenSSL設定の最適化**
   ```dockerfile
   # Dockerfile の先頭に追加済み
   RUN sed -i 's/DEFAULT@SECLEVEL=2/DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf
   ```

2. **ODBCドライバ設定の強化**
   ```dockerfile
   # より具体的なドライバパス指定
   RUN printf "[ODBC Driver 18 for SQL Server]\\n\\
   Description=Microsoft ODBC Driver 18 for SQL Server\\n\\
   Driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.so.1.1\\n\\
   UsageCount=1\\n\\
   TrustServerCertificate=Yes\\n" >> /etc/odbcinst.ini
   ```

3. **接続文字列の調整**
   ```python
   # TrustServerCertificate=yes; を明示的に追加
   conn_str = (
       "DRIVER={ODBC Driver 18 for SQL Server};"
       f"SERVER={host},{port};"
       f"DATABASE={database};"
       f"UID={user};"
       f"PWD={password};"
       "Encrypt=no;"
       "TrustServerCertificate=yes;"
       "LoginTimeout=30;"
   )
   ```

**期待効果:**
- SQLSTATE: 42000 エラーの根本解決
- 691ケースの完全実行達成
- テスト仕様書との100%整合

### 方針2: 段階的解決アプローチ（代替案）

**Phase 1: 最小構成でのテスト成功**
- 必須テーブル（`ClientDmBx`等）のみの簡素化SQL初期化スクリプト作成
- 基本的な接続とテーブル操作のみでのE2Eテスト実行確認

**Phase 2: 完全構成への拡張**
- 全37+パイプライン対応のデータベーススキーマ復元
- 691ケースの段階的有効化

## 📈 推奨される実行計画

### 短期目標（1-2週間）

1. **OpenSSL設定変更の適用**
   - Dockerイメージの再ビルド（`--no-cache`）
   - 接続テストの実行・確認

2. **pytest.ini修正の効果確認**
   - テスト発見数の400件台への回復確認
   - 実行時間とリソース消費量の測定

3. **DB初期化プロセスの安定化**
   - `init-databases.sh`の実行順序確認
   - `docker wait`による同期処理の確実性向上

### 中期目標（2-4週間）

1. **691ケース完全実行の達成**
   - 全テストケースのパス率測定
   - 失敗ケースの個別分析・修正

2. **CI/CD統合**
   - GitHub Actions等への組み込み
   - 自動テスト実行環境の構築

## 🚫 検討したが採用しない代替案

### 代替案1: SQL Serverのモック化

**検討内容:**
- `unittest.mock`による`pyodbc`接続のモック化
- インメモリDB（SQLite等）への置換

**不採用理由:**
- テスト仕様書で明示的に「Docker + SQL Server 2022」が要求されている
- ADF パイプラインの実際のデータフローを検証できない
- 691ケースのテストコード大幅改修が必要（コスト大）

### 代替案2: テスト範囲の限定

**検討内容:**
- E2Eテストの範囲を「アプリケーション間連携」のみに限定
- データベース検証を別の統合テストに分離

**不採用理由:**
- 「End-to-End」の定義に反する
- ビジネス要件（データフロー完全検証）を満たさない

## 📝 次のアクションアイテム

### 緊急対応（今日中）

1. **Docker イメージの完全再ビルド**
   ```bash
   docker-compose -f docker-compose.e2e.no-proxy.yml build --no-cache
   ```

2. **接続テストの実行**
   ```bash
   # OpenSSL設定変更後の接続確認
   docker exec -it adf-e2e-test-runner python /usr/local/bin/check_db_connection.py
   ```

3. **pytest設定の効果測定**
   ```bash
   # テスト発見数の確認
   docker exec -it adf-e2e-test-runner pytest tests/e2e --collect-only -q | wc -l
   ```

### 継続対応（1週間以内）

1. **全テストケース実行チャレンジ**
2. **失敗ケースの詳細分析**
3. **パフォーマンス最適化**

## 📊 成功基準

- **接続成功率**: 100% （SQLSTATE: 42000 ゼロ）
- **テスト発見数**: 400+ ケース
- **テスト成功率**: 95%+ （仕様書目標: 100%）
- **実行時間**: 15分以内

---

**このレポートは、E2Eテスト環境の膠着状態打開のために作成されました。SQL Server接続の根本解決により、テスト仕様書で定義された完全なE2Eテスト環境の実現を目指します。**

## 🔄 最新の技術的進捗 (2025年7月1日 15:30更新)

### 根本原因分析の完了

**特定された問題:**
1. **複雑なカスタムentrypoint**: SQL Serverコンテナで複雑な初期化ロジックが実行順序を不安定にしていた
2. **競合状態**: SQL Server起動とデータベース初期化が同一プロセス内で実行され、確実性が欠如
3. **依存関係の曖昧性**: 手動の待機ループによる不確実な初期化完了検知

### 実装された根本解決策

**新アーキテクチャ:**
1. **標準SQL Serverコンテナ**: カスタムentrypointを削除し、MS公式イメージをそのまま使用
2. **分離された初期化コンテナ**: `sql-server-init`専用コンテナでデータベース/テーブル作成
3. **SQLスクリプト化**: 初期化ロジックを`01-create-databases.sql`ファイルに分離
4. **Docker Compose依存関係**: `service_completed_successfully`による確実な実行順序制御

**技術仕様:**
```yaml
sql-server-init:
  depends_on:
    sql-server:
      condition: service_healthy
e2e-test-runner:
  depends_on:
    sql-server-init:
      condition: service_completed_successfully
```

### 期待される効果

- **接続エラー（SQLSTATE: 42000）の根絶**: データベースとテーブルが確実に作成完了後にテスト開始
- **再現性の向上**: 待機時間に依存しない確定的な初期化プロセス
- **デバッグ効率化**: 各段階が独立したコンテナで実行され、問題特定が容易

### 実行中のテスト

現在、新アーキテクチャによるE2Eテスト実行中。SQL Server接続問題の根本解決を検証中。
