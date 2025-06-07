# ADF E2Eテスト環境 - 完成版セットアップガイド

## 🎉 完成したE2Eテスト環境

Azure Data Factory (ADF) プロジェクト用の完全なEnd-to-End (E2E) テスト環境が正常に構築されました。

### ✅ 構築済みコンポーネント

#### 1. **Docker環境**
- `Dockerfile.e2e.complete-light` - Python 3.9ベースの最適化されたテスト環境
- `docker-compose.e2e.yml` - 完全なマルチサービス構成
- SQL Server 2022 + Azurite + Integration Runtime Simulator

#### 2. **テスト実行システム**
- `run-e2e-tests.sh` - 完全自動化されたテスト実行スクリプト
- `validate-e2e-env.sh` - 環境バリデーションスクリプト
- `Makefile` - 簡単なコマンド実行用

#### 3. **設定ファイル**
- `requirements.e2e.txt` - 37個の必要パッケージ（pytest, pandas, pyodbc等）
- `.env.e2e.template` - 環境変数テンプレート
- `E2E_TEST_GUIDE.md` - 詳細な使用ガイド

#### 4. **テストスイート**
- 52個のE2Eテストファイル
- 基本接続、Docker統合、パイプライン、データ品質テスト等

## 🚀 実行方法

### 方法1: 自動実行スクリプト（推奨）

```bash
# 完全自動実行
./run-e2e-tests.sh

# ビルドのみ
./run-e2e-tests.sh --build-only

# テストのみ（ビルドスキップ）
./run-e2e-tests.sh --test-only

# クリーンアップ付き実行
./run-e2e-tests.sh --cleanup
```

### 方法2: Makefileコマンド

```bash
# 環境バリデーション
make e2e-validate

# 完全E2Eテスト実行
make test-e2e

# クイックテスト
make test-e2e-quick

# 環境クリーンアップ
make e2e-clean
```

### 方法3: Docker Compose直接実行

```bash
# イメージビルド
docker-compose -f docker-compose.e2e.yml build

# 完全テスト実行
docker-compose -f docker-compose.e2e.yml up --abort-on-container-exit

# 環境停止
docker-compose -f docker-compose.e2e.yml down
```

## 📊 テスト結果

実行後、`test_results/` ディレクトリに以下が生成されます：

```
test_results/
├── e2e_complete.xml         # JUnit XML結果
├── e2e_report.html          # HTMLレポート
├── coverage.xml             # カバレッジXML
├── coverage_html/           # HTMLカバレッジ
├── basic_connections.xml    # 個別テスト結果
├── docker_integration.xml   # Docker統合テスト
└── ...                      # その他テスト結果
```

## 🔧 カスタマイズ

### 環境変数設定

```bash
# テンプレートから環境変数ファイルをコピー
cp .env.e2e.template .env.e2e

# Azure認証情報を設定
vi .env.e2e
```

### テスト対象の変更

`docker-compose.e2e.yml`の`e2e-test-runner`サービスのcommandセクションを編集：

```yaml
command: >
  sh -c "
    python -m pytest /app/tests/e2e/test_specific_module.py -v --tb=short
  "
```

## 🛠️ トラブルシューティング

### よくある問題と解決方法

#### 1. ポート競合エラー
```bash
# 使用中のポートを確認
sudo lsof -i :1433
sudo lsof -i :10000

# プロセス終了
kill -9 <PID>
```

#### 2. メモリ不足
```bash
# Docker設定でメモリを8GB以上に設定
# Docker Desktop > Settings > Resources > Memory
```

#### 3. SQL Server接続エラー
```bash
# ヘルスチェック確認
docker-compose -f docker-compose.e2e.yml exec sqlserver-test \
  /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd123' -Q 'SELECT 1' -C
```

#### 4. ビルドエラー
```bash
# キャッシュクリア
docker system prune -f
docker-compose -f docker-compose.e2e.yml build --no-cache
```

## 📈 パフォーマンス最適化

### 並列実行
```bash
# 並列テスト（4プロセス）
export PYTEST_XDIST_WORKERS=4
python -m pytest tests/e2e/ -n 4
```

### リソース制限
```yaml
# docker-compose.e2e.ymlで設定
deploy:
  resources:
    limits:
      memory: 4G
      cpus: '2.0'
```

## 🎯 バリデーション結果

最新のバリデーション実行結果：

✅ **Docker環境**: Docker 28.1.1 + Compose v2.34.0  
✅ **必要ファイル**: 5個すべて存在  
✅ **ディレクトリ**: tests/e2e, src, docker すべて存在  
✅ **設定ファイル**: Docker Compose設定有効  
✅ **Python依存関係**: 37個のE2Eパッケージ + 3個の基本パッケージ  
✅ **テストファイル**: 52個のE2Eテストファイル  
✅ **環境変数**: テンプレートとファイル両方存在  
✅ **システムリソース**: 16GB RAM + 122Gi 空き容量  
✅ **Docker動作**: 正常稼働中  

## 🏆 次のステップ

1. **即座に実行可能**:
   ```bash
   ./run-e2e-tests.sh
   ```

2. **Azure認証設定**:
   - `.env.e2e`ファイルでAzure認証情報を設定
   - 実際のADFリソースとの統合テスト実行

3. **CI/CDパイプライン統合**:
   - GitHub Actions/Azure DevOpsでの自動実行
   - テスト結果の自動レポート生成

4. **継続的改善**:
   - 新しいテストケースの追加
   - パフォーマンステストの実装
   - セキュリティテストの強化

## 📞 サポート

問題が発生した場合：

1. `./validate-e2e-env.sh` でバリデーション実行
2. `E2E_TEST_GUIDE.md` で詳細ガイド確認
3. `docker-compose -f docker-compose.e2e.yml logs` でログ確認

---

**🎉 おめでとうございます！完全なADF E2Eテスト環境が準備完了しました！**
