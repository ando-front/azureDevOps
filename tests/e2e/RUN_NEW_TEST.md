# 新規E2Eテスト実行手順書

## テストファイル
`test_e2e_pipeline_point_grant_email_new.py`

## 実行方法

### 方法1: Dockerコンテナでの実行（推奨）

```bash
# プロジェクトディレクトリに移動
cd /mnt/c/Users/angie/git/azureDevOps

# Dockerコンテナでテスト実行
docker-compose -f docker-compose.e2e.yml run --rm e2e-test-runner python -m pytest tests/e2e/test_e2e_pipeline_point_grant_email_new.py -v
```

### 方法2: ローカル環境での実行

```bash
# プロジェクトディレクトリに移動
cd /mnt/c/Users/angie/git/azureDevOps

# 必要なパッケージをインストール
pip install -r requirements.txt

# テスト実行
python -m pytest tests/e2e/test_e2e_pipeline_point_grant_email_new.py -v
```

### 方法3: 個別テストケースの実行

```bash
# ファイル存在時のテスト
python -m pytest tests/e2e/test_e2e_pipeline_point_grant_email_new.py::TestPointGrantEmailPipeline::test_e2e_point_grant_email_with_file_exists -v

# ファイル非存在時のテスト
python -m pytest tests/e2e/test_e2e_pipeline_point_grant_email_new.py::TestPointGrantEmailPipeline::test_e2e_point_grant_email_without_file -v

# データ品質テスト
python -m pytest tests/e2e/test_e2e_pipeline_point_grant_email_new.py::TestPointGrantEmailPipeline::test_e2e_point_grant_email_data_quality -v

# パフォーマンステスト
python -m pytest tests/e2e/test_e2e_pipeline_point_grant_email_new.py::TestPointGrantEmailPipeline::test_e2e_point_grant_email_performance -v
```

## テストの特徴

1. **DB接続不要**: モックオブジェクトを使用してDB接続エラーを回避
2. **高速実行**: 実際のDBやネットワーク接続なしで高速実行可能
3. **包括的カバレッジ**: ETLの全フェーズ（Extract, Transform, Load）をカバー
4. **品質メトリクス**: データ品質の定量的評価

## 期待される結果

すべてのテストケースが成功し、以下の出力が表示されます：

```
test_e2e_point_grant_email_with_file_exists PASSED
test_e2e_point_grant_email_without_file PASSED
test_e2e_point_grant_email_data_quality PASSED
test_e2e_point_grant_email_performance PASSED

====== 4 passed in X.XX seconds ======
```

## トラブルシューティング

### ImportError が発生する場合
プロジェクトのルートディレクトリから実行してください。

### Docker実行でエラーが発生する場合
```bash
# Dockerサービスが起動しているか確認
docker ps

# コンテナをビルド
docker-compose -f docker-compose.e2e.yml build
```

### テストが失敗する場合
- ログ出力を確認してエラーの詳細を把握
- `--tb=long` オプションで詳細なトレースバックを表示