# パイプラインファイルパス問題解決レポート

## 問題の概要

- pytestテスト実行時に、パイプラインJSONファイル（`pi_Copy_marketing_client_dm.json`, `pi_Insert_ClientDmBx.json`, `pi_Send_ActionPointCurrentMonthEntryList.json`）が見つからない
- エラー: `FileNotFoundError: Pipeline file not found in any of these paths`
- コンテナ内でのパス解決が正しく機能していない

## 根本原因の分析

1. **マウント構造の複雑性**: Dockerfileで`COPY . .`により`/app`にファイルがコピーされ、同時にワークフローで`/tests`にマウントされる
2. **パス探索の不十分性**: テストコードのパス候補が限定的で、実際のファイル配置と一致しない
3. **デバッグ情報の不足**: パス探索の詳細な失敗理由が分からない

## 実装した解決策

### 1. 詳細なパス探索ロジックの強化

- **ファイル**: `tests/conftest.py`, `tests/unit/test_pi_Send_ActionPointCurrentMonthEntryList.py`
- **改善点**:
  - より包括的なパス候補リストの追加
  - 各パスの存在確認時の詳細ログ出力
  - ファイルが見つからない場合のディレクトリ構造探索

```python
base_paths = [
    # Docker環境での優先パス
    "/tests/src/dev/pipeline/file.json",
    "/app/src/dev/pipeline/file.json",
    # 相対パス候補
    "src/dev/pipeline/file.json",
    "../../src/dev/pipeline/file.json",
    "../../../src/dev/pipeline/file.json",
    # ワークスペースルートからの相対パス
    os.path.join(os.getcwd(), "src", "dev", "pipeline", "file.json"),
    # 追加の絶対パス候補
    os.path.join("/tests", "src", "dev", "pipeline", "file.json"),
    os.path.join("/app", "src", "dev", "pipeline", "file.json"),
]
```

### 2. パイプラインパス初期化スクリプトの作成

- **ファイル**: `docker/init-pipeline-paths.sh`
- **機能**:
  - パイプラインファイルの元の場所を自動検出
  - テストで期待される複数のパスにシンボリックリンクを作成
  - 設定後の検証とファイル数カウント

### 3. ワークフローの改善

- **ファイル**: `.github/workflows/adf-deploy.yml`
- **追加項目**:
  - `/app/src`ディレクトリの明示的マウント
  - パイプラインファイルアクセシビリティの事前検証
  - 初期化スクリプトの実行
  - 設定後の最終検証

```yaml
- name: Ensure pipeline files are accessible
  run: |
    echo "=== Running pipeline path initialization ==="
    docker exec pytest-test bash /app/docker/init-pipeline-paths.sh
    echo "=== Final verification ==="
    docker exec pytest-test ls -la /app/src/dev/pipeline/ | head -5
    docker exec pytest-test ls -la /tests/src/dev/pipeline/ | head -5
```

### 4. Dockerfileの拡張

- **ファイル**: `Dockerfile`
- **追加**: 初期化スクリプトに実行権限を付与

## 期待される結果

1. **パス解決の成功**: 複数のパス候補により、確実にパイプラインファイルが見つかる
2. **詳細なデバッグ**: 問題発生時の詳細なログ出力
3. **自動復旧**: シンボリックリンクによる自動的なパス問題の解決
4. **テスト成功率の向上**: FileNotFoundErrorの解消

## 検証方法

1. GitHub Actionsワークフロー実行時のログ確認
2. 初期化スクリプトの実行ログ確認
3. pytest実行時のパス探索ログ確認
4. テスト実行結果（エラー→成功）の確認

## 追加の改善案

1. **環境変数による設定**: `PIPELINE_PATH`環境変数でパスを明示的に指定
2. **テストモックの強化**: パイプラインファイルが見つからない場合のモックテスト
3. **パス正規化**: `os.path.normpath()`を使用したパス正規化

---
**作成日**: 2025年6月17日  
**ステータス**: 実装完了、検証待ち
