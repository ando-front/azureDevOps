# 非推奨・旧バージョンスクリプト

このフォルダには、開発過程で作成された旧バージョンのスクリプトが格納されています。
現在は非推奨ですが、参考資料として保管されています。

## ファイル一覧

### 旧SQL外部化スクリプト
- **`enhanced-sql-externalization.ps1`** - 拡張版（パフォーマンス問題あり）
- **`simple-sql-externalization.ps1`** - 簡易版（機能制限あり）
- **`sql-externalization.ps1`** - 基本版（初期バージョン）
- **`test-sql-externalization.ps1`** - テスト版（実験的実装）

## 非推奨の理由

### `enhanced-sql-externalization.ps1`
- **問題**: 長時間クエリでタイムアウト発生
- **制限**: メモリ使用量が過大
- **代替**: `scripts\sql-externalization\optimized-sql-externalization.ps1`

### `simple-sql-externalization.ps1`
- **問題**: 複雑なSQLクエリに対応不可
- **制限**: エラーハンドリング不十分
- **代替**: `scripts\sql-externalization\run-optimized-sql-externalization.ps1`

### `sql-externalization.ps1`
- **問題**: バッチ処理非対応
- **制限**: パフォーマンス監視なし
- **代替**: 上記最適化版スクリプト

### `test-sql-externalization.ps1`
- **問題**: 実験的実装で不安定
- **制限**: プロダクション使用不適
- **代替**: `scripts\diagnostics\troubleshoot-sql-externalization.ps1`

## 使用上の注意

⚠️ **重要**: これらのスクリプトは非推奨です。新しいプロジェクトでは使用せず、最適化版スクリプトを使用してください。

### 学習・参考目的での使用
これらのスクリプトは以下の目的で保管されています：
- 開発過程の記録
- アルゴリズム改善の参考
- トラブルシューティング時の比較対象
- 新人エンジニアの学習資料

### 緊急時の使用
最適化版スクリプトで問題が発生した場合の緊急対応として、一時的に使用可能ですが：
- 十分なテストを実施してください
- 小規模データで動作確認してください
- 可能な限り早急に最適化版に移行してください

## 移行ガイド

### enhanced → optimized への移行
```powershell
# 旧版（非推奨）
.\scripts\deprecated\enhanced-sql-externalization.ps1 -ArmTemplateDirectory "src\dev\arm_template"

# 新版（推奨）
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation both
```

### 主な改善点
1. **タイムアウト制御**: 30秒単位での処理制限
2. **バッチ処理**: メモリ効率的な分割処理
3. **エラーハンドリング**: 包括的な例外処理
4. **監視機能**: リアルタイム進捗表示
5. **レポート機能**: 詳細な実行結果出力

## 削除予定

これらのファイルは将来的に削除される予定です：
- **2025年9月**: 最初の削除検討
- **2025年12月**: 本格的な削除実施

削除前に必要なコードがある場合は、最適化版スクリプトに統合してください。
