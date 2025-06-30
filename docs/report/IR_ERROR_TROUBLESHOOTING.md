# Integration Runtime 権限エラー対処チェックリスト

## ✅ 実行済み確認事項

- [ ] 既存ADF (omni-df-dev) で「Allow sharing with other data factories」を有効化
- [ ] 「Grant permission to specific data factories」で omni-df-cicd-test を明示的に追加
- [ ] 設定を保存後、5-10分待機
- [ ] 現在のユーザーに Data Factory Contributor 権限があることを確認

## 🔄 再試行手順

### Option A: 同じ名前でリンクIR作成

1. omni-df-cicd-test → 管理 → Integration runtimes
2. 「+ 新規」 → Self-Hosted → Linked
3. 名前: `OmniLinkedSelfHostedIntegrationRuntime` (既存と同じ)
4. ソース: omni-df-dev の OmniLinkedSelfHostedIntegrationRuntime

### Option B: 一時的にManaged IRのみ使用

1. すべてのLinked Serviceで connectVia を omni-sharing01-d-jpe に変更
2. Self-hosted IR依存のサービスは一時的に無効化

## 🚨 緊急回避策

もし権限問題が解決しない場合：

- Managed IR (omni-sharing01-d-jpe) のみを使用
- SFTP等の特殊な接続は後回しにして、まずAzure内のサービス接続を動作確認

## 📞 次のステップ

権限設定完了後に以下をお知らせください：

1. 既存ADFでの権限付与が完了したか
2. 再試行の結果
3. エラーメッセージの変化
