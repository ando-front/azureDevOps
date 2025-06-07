# Azure Data Factory ポータル Git統合設定ガイド

## 推奨設定

Azure Data Factory ポータルでGit統合を設定する際は、以下の設定を使用してください：

### リポジトリ設定
- **Repository Type**: Git
- **Repository URL**: `https://github.com/your-organization/tg-ma-MA-ADF-TEST.git`
- **Repository Root**: `/` (ルートディレクトリ)
- **Root Folder**: `/src/dev`

### 理由
1. **`src/dev/pipeline/` を選択する理由**:
   - **完全なパイプラインセット**: 27個の本番用パイプラインJSONファイルが含まれている
   - **ADF標準形式**: ARM テンプレート形式ではなく、ADF ネイティブのJSONフォーマット
   - **ポータル互換性**: Azure Data Factory ポータルで直接編集・デプロイ可能

2. **`arm_template_split/src/pipeline/` を選択しない理由**:
   - **不完全なセット**: 10個のパイプラインのみ（全体の37%）
   - **ARM テンプレート形式**: ADF ポータル編集には不適切
   - **デプロイ専用**: CI/CDパイプライン用の形式

## ディレクトリ構造
```
src/dev/
├── pipeline/           # 27個のパイプラインJSON (推奨パス)
├── dataset/            # データセット定義
├── linkedService/      # リンクサービス定義
├── trigger/            # トリガー定義
├── dataflow/           # データフロー定義
└── integrationRuntime/ # 統合ランタイム定義
```

## クリーンアップ済みファイル

以下のテスト・重複ファイルはプロジェクトから削除されました：

### src/dev/pipeline/ から削除
- `DoUntilPipeline_copy1.json` (重複)
- `pi_alert_test2.json` (テスト用)
- `pi_Copy_marketing_client_dm_bk.json` (バックアップ)
- `pi_Copy_marketing_client_dm_test.json` (テスト用)
- `pi_Copy_marketing_client_dm_test3.json` (テスト用)
- `pi_Copy_marketing_client_dna_test.json` (テスト用)
- `pi_Copy_marketing_client_dna_test3.json` (テスト用)
- `pi_Ins_marketing_client_dna_bk.json` (バックアップ)
- `pi_Send_karte_contract_score_info_test.json` (テスト用)
- `pi_Send_karte_contract_score_info_test2.json` (テスト用)
- `test2.json` (テスト用)

### arm_template_split/src/pipeline/ から削除
- `pi_Copy_marketing_client_dm_bk.json` (バックアップ)
- `pi_Copy_marketing_client_dm_test.json` (テスト用)
- `pi_Copy_marketing_client_dm_test3.json` (テスト用)
- `pi_Copy_marketing_client_dna_test.json` (テスト用)
- `pi_Copy_marketing_client_dna_test3.json` (テスト用)
- `pi_Ins_marketing_client_dna_bk.json` (バックアップ)
- `pi_Send_karte_contract_score_info_test2.json` (テスト用)

## 本番用パイプライン一覧 (27個)

src/dev/pipeline/ 内の本番用パイプライン：

1. `DoUntilPipeline.json`
2. `pi_Copy_marketing_client_dm.json`
3. `pi_Copy_marketing_client_dna.json`
4. `pi_CustmNoRegistComp.json`
5. `pi_Insert_ActionPointEntryEvent.json`
6. `pi_Insert_ActionPointTransactionHistory.json`
7. `pi_Insert_ClientDmBx.json`
8. `pi_Insert_mTGCustomerMaster.json`
9. `pi_Ins_usageservice_mtgid.json`
10. `pi_PointGrantEmail.json`
11. `pi_PointLostEmail.json`
12. `pi_Send_ActionPointCurrentMonthEntryList.json`
13. `pi_Send_ActionPointRecentTransactionHistoryList.json`
14. `pi_Send_ClientDM.json`
15. `pi_Send_Cpkiyk.json`
16. `pi_Send_ElectricityContractThanks.json`
17. `pi_Send_karte_contract_score_info.json`
18. `pi_Send_LIMSettlementBreakdownRepair.json`
19. `pi_Send_LINEIDLinkInfo.json`
20. `pi_Send_MovingPromotionList.json`
21. `pi_Send_mTGMailPermission.json`
22. `pi_Send_OpeningPaymentGuide.json`
23. `pi_Send_PaymentAlert.json`
24. `pi_Send_PaymentMethodChanged.json`
25. `pi_Send_PaymentMethodMaster.json`
26. `pi_Send_UsageServices.json`
27. `pi_UtilityBills.json`

## 注意事項

1. **ブランチ戦略**: `master` ブランチを本番環境、`develop` ブランチを開発環境として使用
2. **パブリッシュ**: ADF ポータルでの変更後は必ずパブリッシュしてGitリポジトリに反映
3. **Pull Request**: 本番環境への変更は必ずPull Requestを通して実施
4. **バックアップ**: 重要な変更前にはブランチを作成してバックアップ

## 設定手順

1. Azure Data Factory ポータルにアクセス
2. 「管理」→「Git構成」を選択
3. リポジトリタイプ: Git
4. 上記の推奨設定を入力
5. 「保存」をクリック

---
更新日: 2025年6月3日
クリーンアップ完了: ✅
