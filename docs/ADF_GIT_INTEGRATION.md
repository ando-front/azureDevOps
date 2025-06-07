# Azure Data Factory Git統合ガイド

Azure Data Factory ポータルでGit統合を設定する際の推奨設定と手順です。

## 推奨設定

### リポジトリ設定

- **Repository Type**: Git
- **Repository URL**: `https://github.com/your-organization/tg-ma-MA-ADF-TEST.git`
- **Repository Root**: `/` (ルートディレクトリ)
- **Root Folder**: `/src/dev`

### src/dev/pipeline/ を推奨する理由

#### 1. 完全なパイプラインセット

- **27個の本番用パイプライン**JSONファイルが含まれている
- **ADF標準形式**: ARM テンプレート形式ではなく、ADF ネイティブのJSONフォーマット
- **ポータル互換性**: Azure Data Factory ポータルで直接編集・デプロイ可能

#### 2. arm_template_split/src/pipeline/ を選択しない理由

- **不完全なセット**: 10個のパイプラインのみ（全体の37%）
- **ARM テンプレート形式**: ADF ポータル編集には不適切
- **デプロイ専用**: CI/CDパイプライン用の形式

## ディレクトリ構造

```text
src/dev/                    # ADF Portal Git統合のルートフォルダ
├── pipeline/              # 27個の本番パイプラインJSON (推奨パス)
├── dataset/               # データセット定義
├── linkedService/         # リンクサービス定義
├── trigger/               # トリガー定義
├── dataflow/              # データフロー定義
└── integrationRuntime/    # 統合ランタイム定義
```

## 本番用パイプライン一覧

src/dev/pipeline/ 内の本番用パイプライン（27個）：

1. DoUntilPipeline.json
2. pi_Copy_marketing_client_dm.json
3. pi_Copy_marketing_client_dna.json
4. pi_CustmNoRegistComp.json
5. pi_Insert_ActionPointEntryEvent.json
6. pi_Insert_ActionPointTransactionHistory.json
7. pi_Insert_ClientDmBx.json
8. pi_Insert_mTGCustomerMaster.json
9. pi_Ins_usageservice_mtgid.json
10. pi_PointGrantEmail.json
11. pi_PointLostEmail.json
12. pi_Send_ActionPointCurrentMonthEntryList.json
13. pi_Send_ActionPointRecentTransactionHistoryList.json
14. pi_Send_ClientDM.json
15. pi_Send_Cpkiyk.json
16. pi_Send_ElectricityContractThanks.json
17. pi_Send_karte_contract_score_info.json
18. pi_Send_LIMSettlementBreakdownRepair.json
19. pi_Send_LINEIDLinkInfo.json
20. pi_Send_MovingPromotionList.json
21. pi_Send_mTGMailPermission.json
22. pi_Send_OpeningPaymentGuide.json
23. pi_Send_PaymentAlert.json
24. pi_Send_PaymentMethodChanged.json
25. pi_Send_PaymentMethodMaster.json
26. pi_Send_UsageServices.json
27. pi_UtilityBills.json

## 設定手順

1. Azure Data Factory ポータルにアクセス
2. 「管理」→「Git構成」を選択
3. リポジトリタイプ: Git
4. 上記の推奨設定を入力
5. 「保存」をクリック

## 注意事項

### ブランチ戦略

- **master** ブランチ: 本番環境
- **develop** ブランチ: 開発環境

### 運用ルール

- **パブリッシュ**: ADF ポータルでの変更後は必ずパブリッシュしてGitリポジトリに反映
- **Pull Request**: 本番環境への変更は必ずPull Requestを通して実施
- **バックアップ**: 重要な変更前にはブランチを作成してバックアップ

### トラブルシューティング

#### 同期エラーが発生した場合

1. ローカルリポジトリの最新化
2. 競合ファイルの手動マージ
3. ADF ポータルでの再同期

#### パブリッシュ権限エラー

1. Azure Active Directory でのロール確認
2. Data Factory Contributor 権限の付与
3. Git設定の再確認
