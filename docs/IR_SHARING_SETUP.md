# Integration Runtime共有セットアップガイド

## 🎯 目的

既存のData Factoryで稼働中のSelf-hosted Integration Runtimeを、新しいADFでも共有して利用する。

## 📋 前提条件

- **既存ADF**: `omni-df-dev` で `OmniLinkedSelfHostedIntegrationRuntime` が稼働中
- **既存リソースグループ**: `0001-tgomni-dev-rg`
- **サブスクリプション**: `25370ec0-e2ac-4a3c-b242-3772024988b2`
- **新規ADF**: （現在のテスト用ADF名を確認する必要があります）

## 🛠️ 手順

### Step 1: 既存ADFでIR共有を有効化

```bash
# 既存ADFの情報を確認
az datafactory integration-runtime show \
  --name OmniLinkedSelfHostedIntegrationRuntime \
  --resource-group "0001-tgomni-dev-rg" \
  --factory-name "omni-df-dev" \
  --subscription "25370ec0-e2ac-4a3c-b242-3772024988b2"

# IR共有を有効化
az datafactory integration-runtime update \
  --name OmniLinkedSelfHostedIntegrationRuntime \
  --resource-group "0001-tgomni-dev-rg" \
  --factory-name "omni-df-dev" \
  --subscription "25370ec0-e2ac-4a3c-b242-3772024988b2" \
  --sharing-enabled true
```

### Step 2: 新規ADFでリンクIRを作成

**注意**: 新規ADF名と新規リソースグループ名を特定してから実行してください。

```bash
# 新規ADFにリンクIRを作成
az datafactory integration-runtime linked-integration-runtime create \
  --resource-group "{新規リソースグループ名}" \
  --factory-name "{新規ADF名}" \
  --name OmniLinkedSelfHostedIntegrationRuntime \
  --integration-runtime-name OmniLinkedSelfHostedIntegrationRuntime \
  --source-data-factory "omni-df-dev" \
  --source-resource-group "0001-tgomni-dev-rg" \
  --subscription "25370ec0-e2ac-4a3c-b242-3772024988b2"
```

### Step 3: JSON設定の調整

現在のGit構成ファイルはそのまま利用可能。IRの参照名は同じなので変更不要。

## ✅ 確認事項

1. 既存ADFでIRが正常に稼働している
2. 既存ADFでIR共有が有効化されている  
3. 新規ADFでリンクIRが正常に作成されている
4. 新規ADFのパイプラインでIRが正常に認識される

## 🔄 Alternative: Managed IRを利用

Self-hosted IRの共有が困難な場合は、Managed IR (`omni-sharing01-d-jpe`) のみを使用する選択肢もあります。
