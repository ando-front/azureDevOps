# Azure Portal での Integration Runtime 共有設定手順

## 🎯 設定情報

- 既存ADF: `omni-df-dev`
- 新規ADF: `omni-df-cicd-test`
- リソースグループ: `0001-tgomni-dev-rg`
- IR名: `OmniLinkedSelfHostedIntegrationRuntime`

## 📋 手動設定手順

### Step 1: 既存ADFでIR共有を有効化

1. **Azure Portal** → **Data Factories** → **`omni-df-dev`**
2. **「管理」** → **「Integration runtimes」**
3. **`OmniLinkedSelfHostedIntegrationRuntime`** をクリック
4. **「共有」** タブをクリック
5. **「Allow sharing with other data factories」** を **有効** に設定
6. **「保存」** をクリック

### Step 2: 新規ADFでリンクIRを作成

1. **Azure Portal** → **Data Factories** → **`omni-df-cicd-test`**
2. **「管理」** → **「Integration runtimes」**
3. **「+ 新規」** をクリック
4. **「Azure, Self-Hosted」** → **「Continue」**
5. **「Self-Hosted」** → **「Continue」**
6. **「Linked」** → **「Continue」**
7. 以下を設定:
   - **名前**: `OmniLinkedSelfHostedIntegrationRuntime`
   - **ソースData Factory**: `omni-df-dev`
   - **ソースIntegration Runtime**: `OmniLinkedSelfHostedIntegrationRuntime`
8. **「作成」** をクリック

### Step 3: 設定確認

1. **`omni-df-cicd-test`** の **「Integration runtimes」** で確認
2. **`OmniLinkedSelfHostedIntegrationRuntime`** の状態が **「実行中」** になることを確認
3. **Linked Services** でIR参照エラーが解消されることを確認

## ✅ 期待される結果

- Integration Runtime一覧で「見つかりません」エラーが解消
- 全てのLinked ServiceでIR参照が正常に動作
- パイプラインの検証エラーが大幅に減少

## 🔧 トラブルシューティング

もし問題が発生した場合:

1. 両方のADFが同じリソースグループにあることを確認
2. IR名が完全に一致していることを確認
3. 既存ADFでIRが実際に稼働していることを確認

## 🚨 **アクセス権限エラーの解決手順**

### エラー: "Access denied. Unable to access shared integration runtime"

このエラーが発生した場合の詳細な解決手順：

#### **Step 1: 既存ADF（omni-df-dev）での詳細設定**

1. **Azure Portal** → **Data Factories** → **`omni-df-dev`**
2. **「管理」** → **「Integration runtimes」**
3. **`OmniLinkedSelfHostedIntegrationRuntime`** をクリック
4. **「共有」** タブで以下を確認:
   - ✅ **「Allow sharing with other data factories」** が **有効**
   - ✅ **「Grant permission to specific data factories」** セクションを確認
   - ✅ 許可するData Factory一覧に **`omni-df-cicd-test`** を **明示的に追加**

#### **Step 2: 明示的な権限付与**

「共有」タブで：

1. **「+ Add data factory」** をクリック
2. **Data Factory Name**: `omni-df-cicd-test` を入力
3. **Resource Group**: `0001-tgomni-dev-rg` を選択
4. **「Add」** をクリック
5. **「保存」** をクリック

#### **Step 3: RBAC権限の確認**

Azure Portal で以下を確認:

1. **`omni-df-cicd-test`** → **「アクセス制御 (IAM)」**
2. 現在のユーザーが以下の権限を持っていることを確認:
   - **Data Factory Contributor** または
   - **Owner** または
   - **Integration Runtime Operator**

#### **Step 4: 再試行**

権限設定後、5-10分待ってから：

1. **`omni-df-cicd-test`** でリンクIR作成を再試行
2. 名前を **`OmniLinkedSelfHostedIntegrationRuntime`** (元の名前と同じ) に設定
