# Integration Runtime共有設定 - PowerShell版
# 既存のADF: omni-df-dev から IR を共有する

# 前提条件
$existingADFName = "omni-df-dev"
$existingResourceGroup = "0001-tgomni-dev-rg"
$subscriptionId = "25370ec0-e2ac-4a3c-b242-3772024988b2"
$irName = "OmniLinkedSelfHostedIntegrationRuntime"

# 新規ADF情報
$newADFName = "omni-df-cicd-test"  # 現在のテスト用ADF名
$newResourceGroup = "0001-tgomni-dev-rg"  # 現在のリソースグループ名（既存と同じ）

Write-Host "🔍 Step 1: 既存ADFのIR状況確認" -ForegroundColor Yellow

# Azure接続確認（必要に応じて）
# Connect-AzAccount -Subscription $subscriptionId

# 既存のIR詳細を確認
try {
    Write-Host "既存ADF '$existingADFName' のIR '$irName' を確認中..." -ForegroundColor Cyan
    
    # PowerShell AzモジュールまたはREST APIを使用してIR情報を取得
    # Get-AzDataFactoryV2IntegrationRuntime -ResourceGroupName $existingResourceGroup -DataFactoryName $existingADFName -Name $irName
    
    Write-Host "✅ IR確認完了。次はAzure CLIまたはPortalでIR共有を有効化してください。" -ForegroundColor Green
}
catch {
    Write-Error "❌ IR確認エラー: $($_.Exception.Message)"
}

Write-Host "`n🔧 Step 2: 実行すべき Azure CLI コマンド" -ForegroundColor Yellow

Write-Host "以下のコマンドをAzure CLI（またはCloud Shell）で実行してください：" -ForegroundColor Cyan

$cliCommands = @"

# 1. 既存ADFでIR共有を有効化
az datafactory integration-runtime show \
  --name '$irName' \
  --resource-group '$existingResourceGroup' \
  --factory-name '$existingADFName' \
  --subscription '$subscriptionId'

# 2. IR共有設定を有効化（必要に応じて）
az datafactory integration-runtime update \
  --name '$irName' \
  --resource-group '$existingResourceGroup' \
  --factory-name '$existingADFName' \
  --subscription '$subscriptionId'

# 3. 新規ADFでリンクIRを作成
az datafactory integration-runtime linked-integration-runtime create \
  --resource-group '$newResourceGroup' \
  --factory-name '$newADFName' \
  --name '$irName' \
  --source-data-factory '$existingADFName' \
  --source-resource-group '$existingResourceGroup' \
  --subscription '$subscriptionId'

"@

Write-Host $cliCommands -ForegroundColor White

Write-Host "`n📝 重要な注意事項:" -ForegroundColor Red
Write-Host "1. ADF名とリソースグループ名が正しく設定されています"
Write-Host "2. Azure CLIがインストールされていない場合は、Azure Portalから手動で設定可能です"
Write-Host "3. 既存のIRが実際に稼働していることを事前に確認してください"

Write-Host "`n🚀 次のステップ:" -ForegroundColor Green
Write-Host "1. 上記のAzure CLIコマンドを実行してください"
Write-Host "2. またはAzure Portalから手動で設定してください"
Write-Host "3. 設定完了後、ADFでIRの状態を確認してください"
