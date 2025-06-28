# Integration Runtime 共有設定コマンド
# 実行準備完了

# 設定情報
$existingADF = "omni-df-dev"
$newADF = "omni-df-cicd-test"  
$resourceGroup = "0001-tgomni-dev-rg"
$subscription = "25370ec0-e2ac-4a3c-b242-3772024988b2"
$irName = "OmniLinkedSelfHostedIntegrationRuntime"

Write-Host "=== Azure CLI コマンド実行手順 ===" -ForegroundColor Yellow

Write-Host "`n1️⃣ 既存ADFのIR状況確認:" -ForegroundColor Cyan
Write-Host "az datafactory integration-runtime show --name `"$irName`" --resource-group `"$resourceGroup`" --factory-name `"$existingADF`" --subscription `"$subscription`""

Write-Host "`n2️⃣ 新規ADFでリンクIR作成:" -ForegroundColor Cyan  
Write-Host "az datafactory integration-runtime linked-integration-runtime create --resource-group `"$resourceGroup`" --factory-name `"$newADF`" --name `"$irName`" --source-data-factory `"$existingADF`" --source-resource-group `"$resourceGroup`" --subscription `"$subscription`""

Write-Host "`n3️⃣ 作成したIRの確認:" -ForegroundColor Cyan
Write-Host "az datafactory integration-runtime show --name `"$irName`" --resource-group `"$resourceGroup`" --factory-name `"$newADF`" --subscription `"$subscription`""
