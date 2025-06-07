# CI/CDパイプラインガイド

GitHub Actionsを使用したCI/CDパイプラインの設定と運用ガイドです。

## ブランチ戦略

### ブランチ構成

| ブランチ | 目的 | 保護レベル | 自動デプロイ |
|---------|------|-----------|-------------|
| **master** | 本番環境デプロイ専用 | 🔒 高 | Azure本番環境 |
| **develop** | 開発統合・ステージング | 🔒 中 | Azure開発環境 |
| **feature/*** | 機能開発・バグ修正 | 🔓 低 | なし |
| **hotfix/*** | 緊急修正 | 🔒 高 | Azure本番環境 |

### ブランチ戦略フロー

```mermaid
sequenceDiagram
    participant M as master<br/>(本番環境)
    participant D as develop<br/>(統合環境) 
    participant F1 as feature/新機能A
    participant F2 as feature/新機能B
    participant F3 as feature/バグ修正
    participant H as hotfix/緊急修正
    
    Note over M,H: 🚀 開発サイクル開始
    
    M->>D: 1. develop ブランチ作成
    Note over D: developで統合開発開始
    
    D->>F1: 2. feature ブランチ作成
    Note over F1: 新機能A開発
    F1->>F1: 3. 機能実装・単体テスト
    F1->>D: 4. Pull Request → develop
    Note over D: コードレビュー・マージ
    
    D->>F2: 5. feature ブランチ作成
    Note over F2: 新機能B開発
    F2->>F2: 6. 機能実装・単体テスト
    F2->>D: 7. Pull Request → develop
    Note over D: コードレビュー・マージ
    
    D->>F3: 8. feature ブランチ作成
    Note over F3: バグ修正
    F3->>F3: 9. 修正・テスト
    F3->>D: 10. Pull Request → develop
    Note over D: 修正確認・マージ
    
    Note over D: 📋 統合テスト・E2Eテスト実行
    D->>D: 11. 全機能統合テスト
    
    D->>M: 12. Pull Request → master
    Note over M: 🔍 最終レビュー・本番デプロイ
    
    rect rgb(255, 235, 235)
        Note over M,H: ⚠️ 緊急修正が必要な場合
        M->>H: 13. hotfix ブランチ作成
        H->>H: 14. 緊急修正実装
        H->>M: 15. 直接 Pull Request → master
        Note over M: 🚨 緊急デプロイ
        M->>D: 16. hotfix内容を develop にマージ
    end
    
    Note over M,H: 🔄 次のサイクルへ
```

## ワークフロー

### 1. 機能開発

```bash
git checkout develop
git pull origin develop
git checkout -b feature/新機能名
# 開発作業
git push origin feature/新機能名
# Pull Request作成 (feature/* → develop)
```

### 2. 開発統合

```bash
# developブランチで統合テスト
pytest tests/unit/ tests/e2e/ -v
# レビュー後マージ
```

### 3. 本番リリース

```bash
# Pull Request作成 (develop → master)
# 本番デプロイ実行
```

## CI/CDパイプライン時系列フロー

```mermaid
sequenceDiagram
    participant Dev as 👩‍💻 開発者
    participant Git as 📁 Git Repository
    participant CI as 🔧 CI/CD Pipeline
    participant Test as 🧪 Test Environment
    participant Stage as 🎭 Staging Environment
    participant Prod as 🚀 Production Environment
    
    Note over Dev,Prod: 📈 CI/CDフロー開始
    
    Dev->>Git: 1. feature ブランチにコミット・プッシュ
    Git->>CI: 2. Webhook でパイプライン起動
    
    rect rgb(240, 248, 255)
        Note over CI,Test: 🧪 開発段階テスト
        CI->>Test: 3. 単体テスト実行
        Test-->>CI: 4. テスト結果 (90%以上で通過)
        CI->>CI: 5. コード品質チェック (ESLint/Pylint)
        CI->>CI: 6. セキュリティスキャン
    end
    
    CI->>Git: 7. Pull Request 作成通知
    Dev->>Git: 8. コードレビュー・承認
    Git->>CI: 9. develop ブランチマージ
    
    rect rgb(240, 255, 240)
        Note over CI,Stage: 🎭 統合段階テスト
        CI->>Test: 10. E2Eテスト実行
        Test-->>CI: 11. E2Eテスト結果 (85%以上で通過)
        CI->>Stage: 12. Azure開発環境デプロイ
        Stage-->>CI: 13. 統合テスト結果
        CI->>CI: 14. パフォーマンステスト
    end
    
    Dev->>Git: 15. master への Pull Request 作成
    Git->>CI: 16. 本番リリースパイプライン起動
    
    rect rgb(255, 240, 240)
        Note over CI,Prod: 🚀 本番段階デプロイ
        CI->>Test: 17. 全テスト再実行
        Test-->>CI: 18. 全テスト結果 (95%以上で通過)
        CI->>CI: 19. 最終セキュリティチェック
        CI->>Prod: 20. Azure本番環境デプロイ
        Prod-->>CI: 21. ヘルスチェック結果
        CI->>CI: 22. 本番監視開始
    end
    
    Note over Dev,Prod: ✅ リリース完了
```

## パイプライン設定

### トリガー設定

| ブランチ | トリガー | テスト | デプロイ |
|---------|---------|--------|---------|
| **feature/*** | Push | 単体テスト | なし |
| **develop** | PR Merge | 単体 + E2E | Azure開発環境 |
| **master** | PR Merge | 全テスト | Azure本番環境 |

### 品質ゲート

```mermaid
sequenceDiagram
    participant Code as 💻 ソースコード
    participant Unit as 🧪 単体テスト
    participant Quality as 📋 品質チェック
    participant Security as 🔒 セキュリティ
    participant E2E as 🎭 E2Eテスト
    participant Perf as ⚡ パフォーマンス
    participant Deploy as 🚀 デプロイ
    
    Note over Code,Deploy: 🎯 品質ゲートフロー
    
    Code->>Unit: 1. 単体テスト実行
    Unit-->>Code: 📊 90%以上通過必須
    
    rect rgb(240, 248, 255)
        Note over Unit,Quality: 🔍 コード品質チェック段階
        Unit->>Quality: 2. ESLint/Pylint実行
        Quality-->>Unit: 📏 コーディング規約準拠
        Quality->>Quality: 3. コードカバレッジ測定
        Quality-->>Unit: 📈 カバレッジ80%以上
    end
    
    Quality->>Security: 4. セキュリティスキャン開始
    Security-->>Quality: 🛡️ 脆弱性チェック完了
    
    rect rgb(240, 255, 240)
        Note over Security,E2E: 🎭 統合テスト段階
        Security->>E2E: 5. E2Eテスト実行
        E2E-->>Security: 🔧 85%以上通過必須
        E2E->>E2E: 6. 統合シナリオテスト
        E2E-->>Security: ✅ 全統合フロー確認
    end
    
    E2E->>Perf: 7. パフォーマンステスト開始
    Perf-->>E2E: ⚡ レスポンス時間チェック
    Perf->>Perf: 8. ロードテスト実行
    Perf-->>E2E: 📊 負荷耐性確認
    
    rect rgb(255, 240, 240)
        Note over Perf,Deploy: 🚀 本番デプロイ段階
        Perf->>Deploy: 9. 最終品質ゲート
        Deploy-->>Perf: 🎯 95%以上通過必須
        Deploy->>Deploy: 10. 本番環境デプロイ
        Deploy-->>Perf: ✅ デプロイ成功
    end
    
    Note over Code,Deploy: 🏆 品質保証完了
```

## 現在の達成状況

### テスト成功率（実装完了）

- ✅ **統合テスト**: 4/4 成功 (100%) - SQL外部化・ARMテンプレート分割検証
- ✅ **単体テスト**: 24/28 成功 (85.7%) - モック実装による高速実行  
- ✅ **E2Eテスト**: 4/4 成功 (100%) - Docker環境での本格的DB接続テスト
- ✅ **Docker E2E実装**: Point Grant Emailパイプライン完全実装・検証完了
- 🟨 **本番テスト**: 実装準備中（CI/CD統合予定）

### 最新の技術的成果

- **Docker E2E環境**: SQL Server + Azurite + IR Simulatorの完全統合
- **包括的テストスイート**: 完全フロー・データ検証・エラーハンドリング・性能テスト
- **パラメーターバリデーション**: 不正入力に対する堅牢なエラーハンドリング
- **ハイブリッド実行**: IR Simulator + ローカルフォールバック機能
- **自動化されたクリーンアップ**: テスト完了後のデータ自動削除

## GitHub Actions 設定例

### 単体テスト用ワークフロー

```yaml
name: Unit Tests
on:
  push:
    branches: [ feature/*, develop ]
  pull_request:
    branches: [ develop ]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - name: Set up Python
      uses: actions/setup-python@v3
      with:
        python-version: '3.9'
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
    - name: Run unit tests
      run: |
        pytest tests/unit/ -v --cov=src --cov-report=xml
    - name: Upload coverage
      uses: codecov/codecov-action@v3
```

### E2Eテスト用ワークフロー

```yaml
name: E2E Tests
on:
  push:
    branches: [ develop, master ]

jobs:
  e2e-tests:
    runs-on: ubuntu-latest
    services:
      sqlserver:
        image: mcr.microsoft.com/mssql/server:2022-latest
        env:
          SA_PASSWORD: YourStrong!Passw0rd
          ACCEPT_EULA: Y
        ports:
          - 1433:1433
    steps:
    - uses: actions/checkout@v3
    - name: Set up Python
      uses: actions/setup-python@v3
      with:
        python-version: '3.9'
    - name: Install dependencies
      run: |
        pip install -r requirements.e2e.txt
    - name: Run E2E tests
      run: |
        pytest tests/e2e/ -v --tb=short
```

## 本番デプロイ設定

### Azure Data Factory デプロイ

```yaml
name: Deploy to Production
on:
  push:
    branches: [ master ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - name: Azure Login
      uses: azure/login@v1
      with:
        creds: ${{ secrets.AZURE_CREDENTIALS }}
    - name: Deploy ADF
      uses: azure/arm-deploy@v1
      with:
        subscriptionId: ${{ secrets.AZURE_SUBSCRIPTION_ID }}
        resourceGroupName: ${{ secrets.AZURE_RG }}
        template: arm_template_split/ArmTemplate_4_Main.json
        parameters: arm_template_split/ArmParameters.json
```
