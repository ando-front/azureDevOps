# azureDevOps

## WSL Ubuntu・プロキシ環境での開発Tips

### 1. WSL Ubuntuのインストール
Windows11でapt-get等のLinuxコマンドを利用するには、WSL（Windows Subsystem for Linux）上にUbuntuをインストールしてください。

```powershell
wsl --install -d Ubuntu
```

### 2. プロキシ環境でのapt-get利用
社内プロキシ環境下では、apt-getやcurl等のコマンドが外部ネットワークに直接アクセスできない場合があります。

#### bashrcへのプロキシ設定例
`~/.bashrc` の末尾に以下を追記し、プロキシ環境変数を有効化してください（プロキシアドレスはご自身の環境に合わせて修正）。

```bash
export http_proxy="http://your.proxy.server:8080"
export https_proxy="http://your.proxy.server:8080"
export HTTP_PROXY="http://your.proxy.server:8080"
export HTTPS_PROXY="http://your.proxy.server:8080"
```

設定後、以下を実行して反映します。

```bash
source ~/.bashrc
```

### 3. apt-getの利用
プロキシ設定後、通常通り `apt-get update` や `apt-get install` が利用可能になります。

---