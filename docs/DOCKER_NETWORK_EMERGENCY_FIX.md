# Docker ネットワーク接続問題 - 即座の解決策

## 🚨 現在発生中の問題

```bash
Err:1 http://deb.debian.org/debian bookworm InRelease
  Connection failed [IP: 151.101.110.132 80]
E: Unable to locate package curl
```

## ⚡ 即座の解決策

### 1. Dockerfileの修正版（推奨）

現在のDockerfileに以下の修正を適用してください：

```dockerfile
# ネットワーク問題回避版 Dockerfile
FROM python:3.9-slim

# プロキシ設定（ARGで受け取り）
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY

ENV HTTP_PROXY=${HTTP_PROXY}
ENV HTTPS_PROXY=${HTTPS_PROXY}
ENV NO_PROXY=${NO_PROXY}
ENV http_proxy=${HTTP_PROXY}
ENV https_proxy=${HTTPS_PROXY}
ENV no_proxy=${NO_PROXY}

# 環境変数の設定
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app

# 作業ディレクトリの設定
WORKDIR /app

# SSL/ネットワーク問題回避設定
ENV PIP_TRUSTED_HOST=pypi.org,pypi.python.org,files.pythonhosted.org
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PYTHONHTTPSVERIFY=0

# 🔧 ネットワーク問題回避: ミラーサーバーを使用
RUN echo "deb http://ftp.jp.debian.org/debian/ bookworm main" > /etc/apt/sources.list && \
    echo "deb http://ftp.jp.debian.org/debian/ bookworm-updates main" >> /etc/apt/sources.list && \
    echo "deb http://security.debian.org/debian-security bookworm-security main" >> /etc/apt/sources.list

# 🔧 段階的パッケージインストール（ネットワーク問題対応）
RUN apt-get update --fix-missing || true && \
    apt-get install -y --no-install-recommends --allow-unauthenticated \
        ca-certificates \
        curl \
        wget && \
    apt-get clean

# 🔧 追加パッケージ（分割インストール）
RUN apt-get update --fix-missing || true && \
    apt-get install -y --no-install-recommends --allow-unauthenticated \
        gnupg \
        lsb-release \
        apt-transport-https && \
    apt-get clean

# 🔧 ODBC関連パッケージ（最小限）
RUN apt-get update --fix-missing || true && \
    apt-get install -y --no-install-recommends --allow-unauthenticated \
        unixodbc \
        unixodbc-dev \
        freetds-dev \
        freetds-bin && \
    rm -rf /var/lib/apt/lists/*

# Microsoft ODBC ドライバー（簡略版）
RUN curl -fsSL -k https://packages.microsoft.com/keys/microsoft.asc | apt-key add - || true
RUN echo "deb [trusted=yes] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list || true
RUN apt-get update --fix-missing || true
RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated msodbcsql18 mssql-tools18 || true

# ODBC Driver 18 のパスを環境変数に設定
ENV PATH="$PATH:/opt/mssql-tools18/bin"
ENV ODBC_DRIVER="ODBC Driver 18 for SQL Server"

# 段階的Python パッケージインストール
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir --upgrade pip

# 基本テストライブラリ
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir \
    pytest==7.4.3 \
    requests==2.31.0 \
    python-dotenv==1.0.0

# プロジェクトファイルのコピー
COPY . /app/

# デフォルトコマンド
CMD ["pytest", "tests/", "-v"]
```

### 2. Docker Compose修正版

```yaml
version: '3.8'

services:
  e2e-test-runner:
    build:
      context: .
      dockerfile: Dockerfile
      args:
        HTTP_PROXY: ${HTTP_PROXY:-}
        HTTPS_PROXY: ${HTTPS_PROXY:-}
        NO_PROXY: ${NO_PROXY:-}
    environment:
      - HTTP_PROXY=${HTTP_PROXY:-}
      - HTTPS_PROXY=${HTTPS_PROXY:-}
      - NO_PROXY=${NO_PROXY:-}
    networks:
      - test-network
    dns:
      - 8.8.8.8
      - 8.8.4.4
    volumes:
      - .:/app
    command: pytest tests/e2e/ -v

networks:
  test-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16
```

### 3. 環境変数設定（Windows）

```powershell
# PowerShellで実行
$env:HTTP_PROXY="http://proxy.company.com:port"  # 必要に応じて設定
$env:HTTPS_PROXY="http://proxy.company.com:port"  # 必要に応じて設定
$env:NO_PROXY="localhost,127.0.0.1"
```

### 4. Docker Daemon設定（DNS問題対応）

```bash
# Windows Docker Desktop の場合
# Settings → Resources → Network → DNS Server を 8.8.8.8 に設定

# Linux の場合
sudo nano /etc/docker/daemon.json
```

```json
{
  "dns": ["8.8.8.8", "8.8.4.4"],
  "dns-search": [],
  "insecure-registries": [],
  "registry-mirrors": []
}
```

## 🔧 トラブルシューティング手順

### ステップ1: ネットワーク接続確認

```bash
# ホストから接続確認
ping 8.8.8.8
curl -I http://deb.debian.org/debian/
nslookup deb.debian.org
```

### ステップ2: Docker再起動

```bash
# Docker Desktop再起動（Windows）
# または
sudo systemctl restart docker  # Linux
```

### ステップ3: 段階的ビルド

```bash
# キャッシュクリア
docker system prune -a

# 段階的ビルド
docker build --no-cache --progress=plain -t test-image .
```

### ステップ4: 最小限テスト

```bash
# 最小限のイメージでテスト
docker run -it python:3.9-slim /bin/bash

# コンテナ内で確認
apt-get update
```

## ⚠️ 一時的回避策

完全な修正までの間、以下の回避策を使用できます：

```bash
# 既存の動作するイメージを使用
docker pull mcr.microsoft.com/mssql/server:2022-latest
docker run -it mcr.microsoft.com/mssql/server:2022-latest /bin/bash

# または軽量テスト実行
python -m pytest tests/unit/ -v  # ローカル実行
```

## 📋 確認リスト

- [ ] インターネット接続確認
- [ ] DNS設定確認（8.8.8.8）
- [ ] Docker Daemon再起動
- [ ] プロキシ設定確認（企業環境）
- [ ] 修正版Dockerfile適用
- [ ] 段階的ビルド実行
- [ ] 最小限テスト実行

---

**緊急度**: 高  
**影響**: E2Eテスト実行停止  
**解決時間**: 10-30分（ネットワーク環境による）
