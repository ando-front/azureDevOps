# 超軽量Dockerfile - モック版（プロキシ対応、ODBC無し）
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

# Microsoft ODBC ドライバーのインストール（証明書検証をスキップ）
RUN apt-get update && apt-get install -y --allow-unauthenticated curl gnupg ca-certificates wget apt-transport-https iputils-ping
RUN curl -fsSL -k https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN echo "deb [trusted=yes] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated msodbcsql18
RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated mssql-tools18
RUN apt-get install -y --allow-unauthenticated unixodbc unixodbc-dev


RUN apt-get clean

# ODBC Driver 18 のパスを環境変数に設定
ENV PATH="$PATH:/opt/mssql-tools18/bin"
RUN echo /opt/microsoft/msodbcsql18/lib64 > /etc/ld.so.conf.d/msodbcsql18.conf && ldconfig
RUN ls -la /opt/mssql-tools18/
RUN ls -la /opt/mssql-tools18/bin



ENV ODBC_DRIVER="ODBC Driver 18 for SQL Server"

# Configure ODBC Driver to trust server certificate and disable encryption
RUN cat <<EOF > /etc/odbcinst.ini
[ODBC Driver 18 for SQL Server]
Description=Microsoft ODBC Driver 18 for SQL Server
Driver=/usr/lib/libmsodbcsql-18.so
UsageCount=1
TrustServerCertificate=yes
Encrypt=no
EOF

# 段階1: 基本パッケージのみ（信頼できるホスト設定付き）
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir --upgrade pip && \
    pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir pytest==7.4.3

# 段階2: 基本的なライブラリを追加
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir requests==2.31.0 python-dotenv==1.0.0

# ODBC無しで軽量に進む - pyodbc は使わずモックで代替
# TODO: 技術的負債 - pyodbc + ODBC Driver 18のサポート
# 現在は軽量化のためODBCドライバーを除外しているが、完全なDB統合テストには以下が必要:
# 1. Microsoft ODBC Driver 18 for SQL Server のインストール
# 2. pyodbcライブラリの追加
# 注意: プロキシ問題は run-e2e-flexible.sh で解決済み（--proxy オプション対応）
# 軽量版では条件付きスキップで対応中だが、本格的なCI/CDには完全版も検討が必要
# Install minimal required packages only if needed (without ODBC)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     gcc \
#     g++ \
#     unixodbc-dev \
#     curl \
#     gnupg \
#     && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
#     && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
#     && apt-get update \
#     && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
#     && rm -rf /var/lib/apt/lists/*

# requirements.txtをコピーして残りの依存関係をインストール
COPY requirements.txt .
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir -r requirements.txt

# プロジェクトファイルをコピー
COPY . .
COPY docker/test-runner/run_e2e_tests_in_container.sh /usr/local/bin/run_e2e_tests_in_container.sh
RUN chmod +x /usr/local/bin/run_e2e_tests_in_container.sh

# Install pyodbc without ODBC drivers (will be mocked in tests)
# TODO: 技術的負債 - pyodbcをコメントアウト中
# 現在はpyodbc非依存の軽量版だが、完全なDB統合テストには pyodbc の復活が必要
# pyodbcを有効化する場合は上記のODBCドライバーインストールも同時に必要
# RUN pip install --no-cache-dir pyodbc

# スクリプトファイルに実行権限を付与
RUN chmod +x /app/docker/init-pipeline-paths.sh

# デフォルトコマンド
CMD ["python", "-m", "pytest", "tests/", "--tb=short", "-v", "-x"]
