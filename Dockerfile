# 超軽量Dockerfile - モック版（ODBC不要）
FROM python:3.9-slim

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
RUN apt-get update && apt-get install -y --allow-unauthenticated curl gnupg ca-certificates wget apt-transport-https
RUN curl -fsSL -k https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN echo "deb [trusted=yes] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated msodbcsql18
RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated mssql-tools18
RUN apt-get install -y --allow-unauthenticated unixodbc-dev
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

# ODBC Driver 18 のパスを環境変数に設定
ENV PATH="$PATH:/opt/mssql-tools18/bin"
ENV ODBC_DRIVER="ODBC Driver 18 for SQL Server"

# 段階1: 基本パッケージのみ（信頼できるホスト設定付き）
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir --upgrade pip && \
    pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir pytest==7.4.3

# 段階2: 基本的なライブラリを追加
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir requests==2.31.0 python-dotenv==1.0.0

# requirements.txtをコピーして残りの依存関係をインストール
COPY requirements.txt .
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir -r requirements.txt

# プロジェクトファイルをコピー
COPY . .

# デフォルトコマンド
CMD ["python", "-m", "pytest", "tests/", "--tb=short", "-v", "-x"]
