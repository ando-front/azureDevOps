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
