FROM python:3.11

WORKDIR /app

# Microsoft ODBC ドライバーのインストール（証明書検証をスキップ）
RUN echo "Acquire::https::Verify-Peer false;" > /etc/apt/apt.conf.d/99verify-peer.conf \
    && echo "Acquire::https::Verify-Host false;" >> /etc/apt/apt.conf.d/99verify-peer.conf \
    && echo "Acquire::Check-Valid-Until false;" >> /etc/apt/apt.conf.d/99verify-peer.conf \
    && echo "APT::Get::AllowUnauthenticated true;" >> /etc/apt/apt.conf.d/99verify-peer.conf \
    && apt-get update -o Acquire::Check-Valid-Until=false -o Acquire::https::Verify-Peer=false \
    && apt-get install -y --allow-unauthenticated \
    curl \
    gnupg \
    ca-certificates \
    wget \
    apt-transport-https \
    && curl -fsSL -k https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && echo "deb [trusted=yes] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update -o Acquire::Check-Valid-Until=false -o Acquire::https::Verify-Peer=false \
    && ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated msodbcsql18 \
    && ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated mssql-tools18 \
    && apt-get install -y --allow-unauthenticated unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /etc/apt/apt.conf.d/99verify-peer.conf

# ODBC Driver 18 のパスを環境変数に設定
ENV PATH="$PATH:/opt/mssql-tools18/bin"
ENV ODBC_DRIVER="ODBC Driver 18 for SQL Server"

# Disable SSL verification for Python
ENV PYTHONHTTPSVERIFY=0
ENV CURL_CA_BUNDLE=""
ENV REQUESTS_CA_BUNDLE=""

# Install Python dependencies with SSL bypass
COPY requirements.txt .
RUN mkdir -p /etc/pip \
    && echo "[global]" > /etc/pip/pip.conf \
    && echo "trusted-host = pypi.org" >> /etc/pip/pip.conf \
    && echo "               pypi.python.org" >> /etc/pip/pip.conf \
    && echo "               files.pythonhosted.org" >> /etc/pip/pip.conf \
    && echo "cert = " >> /etc/pip/pip.conf \
    && echo "client-cert = " >> /etc/pip/pip.conf \
    && pip install --upgrade pip --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org \
    && pip install -r requirements.txt --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org

# Copy application code
COPY . .

# Create config directory
RUN mkdir -p /app/config

EXPOSE 8080

# Use uvicorn to run the FastAPI application
CMD ["python", "-c", "import uvicorn; uvicorn.run('main:app', host='0.0.0.0', port=8080)"]
