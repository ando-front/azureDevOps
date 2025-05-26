FROM node:18-bullseye-slim

ARG http_proxy
ARG https_proxy
ARG no_proxy
ENV http_proxy=${http_proxy}
ENV https_proxy=${https_proxy}
ENV no_proxy=${no_proxy}
ENV DEBIAN_FRONTEND=noninteractive

# プロキシ設定の確認
RUN echo "Proxy settings: http_proxy=${http_proxy}, https_proxy=${https_proxy}, no_proxy=${no_proxy}"

# Pythonのインストール（プロキシ対応）
RUN apt-get update -o Acquire::http::Proxy="${http_proxy}" -o Acquire::https::Proxy="${https_proxy}" --fix-missing \
    && apt-get install -y --no-install-recommends python3 python3-pip python3-venv build-essential curl ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Python仮想環境のセットアップ
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH="/tests"

# SSL証明書の更新とプロキシ設定
RUN update-ca-certificates

# pip設定の追加（プロキシとSSL証明書検証の設定）
RUN mkdir -p /root/.pip
RUN echo "[global]" > /root/.pip/pip.conf && \
    echo "proxy = ${http_proxy}" >> /root/.pip/pip.conf && \
    echo "trusted-host = pypi.org pypi.python.org files.pythonhosted.org" >> /root/.pip/pip.conf && \
    echo "cert = /etc/ssl/certs/ca-certificates.crt" >> /root/.pip/pip.conf

# venv環境のpipでパッケージをインストール
RUN /opt/venv/bin/python -m pip install --upgrade pip --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org
RUN /opt/venv/bin/python -m pip install azure-storage-blob pytest pytest-cov --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org

# pytestが正しく仮想環境にインストールされているか確認
RUN /opt/venv/bin/python -c "import pytest; print(f'pytest version: {pytest.__version__}')"

RUN npm install -g @azure/storage-blob azurite

# Azuriteの起動
EXPOSE 10000 10001 10002
# SFTPサーバのインストール
RUN apt-get update && apt-get install -y openssh-server

# SFTPサーバの設定
RUN mkdir /var/run/sshd
RUN echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config
RUN sed -i 's/#Port 22/Port 2222/g' /etc/ssh/sshd_config

# テストスクリプトのコピー
COPY tests /tests
COPY src /src

# 必要なディレクトリの作成
RUN mkdir -p /tests/input /tests/output /data
RUN chmod 777 /tests/input /tests/output /data

# SFTPサーバのポートを公開
EXPOSE 2222

# 作業ディレクトリの設定
WORKDIR /tests

# AzuriteとSFTPサーバをバックグラウンドで起動し、テストを実行
CMD ["/bin/bash", "-c", "azurite --location /data --debug /data/debug.log & /usr/sbin/sshd -D & python3 /tests/run_tests.py"]
