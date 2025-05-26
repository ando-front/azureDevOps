FROM node:18-bullseye-slim

ARG http_proxy
ARG https_proxy
ENV http_proxy=${http_proxy}
ENV https_proxy=${https_proxy}

# Pythonのインストール
RUN apt-get update \
    && apt-get install -y python3 python3-pip python3-venv build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Python仮想環境のセットアップ
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip3 install azure-storage-blob
RUN pip3 install pytest

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
