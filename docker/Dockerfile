FROM ubuntu:latest

# 必要なパッケージのインストール
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    npm \
    openssh-server \
    && rm -rf /var/lib/apt/lists/*

# Python仮想環境のセットアップ
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Pythonパッケージのインストール
COPY requirements.txt /tmp/
RUN pip3 install -r /tmp/requirements.txt

# npmパッケージのインストール
RUN npm install -g @azure/storage-blob azurite

# SFTPサーバの設定
RUN mkdir /var/run/sshd
RUN echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config
RUN sed -i 's/#Port 22/Port 2222/g' /etc/ssh/sshd_config

# ディレクトリ構造の作成
COPY . /app
WORKDIR /app

# 必要なディレクトリの作成と権限設定
RUN mkdir -p /app/tests/data/{input,output,sftp} /data \
    && chmod 777 /app/tests/data/input \
    && chmod 777 /app/tests/data/output \
    && chmod 777 /app/tests/data/sftp \
    && chmod 777 /data

# ポートの公開
EXPOSE 10000 10001 10002 2222 80

# サービスの起動
CMD ["/bin/bash", "-c", "azurite --location /data --debug /data/debug.log & /usr/sbin/sshd -D & python3 -m pytest /app/tests/unit/ -v"]