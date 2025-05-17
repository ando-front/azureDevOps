FROM ubuntu:latest

# 必要なパッケージのインストール
RUN apt-get update && apt-get install -y python3 python3-pip python3-venv npm
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip3 install azure-storage-blob pytest pytest-cov

RUN npm install -g @azure/storage-blob azurite

# SFTPサーバのインストールと設定
RUN apt-get update && apt-get install -y openssh-server
RUN mkdir /var/run/sshd
RUN echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config
RUN sed -i 's/#Port 22/Port 2222/g' /etc/ssh/sshd_config

# ソースコードとテストのコピー
COPY . /app
WORKDIR /app

# 必要なディレクトリの作成
RUN mkdir -p tests/data/input tests/data/output tests/data/sftp /data
RUN chmod 777 tests/data/input tests/data/output tests/data/sftp /data

# ポートの公開
EXPOSE 10000 10001 10002 2222

# AzuriteとSFTPサーバをバックグラウンドで起動し、テストを実行
CMD /bin/bash -c "azurite --location /data --debug /data/debug.log & /usr/sbin/sshd -D & python3 -m pytest tests/unit -v"