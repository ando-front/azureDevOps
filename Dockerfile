FROM ubuntu:latest

# 必要なパッケージのインストール
RUN apt-get update && apt-get install -y python3 python3-pip azurite npm
RUN npm install -g @azure/storage-blob

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

# SFTPサーバの起動
CMD /usr/sbin/sshd -D && azurite --location /data --debug /data/debug.log

# SFTPサーバのポートを公開
EXPOSE 2222

# 作業ディレクトリの設定
WORKDIR /tests

# テストの実行
CMD ["python3", "run_tests.py"]
