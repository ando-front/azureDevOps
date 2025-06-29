create_no_proxy_compose_file() {
    cat > docker-compose.e2e.no-proxy.yml << 'EOF'
version: '3.8'

services:
  # SQL Server サービス
  sql-server:
    image: mcr.microsoft.com/mssql/server:2019-latest
    container_name: adf-sql-server-test
    environment:
      - ACCEPT_EULA=Y
      - SA_PASSWORD=YourStrong!Passw0rd123
      - MSSQL_PID=Express
    ports:
      - "1433:1433"
    volumes:
      - ./sql/init:/docker-entrypoint-initdb.d
      - sql_data:/var/opt/mssql
    networks:
      - adf-e2e-network
    # healthcheck:
    #   test: ["CMD-SHELL", "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P YourStrong!Passw0rd123 -Q 'SELECT 1' || exit 1"]
    #   interval: 10s
    #   retries: 10
    #   start_period: 10s
    #   timeout: 3s

  # Azurite (Azure Storage Emulator)
  azurite:
    image: mcr.microsoft.com/azure-storage/azurite:latest
    container_name: adf-azurite-test
    command: "azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0"
    ports:
      - "10000:10000"  # Blob service
      - "10001:10001"  # Queue service
      - "10002:10002"  # Table service
    volumes:
      - azurite_data:/data
    networks:
      - adf-e2e-network

  # E2E テストランナー
  e2e-test-runner:
    build:
      context: .
      dockerfile: Dockerfile
      args:
        - NO_PROXY=true
    container_name: adf-e2e-test-runner
    environment:
      # データベース接続設定
      - SQL_SERVER_HOST=sql-server
      - SQL_SERVER_PORT=1433
      - SQL_SERVER_DATABASE=SynapseTestDB
      - SQL_SERVER_USER=sa
      - SQL_SERVER_PASSWORD=YourStrong!Passw0rd123
      
      # Azure Storage 接続設定
      - AZURITE_HOST=azurite
      - AZURITE_BLOB_PORT=10000
      - AZURITE_CONNECTION_STRING=DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;
      
      # テスト設定
      - E2E_TIMEOUT=300
      - E2E_RETRY_COUNT=3
      - PYTEST_ARGS=--tb=short --maxfail=5
      
      # プロキシ無効化
      - HTTP_PROXY=
      - HTTPS_PROXY=
      - http_proxy=
      - https_proxy=
      - NO_PROXY=*
      - no_proxy=*
      
    volumes:
      - .:/app
      - ./test_results:/app/test_results
      - ./logs:/app/logs
    working_dir: /app
    depends_on:
      sql-server:
        condition: service_started
      azurite:
        condition: service_started
    networks:
      - adf-e2e-network
    command: "tail -f /dev/null"

volumes:
  sql_data:
  azurite_data:

networks:
  adf-e2e-network:
    driver: bridge
EOF
    
    echo "プロキシなし用 Docker Compose ファイルを作成しました"
}

create_no_proxy_compose_file