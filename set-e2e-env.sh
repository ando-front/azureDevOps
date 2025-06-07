#!/bin/bash
# E2E Test Environment Configuration

export SQL_SERVER_CONNECTION_STRING="DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=SynapseTestDB;UID=sa;PWD=YourStrong!Passw0rd123;TrustServerCertificate=yes;Encrypt=yes;"
export SQL_SERVER_HOST="localhost"
export AZURITE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

echo "Environment variables set for E2E testing"
echo "SQL_SERVER_HOST: $SQL_SERVER_HOST"
echo "AZURITE_CONNECTION_STRING: Set"
echo "SQL_SERVER_CONNECTION_STRING: Set"
