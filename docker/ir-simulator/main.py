"""
Integration Runtime Simulator for E2E Testing
Azure Data Factory活動をシミュレートするFastAPIアプリケーション
"""

from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
import json
import uuid
import asyncio
import os
import logging
from datetime import datetime
import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ADF Integration Runtime Simulator", version="1.0.0")

# 環境変数
SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST", "sqlserver-test")
SQL_SERVER_PORT = os.getenv("SQL_SERVER_PORT", "1433")
SQL_SERVER_USER = os.getenv("SQL_SERVER_USER", "sa")
SQL_SERVER_PASSWORD = os.getenv("SQL_SERVER_PASSWORD", "YourStrong!Passw0rd123")
AZURITE_HOST = os.getenv("AZURITE_HOST", "azurite-test")
AZURITE_PORT = os.getenv("AZURITE_PORT", "10000")

# データモデル
class CopyActivityRequest(BaseModel):
    source: Dict[str, Any]
    sink: Dict[str, Any]
    pipeline_name: str
    activity_name: str
    execution_id: Optional[str] = None

class CopyActivityResponse(BaseModel):
    execution_id: str
    status: str
    rows_copied: int
    start_time: str
    end_time: Optional[str] = None
    error_message: Optional[str] = None

class PipelineExecutionRequest(BaseModel):
    pipeline_name: str
    parameters: Optional[Dict[str, Any]] = {}
    execution_id: Optional[str] = None

class PipelineExecutionResponse(BaseModel):
    execution_id: str
    pipeline_name: str
    status: str
    start_time: str
    end_time: Optional[str] = None
    activities_executed: List[str] = []
    error_message: Optional[str] = None

# 実行状態の管理
execution_status: Dict[str, Dict] = {}

def get_sql_connection():
    """SQL Server接続を取得"""
    try:
        connection_string = f"""
        DRIVER={{ODBC Driver 18 for SQL Server}};
        SERVER={SQL_SERVER_HOST},{SQL_SERVER_PORT};
        DATABASE=TGMATestDB;
        UID={SQL_SERVER_USER};
        PWD={SQL_SERVER_PASSWORD};
        TrustServerCertificate=yes;
        Encrypt=no;
        """
        return pyodbc.connect(connection_string)
    except Exception as e:
        logger.error(f"SQL Server connection failed: {e}")
        return None

def get_azurite_client():
    """Azurite Blob Service Clientを取得"""
    connection_string = f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{AZURITE_HOST}:{AZURITE_PORT}/devstoreaccount1;"
    return BlobServiceClient.from_connection_string(connection_string)

@app.get("/")
async def root():
    """ヘルスチェックエンドポイント"""
    return {"message": "ADF Integration Runtime Simulator is running"}

@app.get("/health")
async def health_check():
    """詳細ヘルスチェック"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {}
    }
      # SQL Server接続チェック
    try:
        conn = get_sql_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            conn.close()
            health_status["services"]["sql_server"] = "healthy"
        else:
            health_status["services"]["sql_server"] = "unavailable: pyodbc not installed"
    except Exception as e:
        health_status["services"]["sql_server"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    # Azurite接続チェック
    try:
        blob_client = get_azurite_client()
        # コンテナリストを試行
        list(blob_client.list_containers())
        health_status["services"]["azurite"] = "healthy"
    except Exception as e:
        health_status["services"]["azurite"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    return health_status

@app.post("/copy-activity", response_model=CopyActivityResponse)
async def execute_copy_activity(request: CopyActivityRequest):
    """Copy Activity の実行をシミュレート"""
    execution_id = request.execution_id or str(uuid.uuid4())
    start_time = datetime.now()
    
    logger.info(f"Starting copy activity: {request.activity_name} in pipeline: {request.pipeline_name}")
    
    try:
        # ETLログの記録
        conn = get_sql_connection()
        if conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO [dbo].[pipeline_execution_log] 
                (pipeline_name, execution_start, status, records_processed)
                VALUES (?, ?, ?, ?)
            """, request.pipeline_name, start_time, "RUNNING", 0)
            conn.commit()
        else:
            logger.warning("SQL connection not available - skipping database logging")
        
        # データ転送のシミュレーション
        await asyncio.sleep(2)  # 処理時間をシミュレート
        
        # 成功ケースでのrows_copiedの計算
        rows_copied = 0
        if conn and "client_dm" in request.pipeline_name.lower():
            cursor.execute("SELECT COUNT(*) FROM [dbo].[client_dm]")
            rows_copied = cursor.fetchone()[0]
        elif conn and "clientdmbx" in request.pipeline_name.lower():
            cursor.execute("SELECT COUNT(*) FROM [dbo].[ClientDmBx]")
            rows_copied = cursor.fetchone()[0]
        elif conn and "point_grant" in request.pipeline_name.lower():
            cursor.execute("SELECT COUNT(*) FROM [dbo].[point_grant_email]")
            rows_copied = cursor.fetchone()[0]
        else:
            # SQL接続が無い場合はダミー値を返す
            rows_copied = 100
        
        end_time = datetime.now()
        
        # ログの更新
        if conn:
            cursor.execute("""
                UPDATE [dbo].[pipeline_execution_log] 
                SET execution_end = ?, status = ?, records_processed = ?
                WHERE pipeline_name = ? AND execution_start = ?
            """, end_time, "SUCCESS", rows_copied, request.pipeline_name, start_time)
            conn.commit()
            conn.close()
        
        response = CopyActivityResponse(
            execution_id=execution_id,
            status="SUCCESS",
            rows_copied=rows_copied,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat()
        )
        
        # 実行状態を保存
        execution_status[execution_id] = response.dict()
        
        logger.info(f"Copy activity completed successfully: {execution_id}")
        return response
        
    except Exception as e:
        error_message = str(e)
        logger.error(f"Copy activity failed: {error_message}")
        
        # エラーログの記録
        try:
            cursor.execute("""
                UPDATE [dbo].[pipeline_execution_log] 
                SET execution_end = ?, status = ?, error_message = ?
                WHERE pipeline_name = ? AND execution_start = ?
            """, datetime.now(), "FAILED", error_message, request.pipeline_name, start_time)
            conn.commit()
            conn.close()
        except:
            pass
        
        response = CopyActivityResponse(
            execution_id=execution_id,
            status="FAILED",
            rows_copied=0,
            start_time=start_time.isoformat(),
            error_message=error_message
        )
        
        execution_status[execution_id] = response.dict()
        return response

@app.post("/pipeline-execution", response_model=PipelineExecutionResponse)
async def execute_pipeline(request: PipelineExecutionRequest):
    """パイプライン実行をシミュレート"""
    execution_id = request.execution_id or str(uuid.uuid4())
    start_time = datetime.now()
    
    logger.info(f"Starting pipeline execution: {request.pipeline_name}")
    
    try:
        # パイプライン別の活動をシミュレート
        activities_executed = []
        
        if "client_dm" in request.pipeline_name.lower():
            activities_executed = ["CopyActivity", "DataTransformation", "Validation"]
        elif "point_grant" in request.pipeline_name.lower():
            activities_executed = ["DataExtraction", "EmailPreparation", "EmailSending"]
        else:
            activities_executed = ["DefaultActivity"]
        
        # 各活動の実行をシミュレート
        for activity in activities_executed:
            await asyncio.sleep(1)  # 活動実行時間をシミュレート
            logger.info(f"Executed activity: {activity}")
        
        end_time = datetime.now()
        
        response = PipelineExecutionResponse(
            execution_id=execution_id,
            pipeline_name=request.pipeline_name,
            status="SUCCESS",
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            activities_executed=activities_executed
        )
        
        execution_status[execution_id] = response.dict()
        
        logger.info(f"Pipeline execution completed successfully: {execution_id}")
        return response
        
    except Exception as e:
        error_message = str(e)
        logger.error(f"Pipeline execution failed: {error_message}")
        
        response = PipelineExecutionResponse(
            execution_id=execution_id,
            pipeline_name=request.pipeline_name,
            status="FAILED",
            start_time=start_time.isoformat(),
            activities_executed=activities_executed if 'activities_executed' in locals() else [],
            error_message=error_message
        )
        
        execution_status[execution_id] = response.dict()
        return response

@app.get("/execution-status/{execution_id}")
async def get_execution_status(execution_id: str):
    """実行状態の取得"""
    if execution_id in execution_status:
        return execution_status[execution_id]
    else:
        raise HTTPException(status_code=404, detail="Execution not found")

@app.get("/executions")
async def list_executions():
    """全実行履歴の取得"""
    return {"executions": list(execution_status.values())}

@app.post("/upload-test-data")
async def upload_test_data(file: UploadFile = File(...)):
    """テストデータのアップロード"""
    try:
        # Azuriteにファイルをアップロード
        blob_client = get_azurite_client()
        
        # コンテナが存在しない場合は作成
        container_name = "test-data"
        try:
            blob_client.create_container(container_name)
        except:
            pass  # コンテナが既に存在する場合
        
        # ファイルをアップロード
        blob_name = f"uploaded/{file.filename}"
        blob_client_instance = blob_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )
        
        content = await file.read()
        blob_client_instance.upload_blob(content, overwrite=True)
        
        return {
            "message": "File uploaded successfully",
            "container": container_name,
            "blob_name": blob_name,
            "size": len(content)
        }
        
    except Exception as e:
        logger.error(f"File upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.get("/test-database-status")
async def get_database_status():
    """テストデータベースの状態確認"""
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        # データサマリーの取得
        cursor.execute("SELECT * FROM [dbo].[v_test_data_summary]")
        data_summary = [
            {
                "table_name": row[0],
                "record_count": row[1],
                "last_updated": row[2].isoformat() if row[2] else None
            }
            for row in cursor.fetchall()
        ]
        
        conn.close()
        
        return {
            "status": "connected",
            "database": "TGMATestDB",
            "data_summary": data_summary
        }
        
    except Exception as e:
        logger.error(f"Database status check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
