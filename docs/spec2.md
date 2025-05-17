## パイプライン2仕様書

### 1. LookupSQLMi

*   SQLMiからデータを抽出します。
*   `SqlMISource`を使用します。
*   `SELECT * FROM table1`クエリを使用します。

### 2. CopyToSynapse

*   Synapse Analyticsにデータをコピーします。
*   `SqlDWSink`を使用します。
*   `table1`テーブルにコピーします。

### 3. ExportToCSV

*   Synapse AnalyticsからBlobストレージにCSV出力します。
*   `BlobSink`を使用します。
*   `AzureBlobStorageWriteSettings`を使用します。
*   `DelimitedTextWriteSettings`を使用します。
*   `quoteChar`は`"`を使用します。
*   `escapeChar`は`\`を使用します。
*   `fieldDelimiter`は`,`を使用します。
*   `fileExtension`は`.csv`を使用します。

### 4. GZipCSV

*   CSVをGZ化します。
*   `ExecuteDataFlow`を使用します。
*   `dataflow1`データフローを使用します。

### 5. TransferToSFTP

*   SFTPサーバに転送します。
*   `SftpSink`を使用します。
*   `SftpWriteSettings`を使用します。

```mermaid
graph LR
    A[SQLMi] --> B(LookupSQLMi)
    B --> C(CopyToSynapse)
    C --> D(ExportToCSV)
    D --> E(GZipCSV)
    E --> F(TransferToSFTP)
