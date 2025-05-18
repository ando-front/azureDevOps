## パイプライン2の仕様

### 1. SQLMiからのデータ抽出

このステップでは、SQLMiデータベースからデータを抽出します。

*   `SqlMISource`を使用してSQLMiに接続します。
*   `SELECT * FROM table1`クエリでデータを抽出します。

### 2. Synapse Analyticsへのデータコピー

このステップでは、抽出したデータをSynapse Analyticsにコピーします。

*   `SqlDWSink`を使用してSynapse Analyticsに接続します。
*   `table1`テーブルにデータをコピーします。

### 3. CSV形式でのBlobストレージへの出力

このステップでは、Synapse AnalyticsからBlobストレージにCSV形式でデータを出力します。

*   `BlobSink`を使用してBlobストレージに接続します。
*   `AzureBlobStorageWriteSettings`を使用してBlobストレージの書き込み設定を行います。
*   `DelimitedTextWriteSettings`を使用してCSV形式の詳細設定を行います。
    *   `quoteChar`は`"`を使用します。
    *   `escapeChar`は`\`を使用します。
    *   `fieldDelimiter`は`,`を使用します。
    *   `fileExtension`は`.csv`を使用します。

### 4. CSVファイルのGZip圧縮

このステップでは、CSVファイルをGZip形式で圧縮します。

*   `ExecuteDataFlow`を使用してデータフローを実行します。
*   `dataflow1`データフローを使用します。

### 5. SFTPサーバへの転送

このステップでは、GZip圧縮されたファイルをSFTPサーバに転送します。

*   `SftpSink`を使用してSFTPサーバに接続します。
*   `SftpWriteSettings`を使用してSFTPサーバへの書き込み設定を行います。

```mermaid
graph LR
    A[SQLMi] --> B(LookupSQLMi)
    B --> C(CopyToSynapse)
    C --> D(ExportToCSV)
    D --> E(GZipCSV)
    E --> F(TransferToSFTP)
