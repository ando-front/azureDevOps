-- Synapse Analytics テスト用データベース作成
CREATE DATABASE SynapseTestDB;
GO

USE SynapseTestDB;
GO

-- テスト用テーブル作成例
CREATE TABLE dbo.TestTable
(
    ID int IDENTITY(1,1) PRIMARY KEY,
    Name nvarchar(255) NOT NULL,
    Value decimal(18,2),
    CreatedDate datetime2 DEFAULT GETDATE()
);
GO

-- ClientDm関連のテストテーブル
CREATE TABLE dbo.ClientDm
(
    ClientId int IDENTITY(1,1) PRIMARY KEY,
    ClientName nvarchar(255) NOT NULL,
    ClientCode nvarchar(50),
    Status nvarchar(20) DEFAULT 'Active',
    CreatedDate datetime2 DEFAULT GETDATE(),
    UpdatedDate datetime2 DEFAULT GETDATE()
);
GO

-- PointGrantEmail関連のテストテーブル
CREATE TABLE dbo.PointGrantEmail
(
    EmailId int IDENTITY(1,1) PRIMARY KEY,
    ClientId int,
    EmailAddress nvarchar(255) NOT NULL,
    PointAmount decimal(10,2),
    GrantDate datetime2 DEFAULT GETDATE(),
    Status nvarchar(20) DEFAULT 'Pending',
    FOREIGN KEY (ClientId) REFERENCES dbo.ClientDm(ClientId)
);
GO

-- ActionPointCurrentMonthEntryList関連のテストテーブル
CREATE TABLE dbo.ActionPointEntry
(
    EntryId int IDENTITY(1,1) PRIMARY KEY,
    ClientId int,
    ActionType nvarchar(100),
    PointAmount decimal(10,2),
    EntryDate datetime2 DEFAULT GETDATE(),
    CurrentMonth bit DEFAULT 1,
    FOREIGN KEY (ClientId) REFERENCES dbo.ClientDm(ClientId)
);
GO

-- Send系パイプライン用の追加テーブル
CREATE TABLE dbo.mTGMailPermission
(
    PermissionId int IDENTITY(1,1) PRIMARY KEY,
    ConnectionKey nvarchar(100) NOT NULL,
    CustomerKey nvarchar(100) NOT NULL,
    EmailAddress nvarchar(255) NOT NULL,
    PermissionStatus nvarchar(20) DEFAULT 'GRANTED',
    PermissionDate datetime2 DEFAULT GETDATE(),
    UpdateDateTime datetime2 DEFAULT GETDATE(),
    OutputDateTime datetime2 DEFAULT GETDATE()
);
GO

CREATE TABLE dbo.MovingPromotionList
(
    PromotionId int IDENTITY(1,1) PRIMARY KEY,
    ConnectionKey nvarchar(100) NOT NULL,
    CustomerKey nvarchar(100) NOT NULL,
    PromotionType nvarchar(50),
    PromotionDate datetime2 DEFAULT GETDATE(),
    Status nvarchar(20) DEFAULT 'Active'
);
GO

CREATE TABLE dbo.LINEIDLinkInfo
(
    LinkId int IDENTITY(1,1) PRIMARY KEY,
    ConnectionKey nvarchar(100) NOT NULL,
    CustomerKey nvarchar(100) NOT NULL,
    LineUserId nvarchar(100),
    LinkStatus nvarchar(20) DEFAULT 'LINKED',
    LinkDate datetime2 DEFAULT GETDATE()
);
GO

CREATE TABLE dbo.LIMSettlementBreakdownRepair
(
    RepairId int IDENTITY(1,1) PRIMARY KEY,
    ConnectionKey nvarchar(100) NOT NULL,
    SettlementId nvarchar(100) NOT NULL,
    BreakdownType nvarchar(50),
    Amount decimal(18,2),
    RepairDate datetime2 DEFAULT GETDATE(),
    Status nvarchar(20) DEFAULT 'REPAIRED'
);
GO

CREATE TABLE dbo.ElectricityContractThanks
(
    ThankId int IDENTITY(1,1) PRIMARY KEY,
    ConnectionKey nvarchar(100) NOT NULL,
    CustomerKey nvarchar(100) NOT NULL,
    ContractId nvarchar(100),
    ContractDate datetime2 DEFAULT GETDATE(),
    ThanksStatus nvarchar(20) DEFAULT 'SENT'
);
GO

CREATE TABLE dbo.Cpkiyk
(
    CpkiykId int IDENTITY(1,1) PRIMARY KEY,
    ConnectionKey nvarchar(100) NOT NULL,
    CustomerKey nvarchar(100) NOT NULL,
    ProcessType nvarchar(50),
    ProcessDate datetime2 DEFAULT GETDATE(),
    Status nvarchar(20) DEFAULT 'PROCESSED'
);
GO

CREATE TABLE dbo.KarteContractScoreInfo
(
    ScoreId int IDENTITY(1,1) PRIMARY KEY,
    ConnectionKey nvarchar(100) NOT NULL,
    CustomerKey nvarchar(100) NOT NULL,
    ContractScore decimal(5,2),
    ScoreDate datetime2 DEFAULT GETDATE(),
    ScoreStatus nvarchar(20) DEFAULT 'CALCULATED'
);
GO

CREATE TABLE dbo.ActionPointRecentTransactionHistoryList
(
    TransactionId int IDENTITY(1,1) PRIMARY KEY,
    ConnectionKey nvarchar(100) NOT NULL,
    CustomerKey nvarchar(100) NOT NULL,
    TransactionType nvarchar(50),
    TransactionAmount decimal(18,2),
    TransactionDate datetime2 DEFAULT GETDATE(),
    Points decimal(10,2)
);
GO

-- テストデータ挿入
INSERT INTO dbo.TestTable
    (Name, Value)
VALUES
    ('Test1', 100.50),
    ('Test2', 200.75),
    ('Test3', 300.25);
GO

INSERT INTO dbo.ClientDm
    (ClientName, ClientCode, Status)
VALUES
    ('テストクライアント1', 'TC001', 'Active'),
    ('テストクライアント2', 'TC002', 'Active'),
    ('テストクライアント3', 'TC003', 'Inactive');
GO

INSERT INTO dbo.PointGrantEmail
    (ClientId, EmailAddress, PointAmount, Status)
VALUES
    (1, 'test1@example.com', 1000.00, 'Sent'),
    (2, 'test2@example.com', 2000.00, 'Pending'),
    (3, 'test3@example.com', 1500.00, 'Failed');
GO

INSERT INTO dbo.ActionPointEntry
    (ClientId, ActionType, PointAmount, CurrentMonth)
VALUES
    (1, 'Purchase', 500.00, 1),
    (2, 'Referral', 300.00, 1),
    (3, 'Bonus', 100.00, 0);
GO

INSERT INTO dbo.mTGMailPermission
    (ConnectionKey, CustomerKey, EmailAddress, PermissionStatus)
VALUES
    ('MTG001', 'CUST001', 'test1@example.com', 'GRANTED'),
    ('MTG002', 'CUST002', 'test2@example.com', 'REVOKED'),
    ('MTG003', 'CUST003', 'test3@example.com', 'PENDING');
GO

INSERT INTO dbo.MovingPromotionList
    (ConnectionKey, CustomerKey, PromotionType)
VALUES
    ('MOVE001', 'CUST001', 'STANDARD'),
    ('MOVE002', 'CUST002', 'PREMIUM'),
    ('MOVE003', 'CUST003', 'BASIC');
GO

INSERT INTO dbo.LINEIDLinkInfo
    (ConnectionKey, CustomerKey, LineUserId, LinkStatus)
VALUES
    ('LINE001', 'CUST001', 'U123456789', 'LINKED'),
    ('LINE002', 'CUST002', 'U987654321', 'UNLINKED'),
    ('LINE003', 'CUST003', 'U456789123', 'PENDING');
GO

INSERT INTO dbo.LIMSettlementBreakdownRepair
    (ConnectionKey, SettlementId, BreakdownType, Amount)
VALUES
    ('LIM001', 'SETTLE001', 'REPAIR', 1500.00),
    ('LIM002', 'SETTLE002', 'ADJUSTMENT', 2500.00),
    ('LIM003', 'SETTLE003', 'CORRECTION', 3500.00);
GO

INSERT INTO dbo.ElectricityContractThanks
    (ConnectionKey, CustomerKey, ContractId)
VALUES
    ('ELEC001', 'CUST001', 'CONTRACT001'),
    ('ELEC002', 'CUST002', 'CONTRACT002'),
    ('ELEC003', 'CUST003', 'CONTRACT003');
GO

INSERT INTO dbo.Cpkiyk
    (ConnectionKey, CustomerKey, ProcessType)
VALUES
    ('CPK001', 'CUST001', 'ANALYSIS'),
    ('CPK002', 'CUST002', 'PROCESSING'),
    ('CPK003', 'CUST003', 'VALIDATION');
GO

INSERT INTO dbo.KarteContractScoreInfo
    (ConnectionKey, CustomerKey, ContractScore)
VALUES
    ('KARTE001', 'CUST001', 85.5),
    ('KARTE002', 'CUST002', 92.3),
    ('KARTE003', 'CUST003', 78.8);
GO

INSERT INTO dbo.ActionPointRecentTransactionHistoryList
    (ConnectionKey, CustomerKey, TransactionType, TransactionAmount, Points)
VALUES
    ('APTX001', 'CUST001', 'PURCHASE', 5000.00, 50.0),
    ('APTX002', 'CUST002', 'REFUND', -1500.00, -15.0),
    ('APTX003', 'CUST003', 'BONUS', 2000.00, 100.0);
GO

PRINT 'SynapseTestDB初期化が完了しました';
