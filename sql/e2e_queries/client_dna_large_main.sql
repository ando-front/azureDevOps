----------------------------------------------------------------------
-- 大規模顧客DNAクエリ（30K+ 文字のクエリ用）
-- Source: Externalized from ARM template large queries
-- Purpose: 顧客DNA_tempから顧客DNAへの全量コピー処理
-- 
-- Maketingスキーマのomni.顧客DNA_推定DMから、omniスキーマの顧客DNA_tempにデータを全量コピー
-- 大規模なSELECTクエリ（533カラム以上）を処理
-- 
-- Features:
-- - 全カラムマッピング（533+ columns）
-- - レイアウト変更対応
-- - 削除対象カラムのNULL置き換え
-- - データ型変換処理
-- 
-- History:
-- 2023/11/16 新規作成
-- 2024/05/02 削除対象カラムをNULLに置き換えとする暫定対応
-- 2024/08/07 Maketingスキーマ参照先変更
-- 2024/12/13 レイアウト変更に伴う、カラム追加、削除
-- 2024/12/31 ARMテンプレートから外部化
----------------------------------------------------------------------

-- Placeholder for large client DNA query
-- This file will contain the externalized large SQL queries (30K+ characters)
-- that handle comprehensive customer DNA data processing from Marketing schema
-- to omni schema with full column mapping and data transformation logic.

SELECT [顧客キー] AS CLIENT_KEY
      , [LIV0EU_ガスメータ設置場所番号＿１ｘ] AS LIV0EU_1X
      , [LIV0EU_カスタマ番号＿８ｘ] AS LIV0EU_8X
-- Add remaining 530+ columns here when externalized from ARM templates
-- This includes comprehensive customer data mapping, gas usage information,
-- electric contract details, TES system data, alarm device information,
-- web history tracking, and marketing analytics data
FROM [Marketing].[omni].[顧客DNA_推定DM]
WHERE [処理日] = @processing_date;
