----------------------------------------------------------------------
-- mTG会員マスタ データ処理
-- Source: Externalized from ARM template
-- Purpose: mTG会員の各種マスタデータ処理
-- 
-- Main Features:
-- - LINEID連携累積情報処理
-- - 会員種別データ処理
-- - 契約中フラグ管理
-- - 全量契約中フラグ処理
-- - 段階的な中間テーブル処理
-- 
-- Processing Components:
-- (a) 会員基本情報処理
-- (b) 会員種別処理  
-- (c) LINEID連携累積情報処理
-- (d) 契約中フラグ処理
-- 
-- History:
-- 2025/05/16 新規作成
-- 2024/12/31 ARMテンプレートから外部化
----------------------------------------------------------------------

----------------------------------------------------------------------
-- (a) 会員種別 → mTG会員_会員種別_temp
----------------------------------------------------------------------
TRUNCATE TABLE [omni].[omni_ods_mytginfo_trn_mtgmaster_member_type_temp];

INSERT INTO [omni].[omni_ods_mytginfo_trn_mtgmaster_member_type_temp]
SELECT
    ID_NO, -- 会員ID
    MAX(ID_SYB_CD) as ID_SYB_CD
-- 会員種別
FROM [omni].[omni_ods_mytginfo_trn_cpsyb]
-- 会員種別
GROUP BY ID_NO;

----------------------------------------------------------------------
-- (b) LINEID連携累積情報 → mTG会員_LINEID連携累積情報_temp
----------------------------------------------------------------------
TRUNCATE TABLE [omni].[omni_ods_mytginfo_trn_mtgmaster_lineid_linkageInfo_temp];

INSERT INTO [omni].[omni_ods_mytginfo_trn_mtgmaster_lineid_linkageInfo_temp]
SELECT
    ID_NO, -- 会員ID
    MAX(CASE
            WHEN KJ_FLG = '0' THEN LINE_U_ID
            ELSE NULL
        END
    ) AS LINE_U_ID, -- LINE ユーザーID
    MAX(CASE
            WHEN KJ_FLG = '0' THEN LINE_LINK_DATE
            ELSE NULL
        END
    ) AS LINE_LINK_DATE, -- LINE連携日
    COUNT(CASE WHEN KJ_FLG = '0' THEN 1 ELSE NULL END) AS LINK_COUNT
-- 連携回数
FROM [omni].[omni_ods_mytginfo_trn_line_linkage]
-- LINEID連携テーブル
WHERE STATUS = '有効'
GROUP BY ID_NO;

----------------------------------------------------------------------
-- (c) mTG会員_全量契約中フラグ_tempの全レコードを削除
----------------------------------------------------------------------
TRUNCATE TABLE [omni].[omni_odm_mytginfo_trn_mtgmaster_full_under_contract_flag_temp];

----------------------------------------------------------------------
-- (d) 統合処理：各種マスタ情報の結合
----------------------------------------------------------------------
SELECT
    mt.ID_NO,
    mt.ID_SYB_CD,
    li.LINE_U_ID,
    li.LINE_LINK_DATE,
    li.LINK_COUNT,
    cf.CONTRACT_FLAG,
    FORMAT(GETDATE(), 'yyyy/MM/dd HH:mm:ss') AS PROCESS_DATETIME
FROM [omni].[omni_ods_mytginfo_trn_mtgmaster_member_type_temp] mt
    LEFT JOIN [omni].[omni_ods_mytginfo_trn_mtgmaster_lineid_linkageInfo_temp] li
    ON mt.ID_NO = li.ID_NO
    LEFT JOIN [omni].[contract_flag_data] cf
    ON mt.ID_NO = cf.MEMBER_ID
WHERE mt.ID_NO IS NOT NULL
ORDER BY mt.ID_NO;
