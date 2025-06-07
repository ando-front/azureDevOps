--------------------------------------------------------------------------
-- MA向けリスト連携（電気契約Thanksシナリオ）
-- 2023/07 新規作成
-- 2024/10 出力カラム「契約タイプ」追加、抽出条件変更、mTGID取得「本人特定契約」に変更
--------------------------------------------------------------------------

DECLARE @today_jst DATETIME;        -- 実行日
SET @today_jst=CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time')

DECLARE @targetDate varchar(8);     -- 抽出対象日=実行日-1日
SET @targetDate=format(DATEADD(DAY,-1,@today_jst),'yyyyMMdd');


------------------------------------------------------------------
----① 中間①に出力する
TRUNCATE TABLE [omni].[omni_ods_ma_trn_new_electricity_contract_temp];

INSERT INTO [omni].[omni_ods_ma_trn_new_electricity_contract_temp]
SELECT distinct
     c.KEY_1X                     -- ガスメータ設置場所番号
    ,c.KEY_3X                     -- お客さま登録番号
    ,c.GAS_4X                     -- ガス使用契約番号
    ,c.EL_4X                      -- 電力使用契約番号
    ,c.KEY_8X                     -- カスタマ番号
    ,c.SUPPLY_POINT_ID            -- 供給地点特定番号
    ,c.SA_ID                      -- 電力契約番号
    ,c.GAS_START_DT as START_DATE -- 使用開始日　契約
    ,o.APPLICATION_DT             -- 申込日　指図
    ,c.SA_TYPE_CD                 -- 契約タイプ　2024/10 追加
FROM [omni].[omni_ods_epcis_trn_contract] c              -- 電力ＣＩＳ契約情報
INNER JOIN [omni].[omni_ods_epcis_trn_operation_order] o -- 電力ＣＩＳ指図情報
        ON c.SA_ID=o.SA_ID                                       -- 「電力契約番号」が一致
            AND o.OO_STATUS_FLG='800' AND o.OO_TYPE_CD='ESST'    -- 開始作業完了

WHERE c.UPD_DIVISION='A'               -- 更新区分=A:追加

    AND c.REC_REG_YMD=@targetDate      -- 登録日(ジョブの実行日)=ADFの実行日-1日

    -- and c.SA_TYPE_CD in('ESLVY','ESOLY')     -- 契約タイプ=合算　削除 2024/10

    AND EXISTS (
        SELECT 1 FROM (
            SELECT
                count(distinct ct.SA_ID) as count    -- 「電力契約番号」の数
            FROM [omni].[omni_ods_epcis_trn_contract] ct   -- 電力ＣＩＳ契約情報
            WHERE ct.SUPPLY_POINT_ID=c.SUPPLY_POINT_ID    -- 「供給地点特定番号」が一致し、「お客さま登録番号、お客さま名、電話番号」のいずれか一致
                AND ( ct.KEY_3X=c.KEY_3X                  -- お客さま登録番号
                    OR ct.PER_NAME=c.PER_NAME             -- お客さま名＿漢字
                    OR ct.PER_NAME_KANA=c.PER_NAME_KANA   -- お客さま名＿カナ
                    OR ct.PHONE=c.PHONE                   -- 電話番号
                )
        )tmp
        WHERE count=1
    )  -- 「供給地点特定番号、お客さま登録番号」ごとの最初の契約
;


--------------------------------------------------------------------
----② 中間②に出力する　削除 2024/10


--------------------------------------------------------------------
----③ 電気契約Thanks履歴に出力する
DECLARE @runYMD varchar(8);   -- 実行日
SET @runYMD=format(@today_jst,'yyyyMMdd');

INSERT INTO [omni].[omni_ods_ma_trn_electricity_thanks_history]
SELECT distinct
    --Bx                       -- Bx　削除 2024/10
    --MTGID                    -- mTGID　削除 2024/10
    --INDEX_ID                 -- インデックス番号　削除 2024/10
     KEY_1X                   -- ガスメータ設置場所番号
    ,KEY_3X                   -- お客さま登録番号
    ,GAS_4X                   -- ガス使用契約番号
    ,EL_4X                    -- 電力使用契約番号
    ,KEY_8X                   -- カスタマ番号
    ,SUPPLY_POINT_ID          -- 供給地点特定番号
    ,SA_ID                    -- 電力契約番号
    ,START_DATE               -- 使用開始日
    ,APPLICATION_DT           -- 申込日
    --,USAGESERVICE_OUTPUT_DATE -- 利用サービス出力日　削除 2024/10
    --,''                       -- 初回請求確定出力日　削除 2024/10
    ,SA_TYPE_CD               -- 契約タイプ　2024/10 追加
	,@runYMD
FROM [omni].[omni_ods_ma_trn_new_electricity_contract_temp] ct         -- 中間①
WHERE not EXISTS(
    SELECT 1 FROM [omni].[omni_ods_ma_trn_electricity_thanks_history] th -- 電気契約Thanks履歴
    WHERE ct.SUPPLY_POINT_ID=th.SUPPLY_POINT_ID AND ct.KEY_3X=th.KEY_3X
)  -- 電気契約Thanks履歴に同一の「供給地点特定番号、お客さま登録番号」が存在しない場合に挿入する
;


------------------------------------------------------------------
--④ MA向けに出力する
DECLARE @targetDate30 varchar(8);   -- 抽出対象日=実行日-30日
SET @targetDate30=format(DATEADD(DAY,-30,@today_jst),'yyyyMMdd');

DECLARE @targetDate25 varchar(8);   -- 抽出対象日=実行日-25日
SET @targetDate25=format(DATEADD(DAY,-25,@today_jst),'yyyyMMdd');

DECLARE @outputDT varchar(20);   -- IF出力日時
SET @outputDT=format(CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time'),'yyyy/MM/dd HH:mm:ss');

SELECT
    --Bx                        -- Bx　削除 2024/10
     MTGID                    -- mTGID
    --,INDEX_ID                 -- インデックス番号　削除 2024/10
    ,KEY_1X                   -- ガスメータ設置場所番号
    ,KEY_3X                   -- お客さま登録番号
    ,GAS_4X                   -- ガス使用契約番号
    ,EL_4X                    -- 電力使用契約番号
    ,KEY_8X                   -- カスタマ番号
    ,SUPPLY_POINT_ID          -- 供給地点特定番号
    ,SA_ID                    -- 電力契約番号
    ,START_DATE               -- 使用開始日
    ,APPLICATION_DT           -- 申込日
    ,SA_TYPE_CD               -- 契約タイプ　追加 2024/10
    ,TRKSBTCD                 -- 種別コード　追加 2024/10
    ,@outputDT as OUTPUT_DATETIME  -- IF出力日時
FROM(
    SELECT
         cpkiyk.MTGID
        ,txhis.KEY_1X                   -- ガスメータ設置場所番号
        ,txhis.KEY_3X                   -- お客さま登録番号
        ,txhis.GAS_4X                   -- ガス使用契約番号
        ,txhis.EL_4X                    -- 電力使用契約番号
        ,txhis.KEY_8X                   -- カスタマ番号
        ,txhis.SUPPLY_POINT_ID          -- 供給地点特定番号
        ,txhis.SA_ID                    -- 電力契約番号
        ,txhis.START_DATE               -- 使用開始日
        ,txhis.APPLICATION_DT           -- 申込日
        ,txhis.SA_TYPE_CD               -- 契約タイプ　追加 2024/10
        ,cpkiyk.TRKSBTCD                -- 種別コード　追加 2024/10
--        ,ROW_NUMBER() OVER(PARTITION BY cpkiyk.MTGID ORDER BY cpkiyk.CSV_YMD desc)  as rownum   -- MTGIDが一意になるようにする　削除 2024/10
    FROM [omni].[omni_ods_ma_trn_electricity_thanks_history] txhis -- 電気契約Thanks履歴
    INNER JOIN [omni].[omni_ods_mytginfo_trn_cpkiyk] cpkiyk        -- 本人特定契約　変更 2024/10
           ON cpkiyk.CST_REG_NO=txhis.KEY_3X AND cpkiyk.TAIKAI_FLAG=0
    WHERE @targetDate30<=txhis.START_DATE AND txhis.START_DATE<=@targetDate25  -- 実行日-30日<=使用開始日<=実行日-25日
)tmp
;
