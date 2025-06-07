--------------------------------------------------------------------------
-- MA向けリスト連携（ガス機器・水まわり修理）
--------------------------------------------------------------------------

DECLARE @today_jst DATETIME;        -- 実行日
SET @today_jst = CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time')

DECLARE @targetDate varchar(8);     -- 抽出対象日=実行日-21日
SET @targetDate = format(DATEADD(DAY,-21,@today_jst),'yyyyMMdd');

------------------------------------------------------------------
--① 中間①に出力する
TRUNCATE TABLE [omni].[omni_ods_ma_trn_lim_settlement_breakdown_repair_temp];

INSERT INTO [omni].[omni_ods_ma_trn_lim_settlement_breakdown_repair_temp]

SELECT
      GMT_SET_NO                                                                                           --ガスメータ設置場所番号
     ,WORK_COMPLETION_DATE                                                                                 --作業完了年月日
     ,MAX(CASE WHEN WORK_SUBJECT_CATEGORY_CODE = '11' THEN '1'ELSE '0' END) AS GAS_APPLIANCES_REPAIR_FLAG  --ガス機器修理フラグ
     ,MAX(CASE WHEN WORK_SUBJECT_CATEGORY_CODE = '1A' THEN '1'ELSE '0' END) AS WET_AREA_REPAIR_FLAG        --水まわり修理フラグ
FROM [omni].[omni_ods_livalit_trn_lim_settlement_breakdown_repair]  --ＬＩＭ精算用故障修理データ
WHERE  GMT_SET_NO <> '00000000000'                      --ガスメータ設置場所番号が無い(00000000000)レコード以外抽出する
       AND TG_ACCEPTANCE_INSPECTION_DATE <> '00000000'  --TG検収年月日が無い(00000000)レコード以外を抽出する
       AND WORK_COMPLETION_DATE >= @targetDate          --実行日から21日以内のお客様を抽出する
       AND FIELD_WORK_DETAIL_CATEGORY_CODE = '0301'       --フィールド業務詳細区分コード が0301(一般機器)を抽出する
       AND WORK_SUBJECT_CATEGORY_CODE IN ('11','1A')    --作業対象区分コードが11(一般機器)か1A(水まわり修理(加入なし))を抽出する
GROUP BY GMT_SET_NO,WORK_COMPLETION_DATE 

;
------------------------------------------------------------------

-- 中間①のうち対象外のレコードを削除する
DELETE FROM [omni].[omni_ods_ma_trn_lim_settlement_breakdown_repair_temp]
WHERE GMT_SET_NO in(
    SELECT
         KEY_1X
    FROM (
        SELECT Bx
              ,SERVICE_KEY1 AS KEY_1X    -- ガスメータ設置場所番号
        FROM [omni].[omni_ods_cloak_trn_usageservice]
        WHERE TRANSFER_TYPE = '02'  --異動種別=02(提供中)
          AND SERVICE_KEY_TYPE1 = '001'
        UNION 
        SELECT Bx
              ,SERVICE_KEY2 AS KEY_1X    -- ガスメータ設置場所番号
        FROM [omni].[omni_ods_cloak_trn_usageservice]
        WHERE TRANSFER_TYPE = '02'  --異動種別=02(提供中)
          AND SERVICE_KEY_TYPE2 = '001'
        UNION 
        SELECT Bx
              ,SERVICE_KEY3 AS KEY_1X    -- ガスメータ設置場所番号
        FROM [omni].[omni_ods_cloak_trn_usageservice]
        WHERE TRANSFER_TYPE = '02'  --異動種別=02(提供中)
          AND SERVICE_KEY_TYPE3 = '001'
    ) tmp
    GROUP BY KEY_1X
    HAVING count(*)>1   -- 1x:Bx=1:n -- 出力対象外
)
;

------------------------------------------------------------------
--② MA向けに出力する
DECLARE @outputDT varchar(20);   -- IF出力日時
SET @outputDT = format(CONVERT(DATETIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time'),'yyyy/MM/dd HH:mm:ss');

SELECT      
     Bx                            --Bx
    ,myTGID AS mTGID               --myTGID
    ,INDEX_ID                      --インデックス番号
    ,GMT_SET_NO                    --ガスメータ設置場所番号
    ,WORK_COMPLETION_DATE          --作業完了年月日
    ,GAS_APPLIANCES_REPAIR_FLAG    --ガス機器修理フラグ
    ,WET_AREA_REPAIR_FLAG          --水まわり修理フラグ
    ,@outputDT AS OUTPUT_DATETIME  --IF出力日時
FROM(
    SELECT
           us.Bx                           --Bx
          ,us.myTGID                       --myTGID
          ,us.INDEX_ID                     --インデックス番号
          ,rep.GMT_SET_NO                  --ガスメータ設置場所番号
          ,WORK_COMPLETION_DATE            --作業完了年月日
          ,rep.GAS_APPLIANCES_REPAIR_FLAG  --ガス機器修理フラグ
          ,rep.WET_AREA_REPAIR_FLAG        --水まわり修理フラグ
          ,ROW_NUMBER() OVER(PARTITION BY us.Bx,rep.GMT_SET_NO,rep.WORK_COMPLETION_DATE  ORDER BY OUTPUT_DATE desc)  as rownum -- 同一「Bx,ガスメータ設置場所番号,作業完了年月日」に異なる「INDEX_ID」が存在する場合は「OUTPUT_DATE」が新しいレコードを抽出する
    FROM [omni].[omni_ods_ma_trn_lim_settlement_breakdown_repair_temp] rep  --中間①
    INNER JOIN [omni].[omni_ods_cloak_trn_usageservice] us                  -- 利用サービス
            ON rep.GMT_SET_NO in(us.SERVICE_KEY1,us.SERVICE_KEY2,us.SERVICE_KEY3)
           AND TRANSFER_TYPE = '02'  --異動種別が02(提供中)のレコードを抽出する
)tmp
WHERE rownum = 1
;
