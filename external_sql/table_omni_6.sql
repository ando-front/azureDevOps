----------------------------------------------------------------------
-- omni.顧客DNA_tempからomni.顧客DNAへ全量コピーする
-- 
-- 2023/11/16　新規作成
-- 2024/05/02　削除対象カラムをNULLに置き換えとする暫定対応
-- 2024/08/07　Maketingスキーマ.顧客DNA_推定DMからomni.顧客DNA_推定DMに参照先を変更
-- 2024/12/13　レイアウト変更に伴う、カラム追加、削除
----------------------------------------------------------------------

-- Maketingスキーマのomni.顧客DNA_推定DMから、omniスキーマの顧客DNA_tempにデータを全量コピー
SELECT [顧客キー]                                        AS CLIENT_KEY
      ,[LIV0EU_ガスメータ設置場所番号＿１ｘ]             AS LIV0EU_1X
      ,[LIV0EU_使用契約番号＿４ｘ]                       AS LIV0EU_4X
      ,[LIV0EU_カスタマ番号＿８ｘ]                       AS LIV0EU_8X
      ,[電力CIS契約情報_お客様登録番号_3x]               AS DNRK_CIS_CNTRCT_3X
      ,[電力CIS契約情報_電灯_電力契約番号]               AS DNRK_CIS_CNTRCT_DT_SA_ID
      ,[電力CIS契約情報_動力_電力契約番号]               AS DNRK_CIS_CNTRCT_DR_SA_ID
      ,[LIV0EU_建物番号]                                 AS LIV0EU_BLD_NO
      ,[顧客セグメント]                                  AS CLIENT_SG
--    2024/12/13 カラム「PV推定スコア」削除
      ,[PVスコア_電力]                                   AS PV_SCORE_POWER   -- 2024/12/13 追加
      ,[PVスコアランク_電力]                             AS PV_SCORE_RANK_POWER   -- 2024/12/13 追加
      ,[PVスコア_ガス]                                   AS PV_SCORE_GAS   -- 2024/12/13 追加
      ,[推定PV保有フラグ_ガス]                           AS ESTIMATED_PV_HOLDINGS_FLAG_GAS   -- 2024/12/13 追加
      ,[デモグラフィック情報_世帯主年代_0-29歳]          AS DG_STN_AGE_0_29
      ,[デモグラフィック情報_世帯主年代_30-39歳]         AS DG_STN_AGE_30_39
      ,[デモグラフィック情報_世帯主年代_40-49歳]         AS DG_STN_AGE_40_49
      ,[デモグラフィック情報_世帯主年代_50-59歳]         AS DG_STN_AGE_50_59
      ,[デモグラフィック情報_世帯主年代_60-69歳]         AS DG_STN_AGE_60_69
      ,[デモグラフィック情報_世帯主年代_70-99歳]         AS DG_STN_AGE_70_99
      ,[デモグラフィック情報_推定世帯主年代]             AS DG_SITI_STNS_AGE
      ,[デモグラフィック情報_世帯人数_1人世帯]           AS DG_ST_NUM_1
      ,[デモグラフィック情報_世帯人数_2人世帯]           AS DG_ST_NUM_2
      ,[デモグラフィック情報_世帯人数_3人世帯]           AS DG_ST_NUM_3
      ,[デモグラフィック情報_世帯人数_4人以上世帯]       AS DG_ST_NUM_4
      ,[デモグラフィック情報_推定世帯人数]               AS DG_SITI_ST_NUM
      ,[デモグラフィック情報_未就学児の有無]             AS DG_MSGJ_UM
      ,[デモグラフィック情報_小中学生の有無]             AS DG_SCGS_UM
      ,[デモグラフィック情報_高大専門学生の有無]         AS DG_KDSNMNGS_UM
      ,[デモグラフィック情報_その他の子供の有無]         AS DG_SNTKDM_UM
      ,[デモグラフィック情報_世帯主または配偶者の親の有無] AS DG_STN_OR_HGS_PRNT
      ,[デモグラフィック情報_ペットの有無]               AS DG_PET
      ,[デモグラフィック情報_世帯年収_400万円未満]       AS DG_STNS_4MLON_LESS
      ,[デモグラフィック情報_世帯年収_1000万円以上]      AS DG_STNS_10MLON_OVER
      ,[デモグラフィック情報_推定電力使用量]             AS DG_SITI_DNRKSYO
      ,[ライフスタイル_電気使用傾向_電灯_大分類]         AS LFS_DNRKSY_DT_DBRI
      ,[ライフスタイル_電気使用傾向_電灯_小分類]         AS LFS_DNRKSY_DT_SBRI
      ,[ライフスタイル_電気使用傾向_動力_大分類]         AS LFS_DNRKSY_DR_DBRI
      ,[ライフスタイル_電気使用傾向_動力_小分類]         AS LFS_DNRKSY_DR_SBRI
      ,[ライフスタイル_接電スコア]                       AS LFS_STDNSCR
      ,[ライフスタイル_家を空けやすい]                   AS LFS_ESY_LVE_HOME
      ,[ライフスタイル_朝型]                             AS LFS_MONG_TYP
      ,[ライフスタイル_夜型]                             AS LFS_NGT_TYP
      ,[ライフスタイル_共働き]                           AS LFS_DUAL_TYP
      ,[ライフスタイル_在宅勤務または自宅店舗経営]       AS LFS_ZITK_OR_JTKIEI
      ,[利用外部サービス_ハウスクリーニング]             AS RYGSV_HSCLNG
      ,[利用外部サービス_家電・機器修理保証サービス]     AS RYGSV_KNKSR_SV
      ,[購買傾向_低価格志向]                             AS KBKK_TIKKSK
      ,[購買傾向_高級志向・見栄型]                       AS KBKK_KKYSK_MIE
      ,[購買傾向_高級志向・お金持ち型]                   AS KBKK_KKYSK_MNY
      ,[支払心の痛さ_リフォーム]                         AS SHRI_KKRITS_RFM
      ,[支払心の痛さ_ハウスクリーニング]                 AS SHRI_KKRITS_HSCLNG
      ,[支払心の痛さ_家電・機器修理保証サービス]         AS SHRI_KKRITS_KNKSR_SV
      ,[支払心の痛さ_子どもの見守りサービス]             AS SHRI_KKRITS_KDMMMMR_SV
      ,[支払心の痛さ_ホームセキュリティー]               AS SHRI_KKRITS_HMSCRTY
      ,[支払心の痛さ_HEMS]                               AS SHRI_PAIN_HEMS
      ,[転居予測_2か月以内]                              AS TNKYSK_2MONTH   -- 2024/12/13 追加
      ,[転居予測_転居予測_1年以内]                       AS TNKYSK_1YEAR
      ,[価値観_電気切替時重視点_安定志向]                AS KTKN_DKKRKEJUSTN_ANTISKO
      ,[価値観_電気切替時重視点_環境志向]                AS KTKN_DKKRKEJUSTN_KNKSKO
      ,[価値観_電気切替時重視点_ポイント愛好家]          AS KTKN_DKKRKEJUSTN_POINT
      ,[価値観_電気切替時重視点_価格志向・無関心]        AS KTKN_DKKRKEJUSTN_KKSKO_MKS
      ,[価値観_電気切替時重視点_よくばりさん]            AS KTKN_DKKRKEJUSTN_YKBR
      ,[価値観_推定電気切替時重視点]                     AS KTKN_SITI_DKKRKEJUSTN
      ,[価値観_電気切替時情報源_DM]                      AS KTKN_DKKRKEJHGN_DM
      ,[価値観_電気切替時情報源_Web]                     AS KTKN_DKKRKEJHGN_WEB
      ,[価値観_電気切替時情報源_マス]                    AS KTKN_DKKRKEJHGN_MS
      ,[価値観_電気切替時情報源_営業マン]                AS KTKN_DKKRKEJHGN_EGYMN
      ,[価値観_電気切替時情報源_知人友人]                AS KTKN_DKKRKEJHGN_CJNYJN
      ,[価値観_推定電気切替時情報源]                     AS KTKN_SITIELEKRKEJHGN
      ,[価値観_健康意識]                                 AS KTKN_KNKISK
      ,[価値観_ダイエット意識]                           AS KTKN_DIETISK
      ,[価値観_疲労回復]                                 AS KTKN_HRKHK
      ,[価値観_レトルト好き]                             AS KTKN_RTRTSK
      ,[価値観_高品質食品]                               AS KTKN_KHSSYKHN
      ,[価値観_料理好き]                                 AS KTKN_RYRSK
      ,[価値観_外食好き]                                 AS KTKN_GSSK
      ,[商材_ガス脱落スコア_直近3ヶ月]                   AS SYZI_GS_DTRKSCR_MONTH03
      ,[商材_ガス脱落スコア_直近1年]                     AS SYZI_GS_DTRKSCR_YEAR01
      ,[商材_電気脱落スコア_直近3ヶ月]                   AS SYZI_DK_DTRKSCR_MONTH03
      ,[商材_電気脱落スコア_直近1年]                     AS SYZI_DK_DTRKSCR_YEAR01
      ,[商材_脱落スコア算出モデル種別]                   AS SYZI_DTRKSCR_SNSTMDR_SHBT
      ,[商材_スイッチバックスコア]                       AS SYZI_SWCBK_SCR
      ,[商材_総合設備_テーブルコンロ]                    AS SYZI_SGSTB_TBLKNR
      ,[商材_総合設備_ビルトインコンロ]                  AS SYZI_SGSTB_BLTINKNR
      ,[商材_総合設備_TES]                               AS SYZI_SGSTB_TES
      ,[商材_総合設備_風呂給]                            AS SYZI_SGSTB_HRK
      ,[商材_総合設備_大湯]                              AS SYZI_SGSTB_OY
      ,[商材_総合設備_ファンヒーター]                    AS SYZI_SGSTB_FNHT
      ,[商材_総合設備_ガス式衣類乾燥機]                  AS SYZI_SGSTB_GSKNSK
      ,[商材_サービス_くらし見守りサービス]              AS SYZI_SV_KRSMMMR_SV
      ,[商材_サービス_ガス機器SS]                        AS SYZI_SV_GSKK_SS
      ,[DM施策_電気申込書DMスコア]                       AS DM_DKMSKM_DM_SCR
      ,[DM施策_電気WebDMスコア]                          AS DM_DKWEB_DM_SCR
      ,[DM施策_ガス機器SSDMスコア]                       AS DM_GSKK_SSDM_SCR
      ,[チャネル_電気申込経路WEB]                        AS CNL_DKMSKMKR_WEB
      ,[チャネル_メール開封]                             AS CNL_MLKH
      ,[チャネル_メール添付URLクリック]                  AS CNL_MLTMPURL_CLC
      ,[商材_電気WEB]                                    AS SYZI_DK_WEB
      ,[商材_総合設備_ビルトインコンロ_買替有無]         AS SYZI_SGSTB_BLTINKNR_KIKE
      ,[商材_総合設備_ビルトインコンロ_プレミアム]       AS SYZI_SGSTB_BLTINKNR_PRM
      ,[商材_総合設備_ビルトインコンロ_ハイグレード]     AS SYZI_SGSTB_BLTINKNR_HIGRD
      ,[商材_総合設備_ビルトインコンロ_スタンダード]     AS SYZI_SGSTB_BLTINKNR_STD
      ,[レコード作成日時]                                AS REC_REG_YMD
      ,[レコード更新日時]                                AS REC_UPD_YMD

  FROM [omni].[顧客DNA_推定DM]
  ;