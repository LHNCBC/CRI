# Databricks notebook source
#timeout
#ten min=600
#one hour=3600
#10 hours=36000
#100 hours=360000

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "True");
spark.conf.set("spark.sql.shuffle.partitions",8000);

# COMMAND ----------

# MAGIC %sql select * from <write_bucket>.log  order by time desc ;

# COMMAND ----------

dbutils.notebook.run(
  "/Users/<user_id>/01_DDL_CDM/01_ddl",
  timeout_seconds = 600,
  arguments = {"job_id": "2001"}
)
  

# COMMAND ----------

# MAGIC %run /Users/<user_id>/03_Transform_Load/A1_location $job_id='200';

# COMMAND ----------

# MAGIC %run /Users/<user_id>/03_Transform_Load/A2_care_site $job_id='200;

# COMMAND ----------

# DDL for non-annual tables
dbutils.notebook.run(
  "/Users/<user_id>/02_Extract/02_Extract_DDL",
  timeout_seconds = 600000,
  arguments = {"job_id": "2001"}
)

# COMMAND ----------

#extract base and dates
dbutils.notebook.run(
"/Users/<user_id>/02_Extract/02_demo_all_years",
  timeout_seconds=360000,
  arguments={"job_id":"2001",
             
"demog_elig_base":"AGE,   BENE_ID,  BENE_STATE_CD,  BIRTH_DT,  DEATH_DT,  ETHNCTY_CD,  RACE_ETHNCTY_CD,  RFRNC_YR,  SEX_CD,  STATE_CD,  bene_cnty_cd,  bene_zip_cd",
"demog_elig_dates":"BENE_ID,  ENRLMT_END_DT,  ENRLMT_START_DT,  RFRNC_YR,  STATE_CD"
 })

# COMMAND ----------

#extract 2014
dbutils.notebook.run(
  "/Users/<user_id>/02_Extract/02_2014",
  timeout_seconds = 360000,
  arguments = {"job_id": "2001",

"inpatient_header":" ADMSN_TYPE_CD,  ADMTG_DGNS_CD,  BENE_ID,  BLG_PRVDR_NPI,  BLG_PRVDR_SPCLTY_CD,  CLM_ID, DGNS_CD_1,  DGNS_CD_10,  DGNS_CD_11,  DGNS_CD_12,  DGNS_CD_2,  DGNS_CD_3,  DGNS_CD_4,  DGNS_CD_5,  DGNS_CD_6,  DGNS_CD_7,  DGNS_CD_8,  DGNS_CD_9, DRG_CD,  DSCHRG_DT, HOSP_TYPE_CD, PRCDR_CD_1,  PRCDR_CD_2, PRCDR_CD_3,  PRCDR_CD_4, PRCDR_CD_5,  PRCDR_CD_6, PRCDR_CD_DT_1,  PRCDR_CD_DT_2,  PRCDR_CD_DT_3,  PRCDR_CD_DT_4,  PRCDR_CD_DT_5,  PRCDR_CD_DT_6, PRCDR_CD_SYS_1,  PRCDR_CD_SYS_2,  PRCDR_CD_SYS_3,  PRCDR_CD_SYS_4,  PRCDR_CD_SYS_5,  PRCDR_CD_SYS_6, SRVC_BGN_DT,  SRVC_END_DT,STATE_CD, '2014' as year,'ip_h' as file",

"inpatient_line":"BENE_ID,CLM_ID,LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,NDC,NDC_QTY,NDC_UOM_CD,PRVDR_FAC_TYPE_CD,SRVC_PRVDR_NPI,SRVC_PRVDR_SPCLTY_CD,STATE_CD,TOS_CD,'2014' as year,'ip_l' as file",

"other_services_header":"BENE_ID,BLG_PRVDR_NPI,BLG_PRVDR_SPCLTY_CD,CLM_ID,DGNS_CD_1,DGNS_CD_2,POS_CD,SRVC_BGN_DT,SRVC_END_DT,STATE_CD, '2014' as year,'ot_h' as file",

               "other_services_line":"BENE_ID,CLM_ID,HCBS_SRVC_CD,LINE_PRCDR_CD,LINE_PRCDR_CD_SYS,LINE_PRCDR_MDFR_CD_1,LINE_PRCDR_MDFR_CD_2,LINE_PRCDR_MDFR_CD_3,LINE_PRCDR_MDFR_CD_4,LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,NDC,NDC_QTY,NDC_UOM_CD,SRVC_PRVDR_NPI,SRVC_PRVDR_SPCLTY_CD,STATE_CD,TOOTH_NUM,TOOTH_SRFC_CD,TOS_CD, '2014' as year,'ot_l' as file",

"long_term_header":"BLG_PRVDR_NPI,clm_id,state_cd,bene_id,srvc_bgn_dt,srvc_end_dt,ADMTG_DGNS_CD,dgns_cd_1,dgns_cd_2,dgns_cd_3,dgns_cd_4,dgns_cd_5,BLG_PRVDR_SPCLTY_CD,'2014' as year,'lt_h' as file",

"long_term_line":"clm_id,state_cd,SRVC_PRVDR_NPI,bene_id,ndc_uom_cd,ndc_qty,line_srvc_bgn_dt,line_srvc_end_dt,ndc,SRVC_PRVDR_SPCLTY_CD,'2014' as year,'lt_l' as file",

"rx_header":"BENE_ID,BLG_PRVDR_NPI,DSPNSNG_PRVDR_NPI,PRSCRBNG_PRVDR_NPI,RX_FILL_DT,STATE_CD,clm_id,BLG_PRVDR_SPCLTY_CD, '2014' as year,'rx_h' as file",     

"rx_line":"bene_id,state_cd,clm_id,ndc,dosage_form_cd,days_supply,new_rx_refill_num,ndc_qty,ndc_uom_cd,'2014' as year,'rx_l' as file"
 })

# COMMAND ----------

#extract 2015
dbutils.notebook.run(
  "/Users/<user_id>/02_Extract/02_2015",
  timeout_seconds = 360000,
  arguments = {"job_id": "2001",
"demog_elig_base":"AGE,   BENE_ID,  BENE_STATE_CD,  BIRTH_DT,  DEATH_DT,  ETHNCTY_CD,  RACE_ETHNCTY_CD,  RFRNC_YR,  SEX_CD,  STATE_CD,  bene_cnty_cd,  bene_zip_cd",
"demog_elig_dates":"BENE_ID,  ENRLMT_END_DT,  ENRLMT_START_DT,  RFRNC_YR,  STATE_CD",
"inpatient_header":" ADMSN_TYPE_CD,  ADMTG_DGNS_CD,  BENE_ID,  BLG_PRVDR_NPI,  BLG_PRVDR_SPCLTY_CD,  CLM_ID, DGNS_CD_1,  DGNS_CD_10,  DGNS_CD_11,  DGNS_CD_12,  DGNS_CD_2,  DGNS_CD_3,  DGNS_CD_4,  DGNS_CD_5,  DGNS_CD_6,  DGNS_CD_7,  DGNS_CD_8,  DGNS_CD_9, DRG_CD,  DSCHRG_DT, HOSP_TYPE_CD, PRCDR_CD_1,  PRCDR_CD_2, PRCDR_CD_3,  PRCDR_CD_4, PRCDR_CD_5,  PRCDR_CD_6, PRCDR_CD_DT_1,  PRCDR_CD_DT_2,  PRCDR_CD_DT_3,  PRCDR_CD_DT_4,  PRCDR_CD_DT_5,  PRCDR_CD_DT_6, PRCDR_CD_SYS_1,  PRCDR_CD_SYS_2,  PRCDR_CD_SYS_3,  PRCDR_CD_SYS_4,  PRCDR_CD_SYS_5,  PRCDR_CD_SYS_6, SRVC_BGN_DT,  SRVC_END_DT,STATE_CD, '2015' as year,'ip_h' as file",
"inpatient_line":"BENE_ID,CLM_ID,LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,NDC,NDC_QTY,NDC_UOM_CD,PRVDR_FAC_TYPE_CD,SRVC_PRVDR_NPI,SRVC_PRVDR_SPCLTY_CD,STATE_CD,TOS_CD,'2015' as year,'ip_l' as file",
"other_services_header":"BENE_ID,BLG_PRVDR_NPI,BLG_PRVDR_SPCLTY_CD,CLM_ID,DGNS_CD_1,DGNS_CD_2,POS_CD,SRVC_BGN_DT,SRVC_END_DT,STATE_CD, '2015' as year,'ot_h' as file",
"other_services_line":"BENE_ID,CLM_ID,HCBS_SRVC_CD,LINE_PRCDR_CD,LINE_PRCDR_CD_SYS,LINE_PRCDR_MDFR_CD_1,LINE_PRCDR_MDFR_CD_2,LINE_PRCDR_MDFR_CD_3,LINE_PRCDR_MDFR_CD_4,LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,NDC,NDC_QTY,NDC_UOM_CD,SRVC_PRVDR_NPI,SRVC_PRVDR_SPCLTY_CD,STATE_CD,TOOTH_NUM,TOOTH_SRFC_CD,TOS_CD, '2015' as year,'ot_l' as file",
"long_term_header":"BLG_PRVDR_NPI,clm_id,state_cd,bene_id,srvc_bgn_dt,srvc_end_dt,ADMTG_DGNS_CD,dgns_cd_1,dgns_cd_2,dgns_cd_3,dgns_cd_4,dgns_cd_5,BLG_PRVDR_SPCLTY_CD,'2015' as year,'lt_h' as file",
"long_term_line":"clm_id,state_cd,SRVC_PRVDR_NPI,bene_id,ndc_uom_cd,ndc_qty,line_srvc_bgn_dt,line_srvc_end_dt,ndc,SRVC_PRVDR_SPCLTY_CD,'2015' as year,'lt_l' as file",
"rx_header":"BENE_ID,BLG_PRVDR_NPI,DSPNSNG_PRVDR_NPI,PRSCRBNG_PRVDR_NPI,RX_FILL_DT,STATE_CD,clm_id,BLG_PRVDR_SPCLTY_CD, '2015' as year,'rx_h' as file",     
"rx_line":"bene_id,state_cd,clm_id,ndc,dosage_form_cd,days_supply,new_rx_refill_num,ndc_qty,ndc_uom_cd,'2015' as year,'rx_l' as file"}
)

# COMMAND ----------

#extract 2016
dbutils.notebook.run(
  "/Users/<user_id>/02_Extract/02_2016",
  timeout_seconds = 360000,
  arguments = {"job_id": "2001",
"demog_elig_base":"AGE,   BENE_ID,  BENE_STATE_CD,  BIRTH_DT,  DEATH_DT,  ETHNCTY_CD,  RACE_ETHNCTY_CD,  RFRNC_YR,  SEX_CD,  STATE_CD,  bene_cnty_cd,  bene_zip_cd",
"demog_elig_dates":"BENE_ID,  ENRLMT_END_DT,  ENRLMT_START_DT,  RFRNC_YR,  STATE_CD",
"inpatient_header":" ADMSN_TYPE_CD,  ADMTG_DGNS_CD,  BENE_ID,  BLG_PRVDR_NPI,  BLG_PRVDR_SPCLTY_CD,  CLM_ID, DGNS_CD_1,  DGNS_CD_10,  DGNS_CD_11,  DGNS_CD_12,  DGNS_CD_2,  DGNS_CD_3,  DGNS_CD_4,  DGNS_CD_5,  DGNS_CD_6,  DGNS_CD_7,  DGNS_CD_8,  DGNS_CD_9, DRG_CD,  DSCHRG_DT, HOSP_TYPE_CD, PRCDR_CD_1,  PRCDR_CD_2, PRCDR_CD_3,  PRCDR_CD_4, PRCDR_CD_5,  PRCDR_CD_6, PRCDR_CD_DT_1,  PRCDR_CD_DT_2,  PRCDR_CD_DT_3,  PRCDR_CD_DT_4,  PRCDR_CD_DT_5,  PRCDR_CD_DT_6, PRCDR_CD_SYS_1,  PRCDR_CD_SYS_2,  PRCDR_CD_SYS_3,  PRCDR_CD_SYS_4,  PRCDR_CD_SYS_5,  PRCDR_CD_SYS_6, SRVC_BGN_DT,  SRVC_END_DT,STATE_CD, '2016' as year,'ip_h' as file",
"inpatient_line":"BENE_ID,CLM_ID,LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,NDC,NDC_QTY,NDC_UOM_CD,PRVDR_FAC_TYPE_CD,SRVC_PRVDR_NPI,SRVC_PRVDR_SPCLTY_CD,STATE_CD,TOS_CD,'2016' as year,'ip_l' as file",
"other_services_header":"BENE_ID,BLG_PRVDR_NPI,BLG_PRVDR_SPCLTY_CD,CLM_ID,DGNS_CD_1,DGNS_CD_2,POS_CD,SRVC_BGN_DT,SRVC_END_DT,STATE_CD, '2016' as year,'ot_h' as file",
"other_services_line":"BENE_ID,CLM_ID,HCBS_SRVC_CD,LINE_PRCDR_CD,LINE_PRCDR_CD_SYS,LINE_PRCDR_MDFR_CD_1,LINE_PRCDR_MDFR_CD_2,LINE_PRCDR_MDFR_CD_3,LINE_PRCDR_MDFR_CD_4,LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,NDC,NDC_QTY,NDC_UOM_CD,SRVC_PRVDR_NPI,SRVC_PRVDR_SPCLTY_CD,STATE_CD,TOOTH_NUM,TOOTH_SRFC_CD,TOS_CD, '2016' as year,'ot_l' as file",
"long_term_header":"BLG_PRVDR_NPI,clm_id,state_cd,bene_id,srvc_bgn_dt,srvc_end_dt,ADMTG_DGNS_CD,dgns_cd_1,dgns_cd_2,dgns_cd_3,dgns_cd_4,dgns_cd_5,BLG_PRVDR_SPCLTY_CD,'2016' as year,'lt_h' as file",
"long_term_line":"clm_id,state_cd,SRVC_PRVDR_NPI,bene_id,ndc_uom_cd,ndc_qty,line_srvc_bgn_dt,line_srvc_end_dt,ndc,SRVC_PRVDR_SPCLTY_CD,'2016' as year,'lt_l' as file",
"rx_header":"BENE_ID,BLG_PRVDR_NPI,DSPNSNG_PRVDR_NPI,PRSCRBNG_PRVDR_NPI,RX_FILL_DT,STATE_CD,clm_id,BLG_PRVDR_SPCLTY_CD, '2016' as year,'rx_h' as file",     
"rx_line":"bene_id,state_cd,clm_id,ndc,dosage_form_cd,days_supply,new_rx_refill_num,ndc_qty,ndc_uom_cd,'2016' as year,'rx_l' as file"}
)

# COMMAND ----------

#extract 2017
dbutils.notebook.run(
  "/Users/<user_id>/02_Extract/02_2017",
  timeout_seconds = 360000,
  arguments = {"job_id": "2001",
"demog_elig_base":"AGE,   BENE_ID,  BENE_STATE_CD,  BIRTH_DT,  DEATH_DT,  ETHNCTY_CD,  RACE_ETHNCTY_CD,  RFRNC_YR,  SEX_CD,  STATE_CD,  bene_cnty_cd,  bene_zip_cd",
"demog_elig_dates":"BENE_ID,  ENRLMT_END_DT,  ENRLMT_START_DT,  RFRNC_YR,  STATE_CD",
"inpatient_header":" ADMSN_TYPE_CD,  ADMTG_DGNS_CD,  BENE_ID,  BLG_PRVDR_NPI,  BLG_PRVDR_SPCLTY_CD,  CLM_ID, DGNS_CD_1,  DGNS_CD_10,  DGNS_CD_11,  DGNS_CD_12,  DGNS_CD_2,  DGNS_CD_3,  DGNS_CD_4,  DGNS_CD_5,  DGNS_CD_6,  DGNS_CD_7,  DGNS_CD_8,  DGNS_CD_9, DRG_CD,  DSCHRG_DT, HOSP_TYPE_CD, PRCDR_CD_1,  PRCDR_CD_2, PRCDR_CD_3,  PRCDR_CD_4, PRCDR_CD_5,  PRCDR_CD_6, PRCDR_CD_DT_1,  PRCDR_CD_DT_2,  PRCDR_CD_DT_3,  PRCDR_CD_DT_4,  PRCDR_CD_DT_5,  PRCDR_CD_DT_6, PRCDR_CD_SYS_1,  PRCDR_CD_SYS_2,  PRCDR_CD_SYS_3,  PRCDR_CD_SYS_4,  PRCDR_CD_SYS_5,  PRCDR_CD_SYS_6, SRVC_BGN_DT,  SRVC_END_DT,STATE_CD, '2017' as year,'ip_h' as file",
"inpatient_line":"BENE_ID,CLM_ID,LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,NDC,NDC_QTY,NDC_UOM_CD,PRVDR_FAC_TYPE_CD,SRVC_PRVDR_NPI,SRVC_PRVDR_SPCLTY_CD,STATE_CD,TOS_CD,'2017' as year,'ip_l' as file",
"other_services_header":"BENE_ID,BLG_PRVDR_NPI,BLG_PRVDR_SPCLTY_CD,CLM_ID,DGNS_CD_1,DGNS_CD_2,POS_CD,SRVC_BGN_DT,SRVC_END_DT,STATE_CD, '2017' as year,'ot_h' as file",
"other_services_line":"BENE_ID,CLM_ID,HCBS_SRVC_CD,LINE_PRCDR_CD,LINE_PRCDR_CD_SYS,LINE_PRCDR_MDFR_CD_1,LINE_PRCDR_MDFR_CD_2,LINE_PRCDR_MDFR_CD_3,LINE_PRCDR_MDFR_CD_4,LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,NDC,NDC_QTY,NDC_UOM_CD,SRVC_PRVDR_NPI,SRVC_PRVDR_SPCLTY_CD,STATE_CD,TOOTH_NUM,TOOTH_SRFC_CD,TOS_CD, '2017' as year,'ot_l' as file",
"long_term_header":"BLG_PRVDR_NPI,clm_id,state_cd,bene_id,srvc_bgn_dt,srvc_end_dt,ADMTG_DGNS_CD,dgns_cd_1,dgns_cd_2,dgns_cd_3,dgns_cd_4,dgns_cd_5,BLG_PRVDR_SPCLTY_CD,'2017' as year,'lt_h' as file",
"long_term_line":"clm_id,state_cd,SRVC_PRVDR_NPI,bene_id,ndc_uom_cd,ndc_qty,line_srvc_bgn_dt,line_srvc_end_dt,ndc,SRVC_PRVDR_SPCLTY_CD,'2017' as year,'lt_l' as file",
"rx_header":"BENE_ID,BLG_PRVDR_NPI,DSPNSNG_PRVDR_NPI,PRSCRBNG_PRVDR_NPI,RX_FILL_DT,STATE_CD,clm_id,BLG_PRVDR_SPCLTY_CD, '2017' as year,'rx_h' as file",     
"rx_line":"bene_id,state_cd,clm_id,ndc,dosage_form_cd,days_supply,new_rx_refill_num,ndc_qty,ndc_uom_cd,'2017' as year,'rx_l' as file"}
)

# COMMAND ----------

#extract 2018
dbutils.notebook.run(
  "/Users/<user_id>/02_Extract/02_2018",
  timeout_seconds = 360000,
  arguments = {"job_id": "2001",
"demog_elig_base":"AGE,   BENE_ID,  BENE_STATE_CD,  BIRTH_DT,  DEATH_DT,  ETHNCTY_CD,  RACE_ETHNCTY_CD,  RFRNC_YR,  SEX_CD,  STATE_CD,  bene_cnty_cd,  bene_zip_cd",
"demog_elig_dates":"BENE_ID,  ENRLMT_END_DT,  ENRLMT_START_DT,  RFRNC_YR,  STATE_CD",
"inpatient_header":" ADMSN_TYPE_CD,  ADMTG_DGNS_CD,  BENE_ID,  BLG_PRVDR_NPI,  BLG_PRVDR_SPCLTY_CD,  CLM_ID, DGNS_CD_1,  DGNS_CD_10,  DGNS_CD_11,  DGNS_CD_12,  DGNS_CD_2,  DGNS_CD_3,  DGNS_CD_4,  DGNS_CD_5,  DGNS_CD_6,  DGNS_CD_7,  DGNS_CD_8,  DGNS_CD_9, DRG_CD,  DSCHRG_DT, HOSP_TYPE_CD, PRCDR_CD_1,  PRCDR_CD_2, PRCDR_CD_3,  PRCDR_CD_4, PRCDR_CD_5,  PRCDR_CD_6, PRCDR_CD_DT_1,  PRCDR_CD_DT_2,  PRCDR_CD_DT_3,  PRCDR_CD_DT_4,  PRCDR_CD_DT_5,  PRCDR_CD_DT_6, PRCDR_CD_SYS_1,  PRCDR_CD_SYS_2,  PRCDR_CD_SYS_3,  PRCDR_CD_SYS_4,  PRCDR_CD_SYS_5,  PRCDR_CD_SYS_6, SRVC_BGN_DT,  SRVC_END_DT,STATE_CD, '2018' as year,'ip_h' as file",
"inpatient_line":"BENE_ID,CLM_ID,LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,NDC,NDC_QTY,NDC_UOM_CD,PRVDR_FAC_TYPE_CD,SRVC_PRVDR_NPI,SRVC_PRVDR_SPCLTY_CD,STATE_CD,TOS_CD,'2018' as year,'ip_l' as file",
"other_services_header":"BENE_ID,BLG_PRVDR_NPI,BLG_PRVDR_SPCLTY_CD,CLM_ID,DGNS_CD_1,DGNS_CD_2,POS_CD,SRVC_BGN_DT,SRVC_END_DT,STATE_CD, '2018' as year,'ot_h' as file",
"other_services_line":"BENE_ID,CLM_ID,HCBS_SRVC_CD,LINE_PRCDR_CD,LINE_PRCDR_CD_SYS,LINE_PRCDR_MDFR_CD_1,LINE_PRCDR_MDFR_CD_2,LINE_PRCDR_MDFR_CD_3,LINE_PRCDR_MDFR_CD_4,LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,NDC,NDC_QTY,NDC_UOM_CD,SRVC_PRVDR_NPI,SRVC_PRVDR_SPCLTY_CD,STATE_CD,TOOTH_NUM,TOOTH_SRFC_CD,TOS_CD, '2018' as year,'ot_l' as file",
"long_term_header":"BLG_PRVDR_NPI,clm_id,state_cd,bene_id,srvc_bgn_dt,srvc_end_dt,ADMTG_DGNS_CD,dgns_cd_1,dgns_cd_2,dgns_cd_3,dgns_cd_4,dgns_cd_5,BLG_PRVDR_SPCLTY_CD,'2018' as year,'lt_h' as file",
"long_term_line":"clm_id,state_cd,SRVC_PRVDR_NPI,bene_id,ndc_uom_cd,ndc_qty,line_srvc_bgn_dt,line_srvc_end_dt,ndc,SRVC_PRVDR_SPCLTY_CD,'2018' as year,'lt_l' as file",
"rx_header":"BENE_ID,BLG_PRVDR_NPI,DSPNSNG_PRVDR_NPI,PRSCRBNG_PRVDR_NPI,RX_FILL_DT,STATE_CD,clm_id,BLG_PRVDR_SPCLTY_CD, '2018' as year,'rx_h' as file",     
"rx_line":"bene_id,state_cd,clm_id,ndc,dosage_form_cd,days_supply,new_rx_refill_num,ndc_qty,ndc_uom_cd,'2018' as year,'rx_l' as file"}
)

# COMMAND ----------

dbutils.notebook.run(
  "/Users/<user_id>/02_Extract/Optimize",
  timeout_seconds = 36000000000000,
  arguments = {"job_id": "2001"}
)

# COMMAND ----------


dbutils.notebook.run(
"/Users/<user_id>/03_Transform_Load/Transform_Load_14",
  timeout_seconds=360000000,
  arguments={"job_id":"2014",
             "year":"14"}
)

# COMMAND ----------


dbutils.notebook.run(
"/Users/<user_id>/03_Transform_Load/Transform_Load_15",
  timeout_seconds=360000000,
  arguments={"job_id":"2014",
             "year":"15"}
)

# COMMAND ----------

dbutils.notebook.run(
"/Users/<user_id>/03_Transform_Load/Transform_Load_16",
  timeout_seconds=360000000,
  arguments={"job_id":"2014",
             "year":"16"}
)

# COMMAND ----------

dbutils.notebook.run(
"/Users/<user_id>/03_Transform_Load/Transform_Load_17",
  timeout_seconds=360000000,
  arguments={"job_id":"2014",
             "year":"17"}
)

# COMMAND ----------

dbutils.notebook.run(
"/Users/<user_id>/03_Transform_Load/Transform_Load_18",
  timeout_seconds=360000000,
  arguments={"job_id":"2014",
             "year":"18"}
)

# COMMAND ----------

dbutils.notebook.run(
"/Users/<user_id>/03_Transform_Load/C1_person",
  timeout_seconds=360000000,
  arguments={"job_id":"2014"
             })

# COMMAND ----------

dbutils.notebook.run(
"/Users/<user_id>/03_Transform_Load/C2_observation_period",
  timeout_seconds=360000000,
  arguments={"job_id":"2014"
             })

# COMMAND ----------

dbutils.notebook.run(
"/Users/<user_id>/03_Transform_Load/C3_death",
  timeout_seconds=360000000,
  arguments={"job_id":"2014"
             })

# COMMAND ----------

# MAGIC %sql
# MAGIC create table <write_bucket>.dx_stat as select
# MAGIC count(distinct person_id) as pt_cnt,
# MAGIC count(condition_source_value) as event_cnt,
# MAGIC condition_source_value as concept_code
# MAGIC from <write_bucket>.condition_occurrence
# MAGIC group by
# MAGIC condition_source_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table <write_bucket>.px_stat as select
# MAGIC count(distinct person_id) as pt_cnt,
# MAGIC count(procedure_source_value) as event_cnt,
# MAGIC procedure_source_value as concept_code
# MAGIC from <write_bucket>.procedure_occurrence
# MAGIC group by
# MAGIC procedure_source_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table <write_bucket>.rx_stat as select
# MAGIC count(distinct person_id) as pt_cnt,
# MAGIC count(drug_source_value) as event_cnt,
# MAGIC drug_source_value as concept_code
# MAGIC from <write_bucket>.drug_exposure
# MAGIC group by
# MAGIC drug_source_value;
