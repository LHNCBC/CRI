-- Databricks notebook source
--DEMOG BASE
--drop view demog_elig_base;
create view demog_elig_base as select
  BENE_ID,
  STATE_CD,
  BIRTH_DT,
  DEATH_DT,
  SEX_CD,
  ETHNCTY_CD,	
  RACE_ETHNCTY_CD
from dua_052538.tafr18_demog_elig_base
where state_cd="WI"
;

-- COMMAND ----------

--DEMOG ELIG
drop view demog_elig;
create view demog_elig as  select 
  bene_id ,
  enrlmt_start_dt ,
  enrlmt_end_dt,
  state_cd
from dua_052538.tafr18_demog_elig_dates 
where state_cd="WI"
;


-- COMMAND ----------

--Inpatient Header
drop view inpatient_header;
create view inpatient_header as select 
  BENE_ID,CLM_ID,STATE_CD,"inpatient_header" as file,--primary keys
  ADMSN_TYPE_CD, --admission type
  ADMSN_DT, ADMSN_HR, DSCHRG_DT, DSCHRG_HR, --admit-dschrg dates
  SRVC_BGN_DT, SRVC_END_DT,--service dates
  HOSP_TYPE_CD,--facility type
  RFRG_PRVDR_ID, RFRG_PRVDR_NPI, RFRG_PRVDR_SPCLTY_CD, RFRG_PRVDR_TYPE_CD, --provider, type:RFRG
  ADMTG_PRVDR_ID, ADMTG_PRVDR_NPI, ADMTG_PRVDR_SPCLTY_CD, ADMTG_PRVDR_TXNMY_CD, ADMTG_PRVDR_TYPE_CD,--provider, type:ADMTG 
  BLG_PRVDR_ID, BLG_PRVDR_NPI, BLG_PRVDR_SPCLTY_CD, BLG_PRVDR_TXNMY_CD,BLG_PRVDR_TYPE_CD,--provider, type:BLG
  DGNS_CD_1, DGNS_CD_10, DGNS_CD_11, DGNS_CD_12, DGNS_CD_2, DGNS_CD_3, DGNS_CD_4, DGNS_CD_5, DGNS_CD_6, DGNS_CD_7, DGNS_CD_8, DGNS_CD_9, DRG_CD, ADMTG_DGNS_CD,--dx codes
  PRCDR_CD_1, PRCDR_CD_2, PRCDR_CD_3, PRCDR_CD_4, PRCDR_CD_5, PRCDR_CD_6, --procedure codes
  PRCDR_CD_DT_1, PRCDR_CD_DT_2, PRCDR_CD_DT_3, PRCDR_CD_DT_4, PRCDR_CD_DT_5,PRCDR_CD_DT_6--procedure dates
from from dua_052538.tafr18_inpatient_header_06
where state_cd="WI"
;

-- COMMAND ----------

--Inpatient Line
drop view inpatient_line;
create view inpatient_line as select
  BENE_ID,STATE_CD,CLM_ID,"inpatient_line" as file,--primary keys
  LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,--service dates
  PRVDR_FAC_TYPE_CD,--facility type
  SRVC_PRVDR_ID,SRVC_PRVDR_NPI,SRVC_PRVDR_TXNMY_CD,SRVC_PRVDR_TYPE_CD,SRVC_PRVDR_SPCLTY_CD,--provider, type:SRVC
  OPRTG_PRVDR_NPI,--provider,type:OPRTG
  NDC,NDC_UOM_CD,NDC_QTY--RX
from dua_052538.tafr18_inpatient_line_06
where state_cd="WI"
;


-- COMMAND ----------

--Other Services Header
drop view other_services_header;
create view other_services_header as select 
  BENE_ID,STATE_CD,CLM_ID,"other_services_header" as file,--primary keys
  srvc_bgn_dt,srvc_end_dt,--service dates
  POS_CD,--facility type
  blg_prvdr_id,blg_prvdr_npi,blg_prvdr_txnmy_cd,blg_prvdr_type_cd,blg_prvdr_spclty_cd,--provider,type:BLG
  rfrg_prvdr_id,rfrg_prvdr_npi,rfrg_prvdr_txnmy_cd,rfrg_prvdr_type_cd,rfrg_prvdr_spclty_cd,--provider,type:RFRG
  drctng_prvdr_npi,drctng_prvdr_txnmy_cd,--provider,type:DRCTNG
  sprvsng_prvdr_npi,sprvsng_prvdr_txnmy_cd,--provider,type:SPRVSNG
  hlth_home_prvdr_ind,hlth_home_prvdr_npi,hlth_home_ent_name,--provider,type:HLTH_HOME
  dgns_cd_1,dgns_cd_2--DX
from  dua_052538.tafr18_other_services_header_06
where state_cd="WI"
;


-- COMMAND ----------

--Other Services Line
drop view other_services_line;
create view other_services_line as select
  BENE_ID,STATE_CD,CLM_ID,"other_services_line" as file,--primary keys
  NDC,NDC_UOM_CD,NDC_QTY,--RX
  LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,--service dates
  LINE_PRCDR_CD,LINE_PRCDR_CD_DT,--procedure dates
  LINE_PRCDR_MDFR_CD_1,LINE_PRCDR_MDFR_CD_2,LINE_PRCDR_MDFR_CD_3,LINE_PRCDR_MDFR_CD_4,--procedure mod codes
  TOOTH_DSGNTN_SYS,TOOTH_NUM,TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,TOOTH_SRFC_CD,--Teeth
  SRVC_PRVDR_ID,SRVC_PRVDR_NPI,SRVC_PRVDR_TXNMY_CD,SRVC_PRVDR_TYPE_CD,SRVC_PRVDR_SPCLTY_CD--provider, type:SRVC
from dua_052538.tafr18_other_services_line_06
where state_cd="WI"

; 

-- COMMAND ----------

--Long Term Header
drop view long_term_header;
create view long_term_header as select
  BENE_ID,STATE_CD,CLM_ID,"long_term_header" as file,--primary keys
  SRVC_BGN_DT,SRVC_END_DT,--service dates
  ADMSN_DT,ADMSN_HR,DSCHRG_DT,DSCHRG_HR, --admit-dschrg dates
  ADMTG_DGNS_CD,DGNS_CD_1,DGNS_CD_2,DGNS_CD_3,DGNS_CD_4,DGNS_CD_5,--DX
  ADMTG_PRVDR_ID,ADMTG_PRVDR_NPI,ADMTG_PRVDR_TXNMY_CD,ADMTG_PRVDR_TYPE_CD,ADMTG_PRVDR_SPCLTY_CD,--provider, type:ADMTG 
  BLG_PRVDR_ID,BLG_PRVDR_NPI,BLG_PRVDR_TXNMY_CD,BLG_PRVDR_TYPE_CD,BLG_PRVDR_SPCLTY_CD,--provider, type:PRVDR 
  RFRG_PRVDR_ID,RFRG_PRVDR_NPI,RFRG_PRVDR_TYPE_CD,RFRG_PRVDR_SPCLTY_CD,PRVDR_LCTN_CD--provider, type:RFRG 
from  dua_052538.tafr18_long_term_header_06
where state_cd="WI"
;

-- COMMAND ----------

--Long Term Line
drop view long_term_line;
create view long_term_line as select
  BENE_ID,STATE_CD,CLM_ID,"long_term_line" as file,--primary keys
  LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,--service dates
  PRVDR_FAC_TYPE_CD,--facility type
  SRVC_PRVDR_ID,SRVC_PRVDR_NPI,SRVC_PRVDR_TXNMY_CD,SRVC_PRVDR_TYPE_CD,SRVC_PRVDR_SPCLTY_CD,--provider, type:SRVC
  NDC,NDC_UOM_CD,NDC_QTY--RX
from dua_052538.tafr18_long_term_line_06
where state_cd="WI"
;

-- COMMAND ----------

--RX Header
drop view rx_header;
create view rx_header as select 
  BENE_ID,STATE_CD,CLM_ID,"rx_header" as file,--primary keys
  BLG_PRVDR_ID,BLG_PRVDR_NPI,BLG_PRVDR_TXNMY_CD,BLG_PRVDR_SPCLTY_CD,--provider, type: BLG
  PRSCRBNG_PRVDR_ID,PRSCRBNG_PRVDR_NPI,--provider, type:PRSCRBING
  DSPNSNG_PRVDR_ID,DSPNSNG_PRVDR_NPI,--provider, type:DSPNSNG
  PRVDR_LCTN_CD--facility type
from  dua_052538.tafr18_rx_header_06
where state_cd="WI"
  ;

-- COMMAND ----------

--Medication Line
drop view rx_line;
create view rx_line as select
  BENE_ID,STATE_CD,CLM_ID,"rx_line" as file,--primary keys
  NDC,NDC_UOM_CD,NDC_QTY,MTRC_DCML_QTY,NDC_QTY_ALOWD,DAYS_SUPPLY,NEW_RX_REFILL_NUM,DOSAGE_FORM_CD,--RX
  RX_FILL_DT--RX date
from  dua_052538.tafr18_rx_line_06
where state_cd="WI"



