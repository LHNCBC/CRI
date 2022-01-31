-- Databricks notebook source
--2015
insert into dua_052538_nwi388.log values('$job_id','EXTRACT','1','start_2015',current_timestamp(), null);

-- COMMAND ----------

--var dec

create widget text job_id default "'101'";


create widget text demog_elig_base default "AGE,   BENE_ID,  BENE_STATE_CD,  BIRTH_DT,  DEATH_DT,  ETHNCTY_CD,  RACE_ETHNCTY_CD,  RFRNC_YR,  SEX_CD,  STATE_CD,  bene_cnty_cd,  bene_zip_cd";

create widget text demog_elig_dates default "BENE_ID,  ENRLMT_END_DT,  ENRLMT_START_DT,  RFRNC_YR,  STATE_CD";

create widget text inpatient_header default " ADMSN_TYPE_CD,  ADMTG_DGNS_CD,  BENE_ID,  BLG_PRVDR_NPI,  BLG_PRVDR_SPCLTY_CD,  CLM_ID,  
  DGNS_CD_1,  DGNS_CD_10,  DGNS_CD_11,  DGNS_CD_12,  DGNS_CD_2,  DGNS_CD_3,  DGNS_CD_4,  DGNS_CD_5,  DGNS_CD_6,  DGNS_CD_7,  DGNS_CD_8,  DGNS_CD_9,
  DRG_CD,
  DSCHRG_DT,
  HOSP_TYPE_CD,
  PRCDR_CD_1,  PRCDR_CD_2, PRCDR_CD_3,  PRCDR_CD_4, PRCDR_CD_5,  PRCDR_CD_6,
  PRCDR_CD_DT_1,  PRCDR_CD_DT_2,  PRCDR_CD_DT_3,  PRCDR_CD_DT_4,  PRCDR_CD_DT_5,  PRCDR_CD_DT_6,
  PRCDR_CD_SYS_1,  PRCDR_CD_SYS_2,  PRCDR_CD_SYS_3,  PRCDR_CD_SYS_4,  PRCDR_CD_SYS_5,  PRCDR_CD_SYS_6,
  SRVC_BGN_DT,  SRVC_END_DT,
  STATE_CD";

create widget text inpatient_line default "BENE_ID,CLM_ID,LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,NDC,NDC_QTY,NDC_UOM_CD,PRVDR_FAC_TYPE_CD,SRVC_PRVDR_NPI,SRVC_PRVDR_SPCLTY_CD,STATE_CD,TOS_CD";

create widget text other_services_header default "BENE_ID,BLG_PRVDR_NPI,BLG_PRVDR_SPCLTY_CD,CLM_ID,DGNS_CD_1,DGNS_CD_2,POS_CD,SRVC_BGN_DT,SRVC_END_DT,STATE_CD";

create widget text other_services_line default"BENE_ID,CLM_ID,HCBS_SRVC_CD,LINE_PRCDR_CD,LINE_PRCDR_CD_SYS,LINE_PRCDR_MDFR_CD_1,LINE_PRCDR_MDFR_CD_2,LINE_PRCDR_MDFR_CD_3,
LINE_PRCDR_MDFR_CD_4,LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,NDC,NDC_QTY,NDC_UOM_CD,SRVC_PRVDR_NPI,SRVC_PRVDR_SPCLTY_CD,STATE_CD,TOOTH_NUM,TOOTH_SRFC_CD,TOS_CD ";

create widget text long_term_header default"BLG_PRVDR_NPI,clm_id,state_cd,bene_id,srvc_bgn_dt,srvc_end_dt,ADMTG_DGNS_CD,dgns_cd_1,dgns_cd_2,dgns_cd_3,dgns_cd_4,dgns_cd_5,BLG_PRVDR_SPCLTY_CD";

create widget text long_term_line default"clm_id,state_cd,SRVC_PRVDR_NPI,bene_id,ndc_uom_cd,ndc_qty,line_srvc_bgn_dt,line_srvc_end_dt,ndc,SRVC_PRVDR_SPCLTY_CD";

create widget text rx_header default "BENE_ID,BLG_PRVDR_NPI,DSPNSNG_PRVDR_NPI,PRSCRBNG_PRVDR_NPI,RX_FILL_DT,STATE_CD,clm_id,BLG_PRVDR_SPCLTY_CD";

create widget text rx_line default "ndc,dosage_form_cd ,days_supply,new_rx_refill_num,ndc_qty,NDC_uom_cd,bene_id,state_cd,clm_id";

-- COMMAND ----------

insert into dua_052538_nwi388.demog_elig_base select $demog_elig_base from dua_052538.tafr15_demog_elig_base where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','2','demog_elig_base_2015',current_timestamp(), null);

-- COMMAND ----------

--demog_elig_dates
insert into dua_052538_nwi388.demog_elig_dates select $demog_elig_dates from dua_052538.tafr15_demog_elig_dates where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','3','demog_elig_dates_2015',current_timestamp(), null);

-- COMMAND ----------

--inpt_header
insert into dua_052538_nwi388.inpatient_header 
select $inpatient_header from(
select $inpatient_header    from      dua_052538.tafr15_inpatient_header_01    union
select $inpatient_header    from      dua_052538.tafr15_inpatient_header_02    union
select $inpatient_header    from      dua_052538.tafr15_inpatient_header_03    union
select $inpatient_header    from      dua_052538.tafr15_inpatient_header_04    union
select $inpatient_header    from      dua_052538.tafr15_inpatient_header_05    union
select $inpatient_header    from      dua_052538.tafr15_inpatient_header_06    union
select $inpatient_header    from      dua_052538.tafr15_inpatient_header_07    union
select $inpatient_header    from      dua_052538.tafr15_inpatient_header_08    union
select $inpatient_header    from      dua_052538.tafr15_inpatient_header_09    union
select $inpatient_header    from      dua_052538.tafr15_inpatient_header_10    union
select $inpatient_header    from      dua_052538.tafr15_inpatient_header_11    union
select $inpatient_header    from      dua_052538.tafr15_inpatient_header_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','4','inpatient_header_2015',current_timestamp(), null);

-- COMMAND ----------

--inpt_line
insert into  dua_052538_nwi388.inpatient_line
select $inpatient_line from(
select $inpatient_line from dua_052538.tafr15_inpatient_line_01 union
select $inpatient_line from dua_052538.tafr15_inpatient_line_02 union
select $inpatient_line from dua_052538.tafr15_inpatient_line_03 union
select $inpatient_line from dua_052538.tafr15_inpatient_line_04 union
select $inpatient_line from dua_052538.tafr15_inpatient_line_05 union
select $inpatient_line from dua_052538.tafr15_inpatient_line_06 union
select $inpatient_line from dua_052538.tafr15_inpatient_line_07 union
select $inpatient_line from dua_052538.tafr15_inpatient_line_08 union
select $inpatient_line from dua_052538.tafr15_inpatient_line_09 union
select $inpatient_line from dua_052538.tafr15_inpatient_line_10 union
select $inpatient_line from dua_052538.tafr15_inpatient_line_11 union
select $inpatient_line from dua_052538.tafr15_inpatient_line_12 )
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','5','inpatient_line_2015',current_timestamp(), null);

-- COMMAND ----------

--ot_header
insert into dua_052538_nwi388.other_services_header
select  $other_services_header from(
select  $other_services_header from dua_052538.tafr15_other_services_header_01 union
select  $other_services_header from dua_052538.tafr15_other_services_header_02 union
select  $other_services_header from dua_052538.tafr15_other_services_header_03 union
select  $other_services_header from dua_052538.tafr15_other_services_header_04 union
select  $other_services_header from dua_052538.tafr15_other_services_header_05 union
select  $other_services_header from dua_052538.tafr15_other_services_header_06 union
select  $other_services_header from dua_052538.tafr15_other_services_header_07 union
select  $other_services_header from dua_052538.tafr15_other_services_header_08 union
select  $other_services_header from dua_052538.tafr15_other_services_header_09 union
select  $other_services_header from dua_052538.tafr15_other_services_header_10 union
select  $other_services_header from dua_052538.tafr15_other_services_header_11 union
select  $other_services_header from dua_052538.tafr15_other_services_header_12 )
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','6','other_services_header_2015',current_timestamp(), null);

-- COMMAND ----------

--ot_line
insert into dua_052538_nwi388.other_services_line
select $other_services_line from(
select $other_services_line from dua_052538.tafr15_other_services_line_01 union
select $other_services_line from dua_052538.tafr15_other_services_line_02 union
select $other_services_line from dua_052538.tafr15_other_services_line_03 union
select $other_services_line from dua_052538.tafr15_other_services_line_04 union
select $other_services_line from dua_052538.tafr15_other_services_line_05 union
select $other_services_line from dua_052538.tafr15_other_services_line_06 union
select $other_services_line from dua_052538.tafr15_other_services_line_07 union
select $other_services_line from dua_052538.tafr15_other_services_line_08 union
select $other_services_line from dua_052538.tafr15_other_services_line_09 union
select $other_services_line from dua_052538.tafr15_other_services_line_10 union
select $other_services_line from dua_052538.tafr15_other_services_line_11 union
select $other_services_line from dua_052538.tafr15_other_services_line_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','7','other_services_line_2015',current_timestamp(), null);

-- COMMAND ----------

--lt_header
insert into dua_052538_nwi388.long_term_header
select $long_term_header from(
select $long_term_header from dua_052538.tafr15_long_term_header_01 union
select $long_term_header from dua_052538.tafr15_long_term_header_02 union
select $long_term_header from dua_052538.tafr15_long_term_header_03 union
select $long_term_header from dua_052538.tafr15_long_term_header_04 union
select $long_term_header from dua_052538.tafr15_long_term_header_05 union
select $long_term_header from dua_052538.tafr15_long_term_header_06 union
select $long_term_header from dua_052538.tafr15_long_term_header_07 union
select $long_term_header from dua_052538.tafr15_long_term_header_08 union
select $long_term_header from dua_052538.tafr15_long_term_header_09 union
select $long_term_header from dua_052538.tafr15_long_term_header_10 union
select $long_term_header from dua_052538.tafr15_long_term_header_11 union
select $long_term_header from dua_052538.tafr15_long_term_header_12 )
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','8','long_term_header_2015',current_timestamp(), null);

-- COMMAND ----------

--lt_line
insert into dua_052538_nwi388.long_term_line
select $long_term_line from(
select $long_term_line from dua_052538.tafr15_long_term_line_01 union
select $long_term_line from dua_052538.tafr15_long_term_line_02 union
select $long_term_line from dua_052538.tafr15_long_term_line_03 union
select $long_term_line from dua_052538.tafr15_long_term_line_04 union
select $long_term_line from dua_052538.tafr15_long_term_line_05 union
select $long_term_line from dua_052538.tafr15_long_term_line_06 union
select $long_term_line from dua_052538.tafr15_long_term_line_07 union
select $long_term_line from dua_052538.tafr15_long_term_line_08 union
select $long_term_line from dua_052538.tafr15_long_term_line_09 union
select $long_term_line from dua_052538.tafr15_long_term_line_10 union
select $long_term_line from dua_052538.tafr15_long_term_line_11 union
select $long_term_line from dua_052538.tafr15_long_term_line_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','9','long_term_line_2015',current_timestamp(), null);

-- COMMAND ----------

--rx_header
insert into dua_052538_nwi388.rx_header
select $rx_header from(
select $rx_header from dua_052538.tafr15_rx_header_01 union
select $rx_header from dua_052538.tafr15_rx_header_02 union
select $rx_header from dua_052538.tafr15_rx_header_03 union
select $rx_header from dua_052538.tafr15_rx_header_04 union
select $rx_header from dua_052538.tafr15_rx_header_05 union
select $rx_header from dua_052538.tafr15_rx_header_06 union
select $rx_header from dua_052538.tafr15_rx_header_07 union
select $rx_header from dua_052538.tafr15_rx_header_08 union
select $rx_header from dua_052538.tafr15_rx_header_09 union
select $rx_header from dua_052538.tafr15_rx_header_10 union
select $rx_header from dua_052538.tafr15_rx_header_11 union
select $rx_header from dua_052538.tafr15_rx_header_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','10','rx_header_2015',current_timestamp(), null);

-- COMMAND ----------

--rx_line
insert into dua_052538_nwi388.rx_line
select $rx_line from(
select $rx_line from dua_052538.tafr15_rx_line_01 union
select $rx_line from dua_052538.tafr15_rx_line_02 union
select $rx_line from dua_052538.tafr15_rx_line_03 union
select $rx_line from dua_052538.tafr15_rx_line_04 union
select $rx_line from dua_052538.tafr15_rx_line_05 union
select $rx_line from dua_052538.tafr15_rx_line_06 union
select $rx_line from dua_052538.tafr15_rx_line_07 union
select $rx_line from dua_052538.tafr15_rx_line_08 union
select $rx_line from dua_052538.tafr15_rx_line_09 union
select $rx_line from dua_052538.tafr15_rx_line_10 union
select $rx_line from dua_052538.tafr15_rx_line_11 union
select $rx_line from dua_052538.tafr15_rx_line_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','11','rx_line_2015',current_timestamp(), null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','12','end_2015',current_timestamp(), null);