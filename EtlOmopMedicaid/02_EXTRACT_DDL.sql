-- Databricks notebook source
insert into dua_052538_nwi388.log values('$job_id','EXTRACT_DDL','0','extract ddl start',current_timestamp(), null);

-- COMMAND ----------

--table dec
drop table if exists dua_052538_nwi388.demog_elig_base;
create table dua_052538_nwi388.demog_elig_base( 
 AGE string,   
 BENE_ID string,  
 BENE_STATE_CD string,  
 BIRTH_DT date,  
 DEATH_DT date,  
 ETHNCTY_CD string,  
 RACE_ETHNCTY_CD string,  
 RFRNC_YR string,  
 SEX_CD string,  
 STATE_CD string,  
 bene_cnty_cd string,  
 bene_zip_cd string) partitioned by (state_cd);
 
drop table if exists dua_052538_nwi388.demog_elig_dates;
create table dua_052538_nwi388.demog_elig_dates(
BENE_ID string,  
ENRLMT_END_DT date,  
ENRLMT_START_DT date,
RFRNC_YR string,
STATE_CD string) partitioned by (state_cd);

drop table if exists dua_052538_nwi388.inpatient_header;
create table dua_052538_nwi388.inpatient_header(
ADMSN_TYPE_CD string,  
ADMTG_DGNS_CD string,  
BENE_ID string,  
BLG_PRVDR_NPI string,  
BLG_PRVDR_SPCLTY_CD string,  
CLM_ID string,  
DGNS_CD_1 string,  
DGNS_CD_10 string,  
DGNS_CD_11 string,  
DGNS_CD_12 string,  
DGNS_CD_2 string,  
DGNS_CD_3 string, 
DGNS_CD_4 string,  
DGNS_CD_5 string,  
DGNS_CD_6 string,  
DGNS_CD_7 string,  
DGNS_CD_8 string,  
DGNS_CD_9 string,
DRG_CD string,
DSCHRG_DT date,
HOSP_TYPE_CD string,
PRCDR_CD_1 string,  
PRCDR_CD_2 string, 
PRCDR_CD_3 string,  
PRCDR_CD_4 string, 
PRCDR_CD_5 string,  
PRCDR_CD_6 string,
PRCDR_CD_DT_1 date,  
PRCDR_CD_DT_2 date,  
PRCDR_CD_DT_3 date,  
PRCDR_CD_DT_4 date,  
PRCDR_CD_DT_5 date,  
PRCDR_CD_DT_6 date,
PRCDR_CD_SYS_1 string,  
PRCDR_CD_SYS_2 string,  
PRCDR_CD_SYS_3 string,  
PRCDR_CD_SYS_4 string,  
PRCDR_CD_SYS_5 string,  
PRCDR_CD_SYS_6 string,
SRVC_BGN_DT date,  
SRVC_END_DT date,
STATE_CD string) partitioned by (state_cd);
  
drop table if exists dua_052538_nwi388.inpatient_line;
create table dua_052538_nwi388.inpatient_line(
BENE_ID string,
CLM_ID string,
LINE_SRVC_BGN_DT date,
LINE_SRVC_END_DT date,
NDC string,
NDC_QTY string,
NDC_UOM_CD string,
PRVDR_FAC_TYPE_CD string,
SRVC_PRVDR_NPI string,
SRVC_PRVDR_SPCLTY_CD string,
STATE_CD string,
TOS_CD string)partitioned by (state_cd);

drop table if exists dua_052538_nwi388.other_services_header;
create table dua_052538_nwi388.other_services_header(
BENE_ID string,
BLG_PRVDR_NPI string,
BLG_PRVDR_SPCLTY_CD string,
CLM_ID string,
DGNS_CD_1 string,
DGNS_CD_2 string,
POS_CD string,
SRVC_BGN_DT date,
SRVC_END_DT date,
STATE_CD string)partitioned by (state_cd);

drop table if exists dua_052538_nwi388.other_services_line;
create table dua_052538_nwi388.other_services_line(
BENE_ID string,
CLM_ID string,
HCBS_SRVC_CD string,
LINE_PRCDR_CD string,
LINE_PRCDR_CD_SYS string,
LINE_PRCDR_MDFR_CD_1 string,
LINE_PRCDR_MDFR_CD_2 string,
LINE_PRCDR_MDFR_CD_3 string,
LINE_PRCDR_MDFR_CD_4 string,
LINE_SRVC_BGN_DT string,
LINE_SRVC_END_DT string,
NDC string,
NDC_QTY string,
NDC_UOM_CD string,
SRVC_PRVDR_NPI string,
SRVC_PRVDR_SPCLTY_CD string,
STATE_CD string,
TOOTH_NUM string,
TOOTH_SRFC_CD string,
TOS_CD string)partitioned by (state_cd);

drop table if exists dua_052538_nwi388.long_term_header;
create table dua_052538_nwi388.long_term_header(
BLG_PRVDR_NPI string,
clm_id string,
state_cd string,
bene_id string,
srvc_bgn_dt string,
srvc_end_dt string,
ADMTG_DGNS_CD string,
dgns_cd_1 string,
dgns_cd_2 string,
dgns_cd_3 string,
dgns_cd_4 string,
dgns_cd_5 string,
BLG_PRVDR_SPCLTY_CD string)partitioned by (state_cd);

drop table if exists dua_052538_nwi388.long_term_line;
create table dua_052538_nwi388.long_term_line(
clm_id string,
state_cd string,
SRVC_PRVDR_NPI string,
bene_id string,
ndc_uom_cd string,
ndc_qty string,
line_srvc_bgn_dt date,
line_srvc_end_dt date,
ndc string,
SRVC_PRVDR_SPCLTY_CD string)partitioned by (state_cd);

drop table if exists dua_052538_nwi388.rx_header;
create table dua_052538_nwi388.rx_header(
BENE_ID string,
BLG_PRVDR_NPI string,
DSPNSNG_PRVDR_NPI string,
PRSCRBNG_PRVDR_NPI string,
RX_FILL_DT date,
STATE_CD string,
clm_id string,
BLG_PRVDR_SPCLTY_CD string
)partitioned by (state_cd);

drop table if exists dua_052538_nwi388.rx_line;
create table dua_052538_nwi388.rx_line( 
bene_id string,
state_cd string,
clm_id string,
ndc string,
dosage_form_cd string,
days_supply string,
new_rx_refill_num string,
ndc_qty string,
NDC_uom_cd string
)partitioned by (state_cd);



-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT_DDL','1','extract ddl end',current_timestamp(), null);