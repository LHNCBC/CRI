-- Databricks notebook source

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','1','start_16',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.inpatient_header_16;
create table dua_052538_nwi388.inpatient_header_16(
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
STATE_CD string,
 year string,
 file string) partitioned by (state_cd);
  
drop table if exists dua_052538_nwi388.inpatient_line_16;
create table dua_052538_nwi388.inpatient_line_16(
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
TOS_CD string,
 year string,
 file string)partitioned by (state_cd);

drop table if exists dua_052538_nwi388.other_services_header_16;
create table dua_052538_nwi388.other_services_header_16(
BENE_ID string,
BLG_PRVDR_NPI string,
BLG_PRVDR_SPCLTY_CD string,
CLM_ID string,
DGNS_CD_1 string,
DGNS_CD_2 string,
POS_CD string,
SRVC_BGN_DT date,
SRVC_END_DT date,
STATE_CD string,
 year string,
 file string)partitioned by (state_cd);

drop table if exists dua_052538_nwi388.other_services_line_16;
create table dua_052538_nwi388.other_services_line_16(
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
TOS_CD string,
 year string,
 file string)partitioned by (state_cd);

drop table if exists dua_052538_nwi388.long_term_header_16;
create table dua_052538_nwi388.long_term_header_16(
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
BLG_PRVDR_SPCLTY_CD string,
 year string,
 file string)partitioned by (state_cd);

drop table if exists dua_052538_nwi388.long_term_line_16;
create table dua_052538_nwi388.long_term_line_16(
clm_id string,
state_cd string,
SRVC_PRVDR_NPI string,
bene_id string,
ndc_uom_cd string,
ndc_qty string,
line_srvc_bgn_dt date,
line_srvc_end_dt date,
ndc string,
SRVC_PRVDR_SPCLTY_CD string,
 year string,
 file string)partitioned by (state_cd);

drop table if exists dua_052538_nwi388.rx_header_16;
create table dua_052538_nwi388.rx_header_16(
BENE_ID string,
BLG_PRVDR_NPI string,
DSPNSNG_PRVDR_NPI string,
PRSCRBNG_PRVDR_NPI string,
RX_FILL_DT date,
STATE_CD string,
clm_id string,
BLG_PRVDR_SPCLTY_CD string,
 year string,
 file string
)partitioned by (state_cd);

drop table if exists dua_052538_nwi388.rx_line_16;
create table dua_052538_nwi388.rx_line_16( 
bene_id string,
state_cd string,
clm_id string,
ndc string,
dosage_form_cd string,
days_supply string,
new_rx_refill_num string,
ndc_qty string,
NDC_uom_cd string,
 year string,
 file string
)partitioned by (state_cd);

-- COMMAND ----------

--inpt_header
insert into dua_052538_nwi388.inpatient_header_16 
select $inpatient_header from(
select $inpatient_header    from      dua_052538.tafr16_inpatient_header_01    union
select $inpatient_header    from      dua_052538.tafr16_inpatient_header_02    union
select $inpatient_header    from      dua_052538.tafr16_inpatient_header_03    union
select $inpatient_header    from      dua_052538.tafr16_inpatient_header_04    union
select $inpatient_header    from      dua_052538.tafr16_inpatient_header_05    union
select $inpatient_header    from      dua_052538.tafr16_inpatient_header_06    union
select $inpatient_header    from      dua_052538.tafr16_inpatient_header_07    union
select $inpatient_header    from      dua_052538.tafr16_inpatient_header_08    union
select $inpatient_header    from      dua_052538.tafr16_inpatient_header_09    union
select $inpatient_header    from      dua_052538.tafr16_inpatient_header_10    union
select $inpatient_header    from      dua_052538.tafr16_inpatient_header_11    union
select $inpatient_header    from      dua_052538.tafr16_inpatient_header_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','4','inpatient_header_16',current_timestamp(), null);

-- COMMAND ----------

--inpt_line
insert into  dua_052538_nwi388.inpatient_line_16
select $inpatient_line from(
select $inpatient_line from dua_052538.tafr16_inpatient_line_01 union
select $inpatient_line from dua_052538.tafr16_inpatient_line_02 union
select $inpatient_line from dua_052538.tafr16_inpatient_line_03 union
select $inpatient_line from dua_052538.tafr16_inpatient_line_04 union
select $inpatient_line from dua_052538.tafr16_inpatient_line_05 union
select $inpatient_line from dua_052538.tafr16_inpatient_line_06 union
select $inpatient_line from dua_052538.tafr16_inpatient_line_07 union
select $inpatient_line from dua_052538.tafr16_inpatient_line_08 union
select $inpatient_line from dua_052538.tafr16_inpatient_line_09 union
select $inpatient_line from dua_052538.tafr16_inpatient_line_10 union
select $inpatient_line from dua_052538.tafr16_inpatient_line_11 union
select $inpatient_line from dua_052538.tafr16_inpatient_line_12 )
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','5','inpatient_line_16',current_timestamp(), null);

-- COMMAND ----------

--ot_header
insert into dua_052538_nwi388.other_services_header_16
select  $other_services_header from(
select  $other_services_header from dua_052538.tafr16_other_services_header_01 union
select  $other_services_header from dua_052538.tafr16_other_services_header_02 union
select  $other_services_header from dua_052538.tafr16_other_services_header_03 union
select  $other_services_header from dua_052538.tafr16_other_services_header_04 union
select  $other_services_header from dua_052538.tafr16_other_services_header_05 union
select  $other_services_header from dua_052538.tafr16_other_services_header_06 union
select  $other_services_header from dua_052538.tafr16_other_services_header_07 union
select  $other_services_header from dua_052538.tafr16_other_services_header_08 union
select  $other_services_header from dua_052538.tafr16_other_services_header_09 union
select  $other_services_header from dua_052538.tafr16_other_services_header_10 union
select  $other_services_header from dua_052538.tafr16_other_services_header_11 union
select  $other_services_header from dua_052538.tafr16_other_services_header_12 )
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','6','other_services_header_16',current_timestamp(), null);

-- COMMAND ----------

--ot_line
insert into dua_052538_nwi388.other_services_line_16
select $other_services_line from(
select $other_services_line from dua_052538.tafr16_other_services_line_01 union
select $other_services_line from dua_052538.tafr16_other_services_line_02 union
select $other_services_line from dua_052538.tafr16_other_services_line_03 union
select $other_services_line from dua_052538.tafr16_other_services_line_04 union
select $other_services_line from dua_052538.tafr16_other_services_line_05 union
select $other_services_line from dua_052538.tafr16_other_services_line_06 union
select $other_services_line from dua_052538.tafr16_other_services_line_07 union
select $other_services_line from dua_052538.tafr16_other_services_line_08 union
select $other_services_line from dua_052538.tafr16_other_services_line_09 union
select $other_services_line from dua_052538.tafr16_other_services_line_10 union
select $other_services_line from dua_052538.tafr16_other_services_line_11 union
select $other_services_line from dua_052538.tafr16_other_services_line_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','7','other_services_line_16',current_timestamp(), null);

-- COMMAND ----------

--lt_header
insert into dua_052538_nwi388.long_term_header_16
select $long_term_header from(
select $long_term_header from dua_052538.tafr16_long_term_header_01 union
select $long_term_header from dua_052538.tafr16_long_term_header_02 union
select $long_term_header from dua_052538.tafr16_long_term_header_03 union
select $long_term_header from dua_052538.tafr16_long_term_header_04 union
select $long_term_header from dua_052538.tafr16_long_term_header_05 union
select $long_term_header from dua_052538.tafr16_long_term_header_06 union
select $long_term_header from dua_052538.tafr16_long_term_header_07 union
select $long_term_header from dua_052538.tafr16_long_term_header_08 union
select $long_term_header from dua_052538.tafr16_long_term_header_09 union
select $long_term_header from dua_052538.tafr16_long_term_header_10 union
select $long_term_header from dua_052538.tafr16_long_term_header_11 union
select $long_term_header from dua_052538.tafr16_long_term_header_12 )
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','8','long_term_header_16',current_timestamp(), null);

-- COMMAND ----------

--lt_line
insert into dua_052538_nwi388.long_term_line_16
select $long_term_line from(
select $long_term_line from dua_052538.tafr16_long_term_line_01 union
select $long_term_line from dua_052538.tafr16_long_term_line_02 union
select $long_term_line from dua_052538.tafr16_long_term_line_03 union
select $long_term_line from dua_052538.tafr16_long_term_line_04 union
select $long_term_line from dua_052538.tafr16_long_term_line_05 union
select $long_term_line from dua_052538.tafr16_long_term_line_06 union
select $long_term_line from dua_052538.tafr16_long_term_line_07 union
select $long_term_line from dua_052538.tafr16_long_term_line_08 union
select $long_term_line from dua_052538.tafr16_long_term_line_09 union
select $long_term_line from dua_052538.tafr16_long_term_line_10 union
select $long_term_line from dua_052538.tafr16_long_term_line_11 union
select $long_term_line from dua_052538.tafr16_long_term_line_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','9','long_term_line_16',current_timestamp(), null);

-- COMMAND ----------

--rx_header
insert into dua_052538_nwi388.rx_header_16
select $rx_header from(
select $rx_header from dua_052538.tafr16_rx_header_01 union
select $rx_header from dua_052538.tafr16_rx_header_02 union
select $rx_header from dua_052538.tafr16_rx_header_03 union
select $rx_header from dua_052538.tafr16_rx_header_04 union
select $rx_header from dua_052538.tafr16_rx_header_05 union
select $rx_header from dua_052538.tafr16_rx_header_06 union
select $rx_header from dua_052538.tafr16_rx_header_07 union
select $rx_header from dua_052538.tafr16_rx_header_08 union
select $rx_header from dua_052538.tafr16_rx_header_09 union
select $rx_header from dua_052538.tafr16_rx_header_10 union
select $rx_header from dua_052538.tafr16_rx_header_11 union
select $rx_header from dua_052538.tafr16_rx_header_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','10','rx_header_16',current_timestamp(), null);

-- COMMAND ----------

--rx_line
insert into dua_052538_nwi388.rx_line_16
select $rx_line from(
select $rx_line from dua_052538.tafr16_rx_line_01 union
select $rx_line from dua_052538.tafr16_rx_line_02 union
select $rx_line from dua_052538.tafr16_rx_line_03 union
select $rx_line from dua_052538.tafr16_rx_line_04 union
select $rx_line from dua_052538.tafr16_rx_line_05 union
select $rx_line from dua_052538.tafr16_rx_line_06 union
select $rx_line from dua_052538.tafr16_rx_line_07 union
select $rx_line from dua_052538.tafr16_rx_line_08 union
select $rx_line from dua_052538.tafr16_rx_line_09 union
select $rx_line from dua_052538.tafr16_rx_line_10 union
select $rx_line from dua_052538.tafr16_rx_line_11 union
select $rx_line from dua_052538.tafr16_rx_line_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','11','rx_line_16',current_timestamp(), null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','12','end_16',current_timestamp(), null);