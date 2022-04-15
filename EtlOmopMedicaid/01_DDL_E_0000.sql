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

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT_DDL','1','extract ddl end',current_timestamp(), null);



--demog_elig_dates
insert into dua_052538_nwi388.demog_elig_dates select $demog_elig_dates from dua_052538.tafr14_demog_elig_dates where bene_id is not null;
insert into dua_052538_nwi388.demog_elig_dates select $demog_elig_dates from dua_052538.tafr15_demog_elig_dates where bene_id is not null;
insert into dua_052538_nwi388.demog_elig_dates select $demog_elig_dates from dua_052538.tafr16_demog_elig_dates where bene_id is not null;
insert into dua_052538_nwi388.demog_elig_dates select $demog_elig_dates from dua_052538.tafr17_demog_elig_dates where bene_id is not null;
insert into dua_052538_nwi388.demog_elig_dates select $demog_elig_dates from dua_052538.tafr18_demog_elig_dates where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','2','demog_elig_dates',current_timestamp(), null);

-- COMMAND ----------

--demog_elig_base
insert into dua_052538_nwi388.demog_elig_base select $demog_elig_base from dua_052538.tafr14_demog_elig_base where bene_id is not null;
insert into dua_052538_nwi388.demog_elig_base select $demog_elig_base from dua_052538.tafr15_demog_elig_base where bene_id is not null;
insert into dua_052538_nwi388.demog_elig_base select $demog_elig_base from dua_052538.tafr16_demog_elig_base where bene_id is not null;
insert into dua_052538_nwi388.demog_elig_base select $demog_elig_base from dua_052538.tafr17_demog_elig_base where bene_id is not null;
insert into dua_052538_nwi388.demog_elig_base select $demog_elig_base from dua_052538.tafr18_demog_elig_base where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','2','demog_elig_base',current_timestamp(), null);
