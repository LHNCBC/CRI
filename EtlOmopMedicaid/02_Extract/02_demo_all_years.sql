-- Databricks notebook source
drop table if exists <write_bucket>.demog_elig_dates;
create table <write_bucket>.demog_elig_dates(
BENE_ID bigint,  
ENRLMT_END_DT string,  
ENRLMT_START_DT string,  
RFRNC_YR string,  
STATE_CD string
);

-- COMMAND ----------

drop table if exists <write_bucket>.demog_elig_base;
create table <write_bucket>.demog_elig_base(
AGE string,   
BENE_ID bigint, 
BENE_STATE_CD string, 
BIRTH_DT string,  
DEATH_DT string,  
ETHNCTY_CD string,  
RACE_ETHNCTY_CD string,  
RFRNC_YR string,  
SEX_CD string, 
STATE_CD string,  
bene_cnty_cd string, 
bene_zip_cd string
);

-- COMMAND ----------

--demog_elig_dates
insert into <write_bucket>.demog_elig_dates select $demog_elig_dates from <DUA>.tafr14_demog_elig_dates where bene_id is not null;
insert into <write_bucket>.demog_elig_dates select $demog_elig_dates from <DUA>.tafr15_demog_elig_dates where bene_id is not null;
insert into <write_bucket>.demog_elig_dates select $demog_elig_dates from <DUA>.tafr16_demog_elig_dates where bene_id is not null;
insert into <write_bucket>.demog_elig_dates select $demog_elig_dates from <DUA>.tafr17_demog_elig_dates where bene_id is not null;
insert into <write_bucket>.demog_elig_dates select $demog_elig_dates from <DUA>.tafr18_demog_elig_dates where bene_id is not null;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','EXTRACT','1','demog_elig_dates',current_timestamp() );

-- COMMAND ----------

--demog_elig_base
insert into <write_bucket>.demog_elig_base select $demog_elig_base from <DUA>.tafr14_demog_elig_base where bene_id is not null;
insert into <write_bucket>.demog_elig_base select $demog_elig_base from <DUA>.tafr15_demog_elig_base where bene_id is not null;
insert into <write_bucket>.demog_elig_base select $demog_elig_base from <DUA>.tafr16_demog_elig_base where bene_id is not null;
insert into <write_bucket>.demog_elig_base select $demog_elig_base from <DUA>.tafr17_demog_elig_base where bene_id is not null;
insert into <write_bucket>.demog_elig_base select $demog_elig_base from <DUA>.tafr18_demog_elig_base where bene_id is not null;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','EXTRACT','2','demog_elig_base',current_timestamp() );