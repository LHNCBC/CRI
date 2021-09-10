-- Databricks notebook source
create widget text scope default "1s1m";
create widget text job_id default "'000'";
create widget text state default "VA";


-- COMMAND ----------

--demog elig base
drop table if exists dua_052538_nwi388.demog_elig_base;
create table dua_052538_nwi388.demog_elig_base using delta partitioned by (state_cd) as
select
  *
from(
select * from  dua_052538.tafr18_demog_elig_base where   bene_id is not null union
select * from  dua_052538.tafr17_demog_elig_base where   bene_id is not null union
select * from  dua_052538.tafr16_demog_elig_base where   bene_id is not null 
  );
  
  
optimize dua_052538_nwi388.demog_elig_base zorder by(death_dt);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','FEEDER_delta','2','create view demog_elig_base',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

--demog elig dates
drop table if exists dua_052538_nwi388.demog_elig_dates;
create table dua_052538_nwi388.demog_elig_dates using delta partitioned by (state_cd) as
select
  *
from(
select * from  dua_052538.tafr18_demog_elig_dates where bene_id is not null union
select * from  dua_052538.tafr17_demog_elig_dates where bene_id is not null union
select * from  dua_052538.tafr16_demog_elig_dates where bene_id is not null);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','FEEDER_delta','4','create view demog_elig_dates',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

--create view for inpatient header original tables
drop table if exists dua_052538_nwi388.inpatient_header;
create table dua_052538_nwi388.inpatient_header using delta partitioned by (state_cd) as
select
  *
from(
  select * from dua_052538.tafr18_inpatient_header_06 where bene_id is not null union
  select * from dua_052538.tafr17_inpatient_header_06 where bene_id is not null union
  select * from dua_052538.tafr16_inpatient_header_06 where bene_id is not null 
  );
optimize dua_052538_nwi388.inpatient_header zorder by(clm_id);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','FEEDER_delta','6','create view inpatient_header',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

--inpatient line
drop table if exists dua_052538_nwi388.inpatient_line;
create table dua_052538_nwi388.inpatient_line using delta partitioned by (state_cd) as
select
  *
from(
 select * from  dua_052538.tafr18_inpatient_line_06 where bene_id is not null union
 select * from  dua_052538.tafr17_inpatient_line_06 where bene_id is not null union
 select * from  dua_052538.tafr16_inpatient_line_06 where bene_id is not null
  );
optimize dua_052538_nwi388.inpatient_line zorder by(clm_id);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','FEEDER_delta','8','create view inpatient_line',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

--other services header
drop table if exists dua_052538_nwi388.other_services_header;
create table dua_052538_nwi388.other_services_header using delta partitioned by (state_cd) as
select
  *
from(
 select * from dua_052538.tafr18_other_services_header_06 where bene_id is not null union
select * from dua_052538.tafr17_other_services_header_06 where bene_id is not null union
select * from dua_052538.tafr16_other_services_header_06 where bene_id is not null
  );
optimize dua_052538_nwi388.other_services_header zorder by(clm_id);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','FEEDER_delta','10','create view other_services_line',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

--other services line
drop table if exists dua_052538_nwi388.other_services_line;
create table dua_052538_nwi388.other_services_line using delta partitioned by (state_cd) as
select
  *
from(
select * from dua_052538.tafr18_other_services_line_06 where bene_id is not null union
select * from dua_052538.tafr17_other_services_line_06 where bene_id is not null union
select * from dua_052538.tafr16_other_services_line_06 where bene_id is not null
);
optimize dua_052538_nwi388.other_services_line zorder by(clm_id);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','FEEDER_delta','12','create view other_services_line',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

--other services line for provider
drop table if exists dua_052538_nwi388.other_services_line_p;
create table dua_052538_nwi388.other_services_line_p using delta
select
  clm_id as clm_id_b,
  SRVC_PRVDR_NPI
from(
select * from dua_052538.tafr18_other_services_line_06 where bene_id is not null union
select * from dua_052538.tafr17_other_services_line_06 where bene_id is not null union
select * from dua_052538.tafr16_other_services_line_06 where bene_id is not null
);
optimize dua_052538_nwi388.other_services_line_p zorder by(clm_id_b);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','FEEDER_delta','14','create view other_services_line_p',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

--long term header
drop table if exists dua_052538_nwi388.long_term_header;
create table dua_052538_nwi388.long_term_header using delta partitioned by (state_cd) as
select
  *
from(
select * from  dua_052538.tafr18_long_term_header_06 where bene_id is not null union
select * from  dua_052538.tafr17_long_term_header_06 where bene_id is not null union
select * from  dua_052538.tafr16_long_term_header_06 where bene_id is not null 
);
optimize dua_052538_nwi388.long_term_header zorder by(clm_id);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','FEEDER_delta','16','create view long_term_header',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

--long term line
drop table if exists dua_052538_nwi388.long_term_line;
create table dua_052538_nwi388.long_term_line using delta partitioned by (state_cd) as
select
  *
from(

select * from  dua_052538.tafr18_long_term_line_06 where bene_id is not null union
select * from  dua_052538.tafr17_long_term_line_06 where bene_id is not null union
select * from  dua_052538.tafr16_long_term_line_06 where bene_id is not null 
  
  );
optimize dua_052538_nwi388.long_term_line zorder by(clm_id);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','FEEDER_delta','18','create view long_term_line',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

--rx header
drop table if exists dua_052538_nwi388.rx_header;
create table dua_052538_nwi388.rx_header using delta partitioned by (state_cd) as
select
  *
from(
select * from dua_052538.tafr18_rx_header_06 where bene_id is not null union
select * from dua_052538.tafr17_rx_header_06 where bene_id is not null union
select * from dua_052538.tafr16_rx_header_06 where bene_id is not null
  );
optimize dua_052538_nwi388.rx_header zorder by(clm_id);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','FEEDER_delta','20','create view rx_header',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.rx_header_p;
create table dua_052538_nwi388.rx_header_p using delta as
select
  clm_id as clm_id_b,
  PRSCRBNG_PRVDR_NPI
from(
select * from  dua_052538.tafr18_rx_header_06 where   bene_id is not null union
select * from  dua_052538.tafr17_rx_header_06 where   bene_id is not null union
select * from  dua_052538.tafr16_rx_header_06 where   bene_id is not null 
);
 
optimize dua_052538_nwi388.rx_header_p zorder by(clm_id_b);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','FEEDER_delta','22','create view rx_header_p',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

--RX Line
drop table if exists dua_052538_nwi388.rx_line;
create table dua_052538_nwi388.rx_line using delta partitioned by (state_cd) as
select
  *
from(
select * from dua_052538.tafr18_rx_line_06 where bene_id is not null union
select * from dua_052538.tafr17_rx_line_06 where bene_id is not null union
select * from dua_052538.tafr16_rx_line_06 where bene_id is not null
  );
optimize dua_052538_nwi388.rx_line zorder by(clm_id);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','FEEDER_delta','24','create view rx_line',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

--Medicaid TAF  ends
