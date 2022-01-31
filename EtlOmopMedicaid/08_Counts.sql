-- Databricks notebook source
create widget text job_id default "000";

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','counts','0','start_counts',current_timestamp(),null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '2' as command,
  'demog_elig_base' as task,
  current_timestamp() as time,
   count(*) as row_count
from
  demog_elig_base;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '3' as command,
  'demog_elig_dates' as task,
  current_timestamp() as time,
  count(*) as row_count
from
  demog_elig_dates;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '4' as command,
  'inpatient_header' as task,
  current_timestamp() as time,
  count(*) as row_count
from
  inpatient_header ;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '5' as command,
  'inpatient_line' as task,
  current_timestamp() as time,
  count(*) as row_count
from
  inpatient_line;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '6' as command,
  'other_services_header' as task,
  current_timestamp() as time,
  count(*) as row_count
from
  other_services_header
 ;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '7' as command,
  'other_services_line' as task,
  current_timestamp() as time,
  count(*) as row_count
from
  other_services_line
  roup by state_cd;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '8' as command,
  'RX_header' as task,
  current_timestamp() as time,
  count(*) as row_count
from
  RX_header
 ;

-- COMMAND ----------

 into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '9' as command,
  'RX_line' as task,
  current_timestamp() as time,
   count(*) as row_count
from
  RX_line
  ;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '10' as command,
  'long_term_header' as task,
  current_timestamp() as time,
    count(*) as row_count
from
  long_term_header
  ;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '11' as command,
  'long_term_line' as task,
  current_timestamp() as time,
   count(*) as row_count
from
  long_term_line;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '12' as command,
  'person' as task,
  current_timestamp() as time,
  count(*) as row_count
from
  dua_052538_nwi388.person;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '13' as command,
  'observation_period' as task,
  current_timestamp() as time,
   count(*) as row_count
from
  dua_052538_nwi388.observation_period;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '14' as command,
  'provider' as task,
  current_timestamp() as time,
  
  
  count(*) as row_count
from
  dua_052538_nwi388.provider;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '15' as command,
  'visit_occurrence' as task,
  current_timestamp() as time,
  
  
  count(*) as row_count
from
  dua_052538_nwi388.visit_occurrence;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '16' as command,
  'procedure_occurrence' as task,
  current_timestamp() as time,
  
  
  count(*) as row_count
from
  dua_052538_nwi388.procedure_occurrence;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '17' as command,
  'condition_occurrence' as task,
  current_timestamp() as time,
  
  
  count(*) as row_count
from
  dua_052538_nwi388.condition_occurrence;

-- COMMAND ----------

insert into
  dua_052538_nwi388.log
select
  '$job_id' as job_id,
  'counts' as notebook,
  '18' as command,
  'drug_exposure' as task,
  current_timestamp() as time,
  
  
  count(*) as row_count
from
  dua_052538_nwi388.drug_exposure;