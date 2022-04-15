-- Databricks notebook source
insert into dua_052538_nwi388.log values('$job_id','Observation','1','start Observation',current_timestamp(),null);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #server terms
-- MAGIC spark.conf.set("spark.sql.shuffle.partitions",7000);
-- MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "True");

-- COMMAND ----------

--widget
create widget text job_id default "102";

-- COMMAND ----------



insert into dua_052538_nwi388.observation
select
ROW_NUMBER() OVER(
    ORDER BY
      bene_id
  ) as observation_id,
bene_id as person_id,
case when 
concept_id='' then 0
else concept_id end as observation_concept_id,
event_start_date as observation_date,
null as observation_datetime,
32810 as observation_type_concept_id,--ehr
null as value_as_number,
null as value_as_string,
null as value_as_concept_id,
null as qualifier_concept_id,
null as unit_concept_id,
npi,
forign_key as visit_occurrence_id,
null as visit_detail_id,
event_source_value as observation_source_value,
concept_id as observation_source_concept_id,
null as unit_source_value,
null as qualifier_source_value 
from   dua_052538_nwi388.hold_condition_occurrence
where domain_id='Observation'

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Observation','2','condition route to Observation cdm',current_timestamp(),null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Observation','3','end Observation',current_timestamp(),null);