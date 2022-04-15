-- Databricks notebook source
insert into dua_052538_nwi388.log values('$job_id','Measurement','1','start measurement',current_timestamp(),null);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #server terms
-- MAGIC spark.conf.set("spark.sql.shuffle.partitions",7000);
-- MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "True");

-- COMMAND ----------

--widget
create widget text job_id default "102";


-- COMMAND ----------

 
 insert into dua_052538_nwi388.measurement
 select
 ROW_NUMBER() OVER(
    ORDER BY
      bene_id
  ) as measurement_id,
  bene_id as person_id,
  case when--case when
  concept_id ='' then 0
  else concept_id end as measurement_concept_id,
  event_start_date as measurement_date,
  null as measurement_time,
  32810 as measurement_type_concept_id,--ehr
  null as operator_concept_id,
  null as value_as_number,
  null as value_as_concept_id,
  null as value_as_string,
  null as unit_concept_id,
  null as range_low,
  null as range_high,
  npi,
 forign_key as visit_occurrence_id,
  null as visit_detail_id,
  event_source_value as measurement_source_value,
  concept_id as measurement_source_concept_id,
  null as unit_source_value,
  null as value_source_value
from
    dua_052538_nwi388.hold_condition_occurrence
where
  domain_id = 'Measurement'

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Measurement','2','hold condition to measurement cdm',current_timestamp(),null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Measurement','3','end measurement',current_timestamp(),null);