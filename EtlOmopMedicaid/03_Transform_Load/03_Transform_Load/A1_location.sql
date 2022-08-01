-- Databricks notebook source
insert into <write_bucket>.log values('$job_id','Location','1','start location',current_timestamp() );

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #server terms
-- MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "True");
-- MAGIC spark.conf.set("spark.sql.shuffle.partitions",8000)

-- COMMAND ----------

drop table if exists <write_bucket>.hold_location;
create table <write_bucket>.hold_location (
  id string,
  address_1 string,
  address_2 string,
  city string,
  state string,
  county string,
  zip string,
  location_source_value string,
  type string,
  RFRNC_YR string
);

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','create hold location','2','start location',current_timestamp() );

-- COMMAND ----------

drop view if exists hold_location_a;
create view hold_location_a as
select
  bene_id as id,
  null as address_1,
  null as address_2,
  null as city,
  bene_state_cd as state,
  bene_cnty_cd as county,
  bene_zip_cd as zip,
  "demog_elig_base" as location_source_value,
  "demog" as type,
  RFRNC_YR
from
  <write_bucket>.demog_elig_base;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Location','3','create hold location a',current_timestamp() );

-- COMMAND ----------

drop view if exists hold_location_b;
create view hold_location_b as
select
  npi as id,
  Provider_First_Line_Business_Practice_Location_Address as address_1,
  Provider_Second_Line_Business_Practice_Location_Address as address_2,
  Provider_Business_Practice_Location_Address_City_Name as city,
  Provider_Business_Practice_Location_Address_State_Name as state,
  null as county,
  Provider_Business_Practice_Location_Address_Postal_Code as zip,
  "npi_place" as location_source_value,
  entity_type_code as type,
  null as RFRNC_YR
from
  <write_bucket>.npi_place;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Location','4','create hold location b',current_timestamp() );

-- COMMAND ----------

insert into
  <write_bucket>.hold_location
select
  distinct id,
  address_1,
  address_2,
  city,
  state,
  county,
  zip,
  location_source_value,
  type,
  RFRNC_YR
from
  hold_location_a
group by
  id,
  address_1,
  address_2,
  city,
  state,
  county,
  zip,
  location_source_value,
  type,
  RFRNC_YR;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Location','5','hold location a to hold location',current_timestamp() );

-- COMMAND ----------

insert into
  <write_bucket>.hold_location
select
  distinct id,
  address_1,
  address_2,
  city,
  state,
  county,
  zip,
  location_source_value,
  type,
  RFRNC_YR
from
  hold_location_b
where
  type = 2
group by
  id,
  address_1,
  address_2,
  city,
  state,
  county,
  zip,
  location_source_value,
  type,
  RFRNC_YR;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Location','6','hold location b to hold location',current_timestamp() );

-- COMMAND ----------

drop view if exists transform_location;
create view transform_location as
select
  distinct zip,
  ROW_NUMBER() OVER (
    ORDER BY
      (zip)
  ) as location_id,
  address_1,
  address_2,
  city,
  state,
  county
from
  <write_bucket>.hold_location
group by
  zip,
  county,
  state,
  city,
  address_2,
  address_1,
  location_source_value
order by
  zip desc;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Location','7','create transform location',current_timestamp() );

-- COMMAND ----------

insert into
  <write_bucket>.location
select
  location_id,
  address_1,
  address_2,
  city,
  state,
  zip,
  county,
  null as location_source_value
from
  transform_location;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Location','8','transform location to final cdm',current_timestamp() );

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Location','9','end location',current_timestamp() );