-- Databricks notebook source

--Provider contains the same provider-specialty-location pair for every year; this will make providers more distinct.
drop table if exists <write_bucket>.provider1; 
create table <write_bucket>.provider1 as
select 
distinct npi,
provider_id,
  provider_name,
  dea,
  specialty_concept_id,
  care_site_id,
  year_of_birth,
  gender_concept_id,
  provider_source_value,
  specialty_source_value,
  specialty_source_concept_id,
  gender_source_value,
  gender_source_concept_id

from <write_bucket>.provider
group by
npi,
provider_id,
  provider_name,
  dea,
  specialty_concept_id,
  care_site_id,
  year_of_birth,
  gender_concept_id,
  provider_source_value,
  specialty_source_value,
  specialty_source_concept_id,
  gender_source_value,
  gender_source_concept_id;
  
  drop table <write_bucket>.provider;
  
  create table  <write_bucket>.provider as
  select * from  <write_bucket>.provider1;
  
  drop table  <write_bucket>.provider1;

-- COMMAND ----------

--care site contains all CMS care sites, not just distinct taf care sites
drop view if exists provider_care_site;
create view provider_care_site as
select
distinct care_site_id as care_site_id1 from <write_bucket>.provider;

create table <write_bucket>.care_site1 as
select 
care_site_id,
care_site_name,
place_of_service_concept_id,
location_id,
care_site_source_value,
place_of_service_source_value

from 
<write_bucket>.care_site a
inner join
provider_care_site b
on
a.care_site_id=b.care_site_id1;

drop view provider_care_site;
drop table <write_bucket>.care_site;

create table <write_bucket>.care_site as select * from <write_bucket>.care_site1;
drop table <write_bucket>.care_site1; 


-- COMMAND ----------

--location contains all CMS locations, not just distinct taf locations
drop view if exists care_site_location;
create view care_site_location as
select
distinct location_id as location_id1 from <write_bucket>.care_site;

create table <write_bucket>.location1 as
select 
location_id,
address_1,
address_2,
city,
state,
zip,
county,
location_source_value

from 
<write_bucket>.location a
inner join
care_site_location b
on
a.location_id=b.location_id1;

drop view care_site_location;
drop table <write_bucket>.location;

create table <write_bucket>.location as select * from <write_bucket>.location1;
drop table <write_bucket>.location1; 

