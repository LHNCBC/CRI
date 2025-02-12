-- Databricks notebook source
insert into <write_bucket>.log values('$job_id','care site','1','start care site',current_timestamp() );

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #server terms
-- MAGIC spark.conf.set("spark.sql.shuffle.partitions",7000);
-- MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "True");

-- COMMAND ----------


drop table if exists  <write_bucket>.hold_care_site ;
create table <write_bucket>.hold_care_site as
select
npi,
Provider_Organization_Name_Legal_Business_Name,
Healthcare_Provider_Taxonomy_Group_1,
id,
address_1,
address_2,
city,
state,
zip
from hold_location_b
left join
<write_bucket>.npi_provider
on
id=npi
where entity_type_code=2;


-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','care site','2','create hold care site',current_timestamp() );

-- COMMAND ----------

drop view if exists route_care_site;
create view route_care_site
as
select
ROW_NUMBER() OVER (
    ORDER BY
      (npi)
) as care_site_id,
Provider_Organization_Name_Legal_Business_Name as care_site_name,
null as place_of_service_concept_id,
b.location_id,
null as care_site_source_value,
Healthcare_Provider_Taxonomy_Group_1 as place_of_service_source_value
from
<write_bucket>.hold_care_site a
left join
<write_bucket>.location b
on
a.address_1=b.address_1 and
a.address_2=b.address_2 and
a.city=b.city and
a.state=b.state and
a.zip=b.zip;


drop view if exists route_care_site1;
create view route_care_site1
as
select
ROW_NUMBER() OVER (
    ORDER BY
      (npi)
) as care_site_id,
npi,
b.location_id
from
<write_bucket>.hold_care_site a
left join
<write_bucket>.location b
on
a.address_1=b.address_1 and
a.address_2=b.address_2 and
a.city=b.city and
a.state=b.state and
a.zip=b.zip;



-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','care site','3','create route care site',current_timestamp() );

-- COMMAND ----------


insert into <write_bucket>.care_site
select
 care_site_id,
  care_site_name,
  place_of_service_concept_id,
  location_id,
  care_site_source_value,
  place_of_service_source_value

from route_care_site
;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','care site','4','route care site to care site cdm',current_timestamp() );

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','care site','5','end care site',current_timestamp() );