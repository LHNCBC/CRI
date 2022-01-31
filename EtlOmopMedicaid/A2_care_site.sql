-- Databricks notebook source
insert into dua_052538_nwi388.log values('$job_id','care site','1','start care site',current_timestamp(), null);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #server terms
-- MAGIC spark.conf.set("spark.sql.shuffle.partitions",7000);
-- MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "True");

-- COMMAND ----------

--widget
create widget text job_id default "102";

-- COMMAND ----------


drop table if exists  dua_052538_nwi388.hold_care_site ;
create table dua_052538_nwi388.hold_care_site as
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
dua_052538_nwi388.npi_provider
on
id=npi
where entity_type_code=2;


-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','care site','2','create hold care site',current_timestamp(), null);

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
dua_052538_nwi388.hold_care_site a
left join
dua_052538_nwi388.location b
on
a.address_1=b.address_1 and
a.address_2=b.address_2 and
a.city=b.city and
a.state=b.state and
a.zip=b.zip;



-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','care site','3','create route care site',current_timestamp(), null);

-- COMMAND ----------


insert into dua_052538_nwi388.care_site
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

insert into dua_052538_nwi388.log values('$job_id','care site','4','route care site to care site cdm',current_timestamp(), null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','care site','5','end care site',current_timestamp(), null);