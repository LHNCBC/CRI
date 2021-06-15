-- Databricks notebook source
--Medicaid TAF Begins

-- COMMAND ----------

--DEMOG BASE
drop view if exists demog_elig_base;
create view demog_elig_base as
select
  *
from
  dua_052538.tafr18_demog_elig_base
where
  state_cd = "WI"
  and bene_id is not null;

-- COMMAND ----------

--DEMOG ELIG
drop view if exists demog_elig;
create view demog_elig as
select
  *
from
  dua_052538.tafr18_demog_elig_dates
where
  state_cd = "WI"
  and bene_id is not null;

-- COMMAND ----------

--Inpatient Header
drop view if exists inpatient_header;
create view inpatient_header as
select
  *
from
  dua_052538.tafr18_inpatient_header_06
where
  state_cd = "WI"
  and bene_id is not null;

-- COMMAND ----------

--Inpatient Line
drop view if exists inpatient_line;
create view inpatient_line as
select
  *
from
  dua_052538.tafr18_inpatient_line_06
where
  state_cd = "WI"
  and bene_id is not null;

-- COMMAND ----------

--Other Services Header
drop view if exists other_services_header;
create view other_services_header as
select
  *
from
  dua_052538.tafr18_other_services_header_06
where
  state_cd = "WI"
  and bene_id is not null;

-- COMMAND ----------

--Other Services Line
drop view if exists other_services_line;
create view other_services_line as
select
  *
from
  dua_052538.tafr18_other_services_line_06
where
  state_cd = "WI"
  and bene_id is not null;

-- COMMAND ----------

--Other_services_line_for_provider
--note should the header view add these two values?
drop view if exists other_services_line_p;
create view other_services_line_p as
select clm_id as clm_id_b,SRVC_PRVDR_NPI 
from dua_052538.tafr18_other_services_line_06
where
  state_cd = "WI"
  and bene_id is not null;

-- COMMAND ----------

--Long Term Header
drop view if exists long_term_header;
create view long_term_header as
select
  *
from
  dua_052538.tafr18_long_term_header_06
where
  state_cd = "WI"
  and bene_id is not null;

-- COMMAND ----------

--Long Term Line
drop view if exists long_term_line;
create view long_term_line as
select
  *
from
  dua_052538.tafr18_long_term_line_06
where
  state_cd = "WI"
  and bene_id is not null;

-- COMMAND ----------

--RX Header
drop view if exists rx_header;
create view rx_header as
select
  *
from
  dua_052538.tafr18_rx_header_06
where
  state_cd = "WI"
  and bene_id is not null;

-- COMMAND ----------

drop view if exists rx_header_p;
create view rx_header_p as
select
  clm_id as clm_id_b,
  PRSCRBNG_PRVDR_NPI
from
  dua_052538.tafr18_rx_header_06
where
  state_cd = "WI"
  and bene_id is not null;

-- COMMAND ----------

--RX Line
drop view if exists rx_line;
create view rx_line as
select
  *
from
  dua_052538.tafr18_rx_line_06
where
  state_cd = "WI"
  and bene_id is not null;

-- COMMAND ----------

--Medicaid TAF  ends

-- COMMAND ----------

--Dictionary  begins

-- COMMAND ----------

-- MAGIC %r
-- MAGIC #load athena from DB file store
-- MAGIC #do not do this if you do not want to update ATHENA!!!
-- MAGIC library(sparklyr)
-- MAGIC sc<-spark_connect(method='databricks')
-- MAGIC 
-- MAGIC CONCEPT_RELATIONSHIP<-spark_read_csv(sc, "dbfs:/FileStore/shared_uploads/NWI388/CONCEPT_RELATIONSHIP-1.csv",memory = FALSE,header = TRUE,infer_schema = TRUE, delimiter = ",",)
-- MAGIC CONCEPT<-spark_read_csv(sc, "dbfs:/FileStore/shared_uploads/NWI388/CONCEPT-2.csv",memory = FALSE,header = TRUE,infer_schema = TRUE,delimiter = "\t",)
-- MAGIC CONCEPT_ANCESTOR<-spark_read_csv(sc, "dbfs:/FileStore/shared_uploads/NWI388/CONCEPT_ANCESTOR.csv",memory = FALSE,header = TRUE,infer_schema = TRUE,delimiter = ",", )
-- MAGIC RELATIONSHIP<-spark_read_csv(sc, "dbfs:/FileStore/shared_uploads/NWI388/RELATIONSHIP.csv",memory = FALSE,header = TRUE, infer_schema = TRUE,delimiter = ",", )
-- MAGIC VOCABULARY<-spark_read_csv(sc, "dbfs:/FileStore/shared_uploads/NWI388/VOCABULARY.csv",memory = FALSE,header = TRUE,infer_schema = TRUE,delimiter = ",",)
-- MAGIC 
-- MAGIC sdf_register(CONCEPT_RELATIONSHIP,"CONCEPT_RELATIONSHIP");
-- MAGIC sdf_register(CONCEPT,"CONCEPT");
-- MAGIC sdf_register(CONCEPT_ANCESTOR,"CONCEPT_ANCESTOR");
-- MAGIC sdf_register(RELATIONSHIP,"RELATIONSHIP");
-- MAGIC sdf_register(VOCABULARY,"VOCABULARY");

-- COMMAND ----------

--Athena provides px-dx-rx code to ohdsi concept ids
--note filename_x means the destination file is not final, and data types need to be fixed in the dictionaries
insert into
  dua_052538_NWI388.concept_x
select
  *
from
  CONCEPT;
insert into
  dua_052538_nwi388.CONCEPT_RELATIONSHIP_x
select
  *
from
  CONCEPT_RELATIONSHIP;
insert into
  dua_052538_nwi388.RELATIONSHIP_x
select
  *
from
  RELATIONSHIP;
insert into
  dua_052538_nwi388.CONCEPT_ANCESTOR_x
select
  *
from
  CONCEPT_ANCESTOR;
insert into
  dua_052538_nwi388.VOCABULARY_x
select
  *
from
  VOCABULARY;

-- COMMAND ----------

--parse athena into vocaulary level views
--these are used for vocabulary testing
drop view if exists lkup_NDC;
create view lkup_NDC as
select
  *
from
  dua_052538_nwi388.concept_x
where
  vocabulary_id = 'NDC';
drop view if exists lkup_ICD10CM;
create view lkup_ICD10CM as
select
  concept_id,
  concept_name,
  domain_id,
  vocabulary_id,
  concept_class_id,
  standard_concept,
  REPLACE(concept_code, '.', '') as concept_code,
  valid_start_date,
  valid_end_date,
  invalid_reason
from
  dua_052538_nwi388.concept_x
where
  vocabulary_id = 'ICD10CM'
union
select
  *
from
  dua_052538_nwi388.concept_x
where
  vocabulary_id = 'DRG';
drop view if exists lkup_cmspos;
create view lkup_cmspos as
select
  *
from
  dua_052538_nwi388.concept_x
where
  vocabulary_id = 'CMS Place of Service';
drop view if exists lkup_ICD10PCS;
create view lkup_ICD10PCS as
select
  *
from
  dua_052538_nwi388.concept_x
where
  vocabulary_id = 'ICD10PCS'
  and concept_class_id = 'ICD10PCS';
drop view if exists lkup_ICD9CM;
create view lkup_ICD9CM as
select
  *
from
  dua_052538_nwi388.concept_x
where
  vocabulary_id = 'ICD9CM';
drop view if exists lkup_ICD9PROC;
create view lkup_ICD9PROC as
select
  *
from
  dua_052538_nwi388.concept_x
where
  vocabulary_id = 'ICD9Proc';
drop view if exists lkup_CMSPLACEOFSERVICE;
create view lkup_CMSPLACEOFSERVICE as
select
  *
from
  dua_052538_nwi388.concept_x
where
  vocabulary_id = 'CMS Place of Service';
drop view if exists lkup_HCPCS;
create view lkup_HCPCS as
select
  *
from
  dua_052538_nwi388.concept_x
where
  vocabulary_id = 'HCPCS';
--device domain-route to device table?
  drop view if exists lkup_CPT4;
create view lkup_CPT4 as
select
  *
from
  dua_052538_nwi388.concept_x
where
  vocabulary_id = 'CPT4';
drop view if exists lkup_DRG;
create view lkup_DRG as
select
  *
from
  dua_052538_nwi388.concept_x
where
  vocabulary_id = 'DRG'
  and concept_class_id = 'MS-DRG';
drop view if exists lkup_hcps_cpt4;
create view lkup_hcps_cpt4 as
select
  *
from
  lkup_HCPCS
union
select
  *
from
  lkup_CPT4;

-- COMMAND ----------

-- MAGIC %r
-- MAGIC 
-- MAGIC #load NPI from DB filestore
-- MAGIC #do not do this if you do not want to update NPI
-- MAGIC #Note this is a 'slim' version of NPI which only contains non-repetative values
-- MAGIC #Note DB will not accept upload larger than 2 gigs ish- full npi is 8 gigs per month file
-- MAGIC 
-- MAGIC #library(sparklyr)
-- MAGIC #sc<-spark_connect(method='databricks')
-- MAGIC 
-- MAGIC #NPI<-spark_read_csv(sc,"dbfs:/FileStore/shared_uploads/NWI388/npidata_pfile.csv", 
-- MAGIC #                          
-- MAGIC #                           memory = FALSE,
-- MAGIC #                           header = TRUE,
-- MAGIC #                           infer_schema = TRUE,
-- MAGIC #                           delimiter = ","
-- MAGIC #                          )
-- MAGIC #sdf_register(NPI,"NPI")

-- COMMAND ----------

--drop table if exists dua_052538_nwi388.NPI;
--create table  dua_052538_nwi388.NPI using delta as select * from NPI;

-- COMMAND ----------

--parse npi into spark sql view
--Provider name and care site are learnded from this dictionary
create view NPI_provider as
select
  *
from
  dua_052538_nwi388.NPI;

-- COMMAND ----------

-- MAGIC %r
-- MAGIC #load places npi
-- MAGIC library(sparklyr)
-- MAGIC sc<-spark_connect(method='databricks')
-- MAGIC NPI_place<-spark_read_csv(sc,"dbfs:/FileStore/shared_uploads/NWI388/npi_place.csv", 
-- MAGIC memory = FALSE,header = TRUE,infer_schema = TRUE,delimiter = ",")
-- MAGIC head (NPI_place)
-- MAGIC sdf_register(NPI_place,"NPI_place")

-- COMMAND ----------

create table dua_052538_NWI388.NPI_place using delta as
select
  *
from
  NPI_place;

-- COMMAND ----------

--load CCW dictionary
--Provider specialty to CDM codes
--revenue center to CDM codes
--care site POS/TOS to CDM codes

-- COMMAND ----------

--Dictionary ends

-- COMMAND ----------

