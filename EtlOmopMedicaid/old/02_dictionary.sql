-- Databricks notebook source
-- MAGIC %r
-- MAGIC #load athena from DB file store
-- MAGIC #do not do this if you do not want to update ATHENA!!!
-- MAGIC 
-- MAGIC 
-- MAGIC #note there are two versions of athena in the ETL at this time, one has ICD10CM and the other does not
-- MAGIC #this means that concept table is good for tagging dx-px-rx with concept codes but relaitonship file will not hit dx codes
-- MAGIC 
-- MAGIC library(sparklyr)
-- MAGIC sc<-spark_connect(method='databricks')

-- COMMAND ----------

-- MAGIC %r
-- MAGIC 
-- MAGIC cr_schema<-c(concept_id_1="integer",
-- MAGIC            concept_id_2="integer",
-- MAGIC            relationship_id="character",
-- MAGIC            valid_start_date="date",
-- MAGIC            valid_end_date="date",
-- MAGIC            invalid_reason="character")
-- MAGIC 
-- MAGIC CONCEPT_RELATIONSHIP<-spark_read_csv(sc, "dbfs:/FileStore/shared_uploads/NWI388/CONCEPT_RELATIONSHIP-1.csv",
-- MAGIC                                      memory = FALSE,
-- MAGIC                                      header = TRUE,
-- MAGIC                                      columns=cr_schema,
-- MAGIC                                      delimiter = ",")
-- MAGIC                                      
-- MAGIC sdf_register(CONCEPT_RELATIONSHIP,"CONCEPT_RELATIONSHIP");

-- COMMAND ----------

insert into
  dua_052538_nwi388.CONCEPT_RELATIONSHIP
select
  *
from
  CONCEPT_RELATIONSHIP;
  

-- COMMAND ----------

-- MAGIC %r
-- MAGIC concept_schema<-c(concept_id="integer",
-- MAGIC                  concept_name="character",
-- MAGIC                  domain_id="character",
-- MAGIC                  vocabulary_id="character",
-- MAGIC                  concept_class_id="character",
-- MAGIC                  standard_concept="character",
-- MAGIC                  concept_code="character",
-- MAGIC                  valid_start_date="date",
-- MAGIC                  valid_end_date="date",
-- MAGIC                  invalid_reason="character")
-- MAGIC 
-- MAGIC CONCEPT<-spark_read_csv(sc,
-- MAGIC                         "dbfs:/FileStore/shared_uploads/NWI388/CONCEPT-2.csv",
-- MAGIC                         memory = FALSE,
-- MAGIC                         header = TRUE,
-- MAGIC                         columns=concept_schema,
-- MAGIC                         delimiter = "\t",)
-- MAGIC sdf_register(CONCEPT,"CONCEPT");

-- COMMAND ----------

--Athena provides dx-px-rx code to ohdsi concept ids
insert into
  dua_052538_NWI388.concept
select
  *
from
  CONCEPT;

-- COMMAND ----------

-- MAGIC %r
-- MAGIC CONCEPT_ANCESTOR<-spark_read_csv(sc,
-- MAGIC                                  "dbfs:/FileStore/shared_uploads/NWI388/CONCEPT_ANCESTOR.csv",
-- MAGIC                                  memory = FALSE,
-- MAGIC                                  header = TRUE,
-- MAGIC                                  infer_schema = TRUE,
-- MAGIC                                  delimiter = ",", )
-- MAGIC                                  
-- MAGIC sdf_register(CONCEPT_ANCESTOR,"CONCEPT_ANCESTOR");

-- COMMAND ----------

insert into
  dua_052538_nwi388.CONCEPT_ANCESTOR
select
  *
from
  CONCEPT_ANCESTOR;


-- COMMAND ----------

-- MAGIC %r
-- MAGIC RELATIONSHIP<-spark_read_csv(sc,
-- MAGIC                              "dbfs:/FileStore/shared_uploads/NWI388/RELATIONSHIP.csv",
-- MAGIC                              memory = FALSE,
-- MAGIC                              header = TRUE,
-- MAGIC                              infer_schema = TRUE,
-- MAGIC                              delimiter = ",", )
-- MAGIC                              
-- MAGIC sdf_register(RELATIONSHIP,"RELATIONSHIP");

-- COMMAND ----------


insert into
  dua_052538_nwi388.RELATIONSHIP
select
  *
from
  RELATIONSHIP;

-- COMMAND ----------

-- MAGIC %r
-- MAGIC VOCABULARY<-spark_read_csv(sc, 
-- MAGIC                            "dbfs:/FileStore/shared_uploads/NWI388/VOCABULARY.csv",
-- MAGIC                            memory = FALSE,
-- MAGIC                            header = TRUE,
-- MAGIC                            infer_schema = TRUE,
-- MAGIC                            delimiter = ",",)
-- MAGIC 
-- MAGIC 
-- MAGIC sdf_register(VOCABULARY,"VOCABULARY");

-- COMMAND ----------

insert into
  dua_052538_nwi388.VOCABULARY
select
  *
from
  VOCABULARY;

-- COMMAND ----------

-- MAGIC 
-- MAGIC %r
-- MAGIC #px handshake
-- MAGIC 
-- MAGIC handshake<-c(vocabulary_id_a="character",vocabulary_id="character")
-- MAGIC px_handshake<-spark_read_csv(sc,
-- MAGIC                              "dbfs:/FileStore/shared_uploads/NWI388/px_handshake-1.csv",
-- MAGIC                              memory = FALSE,
-- MAGIC                              header = TRUE,
-- MAGIC                              columns=handshake,
-- MAGIC                              delimiter = "," )
-- MAGIC                              
-- MAGIC sdf_register(px_handshake,"px_handshake");

-- COMMAND ----------

drop table if exists  dua_052538_nwi388.px_handshake;
create table  dua_052538_nwi388.px_handshake as
select * from px_handshake;


-- COMMAND ----------

-- MAGIC %r
-- MAGIC 
-- MAGIC #inport a
-- MAGIC 
-- MAGIC concept_schema<-c(concept_id="integer",
-- MAGIC                  concept_name="character",
-- MAGIC                  domain_id="character",
-- MAGIC                  vocabulary_id="character",
-- MAGIC                  concept_class_id="character",
-- MAGIC                  standard_concept="character",
-- MAGIC                  concept_code="character",
-- MAGIC                  valid_start_date="date",
-- MAGIC                  valid_end_date="date",
-- MAGIC                  invalid_reason="character")
-- MAGIC 
-- MAGIC CONCEPT_ccw<-spark_read_csv(sc,
-- MAGIC                         "dbfs:/FileStore/shared_uploads/NWI388/ccw_concept.csv",
-- MAGIC                         memory = FALSE,
-- MAGIC                         header = TRUE,
-- MAGIC                         columns=concept_schema,
-- MAGIC                         delimiter = ",", )
-- MAGIC sdf_register(CONCEPT_ccw,"CONCEPT_ccw");
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC #import b
-- MAGIC 
-- MAGIC cr_schema<-c(concept_id_1="integer",
-- MAGIC            concept_id_2="integer",
-- MAGIC            relationship_id="character",
-- MAGIC            valid_start_date="date",
-- MAGIC            valid_end_date="date",
-- MAGIC            invalid_reason="character")
-- MAGIC 
-- MAGIC CONCEPT_RELATIONSHIP_ccw<-spark_read_csv(sc, 
-- MAGIC                                      "dbfs:/FileStore/shared_uploads/NWI388/ccw_relationship.csv",
-- MAGIC                                      memory = FALSE,
-- MAGIC                                      header = TRUE,
-- MAGIC                                      columns=cr_schema,
-- MAGIC                                      delimiter = ",")
-- MAGIC                                      
-- MAGIC                              
-- MAGIC sdf_register(CONCEPT_RELATIONSHIP_ccw,"CONCEPT_RELATIONSHIP_ccw");

-- COMMAND ----------

insert into dua_052538_nwi388.concept
select * from 
CONCEPT_ccw;

insert into dua_052538_nwi388.concept_relationship
select * from
CONCEPT_RELATIONSHIP_ccw


-- COMMAND ----------

--NPI below this point

-- COMMAND ----------

-- MAGIC 
-- MAGIC %r
-- MAGIC #load places npi
-- MAGIC #library(sparklyr)
-- MAGIC #sc<-spark_connect(method='databricks')
-- MAGIC #NPI_place<-spark_read_csv(sc,"dbfs:/FileStore/shared_uploads/NWI388/npi_place.csv", 
-- MAGIC #memory = FALSE,header = TRUE,infer_schema = TRUE,delimiter = ",")
-- MAGIC #head (NPI_place)
-- MAGIC #sdf_register(NPI_place,"NPI_place")

-- COMMAND ----------

-- MAGIC 
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


--parse npi into spark sql view
--Provider name and care site are learnded from this dictionary
--drop view if exists NPI_provider;
--create view NPI_provider as
--select
--  *
--from
--  dua_052538_nwi388.NPI;

-- COMMAND ----------

-- MAGIC 
-- MAGIC 
-- MAGIC %r
-- MAGIC #load places npi
-- MAGIC #library(sparklyr)
-- MAGIC #sc<-spark_connect(method='databricks')
-- MAGIC #NPI_place<-spark_read_csv(sc,"dbfs:/FileStore/shared_uploads/NWI388/npi_place.csv", 
-- MAGIC #memory = FALSE,header = TRUE,infer_schema = TRUE,delimiter = ",")
-- MAGIC #head (NPI_place)
-- MAGIC #sdf_register(NPI_place,"NPI_place")

-- COMMAND ----------


--drop table if exists dua_052538_NWI388.NPI_place;
--create table dua_052538_NWI388.NPI_place using delta as
--select
--  *
--from
--  NPI_place;

-- COMMAND ----------


--load CCW dictionary
--Provider specialty to CDM codes
--revenue center to CDM codes
--care site POS/TOS to CDM codes
