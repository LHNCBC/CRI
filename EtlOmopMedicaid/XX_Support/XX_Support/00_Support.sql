-- Databricks notebook source

--start log

-- COMMAND ----------


drop table if exists <write_bucket>.log;
create table <write_bucket>.log
(
job_id string,
notebook string,
command string,
task string,
time string);


-- COMMAND ----------

--end log

-- COMMAND ----------

--Athena DDL Starts

-- COMMAND ----------

drop table if exists <write_bucket>.METADATA;
create table <write_bucket>.METADATA (
  metadata_concept_id bigint,
  metadata_type_concept_id bigint,
  name string,
  value_as_string string,
  value_as_concept_id bigint,
  metadata_date date,
  metadata_datetime string
);

-- COMMAND ----------

drop table if exists <write_bucket>.CDM_SOURCE;
create table <write_bucket>.CDM_SOURCE (
  cdm_source_name string,
  cdm_source_abbreviation string,
  cdm_holder string,
  source_description string,
  source_documentation_reference string,
  cdm_etl_reference string,
  source_release_date date,
  cdm_release_date date,
  cdm_version string,
  vocabulary_version string
);

-- COMMAND ----------

drop table if exists <write_bucket>.CONCEPT;
create table <write_bucket>.CONCEPT


(
  concept_id bigint,
  concept_name string,
  domain_id string,
  vocabulary_id string,
  concept_class_id string,
  standard_concept string,
  concept_code string,
  valid_start_date date,
  valid_end_date date,
  invalid_reason string
);



-- COMMAND ----------

drop table if exists <write_bucket>.VOCABULARY;
create table <write_bucket>.VOCABULARY (
  vocabulary_id string,
  vocabulary_name string,
  vocabulary_reference string,
  vocabulary_version string,
  vocabulary_concept_id bigint
);

-- COMMAND ----------

drop table if exists <write_bucket>.DOMAIN;
create table <write_bucket>.DOMAIN (
  domain_id string,
  domain_name string,
  domain_concept_id bigint
);

-- COMMAND ----------

drop table if exists <write_bucket>.CONCEPT_CLASS;
create table <write_bucket>.CONCEPT_CLASS (
  concept_class_id string,
  concept_class_name string,
  concept_class_concept_id bigint
);

-- COMMAND ----------

drop table if exists <write_bucket>.CONCEPT_RELATIONSHIP;
create table <write_bucket>.CONCEPT_RELATIONSHIP (
  concept_id_1 bigint,
  concept_id_2 bigint,
  relationship_id string,
  valid_start_date date,
  valid_end_date date,
  invalid_reason string
);



-- COMMAND ----------

drop table if exists <write_bucket>.RELATIONSHIP;
create table <write_bucket>.RELATIONSHIP (
  relationship_id string,
  relationship_name string,
  is_hierarchical string,
  defines_ancestry string,
  reverse_relationship_id string,
  relationship_concept_id bigint
);

-- COMMAND ----------

drop table if exists <write_bucket>.CONCEPT_SYNONYM;
create table <write_bucket>.CONCEPT_SYNONYM (
  concept_id bigint,
  concept_synonym_name string,
  language_concept_id bigint
);

-- COMMAND ----------

drop table if exists <write_bucket>.CONCEPT_ANCESTOR;
create table <write_bucket>.CONCEPT_ANCESTOR (
  ancestor_concept_id bigint,
  descendant_concept_id bigint,
  min_levels_of_separation bigint,
  max_levels_of_separation bigint
);

-- COMMAND ----------

drop table if exists <write_bucket>.SOURCE_TO_CONCEPT_MAP;
create table <write_bucket>.SOURCE_TO_CONCEPT_MAP (
  source_code string,
  source_concept_id bigint,
  source_vocabulary_id string,
  source_code_description string,
  target_concept_id bigint,
  target_vocabulary_id string,
  valid_start_date date,
  valid_end_date date,
  invalid_reason string
);

-- COMMAND ----------

drop table if exists <write_bucket>.DRUG_STRENGTH;
create table <write_bucket>.DRUG_STRENGTH (
  drug_concept_id bigint,
  ingredient_concept_id bigint,
  amount_value string,
  amount_unit_concept_id bigint,
  numerator_value string,
  numerator_unit_concept_id bigint,
  denominator_value string,
  denominator_unit_concept_id bigint,
  box_size bigint,
  valid_start_date date,
  valid_end_date date,
  invalid_reason string
);

-- COMMAND ----------

drop table if exists <write_bucket>.COHORT_DEFINITION;
create table <write_bucket>.COHORT_DEFINITION (
  cohort_definition_id bigint,
  cohort_definition_name string,
  cohort_definition_description string,
  definition_type_concept_id bigint,
  cohort_definition_syntax string,
  subject_concept_id bigint,
  cohort_initiation_date date
);

-- COMMAND ----------

drop table if exists <write_bucket>.ATTRIBUTE_DEFINITION;
create table <write_bucket>.ATTRIBUTE_DEFINITION (
  attribute_definition_id bigint,
  attribute_name string,
  attribute_description string,
  attribute_type_concept_id bigint,
  attribute_syntax string
);

-- COMMAND ----------

--Athena DDL Ends

-- COMMAND ----------

--Athena LOAD Starts

-- COMMAND ----------

-- MAGIC %r
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
-- MAGIC CONCEPT_RELATIONSHIP<-spark_read_csv(sc, "dbfs:/FileStore/shared_uploads/<user_id>/CONCEPT_RELATIONSHIP-2.csv",
-- MAGIC                                      memory = FALSE,
-- MAGIC                                      header = TRUE,
-- MAGIC                                      columns=cr_schema,
-- MAGIC                                      delimiter = ",")
-- MAGIC                                      
-- MAGIC sdf_register(CONCEPT_RELATIONSHIP,"CONCEPT_RELATIONSHIP");

-- COMMAND ----------

insert into
  <write_bucket>.CONCEPT_RELATIONSHIP
select
  *
from
  CONCEPT_RELATIONSHIP;
  
  
optimize <write_bucket>.CONCEPT_RELATIONSHIP  ZORDER by(concept_id_1);


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
-- MAGIC                         "dbfs:/FileStore/shared_uploads/<user_id>/CONCEPT-3.csv",
-- MAGIC                         memory = FALSE,
-- MAGIC                         header = TRUE,
-- MAGIC                         columns=concept_schema,
-- MAGIC                         delimiter = "\t",)
-- MAGIC sdf_register(CONCEPT,"CONCEPT");

-- COMMAND ----------

--Athena provides dx-px-rx code to ohdsi concept ids
insert into
  <write_bucket>.concept
select
  *
from
  CONCEPT;
  
optimize <write_bucket>.concept  ZORDER by(concept_id);

-- COMMAND ----------

-- MAGIC %r
-- MAGIC CONCEPT_ANCESTOR<-spark_read_csv(sc,
-- MAGIC                                  "dbfs:/FileStore/shared_uploads/<user_id>/CONCEPT_ANCESTOR-1.csv",
-- MAGIC                                  memory = FALSE,
-- MAGIC                                  header = TRUE,
-- MAGIC                                  infer_schema = TRUE,
-- MAGIC                                  delimiter = "\t", )
-- MAGIC                                  
-- MAGIC sdf_register(CONCEPT_ANCESTOR,"CONCEPT_ANCESTOR");

-- COMMAND ----------

insert into
  <write_bucket>.CONCEPT_ANCESTOR
select
  *
from
  CONCEPT_ANCESTOR;


-- COMMAND ----------

-- MAGIC %r
-- MAGIC RELATIONSHIP<-spark_read_csv(sc,
-- MAGIC                              "dbfs:/FileStore/shared_uploads/<user_id>/RELATIONSHIP-2.csv",
-- MAGIC                              memory = FALSE,
-- MAGIC                              header = TRUE,
-- MAGIC                              infer_schema = TRUE,
-- MAGIC                              delimiter = "\t", )
-- MAGIC                              
-- MAGIC sdf_register(RELATIONSHIP,"RELATIONSHIP");

-- COMMAND ----------

insert into
  <write_bucket>.RELATIONSHIP
select
  *
from
  RELATIONSHIP;

-- COMMAND ----------

-- MAGIC %r
-- MAGIC VOCABULARY<-spark_read_csv(sc, 
-- MAGIC                            "dbfs:/FileStore/shared_uploads/<user_id>/VOCABULARY-1.csv",
-- MAGIC                            memory = FALSE,
-- MAGIC                            header = TRUE,
-- MAGIC                            infer_schema = TRUE,
-- MAGIC                            delimiter = "\t",)
-- MAGIC 
-- MAGIC 
-- MAGIC sdf_register(VOCABULARY,"VOCABULARY");

-- COMMAND ----------

insert into
  <write_bucket>.VOCABULARY
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
-- MAGIC                              "dbfs:/FileStore/shared_uploads/<user_id>/px_handshake-1.csv",
-- MAGIC                              memory = FALSE,
-- MAGIC                              header = TRUE,
-- MAGIC                              columns=handshake,
-- MAGIC                              delimiter = "," )
-- MAGIC                              
-- MAGIC sdf_register(px_handshake,"px_handshake");

-- COMMAND ----------

drop table if exists  <write_bucket>.px_handshake;
create table  <write_bucket>.px_handshake as
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
-- MAGIC                         "dbfs:/FileStore/shared_uploads/<user_id>/ccw_concept.csv",
-- MAGIC                         memory = FALSE,
-- MAGIC                         header = TRUE,
-- MAGIC                         columns=concept_schema,
-- MAGIC                         delimiter = ",", )
-- MAGIC sdf_register(CONCEPT_ccw,"CONCEPT_ccw");

-- COMMAND ----------

insert into <write_bucket>.concept
select * from 
CONCEPT_ccw;

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
-- MAGIC CONCEPT_RELATIONSHIP_ccw<-spark_read_csv(sc, 
-- MAGIC                                      "dbfs:/FileStore/shared_uploads/<user_id>/ccw_relationship.csv",
-- MAGIC                                      memory = FALSE,
-- MAGIC                                      header = TRUE,
-- MAGIC                                      columns=cr_schema,
-- MAGIC                                      delimiter = ",")
-- MAGIC                                      
-- MAGIC                              
-- MAGIC sdf_register(CONCEPT_RELATIONSHIP_ccw,"CONCEPT_RELATIONSHIP_ccw");

-- COMMAND ----------

insert into <write_bucket>.concept_relationship
select * from
CONCEPT_RELATIONSHIP_ccw

-- COMMAND ----------

--Athena LOAD Ends

-- COMMAND ----------

--NPI Starts

-- COMMAND ----------

-- MAGIC 
-- MAGIC %r
-- MAGIC 
-- MAGIC library(sparklyr)
-- MAGIC sc<-spark_connect(method='databricks')
-- MAGIC NPI_place<-spark_read_csv(sc,"dbfs:/FileStore/shared_uploads/<user_id>/npi_place.csv", 
-- MAGIC memory = FALSE,header = TRUE,infer_schema = TRUE,delimiter = ",")
-- MAGIC head (NPI_place)
-- MAGIC sdf_register(NPI_place,"NPI_place")

-- COMMAND ----------

-- MAGIC 
-- MAGIC %r
-- MAGIC 
-- MAGIC #load NPI from DB filestore
-- MAGIC #do not do this if you do not want to update NPI
-- MAGIC #Note this is a 'slim' version of NPI which only contains non-repetative values
-- MAGIC #Note DB will not accept upload larger than 2 gigs ish- full npi is 8 gigs per month file
-- MAGIC 
-- MAGIC library(sparklyr)
-- MAGIC sc<-spark_connect(method='databricks')
-- MAGIC 
-- MAGIC NPI<-spark_read_csv(sc,"dbfs:/FileStore/shared_uploads/<user_id>/npidata_pfile.csv", 
-- MAGIC                           
-- MAGIC                            memory = FALSE,
-- MAGIC                            header = TRUE,
-- MAGIC                            infer_schema = TRUE,
-- MAGIC                            delimiter = ","
-- MAGIC                           )
-- MAGIC sdf_register(NPI,"NPI")

-- COMMAND ----------

drop table if exists <write_bucket>.NPI_provider;
create table <write_bucket>.NPI_provider using delta as
select
  *
from
  NPI;
  
--parse npi into spark sql view Provider name and care site are learnded from this dictionary drop view if exists NPI_provider;
optimize <write_bucket>.npi_provider  ZORDER by(npi);

drop view if exists NPI_provider;
create view NPI_provider as
select
  *
from
  <write_bucket>.NPI;

-- COMMAND ----------

-- MAGIC %r
-- MAGIC 
-- MAGIC library(sparklyr)
-- MAGIC sc<-spark_connect(method='databricks')
-- MAGIC NPI_place<-spark_read_csv(sc,"dbfs:/FileStore/shared_uploads/<user_id>/npi_place.csv", 
-- MAGIC memory = FALSE,header = TRUE,infer_schema = TRUE,delimiter = ",")
-- MAGIC head (NPI_place)
-- MAGIC sdf_register(NPI_place,"NPI_place")

-- COMMAND ----------

--NPI Ends
