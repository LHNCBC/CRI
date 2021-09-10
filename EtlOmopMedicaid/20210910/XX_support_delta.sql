-- Databricks notebook source
--run this file at least once in your life 
--only run if you need to update third party databases

--create log here
--ddl for athena
--load data from data store into athena
--load data from data store into NPI



-- COMMAND ----------

create widget text scope default "1s1m";
create widget text job_id default "000";
create widget text state default "'VA'";

-- COMMAND ----------

--start log

-- COMMAND ----------

--
drop table if exists dua_052538_nwi388.h_log;
create table dua_052538_nwi388.h_log
(
job_id string,
notebook string,
command string,
task string,
time string,
state string,
scope string,
row_count string);


-- COMMAND ----------

--end log

-- COMMAND ----------

--Athena DDL Starts

-- COMMAND ----------

drop table if exists dua_052538_nwi388.METADATA;
create table dua_052538_nwi388.METADATA (
  metadata_concept_id bigint,
  metadata_type_concept_id bigint,
  name string,
  value_as_string string,
  value_as_concept_id bigint,
  metadata_date date,
  metadata_datetime string
);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DDL','48','create ddl metadata',current_timestamp(),$state,'$scope');

-- COMMAND ----------

drop table if exists dua_052538_nwi388.CDM_SOURCE;
create table dua_052538_nwi388.CDM_SOURCE (
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

insert into dua_052538_nwi388.h_log values('$job_id','DDL','50','create ddl cdm_source',current_timestamp(),$state,'$scope');

-- COMMAND ----------

drop table if exists dua_052538_nwi388.CONCEPT;
create table dua_052538_nwi388.CONCEPT using delta
partitioned by  (vocabulary_id)

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

insert into dua_052538_nwi388.h_log values('$job_id','DDL','52','create ddl concept',current_timestamp(),$state,'$scope');

-- COMMAND ----------

drop table if exists dua_052538_nwi388.VOCABULARY;
create table dua_052538_nwi388.VOCABULARY (
  vocabulary_id string,
  vocabulary_name string,
  vocabulary_reference string,
  vocabulary_version string,
  vocabulary_concept_id bigint
);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DDL','54','create ddl vocabulary',current_timestamp(),$state,'$scope');

-- COMMAND ----------

drop table if exists dua_052538_nwi388.DOMAIN;
create table dua_052538_nwi388.DOMAIN (
  domain_id string,
  domain_name string,
  domain_concept_id bigint
);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DDL','56','create ddl domain',current_timestamp(),$state,'$scope');

-- COMMAND ----------

drop table if exists dua_052538_nwi388.CONCEPT_CLASS;
create table dua_052538_nwi388.CONCEPT_CLASS (
  concept_class_id string,
  concept_class_name string,
  concept_class_concept_id bigint
);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DDL','58','create ddl concept_class',current_timestamp(),$state,'$scope');

-- COMMAND ----------

drop table if exists dua_052538_nwi388.CONCEPT_RELATIONSHIP;
create table dua_052538_nwi388.CONCEPT_RELATIONSHIP (
  concept_id_1 bigint,
  concept_id_2 bigint,
  relationship_id string,
  valid_start_date date,
  valid_end_date date,
  invalid_reason string
);



-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DDL','60','create ddl concept_relationship',current_timestamp(),$state,'$scope');

-- COMMAND ----------

drop table if exists dua_052538_nwi388.RELATIONSHIP;
create table dua_052538_nwi388.RELATIONSHIP (
  relationship_id string,
  relationship_name string,
  is_hierarchical string,
  defines_ancestry string,
  reverse_relationship_id string,
  relationship_concept_id bigint
);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DDL','62','create ddl relationship',current_timestamp(),$state,'$scope');

-- COMMAND ----------

drop table if exists dua_052538_nwi388.CONCEPT_SYNONYM;
create table dua_052538_nwi388.CONCEPT_SYNONYM (
  concept_id bigint,
  concept_synonym_name string,
  language_concept_id bigint
);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DDL','64','create ddl concept_synonym',current_timestamp(),$state,'$scope');

-- COMMAND ----------

drop table if exists dua_052538_nwi388.CONCEPT_ANCESTOR;
create table dua_052538_nwi388.CONCEPT_ANCESTOR (
  ancestor_concept_id bigint,
  descendant_concept_id bigint,
  min_levels_of_separation bigint,
  max_levels_of_separation bigint
);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DDL','66','create ddl concept_ancestor',current_timestamp(),$state,'$scope');

-- COMMAND ----------

drop table if exists dua_052538_nwi388.SOURCE_TO_CONCEPT_MAP;
create table dua_052538_nwi388.SOURCE_TO_CONCEPT_MAP (
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

insert into dua_052538_nwi388.h_log values('$job_id','DDL','68','create ddl SOURCE_TO_CONCEPT_MAP',current_timestamp(),$state,'$scope');

-- COMMAND ----------

drop table if exists dua_052538_nwi388.DRUG_STRENGTH;
create table dua_052538_nwi388.DRUG_STRENGTH (
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

insert into dua_052538_nwi388.h_log values('$job_id','DDL','70','create ddl DRUG_STRENGTH',current_timestamp(),$state,'$scope');

-- COMMAND ----------

drop table if exists dua_052538_nwi388.COHORT_DEFINITION;
create table dua_052538_nwi388.COHORT_DEFINITION (
  cohort_definition_id bigint,
  cohort_definition_name string,
  cohort_definition_description string,
  definition_type_concept_id bigint,
  cohort_definition_syntax string,
  subject_concept_id bigint,
  cohort_initiation_date date
);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DDL','72','create ddl COHORT_DEFINITION',current_timestamp(),$state,'$scope');

-- COMMAND ----------

drop table if exists dua_052538_nwi388.ATTRIBUTE_DEFINITION;
create table dua_052538_nwi388.ATTRIBUTE_DEFINITION (
  attribute_definition_id bigint,
  attribute_name string,
  attribute_description string,
  attribute_type_concept_id bigint,
  attribute_syntax string
);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DDL','74','create ddl ATTRIBUTE_DEFINITION',current_timestamp(),$state,'$scope');

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
-- MAGIC CONCEPT_RELATIONSHIP<-spark_read_csv(sc, "dbfs:/FileStore/shared_uploads/NWI388/CONCEPT_RELATIONSHIP-2.csv",
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
  
  
optimize dua_052538_nwi388.CONCEPT_RELATIONSHIP  ZORDER by(concept_id_1);


-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DICTIONARY','40','create table concept_relationship',current_timestamp(),$state,'$scope');

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
-- MAGIC                         "dbfs:/FileStore/shared_uploads/NWI388/CONCEPT-3.csv",
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
  
optimize dua_052538_nwi388.concept  ZORDER by(concept_id);

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DICTIONARY','7','create table concept',current_timestamp(),$state,'$scope');

-- COMMAND ----------

-- MAGIC %r
-- MAGIC CONCEPT_ANCESTOR<-spark_read_csv(sc,
-- MAGIC                                  "dbfs:/FileStore/shared_uploads/NWI388/CONCEPT_ANCESTOR-1.csv",
-- MAGIC                                  memory = FALSE,
-- MAGIC                                  header = TRUE,
-- MAGIC                                  infer_schema = TRUE,
-- MAGIC                                  delimiter = "\t", )
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

insert into dua_052538_nwi388.h_log values('$job_id','DICTIONARY','10','create table concept_ancestor',current_timestamp(),$state,'$scope');

-- COMMAND ----------

-- MAGIC %r
-- MAGIC RELATIONSHIP<-spark_read_csv(sc,
-- MAGIC                              "dbfs:/FileStore/shared_uploads/NWI388/RELATIONSHIP-2.csv",
-- MAGIC                              memory = FALSE,
-- MAGIC                              header = TRUE,
-- MAGIC                              infer_schema = TRUE,
-- MAGIC                              delimiter = "\t", )
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

insert into dua_052538_nwi388.h_log values('$job_id','DICTIONARY','13','create table relationship',current_timestamp(),$state,'$scope');

-- COMMAND ----------

-- MAGIC %r
-- MAGIC VOCABULARY<-spark_read_csv(sc, 
-- MAGIC                            "dbfs:/FileStore/shared_uploads/NWI388/VOCABULARY-1.csv",
-- MAGIC                            memory = FALSE,
-- MAGIC                            header = TRUE,
-- MAGIC                            infer_schema = TRUE,
-- MAGIC                            delimiter = "\t",)
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

insert into dua_052538_nwi388.h_log values('$job_id','DICTIONARY','16','create table vocabulary',current_timestamp(),$state,'$scope');

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

insert into dua_052538_nwi388.h_log values('$job_id','DICTIONARY','19','create table px_handshake',current_timestamp(),$state,'$scope');

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

-- COMMAND ----------

insert into dua_052538_nwi388.concept
select * from 
CONCEPT_ccw;

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DICTIONARY','22','create table concept_ccw',current_timestamp(),$state,'$scope');

-- COMMAND ----------

#import b

cr_schema<-c(concept_id_1="integer",
           concept_id_2="integer",
           relationship_id="character",
           valid_start_date="date",
           valid_end_date="date",
           invalid_reason="character")

CONCEPT_RELATIONSHIP_ccw<-spark_read_csv(sc, 
                                     "dbfs:/FileStore/shared_uploads/NWI388/ccw_relationship.csv",
                                     memory = FALSE,
                                     header = TRUE,
                                     columns=cr_schema,
                                     delimiter = ",")
                                     
                             
sdf_register(CONCEPT_RELATIONSHIP_ccw,"CONCEPT_RELATIONSHIP_ccw");

-- COMMAND ----------

insert into dua_052538_nwi388.concept_relationship
select * from
CONCEPT_RELATIONSHIP_ccw

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','DICTIONARY','22','create table concept_relationship',current_timestamp(),$state,'$scope');

-- COMMAND ----------

--Athena LOAD Ends

-- COMMAND ----------

--NPI Starts

-- COMMAND ----------

-- MAGIC 
-- MAGIC %r
-- MAGIC load places npi
-- MAGIC library(sparklyr)
-- MAGIC sc<-spark_connect(method='databricks')
-- MAGIC NPI_place<-spark_read_csv(sc,"dbfs:/FileStore/shared_uploads/NWI388/npi_place.csv", 
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
-- MAGIC NPI<-spark_read_csv(sc,"dbfs:/FileStore/shared_uploads/NWI388/npidata_pfile.csv", 
-- MAGIC                           
-- MAGIC                            memory = FALSE,
-- MAGIC                            header = TRUE,
-- MAGIC                            infer_schema = TRUE,
-- MAGIC                            delimiter = ","
-- MAGIC                           )
-- MAGIC sdf_register(NPI,"NPI")

-- COMMAND ----------

drop table if exists dua_052538_nwi388.NPI_provider;
create table dua_052538_nwi388.NPI_provider using delta as
select
  *
from
  NPI;
  
--parse npi into spark sql view Provider name and care site are learnded from this dictionary drop view if exists NPI_provider;
optimize dua_052538_nwi388.npi_provider  ZORDER by(npi);
drop view if exists NPI_provider;
create view NPI_provider as
select
  *
from
  dua_052538_nwi388.NPI;

-- COMMAND ----------

-- MAGIC %r
-- MAGIC load places npi
-- MAGIC library(sparklyr)
-- MAGIC sc<-spark_connect(method='databricks')
-- MAGIC NPI_place<-spark_read_csv(sc,"dbfs:/FileStore/shared_uploads/NWI388/npi_place.csv", 
-- MAGIC memory = FALSE,header = TRUE,infer_schema = TRUE,delimiter = ",")
-- MAGIC head (NPI_place)
-- MAGIC sdf_register(NPI_place,"NPI_place")

-- COMMAND ----------

--NPI Ends
