-- Databricks notebook source
--this version of ddl uses source data types rather than destination data types
--todo verify variable order
--todo verify destination data types that are spark fridendly 


-- COMMAND ----------


drop table if exists dua_052538_NWI388.PERSON_x;
CREATE TABLE dua_052538_NWI388.PERSON_x (
  person_id integer NOT NULL,
  gender_concept_id string NOT NULL,
  year_of_birth string NOT NULL,
  month_of_birth string,
  day_of_birth string,
  birth_datetime string,
  race_concept_id string NOT NULL,
  ethnicity_concept_id string NOT NULL,
  location_id string,
  provider_id string,
  care_site_id string,
  person_source_value string,
  gender_source_value string,
  gender_source_concept_id string,
  race_source_value string,
  race_source_concept_id string,
  ethnicity_source_value string,
  ethnicity_source_concept_id string
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.DRUG_EXPOSURE_x;
CREATE TABLE dua_052538_nwi388.DRUG_EXPOSURE_x (
  drug_exposure_id string NOT NULL,
  person_id integer NOT NULL,
  drug_concept_id string NOT NULL,
  drug_exposure_start_date string NOT NULL,
  drug_exposure_start_datetime string,
  drug_exposure_end_date string NOT NULL,
  drug_exposure_end_datetime string,
  verbatim_end_date string,
  drug_type_concept_id string NOT NULL,
  stop_reason string,
  refills string,
  quantity float,
  days_supply string,
  sig string,
  route_concept_id string,
  lot_number string,
  provider_id string,
  visit_occurrence_id string,
  visit_detail_id string,
  drug_source_value string,
  drug_source_concept_id string,
  route_source_value string,
  dose_unit_source_value string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.CONDITION_OCCURRENCE_x;
CREATE TABLE dua_052538_nwi388.CONDITION_OCCURRENCE_x (
  condition_occurrence_id string,
  person_id integer,
  condition_concept_id string,
  condition_start_date string,
  condition_start_datetime string,
  condition_end_date string,
  condition_end_datetime string,
  condition_type_concept_id string,
  condition_status_concept_id string,
  stop_reason string,
  provider_id string,
  visit_occurrence_id string,
  visit_detail_id string,
  condition_source_value string,
  condition_source_concept_id string,
  condition_status_source_value string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.VISIT_DETAIL_x;
CREATE TABLE dua_052538_nwi388.VISIT_DETAIL_x (
  visit_detail_id integer NOT NULL,
  person_id integer NOT NULL,
  visit_detail_concept_id string NOT NULL,
  visit_detail_start_date date NOT NULL,
  visit_detail_start_datetime timestamp,
  visit_detail_end_date date NOT NULL,
  visit_detail_end_datetime timestamp,
  visit_detail_type_concept_id string NOT NULL,
  provider_id string,
  care_site_id string,
  visit_detail_source_value string,
  visit_detail_source_concept_id string,
  admitting_source_value string,
  admitting_source_concept_id string,
  discharge_to_source_value string,
  discharge_to_concept_id string,
  preceding_visit_detail_id string,
  visit_detail_parent_id integer,
  visit_occurrence_id integer NOT NULL,
  clm_id string,
  state_cd string,
  file string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.VISIT_OCCURRENCE_x;
CREATE TABLE dua_052538_nwi388.VISIT_OCCURRENCE_x (
  visit_occurrence_id string NOT NULL,
  person_id integer NOT NULL,
  visit_concept_id string NOT NULL,
  visit_start_date date NOT NULL,
  visit_start_datetime string,
  visit_end_date date NOT NULL,
  visit_end_datetime string,
  visit_type_concept_id string NOT NULL,
  provider_id string,
  care_site_id string,
  visit_source_value string,
  visit_source_concept_id string,
  admitting_source_concept_id string,
  admitting_source_value string,
  discharge_to_concept_id string,
  discharge_to_source_value string,
  preceding_visit_occurrence_id string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.OBSERVATION_PERIOD_x;
CREATE TABLE dua_052538_nwi388.OBSERVATION_PERIOD_x (
  observation_period_id string NOT NULL,
  person_id integer NOT NULL,
  observation_period_start_date date NOT NULL,
  observation_period_end_date date NOT NULL,
  period_type_concept_id string NOT NULL,
  state_cd string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.PROCEDURE_OCCURRENCE_x;
CREATE TABLE dua_052538_nwi388.PROCEDURE_OCCURRENCE_x (
  procedure_occurrence_id string,
  person_id integer,
  procedure_concept_id string,
  procedure_date string,
  procedure_datetime string,
  procedure_type_concept_id string,
  modifier_concept_id string,
  quantity string,
  provider_id string,
  visit_occurrence_id string,
  visit_detail_id string,
  procedure_source_value string,
  procedure_source_concept_id string,
  modifier_source_value string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.DEVICE_EXPOSURE_x;
CREATE TABLE dua_052538_nwi388.DEVICE_EXPOSURE_x (
  device_exposure_id integer NOT NULL,
  person_id integer NOT NULL,
  device_concept_id integer NOT NULL,
  device_exposure_start_date date NOT NULL,
  device_exposure_start_datetime timestamp,
  device_exposure_end_date date,
  device_exposure_end_datetime timestamp,
  device_type_concept_id integer NOT NULL,
  unique_device_id string,
  quantity integer,
  provider_id integer,
  visit_occurrence_id integer,
  visit_detail_id integer,
  device_source_value string,
  device_source_concept_id integer,
  clm_id integer,
  state_cd string,
  file string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.MEASUREMENT_x;
CREATE TABLE dua_052538_nwi388.MEASUREMENT_x (
  measurement_id integer NOT NULL,
  person_id integer NOT NULL,
  measurement_concept_id integer NOT NULL,
  measurement_date date NOT NULL,
  measurement_datetime timestamp,
  measurement_time string,
  measurement_type_concept_id integer NOT NULL,
  operator_concept_id integer,
  value_as_number float,
  value_as_concept_id integer,
  unit_concept_id integer,
  range_low float,
  range_high float,
  provider_id integer,
  visit_occurrence_id integer,
  visit_detail_id integer,
  measurement_source_value string,
  measurement_source_concept_id integer,
  unit_source_value string,
  value_source_value string,
  clm_id integer,
  state_cd string,
  file string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.OBSERVATION_x;
CREATE TABLE dua_052538_nwi388.OBSERVATION_x (
  observation_id string NOT NULL,
  person_id integer NOT NULL,
  observation_concept_id integer NOT NULL,
  observation_date date NOT NULL,
  observation_datetime timestamp,
  observation_type_concept_id integer NOT NULL,
  value_as_number float,
  value_as_string string,
  value_as_concept_id Integer,
  qualifier_concept_id integer,
  unit_concept_id integer,
  provider_id integer,
  visit_occurrence_id integer,
  visit_detail_id integer,
  observation_source_value string,
  observation_source_concept_id integer,
  unit_source_value string,
  qualifier_source_value string,
  clm_id integer,
  state_cd string,
  file string
);

-- COMMAND ----------

DROP TABLE if exists dua_052538_nwi388.DEATH_x;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE dua_052538_nwi388.DEATH_x (
  person_id integer,
  death_date date,
  death_datetime string,
  death_type_concept_id string,
  cause_concept_id string,
  cause_source_value string,
  cause_source_concept_id string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.NOTE_x;
CREATE TABLE dua_052538_nwi388.NOTE_x (
  note_id integer NOT NULL,
  person_id integer NOT NULL,
  note_date date NOT NULL,
  note_datetime timestamp,
  note_type_concept_id integer NOT NULL,
  note_class_concept_id integer NOT NULL,
  note_title string,
  note_text string NOT NULL,
  encoding_concept_id integer NOT NULL,
  language_concept_id integer NOT NULL,
  provider_id integer,
  visit_occurrence_id integer,
  visit_detail_id integer,
  note_source_value string,
  clm_id integer,
  state_cd string,
  file string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.NOTE_NLP_x;
CREATE TABLE dua_052538_nwi388.NOTE_NLP_x (
  note_nlp_id integer NOT NULL,
  note_id integer NOT NULL,
  section_concept_id integer,
  snippet string,
  --"offset" string,
  lexical_variant string NOT NULL,
  note_nlp_concept_id integer,
  note_nlp_source_concept_id integer,
  nlp_system string,
  nlp_date date NOT NULL,
  nlp_datetime timestamp,
  term_exists string,
  term_temporal string,
  term_modifiers string,
  clm_id integer,
  state_cd string,
  file string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.SPECIMEN_x;
CREATE TABLE dua_052538_nwi388.SPECIMEN_x (
  specimen_id integer NOT NULL,
  person_id integer NOT NULL,
  specimen_concept_id integer NOT NULL,
  specimen_type_concept_id integer NOT NULL,
  specimen_date date NOT NULL,
  specimen_datetime timestamp,
  quantity float,
  unit_concept_id integer,
  anatomic_site_concept_id integer,
  disease_status_concept_id integer,
  specimen_source_id string,
  specimen_source_value string,
  unit_source_value string,
  anatomic_site_source_value string,
  disease_status_source_value string,
  clm_id integer,
  state_cd string,
  file string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.FACT_RELATIONSHIP_x;
CREATE TABLE dua_052538_nwi388.FACT_RELATIONSHIP_x (
  domain_concept_id_1 integer NOT NULL,
  fact_id_1 integer NOT NULL,
  domain_concept_id_2 integer NOT NULL,
  fact_id_2 integer NOT NULL,
  relationship_concept_id integer NOT NULL,
  clm_id integer,
  state_cd string,
  file string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.LOCATION_x;
CREATE TABLE dua_052538_nwi388.LOCATION_x (
  location_id integer NOT NULL,
  address_1 string,
  address_2 string,
  city string,
  state string,
  zip string,
  county string,
  location_source_value string,
  clm_id integer,
  state_cd string,
  file string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.CARE_SITE_x;
CREATE TABLE dua_052538_nwi388.CARE_SITE_x (
  care_site_id integer NOT NULL,
  care_site_name string,
  place_of_service_concept_id integer,
  location_id integer,
  care_site_source_value string,
  place_of_service_source_value string,
  clm_id integer,
  state_cd string,
  file string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.PROVIDER_x;
CREATE TABLE dua_052538_nwi388.PROVIDER_x (
  npi string,
  provider_name string,
  dea string,
  specialty_concept_id string,
  care_site_id string,
  year_of_birth string,
  gender_concept_id string,
  provider_source_value string,
  specialty_source_value string,
  specialty_source_concept_id string,
  gender_source_value string,
  gender_source_concept_id string,
  provider_id string
);
--dua_052538_nwi388`.`provider`':
--- Cannot safely cast 'provider_id': string to int
-- Cannot safely cast 'specialty_concept_id': string to int
-- Cannot safely cast 'care_site_id': string to int
-- Cannot safely cast 'year_of_birth': string to int
-- Cannot safely cast 'gender_concept_id': string to int
-- Cannot safely cast 'specialty_source_concept_id': string to int
-- Cannot safely cast 'gender_source_concept_id': string to int;

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.PAYER_PLAN_PERIOD_x;
CREATE TABLE dua_052538_nwi388.PAYER_PLAN_PERIOD_x (
  payer_plan_period_id integer NOT NULL,
  person_id integer NOT NULL,
  payer_plan_period_start_date date NOT NULL,
  payer_plan_period_end_date date NOT NULL,
  payer_concept_id integer,
  payer_source_value string,
  payer_source_concept_id integer,
  plan_concept_id integer,
  plan_source_value string,
  plan_source_concept_id integer,
  sponsor_concept_id integer,
  sponsor_source_value string,
  sponsor_source_concept_id integer,
  family_source_value string,
  stop_reason_concept_id integer,
  stop_reason_source_value string,
  stop_reason_source_concept_id integer
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.COST_x;
CREATE TABLE dua_052538_nwi388.COST_x (
  cost_id integer NOT NULL,
  cost_event_id integer NOT NULL,
  cost_domain_id string NOT NULL,
  cost_type_concept_id integer NOT NULL,
  currency_concept_id integer,
  total_charge float,
  total_cost float,
  total_paid float,
  paid_by_payer float,
  paid_by_patient float,
  paid_patient_copay float,
  paid_patient_coinsurance float,
  paid_patient_deductible float,
  paid_by_primary float,
  paid_ingredient_cost float,
  paid_dispensing_fee float,
  payer_plan_period_id integer,
  amount_allowed float,
  revenue_code_concept_id integer,
  revenue_code_source_value string,
  drg_concept_id integer,
  drg_source_value string,
  clm_id integer,
  state_cd string,
  file string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.DRUG_ERA_x;
CREATE TABLE dua_052538_nwi388.DRUG_ERA_x (
  drug_era_id integer NOT NULL,
  person_id integer NOT NULL,
  drug_concept_id integer NOT NULL,
  drug_era_start_date timestamp NOT NULL,
  drug_era_end_date timestamp NOT NULL,
  drug_exposure_count integer,
  gap_days integer
);

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.DOSE_ERA_x;
CREATE TABLE dua_052538_nwi388.DOSE_ERA_x (
  dose_era_id integer NOT NULL,
  person_id integer NOT NULL,
  drug_concept_id integer NOT NULL,
  unit_concept_id integer NOT NULL,
  dose_value float NOT NULL,
  dose_era_start_date timestamp NOT NULL,
  dose_era_end_date timestamp NOT NULL
);

-- COMMAND ----------

--HINT DISTRIBUTE ON KEY (person_id)
DROP TABLE if exists dua_052538_nwi388.CONDITION_ERA_x;
CREATE TABLE dua_052538_nwi388.CONDITION_ERA_x (
  condition_era_id integer NOT NULL,
  person_id integer NOT NULL,
  condition_concept_id integer NOT NULL,
  condition_era_start_date timestamp NOT NULL,
  condition_era_end_date timestamp NOT NULL,
  condition_occurrence_count integer
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.METADATA_x;
CREATE TABLE dua_052538_nwi388.METADATA_x (
  metadata_concept_id integer NOT NULL,
  metadata_type_concept_id integer NOT NULL,
  name string NOT NULL,
  value_as_string string,
  value_as_concept_id integer,
  metadata_date date,
  metadata_datetime timestamp
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.CDM_SOURCE_x;
CREATE TABLE dua_052538_nwi388.CDM_SOURCE_x (
  cdm_source_name string NOT NULL,
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

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.CONCEPT_x;
CREATE TABLE dua_052538_nwi388.CONCEPT_x (
  concept_id integer NOT NULL,
  concept_name string NOT NULL,
  domain_id string NOT NULL,
  vocabulary_id string NOT NULL,
  concept_class_id string NOT NULL,
  standard_concept string,
  concept_code string NOT NULL,
  valid_start_date string NOT NULL,
  valid_end_date string NOT NULL,
  invalid_reason string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.VOCABULARY_x;
CREATE TABLE dua_052538_nwi388.VOCABULARY_x (
  vocabulary_id string NOT NULL,
  vocabulary_name string NOT NULL,
  vocabulary_reference string NOT NULL,
  vocabulary_version string,
  vocabulary_concept_id integer NOT NULL
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.DOMAIN_x;
CREATE TABLE dua_052538_nwi388.DOMAIN_x (
  domain_id string NOT NULL,
  domain_name string NOT NULL,
  domain_concept_id integer NOT NULL
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.CONCEPT_CLASS_x;
CREATE TABLE dua_052538_nwi388.CONCEPT_CLASS_x (
  concept_class_id string NOT NULL,
  concept_class_name string NOT NULL,
  concept_class_concept_id integer NOT NULL
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.CONCEPT_RELATIONSHIP_x;
CREATE TABLE dua_052538_nwi388.CONCEPT_RELATIONSHIP_x (
  concept_id_1 integer NOT NULL,
  concept_id_2 integer NOT NULL,
  relationship_id string NOT NULL,
  valid_start_date string NOT NULL,
  valid_end_date string NOT NULL,
  invalid_reason string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.RELATIONSHIP_x;
CREATE TABLE dua_052538_nwi388.RELATIONSHIP_x (
  relationship_id string NOT NULL,
  relationship_name string NOT NULL,
  is_hierarchical string NOT NULL,
  defines_ancestry string NOT NULL,
  reverse_relationship_id string NOT NULL,
  relationship_concept_id integer NOT NULL
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.CONCEPT_SYNONYM_x;
CREATE TABLE dua_052538_nwi388.CONCEPT_SYNONYM_x (
  concept_id integer NOT NULL,
  concept_synonym_name string NOT NULL,
  language_concept_id integer NOT NULL
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.CONCEPT_ANCESTOR_x;
CREATE TABLE dua_052538_nwi388.CONCEPT_ANCESTOR_x (
  ancestor_concept_id integer NOT NULL,
  descendant_concept_id integer NOT NULL,
  min_levels_of_separation integer NOT NULL,
  max_levels_of_separation integer NOT NULL
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.SOURCE_TO_CONCEPT_MAP_x;
CREATE TABLE dua_052538_nwi388.SOURCE_TO_CONCEPT_MAP_x (
  source_code string NOT NULL,
  source_concept_id integer NOT NULL,
  source_vocabulary_id string NOT NULL,
  source_code_description string,
  target_concept_id integer NOT NULL,
  target_vocabulary_id string NOT NULL,
  valid_start_date date NOT NULL,
  valid_end_date date NOT NULL,
  invalid_reason string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.DRUG_STRENGTH_x;
CREATE TABLE dua_052538_nwi388.DRUG_STRENGTH_x (
  drug_concept_id integer NOT NULL,
  ingredient_concept_id integer NOT NULL,
  amount_value float,
  amount_unit_concept_id integer,
  numerator_value float,
  numerator_unit_concept_id integer,
  denominator_value float,
  denominator_unit_concept_id integer,
  box_size integer,
  valid_start_date date NOT NULL,
  valid_end_date date NOT NULL,
  invalid_reason string
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.COHORT_DEFINITION_x;
CREATE TABLE dua_052538_nwi388.COHORT_DEFINITION_x (
  cohort_definition_id integer NOT NULL,
  cohort_definition_name string NOT NULL,
  cohort_definition_description string,
  definition_type_concept_id integer NOT NULL,
  cohort_definition_syntax string,
  subject_concept_id integer NOT NULL,
  cohort_initiation_date date
);

-- COMMAND ----------

--HINT DISTRIBUTE ON RANDOM
DROP TABLE if exists dua_052538_nwi388.ATTRIBUTE_DEFINITION_x;
CREATE TABLE dua_052538_nwi388.ATTRIBUTE_DEFINITION_x (
  attribute_definition_id integer NOT NULL,
  attribute_name string NOT NULL,
  attribute_description string,
  attribute_type_concept_id integer NOT NULL,
  attribute_syntax string
);

-- COMMAND ----------

--medicare person table-one year
--extend z_everything-
--R try to move from x to
drop table dua_052538_nwi388.z_person;
create table dua_052538_nwi388.z_person(
  person_id integer NOT NULL,
  gender_concept_id string NOT NULL,
  year_of_birth string NOT NULL,
  month_of_birth string,
  day_of_birth string,
  birth_datetime string,
  race_concept_id string NOT NULL,
  ethnicity_concept_id string NOT NULL,
  location_id string,
  provider_id string,
  care_site_id string,
  person_source_value string,
  gender_source_value string,
  gender_source_concept_id string,
  race_source_value string,
  race_source_concept_id string,
  ethnicity_source_value string,
  ethnicity_source_concept_id string,
  state_cd string
);
