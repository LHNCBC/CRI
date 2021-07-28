-- Databricks notebook source
create widget text job default "1s1m";

-- COMMAND ----------

drop table if exists dua_052538_nwi388.PERSON_$job;
create table dua_052538_nwi388.PERSON_$job (
  person_id bigint,
  gender_concept_id bigint,
  year_of_birth bigint,
  month_of_birth bigint,
  day_of_birth bigint,
  birth_datetime string,
  race_concept_id bigint,
  ethnicity_concept_id bigint,
  location_id bigint,
  provider_id bigint,
  care_site_id bigint,
  person_source_value string,
  gender_source_value string,
  gender_source_concept_id bigint,
  race_source_value string,
  race_source_concept_id bigint,
  ethnicity_source_value string,
  ethnicity_source_concept_id bigint
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.OBSERVATION_PERIOD_$job;
create table dua_052538_nwi388.OBSERVATION_PERIOD_$job (
  observation_period_id bigint,
  person_id bigint,
  observation_period_start_date date,
  observation_period_end_date date,
  period_type_concept_id bigint
);

-- COMMAND ----------

-- Cannot safely cast 'specialty_concept_id': string to bigint
-- Cannot safely cast 'care_site_id': string to bigint
-- Cannot safely cast 'year_of_birth': string to bigint
-- Cannot safely cast 'gender_concept_id': string to bigint
-- Cannot safely cast 'specialty_source_concept_id': string to bigint
-- Cannot safely cast 'gender_source_concept_id': string to bigint;
drop table if exists dua_052538_nwi388.provider_$job;
create table dua_052538_nwi388.PROVIDER_$job 
(
  npi string,
  provider_id bigint,
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
  gender_source_concept_id string
);

-- COMMAND ----------

--Cannot safely cast 'admitting_source_concept_id': string to bigint;
drop table if exists dua_052538_nwi388.VISIT_OCCURRENCE_$job;
create table dua_052538_nwi388.VISIT_OCCURRENCE_$job (
  visit_occurrence_id bigint,
  person_id bigint,
  visit_concept_id bigint,
  visit_start_date date,
  visit_start_datetime string,
  visit_end_date date,
  visit_end_datetime string,
  visit_type_concept_id bigint,
  provider_id bigint,
  care_site_id bigint,
  visit_source_value string,
  visit_source_concept_id bigint,
  admitting_source_concept_id string,
  admitting_source_value string,
  discharge_to_concept_id bigint,
  discharge_to_source_value string,
  preceding_visit_occurrence_id bigint
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.VISIT_DETAIL_$job;
create table dua_052538_nwi388.VISIT_DETAIL_$job (
  visit_detail_id bigint,
  person_id bigint,
  visit_detail_concept_id bigint,
  visit_detail_start_date date,
  visit_detail_start_datetime string,
  visit_detail_end_date date,
  visit_detail_end_datetime string,
  visit_detail_type_concept_id bigint,
  provider_id bigint,
  care_site_id bigint,
  visit_detail_source_value string,
  visit_detail_source_concept_id bigint,
  admitting_source_value string,
  admitting_source_concept_id bigint,
  discharge_to_source_value string,
  discharge_to_concept_id bigint,
  preceding_visit_detail_id bigint,
  visit_detail_parent_id bigint,
  visit_occurrence_id bigint
);

-- COMMAND ----------

-- Cannot safely cast 'provider_id': string to bigint;
drop table if exists dua_052538_nwi388.PROCEDURE_OCCURRENCE_$job;
create table dua_052538_nwi388.PROCEDURE_OCCURRENCE_$job (
  procedure_occurrence_id bigint,
  person_id bigint,
  procedure_concept_id bigint,
  procedure_date date,
  procedure_datetime string,
  procedure_type_concept_id bigint,
  modifier_concept_id string,
  quantity bigint,
  provider_id string,
  visit_occurrence_id bigint,
  visit_detail_id bigint,
  procedure_source_value string,
  procedure_source_concept_id bigint,
  modifier_source_value string
);

-- COMMAND ----------

-- Cannot safely cast 'provider_id': string to bigint;
drop table if exists dua_052538_nwi388.CONDITION_OCCURRENCE_$job;
create table dua_052538_nwi388.CONDITION_OCCURRENCE_$job (
  condition_occurrence_id bigint,
  person_id bigint,
  condition_concept_id bigint,
  condition_start_date date,
  condition_start_datetime string,
  condition_end_date date,
  condition_end_datetime string,
  condition_type_concept_id bigint,
  condition_status_concept_id bigint,
  stop_reason string,
  provider_id string,
  visit_occurrence_id bigint,
  visit_detail_id bigint,
  condition_source_value string,
  condition_source_concept_id bigint,
  condition_status_source_value string
);

-- COMMAND ----------

-- Cannot safely cast 'refills': string to bigint
-- Cannot safely cast 'provider_id': string to bigint;
drop table if exists dua_052538_nwi388.DRUG_EXPOSURE_$job;
create table dua_052538_nwi388.DRUG_EXPOSURE_$job (
  drug_exposure_id bigint,
  person_id bigint,
  drug_concept_id bigint,
  drug_exposure_start_date date,
  drug_exposure_start_datetime string,
  drug_exposure_end_date date,
  drug_exposure_end_datetime string,
  verbatim_end_date date,
  drug_type_concept_id bigint,
  stop_reason string,
  refills string,
  quantity string,
  days_supply int,
  sig string,
  route_concept_id bigint,
  lot_number string,
  provider_id string,
  visit_occurrence_id bigint,
  visit_detail_id bigint,
  drug_source_value string,
  drug_source_concept_id bigint,
  route_source_value string,
  dose_unit_source_value string
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.DEVICE_EXPOSURE_$job;
create table dua_052538_nwi388.DEVICE_EXPOSURE_$job (
  device_exposure_id bigint,
  person_id bigint,
  device_concept_id bigint,
  device_exposure_start_date date,
  device_exposure_start_datetime string,
  device_exposure_end_date date,
  device_exposure_end_datetime string,
  device_type_concept_id bigint,
  unique_device_id string,
  quantity bigint,
  provider_id bigint,
  visit_occurrence_id bigint,
  visit_detail_id bigint,
  device_source_value string,
  device_source_concept_id bigint
);

-- COMMAND ----------

--- Cannot safely cast 'provider_id': string to bigint;
drop table if exists dua_052538_nwi388.MEASUREMENT_$job;
create table dua_052538_nwi388.MEASUREMENT_$job (
  measurement_id bigint,
  person_id bigint,
  measurement_concept_id bigint,
  measurement_date date,
  measurement_datetime string,
  measurement_time string,
  measurement_type_concept_id bigint,
  operator_concept_id bigint,
  value_as_number string,
  value_as_concept_id bigint,
  unit_concept_id bigint,
  range_low string,
  range_high string,
  provider_id string,
  visit_occurrence_id bigint,
  visit_detail_id bigint,
  measurement_source_value string,
  measurement_source_concept_id bigint,
  unit_source_value string,
  value_source_value string
);

-- COMMAND ----------

-- Cannot safely cast 'provider_id': string to bigint;
drop table if exists dua_052538_nwi388.OBSERVATION_$job;
create table dua_052538_nwi388.OBSERVATION_$job (
  observation_id bigint,
  person_id bigint,
  observation_concept_id bigint,
  observation_date date,
  observation_datetime string,
  observation_type_concept_id bigint,
  value_as_number string,
  value_as_string string,
  value_as_concept_id bigint,
  qualifier_concept_id bigint,
  unit_concept_id bigint,
  provider_id string,
  visit_occurrence_id bigint,
  visit_detail_id bigint,
  observation_source_value string,
  observation_source_concept_id bigint,
  unit_source_value string,
  qualifier_source_value string
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.death_$job;
create table dua_052538_nwi388.DEATH_$job (
  person_id bigint,
  death_date date,
  death_datetime string,
  death_type_concept_id bigint,
  cause_concept_id bigint,
  cause_source_value string,
  cause_source_concept_id bigint
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.note_$job;
create table dua_052538_nwi388.NOTE_$job (
  note_id bigint,
  person_id bigint,
  note_date date,
  note_datetime string,
  note_type_concept_id bigint,
  note_class_concept_id bigint,
  note_title string,
  note_text string,
  encoding_concept_id bigint,
  language_concept_id bigint,
  provider_id bigint,
  visit_occurrence_id bigint,
  visit_detail_id bigint,
  note_source_value string
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.NOTE_NLP_$job;
create table dua_052538_nwi388.NOTE_NLP_$job (
  note_nlp_id bigint,
  note_id bigint,
  section_concept_id bigint,
  snippet string,
  offset string,
  lexical_variant string,
  note_nlp_concept_id bigint,
  note_nlp_source_concept_id bigint,
  nlp_system string,
  nlp_date date,
  nlp_datetime string,
  term_exists string,
  term_temporal string,
  term_modifiers string
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.SPECIMEN_$job;
create table dua_052538_nwi388.SPECIMEN_$job (
  specimen_id bigint,
  person_id bigint,
  specimen_concept_id bigint,
  specimen_type_concept_id bigint,
  specimen_date date,
  specimen_datetime string,
  quantity string,
  unit_concept_id bigint,
  anatomic_site_concept_id bigint,
  disease_status_concept_id bigint,
  specimen_source_id string,
  specimen_source_value string,
  unit_source_value string,
  anatomic_site_source_value string,
  disease_status_source_value string
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.FACT_RELATIONSHIP_$job;
create table dua_052538_nwi388.FACT_RELATIONSHIP_$job (
  domain_concept_id_1 bigint,
  fact_id_1 bigint,
  domain_concept_id_2 bigint,
  fact_id_2 bigint,
  relationship_concept_id bigint
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.LOCATION_$job;
create table dua_052538_nwi388.LOCATION_$job (
  location_id bigint,
  address_1 string,
  address_2 string,
  city string,
  state string,
  zip string,
  county string,
  location_source_value string
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.care_site_$job;
create table dua_052538_nwi388.CARE_SITE_$job (
  care_site_id bigint,
  care_site_name string,
  place_of_service_concept_id bigint,
  location_id bigint,
  care_site_source_value string,
  place_of_service_source_value string
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.payer_plan_period_$job;
create table dua_052538_nwi388.PAYER_PLAN_PERIOD_$job (
  payer_plan_period_id bigint,
  person_id bigint,
  payer_plan_period_start_date date,
  payer_plan_period_end_date date,
  payer_concept_id bigint,
  payer_source_value string,
  payer_source_concept_id bigint,
  plan_concept_id bigint,
  plan_source_value string,
  plan_source_concept_id bigint,
  sponsor_concept_id bigint,
  sponsor_source_value string,
  sponsor_source_concept_id bigint,
  family_source_value string,
  stop_reason_concept_id bigint,
  stop_reason_source_value string,
  stop_reason_source_concept_id bigint
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.cost_$job;
create table dua_052538_nwi388.COST_$job (
  cost_id bigint,
  cost_event_id bigint,
  cost_domain_id string,
  cost_type_concept_id bigint,
  currency_concept_id bigint,
  total_charge string,
  total_cost string,
  total_paid string,
  paid_by_payer string,
  paid_by_patient string,
  paid_patient_copay string,
  paid_patient_coinsurance string,
  paid_patient_deductible string,
  paid_by_primary string,
  paid_ingredient_cost string,
  paid_dispensing_fee string,
  payer_plan_period_id bigint,
  amount_allowed string,
  revenue_code_concept_id bigint,
  revenue_code_source_value string,
  drg_concept_id bigint,
  drg_source_value string
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.drug_era_$job;
create table dua_052538_nwi388.DRUG_ERA_$job (
  drug_era_id bigint,
  person_id bigint,
  drug_concept_id bigint,
  drug_era_start_date string,
  drug_era_end_date string,
  drug_exposure_count bigint,
  gap_days bigint
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.dose_era_$job;
create table dua_052538_nwi388.DOSE_ERA_$job (
  dose_era_id bigint,
  person_id bigint,
  drug_concept_id bigint,
  unit_concept_id bigint,
  dose_value string,
  dose_era_start_date string,
  dose_era_end_date string
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.CONDITION_ERA_$job;
create table dua_052538_nwi388.CONDITION_ERA_$job (
  condition_era_id bigint,
  person_id bigint,
  condition_concept_id bigint,
  condition_era_start_date string,
  condition_era_end_date string,
  condition_occurrence_count bigint
);

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

drop table if exists dua_052538_nwi388.CONCEPT;
create table dua_052538_nwi388.CONCEPT (
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

drop table if exists dua_052538_nwi388.VOCABULARY;
create table dua_052538_nwi388.VOCABULARY (
  vocabulary_id string,
  vocabulary_name string,
  vocabulary_reference string,
  vocabulary_version string,
  vocabulary_concept_id bigint
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.DOMAIN;
create table dua_052538_nwi388.DOMAIN (
  domain_id string,
  domain_name string,
  domain_concept_id bigint
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.CONCEPT_CLASS;
create table dua_052538_nwi388.CONCEPT_CLASS (
  concept_class_id string,
  concept_class_name string,
  concept_class_concept_id bigint
);

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

drop table if exists dua_052538_nwi388.CONCEPT_SYNONYM;
create table dua_052538_nwi388.CONCEPT_SYNONYM (
  concept_id bigint,
  concept_synonym_name string,
  language_concept_id bigint
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.CONCEPT_ANCESTOR;
create table dua_052538_nwi388.CONCEPT_ANCESTOR (
  ancestor_concept_id bigint,
  descendant_concept_id bigint,
  min_levels_of_separation bigint,
  max_levels_of_separation bigint
);

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

drop table if exists dua_052538_nwi388.ATTRIBUTE_DEFINITION;
create table dua_052538_nwi388.ATTRIBUTE_DEFINITION (
  attribute_definition_id bigint,
  attribute_name string,
  attribute_description string,
  attribute_type_concept_id bigint,
  attribute_syntax string
);
