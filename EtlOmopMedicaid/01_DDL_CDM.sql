-- Databricks notebook source
insert into dua_052538_nwi388.log values('$job_id','DDL','0','start DDL',current_timestamp(),null);

-- COMMAND ----------

create widget text job_id default "101";

-- COMMAND ----------


drop table if exists dua_052538_nwi388.PERSON;
create table dua_052538_nwi388.PERSON (
  person_id string,
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

insert into dua_052538_nwi388.log values('$job_id','DDL','1','create ddl person',current_timestamp(),null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.OBSERVATION_PERIOD;
create table dua_052538_nwi388.OBSERVATION_PERIOD (
  observation_period_id double,
  person_id string,
  observation_period_start_date date,
  observation_period_end_date date,
  period_type_concept_id string
);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','2','create ddl observation_period',current_timestamp(),null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.provider;
create table dua_052538_nwi388.PROVIDER 
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

insert into dua_052538_nwi388.log values('$job_id','DDL','3','create ddl provider',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.VISIT_OCCURRENCE;
create table dua_052538_nwi388.VISIT_OCCURRENCE (
  visit_occurrence_id string,
  person_id string,
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
  preceding_visit_occurrence_id string
);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','4','create ddl visit_occurrence',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.VISIT_DETAIL;
create table dua_052538_nwi388.VISIT_DETAIL (
  visit_detail_id string,
  person_id string,
  visit_detail_concept_id bigint,
  visit_detail_start_date date,
  visit_detail_start_datetime string,
  visit_detail_end_date date,
  visit_detail_end_datetime string,
  visit_detail_type_concept_id bigint,
  provider_id bigint,
  care_site_id bigint,
  visit_detail_source_value string,
  visit_detail_source_concept_id string,
  admitting_source_value string,
  admitting_source_concept_id string,
  discharge_to_source_value string,
  discharge_to_concept_id string,
  preceding_visit_detail_id string,
  visit_detail_parent_id string,
  visit_occurrence_id string
);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','5','create ddl visit_detail',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.PROCEDURE_OCCURRENCE;
create table dua_052538_nwi388.PROCEDURE_OCCURRENCE (
  procedure_occurrence_id bigint,
  person_id string,
  procedure_concept_id string,
  procedure_date date,
  procedure_datetime string,
  procedure_type_concept_id string,
  modifier_concept_id string,
  quantity bigint,
  provider_id string,
  visit_occurrence_id string,
  visit_detail_id string,
  procedure_source_value string,
  procedure_source_concept_id string,
  modifier_source_value string
);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','6','create ddl procedure_occurrence',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.CONDITION_OCCURRENCE;
create table dua_052538_nwi388.CONDITION_OCCURRENCE (
  condition_occurrence_id bigint,
  person_id string,
  condition_concept_id bigint,
  condition_start_date date,
  condition_start_datetime string,
  condition_end_date date,
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

insert into dua_052538_nwi388.log values('$job_id','DDL','7','create ddl condition_occurrence',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.DRUG_EXPOSURE;
create table dua_052538_nwi388.DRUG_EXPOSURE (
  drug_exposure_id bigint,
  person_id string,
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
  days_supply string,
  sig string,
  route_concept_id bigint,
  lot_number string,
  provider_id string,
  visit_occurrence_id string,
  visit_detail_id bigint,
  drug_source_value string,
  drug_source_concept_id bigint,
  route_source_value string,
  dose_unit_source_value string
);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','8','create ddl drug_exposure',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.DEVICE_EXPOSURE;
create table dua_052538_nwi388.DEVICE_EXPOSURE (
  device_exposure_id bigint,
  person_id string,
  device_concept_id bigint,
  device_exposure_start_date date,
  device_exposure_start_datetime string,
  device_exposure_end_date date,
  device_exposure_end_datetime string,
  device_type_concept_id bigint,
  unique_device_id string,
  quantity bigint,
  provider_id bigint,
  visit_occurrence_id string,
  visit_detail_id bigint,
  device_source_value string,
  device_source_concept_id bigint
);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','9','create ddl device_exposure',current_timestamp(), null);

-- COMMAND ----------


drop table if exists dua_052538_nwi388.MEASUREMENT;
create table dua_052538_nwi388.MEASUREMENT (
  measurement_id bigint,
  person_id string,
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
  visit_occurrence_id string,
  visit_detail_id bigint,
  measurement_source_value string,
  measurement_source_concept_id bigint,
  unit_source_value string,
  value_source_value string
);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','10','create ddl measurement',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.OBSERVATION;
create table dua_052538_nwi388.OBSERVATION(
  observation_id bigint,
  person_id string,
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
  visit_occurrence_id string,
  visit_detail_id bigint,
  observation_source_value string,
  observation_source_concept_id bigint,
  unit_source_value string,
  qualifier_source_value string
);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','11','create ddl observation',current_timestamp(), null);

-- COMMAND ----------


drop table if exists dua_052538_nwi388.death;
create table dua_052538_nwi388.DEATH (
  person_id string,
  death_date date,
  death_datetime string,
  death_type_concept_id bigint,
  cause_concept_id bigint,
  cause_source_value string,
  cause_source_concept_id bigint
);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.note;
create table dua_052538_nwi388.NOTE (
  note_id bigint,
  person_id string,
  note_date date,
  note_datetime string,
  note_type_concept_id bigint,
  note_class_concept_id bigint,
  note_title string,
  note_text string,
  encoding_concept_id bigint,
  language_concept_id bigint,
  provider_id bigint,
  visit_occurrence_id string,
  visit_detail_id string,
  note_source_value string
);

-- COMMAND ----------



-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','13','create ddl note',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.NOTE_NLP;
create table dua_052538_nwi388.NOTE_NLP (
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

insert into dua_052538_nwi388.log values('$job_id','DDL','14','create ddl note_nlp',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.SPECIMEN;
create table dua_052538_nwi388.SPECIMEN (
  specimen_id bigint,
  person_id string,
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

insert into dua_052538_nwi388.log values('$job_id','DDL','15','create ddl specimen',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.LOCATION;
create table dua_052538_nwi388.LOCATION (
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

insert into dua_052538_nwi388.log values('$job_id','DDL','16','create ddl location',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.care_site;
create table dua_052538_nwi388.CARE_SITE (
  care_site_id bigint,
  care_site_name string,
  place_of_service_concept_id bigint,
  location_id bigint,
  care_site_source_value string,
  place_of_service_source_value string
);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','17','create ddl care_site',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.payer_plan_period;
create table dua_052538_nwi388.PAYER_PLAN_PERIOD (
  payer_plan_period_id bigint,
  person_id string,
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

insert into dua_052538_nwi388.log values('$job_id','DDL','18','create ddl payer_plan_period',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.cost;
create table dua_052538_nwi388.COST (
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

insert into dua_052538_nwi388.log values('$job_id','DDL','19','create ddl cost',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.drug_era;
create table dua_052538_nwi388.DRUG_ERA (
  drug_era_id bigint,
  person_id string,
  drug_concept_id bigint,
  drug_era_start_date string,
  drug_era_end_date string,
  drug_exposure_count bigint,
  gap_days bigint
);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','20','create ddl drug_era',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.dose_era;
create table dua_052538_nwi388.DOSE_ERA (
  dose_era_id bigint,
  person_id string,
  drug_concept_id bigint,
  unit_concept_id bigint,
  dose_value string,
  dose_era_start_date string,
  dose_era_end_date string
);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','21','create ddl dose_era',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.condition_era;
create table dua_052538_nwi388.condition_era(
  condition_era_id bigint,
  person_id string,
  condition_concept_id bigint,
  condition_era_start_date date,
  condition_era_end_date date,
  condition_occurrence_count bigint
);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','22','create ddl conditon_era',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.FACT_RELATIONSHIP;
create table dua_052538_nwi388.FACT_RELATIONSHIP (
  domain_concept_id_1 string,
  fact_id_1  string,
  domain_concept_id_2 string,
  fact_id_2  string,
  domain_concept_id_3 string,
  fact_id_3  string,
  relationship_concept_id string
  
);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','23','create ddl fact_relationship',current_timestamp(),null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','DDL','24','end ddl',current_timestamp(), null);