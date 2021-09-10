-- Databricks notebook source
create widget text scope default "1s1m";

-- COMMAND ----------

--drop views
drop view if exists route_person;

--feeder
drop view if exists demog_elig_base;
drop view if exists demog_elig;
drop view if exists inpatient_header;
drop view if exists inpatient_line;
drop view if exists other_services_header;
drop view if exists other_services_line;
drop view if exists other_services_line_p;
drop view if exists long_term_header;
drop view if exists long_term_line;
drop view if exists rx_header;
drop view if exists rx_header_p;
drop view if exists rx_line;

--etl
drop view if exists route_person;
drop view if exists transform_provider;
drop view if exists provider_detail;
drop view if exists lkup_cmspos;
drop view if exists transform_visit_occurrence;
drop view if exists provider_id_for_visit_occurrence;
drop view if exists visit_id;
drop view if exists visit_id_for_visit_occurrence;
drop view if exists transform_procedure_occurrence_d1;
drop view if exists provider_id_for_dxpxrx;
drop view if exists visit_id_for_dxpxrx;
drop view if exists lkup_px;
drop view if exists route_procedure_occurrence;
drop view if exists lkup_ICD10CM;
drop view if exists route_condition_occurrence;
drop view if exists lkup_NDC;
drop view if exists route_drug_exposure;

-- COMMAND ----------

--drop tables


--ddl
drop table if exists dua_052538_nwi388.PERSON_$scope;
drop table if exists dua_052538_nwi388.OBSERVATION_PERIOD_$scope;
drop table if exists dua_052538_nwi388.provider_$scope;
drop table if exists dua_052538_nwi388.VISIT_OCCURRENCE_$scope;
drop table if exists dua_052538_nwi388.VISIT_DETAIL_$scope;
drop table if exists dua_052538_nwi388.PROCEDURE_OCCURRENCE_$scope;
drop table if exists dua_052538_nwi388.CONDITION_OCCURRENCE_$scope;
drop table if exists dua_052538_nwi388.DRUG_EXPOSURE_$scope;
drop table if exists dua_052538_nwi388.DEVICE_EXPOSURE_$scope;
drop table if exists dua_052538_nwi388.MEASUREMENT_$scope;
drop table if exists dua_052538_nwi388.OBSERVATION_$scope;
drop table if exists dua_052538_nwi388.death_$scope;
drop table if exists dua_052538_nwi388.note_$scope;
drop table if exists dua_052538_nwi388.NOTE_NLP_$scope;
drop table if exists dua_052538_nwi388.SPECIMEN_$scope;
drop table if exists dua_052538_nwi388.FACT_RELATIONSHIP_$scope;
drop table if exists dua_052538_nwi388.LOCATION_$scope;
drop table if exists dua_052538_nwi388.care_site_$scope;
drop table if exists dua_052538_nwi388.payer_plan_period_$scope;
drop table if exists dua_052538_nwi388.cost_$scope;
drop table if exists dua_052538_nwi388.drug_era_$scope;
drop table if exists dua_052538_nwi388.dose_era_$scope;
drop table if exists dua_052538_nwi388.CONDITION_ERA_$scope;
drop table if exists dua_052538_nwi388.METADATA;
drop table if exists dua_052538_nwi388.CDM_SOURCE;
drop table if exists dua_052538_nwi388.CONCEPT;
drop table if exists dua_052538_nwi388.VOCABULARY;
drop table if exists dua_052538_nwi388.DOMAIN;
drop table if exists dua_052538_nwi388.CONCEPT_CLASS;
drop table if exists dua_052538_nwi388.CONCEPT_RELATIONSHIP;
drop table if exists dua_052538_nwi388.RELATIONSHIP;
drop table if exists dua_052538_nwi388.CONCEPT_SYNONYM;
drop table if exists dua_052538_nwi388.CONCEPT_ANCESTOR;
drop table if exists dua_052538_nwi388.SOURCE_TO_CONCEPT_MAP;
drop table if exists dua_052538_nwi388.DRUG_STRENGTH;
drop table if exists dua_052538_nwi388.COHORT_DEFINITION;
drop table if exists dua_052538_nwi388.ATTRIBUTE_DEFINITION;

--etl
drop table if exists dua_052538_nwi388.route_person;
drop table if exists dua_052538_nwi388.route_observation_period;
drop table if exists dua_052538_nwi388.route_death;
drop table if exists dua_052538_nwi388.hold_provider1;
drop table if exists dua_052538_nwi388.transform_provider_details;
drop table if exists dua_052538_nwi388.transform_provider_specialty;
drop table if exists dua_052538_nwi388.provider_detail_a;
drop table if exists dua_052538_nwi388.provider_detail_b;
drop table if exists dua_052538_nwi388.provider_detail_all;
drop table if exists dua_052538_nwi388.route_provider;
drop table if exists dua_052538_nwi388.hold_visit_occurrence;
drop table if exists dua_052538_nwi388.route_visit_occurrence;
drop table if exists dua_052538_nwi388.hold_procedure_occurrence;
drop table if exists dua_052538_nwi388.hold_procedure_occurrence_d;
drop table if exists dua_052538_nwi388.route_PROCEDURE_OCCURRENCE;
drop table if exists dua_052538_nwi388.hold_condition_occurrence;
drop table if exists dua_052538_nwi388.route_condition_occurrence;
drop table if exists dua_052538_nwi388.hold_drug_exposure;
drop table if exists dua_052538_nwi388.route_DRUG_EXPOSURE;
drop table if exists dua_052538_nwi388.route_observation;
drop table if exists dua_052538_nwi388.route_MEASUREMENT;
 
