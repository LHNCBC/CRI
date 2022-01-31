-- Databricks notebook source
--wipe only removes views and tables made in the TL process.
--DDL removes tables made in the E process.

-- COMMAND ----------

--Location
drop table if exists dua_052538_nwi388.hold_location;
drop view if exists hold_location_a;
drop view if exists hold_location_b;
drop view if exists transform_location;

-- COMMAND ----------

--Care Site
drop table if exists  dua_052538_nwi388.hold_care_site ;
drop view if exists route_care_site;

-- COMMAND ----------

--Provider
drop table if exists dua_052538_nwi388.hold_provider1;
drop table if exists dua_052538_nwi388.transform_provider;
drop table if exists dua_052538_nwi388.transform_provider_details;
 drop table if exists dua_052538_nwi388.transform_provider_specialty;
 drop table if exists dua_052538_nwi388.provider_detail_a;
 drop table if exists dua_052538_nwi388.provider_detail_b;
 drop view if exists provider_detail;
 drop table if exists dua_052538_nwi388.provider_detail_all;

-- COMMAND ----------

--Visit Occurrence
drop table if exists dua_052538_nwi388.hold_visit_occurrence;
drop view if exists lkup_cmspos;
drop table if exists dua_052538_nwi388.provider_id_for_visit_occurrence;

-- COMMAND ----------

--Condition Occurrence
drop view if exists lkup_dx;
drop table if exists dua_052538_nwi388.hold_condition_occurrence;

-- COMMAND ----------

--Procedure Occurrence
drop view if exists lkup_px;
drop table if exists dua_052538_nwi388.provider_id_for_dxpxrx;
drop table if exists dua_052538_nwi388.hold_procedure_occurrence;
drop table if exists dua_052538_nwi388.hold_procedure_occurrence_d;
drop table if exists dua_052538_nwi388.transform_procedure_occurrence_d1;

-- COMMAND ----------

--Drug Exposure
drop view if exists lkup_NDC;
drop table if exists dua_052538_nwi388.hold_drug_exposure;

-- COMMAND ----------

--Person
drop view if exists person_distinct;
drop view if exists route_person;


-- COMMAND ----------

--Observation Period
drop table if exists dua_052538_nwi388.route_observation_period;

-- COMMAND ----------

--Death

-- COMMAND ----------

--Observation

-- COMMAND ----------

--Measurement