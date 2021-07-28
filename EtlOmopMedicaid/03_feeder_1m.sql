-- Databricks notebook source
create widget text state default "WI";

-- COMMAND ----------

--demog elig base

drop view if exists demog_elig_base;
create view demog_elig_base as
select
  *
from
  dua_052538.tafr18_demog_elig_base
where
  state_cd in($state)--will this work? test this
  and bene_id is not null;

  

-- COMMAND ----------

--demog elig dates
drop view if exists demog_elig;
create view demog_elig as
select
  *
from
  dua_052538.tafr18_demog_elig_dates
where
  state_cd in($state)
  and bene_id is not null;

-- COMMAND ----------

--inpatient header
drop view if exists inpatient_header;
create view inpatient_header as
select
  *
from
  dua_052538.tafr18_inpatient_header_06
where
  state_cd in($state)
  and bene_id is not null;

-- COMMAND ----------

--inpatient line
drop view if exists inpatient_line;
create view inpatient_line as
select
  *
from
  dua_052538.tafr18_inpatient_line_06
where
  state_cd in($state)
  and bene_id is not null;

-- COMMAND ----------

--other services header
drop view if exists other_services_header;
create view other_services_header as
select
  *
from
  dua_052538.tafr18_other_services_header_06
where
  state_cd in($state)
  and bene_id is not null;

-- COMMAND ----------

--other services line
drop view if exists other_services_line;
create view other_services_line as
select
  *
from
  dua_052538.tafr18_other_services_line_06
where
  state_cd in($state)
  and bene_id is not null;

-- COMMAND ----------

--other services line for provider
drop view if exists other_services_line_p;
create view other_services_line_p as
select clm_id as clm_id_b,SRVC_PRVDR_NPI 
from dua_052538.tafr18_other_services_line_06
where
  state_cd in($state)
  and bene_id is not null;

-- COMMAND ----------

--long term header
drop view if exists long_term_header;
create view long_term_header as
select
  *
from
  dua_052538.tafr18_long_term_header_06
where
  state_cd in($state)
  and bene_id is not null;

-- COMMAND ----------

--long term line
drop view if exists long_term_line;
create view long_term_line as
select
  *
from
  dua_052538.tafr18_long_term_line_06
where
  state_cd in($state)
  and bene_id is not null;

-- COMMAND ----------

--rx header
drop view if exists rx_header;
create view rx_header as
select
  *
from
  dua_052538.tafr18_rx_header_06
where
  state_cd in($state)
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
  state_cd in($state)
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
  state_cd in($state)
  and bene_id is not null;

-- COMMAND ----------

--Medicaid TAF  ends
