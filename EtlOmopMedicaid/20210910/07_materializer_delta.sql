-- Databricks notebook source
----create view for inpatient header original tables 
--when in etl_hold where and join, use zorder(join) and partrition(where)

-- COMMAND ----------

create widget text scope default "1s1m";
create widget text job_id default "'000'";
create widget text state default "WA";
create widget text drawer default "1s1m_wa";


-- COMMAND ----------


drop table if exists dua_052538_nwi388.demog_elig_base$drawer;
create table dua_052538_nwi388.demog_elig_base$drawer using delta
partitioned by (state_cd) as
select
  *
from
  demog_elig_base;
optimize dua_052538_nwi388.demog_elig_base$drawer  ZORDER by(death_dt);

-- COMMAND ----------

insert into
  dua_052538_nwi388.h_log
values(
    '$job_id',
    'Materializer_delta',
    '2',
    'create view demog_elig_base',
    current_timestamp(),
    $state,
    '$job',
    null
  );

-- COMMAND ----------

--demog elig dates
drop table if exists dua_052538_nwi388.demog_elig_dates$drawer;
create table dua_052538_nwi388.demog_elig_dates$drawer using delta
partitioned by (state_cd) as
select
  *
from
  demog_elig;

-- COMMAND ----------

insert into
  dua_052538_nwi388.h_log
values(
    '$job_id',
    'Materializer_delta',
    '4',
    'create view demog_elig_dates',
    current_timestamp(),
    $state,
    '$scope',
    null
  );

-- COMMAND ----------

--inpatient header
drop table if exists dua_052538_nwi388.inpatient_header$drawer;
create table dua_052538_nwi388.inpatient_header$drawer using delta
partitioned by (state_cd) as
select
  *
from
  inpatient_header;
   optimize dua_052538_nwi388.inpatient_header$drawer ZORDER by(clm_id);

-- COMMAND ----------

insert into
  dua_052538_nwi388.h_log
values(
    '$job_id',
    'Materializer_delta',
    '6',
    'create view inpatient_header',
    current_timestamp(),
    $state,
    '$scope',
    null
  );

-- COMMAND ----------

--inpatient line
drop table if exists dua_052538_nwi388.inpatient_line$drawer;
create table dua_052538_nwi388.inpatient_line$drawer  using delta
partitioned by (state_cd) as
select
  *
from
  inpatient_line;
   optimize dua_052538_nwi388.inpatient_line$drawer ZORDER by(clm_id);

-- COMMAND ----------

insert into
  dua_052538_nwi388.h_log
values(
    '$job_id',
    'Materializer_delta',
    '8',
    'create view inpatient_line',
    current_timestamp(),
    $state,
    '$scope',
    null
  );

-- COMMAND ----------

--other services header
drop table if exists dua_052538_nwi388.other_services_header$drawer;
create table dua_052538_nwi388.other_services_header$drawer  using delta
partitioned by (state_cd) as
select
  *
from
  other_services_header;
   optimize dua_052538_nwi388.other_services_header$drawer ZORDER by(clm_id);

-- COMMAND ----------

insert into
  dua_052538_nwi388.h_log
values(
    '$job_id',
    'Materializer_delta',
    '10',
    'create view other_services_line',
    current_timestamp(),
    $state,
    '$scope',
    null
  );

-- COMMAND ----------

--other services line
drop table if exists dua_052538_nwi388.other_services_line$drawer;
create table dua_052538_nwi388.other_services_line$drawer
using delta partitioned by (state_cd) as
select
  *
from
  other_services_line;
  optimize dua_052538_nwi388.other_services_line$drawer ZORDER by(clm_id);

-- COMMAND ----------

insert into
  dua_052538_nwi388.h_log
values(
    '$job_id',
    'Materializer_delta',
    '12',
    'create view other_services_line',
    current_timestamp(),
    $state,
    '$scope',
    null
  );

-- COMMAND ----------

--other services line for provider
drop table if exists dua_052538_nwi388.other_services_line_p$drawer;
create table dua_052538_nwi388.other_services_line_p$drawer 
using delta as
select
  *
from
  other_services_line_p;
   optimize dua_052538_nwi388.other_services_line_p$drawer ZORDER by(clm_id_b);

-- COMMAND ----------

insert into
  dua_052538_nwi388.h_log
values(
    '$job_id',
    'Materializer_delta',
    '14',
    'create view other_services_line_p',
    current_timestamp(),
    $state,
    '$scope',
    null
  );

-- COMMAND ----------

--long term header
drop table if exists dua_052538_nwi388.long_term_header$drawer;
create table dua_052538_nwi388.long_term_header$drawer 
using delta partitioned by (state_cd) as
select
  *
from
  long_term_header;
  optimize dua_052538_nwi388.long_term_header$drawer ZORDER by(clm_id);

-- COMMAND ----------

insert into
  dua_052538_nwi388.h_log
values(
    '$job_id',
    'Materializer_delta',
    '16',
    'create view long_term_header',
    current_timestamp(),
    $state,
    '$scope',
    null
  );

-- COMMAND ----------

--long term line
drop table if exists dua_052538_nwi388.long_term_line$drawer;
create table dua_052538_nwi388.long_term_line$drawer 
using delta partitioned by (state_cd) as
select
  *
from
  long_term_line;
  optimize dua_052538_nwi388.long_term_line$drawer ZORDER by(clm_id);

-- COMMAND ----------

insert into
  dua_052538_nwi388.h_log
values(
    '$job_id',
    'Materializer_delta',
    '18',
    'create view long_term_line',
    current_timestamp(),
    $state,
    '$scope',
    null
  );

-- COMMAND ----------

--rx header
drop table if exists dua_052538_nwi388.rx_header$drawer;
create table dua_052538_nwi388.rx_header$drawer
using delta partitioned by (state_cd) as
select
  *
from
  rx_header;
   optimize dua_052538_nwi388.rx_header$drawer ZORDER by(clm_id);

-- COMMAND ----------

insert into
  dua_052538_nwi388.h_log
values(
    '$job_id',
    'Materializer_delta',
    '20',
    'create view rx_header',
    current_timestamp(),
    $state,
    '$scope',
    null
  );

-- COMMAND ----------

drop table if exists dua_052538_nwi388.rx_header_p$drawer;
create table dua_052538_nwi388.rx_header_p$drawer 
using delta as
select
  *
from
  rx_header_p;
    optimize dua_052538_nwi388. rx_header_p$drawer ZORDER by(clm_id_b);

-- COMMAND ----------

insert into
  dua_052538_nwi388.h_log
values(
    '$job_id',
    'Materializer_delta',
    '22',
    'create view rx_header_p',
    current_timestamp(),
    $state,
    '$scope',
    null
  );

-- COMMAND ----------

--RX Line
drop table if exists dua_052538_nwi388.rx_line$drawer;
create table dua_052538_nwi388.rx_line$drawer 
using delta partitioned by (state_cd) as
select
  *
from
  rx_line;
   optimize dua_052538_nwi388. rx_line$drawer ZORDER by(clm_id);

-- COMMAND ----------

insert into
  dua_052538_nwi388.h_log
values(
    '$job_id',
    'Materializer_delta',
    '24',
    'create view rx_line',
    current_timestamp(),
    $state,
    '$scope',
    null
  );

-- COMMAND ----------

--Medicaid TAF  ends
