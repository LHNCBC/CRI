-- Databricks notebook source
--table
--person
--todo this method only works on one year of data
--todo multi year data must be deduplicated
--todo multi year data must use smart demography
--todo verify case when codes
--note
drop view if exists transform_person;
create view transform_person as
select
  BENE_ID,
  case
    when SEX_CD = 'F' then 8532
    when SEX_CD = 'M' then 8507
    else 0
  end as gender_concept_id,
  year(BIRTH_DT) as year_of_birth,
  month(BIRTH_DT) as month_of_birth,
  day(BIRTH_DT) as day_of_birth,
  case
    when RACE_ETHNCTY_CD = 1 then 8527
    when RACE_ETHNCTY_CD = 2 then 8516
    when RACE_ETHNCTY_CD = 3 then 8515
    when RACE_ETHNCTY_CD = 4 then 8657
    when RACE_ETHNCTY_CD = 5 then 8557
    when RACE_ETHNCTY_CD = 6 then 8522
    when RACE_ETHNCTY_CD = 7 then 38003563
    else 0
  end as race_concept_id,
  case
    when ETHNCTY_CD = 0 then 38003564
    when ETHNCTY_CD > 0 then 38003563
    else 0
  end as ethnicity_concept_id,
  bene_id as person_source_value,
  SEX_CD as gender_source_value,
  RACE_ETHNCTY_CD as race_source_value,
  ETHNCTY_CD as ethnicity_source_value,
  STATE_CD
from
  demog_elig_base;
insert into
  dua_052538_NWI388.PERSON_x
select
  BENE_ID as person_id,
  gender_concept_id,
  year_of_birth,
  month_of_birth,
  day_of_birth,
  null as birth_datetime,
  race_concept_id,
  ethnicity_concept_id,
  null as location_id,
  null as provider_id,
  null as care_site_id,
  bene_id as person_source_value,
  gender_source_value,
  null as gender_source_concept_id,
  race_source_value,
  null as race_source_concept_id,
  ethnicity_source_value,
  null as ethnicity_source_concept_id
from
  transform_person;

-- COMMAND ----------

--table
--observation_period
--todo
--note no need to use origin/all source data comes from a single table called demog_elig.
insert into
  dua_052538_nwi388.OBSERVATION_PERIOD_x
select
  ROW_NUMBER() OVER(
    ORDER BY
      bene_id ASC
  ) AS observation_period_id,
  -- I dont know that this is right
  bene_id as person_id,
  enrlmt_start_dt as observation_period_start_date,
  enrlmt_end_dt as observation_period_end_date,
  32817 as period_type_concept_id,
  -- 32817=EHR.--no suitable concept for insurance record of enrollment
  state_cd
from
  demog_elig
where
  bene_id is not null;
insert into
  dua_052538_nwi388.OBSERVATION_PERIOD
select
  observation_period_id,
  person_id,
  observation_period_start_date,
  observation_period_end_date,
  period_type_concept_id
from
  dua_052538_nwi388.OBSERVATION_PERIOD_x;

-- COMMAND ----------

--table
--provider
--todo make srvc provider the standard for other services
--todo resovlve RX provider as well
--note npi values come from npi db-
--note diffrent provider classes make diffrent provider tables
drop table if exists dua_052538_nwi388.transform_provider;
create table dua_052538_nwi388.transform_provider(
  forign_key string,
  complex_id string,
  npi string,
  specialty_source_value string,
  class string,
  origin_file string
);
--inpt head
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32855) as forign_key,
  --inpatient claim header
  concat(state_cd, clm_id, "inpatient_header") as complex_id,
  RFRG_PRVDR_NPI as npi,
  RFRG_PRVDR_SPCLTY_CD as specialty_source_value,
  "RFRG" as class,
  "inpatient_header" as origin_file
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32855) as forign_key,
  --inpatient claim header
  concat(state_cd, clm_id, "inpatient_header") as complex_id,
  ADMTG_PRVDR_NPI as npi,
  ADMTG_PRVDR_SPCLTY_CD as specialty_source_value,
  "ADMTG" as class,
  "inpatient_header" as origin_file
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32855) as forign_key,
  --inpatient claim header
  concat(state_cd, clm_id, "inpatient_header") as complex_id,
  BLG_PRVDR_NPI as npi,
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "inpatient_header" as origin_file
from
  inpatient_header;
--impatient line
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32855) as forign_key,
  --inpatient claim header
  concat(state_cd, clm_id, "inpatient_line") as complex_id,
  SRVC_PRVDR_NPI as npi,
  SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
  "SRVC" as class,
  "inpatient_line" as origin_file
from
  inpatient_line;
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32855) as forign_key,
  --inpatient claim header
  concat(state_cd, clm_id, "inpatient_line") as complex_id,
  OPRTG_PRVDR_NPI as npi,
  "NULL" as specialty_source_value,
  "OPRTG" as class,
  "inpatient_line" as origin_file
from
  inpatient_line;
--other services header
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32861) as forign_key,
  --outpatient claim header
  concat(state_cd, clm_id, "other_services_header") as complex_id,
  BLG_PRVDR_NPI as npi,
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "other_services_header" as origin_file
from
  other_services_header;
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32861) as forign_key,
  --outpatient claim header
  concat(state_cd, clm_id, "other_services_header") as complex_id,
  RFRG_PRVDR_NPI as npi,
  RFRG_PRVDR_SPCLTY_CD as specialty_source_value,
  "RFRG" as class,
  "other_services_header" as origin_file
from
  other_services_header;
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32861) as forign_key,
  --outpatient claim header
  concat(state_cd, clm_id, "other_services_header") as complex_id,
  drctng_prvdr_npi as npi,
  "NULL" as specialty_source_value,
  "DRCTNG" as class,
  "other_services_header" as origin_file
from
  other_services_header;
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32861) as forign_key,
  --outpatient claim header
  concat(state_cd, clm_id, "other_services_header") as complex_id,
  sprvsng_prvdr_npi as npi,
  "NULL" as specialty_source_value,
  "SPRVSNG" as class,
  "other_services_header" as origin_file
from
  other_services_header;
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32861) as forign_key,
  --outpatient claim header
  concat(state_cd, clm_id, "other_services_header") as complex_id,
  hlth_home_prvdr_npi as npi,
  "NULL" as specialty_source_value,
  "HLTH_HOME" as class,
  "other_services_header" as origin_file
from
  other_services_header;
--other services line
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32861) as forign_key,
  --outpatient claim header
  concat(state_cd, clm_id, "other_services_line") as complex_id,
  SRVC_PRVDR_NPI as npi,
  SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
  "SRVC" as class,
  "other_services_line" as origin_file
from
  other_services_line;
--lt header
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32846) as forign_key,
  --Facility claim header
  concat(state_cd, clm_id, "long_term_header") as complex_id,
  RFRG_PRVDR_NPI as npi,
  RFRG_PRVDR_SPCLTY_CD as specialty_source_value,
  "RFRG" as class,
  "long_term_header" as origin_file
from
  long_term_header;
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32846) as forign_key,
  --Facility claim header
  concat(state_cd, clm_id, "long_term_header") as complex_id,
  ADMTG_PRVDR_NPI as npi,
  ADMTG_PRVDR_SPCLTY_CD as specialty_source_value,
  "ADMTG" as class,
  "long_term_header" as origin_file
from
  long_term_header;
-- lt line
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32846) as forign_key,
  --Facility claim header
  concat(state_cd, clm_id, "long_term_header") as complex_id,
  BLG_PRVDR_NPI as npi,
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "long_term_header" as origin_file
from
  long_term_header;
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32846) as forign_key,
  --Facility claim header
  concat(state_cd, clm_id, "long_term_line") as complex_id,
  SRVC_PRVDR_NPI as npi,
  SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
  "SRVC" as class,
  "long_term_line" as origin_file
from
  long_term_line;
--rx head
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32869) as forign_key,
  --Pharmacy claim
  concat(state_cd, clm_id, "rx_header") as complex_id,
  BLG_PRVDR_NPI as npi,
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "rx_header" as origin_file
from
  rx_header;
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32869) as forign_key,
  --Pharmacy claim
  concat(state_cd, clm_id, "rx_header") as complex_id,
  PRSCRBNG_PRVDR_NPI as npi,
  "NULL" as specialty_source_value,
  "PRSCRBING" as class,
  "rx_header" as origin_file
from
  rx_header;
insert into
  dua_052538_nwi388.transform_provider
select
  concat(clm_id, state_cd, 32869) as forign_key,
  --Pharmacy claim
  concat(state_cd, clm_id, "rx_header") as complex_id,
  DSPNSNG_PRVDR_NPI as npi,
  "NULL" as specialty_source_value,
  "DSPNSNG" as class,
  "rx_header" as origin_file
from
  rx_header;

-- COMMAND ----------

--note do facility level providers go in the provider table?
drop view if exists provider_table_route;
create view provider_table_route as
select
  distinct a.npi,
  specialty_source_value,
  class,
  Entity_type_code,
  provider_organization_name_legal_business_name,
  provider_last_name_legal_name,
  provider_first_name,
  provider_name_prefix_text,
  healthcare_provider_taxonomy_group_1,
  origin_file,
  complex_id,
  forign_key
from
  dua_052538_nwi388.transform_provider a
  left join NPI_provider b on a.npi = b.npi
where
  a.npi is not null;

-- COMMAND ----------

--note the goal here is to cut NPI file in half, where facility names and indivdual names are covered fully
--note if you want to drop facility providers from provider table this is where to do it
drop table if exists dua_052538_nwi388.provider_for_provider_table;
create table dua_052538_nwi388.provider_for_provider_table (
  provider_name string,
  npi string,
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
  class string,
  complex_id string
);
--care site is currently unresolved
--it should come from visit location table as providers should be one record per NPI, not per npi-per-locaiton
insert into
  dua_052538_nwi388.provider_for_provider_table
select
  concat(
    provider_first_name,
    "_",
    provider_last_name_legal_name
  ) as provider_name,
  npi,
  null as dea,
  --we will never have this
  null as specialty_concept_id,
  --requires ccw dicitonary
  null as care_site_id,
  --requires location table
  null as year_of_birth,
  --we will never have this
  null as gender_concept_id,
  --we will never have this
  npi as provider_source_value,
  specialty_source_value,
  null as specialty_source_concept_id,
  --requires ccw dicitonary
  null as gender_source_value,
  --we will never have this
  null as gender_source_concept_id,
  --we will never have this
  class,
  complex_id
from
  provider_table_route
where
  npi != 'NA'
  and provider_last_name_legal_name != 'NA';
insert into
  dua_052538_nwi388.provider_for_provider_table
select
  concat(
    provider_first_name,
    "_",
    provider_last_name_legal_name
  ) as provider_name,
  npi,
  null as dea,
  null as specialty_concept_id,
  null as care_site_id,
  null as year_of_birth,
  null as gender_concept_id,
  npi as provider_source_value,
  specialty_source_value,
  null as specialty_source_concept_id,
  null as gender_source_value,
  null as gender_source_concept_id,
  class,
  complex_id
from
  provider_table_route
where
  npi != 'NA'
  and provider_last_name_legal_name != 'NA';
insert into
  dua_052538_nwi388.provider_for_provider_table
select
  provider_organization_name_legal_business_name as provider_name,
  npi,
  null as dea,
  null as specialty_concept_id,
  null as care_site_id,
  null as year_of_birth,
  null as gender_concept_id,
  npi as provider_source_value,
  specialty_source_value,
  null as specialty_source_concept_id,
  null as gender_source_value,
  null as gender_source_concept_id,
  class,
  complex_id
from
  provider_table_route
where
  npi != 'NA'
  and provider_last_name_legal_name != 'NA';
insert into
  dua_052538_nwi388.provider_routes
select
  provider_organization_name_legal_business_name as provider_name,
  npi,
  null as dea,
  null as specialty_concept_id,
  null as care_site_id,
  null as year_of_birth,
  null as gender_concept_id,
  npi as provider_source_value,
  specialty_source_value,
  null as specialty_source_concept_id,
  null as gender_source_value,
  null as gender_source_concept_id,
  class,
  complex_id
from
  provider_table_route
where
  npi != 'NA'
  and provider_last_name_legal_name != 'NA';
--load provider into provider x
insert into
  dua_052538_nwi388.provider_x
select
  distinct npi,
  provider_name,
  dea,
  specialty_concept_id,
  care_site_id,
  year_of_birth,
  gender_concept_id,
  provider_source_value,
  specialty_source_value,
  specialty_source_concept_id,
  gender_source_value,
  gender_source_concept_id,
  ROW_NUMBER() OVER (
    ORDER BY
      (
        SELECT
          1
      )
  ) as proivder_id
from
  dua_052538_nwi388.provider_routes
group by
  npi,
  provider_name,
  dea,
  specialty_concept_id,
  care_site_id,
  year_of_birth,
  gender_concept_id,
  provider_source_value,
  specialty_source_value,
  specialty_source_concept_id,
  gender_source_value,
  gender_source_concept_id;

-- COMMAND ----------

--provider for visit table
--BLG means just header level providers
drop view if exists provider_id_for_visit_occurrence;
create view provider_id_for_visit_occurrence as
select
  distinct npi,
  ROW_NUMBER() OVER (
    ORDER BY
      (
        SELECT
          1
      )
  ) as proivder_id
from
  provider_table_route
where
  class = 'BLG'
  or class = 'SRVC'
group by
  npi;
drop view if exists provider_id_for_dxpxrx;
create view provider_id_for_dxpxrx as
select
  distinct provider_id,
  npi as npi_a
from
  dua_052538_nwi388.provider_x;

-- COMMAND ----------

--table
--h_visit_helper
--note this is where complex ids are made

drop table if exists dua_052538_nwi388.h_visit_helper;
create table dua_052538_nwi388.h_visit_helper (
  bene_id bigint not null,
  visit_start_date date,
  visit_end_date date,
  visit_type_concept_id bigint,
  state_cd string,
  clm_id bigint,
  visit_source_value string ,
  admitting_source_value string,
  visit_source_concept_id bigint,
  BLG_PRVDR_NPI string,
  admiting_source_concept_id string,
  forign_key string
);

insert into
  dua_052538_nwi388.h_visit_helper
select
  bene_id,
  ADMSN_DT,
  DSCHRG_DT,
  32855,  --32855=inpatient claim header
  state_cd,
  clm_id,
  HOSP_TYPE_CD,
  ADMSN_TYPE_CD,
  case 
when HOSP_Type_CD =00 then 42898160
when HOSP_Type_CD =01 then 4318944
when HOSP_Type_CD =02 then 4140387
when HOSP_Type_CD =03 then 37310591
when HOSP_Type_CD =04 then null
when HOSP_Type_CD =05 then 4268912
when HOSP_Type_CD =06 then null
when HOSP_Type_CD =07 then 4305507
when HOSP_Type_CD =08 then null
else null end, 
BLG_PRVDR_NPI,
case
when ADMSN_TYPE_CD=1 then 4079617
when ADMSN_TYPE_CD=2 then 8782
when ADMSN_TYPE_CD=3 then 4314435
when ADMSN_TYPE_CD=4 then 45773140
when ADMSN_TYPE_CD=5 then 4079623
else null end,
concat(clm_id, state_cd,32855) as forign_key
from
  inpatient_header
  where bene_id is not null;

insert into
  dua_052538_nwi388.h_visit_helper
select
  bene_id,
  ADMSN_DT,
  case 
  when DSCHRG_DT ='' then DSCHRG_DT
  else ADMSN_DT end,
  32846,  --=Facility claim header
  state_cd,
  clm_id,
  null,
  null,
  null,
  BLG_PRVDR_NPI,
  null,
  concat(clm_id, state_cd,32846) as forign_key
from
  long_term_header
  where bene_id is not null;
--fragment3
insert into
  dua_052538_nwi388.h_visit_helper
select
  bene_id,
  srvc_bgn_dt,
  srvc_bgn_dt,
  32861,        --32861=outpatient claim header
  state_cd,
  clm_id,
  POS_CD,
  null,
  lkup_cmspos.concept_id as visit_source_concept_id,
  BLG_PRVDR_NPI,
  null,
  concat(clm_id, state_cd,32861) as forign_key
from
  other_services_header 
  left join lkup_cmspos
  on POS_CD=concept_code
   where bene_id is not null;

--there is no RX head here, which is why there is no rx visit_id- add a block here and it should work

-- COMMAND ----------

drop view if exists visit_id;
create view visit_id as
select
  bene_id,
  visit_start_date,
  visit_end_date,
  BLG_PRVDR_NPI,
  concat(clm_id, state_cd, visit_type_concept_id) as forign_key,
  ROW_NUMBER() OVER(
    PARTITION BY bene_id
    ORDER BY
      visit_end_date,
      visit_start_date,
      clm_id ASC
  ) as event_id,
  (day(visit_end_date) - day(visit_start_date) + 1) as daydiff,
  rank(day(visit_end_date) - day(visit_start_date)) OVER (
    PARTITION BY bene_id
    ORDER BY
      visit_end_date asc
  ) as same_day,
  sum((day(visit_end_date) - day(visit_start_date) + 1)) over(
    partition by bene_id
    order by
      visit_end_date rows between unbounded preceding
      and current row
  ) as running_total -- 131 days when there should be 90
  --ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
from
  dua_052538_nwi388.h_visit_helper
where
  bene_id is not null
  and visit_start_date is not null
order by
  bene_id,
  visit_start_date asc,
  visit_end_date desc,
  running_total desc;

-- COMMAND ----------

drop view if exists visit_id_for_visit_helper;
create view visit_id_for_visit_helper as
select
  distinct concat(
    bene_id,
    visit_start_date,
    visit_end_date,
    BLG_PRVDR_NPI
  ) as visit_event,
  forign_key,
  visit_start_date,
  visit_end_date,
  bene_id,
  event_id,
  BLG_PRVDR_NPI,
  same_day
from
  visit_id
where
  visit_start_date is not null
  and visit_end_date is not null
  and BLG_PRVDR_NPI is not null
group by
  visit_event,
  forign_key,
  visit_start_date,
  visit_end_date,
  bene_id,
  event_id,
  BLG_PRVDR_NPI,
  same_day;
--visit end date missing what to do-
  --visit start date missing what to do-
  --billing provider missing what to do-there are a lot of records like this...!!!
  --does this have non-null provider ids for the same person?
  drop view if exists visit_id_for_visit;
create view visit_id_for_visit as
select
  distinct visit_event,
  ROW_NUMBER() OVER (
    ORDER BY
      (
        SELECT
          1
      )
  ) as visit_occurrence_id,
  forign_key
from
  visit_id_for_visit_helper
group by
  visit_event,
  forign_key;

-- COMMAND ----------

--table
--visit_occurrence
--note currently visit ids that dont have a billing npi on a header claim return null id-
--note should this change?
insert into
  dua_052538_nwi388.visit_occurrence_x
select
  b.visit_occurrence_id,
  a.bene_id as person_id,
  null as visit_concept_id,
  a.visit_start_date,
  null as visit_start_datetime,
  a.visit_end_date,
  null as visit_end_datetime,
  a.visit_type_concept_id,
  c.proivder_id,
  null as care_site,
  a.visit_source_value,
  a.visit_source_concept_id,
  a.admiting_source_concept_id,
  a.admitting_source_value,
  null as discharge_to_concept_id,
  null as discharge_to_source_value,
  null as preceding_visit_occurrence_id
from
  dua_052538_nwi388.h_visit_helper a
  left join visit_id_for_visit b on a.forign_key = b.forign_key
  left join provider_id_for_visit_occurrence c on a.BLG_PRVDR_NPI = c.npi;

-- COMMAND ----------

--table
--procedure_occurrence
--note transform px wide to long transformation
drop table if exists dua_052538_nwi388.transform_px;
create table dua_052538_nwi388.transform_px (
  bene_id bigint,
  clm_id bigint,
  origin_table string,
  origin string,
  procedure_type_concept_id bigint,
  --does this fit? how big should the int be???
  event_source_value string,
  event_start_date date,
  complex_id string,
  npi string
);
drop view if exists visit_id_for_pxdxrx;
create view visit_id_for_pxdxrx as
select
  a.visit_event,
  a.visit_occurrence_id,
  b.forign_key
from
  visit_id_for_visit a
  left join visit_id_for_visit_helper b on a.visit_event = b.visit_event
order by
  visit_occurrence_id asc;

-- COMMAND ----------

insert into
  dua_052538_nwi388.transform_px
select
  bene_id,
  clm_id,
  "other_services_line" as origin_table,
  state_cd as origin,
  32861 as procedure_type_concept_id,
  LINE_PRCDR_CD as event_source_value,
  --LINE_PRCDR_MDFR_CD_1,
  --LINE_PRCDR_MDFR_CD_2,
  --LINE_PRCDR_MDFR_CD_3,
  --LINE_PRCDR_MDFR_CD_4,
  --TOOTH_DSGNTN_SYS,
  --TOOTH_NUM,
  --TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,
  --TOOTH_SRFC_CD,
  LINE_PRCDR_CD_DT as event_start_date,
  concat(clm_id, state_cd, 32861) as complex_id,
  srvc_prvdr_npi as npi
from
  other_services_line;
--IP header
insert into
  dua_052538_nwi388.transform_px
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  32855 as procedure_type_concept_id,
  prcdr_cd_1 as event_source_value,
  prcdr_cd_dt_1 as event_start_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  blg_prvdr_npi as npi
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_px
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  32855 as procedure_type_concept_id,
  prcdr_cd_2 as event_source_value,
  prcdr_cd_dt_2 as event_start_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  blg_prvdr_npi as npi
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_px
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  32855 as procedure_type_concept_id,
  prcdr_cd_3 as event_source_value,
  prcdr_cd_dt_3 as event_start_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  blg_prvdr_npi as npi
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_px
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  32855 as procedure_type_concept_id,
  prcdr_cd_4 as event_source_value,
  prcdr_cd_dt_4 as event_start_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  blg_prvdr_npi as npi
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_px
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  32855 as procedure_type_concept_id,
  prcdr_cd_5 as event_source_value,
  prcdr_cd_dt_5 as event_start_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  blg_prvdr_npi as npi
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_px
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  32855 as procedure_type_concept_id,
  prcdr_cd_6 as event_source_value,
  prcdr_cd_dt_6 as event_start_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  blg_prvdr_npi as npi
from
  inpatient_header;

-- COMMAND ----------

--note here you drop all records where athena does not find a match...
drop view if exists transform_px_visit;
create view transform_px_visit as
select
  *
from
  dua_052538_nwi388.transform_px a
  left join visit_id_for_pxdxrx b on a.complex_id = b.forign_key
  left join lkup_ICD10PCS c on a.event_source_value = c.concept_code
  left join provider_id_for_dxpxrx d on a.npi = d.npi_a
where
  concept_id is not null;

-- COMMAND ----------

insert into
  dua_052538_NWI388.procedure_occurrence_x
select
  ROW_NUMBER() OVER(
    ORDER BY
      bene_id,
      clm_id,
      origin,
      origin_table ASC --consider using date to assign procedures first to last-
  ) as procedure_occurrence_id,
  bene_id as person_id,
  concept_id as procedure_concept_id,  --no work has been done to keep this value standard
  event_start_date as procedure_date,
  null as procedure_datetime,
  null as procedure_type_concept_id,  --requires ccw table
  null as modifier_concept_id,  --not currently useing this
  null as quantity,
  provider_id as provider_id,
  visit_occurrence_id,
  visit_occurrence_id as visit_detail_id,
  event_source_value as procedure_source_value,
  concept_id as procedure_source_concept_id,
  null as modifier_source_value --not currently useing this
from
  transform_px_visit;

-- COMMAND ----------

--table
--condition_occurrence
--note
drop table if exists dua_052538_nwi388.transform_dx;
create table dua_052538_nwi388.transform_dx (
  bene_id bigint,
  clm_id bigint,
  origin_table string,
  origin string,
  event_source_value string,
  event_start_date date,
  event_end_date date,
  complex_id string,
  npi string,
  condition_type_concept_id bigint
);

-- COMMAND ----------

--OT header
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  a.clm_id,
  "other_services_header" as origin_table,
  state_cd as origin,
  DGNS_CD_1 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32861) as complex_id,
  b.SRVC_PRVDR_NPI as npi,
  32861 as condition_type_concept_id
from
  other_services_header a
  left join other_services_line_p b on a.clm_id = b.clm_id_b;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  a.clm_id,
  "other_services_header" as origin_table,
  state_cd as origin,
  DGNS_CD_2 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32861) as complex_id,
  b.SRVC_PRVDR_NPI as npi,
  32861 as condition_type_concept_id
from
  other_services_header a
  left join other_services_line_p b on a.clm_id = b.clm_id_b;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  ADMTG_DGNS_CD as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_1 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_2 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_3 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_4 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_5 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_6 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_7 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_8 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_9 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_10 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_11 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_12 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  drg_cd as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
--LT Header
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  DGNS_CD_1 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id
from
  long_term_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  DGNS_CD_2 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id
from
  long_term_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  DGNS_CD_3 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id
from
  long_term_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  DGNS_CD_4 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id
from
  long_term_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  DGNS_CD_5 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id
from
  long_term_header;
insert into
  dua_052538_nwi388.transform_dx
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  ADMTG_DGNS_CD as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id
from
  long_term_header;

-- COMMAND ----------

--evaluate where
drop view if exists dx_routes;
create view dx_routes as
select
  d.provider_id,
  c.visit_occurrence_id,
  clm_id,
  origin_table,
  origin,
  event_source_value,
  event_start_date,
  event_end_date,
  complex_id,
  concept_name,
  concept_id,
  domain_id,
  vocabulary_id,
  bene_id,
  condition_type_concept_id
from
  dua_052538_nwi388.transform_dx a
  left join lkup_ICD10CM b on a.event_source_value = b.concept_code
  left join visit_id_for_pxdxrx c on a.complex_id = c.forign_key
  left join provider_id_for_dxpxrx d on a.npi = d.npi_a
where
  event_source_value is not null;

-- COMMAND ----------

--note currently we do not deduplicate px-dx-rx
--note the visit ids are deduplicated by day and provider but the codes themselves are not...

insert into dua_052538_nwi388.condition_occurrence_x
select
ROW_NUMBER() OVER(
    ORDER BY
      bene_id,
      clm_id,
      origin,
      origin_table ASC --consider using date to assign procedures first to last-
  ) as condition_occurrence_id,
bene_id as person_id,
concept_id as condition_concept_id,
event_start_date as condition_start_date,
null as condition_start_datetime,
event_end_date as condition_end_date,
null as condition_end_datetime,
condition_type_concept_id,--file type
null  condition_status_concept_id, --this could be solved with admt dx codes
null as  stop_reason, 
provider_id, 
visit_occurrence_id,
null as visit_detail_id,
event_source_value as   condition_source_value,
concept_id as condition_source_concept_id,
null condition_status_source_value
from dx_routes;



-- COMMAND ----------

--table
--drug_exposure
--note this is ndc level, not clinical drug level
drop table if exists dua_052538_nwi388.transform_rx;
create table dua_052538_nwi388.transform_rx (
  clm_id bigint,
  origin_table string,
  origin string,
  event_source_value string,
  dose_unit_source_value string,
  quantity integer,
  refills string,
  days_supply integer,
  route_source_value string,
  event_start_date date,
  event_end_date date,
  complex_id string,
  bene_id bigint,
  npi string
);

-- COMMAND ----------

--ot_line
insert into
  dua_052538_nwi388.transform_rx
select
  clm_id,
  "other_services_line" as origin_table,
  state_cd as origin,
  ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  '0' as refills,
  0 as days_supply,
  'NULL' as route_source_value,
  LINE_SRVC_BGN_DT as event_start_date,
  LINE_SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32861) as complex_id,
  bene_id,
  srvc_prvdr_npi
from
  other_services_line;
--ip line
insert into
  dua_052538_nwi388.transform_rx
select
  clm_id,
  "inpatient_line" as origin_table,
  state_cd as origin,
  ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  '0' as refills,
  0 as days_supply,
  'NULL' as route_source_value,
  LINE_SRVC_BGN_DT as event_start_date,
  LINE_SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  bene_id,
  srvc_prvdr_npi
from
  inpatient_line;
--rx line
insert into
  dua_052538_nwi388.transform_rx
select
  a.clm_id,
  "rx_line" as origin_table,
  state_cd as origin,
  ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  NEW_RX_REFILL_NUM as refills,
  DAYS_SUPPLY as days_supply,
  DOSAGE_FORM_CD as route_source_value,
  RX_FILL_DT as event_start_date,
  date('01/01/0001') as event_end_date,
  concat(clm_id, state_cd, 581458) as complex_id,
  bene_id,
  b.PRSCRBNG_PRVDR_NPI
from
  rx_line a
  left join rx_header_p b on a.clm_id = b.clm_id_b;
--lt line
insert into
  dua_052538_nwi388.transform_rx
select
  clm_id,
  "long_term_line" as origin_table,
  state_cd as origin,
  ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  0 as refills,
  0 as days_supply,
  'NULL' as route_source_value,
  LINE_SRVC_BGN_DT as event_start_date,
  LINE_SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  bene_id,
  SRVC_PRVDR_NPI
from
  long_term_line;

-- COMMAND ----------

drop view if exists medication_route;
create view medication_route as
select
  *
from
  dua_052538_nwi388.transform_rx
  left join lkup_ndc 
  on event_source_value = concept_code
  left join visit_id_for_pxdxrx  on complex_id = forign_key
  left join provider_id_for_dxpxrx  on npi = npi_a
  where event_source_value is not null;

-- COMMAND ----------

insert into
  dua_052538_nwi388.DRUG_EXPOSURE_x
select
  ROW_NUMBER() OVER(
    ORDER BY
      bene_id,
      clm_id,
      origin,
      origin_table ASC --consider using date to assign procedures first to last-
  ) as drug_exposure_id,
  bene_id as person_id,
  concept_id as drug_concept_id,
  event_start_date as drug_exposure_start_date,
  null as drug_exposure_start_datetime,
  event_end_date as drug_exposure_end_date,
  null as drug_exposure_end_datetime,
  null as verbatim_end_date,
  32810 as drug_type_concept_id,
  null as stop_reason,
  refills,
  quantity,
  days_supply,
  null as sig,
  null as route_concept_id,
  --requires dictionary
  null as lot_number,
  provider_id,
  visit_occurrence_id,
  null as visit_detail_id,
  concept_code as drug_source_value,
  concept_id as drug_source_concept_id,
  route_source_value,
  dose_unit_source_value
from
  medication_route;
