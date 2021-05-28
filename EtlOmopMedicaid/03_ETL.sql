-- Databricks notebook source
--person
--Command took 59.78
-- a multi year file would need to have dead and extra cases removed---
insert into dua_052538_NWI388.PERSON select
BENE_ID as person_id,
"NULL" as gender_concept_id,
year(BIRTH_DT) as year_of_birth,
month(BIRTH_DT) as month_of_birth,
day(BIRTH_DT) as day_of_birth,
"NULL" as birth_datetime, 
"NULL" as race_concept_id, 
"NULL" as ethnicity_concept_id,
"NULL" as location_id,
"NULL" as provider_id,
"NULL" as care_site_id,
bene_id as person_source_value,
SEX_CD as gender_source_value ,
"NULL" as gender_source_concept_id,
RACE_ETHNCTY_CD as race_source_value,
"NULL" as race_source_concept_id,
ETHNCTY_CD as ethnicity_source_value,
"NULL" as ethnicity_source_concept_id,
STATE_CD
from demog_elig_base
;

-- COMMAND ----------

--observation occourrence
--Command took 1.07 minutes
insert into dua_052538_nwi388.OBSERVATION_PERIOD select
ROW_NUMBER() OVER(ORDER BY bene_id ASC) AS observation_period_id,
bene_id as person_id,
enrlmt_start_dt as observation_period_start_date,
enrlmt_end_dt as  observation_period_end_date,
"NULL" as period_type_concept_id,
state_cd
from
demog_elig;



-- COMMAND ----------

--death
insert into dua_052538_NWI388.DEATH select
bene_id as person_id, 
DEATH_DT as death_date,
"NULL" as death_datetime, 
"NULL" as death_type_concept_id , 
"NULL" as cause_concept_id, 
"NULL" as cause_source_value, 
"NULL" as cause_source_concept_id
from dua_052538.tafr18_demog_elig_base
where  DEATH_DT is not null;

-- COMMAND ----------

--visit occurrence
--
insert into dua_052538_nwi388.VISIT_OCCURRENCE select
ROW_NUMBER() OVER(ORDER BY clm_id,state_cd,file ASC) AS visit_occourrence_id
,person_id 
,visit_concept_id 
,visit_start_date 
,visit_start_datetime 
,visit_end_date 
,visit_end_datetime 
,visit_type_concept_id 
,provider_id 
,care_site_id
,visit_source_value
,visit_source_concept_id 
,admitting_source_concept_id 
,admitting_source_value 
,discharge_to_concept_id 
,discharge_to_source_value
,preceding_visit_occurrence_id
,factype1
,factype2
,clm_id
,state_cd 
,file
from( select 
bene_id as person_id,
"NULL" as visit_concept_id,
ADMSN_DT as visit_start_date,
ADMSN_HR as visit_start_datetime,
DSCHRG_DT as visit_end_date,
DSCHRG_HR as visit_end_datetime, 
"NULL" as visit_type_concept_id, 
"NULL" as provider_id ,
"NULL" as care_site_id,
"NULL" as visit_source_value,
"NULL" as visit_source_concept_id, 
"NULL" as admitting_source_concept_id, 
"NULL" as admitting_source_value ,
"NULL" as discharge_to_concept_id ,
"NULL" as discharge_to_source_value,
"NULL" as preceding_visit_occurrence_id,
ADMSN_TYPE_CD as factype1,
HOSP_TYPE_CD as factype2, 
file,
clm_id, 
state_cd
from inpatient_header union select
bene_id as person_id,
"NULL" as visit_concept_id,
 LINE_SRVC_BGN_DT as visit_start_date,
"NULL"as visit_start_datetime,
LINE_SRVC_END_DT as visit_end_date,
"NULL" as visit_end_datetime, 
"NULL" as visit_type_concept_id, 
"NULL" as provider_id ,
"NULL" as care_site_id,
"NULL" as visit_source_value,
"NULL" as visit_source_concept_id, 
"NULL" as admitting_source_concept_id, 
"NULL" as admitting_source_value ,
"NULL" as discharge_to_concept_id ,
"NULL" as discharge_to_source_value,
"NULL" as preceding_visit_occurrence_id,
"NULL" as factype1,
PRVDR_FAC_TYPE_CD as factype2, 
file,
clm_id, 
state_cd
from inpatient_line union select
bene_id as person_id,
"NULL" as visit_concept_id,
ADMSN_DT as visit_start_date,
ADMSN_HR as visit_start_datetime,
DSCHRG_DT as visit_end_date,
DSCHRG_HR as visit_end_datetime, 
"NULL" as visit_type_concept_id, 
"NULL" as provider_id ,
"NULL" as care_site_id,
"NULL" as visit_source_value,
"NULL" as visit_source_concept_id, 
"NULL" as admitting_source_concept_id, 
"NULL" as admitting_source_value ,
"NULL" as discharge_to_concept_id ,
"NULL" as discharge_to_source_value,
"NULL" as preceding_visit_occurrence_id,
"NULL" as factype1,
"NULL" as factype2, 
file,
clm_id, 
state_cd
from long_term_header union select
bene_id as person_id,
"NULL" as visit_concept_id,
LINE_SRVC_BGN_DT as visit_start_date,
"NULL" as visit_start_datetime,
LINE_SRVC_END_DT as visit_end_date,
"NULL" as visit_end_datetime, 
"NULL" as visit_type_concept_id, 
"NULL" as provider_id ,
"NULL" as care_site_id,
"NULL" as visit_source_value,
"NULL" as visit_source_concept_id, 
"NULL" as admitting_source_concept_id, 
"NULL" as admitting_source_value ,
"NULL" as discharge_to_concept_id ,
"NULL" as discharge_to_source_value,
"NULL" as preceding_visit_occurrence_id,
"NULL" as factype1,
PRVDR_FAC_TYPE_CD as factype2, 
file,
clm_id, 
state_cd
from long_term_line union select
bene_id as person_id,
"NULL" as visit_concept_id,
srvc_bgn_dt as visit_start_date,
"NULL" as visit_start_datetime,
srvc_end_dt as visit_end_date,
"NULL" as visit_end_datetime, 
"NULL" as visit_type_concept_id, 
"NULL" as provider_id ,
"NULL" as care_site_id,
"NULL" as visit_source_value,
"NULL" as visit_source_concept_id, 
"NULL" as admitting_source_concept_id, 
"NULL" as admitting_source_value ,
"NULL" as discharge_to_concept_id ,
"NULL" as discharge_to_source_value,
"NULL" as preceding_visit_occurrence_id,
"NULL" as factype1,
POS_CD as factype2, 
file,
clm_id, 
state_cd  
from other_services_header union select
bene_id as person_id,
"NULL" as visit_concept_id,
LINE_SRVC_BGN_DT as visit_start_date,
"NULL" as visit_start_datetime,
LINE_SRVC_END_DT as visit_end_date,
"NULL" as visit_end_datetime, 
"NULL" as visit_type_concept_id, 
"NULL" as provider_id ,
"NULL" as care_site_id,
"NULL" as visit_source_value,
"NULL" as visit_source_concept_id, 
"NULL" as admitting_source_concept_id, 
"NULL" as admitting_source_value ,
"NULL" as discharge_to_concept_id ,
"NULL" as discharge_to_source_value,
"NULL" as preceding_visit_occurrence_id,
"NULL" as factype1,
"NULL" as factype2, 
file,
clm_id, 
state_cd  
from other_services_line);


-- COMMAND ----------

--provider Command took 8.18 minutes
--null is ruining string to ints= maybe use an int friendly null?
--float is too long to be an int
--it sounds like it all needs to be strings or bust...
--maybe push provider id to provider_source_value? and then use row id instead?
-- make a sub table with npi and generate provider ids from them...
--NPI need to be joined to NPI-DB I am thinking alter table?
INSERT INTO dua_052538_nwi388.provider   select
-- inpt head start
RFRG_PRVDR_ID as provider_id, 
"NULL" as provider_name,
RFRG_PRVDR_NPI as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL"as gender_concept_id,
"NULL" as provider_source_value,
RFRG_PRVDR_SPCLTY_CD as specialty_source_value,
"NULL" as specialty_source_concept_id,
"NULL"as gender_source_value, 
"NULL" as gender_source_concept_id,
"RFRG" as class,
clm_id,
state_cd,
file
from inpatient_header
where RFRG_PRVDR_NPI is not null
union select
ADMTG_PRVDR_ID as provider_id, 
"NULL" as provider_name,
ADMTG_PRVDR_NPI as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL"as gender_concept_id,
"NULL" as provider_source_value,
ADMTG_PRVDR_SPCLTY_CD as specialty_source_value,
"NULL"as specialty_source_concept_id, 
"NULL"as gender_source_value,
"NULL" as gender_source_concept_id,
"ADMTG" as class, 
clm_id,
state_cd,  
file 
from inpatient_header
where ADMTG_PRVDR_NPI is not null
union select
BLG_PRVDR_ID as provider_id, 
"NULL" as provider_name,
BLG_PRVDR_NPI as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL"as gender_concept_id,
"NULL" as provider_source_value,
BLG_PRVDR_SPCLTY_CD as specialty_source_value,
"NULL"as specialty_source_concept_id, 
"NULL"as gender_source_value,
"NULL" as gender_source_concept_id,
"BLG" as class, 
clm_id,
state_cd,
file 
from inpatient_header
where BLG_PRVDR_NPI is not null
union 
--inpt head end

--inpt line start
select
SRVC_PRVDR_ID as provider_id, 
"NULL" as provider_name,
SRVC_PRVDR_NPI as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL"as gender_concept_id,
"NULL" as provider_source_value,
SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
"NULL"as specialty_source_concept_id, 
"NULL"as gender_source_value,
"NULL" as gender_source_concept_id,
"SRVC" as class, 
clm_id,
state_cd, 
file 
from inpatient_line
where SRVC_PRVDR_NPI is not null
union select
"NULL" as provider_id, 
"NULL" as provider_name,
OPRTG_PRVDR_NPI as npi,
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL"as gender_concept_id,
"NULL" as provider_source_value,
"NULL" as specialty_source_value,
"NULL"as specialty_source_concept_id, 
"NULL"as gender_source_value,
"NULL" as gender_source_concept_id,
"OPRTG" as class, 
clm_id,
state_cd, 
file 
from inpatient_line
where OPRTG_PRVDR_NPI is not null
union 
--inpt line end
--ot head start 
select
BLG_PRVDR_ID as provider_id, 
"NULL" as provider_name,
BLG_PRVDR_NPI as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL"as gender_concept_id,
"NULL" as provider_source_value,
BLG_PRVDR_SPCLTY_CD as specialty_source_value,
"NULL"as specialty_source_concept_id, 
"NULL"as gender_source_value,
"NULL" as gender_source_concept_id,
"BLG" as class,
clm_id,
state_cd, 
file 
from other_services_header
where BLG_PRVDR_NPI is not null
union select
RFRG_PRVDR_ID as provider_id, 
"NULL" as provider_name,
RFRG_PRVDR_NPI as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL"as gender_concept_id,
"NULL" as provider_source_value,
RFRG_PRVDR_SPCLTY_CD as specialty_source_value,
"NULL"as specialty_source_concept_id, 
"NULL"as gender_source_value,
"NULL" as gender_source_concept_id,
"RFRG" as class, 
clm_id,
state_cd, 
file 
from other_services_header
where BLG_PRVDR_NPI is not null
union select
"NULL" as provider_id, 
"NULL" as provider_name,
drctng_prvdr_npi as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL"as gender_concept_id,
"NULL" as provider_source_value,
"NULL" as specialty_source_value,
"NULL"as specialty_source_concept_id, 
"NULL"as gender_source_value,
"NULL" as gender_source_concept_id,
"DRCTNG" as class,
clm_id,
state_cd,
file 
from other_services_header
where drctng_prvdr_npi is not null
union select
"NULL" as provider_id, 
"NULL" as provider_name,
sprvsng_prvdr_npi as npi,
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL" as gender_concept_id,
"NULL" as provider_source_value,
"NULL" as specialty_source_value,
"NULL" as specialty_source_concept_id, 
"NULL" as gender_source_value,
"NULL" as gender_source_concept_id,
"SPRVSNG" as class,
clm_id,
state_cd,
file 
from other_services_header
where sprvsng_prvdr_npi  is not null
union select
"NULL" as provider_id, 
"NULL" as provider_name,
hlth_home_prvdr_npi as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL" as gender_concept_id,
"NULL" as provider_source_value,
"NULL" as specialty_source_value,
"NULL" as specialty_source_concept_id, 
"NULL" as gender_source_value,
"NULL" as gender_source_concept_id,
"HLTH_HOME" as class, 
clm_id,
state_cd,
file 
from other_services_header
where hlth_home_prvdr_npi is not null
union
--ot head end
--ot line start
select
SRVC_PRVDR_ID as provider_id, 
"NULL" as provider_name,
SRVC_PRVDR_NPI as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL" as gender_concept_id,
"NULL" as provider_source_value,
SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
"NULL" as specialty_source_concept_id, 
"NULL" as gender_source_value,
"NULL" as gender_source_concept_id,
"SRVC" as class, 
clm_id,
state_cd,
file 
from other_services_line
where SRVC_PRVDR_NPI is not null
union 
--ot line end
--lt head start
select
RFRG_PRVDR_ID as provider_id, 
"NULL" as provider_name,
RFRG_PRVDR_NPI as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL" as gender_concept_id,
"NULL" as provider_source_value,
RFRG_PRVDR_SPCLTY_CD as specialty_source_value,
"NULL" as specialty_source_concept_id, 
"NULL" as gender_source_value,
"NULL" as gender_source_concept_id,
"RFRG" as class, 
clm_id,
state_cd,  
file
from long_term_header
where RFRG_PRVDR_NPI is not null
union select
ADMTG_PRVDR_ID as provider_id, 
"NULL" as provider_name,
ADMTG_PRVDR_NPI as npi,
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL" as gender_concept_id,
"NULL" as provider_source_value,
ADMTG_PRVDR_SPCLTY_CD as specialty_source_value,
"NULL" as specialty_source_concept_id, 
"NULL" as gender_source_value,
"NULL" as gender_source_concept_id,
"ADMTG" as class, 
clm_id,
state_cd, 
file 
from long_term_header
where ADMTG_PRVDR_NPI is not null
union select
BLG_PRVDR_ID as provider_id, 
"NULL" as provider_name,
BLG_PRVDR_NPI as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL" as gender_concept_id,
"NULL" as provider_source_value,
BLG_PRVDR_SPCLTY_CD as specialty_source_value,
"NULL" as specialty_source_concept_id, 
"NULL" as gender_source_value,
"NULL" as gender_source_concept_id,
"BLG" as class, 
clm_id,
state_cd,
file 
from long_term_header
where BLG_PRVDR_NPI is not null
union
--lt head end
--lt line start
select
SRVC_PRVDR_ID as provider_id, 
"NULL" as provider_name,
SRVC_PRVDR_NPI as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL" as gender_concept_id,
"NULL" as provider_source_value,
SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
"NULL" as specialty_source_concept_id, 
"NULL" as gender_source_value,
"NULL" as gender_source_concept_id,
"SRVC" as class,
clm_id,
state_cd,
file 
from long_term_line
where SRVC_PRVDR_NPI is not null
union
--lt line end
--rx head start
select
BLG_PRVDR_ID as provider_id, 
"NULL" as provider_name,
BLG_PRVDR_NPI as npi,
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL" as gender_concept_id,
"NULL" as provider_source_value,
BLG_PRVDR_SPCLTY_CD as specialty_source_value,
"NULL" as specialty_source_concept_id, 
"NULL" as gender_source_value,
"NULL" as gender_source_concept_id,
"BLG" as class, 
clm_id,
state_cd,
file 
from rx_header 
where BLG_PRVDR_NPI is not null
union select
PRSCRBNG_PRVDR_ID as provider_id,
"NULL" as provider_name,
PRSCRBNG_PRVDR_NPI as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL" as gender_concept_id,
"NULL" as provider_source_value,
"NULL" as specialty_source_value,
"NULL" as specialty_source_concept_id, 
"NULL" as gender_source_value,
"NULL" as gender_source_concept_id,
"PRSCRBING" as class, 
clm_id,
state_cd,
file 
from rx_header
where PRSCRBNG_PRVDR_NPI is not null
union select
DSPNSNG_PRVDR_ID as provider_id, 
"NULL" as provider_name,
DSPNSNG_PRVDR_NPI as npi, 
"NULL" as dea,
"NULL" as specialty_concept_id,
"NULL" as care_site_id,
"NULL" as year_of_birth,
"NULL" as gender_concept_id,
"NULL" as provider_source_value,
"NULL" as specialty_source_value,
"NULL" as specialty_source_concept_id, 
"NULL" as gender_source_value,
"NULL" as gender_source_concept_id,
"DSPNSNG" as class,
clm_id,
state_cd,
file 
from rx_header
where DSPNSNG_PRVDR_NPI is not null
;
--rx head end

-- COMMAND ----------

--procedure occourrence Command took 3.78 minutes 
--weird date to string exceptions- also if row number is string thats ok-but must be same in s table
insert into dua_052538_nwi388.PROCEDURE_OCCURRENCE select 
ROW_NUMBER() OVER(ORDER BY clm_id,state_cd,file ASC) AS procedure_occurence_id, 
person_id integer, 
procedure_concept_id, 
procedure_date, 
procedure_datetime, 
procedure_type_concept_id, 
modifier_concept_id, 
quantity, 
provider_id, 
visit_occurrence_id, 
visit_detail_id, 
procedure_source_value, 
procedure_source_concept_id, 
modifier_source_value,
modifier_source_value_1,
modifier_source_value_2,
modifier_source_value_3,
modifier_source_value_4,
TOOTH_DSGNTN_SYS,
TOOTH_NUM,
TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,
TOOTH_SRFC_CD,
clm_id,
state_cd,
file
from( select
--start inpatient header
bene_id as person_id, 
"NULL" as procedure_concept_id,
PRCDR_CD_DT_1 as procedure_date,
"NULL" as procedure_datetime, 
"NULL" as procedure_type_concept_id, 
"NULL" as modifier_concept_id, 
"NULL" as quantity, 
"NULL" as provider_id, 
"NULL" as visit_occurrence_id, 
"NULL" as visit_detail_id, 
PRCDR_CD_1 as procedure_source_value,
"NULL" as procedure_source_concept_id, 
"NULL" as modifier_source_value,
"NULL" as modifier_source_value_1,
"NULL" as modifier_source_value_2,
"NULL" as modifier_source_value_3,
"NULL" as modifier_source_value_4,
"NULL" as TOOTH_DSGNTN_SYS,
"NULL" as TOOTH_NUM,
"NULL" as TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,
"NULL" as TOOTH_SRFC_CD,
clm_id,
state_cd, 
file
from inpatient_header union select 
bene_id as person_id, 
"NULL" as procedure_concept_id,
PRCDR_CD_DT_2 as procedure_date,
"NULL" as procedure_datetime, 
"NULL" as procedure_type_concept_id, 
"NULL" as modifier_concept_id, 
"NULL" as quantity, 
"NULL" as provider_id, 
"NULL" as visit_occurrence_id, 
"NULL" as visit_detail_id, 
PRCDR_CD_2 as procedure_source_value,
"NULL" as procedure_source_concept_id, 
"NULL" as modifier_source_value,
"NULL" as modifier_source_value_1,
"NULL" as modifier_source_value_2,
"NULL" as modifier_source_value_3,
"NULL" as modifier_source_value_4,
"NULL" as TOOTH_DSGNTN_SYS,
"NULL" as TOOTH_NUM,
"NULL" as TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,
"NULL" as TOOTH_SRFC_CD,
clm_id,
state_cd, 
file
from inpatient_header union select 
bene_id as person_id, 
"NULL" as procedure_concept_id,
PRCDR_CD_DT_3 as procedure_date,
"NULL" as procedure_datetime, 
"NULL" as procedure_type_concept_id, 
"NULL" as modifier_concept_id, 
"NULL" as quantity, 
"NULL" as provider_id, 
"NULL" as visit_occurrence_id, 
"NULL" as visit_detail_id, 
PRCDR_CD_3 as procedure_source_value,
"NULL" as procedure_source_concept_id, 
"NULL" as modifier_source_value,
"NULL" as modifier_source_value_1,
"NULL" as modifier_source_value_2,
"NULL" as modifier_source_value_3,
"NULL" as modifier_source_value_4,
"NULL" as TOOTH_DSGNTN_SYS,
"NULL" as TOOTH_NUM,
"NULL" as TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,
"NULL" as TOOTH_SRFC_CD,
clm_id,
state_cd, 
file
from inpatient_header union select 
bene_id as person_id, 
"NULL" as procedure_concept_id,
PRCDR_CD_DT_4 as procedure_date,
"NULL" as procedure_datetime, 
"NULL" as procedure_type_concept_id, 
"NULL" as modifier_concept_id, 
"NULL" as quantity, 
"NULL" as provider_id, 
"NULL" as visit_occurrence_id, 
"NULL" as visit_detail_id, 
PRCDR_CD_4 as procedure_source_value,
"NULL" as procedure_source_concept_id, 
"NULL" as modifier_source_value,
"NULL" as modifier_source_value_1,
"NULL" as modifier_source_value_2,
"NULL" as modifier_source_value_3,
"NULL" as modifier_source_value_4,
"NULL" as TOOTH_DSGNTN_SYS,
"NULL" as TOOTH_NUM,
"NULL" as TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,
"NULL" as TOOTH_SRFC_CD,
clm_id,
state_cd, 
file
from inpatient_header union select 
bene_id as person_id, 
"NULL" as procedure_concept_id,
PRCDR_CD_DT_5 as procedure_date,
"NULL" as procedure_datetime, 
"NULL" as procedure_type_concept_id, 
"NULL" as modifier_concept_id, 
"NULL" as quantity, 
"NULL" as provider_id, 
"NULL" as visit_occurrence_id, 
"NULL" as visit_detail_id, 
prcdr_cd_5 as procedure_source_value,
"NULL" as procedure_source_concept_id, 
"NULL" as modifier_source_value,
"NULL" as modifier_source_value_1,
"NULL" as modifier_source_value_2,
"NULL" as modifier_source_value_3,
"NULL" as modifier_source_value_4,
"NULL" as TOOTH_DSGNTN_SYS,
"NULL" as TOOTH_NUM,
"NULL" as TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,
"NULL" as TOOTH_SRFC_CD,
clm_id,
state_cd, 
file
from inpatient_header union 
select 
bene_id as person_id, 
"NULL" as procedure_concept_id,
'prcdr_cd_dt_6' as procedure_date,
"NULL" as procedure_datetime, 
"NULL" as procedure_type_concept_id, 
"NULL" as modifier_concept_id, 
"NULL" as quantity, 
"NULL" as provider_id, 
"NULL" as visit_occurrence_id, 
"NULL" as visit_detail_id, 
prcdr_cd_6 as procedure_source_value,
"NULL" as procedure_source_concept_id, 
"NULL" as modifier_source_value,
"NULL" as modifier_source_value_1,
"NULL" as modifier_source_value_2,
"NULL" as modifier_source_value_3,
"NULL" as modifier_source_value_4,
"NULL" as TOOTH_DSGNTN_SYS,
"NULL" as TOOTH_NUM,
"NULL" as TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,
"NULL" as TOOTH_SRFC_CD,
clm_id,
state_cd, 
file
from inpatient_header union select 
--inpatient header ends
--other services line starts
bene_id as person_id, 
"NULL" as procedure_concept_id,
LINE_PRCDR_CD_DT as procedure_date,
"NULL" as procedure_datetime, 
"NULL" as procedure_type_concept_id, 
"NULL" as modifier_concept_id, 
"NULL" as quantity, 
"NULL" as provider_id, 
"NULL" as visit_occurrence_id, 
"NULL" as visit_detail_id, 
LINE_PRCDR_CD as procedure_source_value,
"NULL" as procedure_source_concept_id, 
"NULL" as modifier_source_value,
LINE_PRCDR_MDFR_CD_1 as modifier_source_value_1,
LINE_PRCDR_MDFR_CD_2 as modifier_source_value_2,
LINE_PRCDR_MDFR_CD_3 as modifier_source_value_3,
LINE_PRCDR_MDFR_CD_4 as modifier_source_value_4,
TOOTH_DSGNTN_SYS,
TOOTH_NUM,
TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,
TOOTH_SRFC_CD,
clm_id,
state_cd, 
file
from other_services_line
--other services line ends
)where procedure_source_value is not null;


-- COMMAND ----------

--condition occurrence
insert into dua_052538_nwi388.CONDITION_OCCURRENCE
select ROW_NUMBER() OVER(ORDER BY clm_id,state_cd,file ASC) AS condition_occurrence_id,
person_id,condition_concept_id,condition_start_date,condition_start_datetime,
condition_end_date,condition_end_datetime,condition_type_concept_id,condition_status_concept_id,
stop_reason,provider_id,visit_occurrence_id,visit_detail_id,condition_source_value,condition_source_concept_id,
condition_status_source_value,clm_id,state_cd,file
from(select 
--inpatient header begins
bene_id as person_id, "NULL" as condition_concept_id, SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime  
,SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id,
"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,DGNS_CD_1 as condition_source_value,"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from inpatient_header
union
select 
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime,
SRVC_END_DT as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id,
"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,DGNS_CD_3 as condition_source_value,
"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from inpatient_header 
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime  
,SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id  
,"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,DGNS_CD_4 as condition_source_value 
,"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from inpatient_header 
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime  
,SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id  
,"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,DGNS_CD_5 as condition_source_value 
,"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from inpatient_header 
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime  
,SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id  
,"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,DGNS_CD_6 as condition_source_value 
,"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from inpatient_header 
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime  
,SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id  
,"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,DGNS_CD_7 as condition_source_value 
,"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from inpatient_header 
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime  
,SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id  
,"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,DGNS_CD_8 as condition_source_value 
,"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from inpatient_header 
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime  
,SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id  
,"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,DGNS_CD_9 as condition_source_value 
,"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from inpatient_header 
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime  
,SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id  
,"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,DGNS_CD_10 as condition_source_value 
,"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from inpatient_header 
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime  
,SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id  
,"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,DGNS_CD_11 as condition_source_value 
,"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from inpatient_header 
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime  
,SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id  
,"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,DGNS_CD_12 as condition_source_value 
,"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from inpatient_header 
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime  
,SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id  
,"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,DRG_CD as condition_source_value 
,"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from inpatient_header 
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime  
,SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id  
,"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,ADMTG_DGNS_CD as condition_source_value 
,"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from inpatient_header 
--inpatient header ends
--other services header begins
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime,
SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id,
"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,dgns_cd_1 as condition_source_value,
"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from other_services_header  
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime,
SRVC_END_DT as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id,
"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,dgns_cd_2 as condition_source_value,
"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from other_services_header 
--other services header ends
-- long_term_header begins
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime,
SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id,
"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,dgns_cd_1 as condition_source_value,
"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from long_term_header
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime,
SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id,
"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,dgns_cd_2 as condition_source_value,
"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from long_term_header
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime,
SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id,
"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,dgns_cd_3 as condition_source_value,
"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from long_term_header
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime,
SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id,
"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,dgns_cd_4 as condition_source_value,
"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from long_term_header
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime,
SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id,
"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,dgns_cd_5 as condition_source_value,
"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from long_term_header
union
select
bene_id as person_id,"NULL" as condition_concept_id,SRVC_BGN_DT as condition_start_date,"NULL" as condition_start_datetime,
SRVC_END_DT  as condition_end_date,"NULL" as condition_end_datetime,"NULL" as condition_type_concept_id,"NULL" as condition_status_concept_id,
"NULL" as stop_reason,"NULL" as provider_id,"NULL" as visit_occurrence_id,"NULL" as visit_detail_id,ADMTG_DGNS_CD as condition_source_value,
"NULL" as condition_source_concept_id,"NULL" as condition_status_source_value,clm_id,state_cd,file
from long_term_header)where condition_source_value is not null;
--long_term_header ends



-- COMMAND ----------

--drug exposure Command took 1.46 minutes
--
insert into dua_052538_nwi388.DRUG_EXPOSURE select
ROW_NUMBER() OVER(ORDER BY clm_id,state_cd,file ASC) AS drug_exposure_id, 
person_id, 
drug_concept_id, 
drug_exposure_start_date, 
drug_exposure_start_datetime, 
drug_exposure_end_date, 
drug_exposure_end_datetime, 
verbatim_end_date, 
drug_type_concept_id, 
stop_reason, 
refills, 
quantity, 
days_supply, 
sig, 
route_concept_id, 
lot_number, 
provider_id, 
visit_occurrence_id, 
visit_detail_id, 
drug_source_value, 
drug_source_concept_id, 
route_source_value, 
dose_unit_source_value,
clm_id,
state_cd,
file 
from(select
bene_id as person_id, 
"NULL" as drug_concept_id, 
LINE_SRVC_BGN_DT as drug_exposure_start_date, 
"NULL" as drug_exposure_start_datetime, 
LINE_SRVC_END_DT as drug_exposure_end_date, 
"NULL" as drug_exposure_end_datetime, 
"NULL" as verbatim_end_date, 
"NULL" as drug_type_concept_id, 
"NULL" as stop_reason, 
"NULL" as refills, 
NDC_QTY as quantity, 
"NULL" as days_supply, 
"NULL" as sig, 
"NULL" as route_concept_id, 
"NULL" as lot_number, 
"NULL" as provider_id, 
"NULL" as visit_occurrence_id, 
"NULL" as visit_detail_id, 
NDC as drug_source_value, 
"NULL" as drug_source_concept_id, 
"NULL" as route_source_value, 
NDC_UOM_CD as dose_unit_source_value,
clm_id,
state_cd,
file 
from inpatient_line union select
bene_id as person_id, 
"NULL" as drug_concept_id, 
LINE_SRVC_BGN_DT as drug_exposure_start_date, 
"NULL" as drug_exposure_start_datetime, 
LINE_SRVC_END_DT as drug_exposure_end_date, 
"NULL" as drug_exposure_end_datetime, 
"NULL" as verbatim_end_date, 
"NULL" as drug_type_concept_id, 
"NULL" as stop_reason, 
"NULL" as refills, 
NDC_QTY as quantity, 
"NULL" as days_supply, 
"NULL" as sig, 
"NULL" as route_concept_id, 
"NULL" as lot_number, 
"NULL" as provider_id, 
"NULL" as visit_occurrence_id, 
"NULL" as visit_detail_id, 
NDC as drug_source_value, 
"NULL" as drug_source_concept_id, 
"NULL" as route_source_value, 
NDC_UOM_CD as dose_unit_source_value,
clm_id,
state_cd,
file 
from long_term_line union select 
bene_id as person_id, 
"NULL" as drug_concept_id, 
RX_FILL_DT as drug_exposure_start_date, 
"NULL" as drug_exposure_start_datetime, 
"NULL" as drug_exposure_end_date, 
"NULL" as drug_exposure_end_datetime, 
"NULL" as verbatim_end_date, 
"NULL" as drug_type_concept_id, 
"NULL" as stop_reason, 
NEW_RX_REFILL_NUM as refills, 
NDC_QTY as quantity, 
DAYS_SUPPLY as days_supply, 
"NULL" as sig, 
 "NULL" asroute_concept_id, 
"NULL" as lot_number, 
"NULL" as provider_id, 
"NULL" as visit_occurrence_id, 
"NULL" as visit_detail_id, 
NDC as drug_source_value, 
"NULL" as  drug_source_concept_id, 
DOSAGE_FORM_CD as route_source_value, 
NDC_UOM_CD as dose_unit_source_value,
clm_id,
state_cd,
file 
from rx_line) where drug_source_value is not null;
  

