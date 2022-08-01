-- Databricks notebook source
insert into <write_bucket>.log values('$job_id','Person','1','start person',current_timestamp() );



-- COMMAND ----------

drop view if exists person_distinct;
create view person_distinct as 
SELECT *
  FROM (SELECT bene_id, rfrnc_yr, age,bene_state_cd,birth_dt,death_dt,ethncty_cd,race_ethncty_cd,sex_cd,state_cd,bene_cnty_cd,bene_zip_cd,
               ROW_NUMBER() OVER (PARTITION BY bene_id ORDER BY rfrnc_yr DESC) rank
          FROM <write_bucket>.demog_elig_base) a
 WHERE a.rank = 1 ;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Person','2','make person distinct',current_timestamp() );

-- COMMAND ----------

drop view if exists route_person;
create view route_person as
select
  BENE_ID,
  case 
    when SEX_CD = 'F' then 8532--Female|Female
    when SEX_CD = 'M' then 8507--Male|Male
    else 0--|blank
  end as gender_concept_id,
  year(BIRTH_DT) as year_of_birth,
  month(BIRTH_DT) as month_of_birth,
  day(BIRTH_DT) as day_of_birth,
    case
    when RACE_ETHNCTY_CD = 1 then 8527--White|White
    when RACE_ETHNCTY_CD = 2 then 8516--Black|Black
    when RACE_ETHNCTY_CD = 3 then 8515--Asian|Asian
    when RACE_ETHNCTY_CD = 4 then 8657--AIAN|AIAN
    when RACE_ETHNCTY_CD = 5 then 8557--NAOPI|NAOPI
    when RACE_ETHNCTY_CD = 6 then 8522--|other
    when RACE_ETHNCTY_CD = 7 then 38003563--|Hispanic
    else 0--|Blank
  end as race_concept_id,
  case
    when ETHNCTY_CD = 0 then 38003564--|Not Hispanic
    when ETHNCTY_CD > 0 then 38003563--|Hispanic
    else 0 --|Blank
  end as ethnicity_concept_id,
  bene_id as person_source_value,
  SEX_CD as gender_source_value,
  RACE_ETHNCTY_CD as race_source_value,
  ETHNCTY_CD as ethnicity_source_value
 from
  person_distinct;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Person','3','route person',current_timestamp() );

-- COMMAND ----------

insert into <write_bucket>.person select 
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
 from route_person;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Person','4','route person to person cdm',current_timestamp() );

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Person','5','end person',current_timestamp() );