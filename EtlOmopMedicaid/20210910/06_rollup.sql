-- Databricks notebook source
--condition era
create widget text scope default "1s1m";
create widget text job_id default "000";
create widget text state default "'VA'";


-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','rollup','0','start_rollup',current_timestamp(),$state,'$scope',null);

-- COMMAND ----------

-- create base eras from the concepts found in condition_occurrence
drop table if exists dua_052538_nwi388.cteConditionTarget; 
create table dua_052538_nwi388.cteConditionTarget as
SELECT co.PERSON_ID,
	co.condition_concept_id,
	co.CONDITION_START_DATE,
	COALESCE(co.CONDITION_END_DATE, 
    DATE_ADD(CONDITION_START_DATE,1)) AS CONDITION_END_DATE
FROM dua_052538_nwi388.CONDITION_OCCURRENCE_$scope co;

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','rollup','2','create cteconditiontarget',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.cteCondEndDates; 
create table dua_052538_nwi388.cteCondEndDates as
SELECT PERSON_ID,
	CONDITION_CONCEPT_ID,
	DATE_ADD(EVENT_DATE,-30) AS END_DATE -- unpad the end date
FROM (
	SELECT E1.PERSON_ID
		,E1.CONDITION_CONCEPT_ID
		,E1.EVENT_DATE
		,COALESCE(E1.START_ORDINAL, MAX(E2.START_ORDINAL)) START_ORDINAL
		,E1.OVERALL_ORD
	FROM (
		SELECT PERSON_ID
			,CONDITION_CONCEPT_ID
			,EVENT_DATE
			,EVENT_TYPE
			,START_ORDINAL
			,ROW_NUMBER() OVER (
				PARTITION BY PERSON_ID
				,CONDITION_CONCEPT_ID ORDER BY EVENT_DATE
					,EVENT_TYPE
				) AS OVERALL_ORD -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
		FROM (
			-- select the start dates, assigning a row number to each
			SELECT PERSON_ID
				,CONDITION_CONCEPT_ID
				,CONDITION_START_DATE AS EVENT_DATE
				,- 1 AS EVENT_TYPE
				,ROW_NUMBER() OVER (
					PARTITION BY PERSON_ID
					,CONDITION_CONCEPT_ID ORDER BY CONDITION_START_DATE
					) AS START_ORDINAL
			FROM dua_052538_nwi388.cteConditionTarget

			UNION ALL

			-- pad the end dates by 30 to allow a grace period for overlapping ranges.
			SELECT PERSON_ID
				,CONDITION_CONCEPT_ID
				,DATE_ADD(CONDITION_END_DATE,30)
				,1 AS EVENT_TYPE
				,NULL
			FROM dua_052538_nwi388.cteConditionTarget
			) RAWDATA
		) E1
	INNER JOIN (
		SELECT PERSON_ID
			,CONDITION_CONCEPT_ID
			,CONDITION_START_DATE AS EVENT_DATE
			,ROW_NUMBER() OVER (
				PARTITION BY PERSON_ID
				,CONDITION_CONCEPT_ID ORDER BY CONDITION_START_DATE
				) AS START_ORDINAL
		FROM dua_052538_nwi388.cteConditionTarget
		) E2 ON E1.PERSON_ID = E2.PERSON_ID
		AND E1.CONDITION_CONCEPT_ID = E2.CONDITION_CONCEPT_ID
		AND E2.EVENT_DATE <= E1.EVENT_DATE
	GROUP BY E1.PERSON_ID
		,E1.CONDITION_CONCEPT_ID
		,E1.EVENT_DATE
		,E1.START_ORDINAL
		,E1.OVERALL_ORD
	) E
WHERE (2 * E.START_ORDINAL) - E.OVERALL_ORD = 0;

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','rollup','4','create cteCondEndDates',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

drop table if exists  dua_052538_nwi388.cteConditionEnds;
create table dua_052538_nwi388.cteConditionEnds as
SELECT c.PERSON_ID
	,c.CONDITION_CONCEPT_ID
	,c.CONDITION_START_DATE
	,MIN(e.END_DATE) AS ERA_END_DATE

FROM dua_052538_nwi388.cteConditionTarget c
INNER JOIN dua_052538_nwi388.cteCondEndDates e ON c.PERSON_ID = e.PERSON_ID
	AND c.CONDITION_CONCEPT_ID = e.CONDITION_CONCEPT_ID
	AND e.END_DATE >= c.CONDITION_START_DATE
GROUP BY c.PERSON_ID
	,c.CONDITION_CONCEPT_ID
	,c.CONDITION_START_DATE;

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','rollup','6','create cteConditionEnds',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------


insert into dua_052538_nwi388.condition_era_$scope 
SELECT row_number() OVER (
		ORDER BY person_id
		) AS condition_era_id
	,person_id
	,CONDITION_CONCEPT_ID
	,min(CONDITION_START_DATE) AS CONDITION_ERA_START_DATE
	,ERA_END_DATE AS CONDITION_ERA_END_DATE
	,COUNT(*) AS CONDITION_OCCURRENCE_COUNT
FROM dua_052538_nwi388.cteConditionEnds
GROUP BY person_id
	,CONDITION_CONCEPT_ID
	,ERA_END_DATE;

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','rollup','8','destination condition_era',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

--rx era

drop view if exists ndc;
create view ndc as
select
  *
from
  dua_052538_nwi388.concept
where
  vocabulary_id = 'NDC';
-- ndc to cr and anc
  drop view if exists ndc_cr;
create view ndc_cr as
select
  concept_id,
  concept_name,
  domain_id,
  vocabulary_id,
  concept_class_id,
  concept_id_1,
  concept_id_2,
  relationship_id
from
  ndc
  left join dua_052538_nwi388.concept_relationship on concept_id = concept_id_1;
  
drop view if exists ndc_cr_concept;
create view ndc_cr_concept as
select
  a.concept_id as ndc_concept_id,
  a.concept_name as ndc_concept_name,
  a.domain_id as ndc_domain_id,
  a.vocabulary_id as ndc_vocabulary_id,
  a.concept_class_id as ndc_concept_class,
  a.concept_id_1 as ndc_concept_id_1,
  a.concept_id_2 as ndc_concept_id_2,
  b.concept_id as hs_concept_id,
  b.concept_name as hs_concept_name,
  b.domain_id as hs_domain_id,
  b.vocabulary_id as hs_vocabulary_id,
  a.concept_class_id as ndc_concept_class_id,
  b.concept_class_id as hs_concept_class_id
from
  ndc_cr a
  left join dua_052538_nwi388.concept b on a.concept_id_2 = b.concept_id;
  
  
  --distinct pt id goal 347,547
drop view if exists ndc_to_cd;
create view ndc_to_cd as
select
ndc_concept_id,hs_concept_id as cd_concept_id
from ndc_cr_concept where 
hs_concept_class_id='Clinical Drug'          --286,242 distinct pt ids
or hs_concept_class_id='Quant Clinical Drug' --295,917 distinct pt ids
or hs_concept_class_id='Clinical Drug Form'  --295,928 distinct pt ids 85%
;


-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','rollup','10','destination views for drug_era',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

-- Normalize DRUG_EXPOSURE_END_DATE to either the existing drug exposure end date, or add days supply, or add 1 day to the start date
drop table if exists dua_052538_nwi388.cteDrugTarget;
create table dua_052538_nwi388.cteDrugTarget as 
SELECT 
d.drug_concept_id
,a.ndc_concept_id
,a.cd_concept_id
,c.CONCEPT_ID AS INGREDIENT_CONCEPT_ID
,d.DRUG_EXPOSURE_ID
,d.PERSON_ID
,c.CONCEPT_ID
,d.DRUG_TYPE_CONCEPT_ID
,DRUG_EXPOSURE_START_DATE
,COALESCE(DRUG_EXPOSURE_END_DATE, DATE_ADD(DRUG_EXPOSURE_START_DATE,DAYS_SUPPLY), DATE_ADD(DRUG_EXPOSURE_START_DATE,1)) AS DRUG_EXPOSURE_END_DATE

FROM dua_052538_nwi388.DRUG_EXPOSURE_$scope d

left join ndc_to_cd  a
on d.drug_concept_id=a.ndc_concept_id

inner JOIN dua_052538_nwi388.CONCEPT_ANCESTOR ca 
ON ca.DESCENDANT_CONCEPT_ID = a.cd_concept_id

inner JOIN dua_052538_nwi388.CONCEPT c 
ON ca.ANCESTOR_CONCEPT_ID = c.CONCEPT_ID

WHERE c.VOCABULARY_ID = 'RxNorm'
AND c.CONCEPT_CLASS_ID = 'Ingredient'
;

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','rollup','12','create cteDrugTarget',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.cteEndDates;
create table dua_052538_nwi388.cteEndDates

SELECT PERSON_ID
	,INGREDIENT_CONCEPT_ID
	,DATE_ADD(EVENT_DATE,- 30) AS END_DATE -- unpad the end date

FROM (
	SELECT E1.PERSON_ID
		,E1.INGREDIENT_CONCEPT_ID
		,E1.EVENT_DATE
		,COALESCE(E1.START_ORDINAL, MAX(E2.START_ORDINAL)) START_ORDINAL
		,E1.OVERALL_ORD
	FROM (
		SELECT PERSON_ID
			,INGREDIENT_CONCEPT_ID
			,EVENT_DATE
			,EVENT_TYPE
			,START_ORDINAL
			,ROW_NUMBER() OVER (
				PARTITION BY PERSON_ID
				,INGREDIENT_CONCEPT_ID ORDER BY EVENT_DATE
					,EVENT_TYPE
				) AS OVERALL_ORD -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
		FROM (
			-- select the start dates, assigning a row number to each
			SELECT PERSON_ID
				,INGREDIENT_CONCEPT_ID
				,DRUG_EXPOSURE_START_DATE AS EVENT_DATE
				,0 AS EVENT_TYPE
				,ROW_NUMBER() OVER (
					PARTITION BY PERSON_ID
					,INGREDIENT_CONCEPT_ID ORDER BY DRUG_EXPOSURE_START_DATE
					) AS START_ORDINAL
			FROM dua_052538_nwi388.cteDrugTarget

			UNION ALL

			-- add the end dates with NULL as the row number, padding the end dates by 30 to allow a grace period for overlapping ranges.
			SELECT PERSON_ID
				,INGREDIENT_CONCEPT_ID
				,DATE_ADD(DRUG_EXPOSURE_END_DATE,30)
				,1 AS EVENT_TYPE
				,NULL
			FROM dua_052538_nwi388.cteDrugTarget
			) RAWDATA
		) E1
	INNER JOIN (
		SELECT PERSON_ID
			,INGREDIENT_CONCEPT_ID
			,DRUG_EXPOSURE_START_DATE AS EVENT_DATE
			,ROW_NUMBER() OVER (
				PARTITION BY PERSON_ID
				,INGREDIENT_CONCEPT_ID ORDER BY DRUG_EXPOSURE_START_DATE
				) AS START_ORDINAL
		FROM dua_052538_nwi388.cteDrugTarget
		) E2 ON E1.PERSON_ID = E2.PERSON_ID
		AND E1.INGREDIENT_CONCEPT_ID = E2.INGREDIENT_CONCEPT_ID
		AND E2.EVENT_DATE <= E1.EVENT_DATE
	GROUP BY E1.PERSON_ID
		,E1.INGREDIENT_CONCEPT_ID
		,E1.EVENT_DATE
		,E1.START_ORDINAL
		,E1.OVERALL_ORD
	) E
WHERE 2 * E.START_ORDINAL - E.OVERALL_ORD = 0;

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','rollup','14','create cteEndDates',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.cteDrugExpEnds; 
create table dua_052538_nwi388.cteDrugExpEnds as

SELECT d.PERSON_ID
	,d.INGREDIENT_CONCEPT_ID
	,d.DRUG_TYPE_CONCEPT_ID
	,d.DRUG_EXPOSURE_START_DATE
	,MIN(e.END_DATE) AS ERA_END_DATE
FROM dua_052538_nwi388.cteDrugTarget d
INNER JOIN dua_052538_nwi388.cteEndDates e ON d.PERSON_ID = e.PERSON_ID
	AND d.INGREDIENT_CONCEPT_ID = e.INGREDIENT_CONCEPT_ID
	AND e.END_DATE >= d.DRUG_EXPOSURE_START_DATE
GROUP BY d.PERSON_ID
	,d.INGREDIENT_CONCEPT_ID
	,d.DRUG_TYPE_CONCEPT_ID
	,d.DRUG_EXPOSURE_START_DATE;

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','rollup','16','create cteDrugExpEnds',current_timestamp(),$state,'$scope', null);

-- COMMAND ----------


insert into dua_052538_nwi388.drug_era_$scope
SELECT row_number() OVER (
		ORDER BY person_id
		) AS drug_era_id
	,person_id
	,INGREDIENT_CONCEPT_ID
	,min(DRUG_EXPOSURE_START_DATE) AS drug_era_start_date
	,ERA_END_DATE
	,COUNT(*) AS DRUG_EXPOSURE_COUNT
	,30 AS gap_days
FROM dua_052538_nwi388.cteDrugExpEnds
GROUP BY person_id
	,INGREDIENT_CONCEPT_ID
	,drug_type_concept_id
	,ERA_END_DATE;

-- COMMAND ----------

insert into dua_052538_nwi388.h_log values('$job_id','rollup','18','destination drug_era',current_timestamp(),$state,'$scope', null);
