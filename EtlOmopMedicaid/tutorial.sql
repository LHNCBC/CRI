-- Databricks notebook source
--The goal of this notebook is to walk users through how to use the output tables. The output tables are designed to iteratively reduce the set of tables to a subset. These reductions should shrink the amount of data drastically and improve operation times as the set subsets. Note there should be multiple subsets of desired tables. In the below example the CDM is reduced into three populations and their interactions plotted. This approach requires value sets; three value sets are provided. Value sets are easily made using Athena; though using ATHENA within VRDC limits your ability to edit and recode.

--Consider a traditional bio-stats study workflow with an exposure panel, a control panel, a value set (or case definition where a case could be exposure, outcome, or control) and an outcome or association of interest. Each panel is a list of patients. Patients who have the exposure and not the outcome are statistically compared to patients with the exposure and the outcome. The value sets describe the billing codes which define membership in a panel. For this effort a panel should be a discrete table. Below I walk you through a real world use case of setting up a study.

--Theory of The Case: Spontaneous Abortion (SA) (miscarriage) is a complex clinical event with many causes. Some drugs are suspected as causal in SA though not all pregnancies SA when a suspected drug is administered. This tutorial will make three populations and perform set operations between them to search for cases of Gout-Allopurinol-SA within individuals with a history of SA, and SA Sequela. Note that within the model Gout-Allopurinol-SA cases who are antecedent, or who did not have some of the triplet will also be investigated and can be studied from the three lists where complete membership across all lists is not always required for a research finding. 

--Population 1: Spontaneous Abortion (SA)
--The SA value set describes the ICD10CM codes for SA. SA is subdivided into the index code for the event itself, SA sequela codes and SA history codes. Consider how some one who has a prior history of SA may be more or less likely to have SA in the future. I can imagine a trial design where AL vs SA history compete for association with SA index event. Is it the prior history or the medication?

--Population 2: Allopurinol (AL)
--AL NDC codes are provided to roll up AL NDCs into Dosage. Disambiguation within the value set is an explicit study step. Here the value set is disambiguated on dosage though route of administration is equally valid. I can imagine a trial where kinds of ways of administering a drug compete for association with an outcome. If, for example powder to IV solutions are more implicated than simple IM injections then SA could be caused not by AL but sicker patients getting AL in IV settings. It could also be overdose, where IM is a pre dose drug and powder is easy to over provide in a ‘mix by hand’ administration route. 

--Population 3: Gout (GA)
--The Gout codes are described as Gout only; though area of the body and laterality is described. Disambiguation within the value set is an explicit study step that should be considered. Gout is usually why AL is administered and pregnant women get gout a lot. Laterality and anatomical region are available but here laterality is investigated. I can imagine a trial where right or left side gout is more common in the population; and right side gout is more common in AL_SA as most people are right handed and left side gout may not be as ‘cant-do-my-job, call the doctor’ worthy as right side gout. 

--Tables considered: 
  --Person
  --Condition Occurrence
  --Drug Exposure

--The end data model is: Three lists; with event class disambiguation (SA, SA_Sequela, SA_History, AL- Dosage and Gout with laterality), where each event is attributable to a subject and event date. Subject, date Event within list will support time to event models, as well as list membership models. This tutorial could support a publication which seeks to evaluate the risk of a drug being administered within x days of an SA where x is some metabolically plausible value (pregnancy can last up to 10 months though SA should last shorter). 

--To reach the end model you must first reduce Condition twice (once for SA, again for Gout). You must also reduce Drug Exposure to AL and then finally inner join the subset on person ID with person table. The resulting three tables, SA, GO and AL can be used to pursue exploratory data analysis. If study terms are satisfied (case capture) then move forward with outcomes (SA can be an outcome).  Ideally you would prefix your study event tables with something rational, like a study number, date or cohort ID as these two letter terms are vague and study specific.  

--The end model of cases who had GA_SA_AL is not the true end. You might consider
--Who had GA_AL and no SA (and no pregnancy; requires a pregnancy panel?)
--Who had GA_AL_SA within a time frame (10 months)?
--Who had AL_SA without GA?

--After you settle on an exposure panel (I do recommend time to event so that GA and AL come before SA within 9 prior months) think about other trial arms like first SA, second SA, third SA to outcome.


-- COMMAND ----------

--create value sets
drop table if exists <write_bucket>.al; 
create table <write_bucket>.al 
as
select concept_name, 
concept_code,--this is NDC
concept_id,
vocabulary_id,
case 
--if you know the dose form consider using it
--note there is a 20 mg dose form that might confound any-200-any
when concept_name like '%100%' then 'RX_100'
when concept_name like '%200%' then 'RX_200'
when concept_name like '%300%' then 'RX_300'
when concept_name like '%500%' then 'RX_500'
else 'RX_Other'
end as EVENT_TYPE,-- you can group by this clean term in ways you cant with concept name
'al' as event_class
from
<write_bucket>.concept
where
--consider case sensative versions!
concept_name like '%allopurinol%'  and vocabulary_id='NDC'or --no caps
concept_name like '%Allopurinol%' and vocabulary_id='NDC' or --one cap
concept_name like '%ALLOPURINOL%' and vocabulary_id='NDC';--all caps
select * from <write_bucket>.al ;

-- COMMAND ----------

drop table if exists <write_bucket>.GO; 
create table <write_bucket>.GO 
as
select concept_name, 
REPLACE(concept_code, '.', '') as concept_code,
concept_id,vocabulary_id,
case 
when concept_name like '%left%' then 'Left'
when concept_name like '%right%' then 'Right'
else 'Unspecified'
end as EVENT_TYPE,
'GA' as EVENT_CLASS
from
<write_bucket>.concept
where
concept_name like '%gout%'
and
vocabulary_id in('ICD10CM','ICD9CM');
select * from <write_bucket>.GO ;

-- COMMAND ----------


drop table if exists <write_bucket>.SA;
create table <write_bucket>.SA
as
select concept_name, 
 REPLACE(concept_code, '.', '') as concept_code,
concept_id,
vocabulary_id,
case 
when concept_name like '%Continuing pregnancy%' then 'History'
when concept_name like '%following%' then 'Sequela'
else 'Index'
end as EVENT_TYPE,
'SA' as EVENT_CLASS
from
<write_bucket>.concept
where
concept_name like'%Spontaneous abortion%' and vocabulary_id in('ICD10CM','ICD9CM') or
concept_name like '%spontaneous abortion%' and vocabulary_id in('ICD10CM','ICD9CM');
select * from <write_bucket>.SA ;

-- COMMAND ----------

--make a table with details on AL administration

drop table if exists <write_bucket>.AL_DE;
create table 
<write_bucket>.AL_DE
as 
select 
a.person_id,
a.drug_exposure_start_date as event_date,
b.concept_code,
b.concept_name,
b.vocabulary_id,
b.event_type,
b.event_class,
c.gender_concept_id,
c.year_of_birth,
c.race_concept_id,
c.ethnicity_concept_id
from 
<write_bucket>.drug_exposure a
inner join
<write_bucket>.al b
on
a.drug_source_value=b.concept_code
inner join
<write_bucket>.person c
on 
a.person_id=c.person_id;

select * from <write_bucket>.AL_DE;

-- COMMAND ----------

--make a table with details on SA events
drop table if exists <write_bucket>.SA_DE;
create table 
<write_bucket>.SA_DE
as 
select 
a.person_id,
a.condition_start_date as event_date,
b.concept_code,
b.concept_name,
b.vocabulary_id,
b.event_type,
b.event_class,
c.gender_concept_id,
c.year_of_birth,
c.race_concept_id,
c.ethnicity_concept_id
from 
<write_bucket>.condition_occurrence a
inner join
<write_bucket>.sa b
on
a.condition_source_value=b.concept_code
inner join
<write_bucket>.person c
on 
a.person_id=c.person_id;

select * from <write_bucket>.SA_DE;

-- COMMAND ----------

--make a table with details on GO events
drop table if exists <write_bucket>.GO_DE;
create table 
<write_bucket>.GO_DE
as 
select 
a.person_id,
a.condition_start_date as event_date,
b.concept_code,
b.concept_name,
b.vocabulary_id,
b.event_type,
b.event_class,
c.gender_concept_id,
c.year_of_birth,
c.race_concept_id,
c.ethnicity_concept_id
from 
<write_bucket>.condition_occurrence a
inner join
<write_bucket>.GO b
on
a.condition_source_value=b.concept_code
inner join
<write_bucket>.person c
on 
a.person_id=c.person_id;

select * from <write_bucket>.GO_DE limit 10;


-- COMMAND ----------

--how many cases had what

--Cases with RX by dosage
--note this is really claim count, not units of service
select 
count(distinct person_id) as pt_cnt,
count(concept_code) as ev_cnt,
event_type
from <write_bucket>.AL_DE
group by event_type;

-- COMMAND ----------

--cases with Gout by laterality
select 
count(distinct person_id) as pt_cnt,
count(concept_code) as ev_cnt,
event_type
from <write_bucket>.GO_DE
group by event_type;

-- COMMAND ----------

--who had SA by index, sequela or history code type
select 
count(distinct person_id) as pt_cnt,
count(concept_code) as ev_cnt,
event_type
from <write_bucket>.SA_DE
group by event_type;

-- COMMAND ----------

-- how many people are in both?

select count(distinct a.person_id) from
<write_bucket>.SA_DE a
inner join
<write_bucket>.AL_DE b
on a.person_id=b.person_id 

--there are only 429 pts with AL and SA! 
--Which is good (for PT) but (bad for study)

-- COMMAND ----------

-- if at first you dont succeed, sample, sample again!
--consider all RX and SA pairs below
drop view if exists SA_distinct;
create view SA_distinct as 
select distinct person_id 
from 
<write_bucket>.SA_DE;

drop table if exists <write_bucket>.SA_RX;
create table <write_bucket>.SA_RX as
select 
count(distinct b.person_id) as pt_cnt,
count(b.drug_source_value) as ev_cnt,
b.drug_source_value as concept_code
from
SA_distinct a
inner join
<write_bucket>.drug_exposure b
on a.person_id=b.person_id
group by 
b.drug_source_value;

-- COMMAND ----------

select * from <write_bucket>.SA_RX

-- COMMAND ----------

--now with a new idea (large pt_cnt), consider if rx is implicated within time range of SA

select * from 
<write_bucket>.SA_RX a left join
<write_bucket>.concept b
on a.concept_code=b.concept_code
where concept_name like '%metoclopramide%' or
concept_name like '%Metoclopramide%' or
concept_name like '%METOCLOPRAMIDE%'

;

-- COMMAND ----------

-- cases with condition 
drop table if exists <write_bucket>.SA_RX_case_list;
create table <write_bucket>.SA_RX_case_list as
select distinct a.person_id
from
<write_bucket>.SA_DE a
left join
<write_bucket>.drug_exposure b
on a.person_id=b.person_id
left join <write_bucket>.concept c
on b.drug_source_value=c.concept_code
where 
c.concept_name like '%metoclopramide%' or
c.concept_name like '%Metoclopramide%' or
c.concept_name like '%METOCLOPRAMIDE%';

select count(distinct person_id) from <write_bucket>.SA_RX_case_list; --79k is better than .4K out of 700K SA index cases

-- COMMAND ----------

drop table if exists <write_bucket>.SA_MO_case_event_list;

create table <write_bucket>.SA_MO_case_event_list as
select 
a.person_id,
a.event_type,
a.concept_name as dx,
a.event_date as dx_date,
c.concept_name as rx,
b.drug_exposure_start_date as rx_date,
b.drug_source_value,
datediff(a.event_date,b.drug_exposure_start_date) as diff-- tell me if the SA and RX date are near each other 
from <write_bucket>.SA_DE a
left join
<write_bucket>.drug_exposure b
on a.person_id=b.person_id
left join <write_bucket>.concept c
on b.drug_source_value=c.concept_code
where 
c.concept_name like '%metoclopramide%' or
c.concept_name like '%Metoclopramide%' or
c.concept_name like '%METOCLOPRAMIDE%'
order by person_id;

-- COMMAND ----------

select count(distinct person_id) as pt_cnt,
count(distinct person_id)/79568 as rate,--you can use this to think about diff ranges to use
event_type,diff from <write_bucket>.SA_MO_case_event_list 
group by diff,event_type 
order by pt_cnt desc;

-- COMMAND ----------

--Did we win? Remember ETL dates are service through dates so a diff of -2 (two days after SA bill) would still count. 
--Not so fast, consider a relative odds, where observed births with rx vs observed sa with rx of interest? 
--Your turn, find some pregnancy codes that are present in the CDM, assign cases medication and see if the attack rate within time frame is random or not.
