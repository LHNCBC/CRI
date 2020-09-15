proc sql;
create table
work.age_taf_2016_12
as
select
bene_id,state_cd,2016-year(birth_dt)as age
from
tafr16.demog_elig_base;

proc sql;
create table
work.age_taf_2016_12
as
select
count(bene_id) as pt_cnt,
age,
state_cd
from
work.age_taf_2016_12
group by
age,state_cd;

proc sql;
create table
work.age_max_2013_12
as
select
bene_id,state_cd,2013-year(el_dob) as age
from
max2013.maxdata_ps_2013;

proc sql;
create table
work.age_max_2013_12
as
select
count(bene_id) as pt_cnt,
age,
state_cd
from 
work.age_max_2013_12
group by
age,state_cd;
