-- Databricks notebook source

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','1','start_20',current_timestamp(), null);

-- COMMAND ----------

--inpt_header
insert into dua_052538_nwi388.inpatient_header_20
select $inpatient_header from(
select $inpatient_header    from      dua_052538.tafr20_inpatient_header_01    union
select $inpatient_header    from      dua_052538.tafr20_inpatient_header_02    union
select $inpatient_header    from      dua_052538.tafr20_inpatient_header_03    union
select $inpatient_header    from      dua_052538.tafr20_inpatient_header_04    union
select $inpatient_header    from      dua_052538.tafr20_inpatient_header_05    union
select $inpatient_header    from      dua_052538.tafr20_inpatient_header_06    union
select $inpatient_header    from      dua_052538.tafr20_inpatient_header_07    union
select $inpatient_header    from      dua_052538.tafr20_inpatient_header_08    union
select $inpatient_header    from      dua_052538.tafr20_inpatient_header_09    union
select $inpatient_header    from      dua_052538.tafr20_inpatient_header_10    union
select $inpatient_header    from      dua_052538.tafr20_inpatient_header_11    union
select $inpatient_header    from      dua_052538.tafr20_inpatient_header_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','4','inpatient_header_20',current_timestamp(), null);

-- COMMAND ----------

--inpt_line
insert into  dua_052538_nwi388.inpatient_line_20
select $inpatient_line from(
select $inpatient_line from dua_052538.tafr20_inpatient_line_01 union
select $inpatient_line from dua_052538.tafr20_inpatient_line_02 union
select $inpatient_line from dua_052538.tafr20_inpatient_line_03 union
select $inpatient_line from dua_052538.tafr20_inpatient_line_04 union
select $inpatient_line from dua_052538.tafr20_inpatient_line_05 union
select $inpatient_line from dua_052538.tafr20_inpatient_line_06 union
select $inpatient_line from dua_052538.tafr20_inpatient_line_07 union
select $inpatient_line from dua_052538.tafr20_inpatient_line_08 union
select $inpatient_line from dua_052538.tafr20_inpatient_line_09 union
select $inpatient_line from dua_052538.tafr20_inpatient_line_10 union
select $inpatient_line from dua_052538.tafr20_inpatient_line_11 union
select $inpatient_line from dua_052538.tafr20_inpatient_line_12 )
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','5','inpatient_line_20',current_timestamp(), null);

-- COMMAND ----------

--ot_header
insert into dua_052538_nwi388.other_services_header_20
select  $other_services_header from(
select  $other_services_header from dua_052538.tafr20_other_services_header_01 union
select  $other_services_header from dua_052538.tafr20_other_services_header_02 union
select  $other_services_header from dua_052538.tafr20_other_services_header_03 union
select  $other_services_header from dua_052538.tafr20_other_services_header_04 union
select  $other_services_header from dua_052538.tafr20_other_services_header_05 union
select  $other_services_header from dua_052538.tafr20_other_services_header_06 union
select  $other_services_header from dua_052538.tafr20_other_services_header_07 union
select  $other_services_header from dua_052538.tafr20_other_services_header_08 union
select  $other_services_header from dua_052538.tafr20_other_services_header_09 union
select  $other_services_header from dua_052538.tafr20_other_services_header_10 union
select  $other_services_header from dua_052538.tafr20_other_services_header_11 union
select  $other_services_header from dua_052538.tafr20_other_services_header_12 )
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','6','other_services_header_20',current_timestamp(), null);

-- COMMAND ----------

--ot_line
insert into dua_052538_nwi388.other_services_line_20
select $other_services_line from(
select $other_services_line from dua_052538.tafr20_other_services_line_01 union
select $other_services_line from dua_052538.tafr20_other_services_line_02 union
select $other_services_line from dua_052538.tafr20_other_services_line_03 union
select $other_services_line from dua_052538.tafr20_other_services_line_04 union
select $other_services_line from dua_052538.tafr20_other_services_line_05 union
select $other_services_line from dua_052538.tafr20_other_services_line_06 union
select $other_services_line from dua_052538.tafr20_other_services_line_07 union
select $other_services_line from dua_052538.tafr20_other_services_line_08 union
select $other_services_line from dua_052538.tafr20_other_services_line_09 union
select $other_services_line from dua_052538.tafr20_other_services_line_10 union
select $other_services_line from dua_052538.tafr20_other_services_line_11 union
select $other_services_line from dua_052538.tafr20_other_services_line_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','7','other_services_line_20',current_timestamp(), null);

-- COMMAND ----------

--lt_header
insert into dua_052538_nwi388.long_term_header_20
select $long_term_header from(
select $long_term_header from dua_052538.tafr20_long_term_header_01 union
select $long_term_header from dua_052538.tafr20_long_term_header_02 union
select $long_term_header from dua_052538.tafr20_long_term_header_03 union
select $long_term_header from dua_052538.tafr20_long_term_header_04 union
select $long_term_header from dua_052538.tafr20_long_term_header_05 union
select $long_term_header from dua_052538.tafr20_long_term_header_06 union
select $long_term_header from dua_052538.tafr20_long_term_header_07 union
select $long_term_header from dua_052538.tafr20_long_term_header_08 union
select $long_term_header from dua_052538.tafr20_long_term_header_09 union
select $long_term_header from dua_052538.tafr20_long_term_header_10 union
select $long_term_header from dua_052538.tafr20_long_term_header_11 union
select $long_term_header from dua_052538.tafr20_long_term_header_12 )
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','8','long_term_header_20',current_timestamp(), null);

-- COMMAND ----------

--lt_line
insert into dua_052538_nwi388.long_term_line_20
select $long_term_line from(
select $long_term_line from dua_052538.tafr20_long_term_line_01 union
select $long_term_line from dua_052538.tafr20_long_term_line_02 union
select $long_term_line from dua_052538.tafr20_long_term_line_03 union
select $long_term_line from dua_052538.tafr20_long_term_line_04 union
select $long_term_line from dua_052538.tafr20_long_term_line_05 union
select $long_term_line from dua_052538.tafr20_long_term_line_06 union
select $long_term_line from dua_052538.tafr20_long_term_line_07 union
select $long_term_line from dua_052538.tafr20_long_term_line_08 union
select $long_term_line from dua_052538.tafr20_long_term_line_09 union
select $long_term_line from dua_052538.tafr20_long_term_line_10 union
select $long_term_line from dua_052538.tafr20_long_term_line_11 union
select $long_term_line from dua_052538.tafr20_long_term_line_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','9','long_term_line_20',current_timestamp(), null);

-- COMMAND ----------

--rx_header
insert into dua_052538_nwi388.rx_header_20
select $rx_header from(
select $rx_header from dua_052538.tafr20_rx_header_01 union
select $rx_header from dua_052538.tafr20_rx_header_02 union
select $rx_header from dua_052538.tafr20_rx_header_03 union
select $rx_header from dua_052538.tafr20_rx_header_04 union
select $rx_header from dua_052538.tafr20_rx_header_05 union
select $rx_header from dua_052538.tafr20_rx_header_06 union
select $rx_header from dua_052538.tafr20_rx_header_07 union
select $rx_header from dua_052538.tafr20_rx_header_08 union
select $rx_header from dua_052538.tafr20_rx_header_09 union
select $rx_header from dua_052538.tafr20_rx_header_10 union
select $rx_header from dua_052538.tafr20_rx_header_11 union
select $rx_header from dua_052538.tafr20_rx_header_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','10','rx_header_20',current_timestamp(), null);

-- COMMAND ----------

--rx_line
insert into dua_052538_nwi388.rx_line_20
select $rx_line from(
select $rx_line from dua_052538.tafr20_rx_line_01 union
select $rx_line from dua_052538.tafr20_rx_line_02 union
select $rx_line from dua_052538.tafr20_rx_line_03 union
select $rx_line from dua_052538.tafr20_rx_line_04 union
select $rx_line from dua_052538.tafr20_rx_line_05 union
select $rx_line from dua_052538.tafr20_rx_line_06 union
select $rx_line from dua_052538.tafr20_rx_line_07 union
select $rx_line from dua_052538.tafr20_rx_line_08 union
select $rx_line from dua_052538.tafr20_rx_line_09 union
select $rx_line from dua_052538.tafr20_rx_line_10 union
select $rx_line from dua_052538.tafr20_rx_line_11 union
select $rx_line from dua_052538.tafr20_rx_line_12)
where bene_id is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','11','rx_line_20',current_timestamp(), null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','EXTRACT','12','end_20',current_timestamp(), null);