-- Databricks notebook source
insert into dua_052538_nwi388.log values('$job_id','Optimize','0','start optimize',current_timestamp(), null);

-- COMMAND ----------

optimize dua_052538_nwi388.inpatient_header_$year zorder by(clm_id);
insert into dua_052538_nwi388.log values('$job_id','Optimize','3','inpatient_header',current_timestamp(), null);

-- COMMAND ----------

optimize dua_052538_nwi388.inpatient_line_$year zorder by(clm_id);
insert into dua_052538_nwi388.log values('$job_id','Optimize','4','inpatient_line',current_timestamp(), null);

-- COMMAND ----------

optimize dua_052538_nwi388.other_services_header_$year zorder by(clm_id);
insert into dua_052538_nwi388.log values('$job_id','Optimize','5','other_services_header',current_timestamp(), null);

-- COMMAND ----------

optimize dua_052538_nwi388.other_services_line_$year zorder by(clm_id);
insert into dua_052538_nwi388.log values('$job_id','Optimize','6','other_services_line',current_timestamp(), null);

-- COMMAND ----------

optimize dua_052538_nwi388.long_term_header_$year zorder by(clm_id);
insert into dua_052538_nwi388.log values('$job_id','Optimize','7','long_term_header',current_timestamp(), null);

-- COMMAND ----------

optimize dua_052538_nwi388.long_term_line_$year zorder by(clm_id);
insert into dua_052538_nwi388.log values('$job_id','Optimize','8','long_term_line',current_timestamp(), null);

-- COMMAND ----------

optimize dua_052538_nwi388.rx_header_$year zorder by(clm_id);
insert into dua_052538_nwi388.log values('$job_id','Optimize','9','rx_header',current_timestamp(), null);

-- COMMAND ----------

optimize dua_052538_nwi388.rx_line_$year zorder by(clm_id);
insert into dua_052538_nwi388.log values('$job_id','Optimize','10','rx_line',current_timestamp(), null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Optimize','11','end optimize',current_timestamp(), null);