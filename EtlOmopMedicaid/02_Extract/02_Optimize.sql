-- Databricks notebook source
insert into <write_bucket>.log values('$job_id','Optimize','1','start optimize',current_timestamp() );

-- COMMAND ----------

optimize <write_bucket>.inpatient_header_$year zorder by(clm_id);
insert into <write_bucket>.log values('$job_id','Optimize','2','inpatient_header',current_timestamp() );

-- COMMAND ----------

optimize <write_bucket>.inpatient_line_$year zorder by(clm_id);
insert into <write_bucket>.log values('$job_id','Optimize','3','inpatient_line',current_timestamp() );

-- COMMAND ----------

optimize <write_bucket>.other_services_header_$year zorder by(clm_id);
insert into <write_bucket>.log values('$job_id','Optimize','4','other_services_header',current_timestamp() );

-- COMMAND ----------

optimize <write_bucket>.other_services_line_$year zorder by(clm_id);
insert into <write_bucket>.log values('$job_id','Optimize','5','other_services_line',current_timestamp() );

-- COMMAND ----------

optimize <write_bucket>.long_term_header_$year zorder by(clm_id);
insert into <write_bucket>.log values('$job_id','Optimize','6','long_term_header',current_timestamp() );

-- COMMAND ----------

optimize <write_bucket>.long_term_line_$year zorder by(clm_id);
insert into <write_bucket>.log values('$job_id','Optimize','7','long_term_line',current_timestamp() );

-- COMMAND ----------

optimize <write_bucket>.rx_header_$year zorder by(clm_id);
insert into <write_bucket>.log values('$job_id','Optimize','8','rx_header',current_timestamp() );

-- COMMAND ----------

optimize <write_bucket>.rx_line_$year zorder by(clm_id);
insert into <write_bucket>.log values('$job_id','Optimize','9','rx_line',current_timestamp() );

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Optimize','10','end optimize',current_timestamp() );