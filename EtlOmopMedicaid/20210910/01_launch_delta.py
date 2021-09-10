# Databricks notebook source
# MAGIC %sql
# MAGIC --instructions
# MAGIC --#S#T needs to be configured to one of the available options:
# MAGIC 
# MAGIC --options 
# MAGIC --as1m|one state one month
# MAGIC --1s1y|one state one year
# MAGIC --1s|one state all years
# MAGIC --
# MAGIC --dbutils.notebook.run needs to be configured with("notebookname",max_run_time_in_seconds)
# MAGIC --500,000-138 hours as max run time
# MAGIC --a db notebook can run for a max of 48 hours due to databricks design; runs longer than 48 hours will not finish
# MAGIC 
# MAGIC --01 load 1s1m
# MAGIC --02 load 1s1m from materialized
# MAGIC --05 load optimized
# MAGIC 
# MAGIC --06 run from materialized w optimized
# MAGIC --08 run from materialized with x2 assets optimized
# MAGIC --09 run from feeder with x2 assets and fast write times and zorder optimized--auto?
# MAGIC 
# MAGIC --10 run from materialized with x2 assets and fast write times and zorder optimized--auto?
# MAGIC --11 as1m local
# MAGIC 
# MAGIC --12 1s1m materialized
# MAGIC --13 1s1m feeder
# MAGIC -- 14 1s1m materailized
# MAGIC --QQ so partrition by state cd does what here?
# MAGIC --ALTER TABLE BigTable SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

# COMMAND ----------

#spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
#spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = "True";
spark.conf.set("spark.databricks.io.cache.enabled", "True");


# COMMAND ----------

# MAGIC %run /Users/NWI388/main/02_ddl $scope="as3j" $job_id='017' $state="'asj3'"

# COMMAND ----------

# MAGIC %run /Users/NWI388/main/03_feeder_1m_delta_3junes $scope="as3j" $job_id='017' $state="'as3j'"

# COMMAND ----------

# MAGIC %run /Users/NWI388/main/04_ETL_m_delta $scope="as3j" $job_id='017' $state="'as3j'" $materializer="" $drawer=""

# COMMAND ----------

#%run /Users/NWI388/main/05_destination $scope="as3j" $job_id='017' $state="'as3j'"

# COMMAND ----------

# MAGIC %run /Users/NWI388/main/06_rollup  $scope="as3j" $job_id='017' $state="'as3j'"

# COMMAND ----------

#%run /Users/NWI388/main/07_materializer_delta $scope="as3j" $job_id='017' $state="'as3j'" $drawer="as3j"

# COMMAND ----------

# MAGIC %run /Users/NWI388/main/08_Counts $scope="as3j" $job_id='017' $state="'as3j'"
