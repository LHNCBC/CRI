# Databricks notebook source
# MAGIC %sql
# MAGIC --instructions
# MAGIC --#S#T needs to be configured to one of the available options:
# MAGIC 
# MAGIC --options 
# MAGIC --1s1m|one state one month
# MAGIC --1s1y|one state one year
# MAGIC --1s|one state all years
# MAGIC --
# MAGIC --dbutils.notebook.run needs to be configured with("notebookname",max_run_time_in_seconds)
# MAGIC --500,000-138 hours as max run time
# MAGIC --a db notebook can run for a max of 48 hours due to databricks design; runs longer than 48 hours will not finish

# COMMAND ----------

# MAGIC %run /Users/NWI388/main/01_ddl $job="1s1y" 

# COMMAND ----------

# MAGIC %run /Users/NWI388/main/02_dictionary

# COMMAND ----------

# MAGIC %run /Users/NWI388/main/03_Feeder_1s1y $state="'CA'" 

# COMMAND ----------

# MAGIC %run /Users/NWI388/main/04_ETL

# COMMAND ----------

# MAGIC %run /Users/NWI388/main/05_destination $job="1s1y" 

# COMMAND ----------

#name it something else!
#%run /Users/NWI388/01_ddl_rollup $job="1s1y" 

