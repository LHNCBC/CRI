# Databricks notebook source


# COMMAND ----------

dbutils.notebook.run(
  "/Users/NWI388/03_Transform_Load/A1_location",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001"
  })

  
# COMMAND ----------

dbutils.notebook.run(
  "/Users/NWI388/03_Transform_Load/A2_care_site",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001"
  })

  
# COMMAND ----------

dbutils.notebook.run(
  "/Users/NWI388/03_Transform_Load/A4_person",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001"
  })

# COMMAND ----------

dbutils.notebook.run(
  "/Users/NWI388/03_Transform_Load/A5_observation_period",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001"
  })

# COMMAND ----------

dbutils.notebook.run(
  "/Users/NWI388/03_Transform_Load/A6_death",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001"
  })

