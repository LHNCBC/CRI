# Databricks notebook source

#dbutils.notebook.run(
#  "/Users/<user_id>/03_Transform_Load/A3_provider",
#  timeout_seconds = 360000,
#  arguments = {
#  "job_id": "2001",
#  "year":"16"
#  })

# COMMAND ----------


dbutils.notebook.run(
  "/Users/<user_id>/03_Transform_Load/B1_visit_occurrence ",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"16"
  })

# COMMAND ----------

dbutils.notebook.run(
  "/Users/<user_id>/03_Transform_Load/B2_condition_occurrence",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"16"
  })

# COMMAND ----------

dbutils.notebook.run(
  "/Users/<user_id>/03_Transform_Load/B3_procedure_occurrence",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"16"
  })

# COMMAND ----------

dbutils.notebook.run(
  "/Users/<user_id>/03_Transform_Load/B4_drug_exposure ",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"16"
  })


# COMMAND ----------

dbutils.notebook.run(
  "/Users/<user_id>/03_Transform_Load/D1_observation",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"16"
  })

# COMMAND ----------

dbutils.notebook.run(
  "/Users/<user_id>/03_Transform_Load/D2_measurement",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"16"
  })