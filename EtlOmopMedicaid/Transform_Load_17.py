# Databricks notebook source


dbutils.notebook.run(
  "/Users/NWI388/03_Transform_Load/02_optimize",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"17"
  })

# COMMAND ----------

dbutils.notebook.run(
  "/Users/NWI388/03_Transform_Load/Bo_provider",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"17"
  })

# COMMAND ----------


dbutils.notebook.run(
  "/Users/NWI388/03_Transform_Load/B1_visit_occurrence ",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"17"
  })

# COMMAND ----------

dbutils.notebook.run(
  "/Users/NWI388/03_Transform_Load/B2_condition_occurrence",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"17"
  })

# COMMAND ----------

dbutils.notebook.run(
  "/Users/NWI388/03_Transform_Load/B3_procedure_occurrence",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"17"
  })

# COMMAND ----------

dbutils.notebook.run(
  "/Users/NWI388/03_Transform_Load/B4_drug_exposure ",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"17"
  })


# COMMAND ----------

dbutils.notebook.run(
  "/Users/NWI388/03_Transform_Load/B5_observation",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"17"
  })

# COMMAND ----------

dbutils.notebook.run(
  "/Users/NWI388/03_Transform_Load/B6_measurement",
  timeout_seconds = 360000,
  arguments = {
  "job_id": "2001",
  "year":"17"
  })