# Databricks notebook source
# MAGIC %pip install -U databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
  import databricks.sdk.service.compute as C

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
init_script_path = "/".join(notebook_path.split("/")[:-1] + ["mosaic-gdal-init.sh"])
init_script_path

# COMMAND ----------

w.clusters.create(
  cluster_name="Mosaic Demo Cluster",
  node_type_id="m6i.4xlarge",
  num_workers=4,
  autotermination_minutes=120,
  data_security_mode=C.DataSecurityMode.SINGLE_USER,
  spark_version="13.3.x-scala2.12",
  spark_conf={
    "spark.task.cpus": "4"
  },
  runtime_engine=C.RuntimeEngine.PHOTON,
  init_scripts=[C.InitScriptInfo(workspace=C.WorkspaceStorageInfo(destination=init_script_path))]
)
