# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Azure Event Hub Configuration
# MAGIC

# COMMAND ----------

event_hub_namespace = "eh-healthcare.servicebus.windows.net"
event_hub_name="hospital-eh-data"  
event_hub_conn_str = dbutils.secrets.get(scope = "healthcare-scope", key = "eventhub-connection")


kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

# COMMAND ----------

# MAGIC %md
# MAGIC `Read from eventhub
# MAGIC `

# COMMAND ----------

df_raw = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
          )

# COMMAND ----------

# MAGIC %md
# MAGIC `Cast data to json
# MAGIC `

# COMMAND ----------

df_raw = df_raw.selectExpr("CAST(value AS STRING)as json_raw ")

# COMMAND ----------

# MAGIC %md
# MAGIC `ADLS configuration 
# MAGIC `

# COMMAND ----------

bronze_path = "abfss://bronze@healthcare4dataproject.dfs.core.windows.net/patient_flow"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write stream to bronze
# MAGIC

# COMMAND ----------

(
    df_raw
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "abfss://bronze@healthcare4dataproject.dfs.core.windows.net/_checkpoints/patient_flow")
    .trigger(availableNow=True)
    .start(bronze_path)
)

# COMMAND ----------

display(spark.read.format("delta").load(bronze_path))