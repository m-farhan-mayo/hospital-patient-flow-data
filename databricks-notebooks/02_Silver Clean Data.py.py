# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

bronze_path = "abfss://bronze@healthcare4dataproject.dfs.core.windows.net/patient_flow"
silver_path = "abfss://silver@healthcare4dataproject.dfs.core.windows.net/patient_flow"

# COMMAND ----------

bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

# COMMAND ----------

#Defin Schema
schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("admission_time", StringType()),
    StructField("discharge_time", StringType()),
    StructField("bed_id", IntegerType()),
    StructField("hospital_id", IntegerType())
])

# COMMAND ----------

bronze_df = bronze_df.withColumn("data",from_json(col("json_raw"),schema)).select("data.*")

# COMMAND ----------

bronze_df = bronze_df.withColumn("admission_time", to_timestamp("admission_time"))
bronze_df = bronze_df.withColumn("discharge_time", to_timestamp("discharge_time"))                            

# COMMAND ----------

#invalid admission_times
bronze_df = bronze_df.withColumn("admission_time",
                               when(
                                   col("admission_time").isNull() | (col("admission_time") > current_timestamp()),
                                   current_timestamp())
                               .otherwise(col("admission_time")))



# COMMAND ----------

#Handle Invalid Age
bronze_df = bronze_df.withColumn("age",
                               when(col("age")>100,floor(rand()*90+1).cast("int"))
                               .otherwise(col("age")))

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

(
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema","true")
    .option("checkpointLocation", "abfss://silver@healthcare4dataproject.dfs.core.windows.net/_checkpoints/patient_flow")
    .trigger(availableNow=True)
    .start(silver_path)
)