# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, expr, current_timestamp, to_timestamp, sha2, concat_ws, coalesce, monotonically_increasing_id
from delta.tables import DeltaTable
from pyspark.sql import Window

# COMMAND ----------

silver_path = "abfss://silver@healthcare4dataproject.dfs.core.windows.net/patient_flow"
gold_dim_patient = "abfss://gold@healthcare4dataproject.dfs.core.windows.net/dim_patient"
gold_dim_department = "abfss://gold@healthcare4dataproject.dfs.core.windows.net/dim_department"
gold_fact = "abfss://gold@healthcare4dataproject.dfs.core.windows.net/fact"

# COMMAND ----------

silver_df = spark.read.format("delta").load(silver_path)


# COMMAND ----------

w = Window.partitionBy("patient_id").orderBy(F.col("admission_time").desc())

# COMMAND ----------

silver_df = (
    silver_df
    .withColumn("row_num", F.row_number().over(w))  # Rank by latest admission_time
    .filter(F.col("row_num") == 1)                  # Keep only latest row
    .drop("row_num")
)

# COMMAND ----------

incoming_patient = (silver_df
                    .select("patient_id", "gender", "age")
                    .withColumn("effective_from", current_timestamp())
                   )

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, gold_dim_patient):
    # initialize table with schema and empty data
    incoming_patient.withColumn("surrogate_key", F.monotonically_increasing_id()) \
                    .withColumn("effective_to", lit(None).cast("timestamp")) \
                    .withColumn("is_current", lit(True)) \
                    .write.format("delta").mode("overwrite").save(gold_dim_patient)

# COMMAND ----------

target_patient = DeltaTable.forPath(spark, gold_dim_patient)

# COMMAND ----------

incoming_patient = incoming_patient.withColumn("_hash",F.sha2(F.concat_ws("||", F.coalesce(col("gender"), lit("NA")), F.coalesce(col("age").cast("string"), lit("NA"))), 256)
)

# COMMAND ----------

target_patient_df = spark.read.format("delta").load(gold_dim_patient).withColumn(
    "_target_hash",
    F.sha2(F.concat_ws("||", F.coalesce(col("gender"), lit("NA")), F.coalesce(col("age").cast("string"), lit("NA"))), 256)
).select("surrogate_key", "patient_id", "gender", "age", "is_current", "_target_hash", "effective_from", "effective_to")


# COMMAND ----------

incoming_patient.createOrReplaceTempView("incoming_patient_tmp")
target_patient_df.createOrReplaceTempView("target_patient_tmp")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Mark old current rows as not current where changed

# COMMAND ----------

changes_df = spark.sql("""
SELECT t.surrogate_key, t.patient_id
FROM target_patient_tmp t
JOIN incoming_patient_tmp i
  ON t.patient_id = i.patient_id
WHERE t.is_current = true AND t._target_hash <> i._hash
""")

changed_keys = [row['surrogate_key'] for row in changes_df.collect()]

if changed_keys:
    # Update existing current records: set is_current=false and effective_to=current_timestamp()
    target_patient.update(
        condition = expr("is_current = true AND surrogate_key IN ({})".format(",".join([str(k) for k in changed_keys]))),
        set = {
            "is_current": expr("false"),
            "effective_to": expr("current_timestamp()")
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert new rows for changed & new records

# COMMAND ----------

inserts_df = spark.sql("""
SELECT i.patient_id, i.gender, i.age, i.effective_from, i._hash
FROM incoming_patient_tmp i
LEFT JOIN target_patient_tmp t
  ON i.patient_id = t.patient_id AND t.is_current = true
WHERE t.patient_id IS NULL OR t._target_hash <> i._hash
""").withColumn("surrogate_key", F.monotonically_increasing_id()) \
  .withColumn("effective_to", lit(None).cast("timestamp")) \
  .withColumn("is_current", lit(True)) \
  .select("surrogate_key", "patient_id", "gender", "age", "effective_from", "effective_to", "is_current")

# Append new rows
if inserts_df.count() > 0:
    inserts_df.write.format("delta").mode("append").save(gold_dim_patient)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Department Dimension Table Creation

# COMMAND ----------

incoming_dept = (silver_df
                 .select("department", "hospital_id")
                )


# COMMAND ----------

incoming_dept = incoming_dept.dropDuplicates(["department", "hospital_id"]) \
    .withColumn("surrogate_key", monotonically_increasing_id())

# COMMAND ----------

incoming_dept.select("surrogate_key", "department", "hospital_id") \
    .write.format("delta").mode("overwrite").save(gold_dim_department)



# COMMAND ----------

# MAGIC %md
# MAGIC ##  Create Fact table

# COMMAND ----------

# Read current dims (filter is_current=true)

dim_patient_df = (spark.read.format("delta").load(gold_dim_patient)
                  .filter(col("is_current") == True)
                  .select(col("surrogate_key").alias("surrogate_key_patient"), "patient_id", "gender", "age"))

dim_dept_df = (spark.read.format("delta").load(gold_dim_department)
               .select(col("surrogate_key").alias("surrogate_key_dept"), "department", "hospital_id"))



# COMMAND ----------

# Build base fact from silver events
fact_base = (silver_df
             .select("patient_id", "department", "hospital_id", "admission_time", "discharge_time", "bed_id")
             .withColumn("admission_date", F.to_date("admission_time"))
            )



# COMMAND ----------

# Join to get surrogate keys
fact_enriched = (fact_base
                 .join(dim_patient_df, on="patient_id", how="left")
                 .join(dim_dept_df, on=["department", "hospital_id"], how="left")
                )

# COMMAND ----------

# Compute metrics

fact_enriched = fact_enriched.withColumn("length_of_stay_hours",
                                         (F.unix_timestamp(col("discharge_time")) - F.unix_timestamp(col("admission_time"))) / 3600.0) \
                             .withColumn("is_currently_admitted", F.when(col("discharge_time") > current_timestamp(), lit(True)).otherwise(lit(False))) \
                             .withColumn("event_ingestion_time", current_timestamp())

# COMMAND ----------

# Let's make column names explicit instead:

fact_final = fact_enriched.select(
    F.monotonically_increasing_id().alias("fact_id"),
    col("surrogate_key_patient").alias("patient_sk"),
    col("surrogate_key_dept").alias("department_sk"),
    "admission_time",
    "discharge_time",
    "admission_date",
    "length_of_stay_hours",
    "is_currently_admitted",
    "bed_id",
    "event_ingestion_time"
)

# COMMAND ----------

# Persist fact table partitioned by admission_date (helps Synapse / queries)
fact_final.write.format("delta").mode("overwrite").save(gold_fact)



# COMMAND ----------

print("Patient dim count:", spark.read.format("delta").load(gold_dim_patient).count())
print("Department dim count:", spark.read.format("delta").load(gold_dim_department).count())
print("Fact rows:", spark.read.format("delta").load(gold_fact).count())