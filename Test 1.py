# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Edit this notebook in databricks and see how it flows into VSC

# COMMAND ----------

# MAGIC %run ./init

# COMMAND ----------

def pipeline (direction: str):
    current_time = current_timestamp()
    df= spark.read.table("default.weather")
    shortlist = df.filter(f"WindGustDir == '{direction}'").select("ID","WindGustDir","Evaporation", "Sunshine")
    shortlist = shortlist.withColumn("datetime", current_time)
    shortlist = shortlist.withColumn("random_number", (rand() * 1000).cast("integer"))
    shortlist = shortlist.withColumn("calculated_column", shortlist["Sunshine"] * shortlist["random_number"])
    shortlist.write.mode("append").saveAsTable("weather_aggregated")

    user_value = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user"))
    user_value = user_value.strip("Some(").strip(")")
    windowSpec = Window.orderBy(col("datetime").desc())
    filtered_data = shortlist.withColumn("record_count", count("*").over(windowSpec))
    logged = shortlist.groupBy("WindGustDir", "datetime").count()
    logged = logged.withColumn("Detail", lit(user_value))
    logged = logged.withColumnRenamed("WindGustDir", "Direction")
    logged.write.mode("append").saveAsTable("logging_weather_aggregated")

    return print(f"Pipeline run successfully")

# COMMAND ----------

pipeline('N')

# COMMAND ----------

# Change made on github dev 
# Change made on local dev


