# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder.appName("incremental_load").getOrCreate()

initial_data = [ Row(id=1, name="Amit", last_modified="2024-08-01 10:00:00"),
                Row(id=2, name="Priya", last_modified="2024-08-01 11:00:00"),
                Row(id=3, name="Ravi", last_modified="2024-08-01 12:00:00"),
                Row(id=4, name="Ananya", last_modified="2024-08-01 13:00:00"),
                Row(id=5, name="Rohit", last_modified="2024-08-01 14:00:00"),
                Row(id=6, name="Sneha", last_modified="2024-08-01 15:00:00"),
                Row(id=7, name="Vikram", last_modified="2024-08-01 16:00:00"),
                Row(id=8, name="Neha", last_modified="2024-08-01 17:00:00"),
                Row(id=9, name="Siddharth", last_modified="2024-08-01 18:00:00"),
]

columns = ["id", "name", "last_modified"]

initial_df = spark.createDataFrame(initial_data,columns)

initial_df.show()


# COMMAND ----------

# Write Initial Data to Delta Table
from pyspark.sql.functions import col

initial_df \
    .withColumn("last_modified", col("last_modified").cast("timestamp")) \
    .write.format("delta") \
    .mode("overwrite") \
    .save("dbfs:/user/hive/warehouse/load")


# COMMAND ----------

# Create and Store Initial Watermark
max_timestamp = initial_df.agg({"last_modified": "max"}).collect()[0][0]

# Calculate the maximum last_modified timestamp

watermark_df = spark.createDataFrame([(max_timestamp,)], ["max_last_modified"])
watermark_df.write.format("delta").mode("overwrite").save("dbfs:/user/hive/warehouse/watermark")


# COMMAND ----------

# Define Incremental Data 
incremental_data = [
     
  Row(id=1, name="Amit Sharma", last_modified="2024-08-02 10:00:00"),  # Updated record
    Row(id=11, name="Kajal", last_modified="2024-08-02 12:00:00"),        # New record
    Row(id=12, name="Arjun", last_modified="2024-08-02 14:00:00"),        # New record
    Row(id=13, name="Sanya", last_modified="2024-08-02 16:00:00"),        # New record
    Row(id=14, name="Deepak", last_modified="2024-08-02 18:00:00"),       # New record
    Row(id=15, name="Isha", last_modified="2024-08-02 20:00:00"),         # New record
    Row(id=16, name="Ravi Kumar", last_modified="2024-08-03 10:00:00"),    # Updated record
    Row(id=17, name="Riya", last_modified="2024-08-03 12:00:00"),         # New record
    Row(id=18, name="Siddhi", last_modified="2024-08-03 14:00:00"),       # New record
    Row(id=19, name="Raj", last_modified="2024-08-03 16:00:00")          # New record

]

incremental_df = spark.createDataFrame(incremental_data)


# COMMAND ----------

# Read Last Watermark
last_watermark_df = spark.read.format("delta").load("dbfs:/user/hive/warehouse/watermark")
last_watermark = last_watermark_df.select("max_last_modified").collect()[0][0]


# COMMAND ----------

# Filter Incremental Data

filtered_incremental_df = incremental_df.filter(col("last_modified") > last_watermark)

# COMMAND ----------

# Merge New Changes into Delta Table


from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/load")

delta_table.alias("target") \
    .merge(
        filtered_incremental_df.alias("source"),
        "target.id = source.id"
    ) \
    .whenMatchedUpdate(
        condition="target.last_modified < source.last_modified",
        set={
            "name": "source.name",
            "last_modified": "source.last_modified"
        }
    ) \
    .whenNotMatchedInsert(
        values={
            "id": "source.id",
            "name": "source.name",
            "last_modified": "source.last_modified"
        }
    ) \
    .execute()






# COMMAND ----------


#  Update Watermark with Latest Timestamp


new_max_timestamp = filtered_incremental_df.agg({"last_modified": "max"}).collect()[0][0]

new_watermark_df = spark.createDataFrame([(new_max_timestamp,)], ["max_last_modified"])
new_watermark_df.write.format("delta").mode("overwrite").save("dbfs:/user/hive/warehouse/watermark")

