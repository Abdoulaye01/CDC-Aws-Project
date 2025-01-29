# Databricks notebook source
# MAGIC %md
# MAGIC #### Glue job triggered by lambda

# COMMAND ----------

import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = SparkSession.builder.appName("CDC Aws").getOrCreate()
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["s3_source_file", "s3_source_bucket"])
bucketName = argv[s3_source_bucket]
fileName = argv[s3_source_file]

print(bucketName, "", fileName)

inputFilePath = f"s3a://{bucketName}/{fileName}"
finalFilePath = f"s3a://cdc-output-pyspark/output"

if "LOAD" in fileName:
    flDf = spark.read_csv(inputFilePath)
    flDf = (
        flDf.withColumnRenamed("_c0", "Id")
        .withColumnRenamed("_c1", "FullName")
        .withColumnRenamed("_c2", "City")
    )
    flDf.write.mode("overwrite").csv(finalFilePath)
else:
    upDF = spark.read_csv(inputFilePath)
    # Rename Columns headers
    upDF = (
        upDF.withColumnRenamed("_c0", "Actions")
        .withColumnRenamed("_c1", "Id")
        .withColumnRenamed("_c2", "FullName")
        .withColumnRenamed("_c3", "City")
    )
    ffDF = spark.read_csv(finalFilePath)
    ffDF = (
        ffDF.withColumnRenamed("_c0", "Id")
        .withColumnRenamed("_c1", "FullName")
        .withColumnRenamed("_c2", "City")
    )
    for upRow in upDF.collect():
        if upRow["Actions"] == "U":
            ffDF = ffDF.withColumn(
                "FullName",
                when(ffDF["Id"] == upRow["Id"], upRow["FullName"]).otherwise(
                    ffDF["FullName"]
                ),
            )
            ffDF = ffDF.withColumn(
                "City",
                when(ffDF["City"] == upRow["City"], upRow["City"]).otherwise(
                    ffDF["City"]
                ),
            )
        if upRow["Actions"] == "I":
            insertRow = [list(upRow)[1:]]
            columns = ["Id", "FullName", "City"]
            newDf = spark.createDataFrame(insertRow, columns)
            ffDF = ffDF.union(newDf)
        if upRow["Actions"] == "D":
            ffDF = ffDF.filter(ffDF.Id != upRow["Id"])
    flDf.write.mode("overwrite").csv(finalFilePath)
