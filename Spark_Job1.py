#1 - Loading a Google BigQuery table into a DataFrame
# Initializing SparkSession
from pyspark.sql import SparkSession
import base64
spark = SparkSession.builder.master('local').appName('spark-read-from-bigquery').getOrCreate()
# Creating DataFrames
df = spark.read.format('bigquery').option('project','clouderatakedown-lab').option('table','hsi_consumption.customer').load()
# 2 - Print the Google BigQuery table
df.show()
df.printSchema()
# 3 - Creating or replacing a local temporary view with this DataFrame
df.createOrReplaceTempView("customer")
# Perform select order_id
peopleCountDf = spark.sql("SELECT customer_record_number,cuid,company_code,customer_account_number,customer_status_code from customer limit 1000")
# Display the content of df
peopleCountDf.write.format('bigquery').mode('overwrite').option('temporaryGcsBucket','big_query_from_hadoop').option('table','hsi_consumption.dummyspark').save()

