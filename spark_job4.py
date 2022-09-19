from pyspark.sql import SparkSession
import base64
spark = SparkSession.builder.master('local').appName('spark-read-from-bigquery').getOrCreate()
# Creating DataFrames
df= spark.read.format('bigquery').option('project','clouderatakedown-lab').option('table','hsi_consumption.customer').load()
# 2 - Print the Google BigQuery table
#df1.show()
df.printSchema()
#df2.printSchema()
# 3 - Creating or replacing a local temporary view with this DataFrame
df.createOrReplaceTempView("customer")
query=spark.sql("SELECT COALESCE(CS.cuid,'-1') AS cuid ,COALESCE(CS.customer_account_number,-1) AS customer_account_number ,COALESCE(CS.customer_name,'Undefined') AS customer_name ,CS.customer_address_city_name ,CS.customer_address_state_code ,CS.customer_address_country_code ,CS.customer_address_line_1_address ,CS.customer_address_line_2_address ,CS.customer_ar_parent_number ,COALESCE(CS.customer_billto_account_number,-1) AS customer_billto_account_number ,COALESCE(BAN.customer_name,'Undefined') AS customer_billto_account_name ,COALESCE(BAN.customer_address_line_1_address,'Undefined') AS customer_billto_address_line_1_address ,COALESCE(CS.customer_sales_parent_number,-1) AS customer_sales_parent_number ,COALESCE(PAN.customer_name,'Undefined') AS customer_sales_parent_name ,COALESCE(PAN.customer_address_line_1_address,'Undefined') AS customer_sales_parent_address_line_1_address ,COALESCE(CS.entity_code,'Undefined') AS entity_code ,COALESCE(CS.entity_name,'Undefined') AS entity_name ,CS.sales_plan_code ,CS.sales_plan_name ,COALESCE(CS.customer_contact_name,'Undefined') AS customer_contact_name ,CONCAT(COALESCE(CS.customer_contact_name,'Undefined'),' (',CAST(COALESCE(CS.customer_account_number,-1) AS STRING),')') AS customer_contact ,CONCAT(COALESCE(BAN.customer_name,'Undefined'),' (',CAST(COALESCE(CS.customer_billto_account_number,-1) AS STRING),')') AS customer_bill_to ,CONCAT(COALESCE(CS.customer_name,'Undefined'),' (',CAST(COALESCE(CS.customer_account_number,-1) AS STRING),')') AS customer_ship_to ,CONCAT(COALESCE(CS.entity_name,'Undefined'),' (',COALESCE(CS.entity_code,'Undefined'),')') AS customer_entity ,CONCAT(COALESCE(PAN.customer_name,'Undefined'),' (',CAST(COALESCE(CS.customer_sales_parent_number,-1) AS STRING),')') AS customer_parent FROM customer CS LEFT JOIN ( SELECT DISTINCT customer_account_number ,customer_name ,customer_address_line_1_address FROM customer ) BAN ON CS.customer_billto_account_number = BAN.customer_account_number LEFT JOIN ( SELECT DISTINCT customer_account_number ,customer_name ,customer_address_line_1_address FROM customer ) PAN ON CS.customer_sales_parent_number = PAN.customer_account_number")
query.write.format('bigquery').mode('overwrite').option('temporaryGcsBucket','big_query_from_hadoop').option('table','hsi_consumption.dummyspark4').save()
