from pyspark.sql import SparkSession
import pyspark
import os
import json
import argparse
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import col, sum, count, avg, max, min, datediff, current_date, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType


dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Dibimbing')
        .setMaster('local')
        .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")
    ))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# === EXTRACT: Read Data from PostgreSQL ===
jdbc_url = f"jdbc:postgresql://{postgres_host}/{postgres_dw_db}"
db_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

table_name = "public.retail"
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=db_properties)

# === TRANSFORM: Apply SQL Transformations ===

# Convert InvoiceDate to date format
df = df.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "MM/dd/yyyy"))

# === CUSTOMER PURCHASE BEHAVIOR ===
customer_analysis = df.groupBy("CustomerID").agg(
    count("InvoiceNo").alias("total_purchases"),
    sum(col("Quantity") * col("UnitPrice")).alias("total_revenue"),
    avg(col("Quantity") * col("UnitPrice")).alias("avg_order_value"),
    max("InvoiceDate").alias("last_purchase_date")
)

# === CHURN ANALYSIS (CUSTOMERS WHO HAVEN'T PURCHASED IN LAST 6 MONTHS) ===
customer_analysis = customer_analysis.withColumn(
    "churned", 
    (datediff(current_date(), col("last_purchase_date")) > 180).cast("int")  # 1 = churned, 0 = active
)

# === TOP-SELLING PRODUCTS ===
product_sales = df.groupBy("StockCode", "Description").agg(
    sum("Quantity").alias("total_sold"),
    sum(col("Quantity") * col("UnitPrice")).alias("total_revenue")
).orderBy(col("total_sold").desc())

# === AVERAGE ORDER VALUE PER COUNTRY ===
country_sales = df.groupBy("Country").agg(
    sum(col("Quantity") * col("UnitPrice")).alias("total_sales"),
    avg(col("Quantity") * col("UnitPrice")).alias("avg_order_value")
)

# === LOAD ===
customer_analysis.write.jdbc(url=jdbc_url, table="public.customer_analysis", mode="overwrite", properties=db_properties)
product_sales.write.jdbc(url=jdbc_url, table="public.product_sales", mode="overwrite", properties=db_properties)
country_sales.write.jdbc(url=jdbc_url, table="public.country_sales", mode="overwrite", properties=db_properties)

customer_analysis.show(5)
product_sales.show(5)
country_sales.show(5)