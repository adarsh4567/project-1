from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, row_number
from pyspark.sql.window import Window
import os
import logging

LOG_FILE = "/business_log/business_log.txt"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
  filename=LOG_FILE,
  filemode="a",
  level=logging.INFO,
  format="%(asctime)s %(levelname)s %(name)s - %(message)s",
  datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("PySparkBusinessJob")

def get_statewise_total(df_customers):
  logger.info("Calculating statewise total customers.")
  return df_customers.groupBy("customer_state").agg(count("customer_id").alias("total_customers")).orderBy("customer_state")

def get_statewise_payments(df_customers, df_orders, df_payments):
  logger.info("Calculating statewise most used payment type.")
  df_cust_orders = df_customers.join(df_orders, on="customer_id", how="inner")
  df_cust_orders_payments = df_cust_orders.join(df_payments, on="order_id", how="inner")
  df_count_payments = df_cust_orders_payments.groupBy("customer_state", "payment_type").agg(count("order_id").alias("uses"))
  state_window = Window.partitionBy("customer_state").orderBy(col("uses").desc())
  result = df_count_payments.withColumn("rank", row_number().over(state_window)).filter(col("rank") == 1).select("customer_state", "payment_type").orderBy("customer_state")
  logger.info("Statewise payment type calculation complete.")
  return result

def get_categorywise_review_score(df_order_items, df_products, df_reviews):
  logger.info("Calculating categorywise review scores.")
  df_joined_category = df_order_items.join(df_products, on="product_id", how="inner")
  df_joined_category_scores = df_joined_category.join(df_reviews, on="order_id", how="left")
  df_joined_category_final = df_joined_category_scores.groupBy("product_category_name").agg(count("review_score").alias("score"))
  logger.info("Categorywise review score calculation complete.")
  return df_joined_category_final.orderBy(col("score").desc())

def main():
    try:
        logger.info("Starting PySparkBusinessJob ETL process.")
        spark = SparkSession.builder.appName("PySparkBusinessJob").getOrCreate()

        logger.info("Reading orders dataset from S3.")
        df_orders = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/16-10-2025/olist_orders_dataset.csv", header=True, inferSchema=True)
        logger.info("Reading products dataset from S3.")
        df_products = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/16-10-2025/olist_products_dataset.csv", header=True, inferSchema=True)
        logger.info("Reading customers dataset from S3.")
        df_customers = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/16-10-2025/olist_customers_dataset.csv", header=True, inferSchema=True)
        logger.info("Reading order items dataset from S3.")
        df_order_items = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/16-10-2025/olist_order_items_dataset.csv", header=True, inferSchema=True)
        logger.info("Reading reviews dataset from S3.")
        df_reviews = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/16-10-2025/olist_order_reviews_dataset.csv", header=True, inferSchema=True)
        logger.info("Reading payments dataset from S3.")
        df_payments = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/16-10-2025/olist_order_payments_dataset.csv", header=True, inferSchema=True)

        logger.info("Calculating statewise total customers DataFrame.")
        df_statewise_total = get_statewise_total(df_customers)
        logger.info("Calculating statewise payments DataFrame.")
        df_statewise_payments = get_statewise_payments(df_customers, df_orders, df_payments)
        logger.info("Calculating categorywise review score DataFrame.")
        df_joined_category_desc = get_categorywise_review_score(df_order_items, df_products, df_reviews)

        sfOptionsForStateWiseCustomer = {
          "sfURL": "WVILNDK-KR28144.snowflakecomputing.com",
          "sfUser": "JATIL007",
          "sfPassword": "Jatil06572307011",
          "sfDatabase": "ECOM_DB",
          "sfSchema": "ECOM_SCHEMA",
          "sfWarehouse": "ECOM_WH",
        }

        sfOptionsForStateWisePayments = {
          "sfURL": "WVILNDK-KR28144.snowflakecomputing.com",
          "sfUser": "JATIL007",
          "sfPassword": "Jatil06572307011",
          "sfDatabase": "ECOM_DB",
          "sfSchema": "ECOM_SCHEMA",
          "sfWarehouse": "ECOM_WH",
        }
        

        sfOptionsForCategoryWiseReviewScore = {
          "sfURL": "WVILNDK-KR28144.snowflakecomputing.com",
          "sfUser": "JATIL007",
          "sfPassword": "Jatil06572307011",
          "sfDatabase": "ECOM_DB",
          "sfSchema": "ECOM_SCHEMA",
          "sfWarehouse": "ECOM_WH",
        }

        logger.info("Writing statewise total customers DataFrame to Snowflake.")
        df_statewise_total.write.format("net.snowflake.spark.snowflake").options(**sfOptionsForStateWiseCustomer).option("dbtable", "ECOM_SCHEMA.STATEWISECUSTOMER").mode("overwrite").save()
        logger.info("Writing statewise payments DataFrame to Snowflake.")
        df_statewise_payments.write.format("net.snowflake.spark.snowflake").options(**sfOptionsForStateWisePayments).option("dbtable", "ECOM_SCHEMA.STATEWISEPAYMENT").mode("overwrite").save()
        logger.info("Writing categorywise review score DataFrame to Snowflake.")
        df_joined_category_desc.write.format("net.snowflake.spark.snowflake").options(**sfOptionsForCategoryWiseReviewScore).option("dbtable", "ECOM_SCHEMA.CATEGORYWISEREVIEWSCORE").mode("overwrite").save()

        logger.info("Spark Job for Business tables Done.....")
        spark.stop()
        logger.info("Stopped Spark session.")
    except Exception as e:
        logger.error(f"Error in PySparkBusinessJob: {e}", exc_info=True)

if __name__ == "__main__":
    main()