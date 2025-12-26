
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff
import os
import logging

LOG_FILE = "/delivery_log/delivery_log.txt"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
  filename=LOG_FILE,
  filemode="a",
  level=logging.INFO,
  format="%(asctime)s %(levelname)s %(name)s - %(message)s",
  datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("PySparkDeliveryJob")

def join_orders_items(df_orders, df_order_items):
  logger.info("Joining orders and order items DataFrames.")
  return df_orders.join(df_order_items, on="order_id", how="inner")

def select_delivery_columns(df_joined_orders):
  logger.info("Selecting delivery columns.")
  return df_joined_orders.select(
    col("order_id"),
    col("customer_id"),
    col("order_purchase_timestamp").alias("order_purchase_date"),
    col("order_delivered_customer_date").alias("delivered_date"),
    col("order_estimated_delivery_date").alias("expected_delivery_date"),
    col("product_id"),
    col("seller_id")
  )

def add_delivery_dates(df_selected_orders):
  logger.info("Converting delivery date columns to date type.")
  return df_selected_orders.withColumn("delivered_date", to_date("delivered_date", "yyyy-MM-dd")) \
    .withColumn("expected_delivery_date", to_date("expected_delivery_date", "yyyy-MM-dd")) \
    .withColumn("order_purchase_date", to_date("order_purchase_date", "yyyy-MM-dd"))

def add_delivery_metrics(df_deliver_with_dates):
  logger.info("Adding delivery metrics (time and delay days).")
  return df_deliver_with_dates.withColumn(
    "delivery_time_days", datediff(df_deliver_with_dates["delivered_date"], df_deliver_with_dates["order_purchase_date"])
  ).withColumn(
    "delivery_delay_days", datediff(df_deliver_with_dates["delivered_date"], df_deliver_with_dates["expected_delivery_date"])
  )

def finalize_delivery_table(df_delivery_transformed):
  logger.info("Finalizing delivery table by dropping date columns.")
  return df_delivery_transformed.drop("order_purchase_date", "delivered_date", "expected_delivery_date")

def main():
  try:
    logger.info("Starting PySparkDeliveryJob ETL process.")
    spark = SparkSession.builder.appName("PySparkDeliveryJob").getOrCreate()

    logger.info("Reading orders dataset from S3.")
    df_orders = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_orders_dataset.csv", header=True, inferSchema=True)
    logger.info("Reading order items dataset from S3.")
    df_order_items = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_order_items_dataset.csv", header=True, inferSchema=True)

    logger.info("Joining orders and items.")
    df_joined_orders = join_orders_items(df_orders, df_order_items)
    logger.info("Selecting delivery columns.")
    df_selected_orders = select_delivery_columns(df_joined_orders)
    logger.info("Converting delivery date columns to date type.")
    df_deliver_with_dates = add_delivery_dates(df_selected_orders)
    logger.info("Adding delivery metrics.")
    df_delivery_transformed = add_delivery_metrics(df_deliver_with_dates)
    logger.info("Finalizing delivery table.")
    df_delivery_final = finalize_delivery_table(df_delivery_transformed)

    sfOptions = {
      "sfURL": "NAHZTMI-HZ73012.snowflakecomputing.com",
      "sfUser": "AD07",
      "sfPassword": "Adarsh06572307011",
      "sfDatabase": "ECOM_DB",
      "sfSchema": "ECOM_SCHEMA",
      "sfWarehouse": "ECOM_WH",
    }

    logger.info("Writing delivery DataFrame to Snowflake.")
    df_delivery_final.write.format("net.snowflake.spark.snowflake") \
      .options(**sfOptions) \
      .option("dbtable", "ECOM_SCHEMA.DELIVERY") \
      .mode("overwrite") \
      .save()

    logger.info("Spark Job for Delivery table Done.....")
    spark.stop()
    logger.info("Stopped Spark session.")
  except Exception as e:
    logger.error(f"Error in PySparkDeliveryJob: {e}", exc_info=True)

if __name__ == "__main__":
  main()