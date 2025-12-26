from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit
from pyspark.sql.functions import broadcast
import os
import logging

LOG_FILE = "/data/sales_log.txt"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("PySparkSalesJob")

def join_orders_and_items(df_orders, df_order_items):
    """Joins orders with order items and drops unnecessary columns."""
    logger.info("Joining orders and order items DataFrames.")

    drop_orders_column = ["order_status", "order_approved_at", "order_delivered_carrier_date"]
    df_dropped_orders = df_orders.drop(*drop_orders_column)
    order_product_df = df_dropped_orders.join(df_order_items, on="order_id", how="left")
    drop_joined_column = ["order_item_id", "shipping_limit_date", "freight_value"]

    logger.info("Dropped unnecessary columns after join.")

    return order_product_df.drop(*drop_joined_column)

def add_product_category(df_sales, df_products):
    """Joins with products to add category and cleans up columns."""
    logger.info("Adding product category information.")

    df_sales_with_category = df_sales.join(df_products, on="product_id", how="left")
    drop_sales_column = ["product_id", "product_name_lenght", "product_description_lenght", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    df_dropped_sales = df_sales_with_category.drop(*drop_sales_column)

    logger.info("Dropped unnecessary product columns.")

    return df_dropped_sales.withColumnsRenamed({
        "order_id": "sales_id",
        "product_category_name": "product_category",
        "product_photos_qty": "unit_sold"
    })

def format_date_and_clean(df_sales):
    """Converts timestamp to date and drops old timestamp columns."""
    logger.info("Formatting date and cleaning timestamp columns.")

    df_sales_date_improved = df_sales.withColumn("date", to_date("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss"))
    drop_columns = ["order_purchase_timestamp", "order_delivered_customer_date", "order_estimated_delivery_date"]

    logger.info("Dropped old timestamp columns.")

    return df_sales_date_improved.drop(*drop_columns)

def add_revenue_and_customer_data(df_sales, df_payments, df_customers):
    """Joins with payments and customers to create the final sales table."""
    logger.info("Adding revenue and customer data.")

    df_sales_filtered = df_sales.drop("price", "unit_sold")
    df_joined_payments = df_sales_filtered.join(df_payments, df_sales_filtered["sales_id"] == df_payments["order_id"], "left")

    logger.info("Joined sales with payments.")

    df_sales_payment_filtered = df_joined_payments.drop("payment_sequential", "payment_type", "payment_installments", "order_id")
    df_sales_with_revenue = df_sales_payment_filtered.withColumnRenamed("payment_value", "revenue")

    logger.info("Renamed payment_value to revenue.")

    df_sales_customers_joined = df_sales_with_revenue.join(df_customers, on="customer_id", how="left")

    logger.info("Joined sales with customer data.")

    resulted_joined = df_sales_customers_joined.drop("customer_unique_id", "customer_zip_code_prefix")

    logger.info("Dropped unnecessary customer columns.")
    
    return resulted_joined.withColumnsRenamed({"customer_city": "city", "customer_state": "state"})

def main():
    """Main function to orchestrate the ETL job."""
    try:
        logger.info("Starting PySparkSalesJob ETL process.")
        spark = SparkSession.builder.appName("PySparkSalesJob").getOrCreate()
        # --- Data Ingestion (Reading from S3) ---
        input_path = "s3a://spark-app-storage-remake/raw_ecom_data"
        logger.info("Reading orders dataset from S3.")
        df_orders = spark.read.csv(f"{input_path}/olist_orders_dataset.csv", header=True, inferSchema=True)
        logger.info("Reading products dataset from S3.")
        df_products = spark.read.csv(f"{input_path}/olist_products_dataset.csv", header=True, inferSchema=True)
        logger.info("Reading customers dataset from S3.")
        df_customers = spark.read.csv(f"{input_path}/olist_customers_dataset.csv", header=True, inferSchema=True)
        logger.info("Reading order items dataset from S3.")
        df_order_items = spark.read.csv(f"{input_path}/olist_order_items_dataset.csv", header=True, inferSchema=True)
        logger.info("Reading order payments dataset from S3.")
        df_order_payments = spark.read.csv(f"{input_path}/olist_order_payments_dataset.csv", header=True, inferSchema=True)

        # --- Transformations ---
        logger.info("Starting DataFrame transformations.")
        sales_df_1 = join_orders_and_items(df_orders, df_order_items)
        sales_df_2 = add_product_category(sales_df_1, df_products)
        sales_df_3 = format_date_and_clean(sales_df_2)
        final_sales_df = add_revenue_and_customer_data(sales_df_3, df_order_payments, df_customers)
        logger.info("Completed all DataFrame transformations.")

        # --- Data Loading (Writing to Snowflake) ---
        logger.info("Writing final sales DataFrame to Snowflake.")
        sfOptions = {
          "sfURL": "JIXVRXK-WJ24136.snowflakecomputing.com",
          "sfUser": "AD01",
          "sfPassword": "Adarsh@06572307011",
          "sfDatabase": "ECOM_DB",
          "sfSchema": "ECOM_SCHEMA",
          "sfWarehouse": "ECOM_WH",
        }
        final_sales_df.write.format("net.snowflake.spark.snowflake") \
            .options(**sfOptions) \
            .option("dbtable", "ECOM_SCHEMA.SALES") \
            .mode("overwrite") \
            .save()
        logger.info("Successfully wrote data to Snowflake.")
        spark.stop()
        logger.info("Stopped Spark session.")
    except Exception as e:
        logger.error(f"Error in PySparkSalesJob: {e}", exc_info=True)

if __name__ == "__main__":
    main()
