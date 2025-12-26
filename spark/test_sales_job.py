import pytest
from pyspark.sql import SparkSession
from sales_job import (
    join_orders_and_items,
    # add_product_category,
    # join_with_customers,
    # join_with_geolocation,
    # join_with_sellers
)

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("pytest-pyspark-sales").getOrCreate()
    yield spark
    spark.stop()


def test_join_orders_and_items(spark):
    orders = [
        ("o1", "delivered", "2023-01-01", "2023-01-02", "2023-01-03", "cust1"),
    ]
    orders_schema = ["order_id","order_status","order_approved_at","order_delivered_carrier_date","order_date","customer_id"]
    df_orders = spark.createDataFrame(orders, orders_schema)

    order_items = [
        ("o1", 1, "p1", "2023-01-05", 10.0),
    ]
    items_schema = ["order_id","order_item_id","product_id","shipping_limit_date","freight_value"]
    df_items = spark.createDataFrame(order_items, items_schema)

    result = join_orders_and_items(df_orders, df_items)
    assert "order_status" not in result.columns
    assert result.filter(result.order_id == "o1").count() == 1

def test_add_product_category(spark):
    sales = [
        ("o1", "p1"),
    ]
    sales_schema = ["order_id", "product_id"]
    df_sales = spark.createDataFrame(sales, sales_schema)

    products = [
        ("p1", "cat1", 2, 10, 20, 30, 40, 50, 60),  # Added dummy value for product_width_cm
    ]
    products_schema = [
        "product_id", "product_category_name", "product_photos_qty",
        "product_name_lenght", "product_description_lenght", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm"
    ]
    df_products = spark.createDataFrame(products, products_schema)

    from sales_job import add_product_category
    result = add_product_category(df_sales, df_products)
    assert "product_category" in result.columns
    assert "product_id" not in result.columns
    assert "unit_sold" in result.columns

def test_format_date_and_clean(spark):
    sales = [
        ("o1", "2023-01-01 12:00:00", "2023-01-02", "2023-01-03"),
    ]
    sales_schema = ["order_id", "order_purchase_timestamp", "order_delivered_customer_date", "order_estimated_delivery_date"]
    df_sales = spark.createDataFrame(sales, sales_schema)

    from sales_job import format_date_and_clean
    result = format_date_and_clean(df_sales)
    assert "date" in result.columns
    assert "order_purchase_timestamp" not in result.columns

def test_add_revenue_and_customer_data(spark):
    sales = [
        ("o1", "cust1", 100.0),
    ]
    sales_schema = ["sales_id", "customer_id", "price"]
    df_sales = spark.createDataFrame(sales, sales_schema)

    payments = [
        ("o1", 1, "credit_card", 1, 100.0),
    ]
    payments_schema = ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]
    df_payments = spark.createDataFrame(payments, payments_schema)

    customers = [
        ("cust1", "unique1", "zip1", "city1", "state1"),
    ]
    customers_schema = ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]
    df_customers = spark.createDataFrame(customers, customers_schema)

    from sales_job import add_revenue_and_customer_data
    result = add_revenue_and_customer_data(df_sales, df_payments, df_customers)
    assert "revenue" in result.columns
    assert "city" in result.columns
    assert "state" in result.columns
    assert "customer_unique_id" not in result.columns


