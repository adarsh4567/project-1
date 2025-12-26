import pytest
from pyspark.sql import SparkSession
from delivery_job import (
    join_orders_items,
    select_delivery_columns,
    add_delivery_dates,
    add_delivery_metrics,
    finalize_delivery_table
)

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("pytest-pyspark-delivery").getOrCreate()
    yield spark
    spark.stop()

def test_join_orders_items(spark):
    orders = [
        ("o1", "c1", "2023-01-01 10:00:00", "2023-01-05", "2023-01-07", "p1", "s1"),
    ]
    orders_schema = ["order_id", "customer_id", "order_purchase_timestamp", "order_delivered_customer_date", "order_estimated_delivery_date", "product_id", "seller_id"]
    df_orders = spark.createDataFrame(orders, orders_schema)
    order_items = [
        ("o1", 1, "p1", "2023-01-07", 10.0, "s1"),
    ]
    items_schema = ["order_id", "order_item_id", "product_id", "shipping_limit_date", "freight_value", "seller_id"]
    df_items = spark.createDataFrame(order_items, items_schema)
    result = join_orders_items(df_orders, df_items)
    assert "order_id" in result.columns
    assert result.filter(result.order_id == "o1").count() == 1

def test_select_delivery_columns(spark):
    joined = [
        ("o1", "c1", "2023-01-01 10:00:00", "2023-01-05", "2023-01-07", "p1", "s1"),
    ]
    schema = ["order_id", "customer_id", "order_purchase_timestamp", "order_delivered_customer_date", "order_estimated_delivery_date", "product_id", "seller_id"]
    df_joined = spark.createDataFrame(joined, schema)
    result = select_delivery_columns(df_joined)
    assert "order_id" in result.columns
    assert "order_purchase_date" in result.columns
    assert "delivered_date" in result.columns
    assert "expected_delivery_date" in result.columns

def test_add_delivery_dates(spark):
    selected = [
        ("o1", "c1", "2023-01-01", "2023-01-05", "2023-01-07", "p1", "s1"),
    ]
    schema = ["order_id", "customer_id", "order_purchase_date", "delivered_date", "expected_delivery_date", "product_id", "seller_id"]
    df_selected = spark.createDataFrame(selected, schema)
    result = add_delivery_dates(df_selected)
    assert "delivered_date" in result.columns
    assert "expected_delivery_date" in result.columns
    assert "order_purchase_date" in result.columns

def test_add_delivery_metrics(spark):
    dates = [
        ("o1", "c1", "2023-01-01", "2023-01-05", "2023-01-03", "p1", "s1"),
    ]
    schema = ["order_id", "customer_id", "order_purchase_date", "delivered_date", "expected_delivery_date", "product_id", "seller_id"]
    df_dates = spark.createDataFrame(dates, schema)
    df_dates = add_delivery_dates(df_dates)
    result = add_delivery_metrics(df_dates)
    assert "delivery_time_days" in result.columns
    assert "delivery_delay_days" in result.columns
    # Check values
    row = result.collect()[0]
    assert row.delivery_time_days == (row.delivered_date - row.order_purchase_date).days
    assert row.delivery_delay_days == (row.delivered_date - row.expected_delivery_date).days

def test_finalize_delivery_table(spark):
    metrics = [
        ("o1", "c1", "2023-01-01", "2023-01-05", "2023-01-03", "p1", "s1", 4, 2),
    ]
    schema = ["order_id", "customer_id", "order_purchase_date", "delivered_date", "expected_delivery_date", "product_id", "seller_id", "delivery_time_days", "delivery_delay_days"]
    df_metrics = spark.createDataFrame(metrics, schema)
    result = finalize_delivery_table(df_metrics)
    assert "delivery_time_days" in result.columns
    assert "delivery_delay_days" in result.columns
    assert "order_purchase_date" not in result.columns
    assert "delivered_date" not in result.columns
    assert "expected_delivery_date" not in result.columns
