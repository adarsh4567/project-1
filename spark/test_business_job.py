import pytest
from pyspark.sql import SparkSession
from business_job import (
    get_statewise_total,
    get_statewise_payments,
    get_categorywise_review_score
)

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("pytest-pyspark-business").getOrCreate()
    yield spark
    spark.stop()

def test_get_statewise_total(spark):
    customers = [
        ("c1", "CA"),
        ("c2", "CA"),
        ("c3", "NY"),
    ]
    schema = ["customer_id", "customer_state"]
    df_customers = spark.createDataFrame(customers, schema)
    result = get_statewise_total(df_customers)
    assert "total_customers" in result.columns
    assert result.filter(result.customer_state == "CA").collect()[0]["total_customers"] == 2
    assert result.filter(result.customer_state == "NY").collect()[0]["total_customers"] == 1

def test_get_statewise_payments(spark):
    customers = [
        ("c1", "CA"),
        ("c2", "CA"),
        ("c3", "NY"),
    ]
    orders = [
        ("o1", "c1"),
        ("o2", "c2"),
        ("o3", "c3"),
    ]
    payments = [
        ("o1", "credit_card"),
        ("o2", "debit_card"),
        ("o3", "credit_card"),
    ]
    customers_schema = ["customer_id", "customer_state"]
    orders_schema = ["order_id", "customer_id"]
    payments_schema = ["order_id", "payment_type"]
    df_customers = spark.createDataFrame(customers, customers_schema)
    df_orders = spark.createDataFrame(orders, orders_schema)
    df_payments = spark.createDataFrame(payments, payments_schema)
    result = get_statewise_payments(df_customers, df_orders, df_payments)
    assert "customer_state" in result.columns
    assert "payment_type" in result.columns
    assert set(result.select("payment_type").rdd.flatMap(lambda x: x).collect()) == {"credit_card", "debit_card"}

def test_get_categorywise_review_score(spark):
    order_items = [
        ("o1", "p1"),
        ("o2", "p2"),
    ]
    products = [
        ("p1", "cat1"),
        ("p2", "cat2"),
    ]
    reviews = [
        ("o1", 5),
        ("o2", 3),
    ]
    order_items_schema = ["order_id", "product_id"]
    products_schema = ["product_id", "product_category_name"]
    reviews_schema = ["order_id", "review_score"]
    df_order_items = spark.createDataFrame(order_items, order_items_schema)
    df_products = spark.createDataFrame(products, products_schema)
    df_reviews = spark.createDataFrame(reviews, reviews_schema)
    result = get_categorywise_review_score(df_order_items, df_products, df_reviews)
    assert "product_category_name" in result.columns
    assert "score" in result.columns
    scores = dict((row["product_category_name"], row["score"]) for row in result.collect())
    assert scores["cat1"] == 1
    assert scores["cat2"] == 1
