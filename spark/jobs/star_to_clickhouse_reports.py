from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from common import (
    CLICKHOUSE_JDBC_URL,
    CLICKHOUSE_PROPERTIES,
    POSTGRES_JDBC_URL,
    POSTGRES_PROPERTIES,
    table_writer,
)


spark = (
    SparkSession.builder.appName("star-to-clickhouse-reports")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

fact = spark.read.jdbc(POSTGRES_JDBC_URL, "fact_sales", properties=POSTGRES_PROPERTIES)
dim_customer = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_customer", properties=POSTGRES_PROPERTIES)
dim_store = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_store", properties=POSTGRES_PROPERTIES)
dim_supplier = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_supplier", properties=POSTGRES_PROPERTIES)
dim_product = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_product", properties=POSTGRES_PROPERTIES)
dim_date = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_date", properties=POSTGRES_PROPERTIES)

customer_dim = dim_customer.select(
    "customer_id",
    F.col("first_name").alias("customer_first_name"),
    F.col("last_name").alias("customer_last_name"),
    F.col("email").alias("customer_email"),
    F.col("country").alias("customer_country"),
)
store_dim = dim_store.select(
    "store_id",
    "store_name",
    "city",
    F.col("country").alias("store_country"),
)
supplier_dim = dim_supplier.select(
    "supplier_id",
    "supplier_name",
    F.col("country").alias("supplier_country"),
)
product_dim = dim_product.select(
    "product_id",
    "product_name",
    "product_category",
    "price",
    "rating",
    "reviews_count",
)
date_dim = dim_date.select(
    F.col("date_id").alias("sale_date_id"),
    "year_number",
    "quarter_number",
    "month_number",
    "month_name",
)

sales = (
    fact.join(customer_dim, "customer_id")
    .join(store_dim, "store_id")
    .join(supplier_dim, "supplier_id")
    .join(product_dim, "product_id")
    .join(date_dim, "sale_date_id")
)

product_window = Window.orderBy(F.desc("total_quantity_sold"), F.desc("total_revenue"))
customer_window = Window.orderBy(F.desc("total_spent"))
store_window = Window.orderBy(F.desc("total_revenue"))
supplier_window = Window.orderBy(F.desc("total_revenue"))
quality_desc_window = Window.orderBy(F.desc("rating"), F.desc("reviews_count"))
quality_asc_window = Window.orderBy(F.asc("rating"), F.desc("reviews_count"))
time_window = Window.orderBy("year_number", "month_number")

country_customer_distribution = (
    dim_customer.groupBy(F.col("country").alias("customer_country"))
    .agg(F.count("*").alias("customers_in_country"))
)

product_sales_report = (
    sales.groupBy("product_id", "product_name", "product_category")
    .agg(
        F.sum("sale_quantity").alias("total_quantity_sold"),
        F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.max("reviews_count").alias("reviews_count"),
    )
    .withColumn("sales_rank", F.row_number().over(product_window))
    .orderBy("sales_rank")
)

customer_sales_report = (
    sales.groupBy("customer_id", "customer_first_name", "customer_last_name", "customer_email", "customer_country")
    .agg(
        F.round(F.sum("sale_total_price"), 2).alias("total_spent"),
        F.count("*").alias("orders_count"),
        F.round(F.avg("sale_total_price"), 2).alias("avg_order_value"),
    )
    .join(country_customer_distribution, "customer_country", "left")
    .withColumn("spending_rank", F.row_number().over(customer_window))
    .orderBy("spending_rank")
)

time_sales_report = (
    sales.groupBy("year_number", "quarter_number", "month_number", "month_name")
    .agg(
        F.count("*").alias("orders_count"),
        F.sum("sale_quantity").alias("total_quantity_sold"),
        F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        F.round(F.avg("sale_total_price"), 2).alias("avg_order_value"),
    )
    .withColumn("previous_period_revenue", F.lag("total_revenue").over(time_window))
    .withColumn(
        "revenue_change_pct",
        F.when(
            F.col("previous_period_revenue").isNull() | (F.col("previous_period_revenue") == 0),
            F.lit(0.0),
        ).otherwise(
            F.round(
                ((F.col("total_revenue") - F.col("previous_period_revenue")) / F.col("previous_period_revenue")) * 100,
                2,
            )
        ),
    )
    .withColumn("previous_period_revenue", F.coalesce(F.col("previous_period_revenue"), F.lit(0.0)))
    .orderBy("year_number", "month_number")
)

store_sales_report = (
    sales.groupBy("store_id", "store_name", "city", "store_country")
    .agg(
        F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        F.count("*").alias("orders_count"),
        F.round(F.avg("sale_total_price"), 2).alias("avg_order_value"),
    )
    .withColumn("revenue_rank", F.row_number().over(store_window))
    .orderBy("revenue_rank")
)

supplier_sales_report = (
    sales.groupBy("supplier_id", "supplier_name", "supplier_country")
    .agg(
        F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        F.round(F.avg("price"), 2).alias("avg_product_price"),
        F.count("*").alias("orders_count"),
    )
    .withColumn("revenue_rank", F.row_number().over(supplier_window))
    .orderBy("revenue_rank")
)

quality_base = (
    sales.groupBy("product_id", "product_name", "product_category")
    .agg(
        F.max("rating").alias("rating"),
        F.max("reviews_count").alias("reviews_count"),
        F.sum("sale_quantity").alias("total_quantity_sold"),
        F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
    )
)

rating_sales_corr = quality_base.select(F.corr("rating", "total_quantity_sold").alias("rating_sales_correlation")).first()
correlation_value = rating_sales_corr["rating_sales_correlation"]

product_quality_report = (
    quality_base.withColumn("highest_rating_rank", F.dense_rank().over(quality_desc_window))
    .withColumn("lowest_rating_rank", F.dense_rank().over(quality_asc_window))
    .withColumn("rating_sales_correlation", F.lit(correlation_value))
    .orderBy(F.desc("rating"), F.desc("reviews_count"))
)

merge_tree = "ENGINE = MergeTree ORDER BY tuple()"
table_writer(product_sales_report, "reports.product_sales_report", CLICKHOUSE_JDBC_URL, CLICKHOUSE_PROPERTIES, create_table_options=merge_tree)
table_writer(customer_sales_report, "reports.customer_sales_report", CLICKHOUSE_JDBC_URL, CLICKHOUSE_PROPERTIES, create_table_options=merge_tree)
table_writer(time_sales_report, "reports.time_sales_report", CLICKHOUSE_JDBC_URL, CLICKHOUSE_PROPERTIES, create_table_options=merge_tree)
table_writer(store_sales_report, "reports.store_sales_report", CLICKHOUSE_JDBC_URL, CLICKHOUSE_PROPERTIES, create_table_options=merge_tree)
table_writer(supplier_sales_report, "reports.supplier_sales_report", CLICKHOUSE_JDBC_URL, CLICKHOUSE_PROPERTIES, create_table_options=merge_tree)
table_writer(product_quality_report, "reports.product_quality_report", CLICKHOUSE_JDBC_URL, CLICKHOUSE_PROPERTIES, create_table_options=merge_tree)

spark.stop()
