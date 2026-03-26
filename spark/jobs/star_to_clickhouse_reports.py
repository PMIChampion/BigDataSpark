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
    SparkSession.builder.appName("snowflake-to-clickhouse-reports")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

fact = spark.read.jdbc(POSTGRES_JDBC_URL, "fact_sales", properties=POSTGRES_PROPERTIES)
dim_customer = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_customer", properties=POSTGRES_PROPERTIES)
dim_store = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_store", properties=POSTGRES_PROPERTIES)
dim_supplier = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_supplier", properties=POSTGRES_PROPERTIES)
dim_product = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_product", properties=POSTGRES_PROPERTIES)
dim_date = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_date", properties=POSTGRES_PROPERTIES)
dim_month = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_month", properties=POSTGRES_PROPERTIES)
dim_quarter = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_quarter", properties=POSTGRES_PROPERTIES)
dim_year = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_year", properties=POSTGRES_PROPERTIES)
dim_country = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_country", properties=POSTGRES_PROPERTIES)
dim_city = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_city", properties=POSTGRES_PROPERTIES)
dim_address = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_address", properties=POSTGRES_PROPERTIES)
dim_customer_pet = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_customer_pet", properties=POSTGRES_PROPERTIES)
dim_pet_type = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_pet_type", properties=POSTGRES_PROPERTIES)
dim_pet_breed = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_pet_breed", properties=POSTGRES_PROPERTIES)
dim_pet_category = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_pet_category", properties=POSTGRES_PROPERTIES)
dim_product_category = spark.read.jdbc(POSTGRES_JDBC_URL, "dim_product_category", properties=POSTGRES_PROPERTIES)

customer_country = dim_country.select(F.col("country_id").alias("customer_country_id"), F.col("country_name").alias("customer_country"))
store_country = dim_country.select(F.col("country_id").alias("store_country_id"), F.col("country_name").alias("store_country"))
supplier_country = dim_country.select(F.col("country_id").alias("supplier_country_id"), F.col("country_name").alias("supplier_country"))

customer_pet_dim = (
    dim_customer_pet.join(dim_pet_type, "pet_type_id")
    .join(dim_pet_breed, "pet_breed_id")
    .join(dim_pet_category, "pet_category_id")
    .select(
        "customer_pet_id",
        "pet_name",
        "pet_type_name",
        "pet_breed_name",
        F.col("pet_category_name").alias("customer_pet_category"),
    )
)

customer_dim = (
    dim_customer.join(customer_country, dim_customer.country_id == customer_country.customer_country_id, "left")
    .join(customer_pet_dim, "customer_pet_id", "left")
    .select(
        "customer_id",
        F.col("first_name").alias("customer_first_name"),
        F.col("last_name").alias("customer_last_name"),
        F.col("email").alias("customer_email"),
        "customer_country",
        "pet_name",
        "pet_type_name",
        "pet_breed_name",
        "customer_pet_category",
    )
)

store_dim = (
    dim_store.join(dim_address, "address_id", "left")
    .join(dim_city, "city_id", "left")
    .join(store_country, dim_city.country_id == store_country.store_country_id, "left")
    .select(
        "store_id",
        "store_name",
        F.col("city_name").alias("city"),
        "store_country",
        F.col("address_line").alias("store_address"),
    )
)

supplier_dim = (
    dim_supplier.join(dim_address, "address_id", "left")
    .join(dim_city, "city_id", "left")
    .join(supplier_country, dim_city.country_id == supplier_country.supplier_country_id, "left")
    .select(
        "supplier_id",
        "supplier_name",
        "supplier_country",
        F.col("city_name").alias("supplier_city"),
    )
)

product_dim = (
    dim_product.join(dim_product_category, "product_category_id", "left")
    .join(dim_pet_category, "pet_category_id", "left")
    .select(
        "product_id",
        "supplier_id",
        "product_name",
        F.col("product_category_name").alias("product_category"),
        F.col("pet_category_name").alias("pet_category"),
        F.col("current_price").alias("price"),
        "rating",
        "reviews_count",
    )
)

date_dim = (
    dim_date.join(dim_month, "month_id", "left")
    .join(dim_quarter, "quarter_id", "left")
    .join(dim_year, "year_id", "left")
    .select(
        F.col("date_id").alias("sale_date_id"),
        "year_number",
        "quarter_number",
        "month_number",
        "month_name",
        "full_date",
    )
)

sales = (
    fact.join(customer_dim, "customer_id")
    .join(store_dim, "store_id")
    .join(product_dim, "product_id")
    .join(supplier_dim, product_dim.supplier_id == supplier_dim.supplier_id, "inner")
    .drop(supplier_dim.supplier_id)
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
    customer_dim.groupBy("customer_country")
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

correlation_value = quality_base.select(
    F.corr("rating", "total_quantity_sold").alias("rating_sales_correlation")
).first()["rating_sales_correlation"]

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
