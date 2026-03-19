from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from common import POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, table_writer, truncate_postgres_tables


def with_surrogate_id(df, key_column, id_column):
    window = Window.orderBy(key_column)
    return df.withColumn(id_column, F.row_number().over(window).cast("long"))


spark = (
    SparkSession.builder.appName("raw-to-star")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

raw = spark.read.jdbc(
    url=POSTGRES_JDBC_URL,
    table="mock_data",
    properties=POSTGRES_PROPERTIES,
)

typed = (
    raw.withColumn("source_row_id", F.col("id").cast("int"))
    .withColumn("customer_age_int", F.col("customer_age").cast("int"))
    .withColumn("product_price_num", F.col("product_price").cast("decimal(10,2)"))
    .withColumn("product_quantity_int", F.col("product_quantity").cast("int"))
    .withColumn("sale_quantity_int", F.col("sale_quantity").cast("int"))
    .withColumn("sale_total_price_num", F.col("sale_total_price").cast("decimal(10,2)"))
    .withColumn("product_weight_num", F.col("product_weight").cast("decimal(10,2)"))
    .withColumn("product_rating_num", F.col("product_rating").cast("decimal(3,1)"))
    .withColumn("product_reviews_int", F.col("product_reviews").cast("int"))
    .withColumn("sale_customer_source_id", F.col("sale_customer_id").cast("int"))
    .withColumn("sale_seller_source_id", F.col("sale_seller_id").cast("int"))
    .withColumn("sale_product_source_id", F.col("sale_product_id").cast("int"))
    .withColumn("sale_date_parsed", F.to_date("sale_date", "M/d/yyyy"))
    .withColumn("product_release_date_parsed", F.to_date("product_release_date", "M/d/yyyy"))
    .withColumn("product_expiry_date_parsed", F.to_date("product_expiry_date", "M/d/yyyy"))
    .withColumn("customer_postal_code", F.when(F.trim(F.col("customer_postal_code")) == "", None).otherwise(F.col("customer_postal_code")))
    .withColumn("seller_postal_code", F.when(F.trim(F.col("seller_postal_code")) == "", None).otherwise(F.col("seller_postal_code")))
    .withColumn("store_state", F.when(F.trim(F.col("store_state")) == "", None).otherwise(F.col("store_state")))
    .withColumn(
        "product_nk",
        F.sha2(
            F.concat_ws(
                "||",
                "product_name",
                "product_category",
                "pet_category",
                F.col("product_price").cast("string"),
                F.col("product_quantity").cast("string"),
                F.col("product_weight").cast("string"),
                "product_color",
                "product_size",
                "product_brand",
                "product_material",
                "product_description",
                F.col("product_rating").cast("string"),
                F.col("product_reviews").cast("string"),
                "product_release_date",
                "product_expiry_date",
            ),
            256,
        ),
    )
)

dim_customer = with_surrogate_id(
    typed.select(
        F.col("customer_email").alias("customer_nk"),
        F.col("customer_first_name").alias("first_name"),
        F.col("customer_last_name").alias("last_name"),
        F.col("customer_age_int").alias("age"),
        F.col("customer_email").alias("email"),
        F.col("customer_country").alias("country"),
        F.col("customer_postal_code").alias("postal_code"),
        F.col("customer_pet_type").alias("pet_type"),
        F.col("customer_pet_name").alias("pet_name"),
        F.col("customer_pet_breed").alias("pet_breed"),
    ).dropDuplicates(["customer_nk"]),
    "customer_nk",
    "customer_id",
).select(
    "customer_id",
    "customer_nk",
    "first_name",
    "last_name",
    "age",
    "email",
    "country",
    "postal_code",
    "pet_type",
    "pet_name",
    "pet_breed",
)

dim_seller = with_surrogate_id(
    typed.select(
        F.col("seller_email").alias("seller_nk"),
        F.col("seller_first_name").alias("first_name"),
        F.col("seller_last_name").alias("last_name"),
        F.col("seller_email").alias("email"),
        F.col("seller_country").alias("country"),
        F.col("seller_postal_code").alias("postal_code"),
    ).dropDuplicates(["seller_nk"]),
    "seller_nk",
    "seller_id",
).select("seller_id", "seller_nk", "first_name", "last_name", "email", "country", "postal_code")

dim_store = with_surrogate_id(
    typed.select(
        F.col("store_email").alias("store_nk"),
        "store_name",
        "store_location",
        F.col("store_city").alias("city"),
        F.col("store_state").alias("state"),
        F.col("store_country").alias("country"),
        F.col("store_phone").alias("phone"),
        F.col("store_email").alias("email"),
    ).dropDuplicates(["store_nk"]),
    "store_nk",
    "store_id",
).select("store_id", "store_nk", "store_name", "store_location", "city", "state", "country", "phone", "email")

dim_supplier = with_surrogate_id(
    typed.select(
        F.col("supplier_email").alias("supplier_nk"),
        "supplier_name",
        F.col("supplier_contact").alias("contact_name"),
        F.col("supplier_email").alias("email"),
        F.col("supplier_phone").alias("phone"),
        F.col("supplier_address").alias("address"),
        F.col("supplier_city").alias("city"),
        F.col("supplier_country").alias("country"),
    ).dropDuplicates(["supplier_nk"]),
    "supplier_nk",
    "supplier_id",
).select("supplier_id", "supplier_nk", "supplier_name", "contact_name", "email", "phone", "address", "city", "country")

dim_product = with_surrogate_id(
    typed.select(
        "product_nk",
        "product_name",
        "product_category",
        "pet_category",
        F.col("product_price_num").alias("price"),
        F.col("product_quantity_int").alias("stock_quantity"),
        F.col("product_weight_num").alias("weight"),
        F.col("product_color").alias("color"),
        F.col("product_size").alias("size"),
        F.col("product_brand").alias("brand"),
        F.col("product_material").alias("material"),
        F.col("product_description").alias("description"),
        F.col("product_rating_num").alias("rating"),
        F.col("product_reviews_int").alias("reviews_count"),
        F.col("product_release_date_parsed").alias("release_date"),
        F.col("product_expiry_date_parsed").alias("expiry_date"),
    ).dropDuplicates(["product_nk"]),
    "product_nk",
    "product_id",
).select(
    "product_id",
    "product_nk",
    "product_name",
    "product_category",
    "pet_category",
    "price",
    "stock_quantity",
    "weight",
    "color",
    "size",
    "brand",
    "material",
    "description",
    "rating",
    "reviews_count",
    "release_date",
    "expiry_date",
)

dim_date = (
    typed.select("sale_date_parsed")
    .dropDuplicates(["sale_date_parsed"])
    .withColumn("date_id", F.date_format("sale_date_parsed", "yyyyMMdd").cast("int"))
    .withColumn("day_of_month", F.dayofmonth("sale_date_parsed"))
    .withColumn("month_number", F.month("sale_date_parsed"))
    .withColumn("month_name", F.date_format("sale_date_parsed", "MMMM"))
    .withColumn("quarter_number", F.quarter("sale_date_parsed"))
    .withColumn("year_number", F.year("sale_date_parsed"))
    .select(
        "date_id",
        F.col("sale_date_parsed").alias("full_date"),
        "day_of_month",
        "month_number",
        "month_name",
        "quarter_number",
        "year_number",
    )
)

fact_sales = (
    typed.join(dim_customer.select("customer_id", "customer_nk"), typed.customer_email == dim_customer.customer_nk, "inner")
    .join(dim_seller.select("seller_id", "seller_nk"), typed.seller_email == dim_seller.seller_nk, "inner")
    .join(dim_store.select("store_id", "store_nk"), typed.store_email == dim_store.store_nk, "inner")
    .join(dim_supplier.select("supplier_id", "supplier_nk"), typed.supplier_email == dim_supplier.supplier_nk, "inner")
    .join(dim_product.select("product_id", "product_nk"), "product_nk", "inner")
    .join(dim_date.select("date_id", "full_date"), typed.sale_date_parsed == dim_date.full_date, "inner")
    .select(
        F.row_number().over(Window.orderBy("mock_data_key")).cast("long").alias("sale_id"),
        "mock_data_key",
        "source_row_id",
        F.col("date_id").alias("sale_date_id"),
        "customer_id",
        "seller_id",
        "store_id",
        "supplier_id",
        "product_id",
        F.col("sale_customer_source_id").alias("source_customer_id"),
        F.col("sale_seller_source_id").alias("source_seller_id"),
        F.col("sale_product_source_id").alias("source_product_id"),
        F.col("sale_quantity_int").alias("sale_quantity"),
        F.col("sale_total_price_num").alias("sale_total_price"),
    )
)

truncate_postgres_tables(
    [
        "fact_sales",
        "dim_date",
        "dim_product",
        "dim_supplier",
        "dim_store",
        "dim_seller",
        "dim_customer",
    ]
)

table_writer(dim_customer, "dim_customer", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_seller, "dim_seller", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_store, "dim_store", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_supplier, "dim_supplier", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_product, "dim_product", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_date, "dim_date", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(fact_sales, "fact_sales", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")

spark.stop()
