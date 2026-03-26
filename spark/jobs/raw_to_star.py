from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from common import POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, table_writer, truncate_postgres_tables


def with_surrogate_id(df, key_column, id_column):
    window = Window.orderBy(key_column)
    return df.withColumn(id_column, F.row_number().over(window).cast("long"))


spark = (
    SparkSession.builder.appName("raw-to-snowflake")
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
    .withColumn("customer_postal_code_clean", F.when(F.trim(F.col("customer_postal_code")) == "", None).otherwise(F.col("customer_postal_code")))
    .withColumn("seller_postal_code_clean", F.when(F.trim(F.col("seller_postal_code")) == "", None).otherwise(F.col("seller_postal_code")))
    .withColumn("store_state_clean", F.when(F.trim(F.col("store_state")) == "", None).otherwise(F.col("store_state")))
    .withColumn(
        "store_city_nk",
        F.concat_ws("||", "store_country", F.coalesce("store_state_clean", F.lit("NO_STATE")), "store_city"),
    )
    .withColumn(
        "supplier_city_nk",
        F.concat_ws("||", "supplier_country", F.lit("NO_STATE"), "supplier_city"),
    )
    .withColumn(
        "store_address_nk",
        F.concat_ws("||", "store_location", "store_city_nk"),
    )
    .withColumn(
        "supplier_address_nk",
        F.concat_ws("||", "supplier_address", "supplier_city_nk"),
    )
    .withColumn(
        "customer_pet_nk",
        F.concat_ws("||", "customer_pet_name", "customer_pet_type", "customer_pet_breed"),
    )
    .withColumn(
        "product_nk",
        F.concat_ws(
            "||",
            "product_name",
            "product_category",
            F.col("product_price").cast("string"),
            F.col("product_quantity").cast("string"),
            "pet_category",
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
            "supplier_email",
        ),
    )
)

country_values = (
    typed.select(F.col("customer_country").alias("country_name"))
    .unionByName(typed.select(F.col("seller_country").alias("country_name")))
    .unionByName(typed.select(F.col("store_country").alias("country_name")))
    .unionByName(typed.select(F.col("supplier_country").alias("country_name")))
    .where(F.col("country_name").isNotNull() & (F.col("country_name") != ""))
    .dropDuplicates(["country_name"])
)
dim_country = with_surrogate_id(country_values, "country_name", "country_id").select("country_id", "country_name")

store_country_ref = dim_country.select(F.col("country_name").alias("store_country"), F.col("country_id").alias("country_id"))
supplier_country_ref = dim_country.select(F.col("country_name").alias("supplier_country"), F.col("country_id").alias("country_id"))
customer_country_ref = dim_country.select(F.col("country_name").alias("customer_country"), F.col("country_id").alias("country_id"))
seller_country_ref = dim_country.select(F.col("country_name").alias("seller_country"), F.col("country_id").alias("country_id"))

dim_state = with_surrogate_id(
    typed.where(F.col("store_state_clean").isNotNull())
    .join(store_country_ref, "store_country")
    .select(
        F.concat_ws("||", "store_country", "store_state_clean").alias("state_nk"),
        F.col("store_state_clean").alias("state_name"),
        "country_id",
    )
    .dropDuplicates(["state_nk"]),
    "state_nk",
    "state_id",
).select("state_id", "state_nk", "state_name", "country_id")

state_ref = dim_state.select("state_id", "state_nk")

store_city_dim = (
    typed.join(store_country_ref, "store_country")
    .join(
        state_ref,
        F.concat_ws("||", typed.store_country, typed.store_state_clean) == state_ref.state_nk,
        "left",
    )
    .select(
        F.col("store_city_nk").alias("city_nk"),
        F.col("store_city").alias("city_name"),
        "state_id",
        "country_id",
    )
)
supplier_city_dim = (
    typed.join(supplier_country_ref, "supplier_country")
    .select(
        F.col("supplier_city_nk").alias("city_nk"),
        F.col("supplier_city").alias("city_name"),
        F.lit(None).cast("long").alias("state_id"),
        "country_id",
    )
)
dim_city = with_surrogate_id(
    store_city_dim.unionByName(supplier_city_dim).dropDuplicates(["city_nk"]),
    "city_nk",
    "city_id",
).select("city_id", "city_nk", "city_name", "state_id", "country_id")

city_ref = dim_city.select("city_id", "city_nk")

store_address_dim = (
    typed.join(city_ref, typed.store_city_nk == city_ref.city_nk, "inner")
    .select(
        F.col("store_address_nk").alias("address_nk"),
        F.col("store_location").alias("address_line"),
        "city_id",
    )
)
supplier_address_dim = (
    typed.join(city_ref, typed.supplier_city_nk == city_ref.city_nk, "inner")
    .select(
        F.col("supplier_address_nk").alias("address_nk"),
        F.col("supplier_address").alias("address_line"),
        "city_id",
    )
)
dim_address = with_surrogate_id(
    store_address_dim.unionByName(supplier_address_dim).dropDuplicates(["address_nk"]),
    "address_nk",
    "address_id",
).select("address_id", "address_nk", "address_line", "city_id")

dim_postal_code_customer = (
    typed.where(F.col("customer_postal_code_clean").isNotNull())
    .join(customer_country_ref, "customer_country")
    .select(
        F.concat_ws("||", "customer_country", "customer_postal_code_clean").alias("postal_code_nk"),
        F.col("customer_postal_code_clean").alias("postal_code"),
        "country_id",
    )
)
dim_postal_code_seller = (
    typed.where(F.col("seller_postal_code_clean").isNotNull())
    .join(seller_country_ref, "seller_country")
    .select(
        F.concat_ws("||", "seller_country", "seller_postal_code_clean").alias("postal_code_nk"),
        F.col("seller_postal_code_clean").alias("postal_code"),
        "country_id",
    )
)
dim_postal_code = with_surrogate_id(
    dim_postal_code_customer.unionByName(dim_postal_code_seller).dropDuplicates(["postal_code_nk"]),
    "postal_code_nk",
    "postal_code_id",
).select("postal_code_id", "postal_code_nk", "postal_code", "country_id")

pet_category_from_products = typed.select(F.col("pet_category").alias("pet_category_name"))
pet_category_from_types = typed.select(F.concat(F.initcap("customer_pet_type"), F.lit("s")).alias("pet_category_name"))
dim_pet_category = with_surrogate_id(
    pet_category_from_products.unionByName(pet_category_from_types).dropDuplicates(["pet_category_name"]),
    "pet_category_name",
    "pet_category_id",
).select("pet_category_id", "pet_category_name")

pet_category_ref = dim_pet_category.select("pet_category_id", "pet_category_name")
dim_pet_type = with_surrogate_id(
    typed.select(
        F.col("customer_pet_type").alias("pet_type_name"),
        F.concat(F.initcap("customer_pet_type"), F.lit("s")).alias("pet_category_name"),
    )
    .dropDuplicates(["pet_type_name"])
    .join(pet_category_ref, "pet_category_name")
    .select("pet_type_name", "pet_category_id"),
    "pet_type_name",
    "pet_type_id",
).select("pet_type_id", "pet_type_name", "pet_category_id")

dim_pet_breed = with_surrogate_id(
    typed.select(F.col("customer_pet_breed").alias("pet_breed_name")).dropDuplicates(["pet_breed_name"]),
    "pet_breed_name",
    "pet_breed_id",
).select("pet_breed_id", "pet_breed_name")

pet_type_ref = dim_pet_type.select("pet_type_id", "pet_type_name")
pet_breed_ref = dim_pet_breed.select("pet_breed_id", "pet_breed_name")
dim_customer_pet = with_surrogate_id(
    typed.select("customer_pet_nk", F.col("customer_pet_name").alias("pet_name"), F.col("customer_pet_type").alias("pet_type_name"), F.col("customer_pet_breed").alias("pet_breed_name"))
    .dropDuplicates(["customer_pet_nk"])
    .join(pet_type_ref, "pet_type_name")
    .join(pet_breed_ref, "pet_breed_name")
    .select("customer_pet_nk", "pet_name", "pet_type_id", "pet_breed_id"),
    "customer_pet_nk",
    "customer_pet_id",
).select("customer_pet_id", "customer_pet_nk", "pet_name", "pet_type_id", "pet_breed_id")

dim_product_category = with_surrogate_id(
    typed.select(F.col("product_category").alias("product_category_name")).dropDuplicates(["product_category_name"]),
    "product_category_name",
    "product_category_id",
).select("product_category_id", "product_category_name")

dim_product_brand = with_surrogate_id(
    typed.select(F.col("product_brand").alias("brand_name")).dropDuplicates(["brand_name"]),
    "brand_name",
    "brand_id",
).select("brand_id", "brand_name")

dim_product_material = with_surrogate_id(
    typed.select(F.col("product_material").alias("material_name")).dropDuplicates(["material_name"]),
    "material_name",
    "material_id",
).select("material_id", "material_name")

dim_product_color = with_surrogate_id(
    typed.select(F.col("product_color").alias("color_name")).dropDuplicates(["color_name"]),
    "color_name",
    "color_id",
).select("color_id", "color_name")

dim_product_size = with_surrogate_id(
    typed.select(F.col("product_size").alias("size_name")).dropDuplicates(["size_name"]),
    "size_name",
    "size_id",
).select("size_id", "size_name")

all_dates = (
    typed.select(F.col("sale_date_parsed").alias("full_date"))
    .unionByName(typed.select(F.col("product_release_date_parsed").alias("full_date")))
    .unionByName(typed.select(F.col("product_expiry_date_parsed").alias("full_date")))
    .dropDuplicates(["full_date"])
)
dim_year = with_surrogate_id(
    all_dates.select(F.year("full_date").alias("year_number")).dropDuplicates(["year_number"]),
    "year_number",
    "year_id",
).select("year_id", "year_number")

year_ref = dim_year.select("year_id", "year_number")
dim_quarter = with_surrogate_id(
    all_dates.select(
        F.concat(F.year("full_date").cast("string"), F.lit("-Q"), F.quarter("full_date").cast("string")).alias("quarter_nk"),
        F.quarter("full_date").alias("quarter_number"),
        F.year("full_date").alias("year_number"),
    )
    .dropDuplicates(["quarter_nk"])
    .join(year_ref, "year_number")
    .select("quarter_nk", "quarter_number", "year_id"),
    "quarter_nk",
    "quarter_id",
).select("quarter_id", "quarter_nk", "quarter_number", "year_id")

quarter_ref = dim_quarter.select("quarter_id", "quarter_nk")
dim_month = with_surrogate_id(
    all_dates.select(
        F.date_format("full_date", "yyyy-MM").alias("month_nk"),
        F.month("full_date").alias("month_number"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.concat(F.year("full_date").cast("string"), F.lit("-Q"), F.quarter("full_date").cast("string")).alias("quarter_nk"),
    )
    .dropDuplicates(["month_nk"])
    .join(quarter_ref, "quarter_nk")
    .select("month_nk", "month_number", "month_name", "quarter_id"),
    "month_nk",
    "month_id",
).select("month_id", "month_nk", "month_number", "month_name", "quarter_id")

month_ref = dim_month.select("month_id", "month_nk")
dim_date = (
    all_dates.withColumn("date_id", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("day_of_month", F.dayofmonth("full_date"))
    .withColumn("month_nk", F.date_format("full_date", "yyyy-MM"))
    .join(month_ref, "month_nk")
    .select("date_id", "full_date", "day_of_month", "month_id")
    .dropDuplicates(["date_id"])
)

address_ref = dim_address.select("address_id", "address_nk")
dim_supplier = with_surrogate_id(
    typed.select(
        F.col("supplier_email").alias("supplier_nk"),
        "supplier_name",
        F.col("supplier_contact").alias("supplier_contact"),
        F.col("supplier_email").alias("supplier_email"),
        F.col("supplier_phone").alias("supplier_phone"),
        F.col("supplier_address_nk").alias("address_nk"),
    )
    .dropDuplicates(["supplier_nk"])
    .join(address_ref, "address_nk")
    .select("supplier_nk", "supplier_name", "supplier_contact", "supplier_email", "supplier_phone", "address_id"),
    "supplier_nk",
    "supplier_id",
).select("supplier_id", "supplier_nk", "supplier_name", "supplier_contact", "supplier_email", "supplier_phone", "address_id")

postal_code_ref = dim_postal_code.select("postal_code_id", "postal_code_nk")
customer_pet_ref = dim_customer_pet.select("customer_pet_id", "customer_pet_nk")
dim_customer = with_surrogate_id(
    typed.select(
        F.col("customer_email").alias("customer_nk"),
        F.col("customer_first_name").alias("first_name"),
        F.col("customer_last_name").alias("last_name"),
        F.col("customer_age_int").alias("age"),
        F.col("customer_email").alias("email"),
        "customer_country",
        F.when(F.col("customer_postal_code_clean").isNotNull(), F.concat_ws("||", "customer_country", "customer_postal_code_clean")).alias("postal_code_nk"),
        "customer_pet_nk",
    )
    .dropDuplicates(["customer_nk"])
    .join(customer_country_ref, "customer_country")
    .join(postal_code_ref, "postal_code_nk", "left")
    .join(customer_pet_ref, "customer_pet_nk")
    .select("customer_nk", "first_name", "last_name", "age", "email", "country_id", "postal_code_id", "customer_pet_id"),
    "customer_nk",
    "customer_id",
).select("customer_id", "customer_nk", "first_name", "last_name", "age", "email", "country_id", "postal_code_id", "customer_pet_id")

dim_seller = with_surrogate_id(
    typed.select(
        F.col("seller_email").alias("seller_nk"),
        F.col("seller_first_name").alias("first_name"),
        F.col("seller_last_name").alias("last_name"),
        F.col("seller_email").alias("email"),
        "seller_country",
        F.when(F.col("seller_postal_code_clean").isNotNull(), F.concat_ws("||", "seller_country", "seller_postal_code_clean")).alias("postal_code_nk"),
    )
    .dropDuplicates(["seller_nk"])
    .join(seller_country_ref, "seller_country")
    .join(postal_code_ref, "postal_code_nk", "left")
    .select("seller_nk", "first_name", "last_name", "email", "country_id", "postal_code_id"),
    "seller_nk",
    "seller_id",
).select("seller_id", "seller_nk", "first_name", "last_name", "email", "country_id", "postal_code_id")

dim_store = with_surrogate_id(
    typed.select(
        F.col("store_email").alias("store_nk"),
        "store_name",
        F.col("store_phone").alias("phone"),
        F.col("store_email").alias("email"),
        F.col("store_address_nk").alias("address_nk"),
    )
    .dropDuplicates(["store_nk"])
    .join(address_ref, "address_nk")
    .select("store_nk", "store_name", "phone", "email", "address_id"),
    "store_nk",
    "store_id",
).select("store_id", "store_nk", "store_name", "phone", "email", "address_id")

product_category_ref = dim_product_category.select("product_category_id", "product_category_name")
pet_category_ref = dim_pet_category.select("pet_category_id", "pet_category_name")
color_ref = dim_product_color.select("color_id", "color_name")
size_ref = dim_product_size.select("size_id", "size_name")
brand_ref = dim_product_brand.select("brand_id", "brand_name")
material_ref = dim_product_material.select("material_id", "material_name")
date_ref = dim_date.select(F.col("date_id"), F.col("full_date"))
supplier_ref = dim_supplier.select("supplier_id", "supplier_nk", "supplier_email")
dim_product = with_surrogate_id(
    typed.select(
        "product_nk",
        "product_name",
        "product_category",
        "pet_category",
        F.col("product_price_num").alias("current_price"),
        F.col("product_quantity_int").alias("stock_quantity"),
        F.col("product_weight_num").alias("product_weight"),
        F.col("product_color").alias("color_name"),
        F.col("product_size").alias("size_name"),
        F.col("product_brand").alias("brand_name"),
        F.col("product_material").alias("material_name"),
        F.col("product_description").alias("description"),
        F.col("product_rating_num").alias("rating"),
        F.col("product_reviews_int").alias("reviews_count"),
        F.col("product_release_date_parsed").alias("release_full_date"),
        F.col("product_expiry_date_parsed").alias("expiry_full_date"),
        F.col("supplier_email").alias("supplier_email"),
    )
    .dropDuplicates(["product_nk"])
    .join(product_category_ref, F.col("product_category") == product_category_ref.product_category_name, "inner")
    .join(pet_category_ref, F.col("pet_category") == pet_category_ref.pet_category_name, "inner")
    .join(color_ref, "color_name")
    .join(size_ref, "size_name")
    .join(brand_ref, "brand_name")
    .join(material_ref, "material_name")
    .join(date_ref.select(F.col("date_id").alias("release_date_id"), F.col("full_date").alias("release_full_date")), "release_full_date")
    .join(date_ref.select(F.col("date_id").alias("expiry_date_id"), F.col("full_date").alias("expiry_full_date")), "expiry_full_date")
    .join(supplier_ref.select("supplier_id", "supplier_email"), "supplier_email")
    .select(
        "product_nk",
        "product_name",
        "product_category_id",
        "current_price",
        "stock_quantity",
        "pet_category_id",
        "product_weight",
        "color_id",
        "size_id",
        "brand_id",
        "material_id",
        "description",
        "rating",
        "reviews_count",
        "release_date_id",
        "expiry_date_id",
        "supplier_id",
    ),
    "product_nk",
    "product_id",
).select(
    "product_id",
    "product_nk",
    "product_name",
    "product_category_id",
    "current_price",
    "stock_quantity",
    "pet_category_id",
    "product_weight",
    "color_id",
    "size_id",
    "brand_id",
    "material_id",
    "description",
    "rating",
    "reviews_count",
    "release_date_id",
    "expiry_date_id",
    "supplier_id",
)

customer_ref = dim_customer.select("customer_id", "customer_nk")
seller_ref = dim_seller.select("seller_id", "seller_nk")
store_ref = dim_store.select("store_id", "store_nk")
product_ref = dim_product.select("product_id", "product_nk")
sale_date_ref = dim_date.select(F.col("date_id").alias("sale_date_id"), F.col("full_date").alias("sale_full_date"))

fact_sales = (
    typed.join(customer_ref, typed.customer_email == customer_ref.customer_nk, "inner")
    .join(seller_ref, typed.seller_email == seller_ref.seller_nk, "inner")
    .join(store_ref, typed.store_email == store_ref.store_nk, "inner")
    .join(product_ref, "product_nk", "inner")
    .join(sale_date_ref, typed.sale_date_parsed == sale_date_ref.sale_full_date, "inner")
    .select(
        F.row_number().over(Window.orderBy("mock_data_key")).cast("long").alias("sale_id"),
        "mock_data_key",
        "source_row_id",
        "sale_date_id",
        "customer_id",
        "seller_id",
        "store_id",
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
        "dim_product",
        "dim_store",
        "dim_seller",
        "dim_customer",
        "dim_supplier",
        "dim_date",
        "dim_month",
        "dim_quarter",
        "dim_year",
        "dim_product_size",
        "dim_product_color",
        "dim_product_material",
        "dim_product_brand",
        "dim_product_category",
        "dim_customer_pet",
        "dim_pet_breed",
        "dim_pet_type",
        "dim_pet_category",
        "dim_postal_code",
        "dim_address",
        "dim_city",
        "dim_state",
        "dim_country",
    ]
)

table_writer(dim_country, "dim_country", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_state, "dim_state", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_city, "dim_city", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_address, "dim_address", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_postal_code, "dim_postal_code", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_pet_category, "dim_pet_category", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_pet_type, "dim_pet_type", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_pet_breed, "dim_pet_breed", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_customer_pet, "dim_customer_pet", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_product_category, "dim_product_category", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_product_brand, "dim_product_brand", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_product_material, "dim_product_material", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_product_color, "dim_product_color", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_product_size, "dim_product_size", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_year, "dim_year", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_quarter, "dim_quarter", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_month, "dim_month", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_date, "dim_date", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_supplier, "dim_supplier", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_customer, "dim_customer", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_seller, "dim_seller", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_store, "dim_store", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(dim_product, "dim_product", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")
table_writer(fact_sales, "fact_sales", POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, mode="append")

spark.stop()
