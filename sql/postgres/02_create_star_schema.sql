CREATE TABLE IF NOT EXISTS dim_country (
    country_id BIGSERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_state (
    state_id BIGSERIAL PRIMARY KEY,
    state_nk TEXT NOT NULL UNIQUE,
    state_name TEXT NOT NULL,
    country_id BIGINT NOT NULL REFERENCES dim_country(country_id)
);

CREATE TABLE IF NOT EXISTS dim_city (
    city_id BIGSERIAL PRIMARY KEY,
    city_nk TEXT NOT NULL UNIQUE,
    city_name TEXT NOT NULL,
    state_id BIGINT REFERENCES dim_state(state_id),
    country_id BIGINT NOT NULL REFERENCES dim_country(country_id)
);

CREATE TABLE IF NOT EXISTS dim_address (
    address_id BIGSERIAL PRIMARY KEY,
    address_nk TEXT NOT NULL UNIQUE,
    address_line TEXT NOT NULL,
    city_id BIGINT NOT NULL REFERENCES dim_city(city_id)
);

CREATE TABLE IF NOT EXISTS dim_postal_code (
    postal_code_id BIGSERIAL PRIMARY KEY,
    postal_code_nk TEXT NOT NULL UNIQUE,
    postal_code TEXT NOT NULL,
    country_id BIGINT NOT NULL REFERENCES dim_country(country_id)
);

CREATE TABLE IF NOT EXISTS dim_pet_category (
    pet_category_id BIGSERIAL PRIMARY KEY,
    pet_category_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_pet_type (
    pet_type_id BIGSERIAL PRIMARY KEY,
    pet_type_name TEXT NOT NULL UNIQUE,
    pet_category_id BIGINT NOT NULL REFERENCES dim_pet_category(pet_category_id)
);

CREATE TABLE IF NOT EXISTS dim_pet_breed (
    pet_breed_id BIGSERIAL PRIMARY KEY,
    pet_breed_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_customer_pet (
    customer_pet_id BIGSERIAL PRIMARY KEY,
    customer_pet_nk TEXT NOT NULL UNIQUE,
    pet_name TEXT NOT NULL,
    pet_type_id BIGINT NOT NULL REFERENCES dim_pet_type(pet_type_id),
    pet_breed_id BIGINT NOT NULL REFERENCES dim_pet_breed(pet_breed_id)
);

CREATE TABLE IF NOT EXISTS dim_product_category (
    product_category_id BIGSERIAL PRIMARY KEY,
    product_category_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_product_brand (
    brand_id BIGSERIAL PRIMARY KEY,
    brand_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_product_material (
    material_id BIGSERIAL PRIMARY KEY,
    material_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_product_color (
    color_id BIGSERIAL PRIMARY KEY,
    color_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_product_size (
    size_id BIGSERIAL PRIMARY KEY,
    size_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_year (
    year_id BIGSERIAL PRIMARY KEY,
    year_number INTEGER NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_quarter (
    quarter_id BIGSERIAL PRIMARY KEY,
    quarter_nk TEXT NOT NULL UNIQUE,
    quarter_number INTEGER NOT NULL,
    year_id BIGINT NOT NULL REFERENCES dim_year(year_id)
);

CREATE TABLE IF NOT EXISTS dim_month (
    month_id BIGSERIAL PRIMARY KEY,
    month_nk TEXT NOT NULL UNIQUE,
    month_number INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    quarter_id BIGINT NOT NULL REFERENCES dim_quarter(quarter_id)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_month INTEGER NOT NULL,
    month_id BIGINT NOT NULL REFERENCES dim_month(month_id)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id BIGINT PRIMARY KEY,
    supplier_nk TEXT NOT NULL UNIQUE,
    supplier_name TEXT NOT NULL,
    supplier_contact TEXT NOT NULL,
    supplier_email TEXT NOT NULL UNIQUE,
    supplier_phone TEXT NOT NULL,
    address_id BIGINT NOT NULL REFERENCES dim_address(address_id)
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id BIGINT PRIMARY KEY,
    customer_nk TEXT NOT NULL UNIQUE,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    age INTEGER NOT NULL,
    email TEXT NOT NULL UNIQUE,
    country_id BIGINT NOT NULL REFERENCES dim_country(country_id),
    postal_code_id BIGINT REFERENCES dim_postal_code(postal_code_id),
    customer_pet_id BIGINT NOT NULL REFERENCES dim_customer_pet(customer_pet_id)
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id BIGINT PRIMARY KEY,
    seller_nk TEXT NOT NULL UNIQUE,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    country_id BIGINT NOT NULL REFERENCES dim_country(country_id),
    postal_code_id BIGINT REFERENCES dim_postal_code(postal_code_id)
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id BIGINT PRIMARY KEY,
    store_nk TEXT NOT NULL UNIQUE,
    store_name TEXT NOT NULL,
    phone TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    address_id BIGINT NOT NULL REFERENCES dim_address(address_id)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id BIGINT PRIMARY KEY,
    product_nk TEXT NOT NULL UNIQUE,
    product_name TEXT NOT NULL,
    product_category_id BIGINT NOT NULL REFERENCES dim_product_category(product_category_id),
    current_price NUMERIC(10, 2) NOT NULL,
    stock_quantity INTEGER NOT NULL,
    pet_category_id BIGINT NOT NULL REFERENCES dim_pet_category(pet_category_id),
    product_weight NUMERIC(10, 2) NOT NULL,
    color_id BIGINT NOT NULL REFERENCES dim_product_color(color_id),
    size_id BIGINT NOT NULL REFERENCES dim_product_size(size_id),
    brand_id BIGINT NOT NULL REFERENCES dim_product_brand(brand_id),
    material_id BIGINT NOT NULL REFERENCES dim_product_material(material_id),
    description TEXT NOT NULL,
    rating NUMERIC(3, 1) NOT NULL,
    reviews_count INTEGER NOT NULL,
    release_date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    expiry_date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    supplier_id BIGINT NOT NULL REFERENCES dim_supplier(supplier_id)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id BIGINT PRIMARY KEY,
    mock_data_key BIGINT NOT NULL UNIQUE,
    source_row_id INTEGER NOT NULL,
    sale_date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    customer_id BIGINT NOT NULL REFERENCES dim_customer(customer_id),
    seller_id BIGINT NOT NULL REFERENCES dim_seller(seller_id),
    store_id BIGINT NOT NULL REFERENCES dim_store(store_id),
    product_id BIGINT NOT NULL REFERENCES dim_product(product_id),
    source_customer_id INTEGER NOT NULL,
    source_seller_id INTEGER NOT NULL,
    source_product_id INTEGER NOT NULL,
    sale_quantity INTEGER NOT NULL,
    sale_total_price NUMERIC(10, 2) NOT NULL
);
