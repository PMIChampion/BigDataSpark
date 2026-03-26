CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id BIGINT PRIMARY KEY,
    customer_nk TEXT NOT NULL UNIQUE,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    age INTEGER NOT NULL,
    email TEXT NOT NULL,
    country TEXT NOT NULL,
    postal_code TEXT,
    pet_type TEXT NOT NULL,
    pet_name TEXT NOT NULL,
    pet_breed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id BIGINT PRIMARY KEY,
    seller_nk TEXT NOT NULL UNIQUE,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    country TEXT NOT NULL,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id BIGINT PRIMARY KEY,
    store_nk TEXT NOT NULL UNIQUE,
    store_name TEXT NOT NULL,
    store_location TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT,
    country TEXT NOT NULL,
    phone TEXT NOT NULL,
    email TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id BIGINT PRIMARY KEY,
    supplier_nk TEXT NOT NULL UNIQUE,
    supplier_name TEXT NOT NULL,
    contact_name TEXT NOT NULL,
    email TEXT NOT NULL,
    phone TEXT NOT NULL,
    address TEXT NOT NULL,
    city TEXT NOT NULL,
    country TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id BIGINT PRIMARY KEY,
    product_nk TEXT NOT NULL UNIQUE,
    product_name TEXT NOT NULL,
    product_category TEXT NOT NULL,
    pet_category TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    stock_quantity INTEGER NOT NULL,
    weight NUMERIC(10, 2) NOT NULL,
    color TEXT NOT NULL,
    size TEXT NOT NULL,
    brand TEXT NOT NULL,
    material TEXT NOT NULL,
    description TEXT NOT NULL,
    rating NUMERIC(3, 1) NOT NULL,
    reviews_count INTEGER NOT NULL,
    release_date DATE NOT NULL,
    expiry_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_month INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    quarter_number INTEGER NOT NULL,
    year_number INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id BIGINT PRIMARY KEY,
    mock_data_key BIGINT NOT NULL UNIQUE,
    source_row_id INTEGER NOT NULL,
    sale_date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    customer_id BIGINT NOT NULL REFERENCES dim_customer(customer_id),
    seller_id BIGINT NOT NULL REFERENCES dim_seller(seller_id),
    store_id BIGINT NOT NULL REFERENCES dim_store(store_id),
    supplier_id BIGINT NOT NULL REFERENCES dim_supplier(supplier_id),
    product_id BIGINT NOT NULL REFERENCES dim_product(product_id),
    source_customer_id INTEGER NOT NULL,
    source_seller_id INTEGER NOT NULL,
    source_product_id INTEGER NOT NULL,
    sale_quantity INTEGER NOT NULL,
    sale_total_price NUMERIC(10, 2) NOT NULL
);
