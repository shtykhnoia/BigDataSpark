CREATE TABLE IF NOT EXISTS mock_data (
    file_id              INTEGER,
    id                   TEXT,
    customer_first_name  TEXT,
    customer_last_name   TEXT,
    customer_age         TEXT,
    customer_email       TEXT,
    customer_country     TEXT,
    customer_postal_code TEXT,
    customer_pet_type    TEXT,
    customer_pet_name    TEXT,
    customer_pet_breed   TEXT,
    seller_first_name    TEXT,
    seller_last_name     TEXT,
    seller_email         TEXT,
    seller_country       TEXT,
    seller_postal_code   TEXT,
    product_name         TEXT,
    product_category     TEXT,
    product_price        TEXT,
    product_quantity     TEXT,
    sale_date            TEXT,
    sale_customer_id     TEXT,
    sale_seller_id       TEXT,
    sale_product_id      TEXT,
    sale_quantity        TEXT,
    sale_total_price     TEXT,
    store_name           TEXT,
    store_location       TEXT,
    store_city           TEXT,
    store_state          TEXT,
    store_country        TEXT,
    store_phone          TEXT,
    store_email          TEXT,
    pet_category         TEXT,
    product_weight       TEXT,
    product_color        TEXT,
    product_size         TEXT,
    product_brand        TEXT,
    product_material     TEXT,
    product_description  TEXT,
    product_rating       TEXT,
    product_reviews      TEXT,
    product_release_date TEXT,
    product_expiry_date  TEXT,
    supplier_name        TEXT,
    supplier_contact     TEXT,
    supplier_email       TEXT,
    supplier_phone       TEXT,
    supplier_address     TEXT,
    supplier_city        TEXT,
    supplier_country     TEXT
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id  INTEGER PRIMARY KEY,
    first_name   VARCHAR(100),
    last_name    VARCHAR(100),
    age          INTEGER,
    email        VARCHAR(200),
    country      VARCHAR(100),
    postal_code  VARCHAR(20),
    pet_type     VARCHAR(50),
    pet_name     VARCHAR(100),
    pet_breed    VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id   INTEGER PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    email       VARCHAR(200),
    country     VARCHAR(100),
    postal_code VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id   INTEGER PRIMARY KEY,
    name         VARCHAR(200),
    category     VARCHAR(100),
    price        DECIMAL(10,2),
    quantity     INTEGER,
    pet_category VARCHAR(100),
    weight       DECIMAL(10,2),
    color        VARCHAR(50),
    size         VARCHAR(50),
    brand        VARCHAR(100),
    material     VARCHAR(100),
    description  TEXT,
    rating       DECIMAL(3,1),
    reviews      INTEGER,
    release_date DATE,
    expiry_date  DATE
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id BIGINT PRIMARY KEY,
    name     VARCHAR(200),
    location VARCHAR(200),
    city     VARCHAR(100),
    state    VARCHAR(100),
    country  VARCHAR(100),
    phone    VARCHAR(50),
    email    VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id BIGINT PRIMARY KEY,
    name        VARCHAR(200),
    contact     VARCHAR(200),
    email       VARCHAR(200),
    phone       VARCHAR(50),
    address     VARCHAR(300),
    city        VARCHAR(100),
    country     VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id     INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES dim_customer(customer_id),
    seller_id   INTEGER REFERENCES dim_seller(seller_id),
    product_id  INTEGER REFERENCES dim_product(product_id),
    store_id    BIGINT  REFERENCES dim_store(store_id),
    supplier_id BIGINT  REFERENCES dim_supplier(supplier_id),
    sale_date   DATE,
    quantity    INTEGER,
    total_price DECIMAL(10,2)
);
