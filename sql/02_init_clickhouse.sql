CREATE TABLE IF NOT EXISTS report_products
(
    product_name  String,
    category      String,
    total_sold    Int64,
    revenue       Float64,
    avg_rating    Float64,
    total_reviews Int64
) ENGINE = MergeTree()
ORDER BY (category, product_name);

CREATE TABLE IF NOT EXISTS report_customers
(
    customer_id Int32,
    full_name   String,
    country     String,
    total_spent Float64,
    avg_check   Float64,
    order_count Int64
) ENGINE = MergeTree()
ORDER BY (country, customer_id);

CREATE TABLE IF NOT EXISTS report_time
(
    year          Int32,
    month         Int32,
    total_revenue Float64,
    avg_order     Float64,
    order_count   Int64
) ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS report_stores
(
    store_id    Int64,
    store_name  String,
    city        String,
    country     String,
    revenue     Float64,
    avg_check   Float64,
    order_count Int64
) ENGINE = MergeTree()
ORDER BY (country, store_name);

CREATE TABLE IF NOT EXISTS report_suppliers
(
    supplier_id       Int64,
    supplier_name     String,
    country           String,
    revenue           Float64,
    avg_product_price Float64,
    order_count       Int64
) ENGINE = MergeTree()
ORDER BY (country, supplier_name);

CREATE TABLE IF NOT EXISTS report_quality
(
    product_id    Int32,
    product_name  String,
    category      String,
    rating        Float64,
    total_reviews Int64,
    total_sold    Int64
) ENGINE = MergeTree()
ORDER BY (category, product_name);
