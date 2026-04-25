from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PG_URL = "jdbc:postgresql://postgres:5432/petstore"
PG_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

CH_URL = "jdbc:clickhouse://clickhouse:8123/default"
CH_PROPS = {
    "user": "default",
    "password": "clickhouse",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


def read_pg(spark, table):
    return (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table)
        .options(**PG_PROPS)
        .load()
    )


def write_ch(df, table):
    df.write.format("jdbc").option("url", CH_URL).option("dbtable", table).option(
        "batchsize", "10000"
    ).options(**CH_PROPS).mode("append").save()


def main():
    spark = SparkSession.builder.appName("StarToClickHouse").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    fact = read_pg(spark, "fact_sales")
    dim_customer = read_pg(spark, "dim_customer")
    dim_product = read_pg(spark, "dim_product")
    dim_store = read_pg(spark, "dim_store")
    dim_supplier = read_pg(spark, "dim_supplier")

    prod = dim_product.select(
        "product_id",
        F.col("name").alias("product_name"),
        F.col("category").alias("product_category"),
        F.col("price").cast("double").alias("product_price"),
        F.col("rating").cast("double").alias("product_rating"),
        F.col("reviews").cast("long").alias("product_reviews"),
    )

    report_products = (
        fact.join(prod, "product_id")
        .groupBy("product_name", "product_category", "product_rating", "product_reviews")
        .agg(
            F.sum("quantity").cast("long").alias("total_sold"),
            F.round(F.sum("total_price"), 2).cast("double").alias("revenue"),
        )
        .select(
            F.col("product_name"),
            F.col("product_category").alias("category"),
            F.col("total_sold"),
            F.col("revenue"),
            F.col("product_rating").alias("avg_rating"),
            F.col("product_reviews").alias("total_reviews"),
        )
    )
    write_ch(report_products, "report_products")

    cust = dim_customer.select(
        "customer_id",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("full_name"),
        F.col("country"),
    )

    report_customers = (
        fact.join(cust, "customer_id")
        .groupBy("customer_id", "full_name", "country")
        .agg(
            F.round(F.sum("total_price"), 2).cast("double").alias("total_spent"),
            F.round(F.avg("total_price"), 2).cast("double").alias("avg_check"),
            F.count("sale_id").cast("long").alias("order_count"),
        )
    )
    write_ch(report_customers, "report_customers")

    report_time = (
        fact.withColumn("year", F.year(F.col("sale_date")))
        .withColumn("month", F.month(F.col("sale_date")))
        .groupBy("year", "month")
        .agg(
            F.round(F.sum("total_price"), 2).cast("double").alias("total_revenue"),
            F.round(F.avg("total_price"), 2).cast("double").alias("avg_order"),
            F.count("sale_id").cast("long").alias("order_count"),
        )
    )
    write_ch(report_time, "report_time")

    store = dim_store.select(
        "store_id",
        F.col("name").alias("store_name"),
        F.col("city").alias("store_city"),
        F.col("country").alias("store_country"),
    )

    report_stores = (
        fact.join(store, "store_id")
        .groupBy("store_id", "store_name", "store_city", "store_country")
        .agg(
            F.round(F.sum("total_price"), 2).cast("double").alias("revenue"),
            F.round(F.avg("total_price"), 2).cast("double").alias("avg_check"),
            F.count("sale_id").cast("long").alias("order_count"),
        )
        .select(
            F.col("store_id"),
            F.col("store_name"),
            F.col("store_city").alias("city"),
            F.col("store_country").alias("country"),
            F.col("revenue"),
            F.col("avg_check"),
            F.col("order_count"),
        )
    )
    write_ch(report_stores, "report_stores")

    sup = dim_supplier.select(
        "supplier_id",
        F.col("name").alias("supplier_name"),
        F.col("country").alias("supplier_country"),
    )
    prod_price = dim_product.select(
        "product_id", F.col("price").cast("double").alias("product_price")
    )

    report_suppliers = (
        fact.join(sup, "supplier_id")
        .join(prod_price, "product_id")
        .groupBy("supplier_id", "supplier_name", "supplier_country")
        .agg(
            F.round(F.sum("total_price"), 2).cast("double").alias("revenue"),
            F.round(F.avg("product_price"), 2).cast("double").alias("avg_product_price"),
            F.count("sale_id").cast("long").alias("order_count"),
        )
        .select(
            F.col("supplier_id"),
            F.col("supplier_name"),
            F.col("supplier_country").alias("country"),
            F.col("revenue"),
            F.col("avg_product_price"),
            F.col("order_count"),
        )
    )
    write_ch(report_suppliers, "report_suppliers")

    report_quality = (
        fact.join(prod, "product_id")
        .groupBy(
            "product_id",
            "product_name",
            "product_category",
            "product_rating",
            "product_reviews",
        )
        .agg(F.sum("quantity").cast("long").alias("total_sold"))
        .select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("product_category").alias("category"),
            F.col("product_rating").alias("rating"),
            F.col("product_reviews").alias("total_reviews"),
            F.col("total_sold"),
        )
    )
    write_ch(report_quality, "report_quality")

    spark.stop()


if __name__ == "__main__":
    main()
