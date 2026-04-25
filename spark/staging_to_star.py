from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, DecimalType

PG_URL = "jdbc:postgresql://postgres:5432/petstore"
PG_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}


def read_pg(spark, table):
    return (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table)
        .options(**PG_PROPS)
        .load()
    )


def write_pg(df, table):
    df.write.format("jdbc").option("url", PG_URL).option("dbtable", table).option(
        "batchsize", "1000"
    ).options(**PG_PROPS).mode("append").save()


def surrogate_key(df, cols):
    return F.crc32(
        F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in cols])
    ).cast(LongType())


def main():
    spark = SparkSession.builder.appName("StagingToStarSchema").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_pg(spark, "mock_data")

    offset = F.col("file_id").cast(LongType()) * 1000

    raw = (
        raw.withColumn("_id", F.col("id").cast(LongType()) + offset)
        .withColumn("_cust_id", F.col("sale_customer_id").cast(LongType()) + offset)
        .withColumn("_sell_id", F.col("sale_seller_id").cast(LongType()) + offset)
        .withColumn("_prod_id", F.col("sale_product_id").cast(LongType()) + offset)
    )

    dim_customer = raw.select(
        F.col("_id").cast(IntegerType()).alias("customer_id"),
        F.col("customer_first_name").alias("first_name"),
        F.col("customer_last_name").alias("last_name"),
        F.col("customer_age").cast(IntegerType()).alias("age"),
        F.col("customer_email").alias("email"),
        F.col("customer_country").alias("country"),
        F.col("customer_postal_code").alias("postal_code"),
        F.col("customer_pet_type").alias("pet_type"),
        F.col("customer_pet_name").alias("pet_name"),
        F.col("customer_pet_breed").alias("pet_breed"),
    ).dropDuplicates(["customer_id"])

    dim_seller = raw.select(
        F.col("_id").cast(IntegerType()).alias("seller_id"),
        F.col("seller_first_name").alias("first_name"),
        F.col("seller_last_name").alias("last_name"),
        F.col("seller_email").alias("email"),
        F.col("seller_country").alias("country"),
        F.col("seller_postal_code").alias("postal_code"),
    ).dropDuplicates(["seller_id"])

    dim_product = raw.select(
        F.col("_id").cast(IntegerType()).alias("product_id"),
        F.col("product_name").alias("name"),
        F.col("product_category").alias("category"),
        F.col("product_price").cast(DecimalType(10, 2)).alias("price"),
        F.col("product_quantity").cast(IntegerType()).alias("quantity"),
        F.col("pet_category"),
        F.col("product_weight").cast(DecimalType(10, 2)).alias("weight"),
        F.col("product_color").alias("color"),
        F.col("product_size").alias("size"),
        F.col("product_brand").alias("brand"),
        F.col("product_material").alias("material"),
        F.col("product_description").alias("description"),
        F.col("product_rating").cast(DecimalType(3, 1)).alias("rating"),
        F.col("product_reviews").cast(IntegerType()).alias("reviews"),
        F.to_date(F.col("product_release_date"), "M/d/yyyy").alias("release_date"),
        F.to_date(F.col("product_expiry_date"), "M/d/yyyy").alias("expiry_date"),
    ).dropDuplicates(["product_id"])

    store_cols = [
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email",
    ]
    dim_store = raw.select(
        surrogate_key(raw, store_cols).alias("store_id"),
        F.col("store_name").alias("name"),
        F.col("store_location").alias("location"),
        F.col("store_city").alias("city"),
        F.col("store_state").alias("state"),
        F.col("store_country").alias("country"),
        F.col("store_phone").alias("phone"),
        F.col("store_email").alias("email"),
    ).dropDuplicates(["store_id"])

    supplier_cols = [
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
    ]
    dim_supplier = raw.select(
        surrogate_key(raw, supplier_cols).alias("supplier_id"),
        F.col("supplier_name").alias("name"),
        F.col("supplier_contact").alias("contact"),
        F.col("supplier_email").alias("email"),
        F.col("supplier_phone").alias("phone"),
        F.col("supplier_address").alias("address"),
        F.col("supplier_city").alias("city"),
        F.col("supplier_country").alias("country"),
    ).dropDuplicates(["supplier_id"])

    write_pg(dim_customer, "dim_customer")
    write_pg(dim_seller, "dim_seller")
    write_pg(dim_product, "dim_product")
    write_pg(dim_store, "dim_store")
    write_pg(dim_supplier, "dim_supplier")

    fact_sales = raw.select(
        F.col("_id").cast(IntegerType()).alias("sale_id"),
        F.col("_cust_id").cast(IntegerType()).alias("customer_id"),
        F.col("_sell_id").cast(IntegerType()).alias("seller_id"),
        F.col("_prod_id").cast(IntegerType()).alias("product_id"),
        surrogate_key(raw, store_cols).alias("store_id"),
        surrogate_key(raw, supplier_cols).alias("supplier_id"),
        F.to_date(F.col("sale_date"), "M/d/yyyy").alias("sale_date"),
        F.col("sale_quantity").cast(IntegerType()).alias("quantity"),
        F.col("sale_total_price").cast(DecimalType(10, 2)).alias("total_price"),
    )

    write_pg(fact_sales, "fact_sales")

    spark.stop()


if __name__ == "__main__":
    main()
