import os
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PG_URL = "jdbc:postgresql://postgres:5432/petstore"
PG_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

CSV_DIR = "/data/csv"


def file_index(name):
    m = re.search(r"\((\d+)\)", name)
    return int(m.group(1)) if m else 0


def main():
    spark = SparkSession.builder.appName("LoadStaging").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    csv_files = sorted(
        [f for f in os.listdir(CSV_DIR) if f.lower().endswith(".csv")]
    )

    combined = None
    for filename in csv_files:
        path = os.path.join(CSV_DIR, filename)
        idx = file_index(filename)
        df = (
            spark.read.option("header", "true")
            .option("multiLine", "true")
            .option("escape", '"')
            .option("quote", '"')
            .csv(path)
            .withColumn("file_id", F.lit(idx))
        )
        combined = df if combined is None else combined.union(df)

    combined.write.format("jdbc").option("url", PG_URL).option(
        "dbtable", "mock_data"
    ).option("batchsize", "1000").options(**PG_PROPS).mode("append").save()

    spark.stop()


if __name__ == "__main__":
    main()
