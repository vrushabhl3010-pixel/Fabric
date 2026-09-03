# Fabric Spark Notebook
from pyspark.sql import functions as F

resources = ["Patient", "Encounter", "Observation", "Condition"]

for resource in resources:
    raw_path = f"Files/raw/fhir/{resource}"

    df = (
        spark.read
        .option("multiLine", True)
        .json(raw_path)
        .withColumn("raw_file_path", F.input_file_name())
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    df = (
        df.select(
            F.explode_outer("entry").alias("entry"),
            "raw_file_path",
            "ingestion_timestamp"
        )
        .select(
            F.col("entry.resource.id").alias("resource_id"),
            F.col("entry.resource.resourceType").alias("resource_type"),
            F.to_json("entry.resource").alias("resource_json"),
            "raw_file_path",
            "ingestion_timestamp"
        )
        .filter("resource_id IS NOT NULL")
        .withColumn(
            "api_url_or_params",
            F.lit(None).cast("string")
        )
    )

    (
        df.write
        .mode("append")
        .format("delta")
        .saveAsTable(f"bronze_{resource.lower()}")
    )

print("Bronze completed")
