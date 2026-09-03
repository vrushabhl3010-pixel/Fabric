# Fabric Spark Notebook
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def keep_latest(df):
    window = (
        Window
        .partitionBy("resource_id")
        .orderBy(F.col("ingestion_timestamp").desc())
    )

    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

patient = keep_latest(
    spark.table("bronze_patient").select(
        "resource_id",
        F.get_json_object("resource_json", "$.active")
            .cast("boolean").alias("active"),
        F.get_json_object("resource_json", "$.gender").alias("gender"),
        F.get_json_object("resource_json", "$.birthDate").alias("birth_date"),
        F.get_json_object("resource_json", "$.name[0].family")
            .alias("family_name"),
        F.get_json_object("resource_json", "$.name[0].given[0]")
            .alias("given_name"),
        "resource_json",
        "raw_file_path",
        "ingestion_timestamp"
    )
)

patient.write.mode("overwrite").format("delta").saveAsTable("silver_patient")

encounter = keep_latest(
    spark.table("bronze_encounter").select(
        "resource_id",
        F.get_json_object("resource_json", "$.status").alias("status"),
        F.get_json_object("resource_json", "$.class.code")
            .alias("class_code"),
        F.get_json_object("resource_json", "$.subject.reference")
            .alias("subject_reference"),
        F.get_json_object("resource_json", "$.period.start")
            .alias("period_start"),
        F.get_json_object("resource_json", "$.period.end")
            .alias("period_end"),
        "resource_json",
        "raw_file_path",
        "ingestion_timestamp"
    )
)

encounter.write.mode("overwrite").format("delta").saveAsTable("silver_encounter")

observation = keep_latest(
    spark.table("bronze_observation").select(
        "resource_id",
        F.get_json_object("resource_json", "$.status").alias("status"),
        F.get_json_object("resource_json", "$.code.text").alias("code_text"),
        F.get_json_object("resource_json", "$.subject.reference")
            .alias("subject_reference"),
        F.get_json_object("resource_json", "$.effectiveDateTime")
            .alias("effective_datetime"),
        F.get_json_object("resource_json", "$.valueQuantity.value")
            .cast("double").alias("value_quantity"),
        F.get_json_object("resource_json", "$.valueQuantity.unit")
            .alias("value_unit"),
        "resource_json",
        "raw_file_path",
        "ingestion_timestamp"
    )
)

observation.write.mode("overwrite").format("delta").saveAsTable("silver_observation")

condition = keep_latest(
    spark.table("bronze_condition").select(
        "resource_id",
        F.get_json_object(
            "resource_json",
            "$.clinicalStatus.coding[0].code"
        ).alias("clinical_status"),
        F.get_json_object(
            "resource_json",
            "$.verificationStatus.coding[0].code"
        ).alias("verification_status"),
        F.get_json_object("resource_json", "$.code.text")
            .alias("code_text"),
        F.get_json_object("resource_json", "$.subject.reference")
            .alias("subject_reference"),
        F.get_json_object("resource_json", "$.onsetDateTime")
            .alias("onset_datetime"),
        "resource_json",
        "raw_file_path",
        "ingestion_timestamp"
    )
)

condition.write.mode("overwrite").format("delta").saveAsTable("silver_condition")

print("Silver completed")
