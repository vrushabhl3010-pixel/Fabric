# Fabric Spark Notebook
from pyspark.sql import functions as F

patient = (
    spark.table("scd2_patient")
    .filter("is_current = true")
    .select(
        F.col("resource_id").alias("patient_id"),
        "active",
        "gender",
        "birth_date",
        "family_name",
        "given_name",
        "effective_from",
        "effective_to"
    )
)

patient.write.mode("overwrite").format("delta").saveAsTable(
    "gold_dim_patient"
)

encounter = (
    spark.table("scd2_encounter")
    .filter("is_current = true")
    .withColumn(
        "patient_id",
        F.regexp_extract(
            "subject_reference",
            r"Patient/([^/]+)",
            1
        )
    )
    .select(
        "resource_id",
        "patient_id",
        "status",
        "class_code",
        "period_start",
        "period_end",
        "effective_from",
        "effective_to"
    )
)

encounter.write.mode("overwrite").format("delta").saveAsTable(
    "gold_fact_encounter"
)

observation = (
    spark.table("scd2_observation")
    .filter("is_current = true")
    .withColumn(
        "patient_id",
        F.regexp_extract(
            "subject_reference",
            r"Patient/([^/]+)",
            1
        )
    )
    .select(
        "resource_id",
        "patient_id",
        "status",
        "code_text",
        "effective_datetime",
        "value_quantity",
        "value_unit",
        "effective_from",
        "effective_to"
    )
)

observation.write.mode("overwrite").format("delta").saveAsTable(
    "gold_fact_observation"
)

condition = (
    spark.table("scd2_condition")
    .filter("is_current = true")
    .withColumn(
        "patient_id",
        F.regexp_extract(
            "subject_reference",
            r"Patient/([^/]+)",
            1
        )
    )
    .select(
        "resource_id",
        "patient_id",
        "clinical_status",
        "verification_status",
        "code_text",
        "onset_datetime",
        "effective_from",
        "effective_to"
    )
)

condition.write.mode("overwrite").format("delta").saveAsTable(
    "gold_fact_condition"
)

print("Gold completed")
