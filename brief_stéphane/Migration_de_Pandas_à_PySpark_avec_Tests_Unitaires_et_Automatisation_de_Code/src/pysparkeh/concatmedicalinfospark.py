from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import *


def concato():

    spark = SparkSession.builder.config(
        "spark.sql.repl.eagerEval.enabled",
        True,
    ).getOrCreate()

    matrix = [
        ["John Doe", "Diabetes"],
        ["Jane Smith", "Heart Disease"],
        ["Alice Brown", "Hypertension"],
    ]

    columns = ["patient_name", "diagnosis"]

    df = spark.createDataFrame(matrix, columns)

    # print(df)
    lal = 0

    df = df.withColumn("diagnosis_lower", F.lower(F.col("diagnosis"))).withColumn(
        "full_info",
        F.concat_ws(
            " - ",
            F.col("patient_name"),
            F.col("diagnosis_lower"),
        ),
    )

    # print(df.show(df.count(), False))

    return df
