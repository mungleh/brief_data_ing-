from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import *


def aggo():

    spark = SparkSession.builder.config(
        "spark.sql.repl.eagerEval.enabled",
        True,
    ).getOrCreate()

    matrix = [
        [1, 34, "Cardiology", 10],
        [2, 45, "Neurology", 12],
        [3, 23, "Cardiology", 5],
        [4, 64, "Orthopedics", 8],
        [5, 52, "Cardiology", 9],
    ]

    columns = ["patient_id", "age", "department", "visit_count"]

    df = spark.createDataFrame(matrix, columns)

    agg_df = df.groupby("department").agg(
        F.sum("visit_count").alias("visit_count"),
        F.mean("age").alias("age"),
        F.max("age").alias(""),
    )

    # department  sum       mean  max

    agg_df = (
        agg_df.withColumnRenamed("visit_count", "sum")
        .withColumnRenamed("age", "mean")
        .withColumnRenamed("", "max")
    )

    # agg_df = spark.createDataFrame(
    #     [('', 'sum', 'mean', 'max')], columns,
    # ).union(agg_df)

    # agg_df = agg_df.withColumnRenamed('visit_count', '').withColumnRenamed(
    #     'age', 'visit_count',
    # )

    # agg_df = agg_df.withColumnRenamed('department', 'age').withColumnRenamed(
    #     'patient_id', 'department',
    # )

    # print(df)

    # print(agg_df)

    return agg_df


# print(aggo())
