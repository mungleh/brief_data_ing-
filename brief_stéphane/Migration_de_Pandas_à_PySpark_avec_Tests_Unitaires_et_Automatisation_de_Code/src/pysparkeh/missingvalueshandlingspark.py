from __future__ import annotations

import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import *


def misso():

    spark = SparkSession.builder.config(
        'spark.sql.repl.eagerEval.enabled', True,
    ).getOrCreate()

    matrix = [
        [1, 34.0, 'Cardiology'],
        [2, None, 'Neurology'],
        [3, 50.0, 'Orthopedics'],
        [4, None, None],
        [5, 15.0, 'Neurology'],
    ]

    columns = ['patient_id', 'age', 'department']

    df = spark.createDataFrame(matrix, columns)

    print(df)

    # mean_val = df.dropna().select(F.mean(F.col("age"))).head()[0]

    # print(mean_val)

    # df = df.na.fill(df.take(F.mean(F.col("age"))), subset=["age"])

    # df = df.na.fill(F.mean(F.col("age")), subset=["age"])
    # df = df.na.fill("Unknown", subset=["department"])

    # df = df.fillna(F.mean(F.col("age")), subset=["age"])
    # df = df.na.fill("Unknown", subset=["department"])

    df = df.na.fill(
        {
            'department': 'Unknown',
            'age': df.dropna().select(F.mean(F.col('age'))).head()[0],
        },
    )

    # df = df.fillna("Unknown", subset=["department"])

    # print(df)

    return df
