from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import *


def condo():

    spark = SparkSession.builder.config(
        'spark.sql.repl.eagerEval.enabled', True,
    ).getOrCreate()

    matrix = [
        [1, 34, 'Cardiology'],
        [2, 70, 'Neurology'],
        [3, 50, 'Orthopedics'],
        [4, 20, 'Cardiology'],
        [5, 15, 'Neurology'],
    ]

    columns = ['patient_id', 'age', 'department']

    df = spark.createDataFrame(matrix, columns)

    # print(df)

    df = df.withColumn(
        'age_category',
        F.when(F.col('age') > 60, 'senior')
        .when(F.col('age') > 18, 'adult')
        .otherwise('minor'),
    )

    # print(df)

    return df
