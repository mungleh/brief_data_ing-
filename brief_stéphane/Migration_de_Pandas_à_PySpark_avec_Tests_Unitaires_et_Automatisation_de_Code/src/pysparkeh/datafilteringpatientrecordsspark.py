from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import *


def datao():

    spark = SparkSession.builder.config(
        'spark.sql.repl.eagerEval.enabled', True,
    ).getOrCreate()

    matrix = [
        [1, 34, 'Cardiology'],
        [2, 45, 'Neurology'],
        [3, 50, 'Orthopedics'],
        [4, 20, 'Cardiology'],
        [5, 15, 'Neurology'],
    ]

    columns = ['patient_id', 'age', 'department']

    df = spark.createDataFrame(matrix, columns)

    # print(df)

    filtedred_df = df.filter(F.col('age') > 30)

    # print(filtedred_df)

    return filtedred_df
