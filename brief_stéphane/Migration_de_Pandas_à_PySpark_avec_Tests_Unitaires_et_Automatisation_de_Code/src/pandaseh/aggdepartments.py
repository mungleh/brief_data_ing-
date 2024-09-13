from __future__ import annotations

import pandas as pd


def agg():

    df = pd.DataFrame(
        {
            "patient_id": [1, 2, 3, 4, 5],
            "age": [34, 45, 23, 64, 52],
            "department": [
                "Cardiology",
                "Neurology",
                "Cardiology",
                "Orthopedics",
                "Cardiology",
            ],
            "visit_count": [10, 12, 5, 8, 9],
        },
    )

    # GroupBy et calculs statistiques
    agg_df = (
        df.groupby('department")
        .agg({"visit_count": "sum", "age": ["mean", "max"]})
        .reset_index()
    )

    # print(df)

    # print(agg_df)

    agg_df.columns = ["department", "sum", "mean", "max"]

    return agg_df


# print(agg())
