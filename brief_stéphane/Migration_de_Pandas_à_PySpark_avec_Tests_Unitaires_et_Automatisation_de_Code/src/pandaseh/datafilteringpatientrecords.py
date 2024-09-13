from __future__ import annotations

import pandas as pd


def data():

    # Données
    df = pd.DataFrame(
        {
            'patient_id': [1, 2, 3, 4, 5],
            'age': [34, 45, 50, 20, 15],
            'department': [
                'Cardiology',
                'Neurology',
                'Orthopedics',
                'Cardiology',
                'Neurology',
            ],
        },
    )

    # print(df)

    # Filtrer les patients âgés de plus de 30 ans
    filtered_df = df[df['age'] > 30]

    # print(filtered_df)

    return filtered_df
