from __future__ import annotations

import pandas as pd


def cond():

    # Données
    df = pd.DataFrame(
        {
            'patient_id': [1, 2, 3, 4, 5],
            'age': [34, 70, 50, 20, 15],
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

    # Ajout d'une colonne conditionnelle (catégorie d'âge)
    df['age_category'] = df['age'].apply(
        lambda x: 'senior' if x > 60 else 'adult' if x > 18 else 'minor',
    )

    # print(df)

    return df
