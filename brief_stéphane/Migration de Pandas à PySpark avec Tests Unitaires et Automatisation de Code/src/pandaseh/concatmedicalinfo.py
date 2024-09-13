from __future__ import annotations

import pandas as pd


def concat():

    # Données
    df = pd.DataFrame(
        {
            'patient_name': ['John Doe', 'Jane Smith', 'Alice Brown'],
            'diagnosis': ['Diabetes', 'Heart Disease', 'Hypertension'],
        },
    )

    # print(df)

    # Conversion en minuscules et ajout d'un champ
    df['diagnosis_lower'] = df['diagnosis'].str.lower()
    df['full_info'] = df['patient_name'] + ' - ' + df['diagnosis_lower']

    # print(df)

    return df
