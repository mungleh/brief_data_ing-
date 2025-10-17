import os
import logging
import psycopg2
import subprocess
import pandas as pd
from sqlalchemy import create_engine


# Config
DATA_DIR = "/opt/airflow/data"

DB_CONFIG = {
    'host': 'postgres',
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_tables():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS file_name (
            id SERIAL PRIMARY KEY,
            filename TEXT UNIQUE,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS data (
            filename TEXT,
            id_resultat FLOAT,
            id_resultat_source FLOAT,
            source TEXT,
            id_athlete_base_resultats TEXT,
            id_personne TEXT,
            athlete_nom TEXT,
            athlete_prenom TEXT,
            id_equipe FLOAT,
            equipe_en TEXT,
            id_pays FLOAT,
            pays_en_base_resultats TEXT,
            classement_epreuve FLOAT,
            performance_finale_texte TEXT,
            performance_finale TEXT,
            id_evenement FLOAT,
            evenement TEXT,
            evenement_en TEXT,
            categorie_age TEXT,
            id_edition FLOAT,
            id_competition_sport FLOAT,
            competition_en TEXT,
            id_type_competition FLOAT,
            type_competition TEXT,
            edition_saison FLOAT,
            date_debut_edition DATE,
            date_fin_edition DATE,
            id_ville_edition FLOAT,
            edition_ville_en TEXT,
            id_nation_edition_base_resultats FLOAT,
            edition_nation_en TEXT,
            id_sport FLOAT,
            sport TEXT,
            sport_en TEXT,
            id_discipline_administrative FLOAT,
            discipline_administrative TEXT,
            id_specialite FLOAT,
            specialite TEXT,
            id_epreuve FLOAT,
            epreuve TEXT,
            epreuve_genre TEXT,
            epreuve_type TEXT,
            est_epreuve_individuelle FLOAT,
            est_epreuve_olympique FLOAT,
            est_epreuve_ete FLOAT,
            est_epreuve_handi FLOAT,
            epreuve_sens_resultat FLOAT,
            id_federation FLOAT,
            federation TEXT,
            federation_nom_court TEXT,
            dt_creation DATE,
            dt_modification DATE,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("table crée")


def check_and_register_files():
    conn = get_db_connection()
    cur = conn.cursor()

    # sort pour comencer du début a la fin
    for filename in sorted(os.listdir(DATA_DIR)):

        # Check if file is already registered
        cur.execute("SELECT 1 FROM file_name WHERE filename = %s;", (filename,))
        if cur.fetchone():
            print(f"fichier existant trouvé, skippé: {filename}")
        else:
            cur.execute("INSERT INTO file_name (filename) VALUES (%s);", (filename,))
            print(f"nouveau fichier trouvé, skippé: {filename}")
            break

    conn.commit()
    cur.close()
    conn.close()

    print(f"table enregistré: {filename}")
    return filename


def insert_file_data():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT filename FROM file_name
        WHERE filename NOT IN (SELECT filename FROM data);
    """)
    result = cur.fetchone()

    if result is None:
        print("Table à jour, skip")
        return

    filename = result[0]

    df = pd.read_csv(f"{DATA_DIR}/{filename}")
    df.insert(loc=0, column='filename', value=filename)

    # 🔥 Convertir les NaN pandas en None Python
    df = df.where(pd.notnull(df), None)

    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    placeholders = ','.join(['%s'] * len(df.columns))
    query = f"INSERT INTO data ({cols}) VALUES ({placeholders})"

    cur.executemany(query, tuples)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ data ajouté pour: {filename}")



def sodascan():
    command = [
        "soda", "scan",
        "-d", "pipi",
        "-c", "/opt/airflow/dags/scripts/soda.yaml",
        "/opt/airflow/dags/scripts/sodacheck.yaml"
    ]

    result = subprocess.run(
        command,
        capture_output=True,  # <== THIS captures logs
        text=True
    )

    logging.info("===== SODA STDOUT =====\n%s", result.stdout)
    logging.error("===== SODA STDERR =====\n%s", result.stderr)
    logging.info("===== RETURN CODE: %s =====", result.returncode)

    if result.returncode not in [0, 2]:
        raise RuntimeError(f"Soda scan failed with exit code {result.returncode}")
