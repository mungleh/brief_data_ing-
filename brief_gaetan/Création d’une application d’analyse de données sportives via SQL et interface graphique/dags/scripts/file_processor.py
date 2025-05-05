import os
import psycopg2

# Config
DATA_DIR = "/opt/airflow/data"
TEMP_FILE = "/tmp/prepared_file.csv"

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
            est_epreuve_individuelle BOOLEAN,
            est_epreuve_olympique BOOLEAN,
            est_epreuve_ete BOOLEAN,
            est_epreuve_handi BOOLEAN,
            epreuve_sens_resultat BOOLEAN,
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

    for filename in os.listdir(DATA_DIR):

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

    # prend nouvelle donnait (compare nom fichier dans data vs file_name)
    cur.execute("""
        SELECT filename FROM file_name
        WHERE filename NOT IN (SELECT filename FROM data)
        ORDER BY id ASC LIMIT 1;
    """)
    result = cur.fetchone()

    if result is None:
        print("Table a jour, skipp")
        return

    filename = result[0]
    filepath = os.path.join(DATA_DIR, filename)

    # Prepare temp file with filename as the first column
    with open(filepath, 'r', encoding='utf-8') as original:
        # ouvre le csv en liste
        lines = original.readlines()

    # transforme liste enenlever les \n
    header = lines[0].strip()
    # rajoute nouvelle colonne filename pour comparaison
    new_header = "filename," + header
    # lines[1:] skip la première ligne donc header
    # f"{filename}," + line.strip() rajoute le faliename a chaque ligne
    data_rows = [
    f"{filename}," + line.strip()
    for line in lines[1:]
    if line.strip()  # only include non-empty lines
]


    # Write new rows with filename column to a temporary file
    with open(TEMP_FILE, "w", encoding='utf-8') as temp:
        temp.write(new_header + "\n")
        temp.write("\n".join(data_rows))

    # STDIN load le data qui est ouvert par le with open
    with open(TEMP_FILE, "r", encoding='utf-8') as f:
        cur.copy_expert(
            f"""
            COPY data ({new_header})
            FROM STDIN WITH CSV HEADER DELIMITER ',' NULL ''
            """,
            f
        )

    conn.commit()

    cur.execute("INSERT INTO data (filename) VALUES (%s);", (filename,))
    conn.commit()

    cur.close()
    conn.close()
    print(f"data ajouté pour: {filename}")
