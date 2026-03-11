"""Pipeline Chicago Crimes avec ingestion, transformation et checks Soda."""

from __future__ import annotations

from datetime import datetime
import logging
import os
import sys

from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(__file__))

from pipeline_utils import (
    build_soda_environment,
    normalize_boolean_field,
    staging_table_name,
)

logger = logging.getLogger(__name__)

CHICAGO_API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
API_LIMIT = 2000
POSTGRES_CONN_ID = "chicago_crimes_db"
SODA_CONFIG_PATH = "/usr/local/airflow/include/soda/configuration.yml"
SODA_CHECKS_DIR = "/usr/local/airflow/include/soda/checks"
RAW_TABLE = "chicago_crimes_raw"
TRANSFORMED_TABLE = "chicago_crimes_transformed"
RAW_STAGING = staging_table_name(RAW_TABLE)
TRANSFORMED_STAGING = staging_table_name(TRANSFORMED_TABLE)


def fetch_crimes_data(limit: int, offset: int, since_date: str | None) -> list[dict]:
    """Recupere les derniers crimes depuis l'API Chicago avec pagination."""
    params = {
        "$limit": limit,
        "$offset": offset,
        "$order": "date DESC",
    }

    if since_date:
        params["$where"] = f"date > '{since_date}'"

    response = requests.get(CHICAGO_API_URL, params=params, timeout=120)
    response.raise_for_status()
    return response.json()


def run_soda_scan(check_file: str, scan_name: str):
    """Execute un scan Soda sur la base cible."""
    from soda.scan import Scan

    connection = BaseHook.get_connection(POSTGRES_CONN_ID)
    os.environ.update(build_soda_environment(connection))

    scan = Scan()
    scan.set_scan_definition_name(scan_name)
    scan.set_data_source_name("chicago_crimes")
    scan.add_configuration_yaml_file(SODA_CONFIG_PATH)
    scan.add_sodacl_yaml_file(os.path.join(SODA_CHECKS_DIR, check_file))
    scan.execute()

    results = scan.get_scan_results()
    logger.info("Soda scan %s termine: %s", scan_name, results)
    if scan.has_check_fails():
        raise ValueError(f"Soda scan {scan_name} echoue: {scan.get_checks_fail()}")
    return results


@dag(
    dag_id="chicago_crimes_pipeline",
    description="Pipeline Chicago Crimes - etape 3",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dataops", "chicago", "soda"],
    default_args={"owner": "dataops-team", "retries": 2},
)
def chicago_crimes_pipeline():

    @task()
    def create_tables():
        """Cree les tables production et staging."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
                id VARCHAR(50) PRIMARY KEY,
                case_number VARCHAR(20),
                date VARCHAR(50),
                block VARCHAR(100),
                iucr VARCHAR(10),
                primary_type VARCHAR(100),
                description VARCHAR(255),
                location_description VARCHAR(100),
                arrest VARCHAR(10),
                domestic VARCHAR(10),
                beat VARCHAR(10),
                district VARCHAR(10),
                ward VARCHAR(10),
                community_area VARCHAR(10),
                fbi_code VARCHAR(10),
                x_coordinate VARCHAR(20),
                y_coordinate VARCHAR(20),
                year VARCHAR(10),
                updated_on VARCHAR(50),
                latitude VARCHAR(30),
                longitude VARCHAR(30)
            );
            """
        )

        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {TRANSFORMED_TABLE} (
                id VARCHAR(50) PRIMARY KEY,
                case_number VARCHAR(20),
                crime_date TIMESTAMP,
                crime_year INTEGER,
                crime_month INTEGER,
                crime_day_of_week INTEGER,
                block VARCHAR(100),
                primary_type VARCHAR(100),
                description VARCHAR(255),
                location_description VARCHAR(100),
                arrest BOOLEAN,
                domestic BOOLEAN,
                district VARCHAR(10),
                ward VARCHAR(10),
                community_area VARCHAR(10),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION
            );
            """
        )

        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {RAW_STAGING}
            (LIKE {RAW_TABLE} INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
            """
        )

        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {TRANSFORMED_STAGING}
            (LIKE {TRANSFORMED_TABLE} INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
            """
        )

        logger.info(
            "Tables creees: %s, %s, %s, %s",
            RAW_TABLE,
            TRANSFORMED_TABLE,
            RAW_STAGING,
            TRANSFORMED_STAGING,
        )

    @task()
    def process_all_pages():
        """Traite toutes les pages de donnees de maniere sequentielle."""
        logger.info("Debut du traitement de toutes les pages")
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()
        hook.run(f"TRUNCATE TABLE {RAW_STAGING};")
        df.to_sql(
            RAW_STAGING,
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

        logger.info("%s lignes inserees dans %s", len(df), RAW_STAGING)
        return len(df)

    @task()
    def soda_check_raw(row_count: int):
        """Controle qualite sur la table brute de staging."""
        logger.info("Controle Soda sur %s lignes brutes", row_count)
        return run_soda_scan("raw_data_checks.yml", "raw_quality_check")

    @task()
    def transform_data():
        """Transforme les donnees brutes et charge la table transformee en staging."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        df = pd.read_sql(f"SELECT * FROM {RAW_STAGING}", engine)
        logger.info("Donnees brutes lues depuis %s: %s lignes", RAW_STAGING, len(df))
        if df.empty:
            raise ValueError("Aucune donnee brute disponible pour la transformation")

        df = df.dropna(subset=["latitude", "longitude", "primary_type"])
        df["crime_date"] = pd.to_datetime(df["date"], format="mixed", errors="coerce")
        df = df.dropna(subset=["crime_date"])
        df["crime_year"] = df["crime_date"].dt.year
        df["crime_month"] = df["crime_date"].dt.month
        df["crime_day_of_week"] = df["crime_date"].dt.dayofweek

        for field in ["arrest", "domestic"]:
            df[field] = df[field].map(lambda value, f=field: normalize_boolean_field(value, f))

        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df = df.dropna(subset=["latitude", "longitude"])

        transformed_df = df[[
            "id",
            "case_number",
            "crime_date",
            "crime_year",
            "crime_month",
            "crime_day_of_week",
            "block",
            "primary_type",
            "description",
            "location_description",
            "arrest",
            "domestic",
            "district",
            "ward",
            "community_area",
            "latitude",
            "longitude",
        ]].copy()

                    if not transformed_df.empty:
                        logger.info("Insertion des donnees transformees: %d lignes", len(transformed_df))
                        hook.run(f"TRUNCATE TABLE {TRANSFORMED_STAGING}")
                        transformed_df.to_sql(TRANSFORMED_STAGING, engine, if_exists="append", index=False, method="multi", chunksize=1000)
                        transformed_rows = len(transformed_df)
                        total_transformed += transformed_rows

                        # Promouvoir vers production
                        logger.info("Promotion vers production")
                        # use ON CONFLICT DO NOTHING to avoid failing on duplicate primary keys
                        hook.run(
                            f"INSERT INTO {RAW_TABLE} SELECT * FROM {RAW_STAGING} "
                            "ON CONFLICT (id) DO NOTHING"
                        )
                        hook.run(
                            f"INSERT INTO {TRANSFORMED_TABLE} SELECT * FROM {TRANSFORMED_STAGING} "
                            "ON CONFLICT (id) DO NOTHING"
                        )
                        logger.info("Page %d: %d lignes transformees et promotees", pages_processed, transformed_rows)
                    else:
                        logger.warning("Page %d: aucune donnee exploitable apres transformation", pages_processed)
                else:
                    logger.warning("Page %d: aucune donnee dans staging", pages_processed)

                pages_processed += 1

            except Exception as e:
                logger.error("Erreur lors du traitement de la page offset=%d: %s", offset, e)
                raise

        logger.info("Traitement termine: %d pages, %d lignes ingerees, %d lignes transformees",
                   pages_processed, total_ingested, total_transformed)
        return {"total_ingested": total_ingested, "total_transformed": total_transformed, "pages_processed": pages_processed}

    def soda_check_raw(row_count: int):
        """Controle qualite sur la table brute de staging."""
        logger.info("Controle Soda sur %s lignes brutes", row_count)
        return run_soda_scan("raw_data_checks.yml", "raw_quality_check")


    @task()
    def transform_data():
        """Transforme les donnees brutes et charge la table transformee en staging."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        df = pd.read_sql(f"SELECT * FROM {RAW_STAGING}", engine)
        logger.info("Donnees brutes lues depuis %s: %s lignes", RAW_STAGING, len(df))
        if df.empty:
            raise ValueError("Aucune donnee brute disponible pour la transformation")

        # 1) Filtrer les lignes sans infos minimales
        df = df.dropna(subset=["latitude", "longitude", "primary_type", "date"])
        # 2) Conversion de la date
        df["crime_date"] = pd.to_datetime(df["date"], format="mixed", errors="coerce")
        df = df.dropna(subset=["crime_date"])

        # 3) Filtrer les dates aberrantes
        now = pd.Timestamp.utcnow()
        df = df[df["crime_date"] <= now]
        # 4) Features temporelles de base
        df["crime_year"] = df["crime_date"].dt.year
        df["crime_month"] = df["crime_date"].dt.month
        df["crime_day_of_week"] = df["crime_date"].dt.dayofweek

        # 5) Normalisation des booléens
        for field in ["arrest", "domestic"]:
            df[field] = df[field].map(lambda value, f=field: normalize_boolean_field(value, f))

        # 6) Conversion des coordonnées
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df = df.dropna(subset=["latitude", "longitude"])

        # 7) Filtrer les coordonnées hors de la zone de Chicago
        # (approx : lat 41.6–42.1, long -87.95 – -87.5)
        df = df[
            (df["latitude"].between(41.6, 42.1)) &
            (df["longitude"].between(-87.95, -87.5))
        ]
        # 8) Sélection des colonnes finales
        transformed_df = df[[
            "id",
            "case_number",
            "crime_date",
            "crime_year",
            "crime_month",
            "crime_day_of_week",
            "block",
            "primary_type",
            "description",
            "location_description",
            "arrest",
            "domestic",
            "district",
            "ward",
            "community_area",
            "latitude",
            "longitude",
        ]].copy()

        if transformed_df.empty:
            raise ValueError("Aucune donnee exploitable apres transformation")

        hook.run(f"TRUNCATE TABLE {TRANSFORMED_STAGING};")
        transformed_df.to_sql(
            TRANSFORMED_STAGING,
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

        logger.info("%s lignes transformees chargees dans %s", len(transformed_df), TRANSFORMED_STAGING)
        return len(transformed_df)



    @task()
    def final_quality_check():
        """Controle qualite final sur les tables production apres toutes les pages."""
        logger.info("Controle Soda final sur les donnees production")
        run_soda_scan("raw_data_checks.yml", "raw_quality_check_final")
        run_soda_scan("transformed_data_checks.yml", "transformed_quality_check_final")

    tables = create_tables()
    process_all = process_all_pages()
    final_checks = final_quality_check()

    tables >> process_all >> final_checks


chicago_crimes_pipeline()
"""Pipeline Chicago Crimes avec ingestion, transformation, pagination et checks Soda."""

from __future__ import annotations

from datetime import datetime
import logging
import os
import sys

from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(__file__))

from pipeline_utils import (
    build_soda_environment,
    normalize_boolean_field,
    staging_table_name,
)

logger = logging.getLogger(__name__)

CHICAGO_API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
API_LIMIT = 2000
POSTGRES_CONN_ID = "chicago_crimes_db"
SODA_CONFIG_PATH = "/usr/local/airflow/include/soda/configuration.yml"
SODA_CHECKS_DIR = "/usr/local/airflow/include/soda/checks"
RAW_TABLE = "chicago_crimes_raw"
TRANSFORMED_TABLE = "chicago_crimes_transformed"
RAW_STAGING = staging_table_name(RAW_TABLE)
TRANSFORMED_STAGING = staging_table_name(TRANSFORMED_TABLE)


# -------------------------------------------------------------------------
# FONCTIONS UTILITAIRES
# -------------------------------------------------------------------------
def fetch_crimes_data(limit: int, offset: int, since_date: str | None) -> list[dict]:
    """Récupère les crimes depuis l'API Chicago avec pagination."""
    params = {
        "$limit": limit,
        "$offset": offset,
        "$order": "date DESC",
    }

    if since_date:
        params["$where"] = f"date > '{since_date}'"

    response = requests.get(CHICAGO_API_URL, params=params, timeout=120)
    response.raise_for_status()
    return response.json()


def run_soda_scan(check_file: str, scan_name: str):
    """Exécute un scan Soda."""
    from soda.scan import Scan

    connection = BaseHook.get_connection(POSTGRES_CONN_ID)
    os.environ.update(build_soda_environment(connection))

    scan = Scan()
    scan.set_scan_definition_name(scan_name)
    scan.set_data_source_name("chicago_crimes")
    scan.add_configuration_yaml_file(SODA_CONFIG_PATH)
    scan.add_sodacl_yaml_file(os.path.join(SODA_CHECKS_DIR, check_file))
    scan.execute()

    if scan.has_check_fails():
        raise ValueError(f"Soda scan {scan_name} échoué : {scan.get_checks_fail()}")

    return scan.get_scan_results()


# -------------------------------------------------------------------------
# DEFINITION DU DAG
# -------------------------------------------------------------------------
@dag(
    dag_id="chicago_crimes_pipeline",
    description="Pipeline Chicago Crimes complet",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dataops", "chicago", "soda"],
    default_args={"owner": "dataops-team", "retries": 2},
)
def chicago_crimes_pipeline():

    # ---------------------------------------------------------------------
    # 1) CREATION DES TABLES
    # ---------------------------------------------------------------------
    @task()
    def create_tables():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
                id VARCHAR(50) PRIMARY KEY,
                case_number VARCHAR(20),
                date VARCHAR(50),
                block VARCHAR(100),
                iucr VARCHAR(10),
                primary_type VARCHAR(100),
                description VARCHAR(255),
                location_description VARCHAR(100),
                arrest VARCHAR(10),
                domestic VARCHAR(10),
                beat VARCHAR(10),
                district VARCHAR(10),
                ward VARCHAR(10),
                community_area VARCHAR(10),
                fbi_code VARCHAR(10),
                x_coordinate VARCHAR(20),
                y_coordinate VARCHAR(20),
                year VARCHAR(10),
                updated_on VARCHAR(50),
                latitude VARCHAR(30),
                longitude VARCHAR(30)
            );
            """
        )

        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {TRANSFORMED_TABLE} (
                id VARCHAR(50) PRIMARY KEY,
                case_number VARCHAR(20),
                crime_date TIMESTAMP,
                crime_year INTEGER,
                crime_month INTEGER,
                crime_day_of_week INTEGER,
                block VARCHAR(100),
                primary_type VARCHAR(100),
                description VARCHAR(255),
                location_description VARCHAR(100),
                arrest BOOLEAN,
                domestic BOOLEAN,
                district VARCHAR(10),
                ward VARCHAR(10),
                community_area VARCHAR(10),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION
            );
            """
        )

        hook.run(f"CREATE TABLE IF NOT EXISTS {RAW_STAGING} (LIKE {RAW_TABLE});")
        hook.run(f"CREATE TABLE IF NOT EXISTS {TRANSFORMED_STAGING} (LIKE {TRANSFORMED_TABLE});")

        logger.info("Tables créées.")

    # ---------------------------------------------------------------------
    # 2) INGESTION + PAGINATION + TRANSFORMATION + PROMOTION
    # ---------------------------------------------------------------------
    @task()
    def process_all_pages():
        """Ingestion + pagination + transformation + promotion."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        total_ingested = 0
        total_transformed = 0
        pages_processed = 0
        offset = 0

        while True:
            try:
                logger.info("Récupération page offset=%d", offset)
                data = fetch_crimes_data(API_LIMIT, offset, since_date=None)
                if not data:
                    logger.info("Plus de données à récupérer, fin de la pagination")
                    break

                df = pd.DataFrame(data)
                if df.empty:
                    logger.warning("Page %d: dataframe vide", pages_processed)
                    break

                # INGESTION BRUTE
                hook.run(f"TRUNCATE TABLE {RAW_STAGING};")
                df.to_sql(
                    RAW_STAGING,
                    engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=1000,
                )
                total_ingested += len(df)
                logger.info(
                    "Page %d: %d lignes insérées dans %s",
                    pages_processed,
                    len(df),
                    RAW_STAGING,
                )

                # TRANSFORMATION
                df = pd.read_sql(f"SELECT * FROM {RAW_STAGING}", engine)
                if df.empty:
                    logger.warning("Page %d: aucune donnée dans staging", pages_processed)
                    pages_processed += 1
                    offset += API_LIMIT
                    continue

                # 1) Filtrer les lignes sans infos minimales
                df = df.dropna(subset=["latitude", "longitude", "primary_type", "date"])
                # 2) Conversion de la date
                df["crime_date"] = pd.to_datetime(df["date"], format="mixed", errors="coerce")
                df = df.dropna(subset=["crime_date"])

                # 3) Filtrer les dates aberrantes
                now = pd.Timestamp.utcnow()
                df = df[df["crime_date"] <= now]
                # 4) Features temporelles de base
                df["crime_year"] = df["crime_date"].dt.year
                df["crime_month"] = df["crime_date"].dt.month
                df["crime_day_of_week"] = df["crime_date"].dt.dayofweek

                # 5) Normalisation des booléens
                for field in ["arrest", "domestic"]:
                    df[field] = df[field].map(
                        lambda value, f=field: normalize_boolean_field(value, f)
                    )

                # 6) Conversion des coordonnées
                df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
                df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
                df = df.dropna(subset=["latitude", "longitude"])

                # 7) Filtrer les coordonnées hors de la zone de Chicago
                df = df[
                    (df["latitude"].between(41.6, 42.1))
                    & (df["longitude"].between(-87.95, -87.5))
                ]

                # 8) Sélection des colonnes finales
                transformed_df = df[
                    [
                        "id",
                        "case_number",
                        "crime_date",
                        "crime_year",
                        "crime_month",
                        "crime_day_of_week",
                        "block",
                        "primary_type",
                        "description",
                        "location_description",
                        "arrest",
                        "domestic",
                        "district",
                        "ward",
                        "community_area",
                        "latitude",
                        "longitude",
                    ]
                ].copy()

                if not transformed_df.empty:
                    logger.info(
                        "Insertion des données transformées: %d lignes",
                        len(transformed_df),
                    )
                    hook.run(f"TRUNCATE TABLE {TRANSFORMED_STAGING}")
                    transformed_df.to_sql(
                        TRANSFORMED_STAGING,
                        engine,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=1000,
                    )
                    transformed_rows = len(transformed_df)
                    total_transformed += transformed_rows

                    # PROMOTION VERS PRODUCTION
                    logger.info("Promotion vers production")
                    hook.run(
                        f"INSERT INTO {RAW_TABLE} SELECT * FROM {RAW_STAGING} "
                        "ON CONFLICT (id) DO NOTHING"
                    )
                    hook.run(
                        f"INSERT INTO {TRANSFORMED_TABLE} SELECT * FROM {TRANSFORMED_STAGING} "
                        "ON CONFLICT (id) DO NOTHING"
                    )
                    logger.info(
                        "Page %d: %d lignes transformées et promues",
                        pages_processed,
                        transformed_rows,
                    )
                else:
                    logger.warning(
                        "Page %d: aucune donnée exploitable après transformation",
                        pages_processed,
                    )

                pages_processed += 1
                offset += API_LIMIT

            except Exception as e:
                logger.error("Erreur lors du traitement de la page offset=%d: %s", offset, e)
                raise

        logger.info(
            "Traitement terminé: %d pages, %d lignes ingérées, %d lignes transformées",
            pages_processed,
            total_ingested,
            total_transformed,
        )
        return {
            "total_ingested": total_ingested,
            "total_transformed": total_transformed,
            "pages_processed": pages_processed,
        }

    # ---------------------------------------------------------------------
    # 3) CHECK FINAL SODA
    # ---------------------------------------------------------------------
    @task()
    def final_quality_check():
        """Contrôle qualité final sur les tables de production."""
        logger.info("Contrôle Soda final sur les données de production")
        run_soda_scan("raw_data_checks.yml", "raw_quality_check_final")
        run_soda_scan("transformed_data_checks.yml", "transformed_quality_check_final")

    # ---------------------------------------------------------------------
    # 4) DEPENDANCES DU DAG
    # ---------------------------------------------------------------------
    tables = create_tables()
    process = process_all_pages()
    final = final_quality_check()

    tables >> process >> final


chicago_crimes_pipeline()
