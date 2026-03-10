"""Fonctions utilitaires partagees par le pipeline Chicago Crimes."""

from __future__ import annotations

from collections.abc import Sequence

STAGING_SUFFIX = "_staging"


def staging_table_name(table_name: str) -> str:
    """Ajoute le suffixe _staging a un nom de table."""
    return f"{table_name}{STAGING_SUFFIX}"


def build_soda_environment(connection) -> dict[str, str]:
    """Convertit une connexion Airflow en variables d'environnement pour Soda."""
    return {
        "POSTGRES_HOST": connection.host or "",
        "POSTGRES_PORT": str(connection.port or 5432),
        "POSTGRES_USER": connection.login or "",
        "POSTGRES_PASSWORD": connection.password or "",
        "POSTGRES_DB": connection.schema or "",
    }


def normalize_boolean_field(value, field_name: str) -> bool:
    """Convertit une valeur true/false en booleen Python."""
    if isinstance(value, bool):
        return value
    if value is None:
        raise ValueError(f"{field_name}: valeur manquante")
    normalized = str(value).strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"{field_name}: valeur invalide {value!r}")


def build_promote_sql(table_pairs: Sequence[tuple[str, str]]) -> str:
    """Genere le SQL de promotion staging -> production dans une transaction."""
    statements = ["BEGIN;"]
    for target, staging in table_pairs:
        statements.append(f"TRUNCATE TABLE {target};")
        statements.append(f"INSERT INTO {target} SELECT * FROM {staging};")
    statements.append("COMMIT;")
    return "\n".join(statements)


def staging_table_name(table_name: str) -> str:
    """Ajoute le suffixe _staging a un nom de table."""
    return f"{table_name}{STAGING_SUFFIX}"


def build_soda_environment(connection) -> dict[str, str]:
    """Convertit une connexion Airflow en variables d'environnement pour Soda."""
    return {
        "POSTGRES_HOST": connection.host or "",
        "POSTGRES_PORT": str(connection.port or 5432),
        "POSTGRES_USER": connection.login or "",
        "POSTGRES_PASSWORD": connection.password or "",
        "POSTGRES_DB": connection.schema or "",
    }


def normalize_boolean_field(value, field_name: str) -> bool:
    """Convertit une valeur true/false en booleen Python."""
    if isinstance(value, bool):
        return value
    if value is None:
        raise ValueError(f"{field_name}: valeur manquante")
    normalized = str(value).strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"{field_name}: valeur invalide {value!r}")


def build_promote_sql(table_pairs: Sequence[tuple[str, str]]) -> str:
    """Genere le SQL de promotion staging -> production dans une transaction."""
    statements = ["BEGIN;"]
    for target, staging in table_pairs:
        statements.append(f"TRUNCATE TABLE {target};")
        statements.append(f"INSERT INTO {target} SELECT * FROM {staging};")
    statements.append("COMMIT;")
    return "\n".join(statements)
