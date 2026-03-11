# Nettoyage post-sandbox

Ce document liste les actions de nettoyage a effectuer une fois que le pipeline
sandbox a rempli son role et que les resultats ont ete valides.

---

## Nettoyage sandbox

Le pipeline sandbox a ete cree pour tester les ameliorations (filtres geo + dates
futures) sur les 8M+ lignes existantes avant de les integrer au pipeline principal.
Les checks SODA daily et bootstrap ont ete enrichis avec ces filtres. Le sandbox
n'a plus de raison d'exister.

### Fichiers a supprimer

| Fichier | Raison |
|---------|--------|
| `dags/chicago_crimes_sandbox.py` | DAG sandbox, plus utile apres validation |
| `include/soda/checks/sandbox_transformed_checks.yml` | Checks SODA pour la table sandbox |
| `tests/test_sandbox.py` | Tests des fonctions sandbox |

### Code a retirer dans `dags/pipeline_utils.py`

Les elements suivants (ajoutees apres la ligne 331) peuvent etre supprimes :

- `SANDBOX_TABLE` (constante)
- `create_sandbox_table()` (fonction)
- `transform_raw_to_table_sql()` (fonction)

**Note** : `transform_raw_to_table_sql()` fait la transformation entierement en SQL
(pas de pandas). Elle pourrait etre utile a l'avenir si le volume quotidien augmente
et que `transform_and_load()` (pandas) devient un goulot. A garder si on prefere
conserver cette option. Voir `docs/CODE_REVIEW.md` section "Perspective".

### Table a supprimer en base

```sql
DROP TABLE IF EXISTS chicago_crimes_transformed_v2;
```

---

## Fichiers non trackes a traiter

| Fichier | Etat | Action suggeree |
|---------|------|-----------------|
| `requirements.txt` | Vide (0 octets) | Supprimer ou remplir avec les deps du projet |
| `.astro/config.yaml` | Port modifie 5433 -> 5432, non committe | Committer si 5432 est le bon port |
| `docs/CODE_REVIEW.md` | Non committe | Committer si on veut garder la revue dans le repo |

---

## Tests existants

### `tests/test_chicago_crimes_pipeline.py` (7 tests)

Tests unitaires du pipeline principal. Ils verifient les fonctions de
`pipeline_utils.py` et le comportement du DAG daily **sans avoir besoin de
PostgreSQL ni de l'API Chicago**. Ils utilisent un `RecordingHook` qui capture
les requetes SQL sans les executer.

| Test | Ce qu'il verifie |
|------|-----------------|
| `test_build_recent_where_clause_uses_expected_window` | La clause SoQL de fenetre temporelle (14 jours) genere la bonne date |
| `test_collect_crimes_data_paginates_until_partial_batch` | La pagination s'arrete quand un batch est incomplet |
| `test_collect_crimes_data_returns_empty_when_first_batch_is_empty` | Retourne une liste vide si l'API ne renvoie rien |
| `test_build_upsert_sql_does_not_update_conflict_key` | L'UPSERT ne met pas a jour la colonne `id` (cle de conflit) |
| `test_prepare_pipeline_tables_creates_prod_and_custom_staging_tables` | Creation des tables prod + staging avec les bons noms |
| `test_upsert_staging_to_production_uses_given_staging_tables` | L'UPSERT utilise bien les tables de staging passees en parametre |
| `test_daily_raw_quality_check_skips_when_ingestion_is_empty` | Le check SODA skip (AirflowSkipException) quand 0 lignes ingerees |

### `tests/test_sandbox.py` (5 tests)

Tests unitaires des fonctions sandbox. Meme pattern que ci-dessus (RecordingHook).
A supprimer avec le reste du sandbox si on nettoie.

| Test | Ce qu'il verifie |
|------|-----------------|
| `test_transform_raw_to_table_sql_generates_geo_filter` | Le SQL genere contient les bornes lat/lon de Chicago |
| `test_transform_raw_to_table_sql_generates_future_date_filter` | Le SQL genere contient `<= NOW()` |
| `test_transform_raw_to_table_sql_without_geo_filter` | `geo_filter=False` retire les clauses geo du SQL |
| `test_transform_raw_to_table_sql_truncates_target` | `truncate_target=True` genere un `TRUNCATE TABLE` |
| `test_create_sandbox_table_uses_transformed_schema` | La table sandbox utilise le schema `TRANSFORMED_TABLE_SCHEMA` |

### Comment les lancer

```bash
# Tous les tests
astro dev pytest

# Un fichier specifique
astro dev pytest tests/test_chicago_crimes_pipeline.py

# Avec detail
astro dev pytest --args "-v"
```

Les tests necessitent Airflow (disponible dans le conteneur Astro). Ils ne
tournent pas en local avec `pytest` directement.
