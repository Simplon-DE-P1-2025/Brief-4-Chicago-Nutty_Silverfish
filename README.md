# Pipeline Chicago Crimes

Ce repo repart du bon ordre logique pour reconstruire le pipeline.

## Etape actuelle

Le repo contient maintenant uniquement le premier socle utile :

- un vrai DAG `chicago_crimes_pipeline`
- la creation des tables `raw`, `transformed` et de leurs tables `_staging`
- une ingestion minimale depuis l'API Chicago vers `chicago_crimes_raw_staging`
- une connexion Airflow locale `chicago_crimes_db`
- la structure Soda prete pour la suite

## Ordre de reconstruction

1. Creer les tables PostgreSQL
2. Ingerer les donnees brutes en staging
3. Transformer les donnees en pandas
4. Ajouter les checks Soda
5. Ajouter la promotion atomique staging -> production
6. Ajouter les tests

## Lancer le projet

```bash
astro dev start

astro dev run connections add chicago_crimes_db \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema postgres \
  --conn-login postgres \
  --conn-password postgres \
  --conn-port 5432
```

Puis declencher le DAG :

```bash
astro dev run dags trigger chicago_crimes_pipeline
```
