# Streaming + stockage + dashboard

Brique streaming du projet : **Kafka + Spark Streaming** pour le traitement en direct, **Postgres** pour le stockage des resultats (batch + streaming), **Grafana** pour le dashboard.

```
producer.py ──▶ Kafka (topic "bank-customers") ──▶ streaming_job.py (Spark) ──▶ Postgres ──▶ Grafana
                                                                                  ▲
                                         load_batch_results.py (sortie batch-churn) ┘
```

## Composants

- **`docker-compose.yml`** : Kafka (`apache/kafka`, mode KRaft, un seul noeud), Postgres 16 (port hote **5433**), Grafana (port 3000). Reseau dedie `churn-streaming`, separe du reseau `hadoop`.
- **`producer.py`** : lit `../data/Customer-Churn-Records.csv` et envoie un message JSON par client sur le topic `bank-customers` (`--rate` messages/s, `--shuffle`, `--loop`, `--limit`, `--dry-run`). Le fichier n'a pas de date : `event_time` est ajoute a l'envoi.
- **`streaming_job.py`** : job PySpark Structured Streaming (Spark 3.5.3) qui consomme le topic et ecrit dans Postgres via `foreachBatch` (micro-batch toutes les 10 s).
- **`load_batch_results.py`** : charge la sortie de `batch-churn` dans `batch_pays_tranche`.
- **`sql/schema.sql`** : schema Postgres, applique au premier demarrage du conteneur.
- **`grafana/`** : datasource Postgres et dashboard `customer-churn.json` provisionnes automatiquement.

## Metriques streaming

| Table | Contenu |
|---|---|
| `streaming_cumule` | clients traites, solde total, nombre d'alertes (cumules) |
| `streaming_pays` | clients, solde et somme des scores de credit par pays |
| `streaming_tranche` | clients et solde par tranche d'age |
| `streaming_top_soldes` | top 10 des soldes du micro-batch courant |
| `streaming_alertes` | solde > 150 000 (`SOLDE_ELEVE`) ou CreditScore < 500 (`SCORE_FAIBLE`) |

Comme dans le projet de reference, une « fenetre » de `streaming_top_soldes` est un micro-batch Spark (pas une fenetre sur le temps evenementiel), et les autres tables sont de vrais cumuls. **Rejouer le producteur plusieurs fois double les cumuls** : pour repartir de zero, `docker compose down -v` puis `up -d`, ou `TRUNCATE` des tables `streaming_*`, et supprimer le dossier `checkpoint/`.

## Lancer la stack

```bash
cd streaming
docker compose up -d                       # Kafka, Postgres, Grafana
pip install kafka-python "pyspark==3.5.3" psycopg2-binary pandas

python load_batch_results.py               # resultats batch -> Postgres

# terminal 1 : job Spark Streaming (demarrer avant le producteur : startingOffsets=latest)
python streaming_job.py

# terminal 2 : producteur
python producer.py --rate 50 --shuffle     # 10 000 clients en ~3 min
```

Dashboard : http://localhost:3000 (admin/admin, ou acces anonyme en lecture). Spark a besoin de Java 8, 11 ou 17 (ou 21 avec Spark 3.5).

## Statut

Verifie en local avec un vrai Postgres 16 et PySpark 3.5.3 (sans Kafka) :
- `schema.sql` s'applique sans erreur ; `load_batch_results.py` charge les 15 lignes pays x tranche (totaux par pays corrects).
- `streaming_job.process_batch` alimente toutes les tables a partir de 10 000 messages produits par `producer.py --dry-run` (2 micro-batches) : 10 000 clients, solde total 764 858 892,88, 969 alertes `SOLDE_ELEVE` + 632 `SCORE_FAIBLE`, totaux par pays et par tranche identiques au batch.

**Non verifie** : lecture reelle depuis Kafka (`docker compose`), connecteur `spark-sql-kafka`, demarrage de Grafana et affichage du dashboard. A tester sur votre machine.
