# Projet de cours - Architecture Big Data (batch + streaming + dashboard)

Source de donnees : **Customer Churn Records** (10 000 clients d'une banque, France / Allemagne / Espagne) - voir [`data/README.md`](data/README.md).

## Architecture

```
                 ┌──────────────────────┐
 CSV source ────▶│  Producteur replay   │──▶ Kafka (topic "bank-customers")
 (Customer Churn)│  (simule le temps    │            │
                 │   reel ligne/ligne)  │            ▼
                 └──────────────────────┘   Spark Structured Streaming
                                             (cumuls, top soldes, alertes)
 CSV ──▶ HDFS ──▶ Jobs MapReduce                       │
                  (batch-churn)                        │
                       │                               ▼
                       └─────────────▶ Postgres ◀──────┘
                                           │
                                           ▼
                                  Grafana (temps reel + historique)
```

## Modules

| Dossier | Role | Statut |
|---|---|---|
| [`batch-churn/`](batch-churn/README.md) | MapReduce : clients et solde par pays x tranche d'age | code ecrit ; sortie attendue simulee, a executer sur le cluster |
| [`streaming/`](streaming/README.md) | Kafka + Spark Streaming + Postgres + Grafana | logique Spark/Postgres verifiee en local ; Kafka et Grafana a tester |
| [`ml/`](ml/README.md) | Modele de prediction (scikit-learn) | execute, voir README |
| `scripts/` | `explore.py`, `simulate_batch.py` | executes |

## Decisions techniques

- **Streaming** : Kafka + Spark Streaming. **Dashboard** : Grafana. **Resultats agreges** : Postgres.
- Le flux est simule (le CSV est statique, sans date) : le producteur rejoue les clients a cadence reguliere.

## Limites

- Pas de colonne `Exited` : pas de prediction du churn avec ce fichier.
- Cumuls streaming non idempotents : rejouer le flux double les valeurs (voir `streaming/README.md`).
