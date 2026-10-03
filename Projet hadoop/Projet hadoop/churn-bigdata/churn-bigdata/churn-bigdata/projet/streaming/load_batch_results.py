"""
Charge la sortie du job batch (batch-churn) dans Postgres (table batch_pays_tranche),
pour que le dashboard Grafana combine vue batch (historique) et vue streaming.

Usage:
    python load_batch_results.py [chemin_vers_part-r-00000]

Par defaut, utilise batch-churn/src/main/resources/output-full/part-r-00000.
Format d'une ligne : pays<TAB>tranche<TAB>nb_clients<TAB>solde_total
"""

import os
import sys
from pathlib import Path

import psycopg2

DEFAULT_OUTPUT = (
    Path(__file__).resolve().parent.parent
    / "batch-churn" / "src" / "main" / "resources" / "output-full" / "part-r-00000"
)

PG_DSN = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", "5433")),
    dbname=os.environ.get("PG_DB", "churn"),
    user=os.environ.get("PG_USER", "churn"),
    password=os.environ.get("PG_PASSWORD", "churn"),
)


def main() -> None:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUTPUT
    print(f"Lecture de {path} ...")

    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            pays, tranche, nb, solde = line.split("\t")
            rows.append((pays, tranche, int(nb), float(solde)))
    print(f"{len(rows)} lignes pays x tranche a charger.")

    conn = psycopg2.connect(**PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO batch_pays_tranche (pays, tranche, nb_clients, solde_total)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (pays, tranche)
                DO UPDATE SET nb_clients = EXCLUDED.nb_clients, solde_total = EXCLUDED.solde_total
                """,
                rows,
            )
        conn.commit()
        print("Charge dans Postgres (table batch_pays_tranche).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
