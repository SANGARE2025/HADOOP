"""
Producteur Kafka "replay" : rejoue Customer-Churn-Records.csv vers Kafka,
un message JSON par client, a cadence reguliere (le fichier n'a pas de date :
l'horodatage event_time est ajoute au moment de l'envoi).

Usage:
    python producer.py [--rate 20] [--limit 0] [--shuffle] [--loop] [--topic bank-customers]
    python producer.py --limit 5 --dry-run        # affiche les messages sans Kafka

--rate    : messages par seconde (defaut 20 -> 10 000 clients en ~8 min).
--limit   : nombre max de lignes a envoyer (0 = tout le fichier).
--shuffle : melange les lignes (graine fixe) pour un flux moins ordonne.
--loop    : rejoue le fichier en boucle (flux infini pour la demo).
--dry-run : n'envoie rien, affiche les messages (test sans Kafka).
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "Customer-Churn-Records.csv"


def to_message(row) -> dict:
    return {
        "customer_id": int(row.CustomerId),
        "credit_score": int(row.CreditScore),
        "geography": str(row.Geography),
        "gender": str(row.Gender),
        "age": int(row.Age),
        "tenure": int(row.Tenure),
        "balance": float(row.Balance),
        "num_of_products": int(row.NumOfProducts),
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=20.0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--csv", type=str, default=str(DATA_PATH))
    parser.add_argument("--topic", type=str, default="bank-customers")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    if args.shuffle:
        df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)
    if args.limit:
        df = df.head(args.limit)
    print(f"{len(df)} lignes a rejouer depuis {args.csv}")

    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            linger_ms=20,
        )

    delay = 1.0 / args.rate if args.rate > 0 else 0.0
    sent = 0
    t0 = time.time()
    try:
        while True:
            for row in df.itertuples(index=False):
                message = to_message(row)
                if args.dry_run:
                    print(json.dumps(message))
                else:
                    producer.send(args.topic, key=message["geography"], value=message)
                sent += 1
                if not args.dry_run and sent % 500 == 0:
                    print(f"  {sent} messages envoyes ({time.time() - t0:.1f}s)")
                if delay and not args.dry_run:
                    time.sleep(delay)
            if not args.loop:
                break
    except KeyboardInterrupt:
        print("Interrompu par l'utilisateur.")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
        print(f"Termine : {sent} messages (topic '{args.topic}').")


if __name__ == "__main__":
    main()
