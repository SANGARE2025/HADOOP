"""
Job Spark Structured Streaming : consomme le topic Kafka "bank-customers" et
maintient des agregats en direct dans Postgres.

Design (meme choix que le projet de reference) : on utilise foreachBatch, donc
chaque micro-batch (TRIGGER_SECONDS) est traite comme un DataFrame statique.
Une "fenetre" de streaming_top_soldes = un micro-batch.

Metriques ecrites dans Postgres (voir sql/schema.sql) :
- streaming_cumule      : nombre de clients, solde total et nombre d'alertes cumules.
- streaming_pays        : clients, solde et somme des scores de credit par pays.
- streaming_tranche     : clients et solde par tranche d'age.
- streaming_top_soldes  : top 10 des soldes du micro-batch courant.
- streaming_alertes     : clients avec solde > 150 000 ou score de credit < 500.

Usage:
    python streaming_job.py
"""

import os

import psycopg2
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (col, count as spark_count, from_json, max as spark_max,
                                   min as spark_min, row_number, sum as spark_sum, when)
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField,
                               StructType)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "bank-customers")
TRIGGER_SECONDS = "10 seconds"
TOP_N = 10
SEUIL_SOLDE = 150000.0
SEUIL_SCORE = 500

PG_DSN = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", "5433")),
    dbname=os.environ.get("PG_DB", "churn"),
    user=os.environ.get("PG_USER", "churn"),
    password=os.environ.get("PG_PASSWORD", "churn"),
)

SCHEMA = StructType([
    StructField("customer_id", LongType()),
    StructField("credit_score", IntegerType()),
    StructField("geography", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("tenure", IntegerType()),
    StructField("balance", DoubleType()),
    StructField("num_of_products", IntegerType()),
    StructField("event_time", StringType()),
])


def parse(raw_df):
    """DataFrame avec une colonne 'value' (JSON) -> DataFrame type, avec tranche d'age."""
    return (
        raw_df.select(from_json(col("value").cast("string"), SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", col("event_time").cast("timestamp"))
        .withColumn(
            "tranche",
            when(col("age") < 30, "<30")
            .when(col("age") < 40, "30-39")
            .when(col("age") < 50, "40-49")
            .when(col("age") < 60, "50-59")
            .otherwise("60+"),
        )
    )


def get_conn():
    return psycopg2.connect(**PG_DSN)


def process_batch(batch_df, batch_id: int) -> None:
    batch_df = batch_df.cache()
    total = batch_df.count()
    if total == 0:
        batch_df.unpersist()
        return

    conn = get_conn()
    try:
        agg = batch_df.agg(
            spark_sum("balance").alias("solde"),
            spark_count("*").alias("n"),
            spark_min("event_time").alias("min_ts"),
            spark_max("event_time").alias("max_ts"),
        ).collect()[0]

        # --- alertes -------------------------------------------------------
        alertes = batch_df.filter(
            (col("balance") > SEUIL_SOLDE) | (col("credit_score") < SEUIL_SCORE)
        ).collect()
        alert_rows = []
        for r in alertes:
            if r["balance"] is not None and r["balance"] > SEUIL_SOLDE:
                alert_rows.append((r["event_time"], r["customer_id"], r["geography"],
                                   "SOLDE_ELEVE", r["balance"], r["credit_score"]))
            if r["credit_score"] is not None and r["credit_score"] < SEUIL_SCORE:
                alert_rows.append((r["event_time"], r["customer_id"], r["geography"],
                                   "SCORE_FAIBLE", r["balance"], r["credit_score"]))

        with conn.cursor() as cur:
            # cumul global
            cur.execute(
                """
                INSERT INTO streaming_cumule (id, nb_clients, solde_total, nb_alertes, updated_at)
                VALUES (1, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE
                    SET nb_clients  = streaming_cumule.nb_clients + EXCLUDED.nb_clients,
                        solde_total = streaming_cumule.solde_total + EXCLUDED.solde_total,
                        nb_alertes  = streaming_cumule.nb_alertes + EXCLUDED.nb_alertes,
                        updated_at  = now()
                """,
                (int(agg["n"]), float(agg["solde"] or 0.0), len(alert_rows)),
            )

            # par pays
            for r in (batch_df.groupBy("geography")
                      .agg(spark_count("*").alias("n"), spark_sum("balance").alias("solde"),
                           spark_sum("credit_score").alias("score")).collect()):
                cur.execute(
                    """
                    INSERT INTO streaming_pays (pays, nb_clients, solde_total, score_total, updated_at)
                    VALUES (%s, %s, %s, %s, now())
                    ON CONFLICT (pays) DO UPDATE
                        SET nb_clients  = streaming_pays.nb_clients + EXCLUDED.nb_clients,
                            solde_total = streaming_pays.solde_total + EXCLUDED.solde_total,
                            score_total = streaming_pays.score_total + EXCLUDED.score_total,
                            updated_at  = now()
                    """,
                    (r["geography"], int(r["n"]), float(r["solde"] or 0.0), float(r["score"] or 0.0)),
                )

            # par tranche d'age
            for r in (batch_df.groupBy("tranche")
                      .agg(spark_count("*").alias("n"), spark_sum("balance").alias("solde")).collect()):
                cur.execute(
                    """
                    INSERT INTO streaming_tranche (tranche, nb_clients, solde_total, updated_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (tranche) DO UPDATE
                        SET nb_clients  = streaming_tranche.nb_clients + EXCLUDED.nb_clients,
                            solde_total = streaming_tranche.solde_total + EXCLUDED.solde_total,
                            updated_at  = now()
                    """,
                    (r["tranche"], int(r["n"]), float(r["solde"] or 0.0)),
                )

            # top soldes du micro-batch
            ranked = (batch_df.withColumn("rang", row_number().over(Window.orderBy(col("balance").desc())))
                      .filter(col("rang") <= TOP_N).collect())
            for r in ranked:
                cur.execute(
                    """
                    INSERT INTO streaming_top_soldes
                        (window_start, window_end, customer_id, pays, solde, credit_score)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (window_start, customer_id) DO UPDATE
                        SET window_end = EXCLUDED.window_end, solde = EXCLUDED.solde
                    """,
                    (agg["min_ts"], agg["max_ts"], r["customer_id"], r["geography"],
                     r["balance"], r["credit_score"]),
                )

            # alertes
            if alert_rows:
                cur.executemany(
                    """
                    INSERT INTO streaming_alertes
                        (event_time, customer_id, pays, type, solde, credit_score)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    alert_rows,
                )
        conn.commit()
        print(f"[batch {batch_id}] {total} clients | solde batch = {agg['solde']:.2f} | "
              f"{len(alert_rows)} alertes | fenetre {agg['min_ts']} -> {agg['max_ts']}")
    finally:
        conn.close()
        batch_df.unpersist()


def main() -> None:
    spark = (
        SparkSession.builder.appName("churn-streaming")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    query = (
        parse(raw).writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", "./checkpoint")
        .trigger(processingTime=TRIGGER_SECONDS)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
