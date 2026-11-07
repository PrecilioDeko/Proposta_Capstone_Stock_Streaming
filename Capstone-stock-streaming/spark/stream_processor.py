"""
STREAM PROCESSING — Apache Spark Structured Streaming
=======================================================
1. Membaca data mentah dari Kafka topic `stock-raw`.
2. Membersihkan & menyamakan format data dari 4 perusahaan.
3. Menghitung: perubahan harga, persentase naik/turun, volatilitas harian.
4. Menyimpan:
   - Data detail hasil olahan  -> PostgreSQL (terstruktur)  + HDFS (parquet)
   - Ringkasan performa emiten -> MongoDB (tidak terstruktur / fleksibel)
"""

import os
import logging
from pymongo import MongoClient

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, when, round as spark_round, current_timestamp, to_date, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SPARK] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "stock-raw")
POSTGRES_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/stockdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "stockuser")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "stockpass")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017")
HDFS_URI = os.environ.get("HDFS_URI", "hdfs://namenode:9000")

JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# Skema pesan JSON yang dikirim oleh producer
RAW_SCHEMA = StructType([
    StructField("ticker", StringType()),
    StructField("company", StringType()),
    StructField("date", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("adjclose", DoubleType()),
    StructField("volume", LongType()),
    StructField("event_time", StringType()),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StockMarketStreamingAnalytics")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.defaultFS", HDFS_URI)
        .getOrCreate()
    )


def clean_and_enrich(raw_df):
    """Membersihkan data & menghitung metrik analitik per baris."""
    df = raw_df.select(
        col("ticker"),
        col("company"),
        to_date(col("date"), "yyyy-MM-dd").alias("trade_date"),
        col("open"), col("high"), col("low"), col("close"), col("adjclose"),
        col("volume"),
        # Konversi string ISO8601 dari producer menjadi tipe timestamp asli,
        # supaya cocok dengan kolom "event_time TIMESTAMP" di PostgreSQL.
        to_timestamp(col("event_time")).alias("event_time"),
    )

    # Buang baris tidak valid (harga kosong / volume negatif)
    df = df.filter(
        col("trade_date").isNotNull()
        & col("open").isNotNull()
        & col("close").isNotNull()
        & (col("open") > 0)
    )

    df = df.withColumn("price_change", spark_round(col("close") - col("open"), 4))
    df = df.withColumn(
        "pct_change",
        spark_round(((col("close") - col("open")) / col("open")) * 100, 4),
    )
    df = df.withColumn(
        "volatility_pct",
        spark_round(
            when(col("low") > 0, ((col("high") - col("low")) / col("low")) * 100).otherwise(None),
            4,
        ),
    )
    df = df.withColumn(
        "trend",
        when(col("pct_change") > 0, "NAIK")
        .when(col("pct_change") < 0, "TURUN")
        .otherwise("STABIL"),
    )
    df = df.withColumn("processed_at", current_timestamp())
    return df


def write_to_postgres(batch_df, batch_id):
    (
        batch_df.write
        .jdbc(url=POSTGRES_URL, table="stock_prices", mode="append", properties=JDBC_PROPERTIES)
    )
    log.info(f"[Batch {batch_id}] {batch_df.count()} baris ditulis ke PostgreSQL (stock_prices).")


def write_to_hdfs(batch_df, batch_id):
    path = f"{HDFS_URI}/stock-data/processed"
    (
        batch_df.write
        .mode("append")
        .parquet(path)
    )
    log.info(f"[Batch {batch_id}] Data ditulis ke HDFS: {path}")


def write_performance_summary_to_mongo(batch_df, batch_id):
    """Agregasi ringkas per emiten untuk micro-batch ini -> disimpan ke MongoDB."""
    agg = (
        batch_df.groupBy("ticker", "company")
        .agg(
            {"pct_change": "avg", "volatility_pct": "avg", "close": "last", "volume": "sum"}
        )
    )
    rows = agg.collect()
    if not rows:
        return

    client = MongoClient(MONGO_URI)
    coll = client["stockdb"]["company_performance"]

    docs = []
    for r in rows:
        docs.append({
            "batch_id": batch_id,
            "ticker": r["ticker"],
            "company": r["company"],
            "avg_pct_change": r["avg(pct_change)"],
            "avg_volatility_pct": r["avg(volatility_pct)"],
            "last_close": r["last(close)"],
            "total_volume": r["sum(volume)"],
        })
    if docs:
        coll.insert_many(docs)
    client.close()
    log.info(f"[Batch {batch_id}] Ringkasan performa {len(docs)} emiten ditulis ke MongoDB (company_performance).")


def write_raw_json_to_mongo(batch_df, batch_id):
    """Simpan juga data mentah hasil bersih ke MongoDB sebagai koleksi fleksibel."""
    pdf = batch_df.toPandas()
    if pdf.empty:
        return
    pdf["trade_date"] = pdf["trade_date"].astype(str)
    pdf["processed_at"] = pdf["processed_at"].astype(str)

    client = MongoClient(MONGO_URI)
    coll = client["stockdb"]["stock_prices_raw"]
    coll.insert_many(pdf.to_dict("records"))
    client.close()
    log.info(f"[Batch {batch_id}] {len(pdf)} dokumen ditulis ke MongoDB (stock_prices_raw).")


def process_batch(batch_df, batch_id):
    batch_df.persist()
    try:
        if batch_df.count() == 0:
            return
        write_to_postgres(batch_df, batch_id)
        write_performance_summary_to_mongo(batch_df, batch_id)
        write_raw_json_to_mongo(batch_df, batch_id)
        write_to_hdfs(batch_df, batch_id)
    except Exception as e:
        log.error(f"[Batch {batch_id}] Gagal memproses batch: {e}")
    finally:
        batch_df.unpersist()


def main():
    log.info("=== STOCK MARKET STREAM PROCESSOR (Spark Structured Streaming) ===")
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw_stream.select(
        from_json(col("value").cast("string"), RAW_SCHEMA).alias("data")
    ).select("data.*")

    enriched = clean_and_enrich(parsed)

    query = (
        enriched.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{HDFS_URI}/stock-data/checkpoints")
        .trigger(processingTime="5 seconds")
        .outputMode("update")
        .start()
    )

    log.info("Spark Streaming job berjalan. Menunggu data dari Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
