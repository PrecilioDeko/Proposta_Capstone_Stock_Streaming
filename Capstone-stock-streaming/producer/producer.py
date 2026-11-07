"""
PRODUCER — Data Ingestion Layer
=================================
Membaca dataset CSV historis 4 perusahaan (Mandiri, Telkomsel, ANZ, BRI)
lalu mengirim setiap baris data sebagai pesan JSON ke topic Kafka
`stock-raw`, satu per satu dengan jeda waktu (STREAM_DELAY_SECONDS)
untuk mensimulasikan aliran data real-time.

Semua 4 dataset digabung lalu diurutkan berdasarkan tanggal, sehingga
Spark di sisi hilir menerima data dari keempat perusahaan secara
berselang-seling seperti kondisi pasar sungguhan.
"""

import os
import json
import time
import glob
import logging
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PRODUCER] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "stock-raw")
STREAM_DELAY = float(os.environ.get("STREAM_DELAY_SECONDS", "0.5"))
LOOP_FOREVER = os.environ.get("LOOP_FOREVER", "true").lower() == "true"
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")

# Nama tampilan tiap emiten berdasarkan nama file dataset
COMPANY_NAMES = {
    "MANDIRI": "Bank Mandiri",
    "TLKM": "Telkomsel",
    "ANZ": "Bank ANZ",
    "BBRI": "Bank BRI",
}

# Kemungkinan variasi nama kolom di berbagai file dataset -> nama standar
COLUMN_MAP = {
    "date": "date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "adj close": "adjclose",
    "adjclose": "adjclose",
    "volume": "volume",
}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Menyamakan nama kolom dari berbagai format dataset menjadi format baku."""
    rename = {}
    for col in df.columns:
        key = col.strip().lower()
        if key in COLUMN_MAP:
            rename[col] = COLUMN_MAP[key]
    df = df.rename(columns=rename)

    # Pastikan kolom wajib tersedia, isi dengan None jika tidak ada
    required = ["date", "open", "high", "low", "close", "adjclose", "volume"]
    for col in required:
        if col not in df.columns:
            df[col] = None

    return df[required]


def load_all_datasets() -> dict:
    """Membaca semua file CSV di folder data, menormalkan per perusahaan.

    Mengembalikan dict {ticker: DataFrame} (masing-masing sudah diurutkan
    berdasarkan tanggal secara internal), BUKAN satu DataFrame gabungan.
    Ini penting agar producer bisa mengirim data secara *round-robin*
    antar perusahaan (lihat main()), bukan mengurutkan seluruh data
    dari 4 perusahaan jadi satu garis waktu global. Kalau digabung lalu
    diurutkan global, rentang tanggal yang berbeda-beda antar dataset
    membuat sebagian perusahaan baru "muncul" setelah puluhan menit
    streaming -- padahal yang diinginkan adalah ke-4 perusahaan tampil
    beriringan sejak awal.
    """
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    if not files:
        raise FileNotFoundError(
            f"Tidak ada file CSV ditemukan di {DATA_DIR}. "
            "Pastikan 4 dataset (MANDIRI.csv, TLKM.csv, ANZ.csv, BBRI.csv) ada di folder ./data"
        )

    datasets = {}
    for f in files:
        ticker = os.path.splitext(os.path.basename(f))[0].upper()
        company = COMPANY_NAMES.get(ticker, ticker)
        try:
            df = pd.read_csv(f)
        except Exception as e:
            log.warning(f"Gagal membaca {f}: {e}")
            continue

        df = normalize_columns(df)
        df["ticker"] = ticker
        df["company"] = company

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])

        for col in ["open", "high", "low", "close", "adjclose"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")

        df = df.sort_values("date").reset_index(drop=True)
        datasets[ticker] = df
        log.info(f"Dataset dimuat: {f} -> {ticker} ({company}), {len(df)} baris")

    if not datasets:
        raise RuntimeError("Tidak ada dataset valid yang berhasil dimuat.")

    return datasets


def connect_kafka() -> KafkaProducer:
    """Coba konek ke Kafka dengan retry, karena Kafka container butuh waktu untuk siap."""
    for attempt in range(1, 31):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            log.info(f"Terhubung ke Kafka broker: {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            log.info(f"Menunggu Kafka siap... (percobaan {attempt}/30)")
            time.sleep(5)
    raise RuntimeError("Tidak bisa konek ke Kafka setelah beberapa percobaan.")


def row_to_message(row) -> dict:
    return {
        "ticker": row["ticker"],
        "company": row["company"],
        "date": row["date"].strftime("%Y-%m-%d") if pd.notnull(row["date"]) else None,
        "open": None if pd.isna(row["open"]) else float(row["open"]),
        "high": None if pd.isna(row["high"]) else float(row["high"]),
        "low": None if pd.isna(row["low"]) else float(row["low"]),
        "close": None if pd.isna(row["close"]) else float(row["close"]),
        "adjclose": None if pd.isna(row["adjclose"]) else float(row["adjclose"]),
        "volume": int(row["volume"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main():
    log.info("=== STOCK MARKET DATA PRODUCER ===")
    datasets = load_all_datasets()
    total_rows = sum(len(df) for df in datasets.values())
    tickers = list(datasets.keys())
    log.info(
        f"Total {total_rows} baris data siap dialirkan dari {len(tickers)} emiten: {', '.join(tickers)}"
    )
    log.info("Mode pengiriman: ROUND-ROBIN (bergiliran per perusahaan) agar ke-4 "
              "perusahaan tampil beriringan sejak awal, bukan menunggu urutan tanggal gabungan.")

    producer = connect_kafka()

    round_num = 1
    while True:
        log.info(f"--- Mulai putaran streaming #{round_num} ---")
        # Iterator per ticker, supaya bisa diambil satu-satu bergantian
        iterators = {t: datasets[t].iterrows() for t in tickers}
        active = set(tickers)
        sent = 0

        while active:
            for t in list(active):
                try:
                    _, row = next(iterators[t])
                except StopIteration:
                    active.discard(t)
                    continue
                msg = row_to_message(row)
                producer.send(KAFKA_TOPIC, key=msg["ticker"], value=msg)
                sent += 1
                if sent % 200 == 0:
                    producer.flush()
                    log.info(f"Terkirim {sent}/{total_rows} pesan...")
                time.sleep(STREAM_DELAY)

        producer.flush()
        log.info(f"--- Putaran #{round_num} selesai. Total {sent} pesan terkirim ke topic '{KAFKA_TOPIC}' ---")

        if not LOOP_FOREVER:
            break
        round_num += 1
        log.info("LOOP_FOREVER=true -> mengulang streaming dari awal dataset untuk demo real-time berkelanjutan.")


if __name__ == "__main__":
    main()
