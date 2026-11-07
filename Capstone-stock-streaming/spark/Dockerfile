FROM apache/spark:3.5.1-python3

USER root
# Pastikan container langsung menjalankan CMD kita (bukan entrypoint bawaan image)
ENTRYPOINT []

# Base image apache/spark minimal, pastikan curl tersedia untuk unduh jar
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# pymongo untuk menulis hasil olahan ke MongoDB dari dalam job Spark
# pandas + pyarrow dibutuhkan agar df.toPandas() berfungsi (dipakai saat
# menulis salinan data mentah ke koleksi MongoDB stock_prices_raw)
# CATATAN: image apache/spark:3.5.1-python3 memakai Python 3.8, sehingga
# versi pandas/pyarrow harus yang masih mendukung Python 3.8 (pandas>=2.1
# sudah mensyaratkan Python>=3.9, jadi TIDAK BISA dipakai di sini).
RUN pip install --no-cache-dir pymongo==4.7.2 "pandas==2.0.3" "pyarrow==14.0.2"

# ---------------------------------------------------------------------
# PENTING: Semua jar konektor Kafka diunduh SEKALI DI SINI saat build
# image (bukan lewat --packages saat container start / restart).
# Ini mencegah Spark mengunduh ulang ratusan MB dari internet setiap
# kali container di-restart -- yang sebelumnya menyebabkan gagal terus
# di koneksi internet lambat/tidak stabil.
# Catatan: hadoop-client-api & hadoop-client-runtime TIDAK diunduh di
# sini karena sudah tersedia bawaan di dalam image apache/spark.
# ---------------------------------------------------------------------
RUN curl -fL --retry 5 --retry-delay 5 --connect-timeout 30 -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -fL --retry 5 --retry-delay 5 --connect-timeout 30 -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -fL --retry 5 --retry-delay 5 --connect-timeout 30 -o /opt/spark/jars/kafka-clients-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fL --retry 5 --retry-delay 5 --connect-timeout 30 -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fL --retry 5 --retry-delay 5 --connect-timeout 30 -o /opt/spark/jars/lz4-java-1.8.0.jar \
      https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar && \
    curl -fL --retry 5 --retry-delay 5 --connect-timeout 30 -o /opt/spark/jars/snappy-java-1.1.10.3.jar \
      https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.3/snappy-java-1.1.10.3.jar

# Driver JDBC PostgreSQL
RUN curl -fL --retry 5 --retry-delay 5 --connect-timeout 30 -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

WORKDIR /app
COPY stream_processor.py .

# Pakai jar lokal yang sudah di-bake di image (TIDAK ada akses internet
# lagi saat container start), jadi start-up cepat & tidak tergantung
# koneksi internet setiap kali restart.
CMD ["/opt/spark/bin/spark-submit", \
     "/app/stream_processor.py"]
