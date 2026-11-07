# 📈 Stock Market Streaming Analytics — Projetu Capstone

Analíze folin aksaun no tendénsia merkadu real-time ba **kompañia 4**:
**Bank Mandiri • Telkomsel (TLKM) • Bank ANZ • Bank BRI**

Sistema tomak halao iha **Docker Desktop** liuhusi komandu ida de'it: `docker compose up`.
La presiza instala Kafka, Spark, PostgreSQL, MongoDB, ka HDFS manuál.

---

## 🏗️ Arsitetura Sistema

```
[ Dataset CSV Kompañia 4 ]
            │
            ▼
   ┌─────────────────┐
   │  Producer (Py)   │  → lee CSV, haruka liña dadus ida-ida (simulasaun real-time)
   └────────┬─────────┘
            ▼
   ┌─────────────────┐
   │  Apache Kafka    │  topic: stock-raw
   │  (+ Kafka UI)    │
   └────────┬─────────┘
            ▼
   ┌─────────────────────────┐
   │ Apache Spark Streaming   │  hamoos dadus, hanesan formatu,
   │ (Structured Streaming)   │  sura % mudansa folin no volatilidade
   └───┬───────┬───────┬─────┘
       ▼       ▼       ▼
  PostgreSQL  MongoDB  HDFS
 (estruturadu)(fleksivel)(parquet)
       │       │
       └───┬───┘
           ▼
   ┌─────────────────┐
   │ Streamlit        │  → http://localhost:8501
   │ Dashboard        │
   └─────────────────┘
```

## 📁 Estrutura Pasta

```
capstone-stock-streaming/
├── docker-compose.yml          <- halao SERVISU HOTU
├── data/                       <- dataset kompañia 4 (SIRA SAI PREENCHIDU ONA)
│   ├── MANDIRI.csv
│   ├── TLKM.csv
│   ├── ANZ.csv
│   └── BBRI.csv
├── producer/                   <- Kafka producer (Python)
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
├── spark/                      <- Spark Structured Streaming job
│   ├── Dockerfile
│   └── stream_processor.py
├── postgres/
│   └── init.sql                <- esquema tabela kria automatikamente bainhira komesa
└── dashboard/                  <- Streamlit dashboard
    ├── Dockerfile
    ├── app.py
    └── requirements.txt
```

---

## ✅ Ita-nia Dataset — Prontu Atu Uza

Dataset ne'ebé Ita upload ona hatama diretamente ba pasta `data/` ho naran padraun:

| Fitxeiru Ita-nian                       | Troka ba           |
|------------------------------------------|--------------------|
| `Stock_TLKM_2005-2024.csv`              | `data/TLKM.csv`    |
| `BBRI_JK.csv`                           | `data/BBRI.csv`    |
| `DATA_SET_ANZ.csv`                      | `data/ANZ.csv`     |
| `MANDIRI.csv`                           | `data/MANDIRI.csv` |

**Ita la presiza halo buat ruma atu hatama dataset** — producer sei lee automatikamente
fitxeiru `.csv` hotu iha pasta `data/` bainhira sistema halao, no sei hanesan
formatu koluna automatikamente (koluna sira hanesan `Open/open`, `Adj Close/adjclose`,
no seluk tan ne'ebé la hanesan iha fitxeiru ida-idak sei normaliza automatikamente).

Se iha tempu ruma Ita hakarak troka dataset:
1. Troka/aumenta fitxeiru CSV iha pasta `data/` (formatu koluna livre: `Date, Open, High, Low, Close, Adj Close/adjclose, Volume`).
2. Naran fitxeiru (la inklui `.csv`) sei uza hanesan kódigu ticker.
3. Halao fali: `docker compose restart producer`.

---

## 🚀 Oinsá Atu Halao

### 1. Rekizitu
- Instala **Docker Desktop** (Windows/Mac/Linux) no asegura katak nia **running** ona.
- Iha Docker Desktop → Settings → Resources, fó RAM **GB 6–8** ka liu
  (sistema ne'e halao Kafka + Spark + HDFS + database rua hamutuk).

### 2. Halao Servisu Hotu
Loke terminal iha pasta projetu ne'e, depois halao:

```bash
docker compose up -d --build
```

Komandu ne'e sei:
- Harii image ba `producer`, `spark-app`, no `dashboard`.
- Halao Kafka, HDFS (namenode+datanode), PostgreSQL, MongoDB.
- Kria tabela PostgreSQL automatikamente husi `postgres/init.sql`.
- Komesa haruka dadus husi dataset ba Kafka → Spark → database.

Verifika estadu container hotu:
```bash
docker compose ps
```

Haree log husi komponente ida-idak (opsionál, atu debugging):
```bash
docker compose logs -f producer      # haree dadus ne'ebé haruka
docker compose logs -f spark-app     # haree prosesu Spark Streaming
docker compose logs -f dashboard
```

> ⏱️ **Nota:** Spark image download connector Kafka iha primeiru vez bainhira komesa
> (presiza internet no tempu minutu 1–3 iha komesu). Depois ne'e prosesu sei lao normal.

### 3. Loke Dashboard iha Browser

| Servisu                        | URL                                              | Utilidade                                   |
|--------------------------------|---------------------------------------------------|----------------------------------------------|
| **Dashboard Prinsipál**        | http://localhost:8501                             | Gráfiku no komparasaun kompañia 4            |
| Kafka UI (monitor topic)       | http://localhost:8081                             | Haree fluxu mensajen Kafka                   |
| Mongo Express (haree MongoDB)  | http://localhost:8082                             | Haree dadus la estruturadu                   |
| HDFS Web UI                    | http://localhost:9870                             | Haree fitxeiru parquet ne'ebé armazena iha HDFS |
| Spark UI                       | http://localhost:4040                             | Monitor job streaming Spark                  |
| PostgreSQL                     | `localhost:5432` (db: `stockdb`, user: `stockuser`, pass: `stockpass`) | Konekta liuhusi DBeaver/pgAdmin |

Loke **http://localhost:8501** iha Edge/Firefox/Chrome — dashboard sei prense
automatikamente iha segundu uitoan depois dadus komesa lao, no **auto-refresh
kada segundu 5** atu hatudu rezultadu foun.

### 4. Para Sistema
```bash
docker compose down          # para container hotu (dadus nafatin armazena)
docker compose down -v       # para + hamoos dadus hotu (reset totál)
```

---

## 🔍 Saida mak Akontese iha Kotuk

1. **Producer** lee fitxeiru CSV 4, hanesan naran koluna, hamutuk no
   hakbiit tuir data, depois haruka liña ida kada segundu 0.5
   ba topic Kafka `stock-raw` (simula dadus real-time). Bainhira dataset
   remata, nia fila fali ba komesu atu demo nafatin "moris".
2. **Spark Structured Streaming** lee topic ne'e kada segundu 5,
   hamoos dadus, depois sura:
   - `price_change` = close − open
   - `pct_change` = (close − open) / open × 100
   - `volatility_pct` = (high − low) / low × 100
   - `trend` = NAIK / TURUN / STABIL
3. Rezultadu hakerek ba **fatin 3 hamutuk**:
   - `PostgreSQL.stock_prices` → dadus estruturadu, fonte prinsipál ba dashboard.
   - `MongoDB.stock_prices_raw` & `MongoDB.company_performance` → dadus
     fleksivel/rezumu performansa kada batch.
   - `HDFS /stock-data/processed` → arkivu dadus istóriku formatu Parquet.
4. **Streamlit Dashboard** lee `PostgreSQL.stock_prices` kada segundu 5 no
   hatudu tendénsia folin, komparasaun performansa, volatilidade, no volume kompañia 4.

---

## 🛠️ Personalizasaun

| Hakarak muda...                             | Muda iha...                                              |
|----------------------------------------------|-----------------------------------------------------------|
| Velosidade simulasaun streaming              | `STREAM_DELAY_SECONDS` iha `docker-compose.yml` (producer) |
| Intervalu prosesamentu Spark                 | `trigger(processingTime="5 seconds")` iha `stream_processor.py` |
| Intervalu refresh dashboard                  | `st_autorefresh(interval=5000, ...)` iha `dashboard/app.py` |
| Aumenta kompañia ba-5, ba-6, no seluk tan    | Aumenta fitxeiru CSV foun iha `data/`                      |

---

## 🐞 Troubleshooting

- **Dashboard hatudu "Menunggu data..." nafatin** → verifika `docker compose logs -f spark-app`,
  asegura la iha erru konesaun ba Kafka/Postgres. Hein minutu 1-2 iha komesu
  (Spark presiza tempu atu download dependénsia Kafka connector iha primeiru vez).
- **Container kafka/namenode reinisia nafatin** → normalmente RAM Docker Desktop
  la sufisiente. Aumenta alokasaun RAM iha Docker Desktop Settings.
- **Port uza ona (port already in use)** → muda mapeamentu port iha
  `docker-compose.yml`, ezemplu `"8501:8501"` ba `"8600:8501"`.
- **Erru `docker.io/bitnami/kafka:3.7: not found`** ka **`docker.io/bitnami/spark:3.5: not found`** →
  Ida ne'e laos Ita-nia sala konfigurasaun. Husi Agostu 2025, Bitnami muda
  image ho tag versaun espesífiku barak ba pakote ne'ebé selu, nune'e la gratis
  tan iha Docker Hub. Projetu ne'e uza ona image ofisiál hanesan substitutu:
  **`apache/kafka:3.7.0`** no **`apache/spark:3.5.1-python3`**.
  Se Ita sei iha versaun tuan projetu ne'e, download fali ZIP
  foun, depois halao fali:
  ```bash
  docker compose down
  docker compose up -d --build
  ```
- **Log `spark-app` hatudu prosesu download repete no naruk tebes** (`downloading https://repo1.maven.org/...`, depois `exited with code 1 (restarting)`) →
  Ida ne'e akontese tanba versaun inísiu projetu ne'e uza `--packages` ne'ebé halo Spark
  download fali biblioteka rihun ba rihun MB husi internet **kada vez container
  reinisia**. Iha konesaun internet neineik/la estável, download dala barak tun iha
  klaran no Spark falla nafatin. Projetu ne'e hadi'a ona:
  biblioteka hotu agora *bake* diretamente ba Docker image bainhira `build`
  (dala ida de'it), nune'e container komesa la presiza internet tan no lalais liu
  no estável liu. Se Ita sei iha versaun tuan, download fali ZIP
  foun no halao:
  ```bash
  docker compose down
  docker compose up -d --build
  ```

---

## 🧰 Teknolojia ne'ebé Uza
Python • Apache Kafka (modu KRaft) • Apache Spark Structured Streaming •
PostgreSQL • MongoDB • HDFS (Hadoop) • Streamlit • Plotly • Docker & Docker Compose
