"""
VISUALIZATION LAYER — Streamlit Dashboard
============================================
Menampilkan perbandingan kinerja saham 4 perusahaan:
Bank Mandiri, Telkomsel, Bank ANZ, Bank BRI
Data diambil langsung dari PostgreSQL (hasil olahan Spark Streaming).
Bahasa antarmuka: Tetun (sesuai revisi dosen).
"""

import os
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="Stock Market Streaming Analytics",
    page_icon="",
    layout="wide",
)

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "stockdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "stockuser")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "stockpass")

ENGINE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"

COMPANY_COLORS = {
    "Bank Mandiri": "#003D79",
    "Telkomsel": "#E60012",
    "Bank ANZ": "#0072AA",
    "Bank BRI": "#00529C",
}

# =====================================================================
# KONVERSI HARGA KE USD
# Dataset BRI, Mandiri, dan Telkom adalah harga saham Bursa Efek
# Indonesia dalam Rupiah, jadi dikalikan faktor (1 / 15.500). Angka
# 15.500 hanya perkiraan, ubah di sini kalau perlu.
# ANZ skalanya beda (belasan, bukan ribuan) dan mata uang aslinya
# BELUM DIVERIFIKASI, sehingga faktornya 1.0 (dianggap sudah USD).
# Kalau ternyata AUD, ganti mis. 0.65.
# Kolom persen (pct_change, volatility_pct) dan volume tidak terpengaruh.
# =====================================================================
PRICE_TO_USD = {"BBRI": 1 / 15500, "MANDIRI": 1 / 15500, "TLKM": 1 / 15500, "ANZ": 1.0}
PRICE_COLUMNS = ["open", "high", "low", "close", "adjclose", "price_change"]


def convert_prices_to_usd(df):
    factor = df["ticker"].astype(str).str.upper().map(PRICE_TO_USD).fillna(1.0)
    for col in PRICE_COLUMNS:
        df[col] = df[col] * factor
    return df


# =====================================================================
# TEKS ANTARMUKA — Tetun saja (revisi dosen: hapus Bahasa Indonesia)
# Catatan: terjemahan Tetun dibuat dengan bantuan istilah umum Tetun
# Ofisiál. Untuk keperluan presentasi resmi/akademis, disarankan
# direview oleh penutur asli Tetun.
# =====================================================================
TEXT = {
    "tet": {
        "title": " Stock Market Streaming Analytics",
        "subtitle": "Analíze Folin Aksaun no Tendénsia Merkadu Real-Time — Bank Mandiri • Telkomsel • Bank ANZ • Bank BRI",
        "waiting": " Hein dadus tama husi pipeline Kafka → Spark → PostgreSQL. Dashboard ida ne'e sei atualiza automátikamente.",
        "filter_header": " Filtru",
        "choose_company": "Hili Kompañia",
        "date_range": "Intervalu Loron",
        "date_help": "Klik dala ida atu loke kalendáriu, depois hili loron hahú no loron remata.",
        "autorefresh_toggle": " Atualiza Automátiku ativu",
        "autorefresh_help": "Taka provizóriu se susar atu hili loron, atu pájina la atualiza bainhira Ita klik.",
        "autorefresh_interval_label": "Intervalu atualiza (segundu)",
        "autorefresh_interval_help": "Hili oinsá dashboard atualiza nia an.",
        "total_rows": "Total Linha Dadus Prosesadu",
        "last_update": "Atualizasaun ikus dadus",
        "choose_company_warning": "Favór hili pelumenus kompañia ida iha sidebar.",
        "latest_condition": " Kondisaun Ikus",
        "price_trend": " Tendénsia Folin Taka (Close Price)",
        "date_axis": "Loron",
        "close_axis": "Folin Taka (USD, eskala log)",
        "company_axis": "Kompañia",
        "avg_pct_change": " Média Pursentu Mudansa Folin",
        "avg_pct_change_axis": "Média % Mudansa",
        "volatility": " Volatilidade Folin (Establidade)",
        "volatility_axis": "Média Volatilidade (%)",
        "volume_title": " Volume Komérsiu",
        "volume_axis": "Volume",
        "latest_data": " Dadus Foun",
        "footer": "Dashboard atualiza automátiku kada {sec} segundu • Fonte: PostgreSQL (rezultadu Spark Structured Streaming)",
        "footer_paused": "Atualiza automátiku hein • Fonte: PostgreSQL (rezultadu Spark Structured Streaming)",
        "col_company": "Kompañia",
        "col_date": "Loron",
        "col_open": "Loke",
        "col_high": "Aas Liu",
        "col_low": "Ki'ik Liu",
        "col_close": "Taka",
        "col_volume": "Volume",
        "col_pct_change": "% Mudansa",
        "col_volatility": "Volatilidade (%)",
        "col_trend": "Tendénsia",
        "usd_note": "Folin hatudu iha USD (konverte husi osan orijinál, estimasaun).",
        "trend_map": {"NAIK": "SAE", "TURUN": "TUN", "STABIL": "ESTÁVEL"},
    },
}

# Revisi dosen: sistem ini HANYA berbahasa Tetun, tidak ada pilihan
# bahasa lain lagi. Dropdown pemilih bahasa sengaja dihapus.
T = TEXT["tet"]

# -------------------------------------------------------------------
# Auto-refresh — bisa dijeda supaya date picker & filter lain bisa
# diklik dengan tenang (sebelumnya refresh 5 detik menutup kalender
# tanggal sebelum sempat dipilih). Interval-nya sekarang bisa diatur
# sendiri lewat slider, tidak perlu edit kode lagi.
# -------------------------------------------------------------------
with st.sidebar:
    autorefresh_on = st.toggle(T["autorefresh_toggle"], value=True, help=T["autorefresh_help"])
    refresh_seconds = st.slider(
        T["autorefresh_interval_label"],
        min_value=1, max_value=60, value=5, step=1,
        help=T["autorefresh_interval_help"],
        disabled=not autorefresh_on,
    )

REFRESH_INTERVAL_MS = refresh_seconds * 1000

if autorefresh_on:
    st_autorefresh(interval=REFRESH_INTERVAL_MS, key="auto_refresh")


@st.cache_resource
def get_engine():
    return create_engine(ENGINE_URL, pool_pre_ping=True)


def load_data() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT ticker, company, trade_date, open, high, low, close, adjclose,
               volume, price_change, pct_change, volatility_pct, trend, processed_at
        FROM stock_prices
        ORDER BY trade_date ASC
    """
    try:
        df = pd.read_sql(query, engine)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        return convert_prices_to_usd(df)
    except Exception as e:
        st.warning(
            f"Belum bisa membaca data dari PostgreSQL ({e}). "
            "Pastikan producer & spark-app sudah berjalan dan mulai mengirim data."
        )
        return pd.DataFrame()


st.title(T["title"])
st.caption(T["subtitle"])

df = load_data()

if df.empty:
    st.info(T["waiting"])
    st.stop()

companies = sorted(df["company"].unique().tolist())

with st.sidebar:
    st.header(T["filter_header"])
    selected_companies = st.multiselect(
        T["choose_company"], options=companies, default=companies
    )
    min_date, max_date = df["trade_date"].min(), df["trade_date"].max()
    date_range = st.date_input(
        T["date_range"],
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        help=T["date_help"],
    )
    st.markdown("---")
    st.metric(T["total_rows"], f"{len(df):,}")
    st.caption(f"{T['last_update']}: {df['processed_at'].max()}")

if not selected_companies:
    st.warning(T["choose_company_warning"])
    st.stop()

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

mask = (
    df["company"].isin(selected_companies)
    & (df["trade_date"] >= pd.to_datetime(start_date))
    & (df["trade_date"] <= pd.to_datetime(end_date))
)
fdf = df.loc[mask].copy()
fdf["trend_display"] = fdf["trend"].map(T["trend_map"]).fillna(fdf["trend"])


st.subheader(T["latest_condition"])
latest = fdf.sort_values("trade_date").groupby("company").tail(1)

cols = st.columns(len(selected_companies)) if selected_companies else [st]
for c, company in zip(cols, selected_companies):
    row = latest[latest["company"] == company]
    if row.empty:
        continue
    row = row.iloc[0]
    price = row["close"]
    c.metric(
        label=f"{company} (USD)",
        value=f"$ {price:,.4f}" if price < 1 else f"$ {price:,.2f}",
        delta=f"{row['pct_change']:.2f}% ({row['trend_display']})",
    )

st.markdown("---")


st.subheader(T["price_trend"])
fig_line = px.line(
    fdf.sort_values("trade_date"),
    x="trade_date", y="close", color="company",
    color_discrete_map=COMPANY_COLORS,
    labels={"trade_date": T["date_axis"], "close": T["close_axis"], "company": T["company_axis"]},
)
fig_line.update_layout(height=450, legend_title_text=T["company_axis"])
fig_line.update_yaxes(type="log")
st.plotly_chart(fig_line, use_container_width=True)


col1, col2 = st.columns(2)

with col1:
    st.subheader(T["avg_pct_change"])
    perf = fdf.groupby("company", as_index=False)["pct_change"].mean().round(3)
    fig_bar = px.bar(
        perf, x="company", y="pct_change", color="company",
        color_discrete_map=COMPANY_COLORS,
        labels={"pct_change": T["avg_pct_change_axis"], "company": T["company_axis"]},
    )
    fig_bar.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig_bar, use_container_width=True)

with col2:
    st.subheader(T["volatility"])
    vol = fdf.groupby("company", as_index=False)["volatility_pct"].mean().round(3)
    fig_vol = px.bar(
        vol, x="company", y="volatility_pct", color="company",
        color_discrete_map=COMPANY_COLORS,
        labels={"volatility_pct": T["volatility_axis"], "company": T["company_axis"]},
    )
    fig_vol.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig_vol, use_container_width=True)

# -------------------------------------------------------------------
# VOLUME PERDAGANGAN
# -------------------------------------------------------------------
st.subheader(T["volume_title"])
fig_vol_line = px.area(
    fdf.sort_values("trade_date"),
    x="trade_date", y="volume", color="company",
    color_discrete_map=COMPANY_COLORS,
    labels={"trade_date": T["date_axis"], "volume": T["volume_axis"], "company": T["company_axis"]},
)
fig_vol_line.update_layout(height=350)
st.plotly_chart(fig_vol_line, use_container_width=True)

# -------------------------------------------------------------------
# TABEL DATA TERBARU
# -------------------------------------------------------------------
st.subheader(T["latest_data"])
table_df = fdf.sort_values("trade_date", ascending=False)[
    ["company", "trade_date", "open", "high", "low", "close", "volume", "pct_change", "volatility_pct", "trend_display"]
].head(50)
table_df = table_df.rename(columns={
    "company": T["col_company"],
    "trade_date": T["col_date"],
    "open": T["col_open"],
    "high": T["col_high"],
    "low": T["col_low"],
    "close": T["col_close"],
    "volume": T["col_volume"],
    "pct_change": T["col_pct_change"],
    "volatility_pct": T["col_volatility"],
    "trend_display": T["col_trend"],
})
st.dataframe(table_df, use_container_width=True, hide_index=True)

st.caption(T["usd_note"])
if autorefresh_on:
    st.caption(T["footer"].format(sec=REFRESH_INTERVAL_MS // 1000))
else:
    st.caption(T["footer_paused"])
