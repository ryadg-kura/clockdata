import os
import streamlit as st
from data_loader import (
    compute_kpis,
    filter_device,
    get_bpm_history,
    get_devices,
    get_steps_per_day,
    load_analysis,
    load_gold,
)

GOLD_PATH = os.getenv("GOLD_PATH", "../data/gold")
ANALYSIS_PATH = os.getenv("ANALYSIS_PATH", "../data/gold/analysis")

st.set_page_config(page_title="ClockData — Santé", layout="wide")
st.title("ClockData — Statistiques de santé")

df = load_gold(GOLD_PATH)

if df is None or df.empty:
    st.warning(
        f"Aucune donnée disponible dans `{GOLD_PATH}`.\n\n"
        "Lancez d'abord le pipeline ou générez des données d'exemple :\n"
        "```\npython generate_sample_data.py\n```"
    )
    st.stop()

st.sidebar.header("Filtres")
devices = get_devices(df)
selected = st.sidebar.selectbox("Appareil", ["Tous les appareils"] + devices)
device_id = None if selected == "Tous les appareils" else selected
filtered = filter_device(df, device_id)

kpis = compute_kpis(filtered)
col1, col2, col3, col4 = st.columns(4)
col1.metric("BPM moyen", f"{kpis['avg_bpm']:.1f}")
col2.metric("BPM max", str(kpis["max_bpm"]))
col3.metric("BPM min", str(kpis["min_bpm"]))
col4.metric("Total de pas", f"{kpis['total_steps']:,}".replace(",", " "))

st.divider()

st.subheader("Historique BPM par heure")
bpm_hist = get_bpm_history(filtered).set_index("datetime")
st.line_chart(bpm_hist[["avg_bpm", "max_bpm", "min_bpm"]])

st.subheader("Pas par jour")
steps_day = get_steps_per_day(filtered).set_index("day")
st.bar_chart(steps_day["total_steps"])

st.caption(f"Source Gold : `{os.path.abspath(GOLD_PATH)}`")

st.divider()
st.subheader("Analyse — 4 questions")

analysis_df = load_analysis(ANALYSIS_PATH)

if analysis_df is None or analysis_df.empty:
    st.info(
        f"Aucune analyse disponible dans `{ANALYSIS_PATH}`. "
        "Lancez analysis-service pour générer les résultats."
    )
else:
    for i, row in enumerate(analysis_df.itertuples(), start=1):
        st.markdown(f"**{i}. {row.question}**")
        st.write(row.answer)
