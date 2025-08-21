# app_control_ad.py
# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date, datetime, timedelta
import io, re
from math import floor

st.set_page_config(page_title="Control Alcohol y Drogas", layout="wide")

# ========================= Parámetros de negocio (ajustables) =========================
JORNADAS_DIA_PRIOR = {"4X3DIP01", "4X3DIE01"}  # cupo especial Lun–Jue (Día)
FUNCION_OPERADOR = "OPERADOR MINA"             # coincidencia contiene (case-insensitive)
COL_GERENCIA_ORIG = "Texto Gerencia"
COL_FUNCION_ORIG  = "Denominación Función"
COL_TURNO_ORIG    = "Regla p.plan h.tbjo."
COL_JORNADA_ORIG  = "Jornada"  # si no existe, el código intenta detectar heurísticamente

# Límites / espaciado
MAX_VECES_ANO = 4
PRIORIDAD_ESPACIADO_DIAS = [90, 60, 30, 0]  # se relaja si no alcanza cupo
REINGRESO_NO_SHOW_DIAS = 60  # si no asistió en últimos N días, reintegrar a la tómbola

# =====================================================================================

REQ_COLS_ORIG = [
    "SAP","Nombre","Área pers.","Posición","Denominación Posición","Función","Denominación Función",
    "Unidad org.","Denominación Organización","Texto Gerencia","Texto Subgerencia",
    "Texto Superintendencia","Texto Departamento","Rol Unico Tributario","Regla p.plan h.tbjo."
]

def _normalize_cols(cols):
    return (pd.Series(cols, dtype=str)
            .str.strip().str.replace(r"\s+", "_", regex=True)
            .str.replace(r"[^\w]", "", regex=True).str.upper().tolist())

def detect_col(df_norm, map_norm_to_orig, map_orig_to_norm, orig_name, heuristics=None, required=True):
    norm = map_orig_to_norm.get(orig_name)
    if norm and norm in df_norm.columns:
        return norm, orig_name
    if heuristics:
        for h in heuristics:
            cands = [c for c in df_norm.columns if h(c)]
            if cands:
                return cands[0], map_norm_to_orig[cands[0]]
    if required:
        raise RuntimeError(f"No se encontró la columna requerida: {orig_name}")
    return None, None

def read_sheet(uploaded_file, sheet_like):
    xls = pd.ExcelFile(uploaded_file)
    names = {s.lower(): s for s in xls.sheet_names}
    key = sheet_like.lower()
    use = names.get(key)
    if use is None:
        for s in xls.sheet_names:
            if s.lower().startswith(key[:5]):
                use = s
                break
        if use is None:
            use = xls.sheet_names[0]
    df_raw = pd.read_excel(uploaded_file, sheet_name=use)
    return df_raw, use

def map_columns(df_raw):
    df = df_raw.copy()
    norm = _normalize_cols(df.columns)
    map_norm_to_orig = dict(zip(norm, df.columns))
    map_orig_to_norm = dict(zip(df.columns, norm))
    df.columns = norm
    return df, map_norm_to_orig, map_orig_to_norm

def construir_salida(sample, map_norm_to_orig, map_orig_to_norm, fecha_txt):
    req_norm = [map_orig_to_norm.get(c) for c in REQ_COLS_ORIG]
    req_norm = [c for c in req_norm if c in sample.columns and c is not None]
    salida = sample[req_norm].copy()
    salida.columns = [map_norm_to_orig[c] for c in salida.columns]
    salida.insert(0, "Fecha aleatorio", fecha_txt)
    return salida

def df_to_excel_download(df, sheet="Control"):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        df.to_excel(w, sheet_name=sheet, index=False)
    return buf.getvalue()

def distribuir_proporcional(series_counts: pd.Series, total: int) -> dict:
    d = series_counts.dropna().astype(int).to_dict()
    if total <= 0 or sum(d.values()) == 0:
        return {k: 0 for k in d}
    total_pool = sum(d.values())
    cuotas = {k: (d[k] / total_pool) * total for k in d}
    asigna = {k: int(floor(c)) for k, c in cuotas.items()}
    residuo = total - sum(asigna.values())
    if residuo > 0:
        decs = sorted(((k, cuotas[k]-floor(cuotas[k])) for k in cuotas), key=lambda x:x[1], reverse=True)
        for i in range(residuo):
            asigna[decs[i % len(decs)][0]] += 1
    for k in list(asigna.keys()):
        asigna[k] = min(asigna[k], d[k])
    faltan = total - sum(asigna.values())
    if faltan > 0:
        for k in sorted(d, key=lambda x: d[x]-asigna[x], reverse=True):
            rem = d[k] - asigna[k]
            if rem <= 0: continue
            toma = min(rem, faltan)
            asigna[k] += toma
            faltan -= toma
            if faltan == 0: break
    return asigna

def safe_contains(val, needle):
    return needle.lower() in str(val).lower()

def filtrar_no_show_relaja(hist_df, asis_df):
    """
    Si alguien tiene registro 'NO' de asistencia reciente, ignoramos su última citación
    al evaluar tope/espaciado (reingresa a la tómbola).
    """
    if asis_df is None or asis_df.empty or hist_df is None or hist_df.empty:
        return hist_df if hist_df is not None else pd.DataFrame()
    dfh = hist_df.copy()
    # Inferencia simple de claves en asistencia
    cols = {c.upper(): c for c in asis_df.columns}
    cand_id = next((cols[c] for c in cols if c in {"SAP","RUT","RUN","ID"}), None)
    cand_as = next((cols[c] for c in cols if "ASIST" in c or "PRESENT" in c), None)
    cand_fe = next((cols[c] for c in cols if "FECHA" in c), None)
    if not all([cand_id, cand_as, cand_fe]):
        return dfh
    # Filtrar últimos N días
    recent = asis_df.copy()
    recent[cand_fe] = pd.to_datetime(recent[cand_fe], errors="coerce")
    cutoff = pd.Timestamp.today() - pd.Timedelta(days=REINGRESO_NO_SHOW_DIAS)
    recent = recent[recent[cand_fe] >= cutoff]
    mask_no = recent[cand_as].astype(str).str.upper().isin(["NO","N","0","FALTA","ABSENT"])
    reingreso_ids = set(recent.loc[mask_no, cand_id].astype(str))
    if not reingreso_ids:
        return dfh
    # Si en historial hay esos IDs, les anulamos la fecha (para forzar reingreso)
    if "SURROGATE_ID" in dfh.columns:
        mask_hit = dfh["SURROGATE_ID"].astype(str).isin(reingreso_ids)
        dfh.loc[mask_hit, "FECHA"] = pd.NaT
    return dfh

# ========================= UI =========================
st.title("🧪 Control de Alcohol y Drogas")

up = st.file_uploader("Sube el Excel 'Control Alcohol y Drogas.xlsx' (hojas: dotacion, asistencia)", type=["xlsx"])

colX, colY = st.columns([1,1])
with colX:
    fecha_ctrl = st.date_input("Fecha del control", value=date.today())
with colY:
    jornada = st.selectbox("Jornada", options=["Día","Noche"], index=0)

if not up:
    st.info("Carga el Excel para continuar.")
    st.stop()

# -------- leer DOTACIÓN --------
try:
    df_dot_raw, hoja_dot = read_sheet(up, "dotacion")
    df_dot, map_n2o, map_o2n = map_columns(df_dot_raw)
except Exception as e:
    st.error(f"Error leyendo hoja DOTACION: {e}")
    st.stop()

# columnas clave
try:
    col_turno, col_turno_orig = detect_col(
        df_dot, map_n2o, map_o2n, COL_TURNO_ORIG,
        heuristics=[lambda c: ("REGLA" in c and "PLAN" in c and "TBJO" in c)]
    )
    col_func, col_func_orig = detect_col(
        df_dot, map_n2o, map_o2n, COL_FUNCION_ORIG,
        heuristics=[lambda c: ("DENOMINACION" in c or "DENOMINACIÓN" in c) and "FUNC" in c]
    )
    col_ger, col_ger_orig = detect_col(
        df_dot, map_n2o, map_o2n, COL_GERENCIA_ORIG,
        heuristics=[lambda c: "GEREN" in c]
    )
    col_jorn, col_jorn_orig = detect_col(
        df_dot, map_n2o, map_o2n, COL_JORNADA_ORIG,
        heuristics=[lambda c: "JORN" in c or "CALENDAR" in c], required=False
    )
except Exception as e:
    st.error(str(e)); st.stop()

# ID
sap_norm = map_o2n.get("SAP")
if sap_norm and sap_norm in df_dot.columns:
    df_dot["SURROGATE_ID"] = df_dot[sap_norm].astype(str)
else:
    df_dot["SURROGATE_ID"] = df_dot.index.astype(str)

# limpiar nulos básicos
df_dot = df_dot[~df_dot[col_turno].isna()].copy()
df_dot = df_dot[~df_dot[col_ger].isna()].copy()

# turnos disponibles
turnos = (df_dot[col_turno].astype(str).str.strip().replace("", np.nan).dropna().drop_duplicates().sort_values().tolist())
turno_obj = st.selectbox("Turno objetivo (para Operador Mina)", options=turnos)

# -------- leer ASISTENCIA (opcional) --------
try:
    df_asis_raw, hoja_asis = read_sheet(up, "asistencia")
    df_asis = df_asis_raw.copy()
except Exception:
    df_asis = None
    hoja_asis = "(sin hoja asistencia)"

st.caption(f"Hojas usadas: dotación = **{hoja_dot}**, asistencia = **{hoja_asis}**")
st.divider()

# ========================= HISTORIAL (en memoria por archivo) =========================
# En cloud no escribimos a disco; permitimos subir un historial CSV previo y descargar el actualizado
hist_up = st.file_uploader("Sube (opcional) historial CSV previo", type=["csv"], help="Si no subes, se considera que no hay historial.")
if hist_up:
    hist = pd.read_csv(hist_up, dtype={"SURROGATE_ID": str})
else:
    hist = pd.DataFrame(columns=["SURROGATE_ID","FECHA","TURNO","TIMESTAMP","NOMBRE"])

# Normalizar historial
if not hist.empty:
    if "FECHA" in hist.columns:
        hist["FECHA"] = pd.to_datetime(hist["FECHA"], errors="coerce")
    if "SURROGATE_ID" in hist.columns:
        hist["SURROGATE_ID"] = hist["SURROGATE_ID"].astype(str)

# Si hay no-shows recientes, los reintegramos (ignorar su última FECHA/conteo)
hist = filtrar_no_show_relaja(hist, df_asis)

# ========================= Construcción de pools =========================
today = pd.Timestamp(fecha_ctrl)
year_ago = today - pd.Timedelta(days=365)

# límites por año / espaciado — SIEMPRE devolver (eligibles_flag, days_since_last)
def elegible_por_hist(id_series: pd.Series):
    """
    Devuelve SIEMPRE (eligibles_flag, days_since_last).
    - eligibles_flag: Series booleana por ID (tope 4 en 365 días).
    - days_since_last: días desde la última citación (99999 si nunca).
    """
    ids = id_series.astype(str)

    # Sin historial o sin FECHA: todos elegibles, sin última citación
    if hist is None or hist.empty or "FECHA" not in hist.columns:
        eligibles = pd.Series(True, index=ids.index)
        days_since = pd.Series(99999, index=ids.index)
        return eligibles, days_since

    # Asegurar FECHA como datetime
    h = hist.copy()
    h["FECHA"] = pd.to_datetime(h["FECHA"], errors="coerce")

    # Ventana 365 días
    h_recent = h[h["FECHA"] >= year_ago]

    # Conteo por ID en 365 días
    counts = h_recent.groupby(h_recent["SURROGATE_ID"].astype(str)).size() if not h_recent.empty else pd.Series(dtype=int)

    # Última fecha por ID
    last_date = h.groupby(h["SURROGATE_ID"].astype(str))["FECHA"].max() if not h.empty else pd.Series(dtype="datetime64[ns]")

    # Elegibles por tope
    eligibles = pd.Series(True, index=ids.index)
    if not counts.empty:
        ids_tope = counts[counts >= MAX_VECES_ANO].index
        eligibles[ids.isin(ids_tope)] = False

    # Días desde última citación
    def _days_since(x):
        ld = last_date.get(x, pd.NaT) if isinstance(last_date, pd.Series) else pd.NaT
        if pd.isna(ld):
            return 99999
        return (today - ld).days

    days_since = ids.map(_days_since)
    return eligibles, days_since

eligibles_flag, days_since_last = elegible_por_hist(df_dot["SURROGATE_ID"])
df_dot["ELIGIBLE_BASE"] = eligibles_flag.fillna(True)
df_dot["DIAS_DESDE_ULT"] = days_since_last.fillna(99999)

# Muestreo con prioridad de espaciado
def sample_with_spacing(pool: pd.DataFrame, n: int, prefer_days=PRIORIDAD_ESPACIADO_DIAS, seed=0) -> pd.DataFrame:
    if n <= 0 or pool.empty:
        return pool.head(0).copy()
    rng = np.random.default_rng(seed)
    remaining = n
    parts = []
    used_idx = set()
    for thr in prefer_days:
        cand = pool[(pool["ELIGIBLE_BASE"]) & (pool["DIAS_DESDE_ULT"] >= thr) & (~pool.index.isin(used_idx))]
        if cand.empty:
            continue
        take_n = min(remaining, len(cand))
        take = cand.iloc[rng.permutation(len(cand))[:take_n]]
        parts.append(take)
        used_idx.update(take.index)
        remaining -= take_n
        if remaining == 0:
            break
    if remaining > 0:
        cand = pool[(pool["ELIGIBLE_BASE"]) & (~pool.index.isin(used_idx))]
        take_n = min(remaining, len(cand))
        if take_n > 0:
            take = cand.iloc[rng.permutation(len(cand))[:take_n]]
            parts.append(take)
    return pd.concat(parts) if parts else pool.head(0).copy()

# ========================= Reglas de cupos =========================
dow = today.weekday()  # 0=lun ... 6=dom

if jornada == "Día":
    if dow <= 3:  # Lun–Jue
        cupos = {"JORNADAS_DIA_PRIOR": 4, "OPERADORES_MINA": 4, "EXTRAS": 6}
    else:        # Vie–Dom
        cupos = {"JORNADAS_DIA_PRIOR": 0, "OPERADORES_MINA": 4, "EXTRAS": 6}
else:  # Noche
    cupos = {"JORNADAS_DIA_PRIOR": 0, "OPERADORES_MINA": 4, "EXTRAS": 2}

st.subheader("Cupos del día")
st.write(cupos)

# Pools:
pool_base = df_dot.copy()

# 1) Pool jornadas prioritarias (Lun–Jue Día)
if col_jorn is not None and cupos["JORNADAS_DIA_PRIOR"] > 0:
    pool_jorn = pool_base[pool_base[col_jorn].astype(str).str.upper().isin(JORNADAS_DIA_PRIOR)].copy()
else:
    pool_jorn = pool_base.head(0).copy()

# 2) Pool operadores mina del turno objetivo
pool_oper = pool_base[
    (pool_base[col_turno].astype(str) == str(turno_obj)) &
    (pool_base[col_func].apply(lambda x: safe_contains(x, FUNCION_OPERADOR)))
].copy()

# 3) Pool extras: todo elegible menos lo ya usado en 1) y 2)
def exclude_taken(df_all, taken_idx):
    return df_all[~df_all.index.isin(taken_idx)].copy()

seed = int(today.strftime("%Y%m%d"))

sel_parts = []

# Selección 1: Jornadas prioritarias
take1 = sample_with_spacing(pool_jorn, cupos["JORNADAS_DIA_PRIOR"], seed=seed+1)
sel_parts.append(take1)
used = set(take1.index)

# Selección 2: Operadores Mina del turno objetivo (excluyendo los ya tomados)
pool_oper2 = exclude_taken(pool_oper, used)
take2 = sample_with_spacing(pool_oper2, cupos["OPERADORES_MINA"], seed=seed+2)
sel_parts.append(take2)
used.update(take2.index)

# Selección 3: Extras proporcionales por Gerencia sobre el pool restante elegible
pool_rest = exclude_taken(pool_base, used)
pool_rest = pool_rest[pool_rest["ELIGIBLE_BASE"]].copy()
if cupos["EXTRAS"] > 0 and not pool_rest.empty:
    by_ger = pool_rest.groupby(col_ger).size()
    asignas = distribuir_proporcional(by_ger, cupos["EXTRAS"])
    parts_extra = []
    for ger, n_g in asignas.items():
        if n_g <= 0: continue
        sub = pool_rest[pool_rest[col_ger] == ger]
        take_g = sample_with_spacing(sub, n_g, seed=seed+3)
        parts_extra.append(take_g)
    take3 = pd.concat(parts_extra) if parts_extra else pool_rest.head(0).copy()
    sel_parts.append(take3)
else:
    take3 = pool_rest.head(0).copy()

# Resultado final
resultado = pd.concat(sel_parts, ignore_index=False) if sel_parts else pool_base.head(0).copy()
resultado = resultado.drop_duplicates(subset=["SURROGATE_ID"])

# Marcas de origen
resultado["SELECCION"] = ""
resultado.loc[resultado.index.isin(take1.index), "SELECCION"] += "JORNADA;"
resultado.loc[resultado.index.isin(take2.index), "SELECCION"] += "OPERADOR;"
resultado.loc[resultado.index.isin(take3.index), "SELECCION"] += "EXTRA;"

# ========================= Presentación y descargas =========================
fecha_txt = today.strftime("%Y-%m-%d")
# salida con nombres originales y fecha al inicio
salida = construir_salida(resultado, map_n2o, map_o2n, fecha_txt)
st.success(f"Seleccionados: {len(salida)}")
st.dataframe(salida, use_container_width=True)

# Excel de muestra
excel_bytes = df_to_excel_download(salida, sheet="Control")
st.download_button("⬇️ Descargar Excel de Control", excel_bytes,
                   file_name=f"control_AD_{fecha_txt}.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Historial actualizado (en memoria para descarga)
hist_update = pd.DataFrame({
    "SURROGATE_ID": resultado["SURROGATE_ID"].astype(str).tolist(),
    "FECHA": [fecha_txt] * len(resultado),
    "TURNO": resultado[col_turno].astype(str).tolist(),
    "TIMESTAMP": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * len(resultado),
    "NOMBRE": (df_dot[map_o2n.get("Nombre")]
               if map_o2n.get("Nombre") in df_dot.columns else pd.Series([""]*len(resultado))).reindex(resultado.index).tolist()
})
hist2 = pd.concat([hist, hist_update], ignore_index=True)
csv_hist = hist2.copy()
csv_hist["FECHA"] = csv_hist["FECHA"].astype(str)

csv_bytes = csv_hist.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
st.download_button("⬇️ Descargar Historial Actualizado (CSV)", csv_bytes,
                   file_name="historial_actualizado.csv", mime="text/csv")

# Resumen por origen y por gerencia en extras
with st.expander("📊 Resumen"):
    res_origen = resultado["SELECCION"].str.split(";", expand=True).stack().str.upper().str.strip()
    res_origen = res_origen[res_origen != ""].value_counts().rename_axis("Tipo").reset_index(name="N")
    st.subheader("Por tipo de selección")
    st.dataframe(res_origen, use_container_width=True)

    if 'take3' in locals() and not take3.empty:
        st.subheader("Extras por gerencia (proporcionalidad)")
        df_g = take3.groupby(col_ger).size().rename("N").reset_index()
        st.dataframe(df_g, use_container_width=True)

st.caption("Reglas: cupos por día/jornada, tope 4 por año, preferencia ≥90 días (relaja 60/30/0), reingreso por no-show reciente, extras proporcionales por Gerencia.")
