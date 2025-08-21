# app_control_ad.py
# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, datetime
import io, re
from math import floor

st.set_page_config(page_title="Control Alcohol y Drogas", layout="wide")

# ========================= Parámetros de negocio =========================
JORNADAS_4X3_COD = {"4X3DIP01", "4X3DIE01"}     # vienen de la MISMA columna "Regla p.plan h.tbjo."
FUNCION_OPERADOR = "OPERADOR MINA"              # match contains (case-insensitive)

COL_GERENCIA_ORIG = "Texto Gerencia"
COL_FUNCION_ORIG  = "Denominación Función"
COL_REGLA_ORIG    = "Regla p.plan h.tbjo."      # ÚNICA columna para turno/jornada

MAX_VECES_ANO = 4
PRIORIDAD_ESPACIADO_DIAS = [90, 60, 30, 0]
REINGRESO_NO_SHOW_DIAS = 60
# =======================================================================

# Columnas que mostraremos en el Excel final (quitamos Subgerencia y Departamento)
REQ_COLS_ORIG = [
    "SAP","Nombre","Área pers.","Posición","Denominación Posición","Función","Denominación Función",
    "Unidad org.","Denominación Organización","Texto Gerencia",
    "Texto Superintendencia",
    "Rol Unico Tributario","Regla p.plan h.tbjo."
]

# -------------------- utilidades --------------------
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
            if s.lower().startswith(key[:5]): use = s; break
        if use is None: use = xls.sheet_names[0]
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
    # columnas nuevas al final: métricas
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

def parse_bool_asistencia(x: str) -> bool:
    s = str(x).strip().upper()
    return s in {"SI","S","1","ASISTE","PRESENTE","TRUE","T"}  # positivos comunes

def filtrar_no_show_relaja(hist_df, asis_df):
    """
    Reintegra a la tómbola si hay NO-SHOW reciente en asistencia (últimos REINGRESO_NO_SHOW_DIAS):
    en la práctica, anulamos FECHA en historial para ese ID (no cuenta para tope/espaciado).
    """
    if asis_df is None or asis_df.empty or hist_df is None or hist_df.empty:
        return hist_df if hist_df is not None else pd.DataFrame()
    dfh = hist_df.copy()
    cols = {c.upper(): c for c in asis_df.columns}
    cand_id = next((cols[c] for c in cols if c in {"SAP","RUT","RUN","ID"}), None)
    cand_as = next((cols[c] for c in cols if "ASIST" in c or "PRESENT" in c), None)
    cand_fe = next((cols[c] for c in cols if "FECHA" in c), None)
    if not all([cand_id, cand_as, cand_fe]):
        return dfh
    recent = asis_df.copy()
    recent[cand_fe] = pd.to_datetime(recent[cand_fe], errors="coerce")
    cutoff = pd.Timestamp.today() - pd.Timedelta(days=REINGRESO_NO_SHOW_DIAS)
    recent = recent[recent[cand_fe] >= cutoff]
    mask_no = ~recent[cand_as].apply(parse_bool_asistencia)
    reingreso_ids = set(recent.loc[mask_no, cand_id].astype(str))
    if not reingreso_ids:
        return dfh
    if "SURROGATE_ID" in dfh.columns:
        dfh.loc[dfh["SURROGATE_ID"].astype(str).isin(reingreso_ids), "FECHA"] = pd.NaT
    return dfh

# -------------------- UI --------------------
st.title("🧪 Control de Alcohol y Drogas")

up = st.file_uploader("Sube el Excel 'Control Alcohol y Drogas.xlsx' (hojas: dotacion, asistencia)", type=["xlsx"])

c1, c2 = st.columns([1,1])
with c1:
    fecha_ctrl = st.date_input("Fecha del control", value=date.today())
with c2:
    jornada = st.selectbox("Jornada", options=["Día","Noche"], index=0)

if not up:
    st.info("Carga el Excel para continuar.")
    st.stop()

# leer dotación
try:
    df_dot_raw, hoja_dot = read_sheet(up, "dotacion")
    df_dot, map_n2o, map_o2n = map_columns(df_dot_raw)
except Exception as e:
    st.error(f"Error leyendo hoja DOTACION: {e}")
    st.stop()

# columnas clave (todas desde dotación)
try:
    col_regla, col_regla_orig = detect_col(
        df_dot, map_n2o, map_o2n, COL_REGLA_ORIG,
        heuristics=[lambda c: ("REGLA" in c and "PLAN" in c and "TBJO" in c)]
    )
    col_func,  col_func_orig  = detect_col(
        df_dot, map_n2o, map_o2n, COL_FUNCION_ORIG,
        heuristics=[lambda c: ("DENOMINACION" in c or "DENOMINACIÓN" in c) and "FUNC" in c]
    )
    col_ger,   col_ger_orig   = detect_col(
        df_dot, map_n2o, map_o2n, COL_GERENCIA_ORIG,
        heuristics=[lambda c: "GEREN" in c]
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
df_dot = df_dot[~df_dot[col_regla].isna()].copy()
df_dot = df_dot[~df_dot[col_ger].isna()].copy()

# turnos disponibles (multi-selección desde la MISMA columna)
turnos_all = (df_dot[col_regla].astype(str).str.strip()
              .replace("", np.nan).dropna().drop_duplicates().sort_values().tolist())
turns_sel = st.multiselect("Selecciona uno o más turnos (Regla p.plan h.tbjo.)", options=turnos_all, default=turnos_all[:1])
if not turns_sel:
    st.warning("Selecciona al menos un turno.")
    st.stop()

# asistencia (opcional)
try:
    df_asis_raw, hoja_asis = read_sheet(up, "asistencia")
    df_asis = df_asis_raw.copy()
except Exception:
    df_asis = None
    hoja_asis = "(sin hoja asistencia)"

st.caption(f"Hojas usadas: dotación = **{hoja_dot}**, asistencia = **{hoja_asis}**")
st.divider()

# historial (subida opcional + descarga)
hist_up = st.file_uploader("Sube (opcional) historial CSV previo", type=["csv"])
if hist_up:
    hist = pd.read_csv(hist_up, dtype={"SURROGATE_ID": str})
else:
    hist = pd.DataFrame(columns=["SURROGATE_ID","FECHA","TURNO","TIMESTAMP","NOMBRE"])

if not hist.empty:
    if "FECHA" in hist.columns:
        hist["FECHA"] = pd.to_datetime(hist["FECHA"], errors="coerce")
    if "SURROGATE_ID" in hist.columns:
        hist["SURROGATE_ID"] = hist["SURROGATE_ID"].astype(str)

# Reingreso por no-show reciente
hist = filtrar_no_show_relaja(hist, df_asis)

# -------- elegibilidad por historial --------
today = pd.Timestamp(fecha_ctrl)
year_ago = today - pd.Timedelta(days=365)

def elegible_por_hist(id_series: pd.Series):
    ids = id_series.astype(str)
    if hist is None or hist.empty or "FECHA" not in hist.columns:
        eligibles = pd.Series(True, index=ids.index)
        days_since = pd.Series(99999, index=ids.index)
        return eligibles, days_since
    h = hist.copy()
    h["FECHA"] = pd.to_datetime(h["FECHA"], errors="coerce")
    h_recent = h[h["FECHA"] >= year_ago]
    counts = h_recent.groupby(h_recent["SURROGATE_ID"].astype(str)).size() if not h_recent.empty else pd.Series(dtype=int)
    last_date = h.groupby(h["SURROGATE_ID"].astype(str))["FECHA"].max() if not h.empty else pd.Series(dtype="datetime64[ns]")
    eligibles = pd.Series(True, index=ids.index)
    if not counts.empty:
        ids_tope = counts[counts >= MAX_VECES_ANO].index
        eligibles[ids.isin(ids_tope)] = False
    def _days_since(x):
        ld = last_date.get(x, pd.NaT) if isinstance(last_date, pd.Series) else pd.NaT
        if pd.isna(ld): return 99999
        return (today - ld).days
    days_since = ids.map(_days_since)
    return eligibles, days_since

eligibles_flag, days_since_last = elegible_por_hist(df_dot["SURROGATE_ID"])
df_dot["ELIGIBLE_BASE"]   = eligibles_flag.fillna(True)
df_dot["DIAS_DESDE_ULT"]  = days_since_last.fillna(99999)

def sample_with_spacing(pool: pd.DataFrame, n: int, prefer_days=PRIORIDAD_ESPACIADO_DIAS, seed=0) -> pd.DataFrame:
    if n <= 0 or pool.empty: return pool.head(0).copy()
    rng = np.random.default_rng(seed)
    remaining, parts, used = n, [], set()
    for thr in prefer_days:
        cand = pool[(pool["ELIGIBLE_BASE"]) & (pool["DIAS_DESDE_ULT"] >= thr) & (~pool.index.isin(used))]
        if cand.empty: continue
        take_n = min(remaining, len(cand))
        take = cand.iloc[rng.permutation(len(cand))[:take_n]]
        parts.append(take); used.update(take.index); remaining -= take_n
        if remaining == 0: break
    if remaining > 0:
        cand = pool[(pool["ELIGIBLE_BASE"]) & (~pool.index.isin(used))]
        take_n = min(remaining, len(cand))
        if take_n > 0:
            take = cand.iloc[rng.permutation(len(cand))[:take_n]]
            parts.append(take)
    return pd.concat(parts) if parts else pool.head(0).copy()

# -------- cupos por día/jornada --------
dow = today.weekday()  # 0=lun ... 6=dom
if jornada == "Día":
    cupos = {"JORNADAS_4X3": 4 if dow <= 3 else 0, "OPERADORES_MINA": 4, "EXTRAS": 6}
else:
    cupos = {"JORNADAS_4X3": 0, "OPERADORES_MINA": 4, "EXTRAS": 2}

st.subheader("Cupos del día")
st.write(cupos)

# -------- pools --------
pool_base = df_dot.copy()

# 1) 4x3 (Lun–Jue Día), desde la MISMA columna "Regla p.plan h.tbjo.", sin depender de turnos elegidos
reglas_norm = df_dot[col_regla].astype(str).str.upper().str.replace(r"\s+", "", regex=True)
pool_4x3 = pool_base[reglas_norm.isin({j.replace(" ", "").upper() for j in JORNADAS_4X3_COD})].copy() \
           if cupos["JORNADAS_4X3"] > 0 else pool_base.head(0).copy()

# 2) Operadores Mina de los turnos seleccionados
pool_oper = pool_base[
    (pool_base[col_regla].astype(str).isin([str(t) for t in turns_sel])) &
    (pool_base[col_func].apply(lambda x: safe_contains(x, FUNCION_OPERADOR)))
].copy()

# 3) Extras: MISMO(os) turno(s) seleccionados, excluyendo ya tomados y solo elegibles
def exclude_taken(df_all, taken_idx):
    return df_all[~df_all.index.isin(taken_idx)].copy()

seed = int(today.strftime("%Y%m%d"))
selected_parts = []
warnings = []

# Selección 1: 4x3
take1 = sample_with_spacing(pool_4x3, cupos["JORNADAS_4X3"], seed=seed+1)
selected_parts.append(take1)
used = set(take1.index)
if cupos["JORNADAS_4X3"] > 0 and len(take1) < cupos["JORNADAS_4X3"]:
    warnings.append(f"4x3: requeridos {cupos['JORNADAS_4X3']}, disponibles {len(take1)} "
                    f"(valores {', '.join(sorted(JORNADAS_4X3_COD))} en '{COL_REGLA_ORIG}').")

# Selección 2: Operadores Mina (mismo turno)
pool_oper2 = exclude_taken(pool_oper, used)
take2 = sample_with_spacing(pool_oper2, cupos["OPERADORES_MINA"], seed=seed+2)
selected_parts.append(take2)
used.update(take2.index)
if len(take2) < cupos["OPERADORES_MINA"]:
    warnings.append(f"Operadores Mina: requeridos {cupos['OPERADORES_MINA']}, disponibles {len(take2)} "
                    f"(en turnos {', '.join(map(str, turns_sel))}).")

# Selección 3: Extras (mismo turno) con proporcionalidad por Gerencia
pool_rest = exclude_taken(pool_base, used)
pool_rest = pool_rest[
    (pool_rest["ELIGIBLE_BASE"]) &
    (pool_rest[col_regla].astype(str).isin([str(t) for t in turns_sel]))
].copy()

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
    selected_parts.append(take3)
    if len(take3) < cupos["EXTRAS"]:
        warnings.append(f"Extras (mismo turno): requeridos {cupos['EXTRAS']}, disponibles {len(take3)}.")
else:
    take3 = pool_rest.head(0).copy()

# -------- resultado --------
resultado = pd.concat(selected_parts, ignore_index=False) if selected_parts else pool_base.head(0).copy()
resultado = resultado.drop_duplicates(subset=["SURROGATE_ID"])

resultado["SELECCION"] = ""
resultado.loc[resultado.index.isin(take1.index), "SELECCION"] += "4X3;"
resultado.loc[resultado.index.isin(take2.index), "SELECCION"] += "OPERADOR;"
resultado.loc[resultado.index.isin(take3.index), "SELECCION"] += "EXTRA;"

# ---------- métricas (12m): citaciones, asistencias, última citación ----------
def build_metrics(df_ids: pd.Series):
    ids = df_ids.astype(str)
    # citaciones 12m y última citación desde historial
    if hist is None or hist.empty or "FECHA" not in hist.columns:
        cit_12m = pd.Series(0, index=ids.index)
        last_cit = pd.Series(pd.NaT, index=ids.index, dtype="datetime64[ns]")
    else:
        h = hist.copy()
        h["FECHA"] = pd.to_datetime(h["FECHA"], errors="coerce")
        h12 = h[h["FECHA"] >= year_ago]
        cnt = h12.groupby(h12["SURROGATE_ID"].astype(str)).size()
        last = h.groupby(h["SURROGATE_ID"].astype(str))["FECHA"].max()
        cit_12m = ids.map(lambda x: int(cnt.get(x, 0)))
        last_cit = ids.map(lambda x: last.get(x, pd.NaT))
    # asistencias 12m desde hoja asistencia
    if df_asis is None or df_asis.empty:
        asis_12m = pd.Series(0, index=ids.index)
    else:
        cols = {c.upper(): c for c in df_asis.columns}
        cand_id = next((cols[c] for c in cols if c in {"SAP","RUT","RUN","ID"}), None)
        cand_as = next((cols[c] for c in cols if "ASIST" in c or "PRESENT" in c), None)
        cand_fe = next((cols[c] for c in cols if "FECHA" in c), None)
        if all([cand_id, cand_as, cand_fe]):
            a = df_asis.copy()
            a[cand_fe] = pd.to_datetime(a[cand_fe], errors="coerce")
            a = a[a[cand_fe] >= year_ago]
            a["OK"] = a[cand_as].apply(parse_bool_asistencia)
            grp = a[a["OK"]].groupby(a[cand_id].astype(str)).size()
            asis_12m = ids.map(lambda x: int(grp.get(x, 0)))
        else:
            asis_12m = pd.Series(0, index=ids.index)
    return cit_12m, asis_12m, last_cit

cit_12m, asis_12m, last_cit = build_metrics(resultado["SURROGATE_ID"])
resultado["Citaciones (últ. 12m)"] = cit_12m.values
resultado["Asistencias (últ. 12m)"] = asis_12m.values
resultado["Última citación"] = pd.to_datetime(last_cit).dt.date

# -------- mostrar --------
fecha_txt = today.strftime("%Y-%m-%d")
salida = construir_salida(resultado, map_n2o, map_o2n, fecha_txt)

# Añadimos las métricas también al DataFrame de salida (al final)
salida["Citaciones (últ. 12m)"] = resultado["Citaciones (últ. 12m)"].values
salida["Asistencias (últ. 12m)"] = resultado["Asistencias (últ. 12m)"].values
salida["Última citación"] = resultado["Última citación"].values

st.success(f"Seleccionados: {len(salida)}  |  Turnos elegidos: {', '.join(map(str, turns_sel))}")
if warnings:
    st.warning(" / ".join(warnings))

st.dataframe(salida, use_container_width=True)

# descarga excel
excel_bytes = df_to_excel_download(salida, sheet="Control")
st.download_button("⬇️ Descargar Excel de Control", excel_bytes,
                   file_name=f"control_AD_{fecha_txt}.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# historial actualizado para descarga
hist_update = pd.DataFrame({
    "SURROGATE_ID": resultado["SURROGATE_ID"].astype(str).tolist(),
    "FECHA": [fecha_txt]*len(resultado),
    "TURNO": resultado[col_regla].astype(str).tolist(),
    "TIMESTAMP": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]*len(resultado),
    "NOMBRE": (
        df_dot[map_o2n.get("Nombre")]
        if map_o2n.get("Nombre") in df_dot.columns else pd.Series([""]*len(resultado))
    ).reindex(resultado.index).tolist()
})
hist2 = pd.concat([hist, hist_update], ignore_index=True)
csv_bytes = hist2.assign(FECHA=hist2["FECHA"].astype(str)).to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
st.download_button("⬇️ Descargar Historial Actualizado (CSV)", csv_bytes,
                   file_name="historial_actualizado.csv", mime="text/csv")

# resumen
with st.expander("📊 Resumen"):
    res_origen = resultado["SELECCION"].str.split(";", expand=True).stack().str.upper().str.strip()
    res_origen = res_origen[res_origen != ""].value_counts().rename_axis("Tipo").reset_index(name="N")
    st.subheader("Por tipo de selección")
    st.dataframe(res_origen, use_container_width=True)

    if 'take3' in locals() and not take3.empty:
        st.subheader("Extras por Gerencia (mismo turno)")
        st.dataframe(take3.groupby(col_ger).size().rename("N").reset_index(), use_container_width=True)

st.caption("Reglas: 4x3 (Lun–Jue, Día) desde la misma columna de Regla; Operadores Mina y Extras del/los turno(s) seleccionados; tope 4/año y preferencia ≥90 días (relaja 60/30/0); reingreso por no‑show; métricas 12m incluidas.")
