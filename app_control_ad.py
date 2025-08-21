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
JORNADAS_4X3_COD = {"4X3DIP01", "4X3DIE01"}      # vienen de "Regla p.plan h.tbjo."
CARGO_OPERADOR    = "OPERADOR MINA"              # buscar en "Denominación Posición"

COL_GERENCIA_ORIG = "Texto Gerencia"
COL_POSICION_ORIG = "Denominación Posición"
COL_REGLA_ORIG    = "Regla p.plan h.tbjo."      # ÚNICA columna para turno/jornada
COL_ASIS_FLAG     = "ASISTENCIA"                 # en hoja asistencia
COL_ASIS_FECHA    = "FECHA"                      # en hoja asistencia

MAX_VECES_ANO = 4
PRIORIDAD_ESPACIADO_DIAS = [90, 60, 30, 0]       # se relaja si falta gente
REINGRESO_NO_SHOW_DIAS = 60                      # si hubo NO en últimos N días, reingresa

# =======================================================================

# Columnas a exportar (quitamos Texto Subgerencia y Texto Departamento)
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
    # nuevas columnas ya calculadas se agregan luego
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

# columnas clave en dotación
try:
    col_regla,    _ = detect_col(
        df_dot, map_n2o, map_o2n, COL_REGLA_ORIG,
        heuristics=[lambda c: ("REGLA" in c and "PLAN" in c and "TBJO" in c)]
    )
    col_posicion, _ = detect_col(
        df_dot, map_n2o, map_o2n, COL_POSICION_ORIG,
        heuristics=[lambda c: ("DENOMINACION" in c or "DENOMINACIÓN" in c) and "POSICI" in c]
    )
    col_ger,      _ = detect_col(
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

# limpieza básica
df_dot = df_dot[~df_dot[col_regla].isna()].copy()
df_dot = df_dot[~df_dot[col_ger].isna()].copy()

# turnos disponibles (multi)
turnos_all = (df_dot[col_regla].astype(str).str.strip()
              .replace("", np.nan).dropna().drop_duplicates().sort_values().tolist())
turns_sel = st.multiselect("Selecciona uno o más turnos (Regla p.plan h.tbjo.)", options=turnos_all, default=turnos_all[:1])
if not turns_sel:
    st.warning("Selecciona al menos un turno.")
    st.stop()

# -------- leer asistencia (requerida para métricas/criterios) --------
try:
    df_asis_raw, hoja_asis = read_sheet(up, "asistencia")
    df_asis = df_asis_raw.copy()
except Exception as e:
    st.error("No se pudo leer la hoja 'asistencia'. Es obligatoria para las métricas (citaciones/asistencias) y criterios de espaciado/tope.")
    st.stop()

# Normalizar asistencia: detectar columnas ID / ASISTENCIA / FECHA
colsA = {c.upper(): c for c in df_asis.columns}
cand_id = next((colsA[c] for c in colsA if c in {"SAP","RUT","RUN","ID"}), None)
cand_as = next((colsA[c] for c in colsA if "ASIST" in c or "PRESENT" in c), None)
cand_fe = next((colsA[c] for c in colsA if "FECHA" in c), None)
if not all([cand_id, cand_as, cand_fe]):
    st.error("En la hoja 'asistencia' deben existir columnas tipo ID (SAP/RUT/RUN/ID), ASISTENCIA y FECHA.")
    st.stop()

df_asis[cand_fe] = pd.to_datetime(df_asis[cand_fe], errors="coerce")
if df_asis[cand_fe].isna().all():
    st.error("No se pudieron interpretar fechas en la hoja 'asistencia'. Revisa la columna FECHA.")
    st.stop()

# ventanas de tiempo
today = pd.Timestamp(fecha_ctrl)
year_ago = today - pd.Timedelta(days=365)
df_asis_12m = df_asis[df_asis[cand_fe].between(year_ago, today, inclusive="both")].copy()
df_asis_recentNO = df_asis[df_asis[cand_fe] >= (today - pd.Timedelta(days=REINGRESO_NO_SHOW_DIAS))].copy()

# Mapas desde asistencia
df_asis_12m["ASIS_OK"] = df_asis_12m[cand_as].apply(parse_bool_asistencia)
cit_count_12m = df_asis_12m[df_asis_12m["ASIS_OK"]].groupby(df_asis_12m[cand_id].astype(str)).size()
last_citation = df_asis.groupby(df_asis[cand_id].astype(str))[cand_fe].max()

# Reingreso por NO reciente: si hay NO en últimos N días, ignoramos su última citación y conteo
mask_no_recent = df_asis_recentNO[cand_as].apply(lambda x: not parse_bool_asistencia(x))
ids_no_recent = set(df_asis_recentNO.loc[mask_no_recent, cand_id].astype(str))

def dias_desde_ultima(id_):
    if str(id_) in ids_no_recent:
        return 99999  # reingresa
    ld = last_citation.get(str(id_), pd.NaT)
    if pd.isna(ld): 
        return 99999
    return (today - ld).days

def conteo_si_12m(id_):
    if str(id_) in ids_no_recent:
        return 0
    return int(cit_count_12m.get(str(id_), 0))

# -------- elegibilidad por asistencia --------
df_dot["ELIGIBLE_BASE"]  = True
df_dot["DIAS_DESDE_ULT"] = df_dot["SURROGATE_ID"].map(dias_desde_ultima)
df_dot["CITACIONES_12M"] = df_dot["SURROGATE_ID"].map(conteo_si_12m)

# Tope 4 por año (según SI en asistencia)
df_dot.loc[df_dot["CITACIONES_12M"] >= MAX_VECES_ANO, "ELIGIBLE_BASE"] = False

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

# 1) 4x3 (Lun–Jue Día), desde la misma columna Regla
reglas_norm = df_dot[col_regla].astype(str).str.upper().str.replace(r"\s+", "", regex=True)
pool_4x3 = pool_base[reglas_norm.isin({j.replace(" ", "").upper() for j in JORNADAS_4X3_COD})].copy() \
           if cupos["JORNADAS_4X3"] > 0 else pool_base.head(0).copy()

# 2) Operadores Mina (cargo) de los turnos seleccionados
pool_oper = pool_base[
    (pool_base[col_regla].astype(str).isin([str(t) for t in turns_sel])) &
    (pool_base[col_posicion].apply(lambda x: safe_contains(x, CARGO_OPERADOR)))
].copy()

# 3) Extras: MISMO(s) turno(s) seleccionados, excluyendo ya tomados y elegibles
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

# Selección 2: Operadores Mina (máx 4) mismos turnos
pool_oper2 = exclude_taken(pool_oper, used)
# Capar el cupo a lo disponible por si hay menos de 4
cup_oper = min(cupos["OPERADORES_MINA"], len(pool_oper2))
take2 = sample_with_spacing(pool_oper2, cup_oper, seed=seed+2)
selected_parts.append(take2)
used.update(take2.index)
if len(take2) < cup_oper:
    warnings.append(f"Operadores Mina: requeridos {cup_oper}, disponibles {len(take2)} (en turnos {', '.join(map(str, turns_sel))}).")

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

# ---------- métricas desde ASISTENCIA ----------
# Citaciones (últ. 12m) = conteo de SI
resultado["Citaciones (últ. 12m)"] = resultado["SURROGATE_ID"].map(lambda x: int(cit_count_12m.get(str(x), 0))).values
# Asistencias (últ. 12m) = SI (igual a lo solicitado)
resultado["Asistencias (últ. 12m)"] = resultado["Citaciones (últ. 12m)"].values
# Última citación = última FECHA registrada en asistencia (cualquier registro)
resultado["Última citación"] = resultado["SURROGATE_ID"].map(lambda x: last_citation.get(str(x), pd.NaT)).values
resultado["Última citación"] = pd.to_datetime(resultado["Última citación"]).dt.date

# -------- salida y descargas --------
fecha_txt = today.strftime("%Y-%m-%d")
salida = construir_salida(resultado, map_n2o, map_o2n, fecha_txt)

# Añadir métricas al Excel final
salida["Citaciones (últ. 12m)"] = resultado["Citaciones (últ. 12m)"].values
salida["Asistencias (últ. 12m)"] = resultado["Asistencias (últ. 12m)"].values
salida["Última citación"]       = resultado["Última citación"].values

st.success(f"Seleccionados: {len(salida)}  |  Turnos elegidos: {', '.join(map(str, turns_sel))}")
if warnings:
    st.warning(" / ".join(warnings))

st.dataframe(salida, use_container_width=True)

# descarga excel
excel_bytes = df_to_excel_download(salida, sheet="Control")
st.download_button("⬇️ Descargar Excel de Control", excel_bytes,
                   file_name=f"control_AD_{fecha_txt}.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# historial actualizado para descarga (con “citación” = selección del día)
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
csv_bytes = hist_update.assign(FECHA=hist_update["FECHA"].astype(str)).to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
st.download_button("⬇️ Descargar Historial del Día (CSV)", csv_bytes,
                   file_name=f"historial_control_{fecha_txt}.csv", mime="text/csv")

# resumen
with st.expander("📊 Resumen"):
    res_origen = resultado["SELECCION"].str.split(";", expand=True).stack().str.upper().str.strip()
    res_origen = res_origen[res_origen != ""].value_counts().rename_axis("Tipo").reset_index(name="N")
    st.subheader("Por tipo de selección")
    st.dataframe(res_origen, use_container_width=True)

    if 'take3' in locals() and not take3.empty:
        st.subheader("Extras por Gerencia (mismo turno)")
        st.dataframe(take3.groupby(col_ger).size().rename("N").reset_index(), use_container_width=True)

st.caption("Reglas: 4x3 (Lun–Jue / Día) desde Regla; Operador Mina (cargo) máx 4 del/los turno(s) seleccionados; Extras del mismo turno con proporcionalidad por Gerencia; tope 4/año y preferencia ≥90 días desde última citación (ambos desde Asistencia).")
