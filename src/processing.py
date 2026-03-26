"""
processing.py
-------------
Calcula rentabilidades efectivas anuales (EA) para cada registro de la
tabla de hechos de Fondos de Inversión Colectiva (FICs), a partir del
valor_unidad_operaciones descargado por ingestion.py.

Pasos del pipeline:
    1. Carga  data/raw/fics_alternativos_latest.parquet
    2. Filtra: por cada grupo (tipo_entidad, codigo_entidad, codigo_negocio,
               tipo_participacion) conserva solo el menor principal_compartimento.
    3. Calcula rentabilidades EA usando lag por posición (no por fecha
               calendario):
                   rent_ea_Nd = (VU_hoy / VU[pos - N]) ^ (365 / dias_reales) - 1
               donde dias_reales = fecha_corte_hoy - fecha_corte_hace_N_registros
               (se usa la diferencia real de días para no asumir periodicidad diaria exacta).
    4. Genera columna max_horizonte con el mayor horizonte calculado en cada fila.
    5. Guarda tres archivos en data/processed/:
         a. fics_rentabilidades_latest.parquet        — tabla ancha completa (original)
         b. fics_rentabilidades_largo_latest.parquet  — claves + fecha + rentabilidades
                                                         en formato LARGO (una fila por
                                                         horizonte)
         c. fics_metricas_latest.parquet              — claves + fecha + métricas
                                                         operativas + max_horizonte

Horizontes calculados (en número de registros hacia atrás):
    30, 60, 120, 180, 360

Columnas de métricas conservadas en la salida:
    valor_unidad_operaciones, numero_unidades_fondo_cierre,
    valor_fondo_cierre_dia_t, numero_inversionistas, rendimientos_abonados

Columna calculada de flujo:
    flujo_neto_inversionistas = aportes_recibidos - retiros_redenciones + anulaciones
    (las tres columnas fuente se descartan de la salida final)

Columnas de rentabilidad resultantes (formato ancho):
    rent_ea_30d, rent_ea_60d, rent_ea_120d, rent_ea_180d, rent_ea_360d

Columna auxiliar:
    max_horizonte  →  entero con el mayor N para el que pudo calcularse la
                      rentabilidad en esa fila (0 si ninguno fue posible).
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

RAW_DIR       = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

ARCHIVO_ENTRADA  = RAW_DIR / "fics_alternativos_latest.parquet"
ARCHIVO_SALIDA   = PROCESSED_DIR / "fics_rentabilidades_latest.parquet"

# Horizontes en número de registros hacia atrás
HORIZONTES: list[int] = [30, 60, 90, 120, 180, 360]

# Clave de grupo para identificar cada serie de fondo/participación
GRUPO_COLS = [
    "tipo_entidad",
    "codigo_entidad",
    "codigo_negocio",
    "tipo_participacion",
]

# Clave completa de la tabla de hechos (incluye fecha)
PK_COLS = GRUPO_COLS + ["fecha_corte"]

# Columnas de métricas que se conservan en la salida final
COLS_METRICAS = [
    "valor_unidad_operaciones",
    "numero_unidades_fondo_cierre",
    "valor_fondo_cierre_dia_t",
    "numero_inversionistas",
    "rendimientos_abonados",
]

# Columnas fuente para calcular flujo_neto_inversionistas
COLS_FLUJO = ["aportes_recibidos", "retiros_redenciones", "anulaciones"]


# ---------------------------------------------------------------------------
# Paso 1 — Carga
# ---------------------------------------------------------------------------

def load_raw() -> pd.DataFrame:
    """
    Carga el parquet de hechos generado por ingestion.py.
    Lanza FileNotFoundError si no existe (se debe correr ingestion primero).
    """
    if not ARCHIVO_ENTRADA.exists():
        raise FileNotFoundError(
            f"No se encontró {ARCHIVO_ENTRADA}. "
            "Ejecute run_ingestion() primero para descargar los datos."
        )

    df = pd.read_parquet(ARCHIVO_ENTRADA)
    print(f"Registros cargados:  {len(df):>10,}")
    print(f"Columnas:            {list(df.columns)}")
    return df


# ---------------------------------------------------------------------------
# Paso 2 — Filtro de principal_compartimento
# ---------------------------------------------------------------------------

def filter_principal_compartimento(df: pd.DataFrame) -> pd.DataFrame:
    """
    Para cada grupo (tipo_entidad, codigo_entidad, codigo_negocio,
    tipo_participacion) conserva únicamente los registros cuyo
    principal_compartimento es el mínimo dentro del grupo.

    Cobertura: el 99.99 % de los grupos tiene un solo compartimento;
    para los que tienen más de uno se elige el menor (habitualmente el
    compartimento "principal" designado por la entidad).
    """
    if "principal_compartimento" not in df.columns:
        print("⚠ Columna 'principal_compartimento' no encontrada; se omite el filtro.")
        return df

    antes = len(df)

    # Calcular el mínimo compartimento por grupo
    min_comp = (
        df.groupby(GRUPO_COLS)["principal_compartimento"]
        .min()
        .rename("_min_comp")
        .reset_index()
    )

    df = df.merge(min_comp, on=GRUPO_COLS, how="left")
    df = df[df["principal_compartimento"] == df["_min_comp"]].drop(columns="_min_comp")
    df = df.reset_index(drop=True)

    despues = len(df)
    eliminados = antes - despues
    if eliminados:
        print(f"Filtro compartimento: {eliminados:,} registros eliminados "
              f"(compartimentos secundarios).")
    else:
        print("Filtro compartimento: todos los registros son del compartimento mínimo.")

    return df


# ---------------------------------------------------------------------------
# Paso 3 — Columna flujo_neto_inversionistas y selección de métricas
# ---------------------------------------------------------------------------

def calcular_flujo_y_seleccionar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Calcula flujo_neto_inversionistas = aportes_recibidos
                                           - retiros_redenciones
                                           + anulaciones

       Cualquier columna fuente ausente o con NaN se trata como 0 para
       que el cálculo sea robusto ante datos incompletos.

    2. Conserva en el DataFrame solo las columnas necesarias para las
       etapas posteriores (claves + métricas + flujo). Las columnas fuente
       de flujo (aportes_recibidos, retiros_redenciones, anulaciones) y
       cualquier otra columna de ingestion.py que no sea relevante para el
       procesamiento se descartan.

    Columnas de salida garantizadas (si existen en la entrada):
        - PK_COLS          : GRUPO_COLS + fecha_corte + principal_compartimento
        - COLS_METRICAS    : valor_unidad_operaciones, numero_unidades_fondo_cierre,
                             valor_fondo_cierre_dia_t, numero_inversionistas,
                             rendimientos_abonados
        - flujo_neto_inversionistas  (calculada aquí)
    """
    df = df.copy()

    # --- Calcular flujo neto con fill_value=0 para NaN en columnas fuente ---
    aportes   = df.get("aportes_recibidos",  pd.Series(0.0, index=df.index)).fillna(0)
    retiros   = df.get("retiros_redenciones", pd.Series(0.0, index=df.index)).fillna(0)
    anulac    = df.get("anulaciones",         pd.Series(0.0, index=df.index)).fillna(0)

    df["flujo_neto_inversionistas"] = aportes - retiros + anulac

    cols_flujo_presentes = [c for c in COLS_FLUJO if c in df.columns]
    if cols_flujo_presentes:
        print(f"Columna 'flujo_neto_inversionistas' calculada "
              f"(fuentes: {cols_flujo_presentes}).")
    else:
        print("⚠ Columnas fuente de flujo no encontradas; "
              "flujo_neto_inversionistas = 0 en todas las filas.")

    # --- Seleccionar columnas de salida ---
    cols_pk      = [c for c in (GRUPO_COLS + ["fecha_corte", "principal_compartimento"])
                    if c in df.columns]
    cols_met     = [c for c in COLS_METRICAS if c in df.columns]
    cols_salida  = cols_pk + cols_met + ["flujo_neto_inversionistas"]

    return df[cols_salida]


# ---------------------------------------------------------------------------
# Paso 4 — Cálculo de rentabilidades EA por lag de posición
# ---------------------------------------------------------------------------

def _calcular_rent_ea(
    vu: pd.Series,
    fechas: pd.Series,
    n: int,
) -> pd.Series:
    """
    Calcula la rentabilidad efectiva anual para un lag de N registros.

    Fórmula:
        rent_ea = (VU_hoy / VU[pos - N]) ^ (365 / dias_reales) - 1

    donde dias_reales es la diferencia calendario entre la fecha del
    registro actual y la fecha del registro en la posición pos - N.

    Parámetros
    ----------
    vu     : pd.Series  — valor_unidad_operaciones, ya ordenado por fecha
    fechas : pd.Series  — fecha_corte correspondiente a cada VU
    n      : int        — número de registros hacia atrás (lag)

    Retorna
    -------
    pd.Series de float con la rentabilidad EA (NaN donde no hay suficiente historial).
    """
    vu_lag     = vu.shift(n)
    fechas_lag = fechas.shift(n)

    # Días reales entre la fecha actual y la fecha del registro lagged
    dias_reales = (fechas - fechas_lag).dt.days

    # Evitar divisiones por cero o negativos (datos desordenados o duplicados)
    valid = (vu_lag > 0) & (dias_reales > 0)

    rent = pd.Series(np.nan, index=vu.index)
    rent[valid] = (vu[valid] / vu_lag[valid]) ** (365.0 / dias_reales[valid]) - 1

    return rent


def calcular_rentabilidades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula, para cada fila del DataFrame, las rentabilidades EA a 30, 60,
    120, 180 y 360 registros de lag.

    Requiere que df esté ordenado por (GRUPO_COLS + fecha_corte), que es
    el orden natural producido por clean_dataframe() en ingestion.py.

    Las columnas nuevas son:
        rent_ea_30d, rent_ea_60d, rent_ea_120d, rent_ea_180d, rent_ea_360d

    El cálculo se hace por grupo para evitar que el lag "cruce" entre
    fondos distintos.
    """
    if "valor_unidad_operaciones" not in df.columns:
        raise ValueError("La columna 'valor_unidad_operaciones' no está en el DataFrame.")
    if "fecha_corte" not in df.columns:
        raise ValueError("La columna 'fecha_corte' no está en el DataFrame.")

    # Asegurar orden correcto antes del lag
    df = df.sort_values(GRUPO_COLS + ["fecha_corte"]).reset_index(drop=True)

    # Inicializar columnas de rentabilidad con NaN
    for n in HORIZONTES:
        df[f"rent_ea_{n}d"] = np.nan

    # Calcular por grupo para que el lag no "sangre" entre fondos distintos
    grupos = df.groupby(GRUPO_COLS, sort=False)
    total_grupos = grupos.ngroups
    print(f"\nCalculando rentabilidades EA para {total_grupos} grupo(s)...")

    for nombre_grupo, idx in grupos.groups.items():
        sub = df.loc[idx].copy()

        for n in HORIZONTES:
            col = f"rent_ea_{n}d"
            sub[col] = _calcular_rent_ea(
                vu=sub["valor_unidad_operaciones"].reset_index(drop=True),
                fechas=sub["fecha_corte"].reset_index(drop=True),
                n=n,
            ).values  # .values para respetar el índice original al asignar

            df.loc[idx, col] = sub[col].values

    return df


# ---------------------------------------------------------------------------
# Paso 5 — Columna max_horizonte
# ---------------------------------------------------------------------------

def calcular_max_horizonte(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega la columna max_horizonte: el mayor N (en días de lag) para el
    que se pudo calcular la rentabilidad EA en cada fila.

    Valor 0 indica que ningún horizonte pudo calcularse (fondo con muy
    pocos registros o VU nulos).

    Ejemplo:
        Si rent_ea_30d y rent_ea_60d tienen valor pero rent_ea_120d es NaN,
        entonces max_horizonte = 60.
    """
    cols_rent = [f"rent_ea_{n}d" for n in HORIZONTES]
    cols_presentes = [c for c in cols_rent if c in df.columns]

    if not cols_presentes:
        df["max_horizonte"] = 0
        return df

    df["max_horizonte"] = df[cols_presentes + []].apply(
        lambda row: max(
            (n for n in HORIZONTES if pd.notna(row.get(f"rent_ea_{n}d", np.nan))),
            default=0,
        ),
        axis=1,
    )

    # Resumen por grupo
    resumen = (
        df.groupby(GRUPO_COLS)["max_horizonte"]
        .max()
        .reset_index()
        .rename(columns={"max_horizonte": "max_horizonte_grupo"})
    )
    print("\nMáximo horizonte calculable por grupo:")
    print(resumen.to_string(index=False))

    return df


# ---------------------------------------------------------------------------
# Paso 6 — Persistencia (tres salidas)
# ---------------------------------------------------------------------------

def _build_largo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte las columnas de rentabilidad EA de formato ancho a formato largo.

    Entrada (ancho):
        tipo_entidad | codigo_entidad | ... | fecha_corte | rent_ea_30d | rent_ea_60d | ...

    Salida (largo):
        tipo_entidad | codigo_entidad | ... | fecha_corte | horizonte_registros | horizonte_dias | rent_ea

    Columnas de salida
    ------------------
    - Todas las columnas de GRUPO_COLS
    - fecha_corte
    - horizonte_registros (int)  : número de registros de lag (30, 60, 120, 180, 360)
    - horizonte_dias (int)       : días calendario reales cubiertos por ese lag
                                   (media del grupo en esa fecha; puede ser NaN si
                                    la columna no existe en df)
    - rent_ea (float)            : rentabilidad efectiva anual calculada

    Solo se incluyen filas donde rent_ea no es NaN (es decir, donde había
    suficiente historial para calcular ese horizonte).
    """
    cols_id   = GRUPO_COLS + ["fecha_corte"]
    cols_rent = [f"rent_ea_{n}d" for n in HORIZONTES if f"rent_ea_{n}d" in df.columns]

    largo = df[cols_id + cols_rent].melt(
        id_vars=cols_id,
        value_vars=cols_rent,
        var_name="horizonte_col",
        value_name="rent_ea",
    )

    # Extraer número de registros de lag desde el nombre de la columna
    # "rent_ea_30d" → 30
    largo["horizonte_registros"] = (
        largo["horizonte_col"]
        .str.extract(r"rent_ea_(\d+)d")[0]
        .astype(int)
    )

    largo = largo.drop(columns="horizonte_col")

    # Descartar filas sin rentabilidad calculada
    largo = largo.dropna(subset=["rent_ea"]).reset_index(drop=True)

    # Ordenar de forma natural
    largo = largo.sort_values(
        GRUPO_COLS + ["fecha_corte", "horizonte_registros"]
    ).reset_index(drop=True)

    return largo


def _build_metricas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la tabla de métricas operativas y flujo, sin las columnas de
    rentabilidad EA.

    Columnas de salida:
        - GRUPO_COLS
        - fecha_corte
        - principal_compartimento
        - valor_unidad_operaciones
        - numero_unidades_fondo_cierre
        - valor_fondo_cierre_dia_t
        - numero_inversionistas
        - rendimientos_abonados
        - flujo_neto_inversionistas
        - max_horizonte
    """
    cols_metricas = (
        GRUPO_COLS
        + ["fecha_corte", "principal_compartimento"]
        + [c for c in COLS_METRICAS if c in df.columns]
        + ["flujo_neto_inversionistas", "max_horizonte"]
    )
    cols_presentes = [c for c in cols_metricas if c in df.columns]
    return df[cols_presentes].reset_index(drop=True)


def save_processed(df: pd.DataFrame) -> dict[str, Path]:
    """
    Guarda tres artefactos en data/processed/:

    1. fics_rentabilidades_latest.parquet
       fics_rentabilidades_<timestamp>.parquet
         → Tabla ancha completa (igual que antes, para compatibilidad).

    2. fics_rentabilidades_largo_latest.parquet
       fics_rentabilidades_largo_<timestamp>.parquet
         → Claves de grupo + fecha_corte + horizonte_registros + rent_ea
           en formato largo (solo filas con rent_ea no nulo).

    3. fics_metricas_latest.parquet
       fics_metricas_<timestamp>.parquet
         → Claves de grupo + fecha_corte + métricas operativas +
           flujo_neto_inversionistas + max_horizonte.

    Retorna un dict con las rutas de los archivos "_latest" de cada salida.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ── Salida 1: tabla ancha completa ──────────────────────────────────────
    archivo_ancho_ts  = PROCESSED_DIR / f"fics_rentabilidades_{timestamp}.parquet"
    archivo_ancho_lat = PROCESSED_DIR / "fics_rentabilidades_latest.parquet"
    df.to_parquet(archivo_ancho_ts,  index=False)
    df.to_parquet(archivo_ancho_lat, index=False)

    # ── Salida 2: rentabilidades en formato largo ────────────────────────────
    df_largo = _build_largo(df)
    archivo_largo_ts  = PROCESSED_DIR / f"fics_rentabilidades_largo_{timestamp}.parquet"
    archivo_largo_lat = PROCESSED_DIR / "fics_rentabilidades_largo_latest.parquet"
    df_largo.to_parquet(archivo_largo_ts,  index=False)
    df_largo.to_parquet(archivo_largo_lat, index=False)

    # ── Salida 3: métricas operativas ────────────────────────────────────────
    df_metricas = _build_metricas(df)
    archivo_met_ts  = PROCESSED_DIR / f"fics_metricas_{timestamp}.parquet"
    archivo_met_lat = PROCESSED_DIR / "fics_metricas_latest.parquet"
    df_metricas.to_parquet(archivo_met_ts,  index=False)
    df_metricas.to_parquet(archivo_met_lat, index=False)

    # ── Resumen en consola ───────────────────────────────────────────────────
    print(f"\nSalida 1 — tabla ancha:           {archivo_ancho_ts.name}")
    print(f"                                  {archivo_ancho_lat.name}")
    print(f"  Filas: {len(df):>10,}   Columnas: {len(df.columns)}")

    print(f"\nSalida 2 — rentabilidades largo:  {archivo_largo_ts.name}")
    print(f"                                  {archivo_largo_lat.name}")
    print(f"  Filas: {len(df_largo):>10,}   Columnas: {len(df_largo.columns)}")
    print(f"  Columnas: {list(df_largo.columns)}")

    print(f"\nSalida 3 — métricas operativas:   {archivo_met_ts.name}")
    print(f"                                  {archivo_met_lat.name}")
    print(f"  Filas: {len(df_metricas):>10,}   Columnas: {len(df_metricas.columns)}")
    print(f"  Columnas: {list(df_metricas.columns)}")

    return {
        "ancho":    archivo_ancho_lat,
        "largo":    archivo_largo_lat,
        "metricas": archivo_met_lat,
    }


# ---------------------------------------------------------------------------
# Resumen diagnóstico
# ---------------------------------------------------------------------------

def _print_resumen(df: pd.DataFrame) -> None:
    """Imprime un resumen por grupo con conteos y cobertura de horizontes."""
    cols_rent = [f"rent_ea_{n}d" for n in HORIZONTES]

    resumen_rows = []
    for nombre, sub in df.groupby(GRUPO_COLS):
        fila = {
            "tipo_entidad":      nombre[0],
            "codigo_entidad":    nombre[1],
            "codigo_negocio":    nombre[2],
            "tipo_participacion":nombre[3],
            "registros":         len(sub),
            "fecha_inicio":      sub["fecha_corte"].min().date(),
            "fecha_fin":         sub["fecha_corte"].max().date(),
        }
        for col in cols_rent:
            if col in sub.columns:
                fila[col + "_ok"] = sub[col].notna().sum()
        resumen_rows.append(fila)

    resumen = pd.DataFrame(resumen_rows)
    print("\n" + "=" * 70)
    print("RESUMEN DE RENTABILIDADES CALCULADAS")
    print("=" * 70)
    print(resumen.to_string(index=False))
    print("=" * 70)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def run_processing() -> dict[str, pd.DataFrame]:
    """
    Pipeline completo de procesamiento de rentabilidades EA.

    Flujo:
        1. Carga data/raw/fics_alternativos_latest.parquet
        2. Filtra al menor principal_compartimento por grupo
        3. Calcula flujo_neto_inversionistas y selecciona columnas de salida
        4. Calcula rentabilidades EA a 30, 60, 120, 180 y 360 registros de lag
        5. Agrega columna max_horizonte
        6. Guarda tres artefactos en data/processed/

    Retorna
    -------
    dict con tres DataFrames:
        {
            "ancho":    pd.DataFrame,  — tabla ancha completa
            "largo":    pd.DataFrame,  — rentabilidades en formato largo
            "metricas": pd.DataFrame,  — métricas operativas + max_horizonte
        }

    Estructura de "largo":
        tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion,
        fecha_corte, horizonte_registros (int), rent_ea (float)

    Estructura de "metricas":
        tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion,
        fecha_corte, principal_compartimento,
        valor_unidad_operaciones, numero_unidades_fondo_cierre,
        valor_fondo_cierre_dia_t, numero_inversionistas,
        rendimientos_abonados, flujo_neto_inversionistas,
        max_horizonte

    Ejemplo de uso desde la app Shiny
    ----------------------------------
    from processing import run_processing

    resultados = run_processing()
    df_largo    = resultados["largo"]
    df_metricas = resultados["metricas"]
    """
    print("=" * 70)
    print("PROCESAMIENTO DE RENTABILIDADES EA — FICs")
    print("=" * 70)

    # 1. Cargar hechos crudos
    df = load_raw()

    # 2. Conservar solo el menor compartimento por grupo
    df = filter_principal_compartimento(df)

    # 3. Calcular flujo_neto_inversionistas y seleccionar columnas
    df = calcular_flujo_y_seleccionar_columnas(df)

    # 4. Calcular rentabilidades EA por lag de posición
    df = calcular_rentabilidades(df)

    # 5. Columna max_horizonte
    df = calcular_max_horizonte(df)

    # 6. Resumen diagnóstico
    _print_resumen(df)

    # 7. Guardar tres salidas y obtener rutas
    rutas = save_processed(df)

    print("\n✓ Procesamiento completado.")
    print("=" * 70)

    # Construir y retornar los tres DataFrames
    return {
        "ancho":    df,
        "largo":    _build_largo(df),
        "metricas": _build_metricas(df),
    }


# ---------------------------------------------------------------------------
# Helpers para la app Shiny
# ---------------------------------------------------------------------------

def load_processed() -> pd.DataFrame:
    """
    Carga la tabla ancha procesada desde data/processed/.
    Útil para que la app consuma los datos sin reejecutar el pipeline.
    """
    archivo = PROCESSED_DIR / "fics_rentabilidades_latest.parquet"
    if not archivo.exists():
        raise FileNotFoundError(
            f"No se encontró {archivo}. "
            "Ejecute run_processing() primero."
        )
    return pd.read_parquet(archivo)


def load_largo() -> pd.DataFrame:
    """
    Carga las rentabilidades en formato largo desde data/processed/.

    Columnas: GRUPO_COLS + fecha_corte + horizonte_registros + rent_ea
    """
    archivo = PROCESSED_DIR / "fics_rentabilidades_largo_latest.parquet"
    if not archivo.exists():
        raise FileNotFoundError(
            f"No se encontró {archivo}. "
            "Ejecute run_processing() primero."
        )
    return pd.read_parquet(archivo)


def load_metricas() -> pd.DataFrame:
    """
    Carga las métricas operativas desde data/processed/.

    Columnas: GRUPO_COLS + fecha_corte + métricas + flujo + max_horizonte
    """
    archivo = PROCESSED_DIR / "fics_metricas_latest.parquet"
    if not archivo.exists():
        raise FileNotFoundError(
            f"No se encontró {archivo}. "
            "Ejecute run_processing() primero."
        )
    return pd.read_parquet(archivo)


def processing_disponible() -> bool:
    """Retorna True si todos los parquets procesados existen y están listos."""
    return all(
        (PROCESSED_DIR / f).exists()
        for f in (
            "fics_rentabilidades_latest.parquet",
            "fics_rentabilidades_largo_latest.parquet",
            "fics_metricas_latest.parquet",
        )
    )


# ---------------------------------------------------------------------------
# Entry point para pruebas locales
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    resultados = run_processing()

    print("\n── Muestra tabla LARGO (primeras 10 filas) ──")
    print(resultados["largo"].head(10).to_string(index=False))

    print("\n── Muestra tabla MÉTRICAS (primeras 5 filas) ──")
    print(resultados["metricas"].head(5).to_string(index=False))