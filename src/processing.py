"""
processing.py
-------------
Calcula la rentabilidad diaria (respecto al día anterior) para cada registro
de la tabla de hechos de Fondos de Inversión Colectiva (FICs), a partir del
valor_unidad_operaciones descargado por ingestion.py.

Pasos del pipeline:
    1. Carga  data/raw/fics_alternativos_latest.parquet
    2. Filtra: por cada grupo (tipo_entidad, codigo_entidad, codigo_negocio,
               tipo_participacion) conserva solo el menor principal_compartimento.
    3. Calcula flujo_neto_inversionistas y selecciona columnas de salida.
    4. Calcula ratio diario usando lag de 1 registro (día anterior):
                   rent_diaria = VU_hoy / VU_ayer
    5. Filtra registros donde rent_diaria es NaN (típicamente el primer registro
               de cada fondo/participación donde no hay día anterior).
    6. Marca festivos y fines de semana para Colombia y EE.UU. en dos columnas
               binarias (0 = día hábil normal, 1 = fin de semana o festivo).
    7. Guarda un único archivo en data/processed/:
         - fics_rentabilidades_latest.parquet  — tabla con claves, métricas,
                                                  rentabilidad diaria y festivos

Columnas de métricas conservadas en la salida:
    valor_unidad_operaciones, numero_unidades_fondo_cierre,
    valor_fondo_cierre_dia_t, numero_inversionistas, rendimientos_abonados

Columna calculada de flujo:
    flujo_neto_inversionistas = aportes_recibidos - retiros_redenciones + anulaciones
    (las tres columnas fuente se descartan de la salida final)

Columna de ratio resultante:
    rent_diaria — ratio diario del cambio de valor_unidad_operaciones
                  respecto al día anterior (sin valores NaN)

Columnas de marcado:
    is_holiday_or_weekend_co — 1 si es fin de semana (sábado, domingo) o festivo colombiano
    is_holiday_or_weekend_us — 1 si es fin de semana (sábado, domingo) o festivo de EE.UU.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import holidays
from vacuum import run_vacuum

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

RAW_DIR       = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

ARCHIVO_ENTRADA  = RAW_DIR / "fics_alternativos_latest.parquet"
ARCHIVO_SALIDA   = PROCESSED_DIR / "fics_rentabilidades_latest.parquet"

# Lag para calcular rentabilidad diaria (1 registro = 1 día anterior)
LAG_DIARIO: int = 1

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

def _calcular_rent_diaria(
    vu: pd.Series,
    fechas: pd.Series,
) -> pd.Series:
    """
    Calcula el ratio diario (respecto al día anterior).

    Fórmula:
        rent_diaria = VU_hoy / VU_ayer

    Parámetros
    ----------
    vu     : pd.Series  — valor_unidad_operaciones, ya ordenado por fecha
    fechas : pd.Series  — fecha_corte correspondiente a cada VU

    Retorna
    -------
    pd.Series de float con el ratio diario (NaN donde no hay día anterior).
    """
    vu_lag     = vu.shift(LAG_DIARIO)
    fechas_lag = fechas.shift(LAG_DIARIO)

    # Días reales entre la fecha actual y la fecha del día anterior
    dias_reales = (fechas - fechas_lag).dt.days

    # Evitar divisiones por cero o negativos (datos desordenados o duplicados)
    valid = (vu_lag > 0) & (dias_reales > 0)

    rent = pd.Series(np.nan, index=vu.index)
    rent[valid] = vu[valid] / vu_lag[valid]

    return rent


def calcular_rentabilidades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el ratio diario (respecto al día anterior) para cada fila.

    Requiere que df esté ordenado por (GRUPO_COLS + fecha_corte), que es
    el orden natural producido por clean_dataframe() en ingestion.py.

    La nueva columna es:
        rent_diaria — ratio del cambio diario de valor unitario

    El cálculo se hace por grupo para evitar que el lag "cruce" entre
    fondos distintos.
    """
    if "valor_unidad_operaciones" not in df.columns:
        raise ValueError("La columna 'valor_unidad_operaciones' no está en el DataFrame.")
    if "fecha_corte" not in df.columns:
        raise ValueError("La columna 'fecha_corte' no está en el DataFrame.")

    # Asegurar orden correcto antes del lag
    df = df.sort_values(GRUPO_COLS + ["fecha_corte"]).reset_index(drop=True)

    # Inicializar columna de rentabilidad con NaN
    df["rent_diaria"] = np.nan

    # Calcular por grupo para que el lag no "sangre" entre fondos distintos
    grupos = df.groupby(GRUPO_COLS, sort=False)
    total_grupos = grupos.ngroups
    print(f"\nCalculando rentabilidades diarias para {total_grupos} grupo(s)...")

    for nombre_grupo, idx in grupos.groups.items():
        sub = df.loc[idx].copy()

        sub["rent_diaria"] = _calcular_rent_diaria(
            vu=sub["valor_unidad_operaciones"].reset_index(drop=True),
            fechas=sub["fecha_corte"].reset_index(drop=True),
        ).values

        df.loc[idx, "rent_diaria"] = sub["rent_diaria"].values

    return df




# ---------------------------------------------------------------------------
# Paso 5 — Filtro de NaN en rent_diaria
# ---------------------------------------------------------------------------

def filter_na_rentabilidades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina los registros donde rent_diaria es NaN.
    Habitualmente estos son el primer registro de cada fondo/participación
    (donde no hay datos del día anterior para calcular la rentabilidad).
    
    Retorna el DataFrame filtrado.
    """
    antes = len(df)
    df = df.dropna(subset=["rent_diaria"]).reset_index(drop=True)
    despues = len(df)
    eliminados = antes - despues
    
    if eliminados:
        print(f"\nFiltro rent_diaria: {eliminados:,} registros con NaN eliminados.")
    else:
        print("\nFiltro rent_diaria: todos los registros tienen rent_diaria válida.")
    
    return df


def marcar_festivos_y_fines_de_semana(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega dos columnas binarias para identificar fines de semana y festivos:
    
    - is_holiday_or_weekend_co: 1 si es sábado, domingo o festivo en Colombia; 0 si no
    - is_holiday_or_weekend_us: 1 si es sábado, domingo o festivo en EE.UU.; 0 si no
    
    Útil para análisis posterior: fines de semana y festivos tienen volatilidad diferente.
    
    Retorna el DataFrame con las dos nuevas columnas.
    """
    df = df.copy()
    
    # Obtener calendarios de festivos para ambos países
    # Se cubre el rango de fechas presentes en los datos
    if df.empty:
        return df
    
    fecha_min = df["fecha_corte"].min()
    fecha_max = df["fecha_corte"].max()
    
    # Crear conjuntos de festivos para cada país
    holidays_co = holidays.Colombia(years=range(fecha_min.year, fecha_max.year + 1))
    holidays_us = holidays.US(years=range(fecha_min.year, fecha_max.year + 1))
    
    # Crear columnas binarias
    # weekday() retorna 0-6 donde 5=sábado, 6=domingo
    df["is_holiday_or_weekend_co"] = df["fecha_corte"].apply(
        lambda fecha: 1 if (fecha.weekday() >= 5 or fecha in holidays_co) else 0
    )
    
    df["is_holiday_or_weekend_us"] = df["fecha_corte"].apply(
        lambda fecha: 1 if (fecha.weekday() >= 5 or fecha in holidays_us) else 0
    )
    
    co_holidays_count = df["is_holiday_or_weekend_co"].sum()
    us_holidays_count = df["is_holiday_or_weekend_us"].sum()
    
    print(f"\nMarcado de festivos y fines de semana:")
    print(f"  Registros en festivos/fines de semana Colombia: {co_holidays_count:,}")
    print(f"  Registros en festivos/fines de semana EE.UU.:  {us_holidays_count:,}")
    
    return df


def save_processed(df: pd.DataFrame) -> Path:
    """
    Guarda un único artefacto en data/processed/:

    - fics_rentabilidades_latest.parquet
      fics_rentabilidades_<timestamp>.parquet
        → Tabla con claves de grupo, fecha_corte, métricas operativas,
          flujo_neto_inversionistas y rent_diaria.

    Retorna la ruta del archivo "_latest".
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    archivo_ts  = PROCESSED_DIR / f"fics_rentabilidades_{timestamp}.parquet"
    archivo_lat = PROCESSED_DIR / "fics_rentabilidades_latest.parquet"

    df.to_parquet(archivo_ts,  index=False)
    df.to_parquet(archivo_lat, index=False)

    print(f"\nSalida guardada:         {archivo_ts.name}")
    print(f"                         {archivo_lat.name}")
    print(f"  Filas: {len(df):>10,}   Columnas: {len(df.columns)}")
    print(f"  Columnas: {list(df.columns)}")

    return archivo_lat


# ---------------------------------------------------------------------------
# Resumen diagnóstico
# ---------------------------------------------------------------------------

def _print_resumen(df: pd.DataFrame) -> None:
    """Imprime un resumen por grupo con conteos y cobertura de rentabilidad diaria."""

    resumen_rows = []
    for nombre, sub in df.groupby(GRUPO_COLS):
        fila = {
            "tipo_entidad":      nombre[0],
            "codigo_entidad":    nombre[1],
            "codigo_negocio":    nombre[2],
            "tipo_participacion": nombre[3],
            "registros":         len(sub),
            "fecha_inicio":      sub["fecha_corte"].min().date(),
            "fecha_fin":         sub["fecha_corte"].max().date(),
            "rent_diaria_ok":    sub["rent_diaria"].notna().sum() if "rent_diaria" in sub.columns else 0,
        }
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

def run_processing() -> pd.DataFrame:
    """
    Pipeline completo de procesamiento de rentabilidades diarias.

    Flujo:
        1. Carga data/raw/fics_alternativos_latest.parquet
        2. Filtra al menor principal_compartimento por grupo
        3. Calcula flujo_neto_inversionistas y selecciona columnas de salida
        4. Calcula rentabilidad diaria (respecto al día anterior)
        5. Filtra registros sin rentabilidad calculada (NaN)
        6. Marca festivos y fines de semana (Colombia y EE.UU.)
        7. Guarda un único artefacto en data/processed/

    Retorna
    -------
    pd.DataFrame con todas las columnas: claves, fecha_corte, métricas,
    flujo_neto_inversionistas, rent_diaria (sin NaN), y marcas de festivos.

    Columnas de salida:
        - tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion
        - fecha_corte
        - principal_compartimento
        - valor_unidad_operaciones
        - numero_unidades_fondo_cierre
        - valor_fondo_cierre_dia_t
        - numero_inversionistas
        - rendimientos_abonados
        - flujo_neto_inversionistas
        - rent_diaria (sin valores NaN)
        - is_holiday_or_weekend_co (1 si es fin de semana o festivo en Colombia)
        - is_holiday_or_weekend_us (1 si es fin de semana o festivo en EE.UU.)

    Ejemplo de uso desde la app Shiny
    ----------------------------------
    from processing import run_processing

    df = run_processing()
    """
    print("=" * 70)
    print("PROCESAMIENTO DE RENTABILIDADES DIARIAS — FICs")
    print("=" * 70)

    # 1. Cargar hechos crudos
    df = load_raw()

    # 2. Conservar solo el menor compartimento por grupo
    df = filter_principal_compartimento(df)

    # 3. Calcular flujo_neto_inversionistas y seleccionar columnas
    df = calcular_flujo_y_seleccionar_columnas(df)

    # 4. Calcular rentabilidades diarias
    df = calcular_rentabilidades(df)

    # 5. Filtrar registros sin rentabilidad calculada (NaN)
    df = filter_na_rentabilidades(df)

    # 6. Marcar festivos y fines de semana
    df = marcar_festivos_y_fines_de_semana(df)

    # 7. Resumen diagnóstico
    _print_resumen(df)

    # 8. Guardar salida única
    save_processed(df)

    # 9. Limpiar históricos antiguos (automático)
    print("\n🧹 Ejecutando limpieza de históricos...")
    resultado_vacuum = run_vacuum(
        days_retention=7,
        dry_run=False,
        verbose=False  # ← Sin prints para no contaminar output
    )
    print(f"   {resultado_vacuum['mensaje_ui']}")

    print("\n✓ Procesamiento completado.")
    print("=" * 70)

    return df


# ---------------------------------------------------------------------------
# Helpers para la app Shiny
# ---------------------------------------------------------------------------

def load_processed() -> pd.DataFrame:
    """
    Carga la tabla procesada desde data/processed/.
    Útil para que la app consuma los datos sin reejecutar el pipeline.
    
    Retorna un DataFrame con claves, fecha_corte, métricas, flujo e rent_diaria.
    """
    archivo = PROCESSED_DIR / "fics_rentabilidades_latest.parquet"
    if not archivo.exists():
        raise FileNotFoundError(
            f"No se encontró {archivo}. "
            "Ejecute run_processing() primero."
        )
    return pd.read_parquet(archivo)


# ---------------------------------------------------------------------------
# Entry point para pruebas locales
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    df = run_processing()

    print("\n── Primeras 10 filas de la tabla procesada ──")
    print(df.head(10).to_string(index=False))

    print("\n── Estadísticas de rent_diaria ──")
    if "rent_diaria" in df.columns:
        print(df["rent_diaria"].describe())
    else:
        print("⚠ Columna 'rent_diaria' no encontrada.")