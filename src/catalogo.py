"""
catalogo.py
-----------
Descarga y construye el catálogo de dimensiones de Fondos de Inversión
Colectiva (FICs) disponibles en el portal de Datos Abiertos de Colombia
(API Socrata).

Este módulo debe ejecutarse ANTES de ingestion.py, ya que provee al usuario
el listado completo de fondos disponibles para que pueda seleccionar hasta
5 productos a analizar.

Estrategia de nombres y cobertura:
    No todas las entidades reportan información en la misma fecha; algunas
    pueden estar varios días o semanas atrasadas respecto a las más activas.
    Para garantizar un catálogo completo, se consultan los últimos 30 días
    calendario a partir de la fecha de ejecución.

    Cuando una misma PK aparece en varias fechas dentro de esa ventana,
    se conserva únicamente el registro más reciente (mayor fecha_corte),
    asegurando que los nombres reflejen el reporte más actualizado de
    cada entidad.

Archivos generados en data/raw/dims/:
    - dim_entidad.parquet
    - dim_fondo.parquet
    - dim_participacion.parquet
    - catalogo_ultima_actualizacion.txt   ← fechas del último run
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Constantes globales
# ---------------------------------------------------------------------------

ENDPOINT = "https://www.datos.gov.co/resource/qhpu-8ixx.json"

# Directorio de salida para dimensiones
DIMS_DIR = Path("data/raw/dims")
DIMS_DIR.mkdir(parents=True, exist_ok=True)

# Límite por llamada a la API (Socrata acepta hasta 50 000)
PAGE_SIZE = 50_000

# Días hacia atrás que se consultan para construir el catálogo.
# Se usa una ventana amplia para capturar entidades que no reportan diariamente.
VENTANA_DIAS = 30

# Columnas que forman cada dimensión
_COLS_DIM_ENTIDAD = [
    "tipo_entidad",
    "nombre_tipo_entidad",
    "codigo_entidad",
    "nombre_entidad",
]

_COLS_DIM_FONDO = [
    "tipo_entidad",
    "codigo_entidad",
    "codigo_negocio",
    "nombre_patrimonio",
    "tipo_negocio",
    "nombre_tipo_patrimonio",
    "subtipo_negocio",
    "nombre_subtipo_patrimonio",
]

_COLS_DIM_PARTICIPACION = [
    "tipo_entidad",
    "codigo_entidad",
    "codigo_negocio",
    "tipo_participacion",
]


# ---------------------------------------------------------------------------
# Paso 1 — Calcular la ventana de fechas a consultar
# ---------------------------------------------------------------------------

def get_ventana_fechas() -> tuple[str, str]:
    """
    Calcula el rango de fechas a consultar: desde hace VENTANA_DIAS días
    hasta hoy (fecha de ejecución).

    Retorna
    -------
    Tupla (fecha_desde, fecha_hasta) en formato ISO 'YYYY-MM-DDTHH:MM:SS.000'.
    """
    hoy        = datetime.now().date()
    fecha_desde = hoy - timedelta(days=VENTANA_DIAS)

    # Formato que acepta SoQL para comparaciones de fecha
    fmt = "%Y-%m-%dT00:00:00.000"
    return fecha_desde.strftime(fmt), hoy.strftime(fmt)


# ---------------------------------------------------------------------------
# Paso 2 — Descargar catálogo paginado para la ventana de fechas
# ---------------------------------------------------------------------------

def fetch_catalogo_page(offset: int, fecha_desde: str, fecha_hasta: str) -> list[dict]:
    """
    Descarga una página del catálogo de fondos para el rango de fechas dado.
    Incluye fecha_corte para poder deduplicar por el registro más reciente.
    Trae solo las columnas de dimensión para minimizar el volumen transferido.
    """
    cols_necesarias = set(
        _COLS_DIM_ENTIDAD
        + _COLS_DIM_FONDO
        + _COLS_DIM_PARTICIPACION
    )
    # Incluimos fecha_corte para poder ordenar y quedarnos con el más reciente
    cols_necesarias.add("fecha_corte")

    params = {
        "$select": ", ".join(sorted(cols_necesarias)),
        "$where":  (
            f"fecha_corte >= '{fecha_desde}' "
            f"AND fecha_corte <= '{fecha_hasta}'"
        ),
        "$limit":  PAGE_SIZE,
        "$offset": offset,
        "$order":  "fecha_corte ASC",
    }
    response = requests.get(ENDPOINT, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def fetch_catalogo_completo(fecha_desde: str, fecha_hasta: str) -> pd.DataFrame:
    """
    Descarga todos los registros de dimensión dentro de la ventana de fechas,
    paginando hasta agotar los resultados.

    Parámetros
    ----------
    fecha_desde : str  — inicio de la ventana (formato ISO)
    fecha_hasta : str  — fin de la ventana (formato ISO)

    Retorna
    -------
    pd.DataFrame con todos los registros del período, incluyendo fecha_corte.
    La deduplicación por PK se realiza en un paso posterior.
    """
    all_records: list[dict] = []
    offset = 0

    print(f"Descargando catálogo — ventana: {fecha_desde[:10]} → {fecha_hasta[:10]}")

    while True:
        print(f"  Registros {offset + 1} – {offset + PAGE_SIZE}...")
        records = fetch_catalogo_page(offset, fecha_desde, fecha_hasta)

        if not records:
            print("  Sin más registros. Descarga completa.")
            break

        all_records.extend(records)
        offset += PAGE_SIZE

        if len(records) < PAGE_SIZE:
            break

    print(f"Total de registros descargados (antes de deduplicar): {len(all_records)}")
    return pd.DataFrame(all_records) if all_records else pd.DataFrame()


# ---------------------------------------------------------------------------
# Paso 3 — Limpiar, tipar y deduplicar por el registro más reciente
# ---------------------------------------------------------------------------

def clean_catalogo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y tipa el DataFrame del catálogo:
      - Convierte columnas numéricas.
      - Convierte fecha_corte a datetime.
      - Normaliza strings (strip).
      - Deduplica por PK completa (tipo_entidad, codigo_entidad,
        codigo_negocio, tipo_participacion) conservando el registro
        con mayor fecha_corte — es decir, el más reciente de la ventana.

    Esto garantiza cobertura total aunque distintas entidades reporten
    en fechas distintas dentro de los últimos 30 días.
    """
    if df.empty:
        return df

    df = df.copy()

    # Convertir fecha_corte (necesaria para ordenar y quedarse con el más reciente)
    df["fecha_corte"] = pd.to_datetime(df["fecha_corte"], errors="coerce")

    # Columnas numéricas enteras
    for col in ("tipo_entidad", "codigo_entidad", "codigo_negocio",
                "tipo_negocio", "subtipo_negocio"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Columnas de texto: strip de espacios
    cols_texto = [
        "nombre_tipo_entidad", "nombre_entidad",
        "nombre_patrimonio", "nombre_tipo_patrimonio",
        "nombre_subtipo_patrimonio", "tipo_participacion",
    ]
    for col in cols_texto:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Deduplicar por PK completa conservando el registro más reciente.
    # sort + keep="last" es equivalente a un GROUP BY ... ORDER BY fecha_corte DESC
    # pero sin necesidad de subconsultas.
    pk = ["tipo_entidad", "codigo_entidad", "codigo_negocio", "tipo_participacion"]
    df = (
        df.sort_values("fecha_corte")
          .drop_duplicates(subset=pk, keep="last")
          .reset_index(drop=True)
    )

    print(f"Registros únicos tras deduplicar por PK: {len(df)}")
    return df


# ---------------------------------------------------------------------------
# Paso 4 — Construir cada dimensión a partir del catálogo limpio
# ---------------------------------------------------------------------------

def build_dim_entidad(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye dim_entidad con una fila por combinación única
    (tipo_entidad, codigo_entidad).

    PK compuesta: tipo_entidad + codigo_entidad
    """
    cols = [c for c in _COLS_DIM_ENTIDAD if c in df.columns]
    return (
        df[cols]
        .drop_duplicates(subset=["tipo_entidad", "codigo_entidad"])
        .sort_values(["tipo_entidad", "codigo_entidad"])
        .reset_index(drop=True)
    )


def build_dim_fondo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye dim_fondo con una fila por combinación única
    (tipo_entidad, codigo_entidad, codigo_negocio).

    PK compuesta: tipo_entidad + codigo_entidad + codigo_negocio
    """
    cols = [c for c in _COLS_DIM_FONDO if c in df.columns]
    return (
        df[cols]
        .drop_duplicates(
            subset=["tipo_entidad", "codigo_entidad", "codigo_negocio"]
        )
        .sort_values(["tipo_entidad", "codigo_entidad", "codigo_negocio"])
        .reset_index(drop=True)
    )


def build_dim_participacion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye dim_participacion con una fila por combinación única
    (tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion).

    Esta tabla permite a la app mostrar qué series/clases están disponibles
    para cada fondo antes de que el usuario seleccione qué descargar.

    PK compuesta: tipo_entidad + codigo_entidad + codigo_negocio + tipo_participacion
    """
    cols = [c for c in _COLS_DIM_PARTICIPACION if c in df.columns]
    return (
        df[cols]
        .drop_duplicates()
        .sort_values(
            ["tipo_entidad", "codigo_entidad", "codigo_negocio", "tipo_participacion"]
        )
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Paso 5 — Persistencia
# ---------------------------------------------------------------------------

def save_dims(
    dim_entidad:       pd.DataFrame,
    dim_fondo:         pd.DataFrame,
    dim_participacion: pd.DataFrame,
    fecha_desde:       str,
    fecha_hasta:       str,
) -> None:
    """
    Guarda las tres dimensiones en data/raw/dims/ y registra la ventana
    de fechas consultada y el timestamp de ejecución.

    La sobreescritura es intencional: el catálogo siempre refleja el
    estado más reciente de la API dentro de la ventana consultada.
    """
    dim_entidad.to_parquet(DIMS_DIR / "dim_entidad.parquet",            index=False)
    dim_fondo.to_parquet(DIMS_DIR / "dim_fondo.parquet",                index=False)
    dim_participacion.to_parquet(DIMS_DIR / "dim_participacion.parquet", index=False)

    # Registro de la última actualización
    (DIMS_DIR / "catalogo_ultima_actualizacion.txt").write_text(
        f"ventana_desde:      {fecha_desde[:10]}\n"
        f"ventana_hasta:      {fecha_hasta[:10]}\n"
        f"timestamp_descarga: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )

    print(f"\nDimensiones guardadas en {DIMS_DIR}/")
    print(f"  dim_entidad.parquet        → {len(dim_entidad):>6} filas")
    print(f"  dim_fondo.parquet          → {len(dim_fondo):>6} filas")
    print(f"  dim_participacion.parquet  → {len(dim_participacion):>6} filas")


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def run_catalogo() -> dict[str, pd.DataFrame]:
    """
    Pipeline completo de descarga y construcción del catálogo de dimensiones.

    Flujo:
        1. Calcula la ventana de fechas: últimos VENTANA_DIAS días.
        2. Descarga todos los registros de dimensión dentro de esa ventana.
        3. Limpia, tipa y deduplica por PK conservando el registro más reciente
           de cada combinación — cubre entidades que no reportan diariamente.
        4. Construye dim_entidad, dim_fondo y dim_participacion.
        5. Guarda los tres parquets en data/raw/dims/.

    Retorna
    -------
    dict con las tres dimensiones como DataFrames:
        {
            "dim_entidad":       pd.DataFrame,
            "dim_fondo":         pd.DataFrame,
            "dim_participacion": pd.DataFrame,
        }

    Ejemplo de uso desde la app Shiny
    ----------------------------------
    from catalogo import run_catalogo

    dims = run_catalogo()
    # Mostrar al usuario los fondos disponibles para seleccionar:
    print(dims["dim_fondo"][["codigo_negocio", "nombre_patrimonio"]])
    """
    print("=" * 60)
    print("ACTUALIZACIÓN DE CATÁLOGO DE FONDOS")
    print("=" * 60)

    # 1. Calcular ventana de fechas
    fecha_desde, fecha_hasta = get_ventana_fechas()
    print(f"Ventana consultada: últimos {VENTANA_DIAS} días "
          f"({fecha_desde[:10]} → {fecha_hasta[:10]})")

    # 2. Descargar catálogo completo dentro de la ventana
    df_raw = fetch_catalogo_completo(fecha_desde, fecha_hasta)

    if df_raw.empty:
        raise RuntimeError(
            "La API no devolvió registros para la ventana consultada. "
            f"({fecha_desde[:10]} → {fecha_hasta[:10]})"
        )

    # 3. Limpiar, tipar y deduplicar (queda el registro más reciente por PK)
    df_clean = clean_catalogo(df_raw)

    # 4. Construir dimensiones
    dim_entidad       = build_dim_entidad(df_clean)
    dim_fondo         = build_dim_fondo(df_clean)
    dim_participacion = build_dim_participacion(df_clean)

    # 5. Guardar
    save_dims(dim_entidad, dim_fondo, dim_participacion, fecha_desde, fecha_hasta)

    print("\n✓ Catálogo actualizado correctamente.")
    print("  El usuario puede ahora seleccionar fondos para analizar.")
    print("=" * 60)

    return {
        "dim_entidad":       dim_entidad,
        "dim_fondo":         dim_fondo,
        "dim_participacion": dim_participacion,
    }


# ---------------------------------------------------------------------------
# Helpers para la app Shiny — lectura de dimensiones ya guardadas
# ---------------------------------------------------------------------------

def load_dims() -> dict[str, pd.DataFrame]:
    """
    Carga las dimensiones desde los parquets guardados en data/raw/dims/.
    Útil para que la app Shiny lea el catálogo sin necesidad de volver
    a llamar a la API.

    Lanza FileNotFoundError si el catálogo no ha sido descargado todavía.
    """
    archivos = {
        "dim_entidad":       DIMS_DIR / "dim_entidad.parquet",
        "dim_fondo":         DIMS_DIR / "dim_fondo.parquet",
        "dim_participacion": DIMS_DIR / "dim_participacion.parquet",
    }

    for nombre, ruta in archivos.items():
        if not ruta.exists():
            raise FileNotFoundError(
                f"No se encontró {ruta}. "
                "Ejecute run_catalogo() primero para descargar el catálogo."
            )

    return {nombre: pd.read_parquet(ruta) for nombre, ruta in archivos.items()}


def catalogo_disponible() -> bool:
    """
    Retorna True si el catálogo ya fue descargado y está listo para usar.
    Útil para que la app Shiny decida si mostrar el selector de fondos
    o pedir al usuario que actualice primero el catálogo.
    """
    return all(
        (DIMS_DIR / f).exists()
        for f in ("dim_entidad.parquet", "dim_fondo.parquet", "dim_participacion.parquet")
    )


# ---------------------------------------------------------------------------
# Entry point para pruebas locales
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    dims = run_catalogo()

    print("\nMuestra dim_entidad:")
    print(dims["dim_entidad"].to_string(index=False))

    print("\nMuestra dim_fondo (primeras 10 filas):")
    print(dims["dim_fondo"].head(10).to_string(index=False))

    print("\nMuestra dim_participacion (primeras 10 filas):")
    print(dims["dim_participacion"].head(10).to_string(index=False))