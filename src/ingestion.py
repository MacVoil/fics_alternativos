"""
ingestion.py
------------
Descarga la tabla de hechos de Fondos de Inversión Colectiva (FICs)
alternativos desde el portal de Datos Abiertos de Colombia (API Socrata)
y la almacena en formato Parquet.

Solo descarga columnas de hechos y claves foráneas (FK). Las columnas
descriptivas (nombres de entidad, fondo, etc.) se gestionan en catalogo.py.
Las rentabilidades (diaria, mensual, semestral, anual) NO se descargan
porque se calcularán desde cero en processing.py a partir del
valor_unidad_operaciones, garantizando control total del cálculo.

Los fondos a descargar NO están hardcodeados: se reciben como parámetro
en `run_ingestion()`, lo que permite que la app Shiny los configure
dinámicamente (hasta 5 fondos).

Cada ejecución sobreescribe completamente los archivos existentes con la
totalidad de registros históricos de los fondos seleccionados.

Estructura esperada de cada fondo (dict):
    {
        "tipo_entidad":      int,  # ej. 5
        "codigo_entidad":    int,  # ej. 16
        "codigo_negocio":    int,  # ej. 10824
        "tipo_participacion": str, # ej. "501"
    }
"""

import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    # Cuando se ejecuta desde src/ (python ingestion.py)
    from vacuum import run_vacuum
except ModuleNotFoundError:
    # Cuando se ejecuta desde la raiz del proyecto o notebooks (import src.ingestion)
    from src.vacuum import run_vacuum

# ---------------------------------------------------------------------------
# Constantes globales
# ---------------------------------------------------------------------------

ENDPOINT = "https://www.datos.gov.co/resource/qhpu-8ixx.json"

# Columnas de la tabla de hechos:
#   - FK: claves que identifican el fondo y la participación
#   - Métricas: valores que cambian diariamente
# Se excluyen columnas descriptivas (nombres) → están en catalogo.py
# Se excluyen rentabilidades calculadas por la API → se recalcularán en processing.py
COLUMNAS = [
    # — Clave temporal —
    "fecha_corte",
    # — FK hacia dimensiones —
    "tipo_entidad",
    "codigo_entidad",
    "codigo_negocio",
    "tipo_participacion",
    "principal_compartimento",
    # — Métricas de valor y volumen —
    "valor_unidad_operaciones",
    "numero_unidades_fondo_cierre",
    "valor_fondo_cierre_dia_t",
    "precierre_fondo_dia_t",
    "numero_inversionistas",
    # — Métricas de flujo —
    "rendimientos_abonados",
    "aportes_recibidos",
    "retiros_redenciones",
    "anulaciones",
]

# Directorio de salida
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Límite por llamada a la API (Socrata acepta hasta 50 000)
PAGE_SIZE = 50_000

# Campos obligatorios en cada dict de fondo
_CAMPOS_REQUERIDOS = {"tipo_entidad", "codigo_entidad", "codigo_negocio", "tipo_participacion"}


# ---------------------------------------------------------------------------
# Validación
# ---------------------------------------------------------------------------

def validate_fondos(fondos: list[dict[str, Any]]) -> None:
    """
    Valida que la lista de fondos cumpla las reglas de negocio:
      - Entre 1 y 5 elementos.
      - Cada elemento contiene los campos requeridos.
      - Los valores numéricos son enteros positivos.
      - tipo_participacion es un string no vacío.
    Lanza ValueError con un mensaje descriptivo si algo falla.
    """
    if not fondos:
        raise ValueError("Debe seleccionar al menos 1 fondo.")
    if len(fondos) > 5:
        raise ValueError(f"Se pueden seleccionar hasta 5 fondos; se recibieron {len(fondos)}.")

    for i, fondo in enumerate(fondos, start=1):
        faltantes = _CAMPOS_REQUERIDOS - fondo.keys()
        if faltantes:
            raise ValueError(
                f"Fondo #{i}: faltan los campos {faltantes}. "
                f"Se requieren: {_CAMPOS_REQUERIDOS}."
            )

        for campo_num in ("tipo_entidad", "codigo_entidad", "codigo_negocio"):
            val = fondo[campo_num]
            if not isinstance(val, int) or val <= 0:
                raise ValueError(
                    f"Fondo #{i}: '{campo_num}' debe ser un entero positivo (recibido: {val!r})."
                )

        tp = fondo["tipo_participacion"]
        if not isinstance(tp, str) or not tp.strip():
            raise ValueError(
                f"Fondo #{i}: 'tipo_participacion' debe ser un string no vacío "
                f"(recibido: {tp!r})."
            )


# ---------------------------------------------------------------------------
# Construcción de la consulta SoQL
# ---------------------------------------------------------------------------

def build_where_clause(fondos: list[dict[str, Any]]) -> str:
    """
    Construye la cláusula WHERE en SoQL para filtrar exactamente los fondos
    recibidos, cruzando tipo_entidad, codigo_entidad, codigo_negocio y
    tipo_participacion con OR entre cada combinación.

    Ejemplo de salida:
        (tipo_entidad=5 AND codigo_entidad=16 AND codigo_negocio=10824
         AND tipo_participacion='A')
        OR
        (tipo_entidad=5 AND codigo_entidad=58 AND codigo_negocio=53962
         AND tipo_participacion='B')
    """
    conditions = []
    for f in fondos:
        tp = f["tipo_participacion"].strip().replace("'", "''")  # escape comilla simple
        conditions.append(
            f"(tipo_entidad={f['tipo_entidad']}"
            f" AND codigo_entidad={f['codigo_entidad']}"
            f" AND codigo_negocio={f['codigo_negocio']}"
            f" AND tipo_participacion='{tp}')"
        )
    return " OR ".join(conditions)


# ---------------------------------------------------------------------------
# Descarga paginada
# ---------------------------------------------------------------------------

def fetch_page(offset: int, where: str) -> list[dict]:
    """Descarga una página de resultados desde la API Socrata."""
    params = {
        "$select": ", ".join(COLUMNAS),
        "$where":  where,
        "$limit":  PAGE_SIZE,
        "$offset": offset,
        "$order":  "fecha_corte ASC",
    }
    response = requests.get(ENDPOINT, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def fetch_all(fondos: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Descarga la totalidad de registros históricos de los fondos indicados,
    paginando la API hasta agotar los resultados.

    Siempre descarga desde el inicio (offset=0), garantizando que el
    resultado represente el histórico completo, no un delta.
    """
    where = build_where_clause(fondos)
    all_records: list[dict] = []
    offset = 0

    print("Iniciando descarga desde la API de Datos Abiertos de Colombia...")
    print(f"Fondos seleccionados: {len(fondos)}")

    while True:
        print(f"  Descargando registros {offset + 1} – {offset + PAGE_SIZE}...")
        records = fetch_page(offset, where)

        if not records:
            print("  Sin más registros. Descarga completa.")
            break

        all_records.extend(records)
        offset += PAGE_SIZE

        # Si la página devuelta es menor que PAGE_SIZE, llegamos al final
        if len(records) < PAGE_SIZE:
            break

    print(f"Total de registros descargados: {len(all_records)}")
    return pd.DataFrame(all_records) if all_records else pd.DataFrame()


# ---------------------------------------------------------------------------
# Limpieza y tipado
# ---------------------------------------------------------------------------

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Selecciona columnas de hechos, convierte tipos y limpia el DataFrame.
    Preserva tipo_participacion como columna de texto.
    """
    if df.empty:
        return df

    # Conservar solo las columnas presentes en la respuesta de la API
    cols_presentes = [c for c in COLUMNAS if c in df.columns]
    df = df[cols_presentes].copy()

    # Convertir fecha
    df["fecha_corte"] = pd.to_datetime(df["fecha_corte"], errors="coerce")

    # Convertir columnas numéricas
    # Nota: rentabilidades NO se incluyen — se calcularán en processing.py
    numericas = [
        "tipo_entidad", "codigo_entidad", "codigo_negocio",
        "principal_compartimento",
        "valor_unidad_operaciones", "numero_unidades_fondo_cierre",
        "valor_fondo_cierre_dia_t", "precierre_fondo_dia_t",
        "numero_inversionistas",
        "rendimientos_abonados", "aportes_recibidos",
        "retiros_redenciones", "anulaciones",
    ]
    for col in numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # tipo_participacion se mantiene como string; limpiar espacios
    if "tipo_participacion" in df.columns:
        df["tipo_participacion"] = df["tipo_participacion"].astype(str).str.strip()

    # Eliminar duplicados exactos
    df = df.drop_duplicates()

    # Ordenar por PK natural de la tabla de hechos
    sort_cols = [
        "tipo_entidad", "codigo_entidad", "codigo_negocio",
        "tipo_participacion", "fecha_corte",
    ]
    sort_cols = [c for c in sort_cols if c in df.columns]
    df = df.sort_values(sort_cols).reset_index(drop=True)

    return df


# ---------------------------------------------------------------------------
# Persistencia — sobreescritura total
# ---------------------------------------------------------------------------

def save_parquet(df: pd.DataFrame) -> Path:
    """
    Guarda el DataFrame sobreescribiendo completamente los archivos Parquet:
      - fics_alternativos_latest.parquet  → siempre apunta al último run
      - fics_alternativos_<timestamp>.parquet → trazabilidad por ejecución

    La sobreescritura total es intencional: cada run reemplaza el histórico
    completo con los fondos actualmente seleccionados en la app.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_ts     = RAW_DIR / f"fics_alternativos_{timestamp}.parquet"
    archivo_latest = RAW_DIR / "fics_alternativos_latest.parquet"

    df.to_parquet(archivo_ts,     index=False)
    df.to_parquet(archivo_latest, index=False)

    print(f"Datos guardados en:          {archivo_ts}")
    print(f"Archivo latest actualizado:  {archivo_latest}")
    return archivo_latest


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def run_ingestion(fondos: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Pipeline completo de ingesta para los fondos indicados por la app.

    Parámetros
    ----------
    fondos : list[dict]
        Lista de 1 a 5 dicts, cada uno con:
            - tipo_entidad      (int)
            - codigo_entidad    (int)
            - codigo_negocio    (int)
            - tipo_participacion (str)

    Retorna
    -------
    pd.DataFrame con los datos limpios descargados y guardados.

    Ejemplo de uso desde la app Shiny
    ----------------------------------
    from ingestion import run_ingestion

    fondos_seleccionados = [
        {"tipo_entidad": 5, "codigo_entidad": 16,
         "codigo_negocio": 10824, "tipo_participacion": "A"},
        {"tipo_entidad": 5, "codigo_entidad": 58,
         "codigo_negocio": 53962, "tipo_participacion": "B"},
    ]
    df = run_ingestion(fondos_seleccionados)
    """
    # 1. Validar entrada
    validate_fondos(fondos)

    # 2. Descargar histórico completo (siempre desde offset 0)
    df_raw = fetch_all(fondos)

    if df_raw.empty:
        print("⚠ La API no devolvió registros para los fondos seleccionados.")
        return df_raw

    # 3. Limpiar y tipar
    df_clean = clean_dataframe(df_raw)

    # 4. Sobreescribir parquet (total, no incremental)
    save_parquet(df_clean)

    # 4b. Limpiar históricos antiguos (automático)
    print("\n🧹 Ejecutando limpieza de históricos...")
    resultado_vacuum = run_vacuum(
        days_retention=7,
        dry_run=False,
        verbose=False  # ← Sin prints para no contaminar output
    )
    print(f"   {resultado_vacuum['mensaje_ui']}")

    # 5. Resumen por fondo / participación
    print("\nResumen de la descarga:")
    group_cols = [
        "tipo_entidad", "codigo_entidad",
        "codigo_negocio", "tipo_participacion",
    ]
    group_cols = [c for c in group_cols if c in df_clean.columns]
    resumen = (
        df_clean.groupby(group_cols)
        .agg(
            registros    = ("fecha_corte", "count"),
            fecha_inicio = ("fecha_corte", "min"),
            fecha_fin    = ("fecha_corte", "max"),
        )
        .reset_index()
    )
    print(resumen.to_string(index=False))

    return df_clean


# ---------------------------------------------------------------------------
# Entry point para pruebas locales (no lo usa la app)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Ejemplo de prueba local — ajustar según los fondos que se quieran verificar
    fondos_prueba = [
        {"tipo_entidad": 5, "codigo_entidad": 16,
         "codigo_negocio": 10824,  "tipo_participacion": "501"},
        {"tipo_entidad": 5, "codigo_entidad": 16,
         "codigo_negocio": 120541, "tipo_participacion": "501"},
        {"tipo_entidad": 5, "codigo_entidad": 16,
         "codigo_negocio": 97466,  "tipo_participacion": "501"},
        {"tipo_entidad": 5, "codigo_entidad": 58,
         "codigo_negocio": 53962,  "tipo_participacion": "800"},
    ]
    run_ingestion(fondos_prueba)