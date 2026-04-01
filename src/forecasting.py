"""
forecasting.py
--------------
Entrenamiento y generación de proyecciones de rentabilidades diarias con AutoGluon TimeSeries.

Flujo:
    1. Lee data/processed/fics_rentabilidades_latest.parquet (generado por processing.py)
    2. Calcula rentabilidades de 30, 60, 90, 180 y 360 días usando medias geométricas (log-returns)
    3. Filtra registros con NaN en rent_360d (típicamente primeros 359 de cada producto)
    4. Transforma a formato largo: cada rentabilidad en columna separada
    5. Por cada combinación (producto + periodo_rentabilidad):
       - Calcula rango de fechas (min, max) y número de días disponibles
       - Agrupa productos con fechas similares (para entrenar en lotes coherentes)
    6. Para cada grupo de productos con fechas similares:
       - Valida mínimo 90 días de datos
       - Si 90-730 días: genera mensaje de alerta sobre horizonte reducido
       - Si ≥731 días: entrena con horizonte de 365 días
       - Entrena AutoGluon con Chronos2 (zero-shot y small fine-tuned)
       - Guarda predicciones en data/forecasts/

Validaciones de datos:
    - Mínimo 90 días por grupo: si no, se reporta y se omite
    - Entre 90-730 días: se entrena pero con horizonte reducido
    - ≥731 días: se entrena con horizonte completo de 365 días

Estructura de salida:
    data/forecasts/
    ├── fics_pronósticos_latest.parquet         — predicciones más recientes
    ├── fics_pronósticos_<timestamp>.parquet    — trazabilidad histórica
    └── forecast_log_<timestamp>.txt            — resumen de entrenamientos

Columnas de salida en predicciones:
    - id_producto: identificador del producto + período de rentabilidad
    - fecha_corte: fecha del último dato histórico
    - fecha_forecast: fecha de la proyección
    - rentabilidad_p50: mediana de la proyección (cuantil 0.5)
    - rentabilidad_p20: cuantil 0.2 (escenario pesimista)
    - rentabilidad_p80: cuantil 0.8 (escenario optimista)
    - dias_forecast: número de días proyectados
    - dias_disponibles_entrenamiento: días históricos usados
    - mensaje_validacion: estado de alerta (p.ej. "Datos limitados a 200 días")
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Tuple, Dict, List, Any
import logging

try:
    # Cuando se ejecuta desde src/
    from vacuum import run_vacuum
except ModuleNotFoundError:
    # Cuando se ejecuta desde la raiz del proyecto o notebooks
    from src.vacuum import run_vacuum

try:
    from autogluon.timeseries import TimeSeriesDataFrame, TimeSeriesPredictor
except ImportError:
    raise ImportError(
        "AutoGluon TimeSeries no está instalado. "
        "Instálalo con: pip install autogluon[timeseries]"
    )

# ---------------------------------------------------------------------------
# Configuración y constantes
# ---------------------------------------------------------------------------

PROCESSED_DIR = Path("data/processed")
FORECASTS_DIR = Path("data/forecasts")
FORECASTS_DIR.mkdir(parents=True, exist_ok=True)

ARCHIVO_ENTRADA = PROCESSED_DIR / "fics_rentabilidades_latest.parquet"
ARCHIVO_SALIDA_LATEST = FORECASTS_DIR / "fics_pronósticos_latest.parquet"
ARCHIVO_OBSERVADOS_LATEST = FORECASTS_DIR / "fics_observados_latest.parquet"

# Períodos de rentabilidad a calcular (en días)
PERIODOS_RENTABILIDAD = [30, 60, 90, 180, 360]

# Configuración de validación de datos
MIN_DIAS_FORECAST = 90  # Mínimo de días para hacer forecast
DIAS_MIN_PARA_365 = 731  # Mínimo para hacer forecast a 365 días
DIAS_MAX_SIN_365 = 730  # Máximo sin poder hacer 365 días completos

# Configuración de AutoGluon
AUTOGLUON_PATH = "autogluon_models/"
AUTOGLUON_TIME_LIMIT = 1800  # 30 minutos por grupo
AUTOGLUON_QUANTILES = [0.2, 0.5, 0.8]  # Percentiles: pesimista, mediana, optimista
AUTOGLUON_VAL_WINDOWS = 10  # Número de ventanas de validación
AUTOGLUON_SEED = 42

# Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Paso 1 — Carga y preparación inicial
# ---------------------------------------------------------------------------

def load_processed_data() -> pd.DataFrame:
    """
    Carga el parquet de rentabilidades generado por processing.py.
    Lanza FileNotFoundError si no existe.
    
    Retorna
    -------
    pd.DataFrame con columnas: fecha_corte, tipo_entidad, codigo_entidad,
    codigo_negocio, tipo_participacion, rent_diaria (y otras métricas).
    """
    if not ARCHIVO_ENTRADA.exists():
        raise FileNotFoundError(
            f"No se encontró {ARCHIVO_ENTRADA}. "
            "Ejecute run_processing() primero para generar rentabilidades."
        )

    df = pd.read_parquet(ARCHIVO_ENTRADA)
    print(f"Registros cargados:  {len(df):>10,}")
    print(f"Columnas de entrada: {len(df.columns)}")
    
    return df


def _create_product_id(row: pd.Series) -> str:
    """Crea ID único del producto concatenando FK."""
    return (
        f"{int(row['tipo_entidad'])}_"
        f"{int(row['codigo_entidad'])}_"
        f"{int(row['codigo_negocio'])}_"
        f"{row['tipo_participacion']}"
    )


def _decompose_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Descompone la columna 'id' (formato: tipo_entidad_codigo_entidad_codigo_negocio_
    tipo_participacion_rent_Xd) de vuelta a las FK originales más tipo_rentabilidad.

    El id tiene la forma: {tipo_entidad}_{codigo_entidad}_{codigo_negocio}_{tipo_participacion}_rent_{X}d
    p.ej.: "5_16_10824_501_rent_30d"

    Separa por '_rent_' desde la derecha para aislar tipo_rentabilidad, luego
    divide la parte izquierda en exactamente 4 campos (maxsplit=3).
    """
    # Separar tipo_rentabilidad del resto
    partes = df['id'].str.rsplit('_rent_', n=1, expand=True)
    id_base = partes[0]             # p.ej. "5_16_10824_501"
    df['tipo_rentabilidad'] = 'rent_' + partes[1]   # p.ej. "rent_30d"

    # Descomponer id_base en sus 4 componentes
    componentes = id_base.str.split('_', n=3, expand=True)
    df['tipo_entidad']      = componentes[0].astype(int)
    df['codigo_entidad']    = componentes[1].astype(int)
    df['codigo_negocio']    = componentes[2].astype(int)
    df['tipo_participacion'] = componentes[3]

    df = df.drop(columns=['id'])
    return df


def prepare_base_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara el dataset base: selecciona columnas necesarias, crea ID de producto
    y ordena por producto y fecha.
    
    Retorna
    -------
    pd.DataFrame con columnas: fecha_corte, id, rent_diaria (ordenado).
    """
    # Seleccionar solo columnas necesarias
    cols_necesarias = ["fecha_corte", "tipo_entidad", "codigo_entidad", 
                       "codigo_negocio", "tipo_participacion", "rent_diaria"]
    
    missing = [c for c in cols_necesarias if c not in df.columns]
    if missing:
        raise ValueError(f"Columnas faltantes en entrada: {missing}")
    
    df_prep = df[cols_necesarias].copy()
    
    # Crear ID de producto
    df_prep["id"] = df_prep.apply(_create_product_id, axis=1)
    
    # Descartar FK individuales (ya están en ID)
    df_prep = df_prep.drop(
        ["tipo_entidad", "codigo_entidad", "codigo_negocio", "tipo_participacion"],
        axis=1
    )
    
    # Ordenar por producto y fecha
    df_prep = df_prep.sort_values(by=["id", "fecha_corte"]).reset_index(drop=True)
    
    print(f"Productos únicos:    {df_prep['id'].nunique():>10,}")
    
    return df_prep


# ---------------------------------------------------------------------------
# Paso 2 — Cálculo de rentabilidades por período
# ---------------------------------------------------------------------------

def _calculate_rental_return(rent_series: pd.Series, window: int) -> pd.Series:
    """
    Calcula la rentabilidad anualizada para un período usando medias geométricas (log-returns).
    
    Fórmula (con estabilidad numérica):
        log_r = sum(ln(rent_diaria)) sobre la ventana
        rent_período = exp(log_r * (365/window)) - 1
    
    Parámetros
    ----------
    rent_series : pd.Series
        Serie de rentabilidades diarias (rent_diaria) ya ordenada por fecha
    window : int
        Número de días para la ventana rolling
    
    Retorna
    -------
    pd.Series con las rentabilidades calculadas (NaN en primeros window-1 registros)
    """
    log_r = np.log(rent_series.values)
    rolling_sum = pd.Series(log_r).rolling(window=window, min_periods=window).sum().values
    
    # Anualizar: multiplicar suma de logs por (365/window), luego exponenciar
    annualized = np.where(
        pd.Series(rolling_sum).notna(),
        np.exp(rolling_sum * (365 / window)) - 1,
        np.nan
    )
    
    return pd.Series(annualized, index=rent_series.index)


def calculate_multiple_rentals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula rentabilidades de 30, 60, 90, 180 y 360 días usando rolling windows
    con geometría (log-returns) para máxima estabilidad numérica.
    
    Retorna
    -------
    pd.DataFrame con columnas adicionales: rent_30d, rent_60d, rent_90d, rent_180d, rent_360d
    """
    df = df.copy()
    
    print(f"\nCalculando rentabilidades por período...")
    
    for periodo in PERIODOS_RENTABILIDAD:
        col_name = f"rent_{periodo}d"
        print(f"  Calculando {col_name}...", end=" ")
        
        df[col_name] = df.groupby("id", sort=False)['rent_diaria'].transform(
            lambda g: _calculate_rental_return(g, periodo)
        )
        
        valid_count = df[col_name].notna().sum()
        print(f"✓ ({valid_count:,} registros válidos)")
    
    return df


# ---------------------------------------------------------------------------
# Paso 3 — Filtrado y transformación
# ---------------------------------------------------------------------------

def filter_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Filtra registros con NaN en rent_360d (primeros 359 de cada producto)
    2. Descarta rent_diaria (ya no necesaria)
    3. Transforma a formato largo: cada rentabilidad en una fila
    4. Actualiza ID para incluir período
    
    Retorna
    -------
    pd.DataFrame largo con columnas: id, fecha_corte, rentabilidad
    (id incluye período identificador, p.ej. "1_2_3_501_rent_30d")
    """
    antes = len(df)
    
    # Filtrar por rent_360d
    df = df.dropna(subset=['rent_360d']).reset_index(drop=True)
    
    eliminados = antes - len(df)
    print(f"\nFiltro rent_360d: {eliminados:,} registros con NaN eliminados.")
    
    # Descartar rent_diaria
    df = df.drop(['rent_diaria'], axis=1)
    
    # Unpivot: cada rentabilidad en una fila
    cols_rent = [f"rent_{p}d" for p in PERIODOS_RENTABILIDAD]
    df_long = df.melt(
        id_vars=['id', 'fecha_corte'],
        value_vars=cols_rent,
        var_name='periodo',
        value_name='rentabilidad'
    )
    
    # Actualizar ID para incluir período
    df_long['id'] = df_long['id'] + '_' + df_long['periodo']
    df_long = df_long.drop(['periodo'], axis=1)
    
    print(f"Formato largo: {len(df_long):,} registros (combinación producto+período)")
    
    return df_long


# ---------------------------------------------------------------------------
# Paso 4 — Análisis de rangos de fechas y agrupación
# ---------------------------------------------------------------------------

def analyze_date_ranges(df: pd.DataFrame) -> pd.DataFrame:
    """
    Por cada ID (producto + período):
    - Calcula min/max de fechas y número de días disponibles
    - Agrupa productos con fechas similares (mismo min, mismo max)
    
    Retorna
    -------
    pd.DataFrame con columnas: id, min_fecha, max_fecha, n_dias, grupo_fechas
    """
    resumen = df.groupby('id')['fecha_corte'].agg(
        min_fecha='min',
        max_fecha='max'
    ).reset_index()
    
    resumen['n_dias'] = (resumen['max_fecha'] - resumen['min_fecha']).dt.days
    
    # Agrupar por fechas idénticas (mismo min/max → mismo grupo de entrenamiento)
    resumen['grupo_fechas'] = resumen.groupby(['min_fecha', 'max_fecha']).ngroup()
    
    print(f"\nAnálisis de fechas:")
    print(f"  Productos únicos:     {len(resumen):,}")
    print(f"  Grupos de fechas:     {resumen['grupo_fechas'].nunique():,}")
    print(f"  Rango de días:        {resumen['n_dias'].min():.0f} a {resumen['n_dias'].max():.0f}")
    
    return resumen


def format_observed_output(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Formatea los datos observados usados para entrenamiento con el esquema requerido.

    Columnas de salida:
        tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion,
        tipo_rentabilidad, fecha_corte, rentabilidad
    """
    out = df_long.copy()
    out = _decompose_id(out)
    cols = [
        'tipo_entidad',
        'codigo_entidad',
        'codigo_negocio',
        'tipo_participacion',
        'tipo_rentabilidad',
        'fecha_corte',
        'rentabilidad',
    ]
    return out[cols]


# ---------------------------------------------------------------------------
# Paso 5 — Validación por grupo
# ---------------------------------------------------------------------------

def validate_group(
    grupo_num: int,
    grupo_ids: List[str],
    resumen: pd.DataFrame
) -> Dict[str, Any]:
    """
    Valida un grupo de productos con fechas similares.
    
    Retorna
    -------
    dict con claves:
        - 'valido': bool — es válido para entrenamiento
        - 'n_dias': int — días disponibles
        - 'dias_forecast': int — días a proyectar
        - 'mensaje': str — mensaje para el usuario
    """
    grupo_info = resumen.loc[resumen['id'].isin(grupo_ids)].iloc[0]
    n_dias = grupo_info['n_dias']
    
    resultado = {
        'valido': False,
        'n_dias': n_dias,
        'dias_forecast': 0,
        'mensaje': ""
    }
    
    if n_dias < MIN_DIAS_FORECAST:
        resultado['mensaje'] = (
            f"Grupo {grupo_num}: Insuficientes datos ({n_dias} días). "
            f"Se requieren mínimo {MIN_DIAS_FORECAST} días. Omitido."
        )
        return resultado
    
    if n_dias >= DIAS_MIN_PARA_365:
        # Forecast de 365 días
        resultado['valido'] = True
        resultado['dias_forecast'] = 365
        resultado['mensaje'] = f"Grupo {grupo_num}: {n_dias} días → Forecast de 365 días"
    else:
        # Forecast adaptativo: mitad de los datos disponibles (máximo DIAS_MAX_SIN_365)
        dias_forecast = min((n_dias - 1) // 2, DIAS_MAX_SIN_365)
        resultado['valido'] = True
        resultado['dias_forecast'] = dias_forecast
        resultado['mensaje'] = (
            f"Grupo {grupo_num}: {n_dias} días → Forecast de {dias_forecast} días "
            f"(datos limitados; no se alcanza 365 días)"
        )
    
    return resultado


# ---------------------------------------------------------------------------
# Paso 6 — Entrenamiento y predicción
# ---------------------------------------------------------------------------

def train_and_predict(
    datos: pd.DataFrame,
    grupo_num: int,
    dias_forecast: int,
    resumen_entrenamiento: List[str]
) -> pd.DataFrame:
    """
    Entrena un modelo AutoGluon con los datos del grupo y genera predicciones.
    
    Parámetros
    ----------
    datos : pd.DataFrame
        Datos del grupo en formato largo (id, fecha_corte, rentabilidad)
    grupo_num : int
        Número del grupo (para logging)
    dias_forecast : int
        Número de días a proyectar
    resumen_entrenamiento : list
        Lista para acumular mensajes de entrenamiento
    
    Retorna
    -------
    pd.DataFrame con predicciones: fecha_pronóstico, id_producto, cuantiles, etc.
    o None si falla
    """
    try:
        print(f"  Grupo {grupo_num}: Preparando datos para TimeSeriesDataFrame...")
        
        # Preparar datos: renombrar fecha_corte a timestamp
        datos_preparados = datos.rename(columns={'fecha_corte': 'timestamp'}).copy()
        
        # Convertir a TimeSeriesDataFrame
        data_ts = TimeSeriesDataFrame.from_data_frame(
            datos_preparados,
            id_column='id',
            timestamp_column='timestamp'
        )
        
        print(f"  Grupo {grupo_num}: Creando predictor (horizonte={dias_forecast} días)...")
        
        # Crear predictor
        predictor = TimeSeriesPredictor(
            target='rentabilidad',
            prediction_length=dias_forecast,
            freq='D',
            eval_metric='WQL',
            quantile_levels=AUTOGLUON_QUANTILES,
            path=AUTOGLUON_PATH
        )
        
        print(f"  Grupo {grupo_num}: Entrenando modelos...")
        
        # Entrenar
        predictor.fit(
            data_ts,
            hyperparameters={
                "Chronos2": [
                    {
                        "ag_args": {"name_suffix": "ZeroShot"},
                        "model_path": "autogluon/chronos-2"
                    },
                    {
                        "ag_args": {"name_suffix": "ZeroShot-small"},
                        "model_path": "autogluon/chronos-2-small",
                    }
                ]
            },
            refit_full=False,
            num_val_windows=AUTOGLUON_VAL_WINDOWS,
            time_limit=AUTOGLUON_TIME_LIMIT,
            random_seed=AUTOGLUON_SEED,
            verbosity=2,
            refit_every_n_windows=1
        )
        
        print(f"  Grupo {grupo_num}: Generando predicciones...")
        
        # Predicciones — retorna un DataFrame con índice MultiIndex (item_id, timestamp)
        predictions = predictor.predict(data_ts)
        
        # Reset index para convertir índice en columnas
        predictions_reset = predictions.reset_index()
        
        # AutoGluon usa 'item_id' para el ID de la serie (no 'id')
        # Renombrar para consistencia
        if 'item_id' in predictions_reset.columns:
            predictions_reset = predictions_reset.rename(columns={'item_id': 'id'})
        
        # Las columnas de cuantiles tendrán nombres como 0.2, 0.5, 0.8
        # Necesito pivotar si hay múltiples cuantiles por timestamp
        # En este caso, los cuantiles ya son columnas separadas
        
        # Asegurar que tenemos la columna id
        if 'id' not in predictions_reset.columns:
            msg = f"Grupo {grupo_num}: ✗ Estructura de predicciones inesperada"
            print(f"  {msg}")
            resumen_entrenamiento.append(msg)
            return None
        
        # Renombrar timestamp a fecha_corte para consistencia
        if 'timestamp' in predictions_reset.columns:
            predictions_reset = predictions_reset.rename(columns={'timestamp': 'fecha_corte'})
        
        # Renombrar columnas de cuantiles a formato pedido (p0.2, p0.5, p0.8)
        # AutoGluon puede nombrarlas como strings "0.2" o como floats 0.2
        predictions_reset = predictions_reset.rename(columns={
            '0.2': 'p0.2', 0.2: 'p0.2',
            '0.5': 'p0.5', 0.5: 'p0.5',
            '0.8': 'p0.8', 0.8: 'p0.8',
        })
        
        # Descomponer id en FK originales + tipo_rentabilidad
        predictions_reset = _decompose_id(predictions_reset)
        
        # Ordenar columnas según especificación
        cols_orden = [
            'tipo_entidad', 'codigo_entidad', 'codigo_negocio', 'tipo_participacion',
            'tipo_rentabilidad', 'fecha_corte', 'mean', 'p0.2', 'p0.5', 'p0.8'
        ]
        # Solo incluir las que existen (mean puede no estar en algunas versiones)
        cols_orden = [c for c in cols_orden if c in predictions_reset.columns]
        predictions_reset = predictions_reset[cols_orden]
        
        msg = f"Grupo {grupo_num}: ✓ {len(predictions_reset):,} predicciones generadas"
        print(f"  {msg}")
        resumen_entrenamiento.append(msg)
        
        return predictions_reset
        
    except Exception as e:
        msg = f"Grupo {grupo_num}: ✗ Error durante entrenamiento: {str(e)}"
        print(f"  {msg}")
        resumen_entrenamiento.append(msg)
        return None


# ---------------------------------------------------------------------------
# Paso 7 — Pipeline principal
# ---------------------------------------------------------------------------

def run_forecasting() -> pd.DataFrame:
    """
    Pipeline completo de forecasting.
    
    Retorna
    -------
    pd.DataFrame con todas las predicciones o None si no hay datos válidos.
    """
    print("=" * 70)
    print("FORECASTING DE RENTABILIDADES — FICs")
    print("=" * 70)
    
    resumen_entrenamiento = []
    
    # 1. Carga
    df = load_processed_data()
    
    # 2. Preparación base
    df = prepare_base_data(df)
    
    # 3. Calcular rentabilidades por período
    df = calculate_multiple_rentals(df)
    
    # 4. Filtrar y transformar
    df = filter_and_transform(df)
    
    # 5. Análisis de rangos de fechas
    resumen = analyze_date_ranges(df)
    
    # 6. Iterar por grupo de fechas
    print(f"\nEntrenando modelos por grupo de fechas...")
    print("-" * 70)
    
    todas_predicciones = []
    todos_observados = []
    grupos = resumen['grupo_fechas'].unique()
    
    for grupo_num, grupo_id in enumerate(sorted(grupos)):
        grupo_ids = resumen[resumen['grupo_fechas'] == grupo_id]['id'].tolist()
        
        # Validar
        validacion = validate_group(grupo_num, grupo_ids, resumen)
        print(f"  {validacion['mensaje']}")
        
        if not validacion['valido']:
            continue
        
        # Filtrar datos del grupo
        datos_grupo = df[df['id'].isin(grupo_ids)].copy()

        # Acumular observados que realmente entran al entrenamiento
        todos_observados.append(format_observed_output(datos_grupo))
        
        # Entrenar y predecir
        predicciones = train_and_predict(
            datos_grupo,
            grupo_num,
            validacion['dias_forecast'],
            resumen_entrenamiento
        )
        
        if predicciones is not None:
            todas_predicciones.append(predicciones)
    
    print("-" * 70)
    
    if not todas_predicciones:
        print("⚠ No se generaron predicciones (sin grupos válidos).")
        return None
    
    # Combinar todas las predicciones
    df_predicciones = pd.concat(todas_predicciones, ignore_index=True)
    df_observados = pd.concat(todos_observados, ignore_index=True)
    
    # 7. Guardar
    return save_forecasts(df_predicciones, df_observados, resumen_entrenamiento)


def save_forecasts(df: pd.DataFrame, df_observados: pd.DataFrame, resumen: List[str]) -> pd.DataFrame:
    """Guarda predicciones, observados y logs en archivos separados."""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_ts = FORECASTS_DIR / f"fics_pronósticos_{timestamp}.parquet"
    archivo_obs_ts = FORECASTS_DIR / f"fics_observados_{timestamp}.parquet"
    archivo_log = FORECASTS_DIR / f"forecast_log_{timestamp}.txt"
    
    # Guardar predicciones
    df.to_parquet(archivo_ts, index=False)
    df.to_parquet(ARCHIVO_SALIDA_LATEST, index=False)

    # Guardar observados
    df_observados.to_parquet(archivo_obs_ts, index=False)
    df_observados.to_parquet(ARCHIVO_OBSERVADOS_LATEST, index=False)
    
    print(f"\n✓ Predicciones guardadas:")
    print(f"  {archivo_ts.name}")
    print(f"  {ARCHIVO_SALIDA_LATEST.name}")
    print(f"  Filas: {len(df):>10,}")
    print(f"\n✓ Observados guardados:")
    print(f"  {archivo_obs_ts.name}")
    print(f"  {ARCHIVO_OBSERVADOS_LATEST.name}")
    print(f"  Filas: {len(df_observados):>10,}")
    
    # Guardar log
    with open(archivo_log, 'w', encoding='utf-8') as f:
        f.write("RESUMEN DE ENTRENAMIENTO\n")
        f.write("=" * 70 + "\n")
        f.write(f"Timestamp: {timestamp}\n\n")
        for linea in resumen:
            f.write(linea + "\n")
    
    print(f"  {archivo_log.name}")
    
    # Limpieza automática
    print("\n🧹 Ejecutando limpieza de históricos...")
    resultado_vacuum = run_vacuum(
        days_retention=7,
        dry_run=False,
        verbose=False
    )
    print(f"   {resultado_vacuum['mensaje_ui']}")
    
    print("\n✓ Forecasting completado.")
    print("=" * 70)
    
    return df


def load_forecasts() -> pd.DataFrame:
    """
    Carga las predicciones más recientes desde data/forecasts/.
    Útil para que la app Shiny consuma sin reejecutar.
    """
    if not ARCHIVO_SALIDA_LATEST.exists():
        raise FileNotFoundError(
            f"No se encontró {ARCHIVO_SALIDA_LATEST}. "
            "Ejecute run_forecasting() primero."
        )
    return pd.read_parquet(ARCHIVO_SALIDA_LATEST)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    df_predicciones = run_forecasting()
    
    if df_predicciones is not None:
        print("\n── Primeras predicciones ──")
        print(df_predicciones.head(10).to_string(index=False))