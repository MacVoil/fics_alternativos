import pandas as pd
from autogluon.timeseries import TimeSeriesDataFrame, TimeSeriesPredictor
import numpy as np

#Leyendo datos procesados
processed_data = pd.read_parquet("data/processed/fics_rentabilidades_latest.parquet")

# Fecha maxima en el dataset
max_fecha = processed_data['fecha_corte'].max()

#Primer día de hace 5 años
start_fecha = max_fecha - pd.DateOffset(years=5)


#Tomando solo las columnas necesarias
processed_data_base = processed_data[
    ["fecha_corte", "tipo_entidad", "codigo_entidad", "codigo_negocio", 
     "tipo_participacion", "rent_diaria"]
]

# Creando ID de producto
processed_data_base["id"] = (
    processed_data_base["tipo_entidad"].astype(str) + "_" +
    processed_data_base["codigo_entidad"].astype(str) + "_" +
    processed_data_base["codigo_negocio"].astype(str) + "_" +
    processed_data_base["tipo_participacion"].astype(str)
)

# Quitando columnas que ya no son necesarias
processed_data_base = processed_data_base.drop(
    ["tipo_entidad", "codigo_entidad", "codigo_negocio", "tipo_participacion"], axis=1
)

# Ordenandolos por producto y fecha
processed_data_base = processed_data_base.sort_values(by=["id", "fecha_corte"]).reset_index(drop=True)

# Calcular rentabilidad 360 con máxima eficiencia y estabilidad numérica
processed_data_base['rent_360d'] = (
    processed_data_base.assign(log_r=np.log(processed_data_base['rent_diaria']))
      .groupby('id')['log_r']
      .rolling(window=360, min_periods=360)
      .sum()
      .reset_index(level=0, drop=True)
      .pipe(lambda x: np.exp(x*(365/360)) - 1)
)

# Calcular rentabilidad 180 con máxima eficiencia y estabilidad numérica
processed_data_base['rent_180d'] = (
    processed_data_base.assign(log_r=np.log(processed_data_base['rent_diaria']))
      .groupby('id')['log_r']
      .rolling(window=180, min_periods=180)
      .sum()
      .reset_index(level=0, drop=True)
      .pipe(lambda x: np.exp(x*(365/180)) - 1)
)

# Calcular rentabilidad 90 con máxima eficiencia y estabilidad numérica
processed_data_base['rent_90d'] = (
    processed_data_base.assign(log_r=np.log(processed_data_base['rent_diaria']))
      .groupby('id')['log_r']
      .rolling(window=90, min_periods=90)
      .sum()
      .reset_index(level=0, drop=True)
      .pipe(lambda x: np.exp(x*(365/90)) - 1)
)

# Calcular rentabilidad 60 con máxima eficiencia y estabilidad numérica
processed_data_base['rent_60d'] = (
    processed_data_base.assign(log_r=np.log(processed_data_base['rent_diaria']))
      .groupby('id')['log_r']
      .rolling(window=60, min_periods=60)
      .sum()
      .reset_index(level=0, drop=True)
      .pipe(lambda x: np.exp(x*(365/60)) - 1)
)

# Calcular rentabilidad 30 con máxima eficiencia y estabilidad numérica
processed_data_base['rent_30d'] = (
    processed_data_base.assign(log_r=np.log(processed_data_base['rent_diaria']))
      .groupby('id')['log_r']
      .rolling(window=30, min_periods=30)
      .sum()
      .reset_index(level=0, drop=True)
      .pipe(lambda x: np.exp(x*(365/30)) - 1)
)

#eliminar filas con NaN en rent_360d (es decir, las primeras 359 filas de cada producto)
processed_data_base = processed_data_base.dropna(subset=['rent_360d']).reset_index(drop=True)

# eliminar columna de rent_diaria ya que no es necesaria para el modelo
processed_data_base = processed_data_base.drop(['rent_diaria'], axis=1)

# Unvipot para dejar cada rentabilidad en una fila diferente, con su respectiva fecha
processed_data_base = processed_data_base.melt(
    id_vars=['id', 'fecha_corte'],
    value_vars=['rent_360d', 'rent_180d', 'rent_90d', 'rent_60d', 'rent_30d'],
    var_name='periodo',
    value_name='rentabilidad'
)

#actualalizando el ID para incluir el periodo de rentabilidad
processed_data_base['id'] = processed_data_base['id'] + '_' + processed_data_base['periodo']

#eliminar la columna de periodo ya que ahora está incluida en el ID
processed_data_base = processed_data_base.drop(['periodo'], axis=1)

# Revisar cuales serian los productos con fechas similares para agruparlos
fechas_por_grupo = processed_data_base.groupby(
    ["id"]
)['fecha_corte'].agg(['min', 'max']).reset_index()

fechas_por_grupo['n_dias'] = (fechas_por_grupo['max'] - fechas_por_grupo['min']).dt.days

fechas_por_grupo['grupo'] = fechas_por_grupo.groupby(['min', 'max']).ngroup()

fechas_por_grupo = fechas_por_grupo.drop(['min', 'max'], axis=1)

print(fechas_por_grupo.head())


# Ejemplo para grupos con igual o más de 731 días de datos (es decir, al menos 2 años de datos)
# Tomando el grupo 1 del dataframe original
all_data = processed_data_base[processed_data_base['id'].isin(fechas_por_grupo[fechas_por_grupo['grupo'] == 0]['id'])]

# Convertir a TimeSeriesDataFrame
data_ts = TimeSeriesDataFrame.from_data_frame(
    all_data, 
    id_column="id", 
    timestamp_column="fecha_corte"
)

data_ts.head()

# creando predictor
predictor = TimeSeriesPredictor(
    target="rentabilidad",  # variable objetivo
    prediction_length=365,  # prediciendo n días hacia adelante
    freq="D",  # frecuencia diaria
    eval_metric="WQL",  # Weighted Quantile Loss
    #known_covariates_names =["periodo"],  # covariables conocidas (periodo de rentabilidad)
    quantile_levels  = [0.2, 0.5, 0.8],
    path="autogluon_models/"
)

# Entrenando el modelo
predictor.fit(
    data_ts, 
    #presets="chronos2",  # preset para mejor calidad de predicción
    hyperparameters={
        "Chronos2": [
            # Zero-shot model
            {
                "ag_args": {"name_suffix": "ZeroShot"},
                "model_path": "autogluon/chronos-2"
            },
            # Fine-tuned model
            {
                #"fine_tune": True, 
                "ag_args": {"name_suffix": "ZeroShot-small"},
                "model_path": "autogluon/chronos-2-small",
                #"eval_during_fine_tune": True,
            }
        ],
        
    },
    refit_full=False,  # no refit completo después de encontrar el mejor modelo
    num_val_windows = 10,  # número de ventanas de validación para evaluar el modelo durante el entrenamiento
    time_limit =3600,  # límite de tiempo de entrenamiento en segundos (1 hora)
    random_seed=42,
    verbosity=3,
    refit_every_n_windows=1,  # refit cada n ventanas de validación para mejorar la estabilidad del modelo
    #excluded_model_types = ["DeepAR", "TemporalFusionTransformer"]
)

predictor.leaderboard()


predictions = predictor.predict(data_ts)
predictions.head()

predictor.plot(data_ts, predictions, quantile_levels=[0.2, 0.5, 0.8], max_history_length=1000, max_num_item_ids=10)

# ejemplo para grupos con menos de 731 días de datos (es decir, menos de 2 años de datos)

all_data = processed_data_base[processed_data_base['id'].isin(fechas_por_grupo[fechas_por_grupo['grupo'] == 2]['id'])]

# Sacar una variable con el número de días de datos disponibles del grupo selecionado
dias_disponibles = fechas_por_grupo[fechas_por_grupo['grupo'] == 2]['n_dias'].iloc[0]

# días maximo a predecir
dias_a_predecir = (dias_disponibles -1) // 2

# Convertir a TimeSeriesDataFrame
data_ts = TimeSeriesDataFrame.from_data_frame(
    all_data, 
    id_column="id", 
    timestamp_column="fecha_corte"
)

data_ts.head()

# creando predictor
predictor = TimeSeriesPredictor(
    target="rentabilidad",  # variable objetivo
    prediction_length=dias_a_predecir,  # prediciendo n días hacia adelante
    freq="D",  # frecuencia diaria
    eval_metric="WQL",  # Weighted Quantile Loss
    #known_covariates_names =["periodo"],  # covariables conocidas (periodo de rentabilidad)
    quantile_levels  = [0.2, 0.5, 0.8],
    path="autogluon_models/"
)

# Entrenando el modelo
predictor.fit(
    data_ts, 
    #presets="chronos2",  # preset para mejor calidad de predicción
    hyperparameters={
        "Chronos2": [
            # Zero-shot model
            {
                "ag_args": {"name_suffix": "ZeroShot"},
                "model_path": "autogluon/chronos-2"
            },
            # Fine-tuned model
            {
                #"fine_tune": True, 
                "ag_args": {"name_suffix": "ZeroShot-small"},
                "model_path": "autogluon/chronos-2-small",
                #"eval_during_fine_tune": True,
            }
        ],
        
    },
    refit_full=False,  # no refit completo después de encontrar el mejor modelo
    num_val_windows = 10,  # número de ventanas de validación para evaluar el modelo durante el entrenamiento
    time_limit =3600,  # límite de tiempo de entrenamiento en segundos (1 hora)
    random_seed=42,
    verbosity=3,
    refit_every_n_windows=1,  # refit cada n ventanas de validación para mejorar la estabilidad del modelo
    #excluded_model_types = ["DeepAR", "TemporalFusionTransformer"]
)

predictor.leaderboard()


predictions = predictor.predict(data_ts)
predictions.head()

predictor.plot(data_ts, predictions, quantile_levels=[0.2, 0.5, 0.8], max_history_length=1000, max_num_item_ids=10)