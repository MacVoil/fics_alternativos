import pandas as pd
from autogluon.timeseries import TimeSeriesDataFrame, TimeSeriesPredictor
import numpy as np

#Leyendo datos procesados
processed_data = pd.read_parquet("data/processed/fics_rentabilidades_latest.parquet")

# Fecha maxima en el dataset
max_fecha = processed_data['fecha_corte'].max()

#Primer día de hace 5 años
start_fecha = max_fecha - pd.DateOffset(years=5)
primer_dia = start_fecha.replace(month=1,day=1)


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
      .pipe(lambda x: np.exp(x) - 1)
)

#eliminar filas con NaN en rent_360d (es decir, las primeras 359 filas de cada producto)
processed_data_base = processed_data_base.dropna(subset=['rent_360d']).reset_index(drop=True)

# eliminar columna de rent_diaria ya que no es necesaria para el modelo
processed_data_base = processed_data_base.drop(['rent_diaria'], axis=1)

# Filtrando datos para los últimos 5 años
processed_data_base = processed_data_base[processed_data_base['fecha_corte'] >= primer_dia]

# Revisar cuales serian los productos con fechas similares para agruparlos
fechas_por_grupo = processed_data_base.groupby(
    ["id"]
)['fecha_corte'].agg(['min', 'max']).reset_index()

fechas_por_grupo['n_annios'] = (fechas_por_grupo['max'] - fechas_por_grupo['min']).dt.days / 365

fechas_por_grupo['grupo'] = fechas_por_grupo.groupby(['min', 'max']).ngroup()

fechas_por_grupo = fechas_por_grupo.drop(['min', 'max'], axis=1)

print(fechas_por_grupo.head())

# Tomando el grupo 1 del dataframe original
all_data = processed_data_base[processed_data_base['id'].isin(fechas_por_grupo[fechas_por_grupo['grupo'] == 1]['id'])]

# Seprar en train y test con corte en un año antes del último dato
ultimo_dato = all_data['fecha_corte'].max()
corte = ultimo_dato - pd.Timedelta(days=365)
train_data = all_data[all_data['fecha_corte'] <= corte]
test_data = all_data[all_data['fecha_corte'] > corte]

# Convertir a TimeSeriesDataFrame
train_data_ts = TimeSeriesDataFrame.from_data_frame(
    train_data, 
    id_column="id", 
    timestamp_column="fecha_corte"
)

# creando predictor
predictor = TimeSeriesPredictor(
    target="rent_360d",  # variable objetivo
    prediction_length=365,  # prediciendo un año hacia adelante
    freq="D",  # frecuencia diaria
    eval_metric="WQL",  # Weighted Quantile Loss
    #known_covariates_names =["is_holiday_or_weekend_co", "is_holiday_or_weekend_us"],
    quantile_levels  = [0.05, 0.25, 0.5, 0.75, 0.95],
    path="autogluon_models/"
)

# Entrenando el modelo
predictor.fit(
    train_data_ts, 
    presets="best_quality",  # preset para mejor calidad de predicción
    refit_full=False,  # no refit completo después de encontrar el mejor modelo
    num_val_windows = 5,  # número de ventanas de validación para evaluar el modelo durante el entrenamiento
    time_limit =3600,  # límite de tiempo de entrenamiento en segundos (1 hora)
    random_seed=42,
    verbosity=3
)

predictor.leaderboard()

test_data_ts = TimeSeriesDataFrame.from_data_frame(
    all_data, 
    id_column="id", 
    timestamp_column="fecha_corte"
)


predictions = predictor.predict(train_data_ts)
predictions.head()

predictor.plot(test_data_ts, predictions, quantile_levels=[0.05, 0.25, 0.5, 0.75, 0.95], max_history_length=1000, max_num_item_ids=4)

# notas: Cambiar logica para no poner fines de semana y feriados. Quiar TFT y DEEPAR 
