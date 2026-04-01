# Contexto del Proyecto: Dashboard FICs Alternativos

## Descripción general

Web app local para visualizar y proyectar la rentabilidad de fondos de inversión colectiva (FICs) alternativos del mercado colombiano. La app permite disparar la actualización de datos, el entrenamiento de modelos de forecast y la visualización del análisis de rentabilidad.

---

## Stack tecnológico

| Componente | Herramienta |
|---|---|
| App | Shiny for Python |
| Almacenamiento | Parquet (pandas + pyarrow) |
| Fuente de datos | API Socrata — datos.gov.co |
| Forecast | AutoGluon TimeSeries |
| Visualización | Plotly |
| Despliegue | Local (máquina del usuario) |

---

## Fuente de datos

- **Endpoint API:** `https://www.datos.gov.co/resource/qhpu-8ixx.json`
- **Proveedor:** Portal de Datos Abiertos de Colombia (Socrata)
- **Autenticación:** No requiere token
- **Columnas de hechos:** fecha_corte, tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion, principal_compartimento, valor_unidad_operaciones, numero_unidades_fondo_cierre, valor_fondo_cierre_dia_t, precierre_fondo_dia_t, numero_inversionistas, rendimientos_abonados, aportes_recibidos, retiros_redenciones, anulaciones
- **Columnas de dimensiones:** nombre_tipo_entidad, nombre_entidad, nombre_patrimonio, nombre_tipo_patrimonio, nombre_subtipo_patrimonio, tipo_negocio, subtipo_negocio

### Particularidades conocidas de la fuente
- `codigo_entidad` es único solo dentro de `tipo_entidad` — la PK de entidad es compuesta `(tipo_entidad, codigo_entidad)`.
- `codigo_negocio` es único solo dentro de `(tipo_entidad, codigo_entidad)` — la PK de fondo es compuesta `(tipo_entidad, codigo_entidad, codigo_negocio)`.
- Los nombres descriptivos (nombre_entidad, nombre_patrimonio, etc.) pueden aparecer con variantes distintas para el mismo código en fechas diferentes. Se resuelve conservando el registro más reciente de cada PK.
- No todas las entidades reportan en la misma fecha; algunas pueden estar varios días rezagadas.

---

## Modelo de datos

### Tablas de dimensiones (gestionadas por `catalogo.py`)

**`dim_entidad`** — PK: `(tipo_entidad, codigo_entidad)`
- tipo_entidad, codigo_entidad
- nombre_tipo_entidad, nombre_entidad

**`dim_fondo`** — PK: `(tipo_entidad, codigo_entidad, codigo_negocio)`
- tipo_entidad, codigo_entidad, codigo_negocio
- nombre_patrimonio, tipo_negocio, nombre_tipo_patrimonio, subtipo_negocio, nombre_subtipo_patrimonio

**`dim_participacion`** — PK: `(tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion)`
- tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion
- Permite mostrar al usuario las series/clases disponibles por fondo

### Tabla de hechos (gestionada por `ingestion.py`)

**`fact_rendimientos_diarios`** — PK: `(fecha_corte, tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion)`
- Claves: fecha_corte, tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion, principal_compartimento
- Métricas de valor: valor_unidad_operaciones, numero_unidades_fondo_cierre, valor_fondo_cierre_dia_t, precierre_fondo_dia_t, numero_inversionistas
- Métricas de flujo: rendimientos_abonados, aportes_recibidos, retiros_redenciones, anulaciones
- Las rentabilidades (diaria, mensual, semestral, anual) **no se descargan** — se calculan en `processing.py`

---

## Estructura de carpetas

```
fics_alternativos/
├── data/
│   ├── raw/
│   │   ├── dims/                          # Dimensiones del catálogo
│   │   │   ├── dim_entidad.parquet
│   │   │   ├── dim_fondo.parquet
│   │   │   ├── dim_participacion.parquet
│   │   │   └── catalogo_ultima_actualizacion.txt
│   │   ├── fics_alternativos_latest.parquet   # Hechos crudos — último run
│   │   └── fics_alternativos_<timestamp>.parquet  # Hechos crudos — trazabilidad
│   ├── processed/     # Rentabilidades diarias calculadas
│   │   ├── fics_rentabilidades_latest.parquet
│   │   └── fics_rentabilidades_<timestamp>.parquet
│   └── forecasts/     # Proyecciones de AutoGluon
│       ├── fics_pronósticos_latest.parquet
│       ├── fics_pronósticos_<timestamp>.parquet
│       └── forecast_log_<timestamp>.txt       # Log de cada entrenamiento
├── src/
│   ├── catalogo.py        # Descarga de dimensiones desde API Socrata
│   ├── ingestion.py       # Descarga de hechos crudos desde API Socrata
│   ├── processing.py      # Cálculo de rentabilidades diarias
│   ├── forecasting.py     # Entrenamiento y predicción AutoGluon
│   └── vacuum.py          # Limpieza automática de archivos históricos
├── autogluon_models/      # Modelos entrenados por AutoGluon
├── app/
│   └── app.py             # Shiny for Python
└── requirements.txt
```

---

## Pipeline de datos

### 0. Catálogo (`catalogo.py`) ✅ Completado
- Consulta los últimos 30 días calendario desde la fecha de ejecución
- Pagina la API trayendo solo columnas de dimensión (`$select` explícito)
- Deduplica por PK conservando el registro con mayor `fecha_corte` — cubre entidades rezagadas
- Construye y guarda `dim_entidad`, `dim_fondo` y `dim_participacion` en `data/raw/dims/`
- Expone `catalogo_disponible()` y `load_dims()` para que la app Shiny los consuma sin volver a llamar a la API

### 1. Ingesta (`ingestion.py`) ✅ Completado
- Recibe lista de hasta 5 fondos seleccionados por el usuario (con PK completa)
- Construye filtro SoQL con la PK compuesta `(tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion)`
- Usa `$select` explícito para traer solo columnas de hechos — sin nombres descriptivos ni rentabilidades
- Pagina la API en bloques de 50.000 registros desde offset 0 (histórico completo)
- Limpia y tipa el DataFrame; ordena por PK natural de hechos
- Guarda en `data/raw/fics_alternativos_latest.parquet` y una copia con timestamp
- **Ejecuta automáticamente limpieza de históricos (`vacuum.py`) al finalizar**

### 2. Procesamiento (`processing.py`) ✅ Completado
- Lee el parquet de `data/raw/fics_alternativos_latest.parquet`
- **Paso 1 — Carga**: carga los hechos crudos
- **Paso 2 — Filtro compartimento**: por cada grupo `(tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion)` conserva solo el menor `principal_compartimento` (aplica principalmente a casos edge con múltiples compartimentos)
- **Paso 3 — Flujo y métricas**: calcula `flujo_neto_inversionistas = aportes_recibidos - retiros_redenciones + anulaciones` y selecciona columnas de salida
- **Paso 4 — Rentabilidad diaria**: calcula el ratio diario respecto al día anterior usando: `rent_diaria = VU_hoy / VU_ayer`
- **Paso 5 — Filtro NaN**: elimina registros donde `rent_diaria` es NaN (típicamente el primer registro de cada fondo/participación)
- **Paso 6 — Persistencia**: guarda resultado único en `data/processed/fics_rentabilidades_latest.parquet` + copia con timestamp
- **Ejecuta automáticamente limpieza de históricos (`vacuum.py`) al finalizar**

**Columnas de salida:**
- Claves: tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion, fecha_corte
- Compartimento: principal_compartimento
- Métricas: valor_unidad_operaciones, numero_unidades_fondo_cierre, valor_fondo_cierre_dia_t, numero_inversionistas, rendimientos_abonados
- Flujo: flujo_neto_inversionistas
- Rentabilidad: rent_diaria (sin valores NaN)

### 4. Mantenimiento de datos (`vacuum.py`) ✅ Completado
- Ejecuta limpieza automática de archivos históricos con timestamp que excedan el período de retención (por defecto 7 días)
- Preserva siempre los archivos `_latest` como referencias actuales
- Permite ejecutarse en modo `dry_run` para validar qué se eliminaría sin hacer cambios
- **Integración completada en tres módulos:**
  - Al final de `ingestion.py`: después de guardar datos crudos
  - Al final de `processing.py`: después de guardar rentabilidades calculadas
  - Al final de `forecasting.py`: después de guardar proyecciones
- Import robusto con try/except en `ingestion.py` y `processing.py` para funcionar tanto desde `src/` como desde la raíz del proyecto o notebooks

### 5. Forecast (`forecasting.py`) ✅ Completado
- Lee `data/processed/fics_rentabilidades_latest.parquet`
- **Paso 1 — Carga y preparación**: selecciona columnas FK + `rent_diaria`, crea `id` de producto (`tipo_entidad_codigo_entidad_codigo_negocio_tipo_participacion`)
- **Paso 2 — Rentabilidades por período**: calcula rentabilidades de 30, 60, 90, 180 y 360 días usando rolling windows con medias geométricas (log-returns), anualizado a 365 días
- **Paso 3 — Filtrado y transformación**: filtra NaN en `rent_360d` (descarta primeros 359 registros de cada producto), descarta `rent_diaria`, transforma a formato largo (cada período en fila separada), incluye el período en el `id`
- **Paso 4 — Análisis de fechas**: por cada `id` (producto + período) calcula min/max de fechas y agrupa productos con fechas idénticas como lote de entrenamiento
- **Paso 5 — Validación por grupo**: mínimo 90 días para entrenar; entre 90-730 días usa horizonte adaptativo `(n_dias-1)//2`; ≥731 días usa horizonte de 365 días
- **Paso 6 — Entrenamiento**: entrena AutoGluon TimeSeries con Chronos2 ZeroShot y ZeroShot-small por cada grupo; genera predicciones con cuantiles p0.2, p0.5, p0.8 + media
- **Paso 7 — Persistencia**: guarda en `data/forecasts/fics_pronósticos_latest.parquet` + copia con timestamp + log de entrenamiento
- **Ejecuta automáticamente limpieza de históricos (`vacuum.py`) al finalizar**

**Columnas de salida:**
- Claves: tipo_entidad, codigo_entidad, codigo_negocio, tipo_participacion (descompuestas del id)
- tipo_rentabilidad: rent_30d, rent_60d, rent_90d, rent_180d o rent_360d
- fecha_corte: fecha de la proyección
- Proyecciones: mean, p0.2, p0.5, p0.8

**Mensajes de validación para la app:**
- `< 90 días`: "Insuficientes datos — no se genera forecast"
- `90-730 días`: "Datos limitados — forecast de X días (no alcanza 365 días)"
- `≥ 731 días`: forecast completo de 365 días

---

## App Shiny — Funcionalidades previstas

- **Botón "Actualizar catálogo"** → ejecuta `catalogo.py` — refresca la lista de fondos disponibles
- **Selector de fondos** → el usuario escoge hasta 5 combinaciones `(fondo + tipo_participacion)` del catálogo
- **Botón "Actualizar datos"** → ejecuta `ingestion.py` con los fondos seleccionados
- **Botón "Procesar rentabilidades"** → ejecuta `processing.py` y genera tabla de rentabilidades diarias
- **Botón "Entrenar modelos"** → ejecuta `forecasting.py`
- Pestaña de visualización de rentabilidades diarias históricas por fondo
- Pestaña de visualización de proyecciones (forecast)
- Tabla interactiva con datos de flujo neto de inversionistas y métricas operativas

---

## Estado del proyecto

| Módulo | Estado |
|---|---|
| catalogo.py | ✅ Completado |
| ingestion.py | ✅ Completado |
| processing.py | ✅ Completado |
| vacuum.py | ✅ Completado |
| forecasting.py | ✅ Completado |
| app.py | 🔲 Pendiente |

---

## Notas y decisiones técnicas

- Se usan archivos Parquet como almacenamiento principal por rendimiento y simplicidad (sin base de datos).
- Cada corrida de ingesta genera un archivo con timestamp + sobreescribe el `_latest` para trazabilidad.
- El procesamiento de rentabilidades se calcula a partir del `valor_unidad_operaciones` usando lag de 1 registro (día anterior), garantizando control total del cálculo y evitando las rentabilidades precalculadas de la API.
- La rentabilidad diaria se calcula como el ratio simple: `rent_diaria = VU_hoy / VU_ayer`.
- Se eliminan los primeros registros de cada serie (`rent_diaria = NaN`) ya que no hay datos del día anterior para calcularla.
- El filtro de `principal_compartimento` conserva el valor mínimo por grupo para casos edge con múltiples compartimentos (99.99% de los grupos tienen solo uno).
- AutoGluon TimeSeries se usa con Chronos2 (zero-shot y small) con cuantiles 0.2/0.5/0.8 y media. Los productos se agrupan por rango de fechas similar para entrenar en lotes coherentes.
- Las PKs de entidad y fondo son **siempre compuestas**: `codigo_entidad` y `codigo_negocio` no son únicos globalmente, solo dentro de su entidad padre.
- El catálogo usa una ventana de 30 días (configurable con `VENTANA_DIAS`) para garantizar cobertura de entidades que no reportan diariamente.
- Los nombres descriptivos de la API son inconsistentes entre fechas; se resuelve tomando el registro más reciente por PK en el proceso de construcción de dimensiones.
- `catalogo.py` debe ejecutarse antes de `ingestion.py` en la primera ejecución, ya que provee al usuario el listado de fondos disponibles para seleccionar.

### Gestión de datos históricos (`vacuum.py`)
- Cada módulo (ingestion.py, processing.py, forecasting.py) genera archivos con timestamp para trazabilidad histórica.
- Sin limpieza, los archivos pueden acumularse significativamente; `vacuum.py` resuelve esto de forma automática y configurable.
- **Patrón de retención**: Conserva todos los archivos `_latest` (referencias actuales) permanentemente y elimina archivos timestamped más antiguos que el período de retención (por defecto 7 días).
- **Archivos ignorados**: Archivos sin patrón de timestamp (p.ej. `dim_*.parquet`, `catalogo_*.txt`) nunca se eliminan.
- **Seguridad**: La función admite modo `dry_run` para validar qué se eliminaría sin hacer cambios reales.
- **Integración**: Se puede invocar manualmente (`run_vacuum()`) o integrar al final de pipelines de ingesta/procesamiento/forecasting.
- **Requisitos de calidad**: Asume que los timestamps en nombres de archivo siguen el patrón exacto `YYYYMMDD_HHMMSS` — cualquier variación es ignorada.