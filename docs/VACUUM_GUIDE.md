# Vacuum.py - Guía de Uso

## Descripción

`vacuum.py` es una utilidad de limpieza automática que mantiene el almacenamiento de datos bajo control eliminando archivos históricos antiguos mientras preserva:
- Todos los archivos `_latest` (referencias actuales)
- Archivos recientes dentro del período de retención configurado

## Archivos Objetivo

El script busca y limpia archivos con patrón de timestamp `YYYYMMDD_HHMMSS`:

```
data/raw/fics_alternativos_20260323_165348.parquet          ← Elimina si > 7 días
data/raw/fics_alternativos_latest.parquet                   ← Siempre preserva
data/processed/fics_rentabilidades_20260327_113253.parquet  ← Elimina si > 7 días
data/processed/fics_rentabilidades_latest.parquet           ← Siempre preserva
data/forecast/*_20260320_*.parquet                          ← Mismo comportamiento
```

## Uso

### 1. **Dry Run (Recomendado primero)**
```python
from vacuum import run_vacuum

# Preview qué se eliminaría sin hacer nada
resultado = run_vacuum(days_retention=7, dry_run=True)
print(resultado)
```

**Output esperado:**
```
======================================================================
VACUUM — Limpieza de históricos (DRY RUN)
======================================================================
Retención: 7 días
Fecha límite: 2026-03-20 11:55:57

======================================================================
RESUMEN DEL VACUUM
======================================================================
Archivos eliminados:             0
Archivos preservados:            6
Archivos '_latest':              2
Archivos sin timestamp:          4
======================================================================

ⓘ Este fue un DRY RUN. No se eliminó nada.
```

### 2. **Ejecución Real**
```python
from vacuum import run_vacuum

# Ejecutar con eliminación real
resultado = run_vacuum(days_retention=7, dry_run=False)

# Verificar estadísticas
print(f"Eliminados: {resultado['eliminados']} archivos")
print(f"Preservados: {resultado['preservados']} archivos")
print(f"Archivos Latest: {resultado['archivos_latest']} archivos")
```

### 3. **Diferentes Períodos de Retención**
```python
# Mantener solo el último día (limpieza agresiva)
run_vacuum(days_retention=1, dry_run=False)

# Mantener 30 días (retención mensual)
run_vacuum(days_retention=30, dry_run=False)

# Mantener 90 días (retención trimestral)
run_vacuum(days_retention=90, dry_run=False)
```

### 4. **Desde Línea de Comandos**
```bash
# Dry run
python src/vacuum.py

# Con parámetros personalizados (editar el bloque if __name__ == "__main__")
# O ejecutar interactivamente en Python
```

## Parámetros

| Parámetro | Tipo | Default | Descripción |
|-----------|------|---------|-------------|
| `days_retention` | int | 7 | Días a retener historiales |
| `dry_run` | bool | False | Si True, solo simula (no elimina) |

## Retorno

La función retorna un diccionario con estadísticas:

```python
{
    'eliminados': 5,              # Archivos borrados
    'preservados': 8,             # Archivos con timestamp preservados
    'archivos_latest': 2,         # Archivos _latest encontrados
    'sin_timestamp': 4,           # Archivos sin patrón de timestamp
    'detalle': [                  # Listado de archivos eliminados
        'data/raw/fics_alternativos_20260320_165348.parquet',
        'data/processed/fics_rentabilidades_20260320_110859.parquet',
        ...
    ],
    'fecha_limite': datetime(...) # Timestamp de corte
}
```

## Integración en Pipelines

### En `ingestion.py`
```python
from vacuum import run_vacuum

def run_ingestion(fondos, dias_a_procesar=90):
    """Procesa datos y limpia históricos viejos."""
    # ... código de ingestion ...
    
    # Ejecutar vacuum después de guardar nuevos datos
    run_vacuum(days_retention=7, dry_run=False)
    print("✓ Datos ingestion limpiados")
```

### En `processing.py`
```python
from vacuum import run_vacuum

def run_processing():
    """Procesa rentabilidades y limpia históricos."""
    # ... código de processing ...
    
    # Ejecutar vacuum después de procesar
    run_vacuum(days_retention=7, dry_run=False)
    print("✓ Datos procesados limpiados")
```

### En `app.py` (Cuando sea implementada)
```python
from vacuum import run_vacuum

@app.callback(
    Output('vacuum-status', 'children'),
    Input('btn-vacuum', 'n_clicks'),
)
def ejecutar_vacuum(n_clicks):
    if n_clicks == 0:
        return "Presiona para limpiar históricos"
    
    resultado = run_vacuum(days_retention=7, dry_run=False)
    return f"Eliminados: {resultado['eliminados']} archivos"
```

## Comportamiento en Casos Especiales

| Caso | Comportamiento |
|------|----------------|
| Archivo sin timestamp | Ignorado (no se elimina) |
| `_latest` file antiguo | Preservado siempre |
| Timestampincorrecto | Ignorado (no coincide regex) |
| Carpeta `data/` inexistente | Ejecuta sin error, retorna 0 eliminados |
| Permisos insuficientes | Reporta en detalle cual archivo falló |

## Recomendaciones

### Retención por Caso de Uso

```
Desarrollo:        1-3 días  (para limpiar rápido durante pruebas)
Testing:           7 días    (por defecto, buen balance)
Staging:           14 días   (permite comparar semanas)
Producción:        30 días   (retención mensual para auditoría)
```

### Scheduler (Próximos pasos)

Para automatizar la limpieza diaria:

**Windows (Task Scheduler):**
```batch
python C:\Users\user\Desktop\Proyectos\fics_alternativos\src\vacuum.py
```

**Linux/Mac (Cron):**
```bash
0 2 * * * cd /path/to/proyecto && python src/vacuum.py
```

**Python (APScheduler):**
```python
from apscheduler.schedulers.background import BackgroundScheduler
from vacuum import run_vacuum

scheduler = BackgroundScheduler()
scheduler.add_job(run_vacuum, 'cron', hour=2)
scheduler.start()
```

## Troubleshooting

**P: El vacuum no encuentra archivos**
R: Verifica que existan archivos con patrón `YYYYMMDD_HHMMSS` en nombres.

**P: ¿Por qué se preservan algunos archivos sin ser `_latest`?**
R: Son archivos sin timestamp en el nombre (ej: `dim_*.parquet`, `catalogo_*.txt`). No se eliminan por seguridad.

**P: ¿Cómo fuerzo eliminar archivos específicos?**
R: Reduce `days_retention` a 0 o usar `shutil.rmtree()` manualmente.

**P: ¿Puedo ejecutar múltiples veces el vacuum?**
R: Sí, es seguro ejecutar varias veces. Los archivos ya eliminados son ignorados.

## Ver También

- [processing.py](../src/processing.py) - Produces timestamped rentability files
- [ingestion.py](../src/ingestion.py) - Produces timestamped raw data files
- [contexto_proyecto.md](./contexto_proyecto.md) - Project architecture
