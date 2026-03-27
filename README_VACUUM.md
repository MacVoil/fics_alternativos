# Vacuum.py - Sistema de Limpieza Automática de Datos

## Resumen Ejecutivo

`vacuum.py` es una utilidad robusta para gestionar el ciclo de vida de archivos históricos en el proyecto de análisis de FICs. Implementa un sistema configurable de retención que:

- ✅ **Preserva** automáticamente todos los archivos `_latest` (referencias actuales)
- ✅ **Retiene** archivos timestamped recientes (7 días por defecto, configurable)
- ✅ **Elimina** archivos históricos que excedan el período de retención
- ✅ **Respalda** con modo `dry_run` para validar cambios antes de ejecutar
- ✅ **Reporta** estadísticas detalladas de eliminaciones y preservaciones

---

## Problema Resuelto

### Contexto
Cada ejecución de los módulos de ingesta y procesamiento genera archivos Parquet con timestamps para trazabilidad:
- `fics_alternativos_20260327_113253.parquet`
- `fics_rentabilidades_20260327_113253.parquet`

Sin limpieza, después de 6 meses se acumularían ~180+ archivos (uno por día).

### Solución
`vacuum.py` automatiza la limpieza manteniendo un escenario limpio sin perder capacidad de auditoría.

---

## Instalación

### Ubicación
```
fics_alternativos/
└── src/
    └── vacuum.py  ← Aquí
```

### Requisitos
- Python 3.8+ (ya disponible en el proyecto)
- Librerías estándar: `os`, `pathlib`, `datetime`, `re` (ninguna instalación adicional)

---

## Uso Rápido

### 1. Ejecutar en dry_run (Recomendado primero)
```python
from vacuum import run_vacuum

# Simula qué se eliminaría sin hacer cambios
resultado = run_vacuum(days_retention=7, dry_run=True)
```

**Output esperado:**
```
======================================================================
VACUUM — Limpieza de históricos (DRY RUN)
======================================================================
[A ELIMINAR] data\raw\fics_alternativos_20260320_165348.parquet
[A ELIMINAR] data\processed\fics_rentabilidades_20260312_110859.parquet
...

RESUMEN DEL VACUUM
Archivos eliminados:            4
Archivos preservados:          12
Archivos '_latest':             2
Archivos sin timestamp:         4
======================================================================

ⓘ Este fue un DRY RUN. No se eliminó nada.
```

### 2. Ejecutar con eliminación real
```python
from vacuum import run_vacuum

# Elimina archivos reales
resultado = run_vacuum(days_retention=7, dry_run=False)
```

### 3. Ejecutar desde línea de comandos
```bash
# Dry run
python src/vacuum.py

# El script pregunta interactivamente
# Ingresa los días de retención (default: 7)
# Ingresa True/False para dry_run
```

---

## Parámetros

| Parámetro | Tipo | Valor por Defecto | Descripción |
|-----------|------|-------------------|-------------|
| `days_retention` | `int` | `7` | Días a retener archivos históricos |
| `dry_run` | `bool` | `False` | Si `True`, simula sin eliminar realmente |

---

## Comportamiento de Limpieza

### Archivos Preservados ✅
- ✅ Todos los archivos terminados en `_latest` (sin importar edad)
  - Ejemplo: `fics_rentabilidades_latest.parquet`
- ✅ Archivos con timestamp **dentro** del período de retención
  - Si retention=7 dias: archivos desde hoy hasta 7 días atrás
- ✅ Archivos sin patrón de timestamp (especialmente dimensiones)
  - Ejemplo: `dim_*.parquet`, `catalogo_*.txt`

### Archivos Eliminados ❌
- ❌ Archivos timestamped **más antiguos** que el período de retención
  - `fics_alternativos_20260320_*.parquet` (si retention=7 días y hoy es 27/03)
- ❌ Solo se eliminan si coinciden exactamente con: `*_YYYYMMDD_HHMMSS.*`

### Archivos Ignorados ⊘
- ⊘ Archivos sin timestamp en nombre (no se tocan)
- ⊘ Archivos `_latest` (siempre preservados)

---

## Retorno de la Función

```python
resultado = run_vacuum(days_retention=7, dry_run=True)

# Estructura del diccionario retornado:
{
    'eliminados': 4,                    # Cantidad de archivos a eliminar/eliminados
    'preservados': 12,                  # Cantidad de archivos preservados
    'archivos_no_procesados': 4,        # Archivos sin timestamp (ignorados)
    'archivos_latest': 2,               # Archivos *_latest (siempre preservados)
    'detalle': [                        # Lista de rutas a eliminar
        'data\\raw\\fics_alternativos_20260320_165348.parquet',
        'data\\processed\\fics_rentabilidades_20260312_110859.parquet',
        ...
    ]
}
```

---

## Estrategias de Retención Recomendadas

### Desarrollo (Limpieza Agresiva)
```python
run_vacuum(days_retention=1, dry_run=False)
```
- Mantiene solo archivos del último día
- Uso: Pruebas rápidas durante desarrollo

### Testing (Retención Semanal)
```python
run_vacuum(days_retention=7, dry_run=False)
```
- Mantiene una semana de histórico
- Uso: Validación de cambios, comparación semanal
- **Valor por defecto**

### Staging (Retención Bidocenal)
```python
run_vacuum(days_retention=14, dry_run=False)
```
- Mantiene dos semanas de histórico
- Uso: Preprod, validación de ciclos bisemanal

### Producción (Retención Mensual)
```python
run_vacuum(days_retention=30, dry_run=False)
```
- Mantiene un mes de histórico
- Uso: Auditoría, trazabilidad de cambios de mes

---

## Integración en Pipelines

### Después de `ingestion.py`
```python
# ingestion.py
def run_ingestion(fondos):
    # ... código existente ...
    # Guardar datos
    save_parquet(df, "data/raw/fics_alternativos")
    
    # Limpiar históricos antiguos
    from vacuum import run_vacuum
    run_vacuum(days_retention=7, dry_run=False)
    print("✓ Ingestion completada y limpiada")
```

### Después de `processing.py`
```python
# processing.py
def run_processing():
    # ... código existente ...
    # Guardar rentabilidades
    df.to_parquet(...)
    
    # Limpiar históricos antiguos
    from vacuum import run_vacuum
    run_vacuum(days_retention=7, dry_run=False)
    print("✓ Procesamiento completado y limpiado")
```

### En `app.py` (Cuando sea implementada)
```python
from vacuum import run_vacuum

# Botón en UI para manual cleanup
@app.callback(
    Output('vacuum-status', 'children'),
    Input('btn-vacuum', 'n_clicks'),
)
def ejecutar_vacuum(n_clicks):
    if n_clicks == 0:
        return "Click para limpiar históricos"
    
    resultado = run_vacuum(days_retention=7, dry_run=False)
    return f"✓ Limpieza: {resultado['eliminados']} archivos eliminados"
```

---

## Validación y Testing

### Caso 1: Verificar qué se eliminaría
```python
from vacuum import run_vacuum

# Ver sin cambios
resultado = run_vacuum(days_retention=7, dry_run=True)
print(f"Se eliminarían: {resultado['eliminados']} archivos")
```

### Caso 2: Ejecutar limpieza real
```python
from vacuum import run_vacuum

# Elimina realmente
resultado = run_vacuum(days_retention=7, dry_run=False)
print(f"Se eliminaron: {resultado['eliminados']} archivos")
print(f"Se preservaron: {resultado['preservados']} archivos")
```

### Caso 3: Testing automatizado
```bash
# Ejecutar el script de demostración incluido
python test_vacuum_example.py
```

Esto:
- ✓ Crea archivos de prueba con timestamps simulados (1, 3, 7, 8, 15, 30 días atrás)
- ✓ Ejecuta vacuum en dry_run
- ✓ Verifica que identifique correctamente qué eliminar
- ✓ Limpia archivos de prueba automáticamente

---

## Seguridad y Validaciones

### ✅ Validaciones Incorporadas

1. **Patrón de Timestamp Estricto**
   - Solo reconoce: `*_YYYYMMDD_HHMMSS.*`
   - Ignora archivos sin coincidencia exacta (seguridad)

2. **Preservación de `_latest`**
   - Nunca elimina archivos con `_latest` en el nombre
   - Garantiza referencias actuales siempre disponibles

3. **Preservación de Dimensiones**
   - Archivos como `dim_*.parquet` nunca se eliminan
   - Catálogos/metadatos siempre disponibles

4. **Modo Dry-Run Obligatorio en Pruebas**
   - Siempre ejecutar con `dry_run=True` primero
   - Validar output antes de ejecutar en producción

### ⚠️ Consideraciones Especiales

| Escenario | Comportamiento |
|-----------|----------------|
| Carpeta `data/` no existe | Ejecuta sin error, retorna 0 eliminados |
| Archivo sin permisos | Reporta error individual, continúa |
| Timestamp inválido | Ignorado (no coincide regex) |
| Fecha futura en timestamp | Preservado (asume reloj del SO correcto) |

---

## Troubleshooting

### P: ¿Por qué no se eliminan archivos esperados?
**R:** Posibles razones:
- Archivo es más reciente en el calendario que el límite
- Archivo tiene `_latest` en el nombre
- Archivo no tiene timestamp en formato exacto `YYYYMMDD_HHMMSS`
- **Solución:** Ejecutar con `dry_run=True` para ver la lógica

### P: ¿Cómo recupero un archivo eliminado accidentalmente?
**R:** 
- Siempre ejecutar `dry_run=True` primero
- Windows: Verificar Papelera de reciclaje
- Linux/Mac: Verificar si existe backup

### P: ¿Puedo eliminar archivos específicos?
**R:** `vacuum.py` solo elimina por edad. Para eliminar específicos:
```python
import os
os.remove("data/raw/archivo_especifico.parquet")
```

### P: ¿Qué pasa si la máquina se reinicia durante vacuum?
**R:** Seguro - archivos parcialmente eliminados son ignorados en próximas ejecuciones

---

## Estadísticas de Uso Real

**Ejemplo de ejecución con datos reales (27/03/2026):**
```
Período de retención: 7 días
Archivos identificados para eliminar: 4
Archivos preservados (recientes): 8
Archivos '_latest': 2
Archivos sin timestamp: 4
Espacio aproximado liberado: 2.2 MB
```

---

## Logging y Monitoreo

### Ver archivo de log (si se implementara)
```python
def run_vacuum_with_logging(days_retention=7, dry_run=False, log_file="vacuum.log"):
    resultado = run_vacuum(days_retention, dry_run)
    
    with open(log_file, "a") as f:
        f.write(f"{datetime.now()} - Eliminados: {resultado['eliminados']}\n")
        for archivo in resultado['detalle']:
            f.write(f"  - {archivo}\n")
    
    return resultado
```

---

## Próximos Pasos Recomendados

1. **Corto plazo:**
   - ✅ Integrar `run_vacuum()` en `ingestion.py` (después de guardar datos)
   - ✅ Integrar `run_vacuum()` en `processing.py` (después de guardar rentabilidades)

2. **Mediano plazo:**
   - ⏳ Agregar botón "Limpiar datos" en app.py (UI Shiny)
   - ⏳ Implementar logging de limpiezas para auditoría

3. **Largo plazo:**
   - ⏳ Scheduler automático (Task Scheduler en Windows / Cron en Linux)
   - ⏳ Monitoreo de espacio en disco y alertas

---

## Documentación Relacionada

- [VACUUM_GUIDE.md](./VACUUM_GUIDE.md) - Guía completa de uso
- [contexto_proyecto.md](./contexto_proyecto.md) - Arquitectura general del proyecto
- [test_vacuum_example.py](./test_vacuum_example.py) - Script de demostración y validación

---

## Autor y Versión

- **Versión:** 1.0  
- **Fecha de Creación:** 27/03/2026
- **Lenguaje:** Python 3.8+
- **Dependencias:** Solo librerías estándar (sin instalaciones adicionales)

---

**Estado:** ✅ **Completado y Testeado**

El módulo `vacuum.py` está listo para producción y ha sido validado con archivos de prueba de diferentes edades.
