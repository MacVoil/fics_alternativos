# Vacuum.py - Mejoras para Integración con Shiny

## 📋 Resumen de Cambios

Se actualizó `src/vacuum.py` para mejor integración con apps Shiny (y otros contextos sin UI terminal).

### Cambios Principales

#### 1. **Nuevo Parámetro: `verbose`**
```python
# Antes
run_vacuum(days_retention=7, dry_run=False)

# Después
run_vacuum(days_retention=7, dry_run=False, verbose=True)
#                                                    ↑ Default: True
```

- `verbose=True` (default): Imprime detalles en consola (comportamiento anterior)
- `verbose=False`: Sin prints - ideal para Shiny o APIs

#### 2. **Nuevo Campo: `mensaje_ui`**
```python
resultado = run_vacuum(days_retention=7, dry_run=False, verbose=False)

# Mensaje amigable listo para mostrar en UI
print(resultado['mensaje_ui'])
# Output: "✓ Se eliminaron 3 archivo(s) exitosamente"
```

Mensajes automáticos según contexto:
- Dry-run con archivos: `"Se eliminarían 4 archivo(s) | 10 preservados"`
- Sin archivos: `"No hay archivos para eliminar"`
- Eliminación exitosa: `"✓ Se eliminaron 3 archivo(s) exitosamente"`
- Error: `"Error: La carpeta 'data' no existe."`

#### 3. **Nuevo Campo: `log_eventos`**
```python
# Eventos detallados para auditoría/UI
resultado['log_eventos']
# [
#   {"tipo": "inicio", "mensaje": "...", "timestamp": "2026-03-27T12:38:00"},
#   {"tipo": "eliminacion", "archivo": "datos_20260320.parquet", "edad_dias": 8, "estado": "eliminado"},
#   ...
#   {"tipo": "resumen", "eliminados": 3, "preservados": 10, "timestamp_final": "..."}
# ]
```

Tipos de evento:
- `"inicio"` - Comienza la limpieza
- `"eliminacion"` - Archivo borrado (con edad_dias y estado)
- `"resumen"` - Estadísticas finales

#### 4. **Nuevo Campo: `exito`**
```python
resultado['exito']  # bool - indica si operación fue exitosa
```

Para control de flujo en callbacks.

---

## 🎯 Uso en Shiny

### Patrón 1: Limpieza Manual (Recomendado para Usuario)

```python
from shiny import App, reactive, render, ui, Inputs, Outputs, Session
from vacuum import run_vacuum

app_ui = ui.page_fluid(
    ui.h2("Limpiar Históricos"),
    
    ui.input_action_button("btn_vacuum", "🔥 Ejecutar Limpieza"),
    ui.output_text("vacuum_status"),
)

def server(input: Inputs, output: Outputs, session: Session):
    
    @reactive.Effect
    @reactive.event(input.btn_vacuum)
    def on_vacuum_click():
        # Ejecutar SILENCIOSAMENTE
        resultado = run_vacuum(
            days_retention=7,
            dry_run=False,
            verbose=False  # ← Sin prints
        )
        
        # Uso del nuevo campo mensaje_ui
        print(f"Resultado: {resultado['mensaje_ui']}")
    
    @render.text
    def vacuum_status():
        resultado = run_vacuum(days_retention=7, dry_run=True, verbose=False)
        return resultado['mensaje_ui']  # ← Listo para mostrar

app = App(app_ui, server)

if __name__ == "__main__":
    app.run()
```

### Patrón 2: Después de Procesamiento (Automático)

```python
# En processing.py o similar
from vacuum import run_vacuum

def procesar_datos():
    print("Procesando rentabilidades...")
    # ... código de procesamiento ...
    
    # AUTOMÁTICAMENTE limpiar después
    resultado = run_vacuum(
        days_retention=7,
        dry_run=False,
        verbose=False
    )
    
    # Log silencioso
    print(f"Limpieza: {resultado['mensaje_ui']}")
    
    return {
        "procesamiento": "exitoso",
        "limpieza": resultado['mensaje_ui'],
        "archivos_liberados": resultado['eliminados']
    }
```

### Patrón 3: Auditoría con log_eventos

```python
# Registrar todos los cambios
resultado = run_vacuum(days_retention=7, dry_run=False, verbose=False)

# Guardar log estructurado
import json

with open("logs/vacuum_auditoria.jsonl", "a") as f:
    for evento in resultado['log_eventos']:
        f.write(json.dumps(evento) + "\n")

# O mostrar en tabla de UI
for evento in resultado['log_eventos']:
    if evento['tipo'] == 'eliminacion':
        print(f"Eliminado: {evento['archivo']} ({evento['edad_dias']}d)")
```

---

## 📊 Estructura Completa del Retorno

```python
{
    # Campos originales
    "eliminados": int,              # Cantidad eliminados
    "preservados": int,             # Cantidad preservados
    "archivos_no_procesados": int,  # Sin timestamp (ignorados)
    "archivos_latest": int,         # Archivos *_latest
    "detalle": list[str],           # Rutas de eliminados
    
    # NUEVOS CAMPOS
    "exito": bool,                  # Operación exitosa
    "mensaje_ui": str,              # Mensaje para mostrar (listo)
    "log_eventos": [                # Auditoría estructurada
        {
            "tipo": "inicio|eliminacion|resumen",
            "mensaje": str,         # (solo en tipo=inicio)
            "archivo": str,         # (solo en tipo=eliminacion)
            "edad_dias": int,       # (solo en tipo=eliminacion)
            "estado": str,          # (solo en tipo=eliminacion)
            "eliminados": int,      # (solo en tipo=resumen)
            "preservados": int,     # (solo en tipo=resumen)
            "latest_files": int,    # (solo en tipo=resumen)
            "sin_timestamp": int,   # (solo en tipo=resumen)
            "timestamp": str,       # ISO format
        },
        ...
    ]
}
```

---

## ✅ Testing

Ejecutar validación:
```bash
python test_vacuum_improvements.py
```

Tests incluyen:
- ✓ Modo silencioso (verbose=False)
- ✓ Estructura del retorno completamente
- ✓ Eventos registrados correctamente
- ✓ Comparación verbose vs silencioso

---

## 🔄 Compatibilidad Hacia Atrás

**✅ Compatible:** El código anterior sigue funcionando

```python
# Código anterior (sigue funcionando)
run_vacuum(days_retention=7, dry_run=False)
# Usa verbose=True por defecto

# Código nuevo (mejor para Shiny)
run_vacuum(days_retention=7, dry_run=False, verbose=False)
```

---

## 📁 Archivos Relacionados

- **[src/vacuum.py](../src/vacuum.py)** - Módulo actualizado
- **[ejemplo_vacuum_en_shiny.py](../ejemplo_vacuum_en_shiny.py)** - Patrones de integración
- **[test_vacuum_improvements.py](../test_vacuum_improvements.py)** - Validación
- **[docs/VACUUM_GUIDE.md](../docs/VACUUM_GUIDE.md)** - Guía completa (anterior)

---

## 🎓 Ejemplo Completo: Widget Shiny

```python
from shiny import App, reactive, render, ui
from vacuum import run_vacuum

def vacuum_card():
    """Widget reutilizable para limpieza"""
    return ui.card(
        ui.h3("🧹 Mantenimiento de Datos"),
        
        ui.row(
            ui.column(6,
                ui.input_slider("vacuum_days", "Retención (días)", 
                               min=1, max=30, value=7)
            ),
            ui.column(6,
                ui.input_checkbox("vacuum_dry", "Simulación", value=True)
            )
        ),
        
        ui.input_action_button("btn_vacuum", "Limpiar", class_="btn-warning"),
        
        ui.br(),
        ui.output_text_verbatim("vacuum_result")
    )

def vacuum_server(input, output, session):
    
    @reactive.Effect
    @reactive.event(input.btn_vacuum)
    def run():
        resultado = run_vacuum(
            days_retention=input.vacuum_days(),
            dry_run=input.vacuum_dry(),
            verbose=False
        )
        
        # Mostrar resultado
        print(f"Status: {resultado['mensaje_ui']}")
        print(f"Exitoso: {resultado['exito']}")
    
    @render.text
    def vacuum_result():
        resultado = run_vacuum(
            days_retention=input.vacuum_days(),
            dry_run=True,
            verbose=False
        )
        
        lines = [
            resultado['mensaje_ui'],
            "",
            f"Archivos: {resultado['preservados']} preservados"
        ]
        return "\n".join(lines)

# Uso en app.py
app = App(
    ui.page_fluid(
        ui.h1("FICs Dashboard"),
        vacuum_card(),
        # ... más contenido ...
    ),
    vacuum_server
)
```

---

## 🚀 Próximos Pasos

1. ✅ Integrar en `processing.py` (auto-cleanup después de procesamiento)
2. ✅ Integrar en `app.py` (UI para limpieza manual)
3. ⏳ Considerar scheduler automático (opcional)
4. ⏳ Centralizar logs de auditoría (opcional)

---

**Estado:** ✅ Completado y Validado  
**Compatibilidad:** ✅ Hacia Atrás Compatible
**Listo para:** 🚀 Producción en Shiny
