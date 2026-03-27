"""
ejemplo_vacuum_en_shiny.py
---------------------------
Ejemplo de cómo integrar vacuum.py en una app Shiny.

Este archivo muestra patrones recomendados para usar vacuum en callbacks de Shiny.
"""

# Esto es un ejemplo conceptual de cómo se vería en app.py

"""
from shiny import App, reactive, render, ui, Inputs, Outputs, Session
from vacuum import run_vacuum

# =========================================================================
# PATRÓN 1: Botón de Limpieza Manual (Recomendado)
# =========================================================================

app_ui = ui.page_fluid(
    ui.h2("Gestión de Vacío"),
    
    ui.card(
        ui.h3("Limpieza de Históricos"),
        ui.p("Las ejecuciones generan archivos con timestamp para trazabilidad."),
        ui.p("Este botón limpia archivos más antiguos que 7 días."),
        
        ui.row(
            ui.column(6,
                ui.input_slider("vacuum_days", "Días a retener", 
                                min=1, max=30, value=7, step=1)
            ),
            ui.column(6,
                ui.input_checkbox("vacuum_dry_run", "Simulación (dry_run)", 
                                 value=True)
            )
        ),
        
        ui.input_action_button("btn_vacuum_preview", "👁️ Previsualizar", 
                              class_="btn-info"),
        ui.input_action_button("btn_vacuum_execute", "🔥 Ejecutar Limpieza", 
                              class_="btn-warning"),
        
        ui.output_text("vacuum_status"),
        
        ui.details(
            ui.h4("📋 Detalle de Archivos"),
            ui.output_ui("vacuum_log_table"),
            summary="Mostrar detalle"
        )
    )
)

def server(input: Inputs, output: Outputs, session: Session):
    
    # Almacenar resultado du vacuum para reutilizar
    vacuum_result = reactive.Value(None)
    
    # =====================================================================
    # CALLBACK 1: Previsualizar qué se limpiaría
    # =====================================================================
    @reactive.Effect
    @reactive.event(input.btn_vacuum_preview)
    def preview_vacuum():
        # Ejecutar en dry_run=True (sin eliminar)
        resultado = run_vacuum(
            days_retention=input.vacuum_days(),
            dry_run=True,
            verbose=False  # ← No imprimir en consola
        )
        vacuum_result.set(resultado)
    
    # =====================================================================
    # CALLBACK 2: Ejecutar limpieza real
    # =====================================================================
    @reactive.Effect
    @reactive.event(input.btn_vacuum_execute)
    def execute_vacuum():
        # Confirmar solo si no es dry_run
        if not input.vacuum_dry_run():
            # En producción agregar confirmación aquí
            show_modal_confirm()
        
        # Ejecutar vacuum real
        resultado = run_vacuum(
            days_retention=input.vacuum_days(),
            dry_run=input.vacuum_dry_run(),
            verbose=False  # ← No imprimir en consola
        )
        vacuum_result.set(resultado)
    
    # =====================================================================
    # RENDER 1: Mostrar estado/mensaje principal
    # =====================================================================
    @render.text
    def vacuum_status():
        result = vacuum_result()
        if result is None:
            return "Presiona 'Previsualizar' o 'Ejecutar Limpieza'"
        
        # Usar el mensaje_ui amigable
        estatus = f\"Estatus: {result['mensaje_ui']}\"
        
        # Agregar línea de información adicional
        if result['eliminados'] > 0 or result['preservados'] > 0:
            detalles = f\"(Eliminados: {result['eliminados']}, Preservados: {result['preservados']})\"
            estatus += f\" {detalles}\"
        
        return estatus
    
    # =====================================================================
    # RENDER 2: Tabla de eventos de limpieza
    # =====================================================================
    @render.ui
    def vacuum_log_table():
        result = vacuum_result()
        if result is None or not result['log_eventos']:
            return ui.p("Sin eventos aún", class_="text-muted")
        
        # Construir tabla HTML desde log_eventos
        rows = []
        for evento in result['log_eventos']:
            tipo = evento.get('tipo', '')
            
            if tipo == 'inicio':
                rows.append(ui.HTML(
                    f\"<tr><td>🟢</td><td>{evento['mensaje']}</td><td>{evento['timestamp']}</td></tr>\"
                ))
            elif tipo == 'eliminacion':
                estado_icon = '✅' if evento['estado'] == 'eliminado' else '⏳'
                rows.append(ui.HTML(
                    f\"<tr><td>{estado_icon}</td><td>{evento['archivo']} ({evento['edad_dias']}d)</td><td>{evento['estado']}</td></tr>\"
                ))
            elif tipo == 'resumen':
                rows.append(ui.HTML(
                    f\"<tr><td>📊</td><td>RESUMEN</td><td>Elim: {evento['eliminados']}, Preserv: {evento['preservados']}</td></tr>\"
                ))
        
        tabla_html = '<table class=\"table table-sm\">' + ''.join(rows) + '</table>'
        return ui.HTML(tabla_html)

app = App(app_ui, server)


# =========================================================================
# PATRÓN 2: Limpieza Automática Después de Procesamiento
# =========================================================================

def procesamiento_con_limpieza():
    '''Esto se ejecutaría dentro de un callback de procesamiento'''
    
    # Step 1: Ejecutar procesamiento
    print("Procesando datos...")
    # ... código de procesamiento ...
    
    # Step 2: Automáticamente limpiar histórico
    print("Limpiando históricos...")
    resultado = run_vacuum(
        days_retention=7,
        dry_run=False,
        verbose=False  # ← Sin prints para no contaminar logs
    )
    
    # Step 3: Loguear resultado
    print(f"Vacío completado: {resultado['mensaje_ui']}")
    
    # Retornar insight
    return {
        "procesamiento": "exitoso",
        "vacuum": resultado['mensaje_ui'],
        "archivos_liberados": resultado['eliminados']
    }


# =========================================================================
# PATRÓN 3: Modal de Confirmación para Limpieza Real
# =========================================================================

def show_modal_confirm():
    '''Muestra un modal de confirmación antes de eliminar realmente'''
    from shiny.ui import modal
    
    m = modal(
        "⚠️ Confirmación de Limpieza",
        "¿Estás seguro de que quieres eliminar archivos históricos?",
        "Esta acción NO es reversible.",
        footer=modal_footer(
            action_button("confirm_vacuum", "Sí, eliminar", class_="btn-danger"),
            modal_button("Cancelar")
        ),
        easy_close=False
    )
    showModal(m)

# En el servidor:
# @reactive.effect
# @reactive.event(input.confirm_vacuum)
# def execute_confirmed():
#     resultado = run_vacuum(days_retention=7, dry_run=False, verbose=False)
#     removeModal()
#     # ... actualizar UI con resultado ...


# =========================================================================
# PATRÓN 4: Uso Silencioso (Background)
# =========================================================================

@reactive.Effect
def cleanup_background():
    '''Ejecuta cada cierto tiempo sin molestrar al usuario'''
    import threading
    import time
    
    def background_cleanup():
        while True:
            time.sleep(3600)  # Cada hora
            run_vacuum(days_retention=7, dry_run=False, verbose=False)
    
    cleanup_thread = threading.Thread(target=background_cleanup, daemon=True)
    cleanup_thread.start()


# =========================================================================
# DOCUMENTACIÓN DEL NUEVO RETORNO
# =========================================================================

'''
El nuevo retorno de run_vacuum() incluye:

{
    "eliminados": int,              # Cantidad de archivos eliminados
    "preservados": int,             # Cantidad preservados (recientes)
    "archivos_no_procesados": int,  # Archivos sin timestamp (ignorados)
    "archivos_latest": int,         # Archivos *_latest (siempre preservados)
    "detalle": list[str],           # Rutas de archivos eliminados
    "exito": bool,                  # Operación exitosa o no
    "mensaje_ui": str,              # ← NUEVO: Mensaje amigable para UI
    "log_eventos": list[dict],      # ← NUEVO: Eventos detallados
}

Campos NUEVOS para UI:
- mensaje_ui: Listo para mostrar directamente en interfaz
- log_eventos: Eventos estructurados para construir tablas/gráficos
- exito: Flag para controlar flujos condicionales
'''

# =========================================================================
# VENTAJAS DEL NUEVO DISEÑO
# =========================================================================

'''
✅ Mejor integración con Shiny:
   - verbose=False desactiva prints (limpia logs)
   - mensaje_ui ya está formateado para mostrar
   - log_eventos estructurado para UI reactiva

✅ Compatible con ejecución async:
   - No bloquea la app con prints
   - Retorna datos que app puede procesar

✅ Auditoría y monitoreo:
   - log_eventos permite registrar toda la limpieza
   - Facilita debugging si hay errores

✅ Reutilizable en diferentes contextos:
   - Callbacks de Shiny
   - Background jobs
   - APIs REST (futuro)
   - Logs centralizados
'''
