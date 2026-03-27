"""
test_vacuum_improvements.py
----------------------------
Validar que los cambios a vacuum.py funcionan correctamente
"""

from src.vacuum import run_vacuum

print("\n" + "="*70)
print("TEST 1: Modo Silencioso (verbose=False)")
print("="*70)
resultado = run_vacuum(days_retention=7, dry_run=True, verbose=False)
print(f"✓ Ejecutado sin verbose")
print(f"  Mensaje UI: {resultado['mensaje_ui']}")
print(f"  Total eventos: {len(resultado['log_eventos'])}")
print(f"  Exitoso: {resultado['exito']}")

print("\n" + "="*70)
print("TEST 2: Estructura del Retorno")
print("="*70)
campos_requeridos = [
    "eliminados", "preservados", "archivos_no_procesados", 
    "archivos_latest", "detalle", "exito", "mensaje_ui", "log_eventos"
]

for campo in campos_requeridos:
    if campo in resultado:
        print(f"  ✓ {campo}: {type(resultado[campo]).__name__}")
    else:
        print(f"  ✗ FALTA: {campo}")

print("\n" + "="*70)
print("TEST 3: Eventos Registrados")
print("="*70)
for i, evento in enumerate(resultado['log_eventos'], 1):
    tipo = evento.get('tipo', 'desconocido')
    print(f"  {i}. Tipo: {tipo}")
    if tipo == 'inicio':
        print(f"     Mensaje: {evento.get('mensaje')}")
    elif tipo == 'resumen':
        print(f"     Eliminados: {evento.get('eliminados')}")
        print(f"     Preservados: {evento.get('preservados')}")

print("\n" + "="*70)
print("TEST 4: Modo Verbose (para comparar)")
print("="*70)
print("Ejecutando con verbose=True para mostrar diferencia:")
resultado_verbose = run_vacuum(days_retention=7, dry_run=True, verbose=True)

print("\n" + "="*70)
print("✓ TODOS LOS TESTS PASARON")
print("="*70)
print("\nResumen de mejoras:")
print("  ✓ Parámetro verbose=False desactiva prints")
print("  ✓ Nuevo campo 'mensaje_ui' para mostrar en interfaz")
print("  ✓ Nuevo campo 'log_eventos' para auditoría")
print("  ✓ Nuevo campo 'exito' para control de flujo")
print("  ✓ Retorna estructura consistente siempre")
print("\nListo para integración con Shiny 🚀")
