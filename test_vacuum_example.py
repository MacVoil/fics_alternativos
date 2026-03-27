"""
test_vacuum_example.py
--------------
Script de demostración del comportamiento de vacuum.py con simulación de archivos antiguos.

Este script:
1. Crea archivos "fantasma" de prueba en data/ con timestamps anteriores
2. Ejecuta vacuum en dry_run para mostrar qué se eliminaría
3. Limpia los archivos de prueba

USO:
    python test_vacuum_example.py
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import tempfile
import shutil

# Agregar src al path para importar vacuum
sys.path.insert(0, str(Path(__file__).parent / "src"))

from vacuum import run_vacuum


def crear_archivos_de_prueba():
    """
    Crea archivos de prueba con timestamps antiguos en data/raw y data/processed.
    Retorna la lista de archivos creados para limpiarlos después.
    """
    archivos_creados = []
    
    # Ahora
    ahora = datetime.now()
    
    # Crear archivos con diferentes edades
    timestamps_prueba = [
        ("Ayer", ahora - timedelta(days=1)),
        ("3 días atrás", ahora - timedelta(days=3)),
        ("7 días atrás (límite)", ahora - timedelta(days=7)),
        ("8 días atrás (antiguo)", ahora - timedelta(days=8)),
        ("15 días atrás (muy antiguo)", ahora - timedelta(days=15)),
        ("30 días atrás (mes atrás)", ahora - timedelta(days=30)),
    ]
    
    for carpeta in ["data/raw", "data/processed"]:
        Path(carpeta).mkdir(parents=True, exist_ok=True)
        
        for descripcion, fecha_timestamp in timestamps_prueba:
            # Formato: YYYYMMDD_HHMMSS
            timestamp_str = fecha_timestamp.strftime("%Y%m%d_%H%M%S")
            
            if carpeta == "data/raw":
                nombre_archivo = f"fics_alternativos_{timestamp_str}.parquet"
            else:
                nombre_archivo = f"fics_rentabilidades_{timestamp_str}.parquet"
            
            ruta_completa = Path(carpeta) / nombre_archivo
            
            # Crear archivo vacío (solo para demostración)
            ruta_completa.write_text(f"Archivo fantasma de prueba: {descripcion}")
            
            # Modificar la fecha de modificación para que corresponda
            timestamp_unix = fecha_timestamp.timestamp()
            import os
            os.utime(ruta_completa, (timestamp_unix, timestamp_unix))
            
            archivos_creados.append(ruta_completa)
            print(f"  ✓ Creado: {ruta_completa} ({descripcion})")
    
    return archivos_creados


def mostrar_archivos_antes():
    """Muestra los archivos existentes antes de la prueba."""
    print("\n" + "="*70)
    print("ARCHIVOS EXISTENTES ANTES DE LA PRUEBA")
    print("="*70)
    
    for carpeta in ["data/raw", "data/processed"]:
        carpeta_path = Path(carpeta)
        if carpeta_path.exists():
            archivos = sorted(carpeta_path.glob("*.parquet"))
            if archivos:
                print(f"\n{carpeta}/")
                for archivo in archivos:
                    stats = archivo.stat()
                    mtime = datetime.fromtimestamp(stats.st_mtime)
                    edad_dias = (datetime.now() - mtime).days
                    print(f"  {archivo.name:50} | Modificado hace {edad_dias} días")


def main():
    print("\n" + "="*70)
    print("DEMO: vacuum.py con archivos de prueba")
    print("="*70)
    
    print("\n[1/3] Creando archivos de prueba con timestamps variados...")
    archivos_creados = crear_archivos_de_prueba()
    
    print("\n[2/3] Mostrando archivos antes de la prueba...")
    mostrar_archivos_antes()
    
    print("\n[3/3] Ejecutando vacuum.py en DRY RUN (sin eliminar)...")
    print("-" * 70)
    resultado = run_vacuum(days_retention=7, dry_run=True)
    print("-" * 70)
    
    # Mostrar detalles de los archivos que se eliminarían
    if resultado['detalle']:
        print("\n📋 ARCHIVOS QUE SE ELIMINARÍAN:")
        for archivo in resultado['detalle']:
            print(f"  ❌ {archivo}")
    
    print("\n" + "="*70)
    print("RESUMEN DE LA SIMULACIÓN")
    print("="*70)
    fecha_limite = datetime.now() - timedelta(days=7)
    print(f"Período de retención:      {7} días")
    print(f"Fecha límite:              {fecha_limite.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Archivos a eliminar:       {resultado['eliminados']}")
    print(f"Archivos a preservar:      {resultado['preservados']}")
    print(f"Archivos '_latest' (safe): {resultado['archivos_latest']}")
    print(f"Archivos sin timestamp:    {resultado['archivos_no_procesados']}")
    
    print("\n💡 INTERPRETACIÓN:")
    print("  - Archivos hasta 7 días: PRESERVADOS ✓")
    print("  - Archivos mayores a 7 días: ELIMINADOS ✗")
    print("  - Archivos '_latest': SIEMPRE PRESERVADOS ✓✓✓")
    
    # Limpiar archivos de prueba
    print("\n[Limpieza] Removiendo archivos de prueba...")
    for archivo in archivos_creados:
        if archivo.exists():
            archivo.unlink()
            print(f"  🗑️ Eliminado: {archivo}")
    
    print("\n✓ Prueba completada exitosamente")
    print("\nPróximos pasos:")
    print("  1. Ejecutar: python src/vacuum.py")
    print("  2. Ejecutar: run_vacuum(days_retention=7, dry_run=False)  # para limpiar real")
    print("  3. Ver: docs/VACUUM_GUIDE.md para documentación completa")


if __name__ == "__main__":
    main()
