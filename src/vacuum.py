"""
vacuum.py
---------
Limpieza automática de archivos históricos con timestamp en las carpetas de datos.

Mantiene solo los archivos "_latest" (sin timestamp) y elimina archivos históricos
que excedan una edad máxima configurable (por defecto 7 días).

Archivos objetivo:
    - data/raw/fics_alternativos_<YYYYMMDD_HHMMSS>.parquet
    - data/processed/fics_rentabilidades_<YYYYMMDD_HHMMSS>.parquet
    - y otros con patrón de timestamp en el nombre

Uso:
    from vacuum import run_vacuum
    run_vacuum(days_retention=7)  # Mantiene solo archivos de hasta 7 días atrás
"""

import os
from pathlib import Path
from datetime import datetime, timedelta
import re

# Directorio raíz de datos
DATA_DIR = Path("data")

# Patrón para detectar timestamps en nombres de archivo: YYYYMMDD_HHMMSS
TIMESTAMP_PATTERN = re.compile(r"(\d{8})_(\d{6})")


def _parse_timestamp_from_filename(filename: str) -> datetime:
    """
    Extrae el timestamp del nombre del archivo.
    
    Ejemplo: "fics_alternativos_20260323_165348.parquet" → datetime(2026, 3, 23, 16, 53, 48)
    
    Retorna:
        datetime del timestamp, o None si no encuentra
    """
    match = TIMESTAMP_PATTERN.search(filename)
    if not match:
        return None
    
    try:
        fecha_str = match.group(1)  # YYYYMMDD
        hora_str = match.group(2)   # HHMMSS
        
        año = int(fecha_str[0:4])
        mes = int(fecha_str[4:6])
        día = int(fecha_str[6:8])
        
        hora = int(hora_str[0:2])
        minuto = int(hora_str[2:4])
        segundo = int(hora_str[4:6])
        
        return datetime(año, mes, día, hora, minuto, segundo)
    except (ValueError, IndexError):
        return None


def _is_file_old(filepath: Path, max_age_days: int) -> bool:
    """
    Determina si un archivo es más antiguo que max_age_days.
    
    Compara la fecha del timestamp en el nombre del archivo con la fecha actual.
    """
    timestamp = _parse_timestamp_from_filename(filepath.name)
    if timestamp is None:
        return False
    
    age = datetime.now() - timestamp
    return age > timedelta(days=max_age_days)


def _should_delete(filepath: Path, max_age_days: int) -> bool:
    """
    Determina si un archivo debe ser eliminado:
    - Debe tener timestamp en el nombre (no es "_latest")
    - Debe ser más antiguo que max_age_days
    - Debe ser un archivo (no carpeta)
    """
    if not filepath.is_file():
        return False
    
    # No eliminar archivos "_latest"
    if "_latest" in filepath.name:
        return False
    
    # Solo eliminar si tiene timestamp y es viejo
    return _is_file_old(filepath, max_age_days)


def run_vacuum(days_retention: int = 7, dry_run: bool = False, verbose: bool = True) -> dict:
    """
    Ejecuta la limpieza de archivos históricos en data/ y subcarpetas.
    
    Parámetros
    ----------
    days_retention : int
        Número de días a retener archivos históricos (por defecto 7).
        Archivos con timestamp más antiguo que esto serán eliminados.
    
    dry_run : bool
        Si es True, solo muestra qué sería eliminado sin eliminar realmente.
    
    verbose : bool
        Si es True, imprime detalles durante la ejecución (por defecto True).
        Útil para desactivar prints cuando se ejecuta desde app Shiny.
    
    Retorna
    -------
    dict con estadísticas:
        {
            "eliminados": int,
            "preservados": int,
            "archivos_no_procesados": int,
            "archivos_latest": int,
            "detalle": list[str],  — lista de rutas eliminadas/a eliminar
            "exito": bool,  — si la operación fue exitosa
            "mensaje_ui": str,  — mensaje amigable para mostrar en UI
            "log_eventos": list[dict],  — eventos detallados del proceso
        }
    """
    if not DATA_DIR.exists():
        if verbose:
            print(f"⚠ La carpeta {DATA_DIR} no existe.")
        return {
            "eliminados": 0,
            "preservados": 0,
            "archivos_no_procesados": 0,
            "archivos_latest": 0,
            "detalle": [],
            "exito": False,
            "mensaje_ui": "Error: La carpeta 'data' no existe.",
            "log_eventos": [{"tipo": "error", "mensaje": "Carpeta data no encontrada"}]
        }
    
    stats = {
        "eliminados": 0,
        "preservados": 0,
        "archivos_no_procesados": 0,
        "archivos_latest": 0,
        "detalle": [],
        "exito": True,
        "mensaje_ui": "",
        "log_eventos": []
    }
    
    mode = "DRY RUN" if dry_run else "EJECUCIÓN"
    if verbose:
        print("=" * 70)
        print(f"VACUUM — Limpieza de históricos ({mode})")
        print("=" * 70)
        print(f"Retención: {days_retention} días")
        print(f"Fecha límite: {(datetime.now() - timedelta(days=days_retention)).strftime('%Y-%m-%d %H:%M:%S')}")
        print()
    
    stats["log_eventos"].append({
        "tipo": "inicio",
        "mensaje": f"Iniciando limpieza ({mode})",
        "timestamp": datetime.now().isoformat()
    })
    
    # Recorrer todos los archivos en data/ recursivamente
    for filepath in DATA_DIR.rglob("*"):
        if not filepath.is_file():
            continue
        
        # Detectar tipo de archivo
        if "_latest" in filepath.name:
            stats["archivos_latest"] += 1
            continue
        
        # Chequear si tiene timestamp
        timestamp = _parse_timestamp_from_filename(filepath.name)
        
        if timestamp is None:
            stats["archivos_no_procesados"] += 1
            continue
        
        # Chequear si es viejo
        if _should_delete(filepath, days_retention):
            stats["eliminados"] += 1
            action = "[ELIMINAR]" if not dry_run else "[A ELIMINAR]"
            # Intentar hacer relativa la ruta, sino usar la ruta como está
            try:
                display_path = filepath.relative_to(Path.cwd())
            except (ValueError, TypeError):
                display_path = filepath
            msg = f"{action} {display_path}"
            if verbose:
                print(msg)
            stats["detalle"].append(str(filepath))
            
            # Registrar evento
            timestamp_archivo = _parse_timestamp_from_filename(filepath.name)
            edad_dias = (datetime.now() - timestamp_archivo).days if timestamp_archivo else 0
            stats["log_eventos"].append({
                "tipo": "eliminacion",
                "archivo": filepath.name,
                "edad_dias": edad_dias,
                "estado": "pendiente" if dry_run else "eliminado"
            })
            
            if not dry_run:
                try:
                    filepath.unlink()
                    stats["log_eventos"][-1]["estado"] = "eliminado"
                except Exception as e:
                    if verbose:
                        print(f"  ⚠ Error al eliminar: {e}")
                    stats["log_eventos"][-1]["estado"] = f"error: {str(e)}"
                    stats["eliminados"] -= 1
        else:
            stats["preservados"] += 1
    
    # Construir mensaje para UI
    if dry_run:
        if stats["eliminados"] > 0:
            stats["mensaje_ui"] = f"Se eliminarían {stats['eliminados']} archivo(s) | {stats['preservados']} preservados"
        else:
            stats["mensaje_ui"] = "No hay archivos para eliminar"
    else:
        if stats["eliminados"] > 0:
            stats["mensaje_ui"] = f"✓ Se eliminaron {stats['eliminados']} archivo(s) exitosamente"
        else:
            stats["mensaje_ui"] = "✓ No había archivos para eliminar"
    
    # Resumen en consola (si verbose)
    if verbose:
        print()
        print("=" * 70)
        print("RESUMEN DEL VACUUM")
        print("=" * 70)
        print(f"Archivos eliminados:        {stats['eliminados']:>6}")
        print(f"Archivos preservados:       {stats['preservados']:>6}")
        print(f"Archivos '_latest':         {stats['archivos_latest']:>6}")
        print(f"Archivos sin timestamp:     {stats['archivos_no_procesados']:>6}")
        print("=" * 70)
        print()
        
        if dry_run:
            print("ⓘ Este fue un DRY RUN. No se eliminó nada.")
            print("  Ejecuta con dry_run=False para eliminar realmente.")
        else:
            if stats["eliminados"] > 0:
                print(f"✓ {stats['eliminados']} archivo(s) eliminado(s).")
            else:
                print("✓ No hay archivos para eliminar.")
        
        print()
    
    # Registrar evento de finalización
    stats["log_eventos"].append({
        "tipo": "resumen",
        "eliminados": stats["eliminados"],
        "preservados": stats["preservados"],
        "latest_files": stats["archivos_latest"],
        "sin_timestamp": stats["archivos_no_procesados"],
        "timestamp_final": datetime.now().isoformat()
    })
    
    return stats


# ---------------------------------------------------------------------------
# Entry point para pruebas locales
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Ejecutar primero en dry_run para ver qué se eliminaría
    print("\n" + "=" * 70)
    print("PRUEBA EN DRY RUN PRIMERO")
    print("=" * 70 + "\n")
    stats = run_vacuum(days_retention=7, dry_run=True)
    
    # Preguntar al usuario si desea proceder
    if stats["eliminados"] > 0:
        print("\n¿Deseas proceder con la eliminación? (s/n): ", end="")
        respuesta = input().strip().lower()
        if respuesta == "s":
            print()
            run_vacuum(days_retention=7, dry_run=False)
        else:
            print("Cancelado.")
    else:
        print("No hay archivos para eliminar.")
