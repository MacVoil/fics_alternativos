#!/usr/bin/env python
"""Script para verificar instalación de AutoGluon con GPU"""

import sys

print("=" * 70)
print("VERIFICACIÓN DE AUTOGLUON CON SOPORTE GPU")
print("=" * 70)

# 1. Verificar PyTorch
print("\n1. Verificando PyTorch...")
try:
    import torch
    print(f"   ✓ PyTorch versión: {torch.__version__}")
    print(f"   ✓ CUDA disponible: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        print(f"   ✓ Device GPU: {torch.cuda.get_device_name(0)}")
        print(f"   ✓ CUDA tipo: {torch.version.cuda}")
        print(f"   ✓ cuDNN versión: {torch.backends.cudnn.version()}")
    else:
        print("   ✗ CUDA NO disponible (revisar instalación)")
        sys.exit(1)
except Exception as e:
    print(f"   ✗ Error al importar PyTorch: {e}")
    sys.exit(1)

# 2. Verificar AutoGluon
print("\n2. Verificando AutoGluon...")
try:
    from autogluon.timeseries import TimeSeriesPredictor, TimeSeriesDataFrame
    try:
        import autogluon
        print(f"   ✓ AutoGluon versión: {autogluon.__version__}")
    except (AttributeError, ImportError):
        print(f"   ✓ AutoGluon instalado (versión disponible)")
    print(f"   ✓ TimeSeriesPredictor disponible")
except Exception as e:
    print(f"   ✗ Error al importar AutoGluon: {e}")
    sys.exit(1)

# 3. Verificar pandas
print("\n3. Verificando pandas...")
try:
    import pandas as pd
    print(f"   ✓ Pandas versión: {pd.__version__}")
except Exception as e:
    print(f"   ✗ Error al importar pandas: {e}")
    sys.exit(1)

# 4. Verificar numpy
print("\n4. Verificando numpy...")
try:
    import numpy as np
    print(f"   ✓ NumPy versión: {np.__version__}")
except Exception as e:
    print(f"   ✗ Error al importar numpy: {e}")
    sys.exit(1)

# 5. Test simple de GPU
print("\n5. Test simple de GPU con PyTorch...")
try:
    x = torch.randn(100, 100).cuda()
    y = torch.randn(100, 100).cuda()
    z = torch.matmul(x, y)
    print(f"   ✓ Operación GPU exitosa (forma resultado: {z.shape})")
except Exception as e:
    print(f"   ✗ Error en operación GPU: {e}")
    sys.exit(1)

print("\n" + "=" * 70)
print("✓ TODAS LAS VERIFICACIONES EXITOSAS")
print("  El ambiente está listo para usar AutoGluon con GPU")
print("=" * 70)
