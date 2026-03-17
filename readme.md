<!-- README.md -->
# Servicio 1 GUI BC3

Este proyecto mantiene la GUI y toda la lógica BC3 local.  
La diferencia clave es que la fase 2 ya no lanza un script del servicio 2, sino que
importa directamente la librería `ruesma_ocr_service`.

## Cambio principal

- Antes: `subprocess` contra `python -m ...bc3_classify_stdin`
- Ahora: cliente Python directo contra `ruesma_ocr_service.Bc3ClassifierLibrary`

## Instalación

1. Instala primero la librería del servicio 2 en el mismo entorno virtual.
2. Ejecuta la GUI con:

```bash
python main_gui.py
```

## Compilación con PyInstaller

```bash
pyinstaller --collect-data ruesma_ocr_service main_gui.py
```
