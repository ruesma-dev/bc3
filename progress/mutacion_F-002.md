<!-- progress/mutacion_F-002.md -->
# F-002 · Campaña de mutación

Generado por `python -m harness.mutacion --feature F-002 --workers 4` el 2026-09-18 01:08.

## Alcance

Origen del diff: **rama** (`75f805e6806ea04ffe092000b50313b4c2d36e0e` .. `feature/F-002-porcentuales-a-ud`).

| Fichero | Líneas en alcance |
|---|---|
| `application/pipeline/pipeline.py` | 2 |
| `application/pipeline/steps.py` | 44 |
| `config/settings.py` | 7 |
| `infrastructure/bc3/bc3_modifier.py` | 10 |
| `infrastructure/bc3/bc3_porcentajes.py` | 689 |
| `interface_adapters/cli/__init__.py` | 2 |
| `interface_adapters/cli/porcentuales_cli.py` | 109 |
| `interface_adapters/controllers/etl_controller.py` | 22 |
| **Total** | **885** |

## Totales

| Métrica | Valor |
|---|---|
| Mutantes generados | 177 |
| Mutantes evaluados | 177 |
| Muertos | 177 |
| Supervivientes | 0 |
| Timeouts | 0 |
| Timeouts repasados en serie | 0: ningún mutante agotó el reloj |
| Sin veredicto (base rota) | 0 |
| Tiempo total | 369.3 s |
| SHA de HEAD medido | `7cf5d9a7656da35edcc3be20f1485a0614050750` |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_x8yv40h9/wk_0` | 71.5 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_x8yv40h9/wk_1` | 73.5 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_x8yv40h9/wk_2` | 73.8 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_x8yv40h9/wk_3` | 71.1 |
| Media por mutante evaluado (s) | 2.1 |
| Timeout efectivo por mutante (s) | 148 — derivado de la línea base × 2.0 |
| Suelo configurado (s) | 120 |
| Workers | 4 |
| Muestreo | no: campaña completa |

## Supervivientes

Ninguno: cada mutación aplicada la cazó al menos un test.

