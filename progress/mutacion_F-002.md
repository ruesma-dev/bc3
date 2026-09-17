<!-- progress/mutacion_F-002.md -->
# F-002 · Campaña de mutación

Generado por `python -m harness.mutacion --feature F-002 --workers 4` el 2026-09-17 16:20.

## Alcance

Origen del diff: **rama** (`75f805e6806ea04ffe092000b50313b4c2d36e0e` .. `feature/F-002-porcentuales-a-ud`).

| Fichero | Líneas en alcance |
|---|---|
| `application/pipeline/pipeline.py` | 2 |
| `application/pipeline/steps.py` | 41 |
| `config/settings.py` | 3 |
| `infrastructure/bc3/bc3_modifier.py` | 10 |
| `infrastructure/bc3/bc3_porcentajes.py` | 569 |
| `interface_adapters/cli/__init__.py` | 2 |
| `interface_adapters/cli/porcentuales_cli.py` | 102 |
| `interface_adapters/controllers/etl_controller.py` | 22 |
| **Total** | **751** |

## Totales

| Métrica | Valor |
|---|---|
| Mutantes generados | 156 |
| Mutantes evaluados | 156 |
| Muertos | 156 |
| Supervivientes | 0 |
| Timeouts | 0 |
| Timeouts repasados en serie | 0: ningún mutante agotó el reloj |
| Sin veredicto (base rota) | 0 |
| Tiempo total | 324.9 s |
| SHA de HEAD medido | `e4aaffe136f9f115bb5737acd173f2e6ac1c8ecc` |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_415p46fn/wk_0` | 78.2 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_415p46fn/wk_1` | 79.4 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_415p46fn/wk_2` | 79.5 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_415p46fn/wk_3` | 78.1 |
| Media por mutante evaluado (s) | 2.1 |
| Timeout efectivo por mutante (s) | 159 — derivado de la línea base × 2.0 |
| Suelo configurado (s) | 120 |
| Workers | 4 |
| Muestreo | no: campaña completa |

## Supervivientes

Ninguno: cada mutación aplicada la cazó al menos un test.

