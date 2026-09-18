<!-- progress/mutacion_F-002.md -->
# F-002 · Campaña de mutación

Generado por `python -m harness.mutacion --feature F-002 --workers 4` el 2026-09-18 11:17.

## Alcance

Origen del diff: **rama** (`75f805e6806ea04ffe092000b50313b4c2d36e0e` .. `feature/F-002-porcentuales-a-ud`).

| Fichero | Líneas en alcance |
|---|---|
| `application/pipeline/pipeline.py` | 2 |
| `application/pipeline/steps.py` | 46 |
| `config/settings.py` | 12 |
| `infrastructure/bc3/bc3_modifier.py` | 10 |
| `infrastructure/bc3/bc3_porcentajes.py` | 753 |
| `interface_adapters/cli/__init__.py` | 2 |
| `interface_adapters/cli/porcentuales_cli.py` | 115 |
| `interface_adapters/controllers/etl_controller.py` | 22 |
| `utils/text_sanitize.py` | 23 |
| **Total** | **985** |

## Totales

| Métrica | Valor |
|---|---|
| Mutantes generados | 198 |
| Mutantes evaluados | 198 |
| Muertos | 198 |
| Supervivientes | 0 |
| Timeouts | 0 |
| Timeouts repasados en serie | 0: ningún mutante agotó el reloj |
| Sin veredicto (base rota) | 0 |
| Tiempo total | 446.7 s |
| SHA de HEAD medido | `3cdd2b79eb4e9aa2a7d69db7ddb80fb04ab8a2a3` |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_r0gtb7cf/wk_0` | 64.9 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_r0gtb7cf/wk_1` | 64.6 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_r0gtb7cf/wk_2` | 66.7 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_r0gtb7cf/wk_3` | 69.5 |
| Media por mutante evaluado (s) | 2.3 |
| Timeout efectivo por mutante (s) | 140 — derivado de la línea base × 2.0 |
| Suelo configurado (s) | 120 |
| Workers | 4 |
| Muestreo | no: campaña completa |

## Supervivientes

Ninguno: cada mutación aplicada la cazó al menos un test.

