<!-- progress/mutacion_F-002.md -->
# F-002 · Campaña de mutación

Generado por `python -m harness.mutacion --feature F-002 --workers 4` el 2026-09-17 16:51.

## Alcance

Origen del diff: **rama** (`75f805e6806ea04ffe092000b50313b4c2d36e0e` .. `feature/F-002-porcentuales-a-ud`).

| Fichero | Líneas en alcance |
|---|---|
| `application/pipeline/pipeline.py` | 2 |
| `application/pipeline/steps.py` | 41 |
| `config/settings.py` | 3 |
| `infrastructure/bc3/bc3_modifier.py` | 10 |
| `infrastructure/bc3/bc3_porcentajes.py` | 564 |
| `interface_adapters/cli/__init__.py` | 2 |
| `interface_adapters/cli/porcentuales_cli.py` | 102 |
| `interface_adapters/controllers/etl_controller.py` | 22 |
| **Total** | **746** |

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
| Tiempo total | 395.1 s |
| SHA de HEAD medido | `8fb5393ad3eb71ad7de3eeba8e07152f1d86c22c` |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_ums6a6s2/wk_0` | 66.3 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_ums6a6s2/wk_1` | 63.8 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_ums6a6s2/wk_2` | 61.7 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_ums6a6s2/wk_3` | 62.0 |
| Media por mutante evaluado (s) | 2.5 |
| Timeout efectivo por mutante (s) | 133 — derivado de la línea base × 2.0 |
| Suelo configurado (s) | 120 |
| Workers | 4 |
| Muestreo | no: campaña completa |

## Supervivientes

Ninguno: cada mutación aplicada la cazó al menos un test.

