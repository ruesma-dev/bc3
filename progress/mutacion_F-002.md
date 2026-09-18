<!-- progress/mutacion_F-002.md -->
# F-002 · Campaña de mutación

Generado por `python -m harness.mutacion --feature F-002 --workers 4` el 2026-09-18 10:10.

## Alcance

Origen del diff: **rama** (`75f805e6806ea04ffe092000b50313b4c2d36e0e` .. `feature/F-002-porcentuales-a-ud`).

| Fichero | Líneas en alcance |
|---|---|
| `application/pipeline/pipeline.py` | 2 |
| `application/pipeline/steps.py` | 44 |
| `config/settings.py` | 8 |
| `infrastructure/bc3/bc3_modifier.py` | 10 |
| `infrastructure/bc3/bc3_porcentajes.py` | 691 |
| `interface_adapters/cli/__init__.py` | 2 |
| `interface_adapters/cli/porcentuales_cli.py` | 109 |
| `interface_adapters/controllers/etl_controller.py` | 22 |
| **Total** | **888** |

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
| Tiempo total | 346.8 s |
| SHA de HEAD medido | `380ee7163791afa0221fc3dc94345de6413f08ab` |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_qxm49a5j/wk_0` | 61.2 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_qxm49a5j/wk_1` | 63.2 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_qxm49a5j/wk_2` | 61.4 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_qxm49a5j/wk_3` | 62.3 |
| Media por mutante evaluado (s) | 2.0 |
| Timeout efectivo por mutante (s) | 127 — derivado de la línea base × 2.0 |
| Suelo configurado (s) | 120 |
| Workers | 4 |
| Muestreo | no: campaña completa |

## Supervivientes

Ninguno: cada mutación aplicada la cazó al menos un test.

