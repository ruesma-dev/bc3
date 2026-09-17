<!-- progress/mutacion_F-002.md -->
# F-002 · Campaña de mutación

Generado por `python -m harness.mutacion --feature F-002 --workers 4` el 2026-09-17 18:31.

## Alcance

Origen del diff: **rama** (`75f805e6806ea04ffe092000b50313b4c2d36e0e` .. `feature/F-002-porcentuales-a-ud`).

| Fichero | Líneas en alcance |
|---|---|
| `application/pipeline/pipeline.py` | 2 |
| `application/pipeline/steps.py` | 41 |
| `config/settings.py` | 3 |
| `infrastructure/bc3/bc3_modifier.py` | 10 |
| `infrastructure/bc3/bc3_porcentajes.py` | 635 |
| `interface_adapters/cli/__init__.py` | 2 |
| `interface_adapters/cli/porcentuales_cli.py` | 102 |
| `interface_adapters/controllers/etl_controller.py` | 22 |
| **Total** | **817** |

## Totales

| Métrica | Valor |
|---|---|
| Mutantes generados | 168 |
| Mutantes evaluados | 168 |
| Muertos | 168 |
| Supervivientes | 0 |
| Timeouts | 0 |
| Timeouts repasados en serie | 0: ningún mutante agotó el reloj |
| Sin veredicto (base rota) | 0 |
| Tiempo total | 305.5 s |
| SHA de HEAD medido | `8c084323746961cf492e85ba9fb0ea71152a9c35` |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002__0yatir9/wk_0` | 40.4 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002__0yatir9/wk_1` | 41.3 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002__0yatir9/wk_2` | 40.5 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002__0yatir9/wk_3` | 41.3 |
| Media por mutante evaluado (s) | 1.8 |
| Timeout efectivo por mutante (s) | 120 — derivado de la línea base × 2.0 |
| Suelo configurado (s) | 120 |
| Workers | 4 |
| Muestreo | no: campaña completa |

## Supervivientes

Ninguno: cada mutación aplicada la cazó al menos un test.

