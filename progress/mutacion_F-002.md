<!-- progress/mutacion_F-002.md -->
# F-002 · Campaña de mutación

Generado por `python -m harness.mutacion --feature F-002 --workers 4` el 2026-09-17 16:04.

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
| Muertos | 153 |
| Supervivientes | 3 |
| Timeouts | 0 |
| Timeouts repasados en serie | 0: ningún mutante agotó el reloj |
| Sin veredicto (base rota) | 0 |
| Tiempo total | 451.9 s |
| SHA de HEAD medido | `897d3e796c2454702aae9547a844a6e900b3ebb4` |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_yqyb1h7y/wk_0` | 84.5 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_yqyb1h7y/wk_1` | 79.5 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_yqyb1h7y/wk_2` | 79.3 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_yqyb1h7y/wk_3` | 81.1 |
| Media por mutante evaluado (s) | 2.9 |
| Timeout efectivo por mutante (s) | 169 — derivado de la línea base × 2.0 |
| Suelo configurado (s) | 120 |
| Workers | 4 |
| Muestreo | no: campaña completa |

## Supervivientes

Cada superviviente es una línea que ningún test comprueba de verdad, o una mutación equivalente. Distinguirlo es trabajo del implementer: ningún análisis puede quedarse sin completar al cerrar la feature.

### 1. `infrastructure/bc3/bc3_porcentajes.py:523` [booleano]

- Original: `dst.parent.mkdir(parents=True, exist_ok=True)`
- Mutado:   `dst.parent.mkdir(parents=False, exist_ok=True)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 2. `infrastructure/bc3/bc3_porcentajes.py:562` [booleano]

- Original: `dst.parent.mkdir(parents=True, exist_ok=True)`
- Mutado:   `dst.parent.mkdir(parents=False, exist_ok=True)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 3. `interface_adapters/cli/porcentuales_cli.py:39` [booleano]

- Original: `destino.parent.mkdir(parents=True, exist_ok=True)`
- Mutado:   `destino.parent.mkdir(parents=False, exist_ok=True)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

