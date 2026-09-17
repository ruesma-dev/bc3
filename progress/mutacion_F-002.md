<!-- progress/mutacion_F-002.md -->
# F-002 · Campaña de mutación

Generado por `python -m harness.mutacion --feature F-002 --workers 4` el 2026-09-17 15:53.

## Alcance

Origen del diff: **rama** (`75f805e6806ea04ffe092000b50313b4c2d36e0e` .. `feature/F-002-porcentuales-a-ud`).

| Fichero | Líneas en alcance |
|---|---|
| `application/pipeline/pipeline.py` | 2 |
| `application/pipeline/steps.py` | 41 |
| `config/settings.py` | 3 |
| `infrastructure/bc3/bc3_modifier.py` | 10 |
| `infrastructure/bc3/bc3_porcentajes.py` | 567 |
| `interface_adapters/cli/__init__.py` | 2 |
| `interface_adapters/cli/porcentuales_cli.py` | 102 |
| `interface_adapters/controllers/etl_controller.py` | 22 |
| **Total** | **749** |

## Totales

| Métrica | Valor |
|---|---|
| Mutantes generados | 174 |
| Mutantes evaluados | 174 |
| Muertos | 112 |
| Supervivientes | 62 |
| Timeouts | 0 |
| Timeouts repasados en serie | 0: ningún mutante agotó el reloj |
| Sin veredicto (base rota) | 0 |
| Tiempo total | 1613.3 s |
| SHA de HEAD medido | `465165ed3c4ac0a6e89941e8b463ec3fa493ac76` |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_cszs8708/wk_0` | 70.9 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_cszs8708/wk_1` | 69.1 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_cszs8708/wk_2` | 73.9 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_cszs8708/wk_3` | 68.8 |
| Media por mutante evaluado (s) | 9.3 |
| Timeout efectivo por mutante (s) | 148 — derivado de la línea base × 2.0 |
| Suelo configurado (s) | 120 |
| Workers | 4 |
| Muestreo | no: campaña completa |

## Supervivientes

Cada superviviente es una línea que ningún test comprueba de verdad, o una mutación equivalente. Distinguirlo es trabajo del implementer: ningún análisis puede quedarse sin completar al cerrar la feature.

### 1. `application/pipeline/steps.py:46` [booleano]

- Original: `out_dir.mkdir(parents=True, exist_ok=True)`
- Mutado:   `out_dir.mkdir(parents=False, exist_ok=True)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 2. `infrastructure/bc3/bc3_modifier.py:36` [booleano]

- Original: `forzar_unicidad: bool = False) -> str:`
- Mutado:   `forzar_unicidad: bool = True) -> str:`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 3. `infrastructure/bc3/bc3_modifier.py:48` [comparacion]

- Original: `if len(code) <= MAX_CODE_LEN and not (forzar_unicidad and code in used):`
- Mutado:   `if len(code) < MAX_CODE_LEN and not (forzar_unicidad and code in used):`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 4. `infrastructure/bc3/bc3_modifier.py:48` [logico]

- Original: `if len(code) <= MAX_CODE_LEN and not (forzar_unicidad and code in used):`
- Mutado:   `if len(code) <= MAX_CODE_LEN and not (forzar_unicidad or code in used):`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 5. `infrastructure/bc3/bc3_porcentajes.py:57` [booleano]

- Original: `@dataclass(frozen=True)`
- Mutado:   `@dataclass(frozen=False)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 6. `infrastructure/bc3/bc3_porcentajes.py:82` [entero]

- Original: `rendimiento: Decimal | float = 0, importe: Decimal | float = 0) -> None:`
- Mutado:   `rendimiento: Decimal | float = 1, importe: Decimal | float = 0) -> None:`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 7. `infrastructure/bc3/bc3_porcentajes.py:82` [entero]

- Original: `rendimiento: Decimal | float = 0, importe: Decimal | float = 0) -> None:`
- Mutado:   `rendimiento: Decimal | float = 0, importe: Decimal | float = 1) -> None:`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 8. `infrastructure/bc3/bc3_porcentajes.py:161` [logico]

- Original: `if f is None or r is None:`
- Mutado:   `if f is None and r is None:`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 9. `infrastructure/bc3/bc3_porcentajes.py:162` [entero]

- Original: `return Decimal(0)`
- Mutado:   `return Decimal(1)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 10. `infrastructure/bc3/bc3_porcentajes.py:176` [logico]

- Original: `if f is None or r is None:`
- Mutado:   `if f is None and r is None:`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 11. `infrastructure/bc3/bc3_porcentajes.py:177` [entero]

- Original: `return Decimal(0)`
- Mutado:   `return Decimal(1)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 12. `infrastructure/bc3/bc3_porcentajes.py:223` [entero]

- Original: `for i in range(0, len(partes) - 2, 3)`
- Mutado:   `for i in range(0, len(partes) - 3, 3)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 13. `infrastructure/bc3/bc3_porcentajes.py:235` [booleano]

- Original: `@dataclass(frozen=True)`
- Mutado:   `@dataclass(frozen=False)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 14. `infrastructure/bc3/bc3_porcentajes.py:247` [booleano]

- Original: `@dataclass(frozen=True)`
- Mutado:   `@dataclass(frozen=False)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 15. `infrastructure/bc3/bc3_porcentajes.py:319` [entero]

- Original: `for codigo, factor, rendimiento in triples[: hasta + 1]:`
- Mutado:   `for codigo, factor, rendimiento in triples[: hasta + 2]:`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 16. `infrastructure/bc3/bc3_porcentajes.py:320` [logico]

- Original: `if a_decimal(factor) is None or a_decimal(rendimiento) is None:`
- Mutado:   `if a_decimal(factor) is None and a_decimal(rendimiento) is None:`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 17. `infrastructure/bc3/bc3_porcentajes.py:321` [booleano]

- Original: `return True`
- Mutado:   `return False`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 18. `infrastructure/bc3/bc3_porcentajes.py:343` [comparacion]

- Original: `codigo = campos[1] if len(campos) > 1 else ""`
- Mutado:   `codigo = campos[1] if len(campos) >= 1 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 19. `infrastructure/bc3/bc3_porcentajes.py:343` [entero]

- Original: `codigo = campos[1] if len(campos) > 1 else ""`
- Mutado:   `codigo = campos[1] if len(campos) > 2 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 20. `infrastructure/bc3/bc3_porcentajes.py:346` [comparacion]

- Original: `unidades[codigo] = campos[2] if len(campos) > 2 else ""`
- Mutado:   `unidades[codigo] = campos[2] if len(campos) >= 2 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 21. `infrastructure/bc3/bc3_porcentajes.py:346` [entero]

- Original: `unidades[codigo] = campos[2] if len(campos) > 2 else ""`
- Mutado:   `unidades[codigo] = campos[2] if len(campos) > 3 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 22. `infrastructure/bc3/bc3_porcentajes.py:347` [comparacion]

- Original: `resumenes[codigo] = campos[3] if len(campos) > 3 else ""`
- Mutado:   `resumenes[codigo] = campos[3] if len(campos) >= 3 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 23. `infrastructure/bc3/bc3_porcentajes.py:347` [entero]

- Original: `resumenes[codigo] = campos[3] if len(campos) > 3 else ""`
- Mutado:   `resumenes[codigo] = campos[3] if len(campos) > 4 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 24. `infrastructure/bc3/bc3_porcentajes.py:348` [comparacion]

- Original: `precios[codigo] = a_decimal(campos[4]) if len(campos) > 4 else None`
- Mutado:   `precios[codigo] = a_decimal(campos[4]) if len(campos) >= 4 else None`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 25. `infrastructure/bc3/bc3_porcentajes.py:348` [entero]

- Original: `precios[codigo] = a_decimal(campos[4]) if len(campos) > 4 else None`
- Mutado:   `precios[codigo] = a_decimal(campos[4]) if len(campos) > 5 else None`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 26. `infrastructure/bc3/bc3_porcentajes.py:349` [comparacion]

- Original: `fechas[codigo] = campos[5] if len(campos) > 5 else ""`
- Mutado:   `fechas[codigo] = campos[5] if len(campos) >= 5 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 27. `infrastructure/bc3/bc3_porcentajes.py:349` [entero]

- Original: `fechas[codigo] = campos[5] if len(campos) > 5 else ""`
- Mutado:   `fechas[codigo] = campos[5] if len(campos) > 6 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 28. `infrastructure/bc3/bc3_porcentajes.py:363` [comparacion]

- Original: `padre = campos[1] if len(campos) > 1 else ""`
- Mutado:   `padre = campos[1] if len(campos) >= 1 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 29. `infrastructure/bc3/bc3_porcentajes.py:363` [entero]

- Original: `padre = campos[1] if len(campos) > 1 else ""`
- Mutado:   `padre = campos[1] if len(campos) > 2 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 30. `infrastructure/bc3/bc3_porcentajes.py:364` [comparacion]

- Original: `triples = triples_de_cuerpo(campos[2] if len(campos) > 2 else "")`
- Mutado:   `triples = triples_de_cuerpo(campos[2] if len(campos) >= 2 else "")`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 31. `infrastructure/bc3/bc3_porcentajes.py:364` [entero]

- Original: `triples = triples_de_cuerpo(campos[2] if len(campos) > 2 else "")`
- Mutado:   `triples = triples_de_cuerpo(campos[2] if len(campos) > 3 else "")`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 32. `infrastructure/bc3/bc3_porcentajes.py:374` [entero]

- Original: `informe.anota(padre, primer_pct[0], MOTIVO_BASE_INDETERMINADA,`
- Mutado:   `informe.anota(padre, primer_pct[1], MOTIVO_BASE_INDETERMINADA,`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 33. `infrastructure/bc3/bc3_porcentajes.py:375` [logico]

- Original: `a_decimal(primer_pct[2]) or 0, 0)`
- Mutado:   `a_decimal(primer_pct[2]) and 0, 0)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 34. `infrastructure/bc3/bc3_porcentajes.py:375` [entero]

- Original: `a_decimal(primer_pct[2]) or 0, 0)`
- Mutado:   `a_decimal(primer_pct[2]) or 1, 0)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 35. `infrastructure/bc3/bc3_porcentajes.py:375` [entero]

- Original: `a_decimal(primer_pct[2]) or 0, 0)`
- Mutado:   `a_decimal(primer_pct[2]) or 0, 1)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 36. `infrastructure/bc3/bc3_porcentajes.py:382` [comparacion]

- Original: `if precio_padre is None:`
- Mutado:   `if precio_padre is not None:`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 37. `infrastructure/bc3/bc3_porcentajes.py:384` [entero]

- Original: `if len(indices) == len(triples) and precio_padre not in (None, Decimal(0)):`
- Mutado:   `if len(indices) == len(triples) and precio_padre not in (None, Decimal(1)):`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 38. `infrastructure/bc3/bc3_porcentajes.py:388` [booleano]

- Original: `redondear=True, base_inicial=base_inicial)`
- Mutado:   `redondear=False, base_inicial=base_inicial)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 39. `infrastructure/bc3/bc3_porcentajes.py:392` [entero]

- Original: `a_decimal(primer_pct[2]) or 0, importes[indices[0]])`
- Mutado:   `a_decimal(primer_pct[2]) or 1, importes[indices[0]])`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 40. `infrastructure/bc3/bc3_porcentajes.py:394` [entero]

- Original: `padre, primer_pct[0], base_inicial)`
- Mutado:   `padre, primer_pct[1], base_inicial)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 41. `infrastructure/bc3/bc3_porcentajes.py:428` [entero]

- Original: `padre = campos[1] if len(campos) > 1 else ""`
- Mutado:   `padre = campos[2] if len(campos) > 1 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 42. `infrastructure/bc3/bc3_porcentajes.py:428` [comparacion]

- Original: `padre = campos[1] if len(campos) > 1 else ""`
- Mutado:   `padre = campos[1] if len(campos) >= 1 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 43. `infrastructure/bc3/bc3_porcentajes.py:428` [entero]

- Original: `padre = campos[1] if len(campos) > 1 else ""`
- Mutado:   `padre = campos[1] if len(campos) > 2 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 44. `infrastructure/bc3/bc3_porcentajes.py:429` [comparacion]

- Original: `triples = triples_de_cuerpo(campos[2] if len(campos) > 2 else "")`
- Mutado:   `triples = triples_de_cuerpo(campos[2] if len(campos) >= 2 else "")`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 45. `infrastructure/bc3/bc3_porcentajes.py:429` [entero]

- Original: `triples = triples_de_cuerpo(campos[2] if len(campos) > 2 else "")`
- Mutado:   `triples = triples_de_cuerpo(campos[2] if len(campos) > 3 else "")`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 46. `infrastructure/bc3/bc3_porcentajes.py:441` [comparacion]

- Original: `codigo = campos[1] if len(campos) > 1 else ""`
- Mutado:   `codigo = campos[1] if len(campos) >= 1 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 47. `infrastructure/bc3/bc3_porcentajes.py:441` [entero]

- Original: `codigo = campos[1] if len(campos) > 1 else ""`
- Mutado:   `codigo = campos[1] if len(campos) > 2 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 48. `infrastructure/bc3/bc3_porcentajes.py:449` [entero]

- Original: `MOTIVO_CONCEPTO_CONSERVADO, 0, 0)`
- Mutado:   `MOTIVO_CONCEPTO_CONSERVADO, 1, 0)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 49. `infrastructure/bc3/bc3_porcentajes.py:449` [entero]

- Original: `MOTIVO_CONCEPTO_CONSERVADO, 0, 0)`
- Mutado:   `MOTIVO_CONCEPTO_CONSERVADO, 0, 1)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 50. `infrastructure/bc3/bc3_porcentajes.py:480` [comparacion]

- Original: `triples = triples_de_cuerpo(campos[2] if len(campos) > 2 else "")`
- Mutado:   `triples = triples_de_cuerpo(campos[2] if len(campos) >= 2 else "")`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 51. `infrastructure/bc3/bc3_porcentajes.py:480` [entero]

- Original: `triples = triples_de_cuerpo(campos[2] if len(campos) > 2 else "")`
- Mutado:   `triples = triples_de_cuerpo(campos[2] if len(campos) > 3 else "")`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 52. `infrastructure/bc3/bc3_porcentajes.py:493` [comparacion]

- Original: `if len(campos) < 2:`
- Mutado:   `if len(campos) <= 2:`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 53. `infrastructure/bc3/bc3_porcentajes.py:493` [entero]

- Original: `if len(campos) < 2:`
- Mutado:   `if len(campos) < 3:`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 54. `infrastructure/bc3/bc3_porcentajes.py:521` [booleano]

- Original: `dst.parent.mkdir(parents=True, exist_ok=True)`
- Mutado:   `dst.parent.mkdir(parents=False, exist_ok=True)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 55. `infrastructure/bc3/bc3_porcentajes.py:531` [booleano]

- Original: `borrando = False  # dentro de un registro multilínea que se está eliminando`
- Mutado:   `borrando = True  # dentro de un registro multilínea que se está eliminando`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 56. `infrastructure/bc3/bc3_porcentajes.py:539` [booleano]

- Original: `borrando = False`
- Mutado:   `borrando = True`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 57. `infrastructure/bc3/bc3_porcentajes.py:542` [comparacion]

- Original: `codigo = campos[1] if len(campos) > 1 else ""`
- Mutado:   `codigo = campos[1] if len(campos) >= 1 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 58. `infrastructure/bc3/bc3_porcentajes.py:542` [entero]

- Original: `codigo = campos[1] if len(campos) > 1 else ""`
- Mutado:   `codigo = campos[1] if len(campos) > 2 else ""`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 59. `infrastructure/bc3/bc3_porcentajes.py:560` [booleano]

- Original: `dst.parent.mkdir(parents=True, exist_ok=True)`
- Mutado:   `dst.parent.mkdir(parents=False, exist_ok=True)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 60. `interface_adapters/cli/porcentuales_cli.py:39` [booleano]

- Original: `destino.parent.mkdir(parents=True, exist_ok=True)`
- Mutado:   `destino.parent.mkdir(parents=False, exist_ok=True)`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 61. `interface_adapters/controllers/etl_controller.py:33` [booleano]

- Original: `show_tree: bool = True,`
- Mutado:   `show_tree: bool = False,`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

### 62. `interface_adapters/controllers/etl_controller.py:34` [booleano]

- Original: `export_csv: bool = True) -> Pipeline:`
- Mutado:   `export_csv: bool = False) -> Pipeline:`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

