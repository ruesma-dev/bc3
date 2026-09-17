<!-- progress/mutacion_F-002.md -->
# F-002 · Campaña de mutación

Generado por `python -m harness.mutacion --feature F-002 --workers 4` el 2026-09-17 17:58.

> ## ⚠ CAMPAÑA NO VÁLIDA
>
> La línea base estaba VERDE al empezar y ROJA al terminar en C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_2jruali8/wk_0 (código 3221225794). La base se rompió durante la campaña, así que los mutantes contados como «muertos» pueden no estarlo: estos números NO valen para cerrar una feature. Arregla la suite y repite la campaña.
>
> **No cierres la feature con estos números.** Arregla la línea base y repite la campaña.

## Alcance

Origen del diff: **rama** (`75f805e6806ea04ffe092000b50313b4c2d36e0e` .. `feature/F-002-porcentuales-a-ud`).

| Fichero | Líneas en alcance |
|---|---|
| `application/pipeline/pipeline.py` | 2 |
| `application/pipeline/steps.py` | 41 |
| `config/settings.py` | 3 |
| `infrastructure/bc3/bc3_modifier.py` | 10 |
| `infrastructure/bc3/bc3_porcentajes.py` | 620 |
| `interface_adapters/cli/__init__.py` | 2 |
| `interface_adapters/cli/porcentuales_cli.py` | 102 |
| `interface_adapters/controllers/etl_controller.py` | 22 |
| **Total** | **802** |

## Totales

| Métrica | Valor |
|---|---|
| Mutantes generados | 173 |
| Mutantes evaluados | 173 |
| Muertos | 125 |
| Supervivientes | 1 |
| Timeouts | 2 |
| Timeouts repasados en serie | 0: campaña en serie, el reloj ya midió a cada mutante a solas |
| Sin veredicto (base rota) | 45 |
| Tiempo total | 1536.7 s |
| SHA de HEAD medido | `9ceef73e1803d6a6e7e4a3b4b822cfea8124afd8` |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_2jruali8/wk_0` | 49.6 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_2jruali8/wk_1` | 49.6 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_2jruali8/wk_2` | 51.2 |
| Línea base (s) — `C:/Users/pgris/AppData/Local/Temp/mutacion_F-002_2jruali8/wk_3` | 51.2 |
| Media por mutante evaluado (s) | 8.9 |
| Timeout efectivo por mutante (s) | 120 — derivado de la línea base × 2.0 |
| Suelo configurado (s) | 120 |
| Workers | 4 |
| Muestreo | no: campaña completa |

## Supervivientes

Cada superviviente es una línea que ningún test comprueba de verdad, o una mutación equivalente. Distinguirlo es trabajo del implementer: ningún análisis puede quedarse sin completar al cerrar la feature.

### 1. `infrastructure/bc3/bc3_porcentajes.py:324` [logico]

- Original: `if f is None or r is None:`
- Mutado:   `if f is None and r is None:`

#### Análisis (PENDIENTE del implementer)

> Por qué ningún test lo caza: PENDIENTE.
> Decisión: ¿test nuevo o mutante equivalente justificado?

## Timeouts

Campaña sin repaso en serie: con un solo evaluador el reloj ya midió a cada mutante a solas, así que estos timeouts son suyos.

- `infrastructure/bc3/bc3_porcentajes.py:395` [entero] a_decimal(primer_pct[2]) or 0, 0) -> a_decimal(primer_pct[2]) or 0, 1)
- `infrastructure/bc3/bc3_porcentajes.py:411` [entero] a_decimal(primer_pct[2]) or 0, 0) -> a_decimal(primer_pct[2]) or 1, 0)

## Sin veredicto: la suite estaba rota por su cuenta

De estos mutantes no se sabe nada. La suite falló sin que la mutación tuviera que ver, así que **no** cuentan como muertos: esas líneas se quedan sin comprobar hasta que la base vuelva a estar verde y la campaña se repita.

- `infrastructure/bc3/bc3_porcentajes.py:405` infrastructure/bc3/bc3_porcentajes.py:405 [entero] and precio_padre not in (None, Decimal(0))) -> and precio_padre not in (None, Decimal(1)))
- `infrastructure/bc3/bc3_porcentajes.py:411` infrastructure/bc3/bc3_porcentajes.py:411 [logico] a_decimal(primer_pct[2]) or 0, 0) -> a_decimal(primer_pct[2]) and 0, 0)
- `infrastructure/bc3/bc3_porcentajes.py:422` infrastructure/bc3/bc3_porcentajes.py:422 [comparacion] if base_inicial is not None: -> if base_inicial is None:
- `infrastructure/bc3/bc3_porcentajes.py:424` infrastructure/bc3/bc3_porcentajes.py:424 [entero] indice=-1, -> indice=-2,
- `infrastructure/bc3/bc3_porcentajes.py:425` infrastructure/bc3/bc3_porcentajes.py:425 [entero] codigo=codigo_de_clon(padre, 0, ocupados), -> codigo=codigo_de_clon(padre, 1, ocupados),
- `infrastructure/bc3/bc3_porcentajes.py:433` infrastructure/bc3/bc3_porcentajes.py:433 [entero] for ordinal, indice in enumerate(indices, start=1): -> for ordinal, indice in enumerate(indices, start=2):
- `infrastructure/bc3/bc3_porcentajes.py:434` infrastructure/bc3/bc3_porcentajes.py:434 [entero] hijo = triples[indice][0] -> hijo = triples[indice][1]
- `infrastructure/bc3/bc3_porcentajes.py:442` infrastructure/bc3/bc3_porcentajes.py:442 [logico] resumen=resumenes.get(hijo) or hijo, -> resumen=resumenes.get(hijo) and hijo,
- `infrastructure/bc3/bc3_porcentajes.py:447` infrastructure/bc3/bc3_porcentajes.py:447 [entero] informe.lineas_convertidas += 1 -> informe.lineas_convertidas += 2
- `infrastructure/bc3/bc3_porcentajes.py:449` infrastructure/bc3/bc3_porcentajes.py:449 [comparacion] if linea_base is not None: -> if linea_base is None:
- `infrastructure/bc3/bc3_porcentajes.py:452` infrastructure/bc3/bc3_porcentajes.py:452 [aritmetico] resto = precio_padre - sum((c.precio for c in clones), Decimal(0)) -> resto = precio_padre + sum((c.precio for c in clones), Decimal(0))
- `infrastructure/bc3/bc3_porcentajes.py:473` infrastructure/bc3/bc3_porcentajes.py:473 [not] if not linea.startswith("~D|"): -> if linea.startswith("~D|"):
- `infrastructure/bc3/bc3_porcentajes.py:476` infrastructure/bc3/bc3_porcentajes.py:476 [entero] padre = campos[1]  # existe siempre: la línea empieza por "~D|" -> padre = campos[2]  # existe siempre: la línea empieza por "~D|"
- `infrastructure/bc3/bc3_porcentajes.py:477` infrastructure/bc3/bc3_porcentajes.py:477 [entero] triples = triples_de_cuerpo(campos[2] if len(campos) > 2 else "") -> triples = triples_de_cuerpo(campos[3] if len(campos) > 2 else "")
- `infrastructure/bc3/bc3_porcentajes.py:477` infrastructure/bc3/bc3_porcentajes.py:477 [entero] triples = triples_de_cuerpo(campos[2] if len(campos) > 2 else "") -> triples = triples_de_cuerpo(campos[2] if len(campos) > 3 else "")
- `infrastructure/bc3/bc3_porcentajes.py:486` infrastructure/bc3/bc3_porcentajes.py:486 [not] if not linea.startswith("~C|"): -> if linea.startswith("~C|"):
- `infrastructure/bc3/bc3_porcentajes.py:489` infrastructure/bc3/bc3_porcentajes.py:489 [entero] codigo = campos[1]  # existe siempre: la línea empieza por "~C|" -> codigo = campos[2]  # existe siempre: la línea empieza por "~C|"
- `infrastructure/bc3/bc3_porcentajes.py:497` infrastructure/bc3/bc3_porcentajes.py:497 [entero] MOTIVO_CONCEPTO_CONSERVADO, 0, 0) -> MOTIVO_CONCEPTO_CONSERVADO, 1, 0)
- `infrastructure/bc3/bc3_porcentajes.py:497` infrastructure/bc3/bc3_porcentajes.py:497 [entero] MOTIVO_CONCEPTO_CONSERVADO, 0, 0) -> MOTIVO_CONCEPTO_CONSERVADO, 0, 1)
- `infrastructure/bc3/bc3_porcentajes.py:529` infrastructure/bc3/bc3_porcentajes.py:529 [entero] triples = triples_de_cuerpo(campos[2]) -> triples = triples_de_cuerpo(campos[3])
- `infrastructure/bc3/bc3_porcentajes.py:537` infrastructure/bc3/bc3_porcentajes.py:537 [entero] tripletas.insert(0, f"{descompuesto.base.codigo}\\1\\1") -> tripletas.insert(1, f"{descompuesto.base.codigo}\\1\\1")
- `infrastructure/bc3/bc3_porcentajes.py:538` infrastructure/bc3/bc3_porcentajes.py:538 [entero] texto = _format_d_triplets(campos[1], tripletas) -> texto = _format_d_triplets(campos[2], tripletas)
- `infrastructure/bc3/bc3_porcentajes.py:539` infrastructure/bc3/bc3_porcentajes.py:539 [entero] return texto[:-1] + _terminador_de(linea) -> return texto[:-2] + _terminador_de(linea)
- `infrastructure/bc3/bc3_porcentajes.py:545` infrastructure/bc3/bc3_porcentajes.py:545 [entero] par = campos[1].split("\\") -> par = campos[2].split("\\")
- `infrastructure/bc3/bc3_porcentajes.py:546` infrastructure/bc3/bc3_porcentajes.py:546 [comparacion] if len(par) != 2: -> if len(par) == 2:
- `infrastructure/bc3/bc3_porcentajes.py:546` infrastructure/bc3/bc3_porcentajes.py:546 [entero] if len(par) != 2: -> if len(par) != 3:
- `infrastructure/bc3/bc3_porcentajes.py:552` infrastructure/bc3/bc3_porcentajes.py:552 [entero] campos[1] = f"{padre}\\{clon}" -> campos[2] = f"{padre}\\{clon}"
- `infrastructure/bc3/bc3_porcentajes.py:553` infrastructure/bc3/bc3_porcentajes.py:553 [aritmetico] return "|".join(campos) + _terminador_de(linea) -> return "|".join(campos) - _terminador_de(linea)
- `infrastructure/bc3/bc3_porcentajes.py:560` infrastructure/bc3/bc3_porcentajes.py:560 [booleano] activo: bool = True) -> InformePorcentuales: -> activo: bool = False) -> InformePorcentuales:
- `infrastructure/bc3/bc3_porcentajes.py:570` infrastructure/bc3/bc3_porcentajes.py:570 [not] if not activo: -> if activo:
- `infrastructure/bc3/bc3_porcentajes.py:571` infrastructure/bc3/bc3_porcentajes.py:571 [booleano] dst.parent.mkdir(parents=True, exist_ok=True) -> dst.parent.mkdir(parents=False, exist_ok=True)
- `infrastructure/bc3/bc3_porcentajes.py:571` infrastructure/bc3/bc3_porcentajes.py:571 [booleano] dst.parent.mkdir(parents=True, exist_ok=True) -> dst.parent.mkdir(parents=True, exist_ok=False)
- `infrastructure/bc3/bc3_porcentajes.py:583` infrastructure/bc3/bc3_porcentajes.py:583 [not] if not linea.startswith("~"): -> if linea.startswith("~"):
- `infrastructure/bc3/bc3_porcentajes.py:585` infrastructure/bc3/bc3_porcentajes.py:585 [not] if not borrando: -> if borrando:
- `infrastructure/bc3/bc3_porcentajes.py:589` infrastructure/bc3/bc3_porcentajes.py:589 [booleano] borrando = False -> borrando = True
- `infrastructure/bc3/bc3_porcentajes.py:595` infrastructure/bc3/bc3_porcentajes.py:595 [booleano] borrando = True -> borrando = False
- `infrastructure/bc3/bc3_porcentajes.py:601` infrastructure/bc3/bc3_porcentajes.py:601 [comparacion] if descompuesto.base is not None: -> if descompuesto.base is None:
- `infrastructure/bc3/bc3_porcentajes.py:602` infrastructure/bc3/bc3_porcentajes.py:602 [entero] emitidos.insert(0, descompuesto.base) -> emitidos.insert(1, descompuesto.base)
- `infrastructure/bc3/bc3_porcentajes.py:613` infrastructure/bc3/bc3_porcentajes.py:613 [booleano] dst.parent.mkdir(parents=True, exist_ok=True) -> dst.parent.mkdir(parents=True, exist_ok=False)
- `interface_adapters/cli/porcentuales_cli.py:39` interface_adapters/cli/porcentuales_cli.py:39 [booleano] destino.parent.mkdir(parents=True, exist_ok=True) -> destino.parent.mkdir(parents=False, exist_ok=True)
- `interface_adapters/cli/porcentuales_cli.py:39` interface_adapters/cli/porcentuales_cli.py:39 [booleano] destino.parent.mkdir(parents=True, exist_ok=True) -> destino.parent.mkdir(parents=True, exist_ok=False)
- `interface_adapters/cli/porcentuales_cli.py:80` interface_adapters/cli/porcentuales_cli.py:80 [not] activo = ajustes.porcentuales_a_ud and not args.sin_conversion -> activo = ajustes.porcentuales_a_ud and args.sin_conversion
- `interface_adapters/cli/porcentuales_cli.py:87` interface_adapters/cli/porcentuales_cli.py:87 [entero] return 1 -> return 2
- `interface_adapters/cli/porcentuales_cli.py:98` interface_adapters/cli/porcentuales_cli.py:98 [entero] return 0 -> return 1
- `interface_adapters/cli/porcentuales_cli.py:101` interface_adapters/cli/porcentuales_cli.py:101 [comparacion] if __name__ == "__main__":  # pragma: no cover -> if __name__ != "__main__":  # pragma: no cover

