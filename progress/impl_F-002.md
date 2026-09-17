<!-- progress/impl_F-002.md -->
# F-002 · Informe de implementación

Feature `critico`, SDD. Spec: `specs/F-002-porcentuales-a-ud/`. Rama
`feature/F-002-porcentuales-a-ud`, un commit por tarea. Tres rondas:

| Ronda | Tareas | Qué trajo |
|---|---|---|
| 1 | T1-T14, T16 | la pasada completa; aprobada por el reviewer |
| 2 | T17-T23, T25 | R9 reescrita: base reconstruida (`ICV260` salía 336,39) |
| 3 | T26-T31, T33 | decimales del precio del clon configurables, 4 por defecto |

**T15, T24 y T32 son MANUALES del humano y siguen PENDIENTES** (abajo).

## Ronda 3 · los decimales del precio del clon

**El defecto.** El humano importó en Presto la salida y el original y los
comparó: el convertido salía **381,64 € por encima** (19.542.981,39 frente a
19.542.599,75). El líder lo desglosó por capítulos: en 8 de los 9 que difieren
el convertido da justo el precio que el `~C` del capítulo ya declaraba, o sea
que ahí la conversión recupera fidelidad. El que sí era nuestro es C06, y su
causa es el redondeo: **`C020615` pasaba de 172,59 a 172,60**, un céntimo que,
multiplicado por su medición de 1.833,59 m², son **18,34 €**.

**La regla nueva** (R6): los decimales del precio del clon los fija
`Settings.porcentuales_decimales` (`PORCENTUALES_DECIMALES` del `.env`),
**4 por defecto**, y el mismo `d` se usa en el precio del clon, en la base
reconstruida de R9 y en el residuo de R9 bis. R6 bis acota el rango a **2..6**
—2 es lo que traen los `~C` originales, 6 donde el desvío ya es cero— y
cualquier otra cosa cae a 4 con aviso por log. R7 escribe el número sin ceros
de relleno: un 7,41 exacto sale `7.41`, no `7.4100`.

`C020615` de `lagunamodificado16julio.bc3`, que es el caso del que salió todo,
está fijado como test parametrizado:

| `d` | precio que da la salida | |
|---|---|---|
| 2 | **172,60** | un céntimo de más → 18,34 € en esa partida |
| 4 | **172,59** | el del fichero original |

Desvío del **precio unitario** sobre `input/`, medido aquí `~D` a `~D` (sin
ponderar por la medición `~M`, que es lo que hace la tabla de `design.md`):

| `d` | Siroco (neto / absoluto) | laguna (neto / absoluto) |
|---|---|---|
| 2 | +0,0296 € / 0,7242 € | +0,1708 € / 1,2565 € |
| **4** | **+0,0043 € / 0,0050 €** | **+0,0022 € / 0,0096 €** |
| 6 | 0 / 0 | −0,0000 € / 0,0001 € |

El neto no se compensa porque `ROUND_HALF_UP` empuja siempre al alza: el error
tiene sesgo, no es ruido. **Dónde se cambia si Presto admite 6 decimales**:
`PORCENTUALES_DECIMALES=6` en el `.env`, o `--decimales 6` en el CLI. Ni una
línea de código.

## Fase RED de la ronda 3

**T26/T27** — `python -m pytest tests/test_f002_porcentuales.py -k "r6_ or r6bis or r7_" -q --tb=short`

```
tests\test_f002_porcentuales.py:24: in <module>
    from infrastructure.bc3.bc3_porcentajes import (
E   ImportError: cannot import name 'DECIMALES_POR_DEFECTO' from
    'infrastructure.bc3.bc3_porcentajes'
1 error in 0.45s
```

Con el código de la ronda 2 el redondeo estaba clavado a 2 decimales: no había
ni `DECIMALES_POR_DEFECTO`, ni `decimales_saneados`, ni forma de pedir 4. El
mismo test, ya en verde, es el que fija los 172,59 / 172,60 de `C020615`.

## Fase RED de las rondas 1 y 2 (resumida)

Trazas completas en los commits `e48d697` (ronda 1) y `6224834` (ronda 2):

| Tarea | Fallo real contra el código de entonces |
|---|---|
| T2 | `ModuleNotFoundError: No module named 'infrastructure.bc3.bc3_porcentajes'` |
| T4 / T6 | `ImportError: cannot import name 'codigo_de_clon'` / `'convertir_porcentuales'` |
| T10 / T11 | `AttributeError: 'Settings' object has no attribute 'porcentuales_a_ud'` / `ImportError: ConvertirPorcentualesStep` |
| **T20** | `AssertionError: ['ICV260: 336.39 != 291.50', 'ICV270: 426.40 != 369.50']` |
| T18 / T19 | no existía `~C\|31.04.03.01.P0\|`; el residuo daba `10.00` en vez de `1.31`; un descuento del −100 % se convertía igual (2 líneas en vez de 0) |

T20 es el que cazó el defecto de la R9 vieja, que sobrevivió a una
implementación y a una revisión completas.

## Qué hace la pasada, en una pantalla

`.bc3` → `.bc3` independiente, antes de `convert_to_material`. Sustituye cada
tripleta porcentual por `clon\1\1` y emite un `~C` propio del par (padre,
línea) con unidad `UD`, tipo `3` y precio = el importe que calcula Presto
(`rendimiento × acumulado de las líneas anteriores`). Cuando **todas** las
líneas de un `~D` son porcentuales y el padre trae precio `P` ≠ 0, reconstruye
la base (`P / Π(1 + r_i)`), la escribe como `<padre>.P0` delante y absorbe en
ella el residuo, de modo que la suma del `~D` es exactamente `P` (R9, R9 bis);
si algún `(1 + r_i)` ≤ 0, no toca el `~D` (R9 ter).

| Fichero | Qué |
|---|---|
| `infrastructure/bc3/bc3_porcentajes.py` | **nuevo**: detección, cálculo en `Decimal`, `planificar` y `convertir_porcentuales` |
| `interface_adapters/cli/porcentuales_cli.py` + `__init__.py` | **nuevos**: fichero suelto (R22), `--decimales`, informe CSV (R20) |
| `infrastructure/bc3/bc3_modifier.py` | **un solo cambio**: keyword `forzar_unicidad=False` |
| `application/pipeline/{pipeline,steps}.py` | `preprocessed_path`, `ConvertirPorcentualesStep` |
| `interface_adapters/controllers/etl_controller.py` | `construir_pipeline()` con el step condicionado a la bandera |
| `config/settings.py` | `porcentuales_a_ud` y `porcentuales_decimales` |
| `tests/test_f002_{porcentuales,invariante,pipeline}.py` | 141 tests |
| `tests/fixtures/f002_*.bc3` | 9 fixtures con números reales de `input/` |

No se toca: `convert_to_material`, `build_tree_service`, los clones `.1`, la
FASE 2 ni `input/` (solo lectura; los tests escriben en `tmp_path`).

## Lo que se verificó con números reales

R19 compara **entrada contra salida** `~D` a `~D` sobre los diez `.bc3` de
`input/`, **con `d = 2` y con `d = 4`**, y la tolerancia se calcula con `d`
(`0,01 + n × 10^(−d) / 2`): al subir la precisión el invariante aprieta cien
veces más en vez de quedarse flojo. 1.602 `~D` comparados por cada `d`, ni uno
fuera. Los `~D` de R9 van por R19 bis: su salida vuelve a dar el precio del
`~C` del padre con cualquier `d`.

| Padre | `P` | `.P0` + porcentuales con `d = 4` | suma |
|---|---|---|---|
| `ICV260` | 291,50 | 225,9792 + 26,6204 + 38,9004 | **291,50** |
| `ICV270` | 369,50 | 286,4471 + 33,7435 + 49,3094 | **369,50** |
| `31.04.03.01` | 1.100,00 | 1.073,1707 + 26,8293 | **1.100,00** |
| `32.03.04.32` | 1.117,65 | 955,2564 + 162,3936 | **1.117,65** |

Literales de Presto escritos a mano en los tests: `43.15` → 81,5364 con `d = 4`
(81,526 con `d = 2`, que era el error de la ronda 1); `05.06.29` → −1,46 y
total 71,54; `07.02.05` → 16,00 con el clon a `0`; `1000080` (la captura de
Elena Díaz) → 1,1997 con `d = 4` frente al exacto 1,199645 y al 1,20 que
enseña su `~C`.

## Decisiones y desviaciones

1. **`decimales_saneados` vive en la pasada, no en `Settings`.** T28 pedía
   sanear en `Settings`; el rango de R6 bis se aplica en `bc3_porcentajes`,
   que es quien redondea, para no meter una regla de dominio en `config/` ni
   que `config` importe `infrastructure`. `Settings` lee el `.env` tal cual y
   la pasada lo acota: el efecto observable de R6 bis es el mismo desde
   cualquier entrada (step, CLI o llamada directa).
2. **R7 estrena recorte de ceros**, así que precios que antes salían `38.90`
   ahora salen `38.9` y `1100.00` sale `1100`. Los tests de las rondas 1 y 2
   se actualizaron a eso; donde el literal documentado era el de 2 decimales
   (`43.15`, el caso del residuo, `1000080`) se fija `decimales=2` explícito
   en vez de cambiar el número.
3. **Código del padre con marca de capítulo**: el clon se construye sobre el
   código sin el `#` (`~D|33.03.01#|` → `33.03.01.P1`) y el `~M` casa por ese
   mismo código. Solo se quita **una** marca; un `##` dejaría `01#.P1` y no hay
   ningún caso en `input/`.
4. **`presupuesto.bc3` pierde 42 `~C` porcentuales sin convertir nada**: es un
   banco de precios cuyos `%` no usa ningún `~D` y R13 manda borrarlos. Queda
   como está, es decisión de negocio del humano.
5. **Guardas y código muertos eliminados**: los levantó la mutación como
   mutantes equivalentes (ver §Supervivientes).

## Pendiente · verificaciones MANUALES (humano)

```
python -m interface_adapters.cli.porcentuales_cli "input/COSTE_250128_Siroco_Rv4mlo.bc3" "output/siroco_sin_pct.bc3"
python -m interface_adapters.cli.porcentuales_cli "input/lagunamodificado16julio.bc3" "output/laguna_sin_pct.bc3"
```

- **T15**: importar `siroco_sin_pct.bc3` y confirmar el total y `43.15`,
  `05.06.29`, `31.04.03.01`, `32.03.04.32`.
- **T24**: importar `laguna_sin_pct.bc3` y confirmar `ICV260` = 291,50 (no
  336,39) e `ICV270` = 369,50.
- **T32**: anotar **cuántos decimales acepta Presto** al importar. Si admite 6,
  `PORCENTUALES_DECIMALES=6` en el `.env` (o `--decimales 6`) y repetir.

**Resultado de las tres: PENDIENTE** — nadie las ha ejecutado; quedan anotadas
en `progress/current.md`.

## Evidencias

| Evidencia | Valor |
|---|---|
| Tests ejecutados | **488 pasan, 1 skip** (`python -m pytest tests -q`); **141** son de F-002 |
| Cobertura de las líneas cambiadas | **98,8 %** (408/413, umbral 80 %, nivel `critico`) |
| Mutantes / supervivientes | **177 generados, 177 muertos, 0 supervivientes**, 0 timeouts, campaña completa sin muestreo (369,3 s, SHA `7cf5d9a` = HEAD, alcance 885 líneas) → `progress/mutacion_F-002.md` |
| Tiempo de ejecución de la suite | **48,3 s** en la última pasada de `init.sh` (los 141 de F-002, ~6 s) |
| `bash harness/init.sh` | **ENTORNO LISTO**, exit code 0 (última ejecución: tras cerrar T31) |

### Supervivientes: cómo se llegó al cero

Siete campañas en total (174 mutantes y 62 supervivientes la primera). Cada
superviviente se cerró con un test o quitando el código que lo generaba;
ninguno quedó justificado «a mano». Por familias:

1. **Contrato de `_shorten_code_unique`** (3), **números ilegibles** (6) y
   **`~C`/`~D` truncados** (12): faltaba el registro con exactamente N campos
   y el caso «uno de los dos números no se lee».
2. **Filas del informe** (8): se miraba el motivo pero no el padre, el código,
   el rendimiento ni el importe.
3. **Redondeo acumulado** (R6, 1), **inmutabilidad del plan** (4 `frozen=True`)
   y **`~T` multilínea y líneas sueltas** (2).
4. **Carpetas de salida** (4 `parents=True`): los tests creaban un solo nivel,
   donde `parents=False` también vale; ahora dos.
5. **Orden de la familia de clones** (1): `insert(0, base)` frente a
   `insert(1, base)` no lo distinguía nadie; ahora la lista se construye como
   `base + clones` y el test fija que `.P0` va antes que `.P1`.
6. **Ronda 3** (2): `Decimal(1).scaleb(-d)` daba igual con `Decimal(2)` porque
   `quantize` solo mira el exponente —se escribe `Decimal(f"1e-{d}")` y el
   literal desaparece—, y el `--decimales` del CLI no lo probaba nadie.
7. **Código muerto** (≈17 equivalentes): `campos[1]` existe siempre tras
   `~C|`/`~D|`/`~T|`/`~M|`; los valores por defecto de
   `InformePorcentuales.anota` y de `construir_pipeline` no los usaba nadie;
   `hay_porcentual()` tampoco; la línea de base fingía un índice de tripleta
   que nadie leía y pasó a ser `LineaBase`. Un mutante equivalente es código
   que sobra.
