<!-- progress/impl_F-002.md -->
# F-002 · Informe de implementación

Feature `critico`, SDD. Spec: `specs/F-002-porcentuales-a-ud/`. Rama
`feature/F-002-porcentuales-a-ud`, un commit por tarea. Cuatro rondas:

| Ronda | Tareas | Qué trajo |
|---|---|---|
| 1 | T1-T14, T16 | la pasada completa; aprobada por el reviewer |
| 2 | T17-T23, T25 | R9 reescrita: base reconstruida (`ICV260` salía 336,39) |
| 3 | T26-T31, T33 | decimales del precio del clon configurables |
| 4 | T34 | el defecto vuelve a **2**, sujeto por R6 ter |

**T15, T24 y T32 son MANUALES del humano y siguen PENDIENTES** (abajo).

## Rondas 3 y 4 · los decimales del precio del clon

**Ronda 3.** El humano comparó en Presto la salida y el original: el convertido
salía 381,64 € por encima. El desglose por capítulos mostró que en 8 de los 9
que difieren el convertido da justo lo que el `~C` del capítulo ya declaraba
—fidelidad recuperada, no error—, y quedaba C06: `C020615` cambiaba un céntimo
que, por su medición de 1.833,59 m², eran 18,34 €. Se hizo configurable el
redondeo (`Settings.porcentuales_decimales`, `PORCENTUALES_DECIMALES`, rango
2..6 por R6 bis, `--decimales` en el CLI) y se puso el defecto en 4.

**Ronda 4: ese defecto era el equivocado y se revierte a 2.** El contraste de
la ronda 3 se hacía contra un simulador de cálculo exacto. Contra el número que
de verdad viene de fuera —el precio que el `~C` de cada partida declara,
escrito por Presto al exportar— gana el 2. Medido aquí: `~D` con porcentual
cuyo importe sobre la SALIDA coincide con ese precio, fuera las cinco partidas
de precio puesto a mano.

| `d` | Siroco | laguna |
|---|---|---|
| **2** | **260/262 (99,2 %)** | **402/402 (100 %)** |
| 3 | 237/262 (90,5 %) | 321/402 (79,9 %) |
| 4 | 247/262 (94,3 %) | 322/402 (80,1 %) |
| 6 | 247/262 (94,3 %) | 320/402 (79,6 %) |

Presto redondea a céntimos **cada línea** del descompuesto: más precisión se
aleja de su resultado, no se acerca. Y `C020615`, el caso que motivó la ronda
3, declara **172,6** en su propio `~C` —`~C|C020615|M2|FACHADA VENTILADA
COMPOSITE|172.6|`—, que es justo lo que da `d = 2`; el 172,59 de `d = 4` es el
cálculo exacto, no el de Presto. No había error que corregir ahí.

De la ronda 3 **se queda todo lo demás**: el parámetro, el rango 2..6 con caída
al defecto y aviso (R6 bis), `--decimales`, el formateo sin ceros de relleno
(R7) y el uso del mismo `d` en la base de R9 y en el residuo de R9 bis. Subir
el valor sigue siendo una línea del `.env`.

## Fase RED de la ronda 4

**R6 ter, el test que decide** —
`python -m pytest tests/test_f002_invariante.py -k reproduce_el_precio -q --tb=short`,
con el defecto de la ronda 3 (4 decimales):

```
E   AssertionError: COSTE_250128_Siroco_Rv4mlo.bc3: 245/259 (94.6%); primeros
E   fallos: ['07.02.04: 40.9673 != 40.96', '32.02.04.36: 27.0029 != 27.01',
E   '32.03.09.07: 18630.8836 != 18630.89', '34.01.17: 1.4742 != 1.48', ...]
E   assert Decimal('0.9459459459459459459459459459') >= Decimal('0.98')

E   AssertionError: lagunamodificado16julio.bc3: 320/400 (80.0%); primeros
E   fallos: ['1038297: 5.3145 != 5.32', '1065284: 3.4055 != 3.4',
E   '1071686: 46.7862 != 46.78', '1085788: 21.4130 != 21.42', ...]
E   assert Decimal('0.8') >= Decimal('0.98')
2 failed, 57 deselected in 0.27s
```

Con el defecto en 2, los dos pasan. Es la clase de test que pedía el reviewer:
compara contra un número que no sale de nuestro cálculo, así que si alguien
vuelve a tocar el redondeo, lo caza. La RED de la ronda 3 —el `ImportError` de
`DECIMALES_POR_DEFECTO`, que entonces no existía— está en el commit `2f72b07`.

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
| `tests/test_f002_{porcentuales,invariante,pipeline}.py` | 143 tests |
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

| Padre | `P` | `.P0` + porcentuales (defecto `d = 2`) | suma |
|---|---|---|---|
| `ICV260` | 291,50 | 225,98 + 26,62 + 38,9 | **291,50** |
| `ICV270` | 369,50 | 286,45 + 33,74 + 49,31 | **369,50** |
| `31.04.03.01` | 1.100,00 | 1.073,17 + 26,83 | **1.100,00** |
| `32.03.04.32` | 1.117,65 | 955,26 + 162,39 | **1.117,65** |

Y **R6 ter**, la comprobación que no sale de nuestro cálculo: el importe de
cada `~D` con porcentual sobre la salida da el precio que su `~C` declara en el
99,2 % de Siroco y el 100 % de laguna, con umbral del 98 % y las cinco partidas
de precio a mano excluidas. Literales de Presto en los tests: `43.15` → 81,526;
`05.06.29` → −1,46 y total 71,54; `07.02.05` → 16,00 con el clon a `0`;
`1000080` (la captura de Elena Díaz) → 1,20, el precio que enseña su `~C`.

## Decisiones y desviaciones

1. **`decimales_saneados` vive en la pasada, no en `Settings`.** T28 pedía
   sanear en `Settings`; el rango de R6 bis se aplica en `bc3_porcentajes`,
   que es quien redondea, para no meter una regla de dominio en `config/` ni
   que `config` importe `infrastructure`. `Settings` lee el `.env` tal cual y
   la pasada lo acota: el efecto observable de R6 bis es el mismo desde
   cualquier entrada (step, CLI o llamada directa).
2. **R7 recorta los ceros de relleno**, así que `38.90` sale `38.9` y
   `1100.00` sale `1100`. Los tests que la ronda 3 pasó a 4 decimales han
   vuelto a sus números, y los que quieren el encadenado exacto piden
   `decimales=4` explícito en vez de depender del defecto.
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
- **T32**: anotar **cuántos decimales acepta Presto** al importar. Ya no decide
  el valor —lo decide R6 ter con datos—, pero si algún día interesa subirlo es
  una línea del `.env`.

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
