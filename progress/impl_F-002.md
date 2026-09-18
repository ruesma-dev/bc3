<!-- progress/impl_F-002.md -->
# F-002 · Informe de implementación

Feature `critico`, SDD. Spec: `specs/F-002-porcentuales-a-ud/`. Rama
`feature/F-002-porcentuales-a-ud`, un commit por tarea. Cinco rondas:

| Ronda | Tareas | Qué trajo |
|---|---|---|
| 1 | T1-T14, T16 | la pasada completa; aprobada por el reviewer |
| 2 | T17-T23, T25 | R9 reescrita: base reconstruida (`ICV260` salía 336,39) |
| 3 | T26-T31, T33 | decimales del precio del clon configurables |
| 4 | T34 | el defecto vuelve a **2**, sujeto por R6 ter |
| 5 | T35-T43, T45 | limpieza del texto: Sigrid no importa lo no-ASCII |

**T15, T24, T32 y T44 son MANUALES del humano y siguen PENDIENTES** (abajo).

## Rondas 3 y 4 · los decimales del precio del clon

La ronda 3 hizo configurable el redondeo (`Settings.porcentuales_decimales`,
`PORCENTUALES_DECIMALES`, rango 2..6 por R6 bis, `--decimales` en el CLI) y puso
el defecto en 4, contrastando contra un cálculo exacto. **La ronda 4 revierte
ese defecto a 2**: contra el número que viene de fuera —el precio que el `~C` de
cada partida declara, escrito por Presto al exportar— gana el 2. `~D` con
porcentual cuyo importe sobre la SALIDA coincide con ese precio, fuera las cinco
partidas de precio puesto a mano:

| `d` | Siroco | laguna |
|---|---|---|
| **2** | **260/262 (99,2 %)** | **402/402 (100 %)** |
| 3 | 237/262 (90,5 %) | 321/402 (79,9 %) |
| 4 | 247/262 (94,3 %) | 322/402 (80,1 %) |
| 6 | 247/262 (94,3 %) | 320/402 (79,6 %) |

Presto redondea a céntimos **cada línea**: más precisión se aleja de su
resultado. `C020615`, el caso que motivó la ronda 3, declara **172,6** en su
`~C`, que es lo que da `d = 2`; el 172,59 de `d = 4` es el cálculo exacto, no
el de Presto. De la ronda 3 se queda todo lo demás; subirlo es una línea del
`.env`.

**Fase RED de la ronda 4**, `pytest tests/test_f002_invariante.py -k
reproduce_el_precio -q --tb=short` con el defecto de la ronda 3 (4 decimales):

```
E   AssertionError: COSTE_250128_Siroco_Rv4mlo.bc3: 245/259 (94.6%); primeros
E   fallos: ['07.02.04: 40.9673 != 40.96', '32.02.04.36: 27.0029 != 27.01', ...]
E   AssertionError: lagunamodificado16julio.bc3: 320/400 (80.0%)
2 failed, 57 deselected in 0.27s
```

Con el defecto en 2 los dos pasan: comparan contra un número que no sale de
nuestro cálculo. La RED de la ronda 3 está en el commit `2f72b07`.

## Ronda 5 · el texto que Sigrid no se tragaba (R24-R26)

Elena importó nuestra salida en Sigrid y cuatro partidas entraron **sin
descompuesto**: `VALV1`, `VALV4`, `VALV5` y `VALV6` («Válvula de bola, ½"»,
`1¼"`, `1½"`, `2"`). `VALV2` («3/4"»), del mismo capítulo, entró bien: las
distingue el no-ASCII del resumen. En `input/` hay 984 conceptos con
descompuesto propio afectados, 173 en el fichero de Elena.

La pasada limpia ahora el resumen de cada `~C`, el texto de cada `~T` —líneas
de continuación incluidas— y el resumen de clones y líneas de base, con el
`clean_text` que ya usa `convert_to_material`, no con una limpieza nueva.
Bandera `porcentuales_limpiar_texto` (`PORCENTUALES_LIMPIAR_TEXTO`, por defecto
true) y `--sin-limpiar-texto` en el CLI. Códigos, precios, factores,
rendimientos y unidades no se tocan; con la bandera apagada, byte a byte.

**Hallazgo, y cambia código compartido**: `clean_text` prometía ASCII y no lo
cumplía. NFKD no descompone las letras con trazo ni las ligadas (Ø, ø, æ, ß) y
pasa el signo micro µ a la mu griega μ; todas son `isalnum()`, así que el
`or ch.isalnum()` del filtro las dejaba entrar. Se ve en `input/`: los ficheros
`_limpio`, ya pasados por el ETL de hoy, conservan 4 conceptos sucios, los del
`Ø`. Arreglado **donde vive esa limpieza, que es uno solo**: se transliteran
(Ø→O, µ→u, ß→ss, æ→ae) y el filtro se queda en pertenecer al ASCII imprimible,
lo que su docstring ya prometía. **También cambia la salida de
`convert_to_material`**: la lee el mismo Sigrid.

**Fase RED**, `pytest tests/test_f002_porcentuales.py -k "r24_ or r18_ or r25_"
-q --tb=line` contra el código de entonces:

```
E   TypeError: convertir_porcentuales() got an unexpected keyword argument 'limpiar_texto'
6 failed, 4 passed, 74 deselected, 1 warning in 1.40s
```

R25 se barre sobre los diez `.bc3` de `input/`: tras la pasada no queda un
no-ASCII en el resumen de ningún concepto con descompuesto propio, y los cuatro
`VALV` se comprueban por su texto exacto (`Valvula de bola, 12"`, `114"`…).
R26 lo cierra: R6 ter, R19 y R19 bis se exigen con la bandera **encendida y
apagada**, y ningún importe se mueve.

## Fase RED de las rondas 1 y 2 (resumida)

Trazas completas en los commits `e48d697` (ronda 1) y `6224834` (ronda 2). La
ronda 1 falló por lo que aún no existía (`ModuleNotFoundError` del módulo,
`ImportError` de `codigo_de_clon`, de `convertir_porcentuales` y del step,
`AttributeError` de `porcentuales_a_ud`). De la ronda 2, la que importa es
**T20** —`AssertionError: ['ICV260: 336.39 != 291.50', 'ICV270: 426.40 !=
369.50']`—, que cazó el defecto de la R9 vieja tras una implementación y una
revisión completas; T18/T19 añadieron el `.P0` que no existía, el residuo que
daba `10.00` en vez de `1.31` y el descuento del −100 % que se convertía igual.

## Qué hace la pasada, en una pantalla

`.bc3` → `.bc3` independiente, antes de `convert_to_material`. Sustituye cada
tripleta porcentual por `clon\1\1` y emite un `~C` propio del par (padre,
línea) con unidad `UD`, tipo `3` y precio = el importe que calcula Presto
(`rendimiento × acumulado de las anteriores`). Si **todas** las líneas de un
`~D` son porcentuales y el padre trae precio `P` ≠ 0, reconstruye la base
(`P / Π(1 + r_i)`), la escribe como `<padre>.P0` delante y absorbe en ella el
residuo (R9, R9 bis); si algún `(1 + r_i)` ≤ 0, no lo toca (R9 ter).

| Fichero | Qué |
|---|---|
| `infrastructure/bc3/bc3_porcentajes.py` | **nuevo**: detección, cálculo en `Decimal`, `planificar` y `convertir_porcentuales` |
| `interface_adapters/cli/porcentuales_cli.py` + `__init__.py` | **nuevos**: fichero suelto (R22), `--decimales`, informe CSV (R20) |
| `infrastructure/bc3/bc3_modifier.py` | **un solo cambio**: keyword `forzar_unicidad=False` |
| `application/pipeline/{pipeline,steps}.py` + `controllers/etl_controller.py` | `preprocessed_path`, `ConvertirPorcentualesStep` y `construir_pipeline()` con el paso condicionado a la bandera |
| `config/settings.py` | las tres banderas: `a_ud`, `decimales`, `limpiar_texto` |
| `utils/text_sanitize.py` | **compartido**: `clean_text` ya devuelve ASCII de verdad |
| `tests/test_f002_{porcentuales,invariante,pipeline}.py` | 191 tests |
| `tests/fixtures/f002_*.bc3` | 10 fixtures con números reales de `input/` |

No se tocan ni `build_tree_service`, ni los clones `.1`, ni la FASE 2, ni
`input/` (solo lectura; los tests escriben en `tmp_path`). De
`convert_to_material` no se cambia una línea, pero su salida sí cambia: comparte
`clean_text` (ronda 5).

## Lo que se verificó con números reales

R19 compara **entrada contra salida** `~D` a `~D` sobre los diez `.bc3` de
`input/`, con `d = 2` y `d = 4` y la tolerancia calculada con `d`
(`0,01 + n × 10^(−d) / 2`): al subir la precisión el invariante aprieta cien
veces más en vez de quedarse flojo. 1.602 `~D` por cada `d`, ni uno fuera. Los
de R9 van por R19 bis: su salida vuelve a dar el precio del `~C` del padre.

Los cuatro reales cuadran al céntimo: `ICV260` 225,98 + 26,62 + 38,9 = 291,50;
`ICV270` 286,45 + 33,74 + 49,31 = 369,50; `31.04.03.01` 1.073,17 + 26,83 =
1.100,00; `32.03.04.32` 955,26 + 162,39 = 1.117,65.

Y **R6 ter**, la comprobación que no sale de nuestro cálculo: el importe de
cada `~D` con porcentual sobre la salida da el precio que su `~C` declara en el
99,2 % de Siroco y el 100 % de laguna (umbral 98 %, fuera las cinco de precio a
mano). Literales de Presto en los tests: `43.15` → 81,526; `05.06.29` → −1,46;
`07.02.05` → 16,00 con el clon a `0`; `1000080` (la captura de Elena) → 1,20.

## Decisiones y desviaciones

1. **`decimales_saneados` vive en la pasada, no en `Settings`.** T28 pedía
   sanear en `Settings`; el rango de R6 bis se aplica en `bc3_porcentajes`,
   que es quien redondea, para no meter una regla de dominio en `config/`. El
   efecto observable es el mismo desde el step, el CLI o la llamada directa.
2. **R7 recorta los ceros de relleno**: `38.90` sale `38.9` y `1100.00` sale
   `1100`. Los tests que quieren el encadenado exacto piden `decimales=4`
   explícito en vez de depender del defecto.
3. **Código del padre con marca de capítulo**: el clon se construye sobre el
   código sin el `#` (`~D|33.03.01#|` → `33.03.01.P1`) y el `~M` casa por ese
   mismo código. Solo se quita **una** marca; no hay ningún `##` en `input/`.
4. **`presupuesto.bc3` pierde 42 `~C` porcentuales sin convertir nada**: es un
   banco de precios cuyos `%` no usa ningún `~D` y R13 manda borrarlos.
   Decisión de negocio del humano.
5. **Guardas y código muertos eliminados**: los levantó la mutación como
   mutantes equivalentes (ver §Supervivientes).
6. **`clean_text` se arregla en su sitio, no se duplica** (ronda 5): el precio
   es que cambia también la salida de `convert_to_material`.

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
  el valor —lo decide R6 ter con datos—; subirlo es una línea del `.env`.
- **T44** (ronda 5): importar `laguna_sin_pct.bc3` **en Sigrid** y confirmar que
  `VALV1`, `VALV4`, `VALV5` y `VALV6` entran ya **con su descompuesto**.

**Resultado de las cuatro: PENDIENTE** — nadie las ha ejecutado; quedan
anotadas en `progress/current.md`.

## Evidencias

| Evidencia | Valor |
|---|---|
| Tests ejecutados | **538 pasan, 1 skip** (`python -m pytest tests -q`); **191** son de F-002 |
| Cobertura de las líneas cambiadas | **98,9 %** (449/454, umbral 80 %, nivel `critico`) |
| Mutantes / supervivientes | **198 generados, 198 muertos, 0 supervivientes**, 0 timeouts, campaña completa sin muestreo (446,7 s, alcance 981 líneas) → `progress/mutacion_F-002.md` |
| Tiempo de ejecución de la suite | **149,7 s** con medición de cobertura en `init.sh` (70 s sin ella) |
| `bash harness/init.sh` | **ENTORNO LISTO**, exit code 0 (tras cerrar T43) |

### Supervivientes: cómo se llegó al cero

Nueve campañas (174 mutantes y 62 supervivientes la primera). Cada uno se
cerró con un test o quitando el código que lo generaba, ninguno «a mano»:

1. **Registros truncados y números ilegibles** (21): faltaba el `~C`/`~D` con
   exactamente N campos y el contrato de `_shorten_code_unique`.
2. **Filas del informe** (8): se miraba el motivo, no el padre, el código, el
   rendimiento ni el importe. **Redondeo acumulado** (1), **plan inmutable**
   (4), **`~T` multilínea** (2) y **carpetas de salida** (4 `parents=True`).
3. **Orden de la familia de clones** (1): `insert(0, base)` frente a
   `insert(1, base)` no lo distinguía nadie; ahora es `base + clones` y un test
   fija que `.P0` va antes que `.P1`.
4. **Rondas 3 y 4** (2): `Decimal(1).scaleb(-d)` daba igual con `Decimal(2)`
   porque `quantize` solo mira el exponente, y el `--decimales` del CLI no lo
   probaba nadie.
5. **Ronda 5** (4): el `~C` truncado a cuatro campos y la línea suelta anterior
   al primer registro —que no es continuación de ningún `~T` y sale con sus
   acentos— pedían test; los otros dos eran el `isalnum()`/`isspace()` del
   filtro de `clean_text`, redundantes con el conjunto ASCII y borrados.
6. **Código muerto** (≈17 equivalentes): `campos[1]` existe siempre tras
   `~C|`/`~D|`/`~T|`/`~M|`; los defectos de `InformePorcentuales.anota` y de
   `construir_pipeline` no los usaba nadie; `hay_porcentual()` tampoco; la
   línea de base fingía un índice de tripleta y pasó a ser `LineaBase`. Un
   mutante equivalente es código que sobra.
