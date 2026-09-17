<!-- progress/impl_F-002.md -->
# F-002 · Informe de implementación

Feature `critico`, SDD. Spec aprobada: `specs/F-002-porcentuales-a-ud/`.
Rama `feature/F-002-porcentuales-a-ud`, un commit por tarea.
**Ronda 1**: T1-T14, T16 (pasada completa, aprobada por el reviewer).
**Ronda 2**: T17-T23 y T25, la R9 reescrita tras el defecto que encontró el
líder sobre la salida real. **T15 y T24 son MANUALES del humano y siguen
PENDIENTES** (comandos abajo).

## Ronda 2 · la R9 nueva: base reconstruida

**El defecto.** La R9 anterior le daba al primer clon el precio `P` del `~C`
del padre y aplicaba los siguientes porcentajes encima. Pero `P` ya trae los
porcentajes dentro, así que con DOS líneas porcentuales se cobraban dos veces:
`ICV260` salía **336,39** con un `~C` de **291,50** (+15,4 %). Con una sola
línea la regla acertaba por casualidad, y por eso Siroco no lo delataba.

**La regla nueva** (R9): se despeja la base hacia atrás,
`base = P / Π(1 + r_i)`, y el `~D` pasa a tener una línea de base
`<padre>.P0` (unidad `UD`, factor 1, rendimiento 1, tipo 3, resumen y fecha
del padre) más una línea por cada porcentual, cada una con D4 sobre el
acumulado. R9 bis: el residuo de redondeo se absorbe **en la línea de base**
(`precio(.P0) = P − Σ importes porcentuales`), nunca retocando una porcentual.
R9 ter: si algún `(1 + r_i)` ≤ 0 la base no se puede despejar y el `~D` se
queda intacto, con motivo `base_no_despejable`.

Los cuatro `~D` solo-porcentuales reales, medidos sobre la salida de `input/`:

| Padre | `P` del `~C` | `.P0` | porcentuales | suma | antes |
|---|---|---|---|---|---|
| `ICV260` | 291,50 | 225,98 | 26,62 · 38,90 | **291,50** | 336,39 |
| `ICV270` | 369,50 | 286,45 | 33,74 · 49,31 | **369,50** | 426,40 |
| `31.04.03.01` | 1.100,00 | 1.073,17 | 26,83 | **1.100,00** | 1.100,00 |
| `32.03.04.32` | 1.117,65 | 955,26 | 162,39 | **1.117,65** | 1.117,65 |

Los dos de Siroco también cambian de forma aunque no de total: donde había un
clon de 1.100,00 ahora hay 1.073,17 + 26,83. Los tests que fijaban el valor
viejo **se han actualizado, no borrado**: cambia el número esperado porque
cambió la regla, y son los mismos tests los que ahora sujetan la nueva.

## Fase RED de la ronda 2

**T20 · R19 bis, el test que caza el defecto** —
`python -m pytest tests/test_f002_invariante.py -k r19bis -q --tb=short`

```
        if abs(calculado - esperado) > Decimal("0.01"):
            desviados.append(f"{padre}: {calculado} != {esperado}")
>       assert desviados == []
E       AssertionError: assert ['ICV260: 336...40 != 369.50'] == []
E         Full diff:
E         - []
E         + [
E         +     'ICV260: 336.39 != 291.50',
E         +     'ICV270: 426.40 != 369.50',
E         + ]
1 failed, 1 passed, 34 deselected in 0.27s
```

El `1 passed` es Siroco: con una sola porcentual la regla vieja daba el total
bueno. Por eso hacía falta un fichero con dos.

**T18 · la línea de base no existe** —
`python -m pytest tests/test_f002_porcentuales.py -k r9_ -q --tb=short`

```
    assert _registro(lineas, f"~C|{codigo}|").split("|")[4] == precio, codigo
E   AssertionError: no hay ninguna línea que empiece por '~C|31.04.03.01.P0|'
E   assert []
```

**T19 · R9 bis (residuo) y R9 ter (descuento total)** —
`python -m pytest tests/test_f002_porcentuales.py -k "r9bis or r9ter" -q --tb=short`

```
    assert _registro(lineas, "~C|09.21.01.P1|").split("|")[4] == "1.31"
E   AssertionError: assert '10.00' == '1.31'

    assert informe.lineas_convertidas == 0
E   AssertionError: assert 2 == 0
E    +  where 2 = InformePorcentuales(...,
E        casos=[CasoPorcentual(padre='09.20.01', codigo='%TODO',
E        motivo='precio_del_padre_aplicado', rendimiento=-1.0, importe=500.0), ...])
```

Las tres pasaron a verde con el commit de T21-T22.

## Fase RED de la ronda 1 (resumida)

Traza completa en el commit `e48d697`; aquí el fallo de cada una:

| Tarea | Comando | Fallo real |
|---|---|---|
| T2 | `pytest -k "r1_ or r2_ or r7_ or r11_"` | `ModuleNotFoundError: No module named 'infrastructure.bc3.bc3_porcentajes'` |
| T4 | `pytest -k r5_` | `ImportError: cannot import name 'codigo_de_clon'` |
| T6 | `pytest tests/test_f002_porcentuales.py` | `ImportError: cannot import name 'convertir_porcentuales'` |
| T10 | `pytest -k r21_` | `AttributeError: 'Settings' object has no attribute 'porcentuales_a_ud'` |
| T11 | `pytest tests/test_f002_pipeline.py` | `ImportError: cannot import name 'ConvertirPorcentualesStep'` |

Cada una pasó a verde con el commit siguiente (T3, T5, T7, T10 y T11).

## Qué cambió

| Fichero | Qué |
|---|---|
| `infrastructure/bc3/bc3_porcentajes.py` | **nuevo**. Detección (R1), cálculo en `Decimal` (R2, R6), `planificar` (pasada 1, con la base reconstruida de R9) y `convertir_porcentuales` (pasada 2) |
| `interface_adapters/cli/porcentuales_cli.py` + `__init__.py` | **nuevos**. Fichero suelto (R22) e informe CSV (R20) |
| `infrastructure/bc3/bc3_modifier.py` | **un solo cambio**: keyword `forzar_unicidad=False` en `_shorten_code_unique` |
| `application/pipeline/{pipeline,steps}.py` | `ETLContext.preprocessed_path`, `ConvertirPorcentualesStep` y `TransformBC3Step` sobre `ctx.preprocessed_path or ctx.original_path` |
| `interface_adapters/controllers/etl_controller.py` | `construir_pipeline()`: el step entra si la bandera está activa |
| `config/settings.py` | `porcentuales_a_ud` (`PORCENTUALES_A_UD`, activa por defecto) |
| `tests/test_f002_{porcentuales,invariante,pipeline}.py` | 110 tests |
| `tests/fixtures/f002_*.bc3` | 9 fixtures con números reales de `input/` |
| `docs/ARCHITECTURE.md`, `.gitignore` | el step nuevo en el pipeline; excepción para versionar las fixtures |

No se toca: `convert_to_material`, `build_tree_service`, los clones `.1`, la
FASE 2 ni `input/` (solo lectura; los tests escriben en `tmp_path`).

## Lo que se verificó con números reales

R19 se comprueba `~D` a `~D`, **entrada contra salida**, sobre los diez `.bc3`
de `input/`: **1.602 `~D` comparados, ni uno fuera de la tolerancia**
(`0,01 + 0,005 × nº de porcentuales`); peor desvío individual 0,0104 € en
`43.15`; desvío acumulado por fichero ≤ 0,171 €. Los `~D` de R9 no entran ahí
—su entrada calcula 0— y se les exige R19 bis: su salida vuelve a dar el
precio del `~C` del padre con un céntimo de tolerancia (tabla de arriba).

Volumen por fichero: El Escorial 112 `~D` / 114 líneas; Siroco 262 / 319;
`lagunamodificado16julio` 406 / 948; `presupuesto_limpio` 23 / 23.

Literales de Presto escritos a mano en los tests (no recalculados): `43.15` →
81,5364 antes / 81,526 después con clones 12,35 · 12,35 · 7,41; `05.06.29` →
−1,46 y total 71,54; `07.02.05` → 16,00 con el clon a `0`; `1000080` (la
captura de Elena Díaz) → 0,93 + 0,11 + **0** + 0,16 = 1,20, el precio de su
`~C`.

## Decisiones y desviaciones

1. **Código del padre con marca de capítulo.** Un `~D` puede llevar el `#` de
   capítulo (`~D|33.03.01#|`, El Escorial) y su `~M` apunta al par sin él. El
   clon se construye sobre el código **sin** el `#` y el remapeo de `~M` casa
   por ese mismo código. Limitación conocida: solo se quita **una** marca, así
   que un `##` dejaría `01#.P1`; no hay ningún caso en `input/`.
2. **`calcular_importes` tiene dos modos.** `redondear=True` fija el precio del
   clon acumulando el valor ya redondeado (R6); la entrada se mide sin
   redondear, porque medir las dos igual escondería un error cometido en los
   dos lados. `base_inicial` pasó a significar «arranca el acumulado aquí»
   (la base reconstruida) en vez de «el primer clon cobra esto», que era el
   defecto de la R9 vieja.
3. **Un `~D` con un número ilegible** en una línea anterior a una porcentual
   cae en R16 (`base_indeterminada`): sin base no se toca.
4. **`presupuesto.bc3` pierde 42 `~C` porcentuales sin convertir nada.** Es un
   banco de precios cuyos `%` no usa ningún `~D`, y R13 dice borrarlos cuando
   ninguna tripleta los referencia. Queda como está: es decisión de negocio
   que el humano está mirando.
5. **Guardas y código muertos eliminados** en el módulo nuevo: los levantó la
   campaña de mutación como mutantes equivalentes (ver §Supervivientes).
6. **El doble de `convert_to_material` usa la firma real** (arreglo del review
   de la ronda 1): con `**kwargs` el `try` de `TransformBC3Step` tenía éxito en
   los tests y el `except` —la rama que SIEMPRE corre en producción— se quedaba
   sin cubrir. Medido: `steps.py` 63 % → 65 %, con las líneas 79-81 cubiertas.

## Pendiente · verificaciones MANUALES (humano)

```
python -m interface_adapters.cli.porcentuales_cli "input/COSTE_250128_Siroco_Rv4mlo.bc3" "output/siroco_sin_pct.bc3"
python -m interface_adapters.cli.porcentuales_cli "input/lagunamodificado16julio.bc3" "output/laguna_sin_pct.bc3"
```

- **T15**: importar `siroco_sin_pct.bc3` en Presto y confirmar el total y el
  precio de `43.15`, `05.06.29`, `31.04.03.01` y `32.03.04.32`.
- **T24**: importar `laguna_sin_pct.bc3` y confirmar `ICV260` = 291,50 (no
  336,39) e `ICV270` = 369,50.

**Resultado de las dos: PENDIENTE** — nadie las ha ejecutado; quedan anotadas
en `progress/current.md`.

## Evidencias

| Evidencia | Valor |
|---|---|
| Tests ejecutados | **457 pasan, 1 skip** (`python -m pytest tests -q`); **110** son de F-002 |
| Cobertura de las líneas cambiadas | PENDIENTE |
| Mutantes / supervivientes | PENDIENTE |
| Tiempo de ejecución de la suite | **58,2 s** (los 110 de F-002, ~2 s) |
| `bash harness/init.sh` | PENDIENTE |

### Supervivientes: cómo se llegó al cero

Cinco campañas en la ronda 1 (174 mutantes y 62 supervivientes la primera;
0 desde la tercera) y una más en la ronda 2 sobre el código nuevo. Cada
superviviente se cerró con un test o quitando el código que lo generaba;
ninguno quedó justificado «a mano». Por familias:

1. **Contrato de `_shorten_code_unique`** (3): nadie comprobaba el defecto
   `forzar_unicidad=False` ni que no registre en `used` lo que devuelve
   intacto.
2. **Números ilegibles** (6): faltaba «uno de los dos números no se lee».
3. **`~C` y `~D` truncados** (12): cada `if len(campos) > N` pide un registro
   con exactamente N campos; un BC3 de test con `~C` de 2, 3, 4, 5 y 6 campos
   y `~D` sin barra y sin pipe final los cazó todos.
4. **Filas del informe** (6): se miraba el motivo pero no el padre, el código,
   el rendimiento ni el importe.
5. **Redondeo acumulado** (R6, 1): sin redondear la base, el segundo clon del
   caso de prueba sale 56,17 en vez de 56,18.
6. **Inmutabilidad del plan** (3 `frozen=True`) y **`~T` multilínea y líneas
   sueltas** (2).
7. **Carpetas de salida** (4 `parents=True`): los tests creaban un solo nivel,
   donde `parents=False` también vale; ahora dos.
8. **Código muerto** (≈15 equivalentes): `campos[1]` existe siempre tras
   `~C|`/`~D|`/`~T|`/`~M|`, los valores por defecto de
   `InformePorcentuales.anota` y `construir_pipeline` no los usaba nadie, y
   `hay_porcentual()` no lo llamaba nadie (lo levantó el review). Un mutante
   equivalente es código que sobra.
