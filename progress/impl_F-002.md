<!-- progress/impl_F-002.md -->
# F-002 · Informe de implementación

Feature `critico`, SDD. Spec aprobada: `specs/F-002-porcentuales-a-ud/`.
Rama `feature/F-002-porcentuales-a-ud`, un commit por tarea (T1-T14).
**T15 es MANUAL del humano y sigue PENDIENTE** (comando abajo).

## Qué cambió

| Fichero | Qué |
|---|---|
| `infrastructure/bc3/bc3_porcentajes.py` | **nuevo**. Toda la pasada: detección (R1), cálculo en `Decimal` (R2, R6), `planificar` (pasada 1) y `convertir_porcentuales` (pasada 2) |
| `interface_adapters/cli/porcentuales_cli.py` + `__init__.py` | **nuevos**. Ejecución sobre un fichero suelto (R22) e informe CSV (R20) |
| `infrastructure/bc3/bc3_modifier.py` | **un solo cambio**: keyword `forzar_unicidad=False` en `_shorten_code_unique` |
| `application/pipeline/pipeline.py` | `ETLContext.preprocessed_path` |
| `application/pipeline/steps.py` | `ConvertirPorcentualesStep`; `TransformBC3Step` usa `ctx.preprocessed_path or ctx.original_path` |
| `interface_adapters/controllers/etl_controller.py` | `construir_pipeline()`: el step entra entre `ResolveInputStep` y `TransformBC3Step` si la bandera está activa |
| `config/settings.py` | `porcentuales_a_ud` (`PORCENTUALES_A_UD`, por defecto activa) |
| `tests/test_f002_{porcentuales,invariante,pipeline}.py` | 93 tests |
| `tests/fixtures/f002_*.bc3` | 7 fixtures con números reales de `input/` |
| `docs/ARCHITECTURE.md`, `.gitignore` | el step nuevo en el orden del pipeline; excepción para versionar `tests/fixtures/*.bc3` |

Lo que **no** se ha tocado: `convert_to_material`, `build_tree_service`, los
clones `.1`, la FASE 2 y `input/` (solo lectura; los tests escriben en
`tmp_path`).

## Fase RED · trazas reales del fallo

**T2 · detección y cálculo** — `python -m pytest tests/test_f002_porcentuales.py -k "r1_ or r2_ or r7_ or r11_" -q --tb=short`

```
tests\test_f002_porcentuales.py:18: in <module>
    from infrastructure.bc3.bc3_porcentajes import (
E   ModuleNotFoundError: No module named 'infrastructure.bc3.bc3_porcentajes'
ERROR tests/test_f002_porcentuales.py
!!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
1 error in 0.27s
```

**T4 · códigos de clon** — `python -m pytest tests/test_f002_porcentuales.py -k r5_ -q --tb=short`

```
tests\test_f002_porcentuales.py:20: in <module>
    from infrastructure.bc3.bc3_porcentajes import (
E   ImportError: cannot import name 'codigo_de_clon' from
    'infrastructure.bc3.bc3_porcentajes'
1 error in 0.22s
```

**T6 · reescritura** — `python -m pytest tests/test_f002_porcentuales.py -q --tb=short`

```
tests\test_f002_porcentuales.py:22: in <module>
    from infrastructure.bc3.bc3_porcentajes import (
E   ImportError: cannot import name 'convertir_porcentuales' from
    'infrastructure.bc3.bc3_porcentajes'
1 error in 0.25s
```

**T10 · bandera** — `python -m pytest tests/test_f002_porcentuales.py -k r21_ -q --tb=short`

```
    assert Settings().porcentuales_a_ud is True
E   AttributeError: 'Settings' object has no attribute 'porcentuales_a_ud'
    ajustes = replace(Settings(), porcentuales_a_ud=False)
E   TypeError: Settings.__init__() got an unexpected keyword argument
    'porcentuales_a_ud'
2 failed, 33 deselected in 0.17s
```

**T11 · step del pipeline** — `python -m pytest tests/test_f002_pipeline.py -q --tb=line`

```
tests\test_f002_pipeline.py:19: in <module>
    from application.pipeline.steps import ConvertirPorcentualesStep, TransformBC3Step
E   ImportError: cannot import name 'ConvertirPorcentualesStep' from
    'application.pipeline.steps'
1 error in 0.86s
```

Cada una pasó a verde con el commit de implementación siguiente (T3, T5, T7,
T10 y T11 respectivamente).

## Lo que se verificó con números reales

Medición de la pasada sobre los diez `.bc3` de `input/` (lectura; salida a un
temporal). R19 se comprueba `~D` a `~D`, **entrada contra salida**, nunca
contra el precio del `~C` del padre:

| Fichero | `~D` con % | líneas convertidas | conceptos % borrados | `~D` comparados | peor desvío | desvío acumulado |
|---|---|---|---|---|---|---|
| `250311_...EL ESCORIAL_R5` | 112 | 114 | 23 | 112 | 0,005 € | −0,019 € |
| `COSTE_250128_Siroco_Rv4mlo` | 262 | 319 | 21 | 260 | 0,0104 € | +0,030 € |
| `lagunamodificado16julio` | 406 | 948 | 18 | 404 | 0,0099 € | +0,171 € |
| `presupuesto_limpio` | 23 | 23 | 5 | 23 | 0,005 € | −0,028 € |
| (y 6 variantes más de los mismos) | | | | | | |

Total: **1.602 `~D` comparados, ni uno fuera de la tolerancia de R19**
(`0,01 + 0,005 × nº de porcentuales`). Ningún `base_indeterminada` (R16) en
ficheros reales: el caso existe y está cubierto con fixtures.

Los **cuatro descompuestos solo-porcentuales de R9**, cada uno con test propio
(`test_f002_r9_*`) y listados por el informe como `precio_del_padre_aplicado`:

| Padre | Fichero | Precio del `~C` | Clon `.P1` | Clon `.P2` |
|---|---|---|---|---|
| `31.04.03.01` | Siroco | 1.100,00 | 1.100,00 | — |
| `32.03.04.32` | Siroco | 1.117,65 | 1.117,65 | — |
| `ICV260` | lagunamodificado16julio | 291,50 | 291,50 | 44,89 |
| `ICV270` | lagunamodificado16julio | 369,50 | 369,50 | 56,90 |

Y los literales de Presto, escritos a mano en los tests (no recalculados):
`43.15` → 81,5364 antes / 81,526 después con clones 12,35 · 12,35 · 7,41;
`05.06.29` → −1,46 y total 71,54; `07.02.05` → 16,00 con el clon a `0`;
`1000080` (el patrón de la captura de Elena Díaz) → 0,93 + 0,11 + **0** + 0,16
= 1,20, que es justo el precio de su `~C`.

## Decisiones y desviaciones

1. **Código del padre con marca de capítulo.** La spec dice
   `<codigo_padre>.P<n>`, pero un `~D` puede llevar el `#` de capítulo
   (`~D|33.03.01#|`, El Escorial) y su `~M` apunta al par sin él
   (`33.03.01\%CC`). El clon se construye sobre el código **sin** el `#`
   (`33.03.01.P1`) y el remapeo de `~M` casa por ese mismo código. Medido: es
   el único `~D` con `#` y porcentuales de los cuatro presupuestos.
   Limitación conocida: solo se quita **una** marca, así que un `##`
   (supercapítulo) dejaría `01#.P1`; no existe ningún caso en `input/`.
2. **`calcular_importes` tiene un modo redondeado** (`redondear=True`), que es
   el que fija el precio del clon acumulando el valor ya redondeado (R6). La
   entrada se mide sin redondear: si se midieran las dos igual, el invariante
   no vería un error cometido en los dos lados (§Tests de `design.md`).
3. **Un `~D` con un número ilegible** (factor o rendimiento no numérico) en una
   línea anterior a una porcentual cae en R16 (`base_indeterminada`) igual que
   si faltara el precio: no se puede calcular la base, así que no se toca.
4. **`presupuesto.bc3` pierde 42 `~C` porcentuales sin convertir nada.** Es un
   banco de precios: tiene conceptos `%` que ningún `~D` usa y R13 dice
   borrarlos cuando ninguna tripleta los referencia. Se aplica la regla tal
   cual está escrita; si el humano prefiere conservarlos cuando no hay
   conversión, es un cambio de una línea en `_decidir_conceptos_a_eliminar`.
5. **Guardas muertas eliminadas** en el módulo nuevo: `campos[1]` existe
   siempre si la línea empieza por `~C|`/`~D|`/`~T|`. Lo levantó la campaña de
   mutación (mutantes equivalentes sobre código inalcanzable).

## Pendiente · T15, verificación MANUAL (humano)

```
python -m interface_adapters.cli.porcentuales_cli "input/COSTE_250128_Siroco_Rv4mlo.bc3" "output/siroco_sin_pct.bc3"
```

Importar `output/siroco_sin_pct.bc3` en Presto y confirmar el total del
presupuesto y el precio de `43.15`, `05.06.29`, `31.04.03.01` y `32.03.04.32`.
**Resultado: PENDIENTE** — nadie lo ha ejecutado; queda anotado en
`progress/current.md`.

## Evidencias

| Evidencia | Valor |
|---|---|
| Tests ejecutados | PENDIENTE |
| Cobertura de las líneas cambiadas | PENDIENTE |
| Mutantes / supervivientes | PENDIENTE |
| Tiempo de la suite | PENDIENTE |
| `bash harness/init.sh` | PENDIENTE |

### Supervivientes de la campaña

PENDIENTE
