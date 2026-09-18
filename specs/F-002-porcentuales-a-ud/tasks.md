<!-- specs/F-002-porcentuales-a-ud/tasks.md -->
# F-002 · Tareas

Cada tarea = un commit `F-002 Tn: ...`. Rigor `critico`: T2-T6 se escriben en
RED (test primero, traza del fallo pegada en `progress/impl_F-002.md`) antes de
la implementación que las pone en verde.

- [x] T1: Crear las siete fixtures de `tests/fixtures/f002_*.bc3` en `latin-1` con los números reales listados en `design.md` §Fixtures  |  Verificación: `python -c "from pathlib import Path; [print(p, len(p.read_text('latin-1').splitlines())) for p in Path('tests/fixtures').glob('f002_*.bc3')]"` lista los 7 ficheros
- [x] T2: Tests de detección y cálculo (R1, R2, R7, R11) sobre `es_porcentual` y `calcular_importes`, incluidos los números literales de Presto 81,5364 / 71,54 / −1,46 / 16,00  |  Verificación: `pytest tests/test_f002_porcentuales.py -k "r1_ or r2_ or r7_ or r11_"` en RED y luego en verde
- [x] T3: Implementar en `infrastructure/bc3/bc3_porcentajes.py` la detección, el cálculo en `Decimal` y las dataclases `CasoPorcentual` / `InformePorcentuales`  |  Verificación: los tests de T2 en verde
- [x] T4: Tests de códigos de clon (R5) incluidos recorte a 20 y colisión forzada, con `f002_codigo_largo.bc3`  |  Verificación: `pytest tests/test_f002_porcentuales.py -k r5_` en RED
- [x] T5: Añadir `forzar_unicidad: bool = False` a `_shorten_code_unique` en `infrastructure/bc3/bc3_modifier.py` e implementar `planificar` (pasada 1) con `codigos_ocupados` incluyendo los truncados a 20  |  Verificación: tests de T4 en verde y `pytest tests/ -q` sin regresiones
- [x] T6: Tests de reescritura (R3, R4, R6, R9, R10, R12, R13, R14, R15, R16, R17, R18, R23) sobre las fixtures  |  Verificación: `pytest tests/test_f002_porcentuales.py` en RED
- [x] T7: Implementar `convertir_porcentuales` (pasada 2) con la reescritura de `~D`, la emisión de los `~C` de clon, el borrado de `~C`/`~T` porcentuales y el remapeo de `~M`  |  Verificación: `pytest tests/test_f002_porcentuales.py` en verde
- [x] T8: Test del invariante (R19) entrada-contra-salida por cada `~D`, más test de orden (permutar dos tripletas cambia el resultado) y test de idempotencia  |  Verificación: `pytest tests/test_f002_invariante.py`
- [x] T9: Test del invariante sobre cada `.bc3` de `input/`, con `pytest.mark.skipif` si la carpeta está vacía, y comprobación de que todo código de `~D` existe como `~C` en la salida  |  Verificación: `pytest tests/test_f002_invariante.py -k input`
- [x] T10: Añadir `porcentuales_a_ud` a `Settings` y el test de R21 (bandera off ⇒ fichero idéntico, byte a byte)  |  Verificación: `pytest tests/test_f002_porcentuales.py -k r21_`
- [x] T11: Añadir `preprocessed_path` a `ETLContext`, crear `ConvertirPorcentualesStep` en `application/pipeline/steps.py` y hacer que `TransformBC3Step` use `ctx.preprocessed_path or ctx.original_path`  |  Verificación: test de pipeline con un `ETLContext` de fixtures que comprueba el encadenado de rutas, sin tocar `input/`
- [x] T12: Insertar el step en `interface_adapters/controllers/etl_controller.py` entre `ResolveInputStep` y `TransformBC3Step`, condicionado a la bandera  |  Verificación: `pytest tests/ -q` en verde y revisión de que la composición sigue en el punto de entrada
- [x] T13: Crear `interface_adapters/cli/porcentuales_cli.py` (R22) con `logging`, sin `print()`, y el volcado del informe a `output/informe_porcentuales.csv` (UTF-8 BOM, `;`, coma decimal)  |  Verificación: `python -m interface_adapters.cli.porcentuales_cli tests/fixtures/f002_cadena.bc3 output/tmp_f002.bc3` termina con código 0 y escribe el informe
- [x] T14: Campaña de mutación completa y análisis de supervivientes (rigor `critico`: cero supervivientes sin justificación aceptada)  |  Verificación: `python -m harness.mutacion --feature F-002` y `progress/mutacion_F-002.md` sin «CAMPAÑA NO VÁLIDA»
- [ ] T15: Verificación MANUAL (humano): ejecutar el CLI sobre `input/COSTE_250128_Siroco_Rv4mlo.bc3`, importar la salida en Presto y confirmar total y precios de `43.15`, `05.06.29`, `31.04.03.01` y `32.03.04.32`  |  Verificación: MANUAL (humano), comando exacto en `design.md` §Verificación MANUAL, resultado anotado en `progress/current.md`
- [x] T16: Ejecutar `bash harness/init.sh` en verde  |  Verificación: `bash harness/init.sh` termina con exit code 0

## Corrección de la R9 (2026-09-17) · base reconstruida

- [x] T17: Crear las fixtures `tests/fixtures/f002_solo_pct_doble.bc3` (`ICV260` e `ICV270` de `input/lagunamodificado16julio.bc3`) y `f002_descuento_total.bc3` (`r = -1`)  |  Verificación: `pytest tests/test_f002_porcentuales.py -q` sigue en verde con las fixtures presentes
- [x] T18: Tests en RED de la R9 nueva con los números de la tabla de `design.md` §Base reconstruida: `ICV260` 225,98 · 26,62 · 38,90; `ICV270` 286,45 · 33,74 · 49,31; `31.04.03.01` 1.073,17 · 26,83; `32.03.04.32` 955,26 · 162,39  |  Verificación: `pytest tests/test_f002_porcentuales.py -k r9_` en RED, con la traza pegada en `progress/impl_F-002.md`
- [x] T19: Test en RED de R9 bis (residuo absorbido en la línea de base: la suma del `~D` da exactamente `P`) y de R9 ter (`(1 + r_i)` ≤ 0 ⇒ `~D` intacto y motivo `base_no_despejable`)  |  Verificación: `pytest tests/test_f002_porcentuales.py -k "r9bis or r9ter"` en RED
- [x] T20: Test en RED de R19 bis sobre los cuatro `~D` solo-porcentuales reales de `input/`: importe calculado sobre la SALIDA == precio del `~C` del padre ± 0,01  |  Verificación: `pytest tests/test_f002_invariante.py -k r19bis` en RED, y con el código viejo debe fallar con `ICV260` 336,39 ≠ 291,50
- [x] T21: Implementar la base reconstruida en `infrastructure/bc3/bc3_porcentajes.py`: `base = P / Π(1 + r_i)`, línea de base `<padre>.P0` (unidad `UD`, factor 1, rendimiento 1, tipo 3) delante de las porcentuales, residuo absorbido en ella, motivos `base_reconstruida` / `base_no_despejable` en el informe  |  Verificación: T18-T20 en verde y `pytest tests/ -q` sin regresiones
- [x] T22: Reservar `<padre>.P0` en `codigos_ocupados` con la misma escalera de unicidad que `.P<n>` (R5)  |  Verificación: `pytest tests/test_f002_porcentuales.py -k r5_` en verde, incluido el caso de código largo
- [x] T23: Regenerar la campaña de mutación sobre el código nuevo (rigor `critico`: cero supervivientes sin justificación aceptada)  |  Verificación: `python -m harness.mutacion --feature F-002` y `progress/mutacion_F-002.md` sin «CAMPAÑA NO VÁLIDA»
- [ ] T24: Verificación MANUAL (humano) de la regla nueva: `python -m interface_adapters.cli.porcentuales_cli "input/lagunamodificado16julio.bc3" "output/laguna_sin_pct.bc3"`, importar en Presto y confirmar `ICV260` = 291,50 (no 336,39) e `ICV270` = 369,50  |  Verificación: MANUAL (humano), resultado anotado en `progress/current.md`
- [x] T25: Ejecutar `bash harness/init.sh` en verde  |  Verificación: `bash harness/init.sh` termina con exit code 0

## Decimales del precio del clon (2026-09-18) · R6, R6 bis

- [x] T26: Test en RED de R6: con `PORCENTUALES_DECIMALES=2` la salida de `43.15` es la vieja (12,35 · 12,35 · 7,41, suma 81,526) y con el defecto 4 es la nueva (12,3540 · 12,3540 · 7,4124, suma 81,5364)  |  Verificación: `pytest tests/test_f002_porcentuales.py -k r6_` en RED, con la traza en `progress/impl_F-002.md`
- [x] T27: Test en RED de R6 bis (valores fuera de 2..6 o no numéricos caen a 4 con aviso en log) y de R7 (punto decimal, sin exponentes, sin ceros de relleno, `0` en vez de `-0`)  |  Verificación: `pytest tests/test_f002_porcentuales.py -k "r6bis or r7_"` en RED
- [x] T28: Añadir `porcentuales_decimales` a `Settings` (`_env_int("PORCENTUALES_DECIMALES", "4")`, saneado a 2..6) y propagarlo por `ConvertirPorcentualesStep` y el CLI  |  Verificación: T26 y T27 en verde
- [x] T29: Parametrizar el redondeo en `infrastructure/bc3/bc3_porcentajes.py` (`quantize(Decimal(1).scaleb(-d), ROUND_HALF_UP)` en el precio del clon, en la base de R9 y en el residuo de R9 bis) y el formateo del número de R7  |  Verificación: `pytest tests/ -q` en verde
- [x] T30: Test de R19 y R19 bis **con `d = 2` y con `d = 4`** (`Settings` clonado con `replace`), sobre fixtures y sobre cada `.bc3` de `input/`: la tolerancia se calcula con `d` y la suma de los `~D` de R9 sigue dando `P` exacto  |  Verificación: `pytest tests/test_f002_invariante.py -q`
- [x] T31: Regenerar la campaña de mutación sobre el código nuevo  |  Verificación: `python -m harness.mutacion --feature F-002` y `progress/mutacion_F-002.md` sin «CAMPAÑA NO VÁLIDA»
- [ ] T32: Verificación MANUAL (humano): importar en Presto la salida con 4 decimales y anotar **cuántos decimales acepta**; si acepta 6, subir `PORCENTUALES_DECIMALES=6` y repetir  |  Verificación: MANUAL (humano), pasos en `design.md` §Verificación MANUAL, resultado en `progress/current.md`
- [x] T33: Ejecutar `bash harness/init.sh` en verde  |  Verificación: `bash harness/init.sh` termina con exit code 0
- [x] T34: Volver el defecto de `porcentuales_decimales` a **2** y fijarlo con el invariante de R6 ter (el importe de cada `~D` reproduce el precio que el `~C` de su padre declara), tras medir que 2 acierta 99,2 % / 100 % y 4 baja a 94,3 % / 80,1 %  |  Verificación: `pytest tests/test_f002_invariante.py -k reproduce_el_precio` en RED con el defecto 4 y en verde con el 2

## Limpieza de texto (2026-09-18) · R18, R24-R26

- [x] T35: Crear la fixture `tests/fixtures/f002_acentos.bc3` con los cuatro `VALV` reales («Válvula de bola, ½"», `1¼"`, `1½"`, `2"`), un `~T` con acentos y un descompuesto porcentual cuyo clon hereda un resumen con `Ñ`  |  Verificación: `pytest tests/test_f002_porcentuales.py -q` sigue en verde con la fixture presente
- [x] T36: Test en RED de R24: con la bandera encendida, el resumen de los cuatro `VALV` sale limpio (sin no-ASCII) y el del clon y el de la línea de base también; se comprueba el texto exacto antes y después  |  Verificación: `pytest tests/test_f002_porcentuales.py -k r24_` en RED, con la traza en `progress/impl_F-002.md`
- [x] T37: Test en RED de R18 con la bandera **apagada**: la salida es **byte a byte** idéntica a la entrada salvo las líneas porcentuales, comparando los bytes `latin-1` del fichero completo  |  Verificación: `pytest tests/test_f002_porcentuales.py -k r18_` en RED
- [x] T38: Test en RED de R25: con la bandera encendida, ningún código, precio, factor ni rendimiento cambia respecto a la salida con la bandera apagada (se comparan solo esos campos, campo a campo)  |  Verificación: `pytest tests/test_f002_porcentuales.py -k r25_` en RED
- [x] T39: Añadir `porcentuales_limpiar_texto` a `Settings` (`_env_bool("PORCENTUALES_LIMPIAR_TEXTO", "true")`) y propagarlo por `ConvertirPorcentualesStep` y el CLI  |  Verificación: T36-T38 siguen en RED por el código, no por la configuración
- [x] T40: Implementar la limpieza en `infrastructure/bc3/bc3_porcentajes.py` usando `utils.text_sanitize.clean_text` —sin escribir otra limpieza— sobre el resumen del `~C`, el texto del `~T` y el resumen de clones y líneas de base  |  Verificación: T36-T38 en verde y `pytest tests/ -q` sin regresiones
- [x] T41: Test sobre cada `.bc3` de `input/`: tras la pasada con la bandera encendida, **ningún `~C` con descompuesto propio conserva caracteres no ASCII en su resumen** (R25)  |  Verificación: `pytest tests/test_f002_invariante.py -k acentos`
- [x] T42: Test de R26: R6 ter, R19 y R19 bis pasan con la bandera encendida **y** apagada  |  Verificación: `pytest tests/test_f002_invariante.py -q` parametrizado por la bandera
- [ ] T43: Regenerar la campaña de mutación sobre el código nuevo  |  Verificación: `python -m harness.mutacion --feature F-002` y `progress/mutacion_F-002.md` sin «CAMPAÑA NO VÁLIDA»
- [ ] T44: Verificación MANUAL (humano): pasar el CLI sobre el presupuesto de Elena Díaz, importarlo en Sigrid y confirmar que `VALV1`, `VALV4`, `VALV5` y `VALV6` entran ya con su descompuesto  |  Verificación: MANUAL (humano), pasos en `design.md` §Verificación MANUAL, resultado en `progress/current.md`
- [ ] T45: Ejecutar `bash harness/init.sh` en verde  |  Verificación: `bash harness/init.sh` termina con exit code 0
