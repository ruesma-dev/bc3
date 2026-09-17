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
- [ ] T11: Añadir `preprocessed_path` a `ETLContext`, crear `ConvertirPorcentualesStep` en `application/pipeline/steps.py` y hacer que `TransformBC3Step` use `ctx.preprocessed_path or ctx.original_path`  |  Verificación: test de pipeline con un `ETLContext` de fixtures que comprueba el encadenado de rutas, sin tocar `input/`
- [ ] T12: Insertar el step en `interface_adapters/controllers/etl_controller.py` entre `ResolveInputStep` y `TransformBC3Step`, condicionado a la bandera  |  Verificación: `pytest tests/ -q` en verde y revisión de que la composición sigue en el punto de entrada
- [ ] T13: Crear `interface_adapters/cli/porcentuales_cli.py` (R22) con `logging`, sin `print()`, y el volcado del informe a `output/informe_porcentuales.csv` (UTF-8 BOM, `;`, coma decimal)  |  Verificación: `python -m interface_adapters.cli.porcentuales_cli tests/fixtures/f002_cadena.bc3 output/tmp_f002.bc3` termina con código 0 y escribe el informe
- [ ] T14: Campaña de mutación completa y análisis de supervivientes (rigor `critico`: cero supervivientes sin justificación aceptada)  |  Verificación: `python -m harness.mutacion --feature F-002` y `progress/mutacion_F-002.md` sin «CAMPAÑA NO VÁLIDA»
- [ ] T15: Verificación MANUAL (humano): ejecutar el CLI sobre `input/COSTE_250128_Siroco_Rv4mlo.bc3`, importar la salida en Presto y confirmar total y precios de `43.15`, `05.06.29`, `31.04.03.01` y `32.03.04.32`  |  Verificación: MANUAL (humano), comando exacto en `design.md` §Verificación MANUAL, resultado anotado en `progress/current.md`
- [ ] T16: Ejecutar `bash harness/init.sh` en verde  |  Verificación: `bash harness/init.sh` termina con exit code 0
