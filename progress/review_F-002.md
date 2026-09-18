<!-- progress/review_F-002.md -->
Revisión incremental desde `12fbb9e` (pasada 5) — `git diff 12fbb9e..HEAD`, HEAD `3ccc4e8fd5e195637f8e7906fba7d59a655746fa`, 12 commits, 14 ficheros. Cubre la ronda 4 (vuelta a 2 decimales) y la 5 (limpieza de texto). Lo aprobado hasta `12fbb9e` queda dado por bueno salvo lo que estas dos rondas tocan, que releo y remido.

# F-002 · Review · Porcentuales a UD

**Veredicto: APPROVED**, con una reserva documentada que **no bloquea** y que va en «Lo que queda sin red».

`bash harness/init.sh` → **exit 0**, `ENTORNO LISTO` (entero): pytest en verde; `PUERTA COBERTURA [OK] 98,9 %` de 454 líneas cambiadas (449/454); `PUERTA TAMAÑO [OK]`, con `impl` en **220/220** y `design` en **250/250**, al ras.

## 1 · ¿Era correcto tocar el sitio compartido? Sí, y además lo manda la spec

Mi juicio coincide con el tuyo, y por una razón que he podido medir en vez de argumentar. Ejecuté `convert_to_material` sobre `250311_COSTE_88V EL ESCORIAL_R5.bc3` **dos veces**, con el `text_sanitize` viejo y con el nuevo, en dos árboles separados:

| | salida | líneas | difieren | **caracteres no-ASCII en la salida** |
|---|---|---|---|---|
| `clean_text` viejo | ok | 9.510 | — | **38** |
| `clean_text` nuevo | ok | 9.510 | 41 | **0** |

Escribir una limpieza propia para F-002 habría dejado esos **38 caracteres no-ASCII saliendo por `convert_to_material` hacia el mismo Sigrid** que rechaza los descompuestos por ese motivo, y el resultado de un fichero habría dependido de por dónde hubiera entrado. Se arregla en el único sitio donde estaba el agujero. Además no es una decisión suelta del implementer: **R24 lo exige por escrito** («el mismo que usa `convert_to_material`: **no se escribe otra limpieza**»), y esa spec la aprobó el humano.

Comprobé la trampa de dominio que esto podía abrir. Una de las 41 líneas es un `~D` (`DIAGONALES ø12` → `DIAGONALES o12`), o sea que el cambio toca **códigos** dentro de `convert_to_material`. No rompe nada porque el `~C` correspondiente pasa por la misma función y cambia igual: **códigos de `~D` sin su `~C`: 207 antes y 207 después**, ni uno nuevo.

Sobre el corpus real entero (10 ficheros, **49.439 líneas**) los únicos caracteres cuyo tratamiento cambia son exactamente los cuatro del mapa: `Ø` ×127, `µ` ×27, `ø` ×17, `ß` ×3. **Ningún carácter se pierde en silencio.**

## 2 · La red de ese cambio: existe, pero es mínima, y hay que decirlo

El cambio tiene **dos partes**, y solo la primera es la que se anuncia:

- **(a) el mapa de transliteraciones** (`Ø→O`, `æ→ae`, `ß→ss`, `µ→u`…), que es puramente aditivo;
- **(b) el filtro se estrecha**: de `ch in _ALLOWED or ch.isalnum() or ch.isspace()` a solo `ch in _ALLOWED`.

(b) es más ancho que el mapa: una letra no-ASCII que **no** esté en el mapa y que NFKD no descomponga ya no pasa, **se borra**. Medido: `'resistencia Ω ohmios'` → `'resistencia  ohmios'`; `'texto Русский'` → `'texto '`. El propio test nuevo lo documenta (espera `'…, ae, , 12"'`, con el hueco donde estaba la `Ω`). En el corpus real **no ocurre ni una vez**, y la preocupación por los espacios no-ASCII es infundada: NFKD ya pliega el NBSP a un espacio normal, así que quitar `isspace()` es inocuo (comprobado: `'Valvula\xa0de bola'` da `'Valvula de bola'` con las dos versiones). Con todo, es un cambio de contrato —perder texto en vez de dejar pasar no-ASCII— y para Sigrid es el comportamiento correcto: la función ahora **cumple lo que promete** (`isascii()` da `True` con la nueva y `False` con la vieja).

**La red, en concreto:**

- `clean_text` tiene **un** test, nuevo de esta ronda: `test_f002_r24_la_limpieza_compartida_devuelve_siempre_ascii`, que fija `.isascii()` y la transliteración exacta. Es el que sujeta el contrato.
- **`convert_to_material` no tiene NINGÚN test.** `grep -rn convert_to_material tests/` solo devuelve un espía de `monkeypatch` que lo **sustituye** y dos menciones en docstrings. Su otro consumidor —`phase2_code_mapper`, que manda descripciones al clasificador— tampoco.
- **La campaña de mutación no cubre el cambio**: `utils/text_sanitize.py` entra en el alcance con 23 líneas y **0 mutantes**. Hice el control de C4 bis para el cero: sobre el fichero entero, ignorando el alcance, salen **2 mutantes**, y los dos caen en líneas que este delta no toca. El cero es **estructural** (un dict literal y dos comprensiones no tienen operador mutable), no un generador roto.

O sea: fuera de ese único test, **lo único que hoy respalda que la salida del ETL no ha empeorado es la medición que he hecho yo en esta review**. Es deuda **previa** —`convert_to_material` tampoco tenía tests antes—, el delta sí trae su test de contrato, y la medición sale limpia, así que no lo convierto en bloqueo; pero queda escrito abajo como tarea.

## 3 · R24 / R25 / R26, medidos sobre tres presupuestos reales

Ejecuté la pasada con la bandera **encendida y apagada** y comparé campo a campo, separando por tipo de registro:

| Fichero | líneas distintas ON/OFF | `~C`: campos que cambian | `~T`: campos | otros `~` |
|---|---|---|---|---|
| `lagunamodificado16julio` | 5.451 (`~C` 792, `~T` 1.307, cont. 3.352) | **[3]** (resumen) | **[2]** (cuerpo) | **0** |
| `COSTE_250128_Siroco_Rv4mlo` | 1.392 | **[3]** | **[2]** | **0** |
| `250311_…EL ESCORIAL_R5` | 3.964 | **[3]** | **[2]** | **0** |

**Solo el resumen del `~C` y el cuerpo del `~T`.** Nunca el campo 1 (código), el 2 del `~C` (unidad), el 4 (precio), la fecha ni el tipo. Y lo que zanja R26: **los precios de TODOS los `~C` son idénticos con la bandera en los dos estados, y las líneas `~D` son idénticas carácter a carácter** en los tres ficheros. No hay cruce posible entre limpieza y cálculo: ningún importe puede moverse porque ningún número cambia. Los invariantes lo sujetan además en la suite, parametrizados `limpiar=[True, False]` (R19, R19 bis y R6 ter).

**R25**, sobre la salida real: `~C` con descompuesto propio que conservan no-ASCII en el resumen → laguna **173 → 0** (los 173 exactos de la spec), Siroco **265 → 0**, Escorial **269 → 0**.

**R18 (d)**: con la bandera **apagada**, los únicos tipos de registro que cambian respecto a la entrada son `~C` y `~D` (y el `~M` de Escorial), es decir los casos (a), (b) y (c) de siempre. `input/` queda byte a byte idéntico en todas las ejecuciones.

## 4 · La vuelta atrás de la ronda 4: nada se borró, y el criterio nuevo es mejor

Cuatro tests «desaparecen» de `test_f002_porcentuales.py` (73 → **82**): son los cuatro de la ronda 3, **renombrados con la expectativa invertida** (`..._decimales_por_defecto_son_cuatro` → `..._son_dos`; `..._c020615_pierde_un_centimo_con_dos_decimales` → `..._c020615_reproduce_su_c_con_dos_decimales`). `C020615` **sigue teniendo test**, ahora afirmando lo contrario. `test_f002_invariante.py` 9 → 12, `test_f002_pipeline.py` 6 → 6 intacto.

Lo que sustituye al caso suelto es **R6 ter**, un invariante de corpus contra un número ajeno —el precio que Presto escribió en cada `~C`—, con umbral `ACIERTOS_MINIMOS = 0.98` y mínimo de 200 partidas comparadas. Lo medí por mi cuenta sobre la salida real, excluidas las cinco partidas de precio a mano: **Siroco 258/259 = 99,6 %** y **laguna 404/404 = 100 %** con el defecto de 2 decimales. Pasa con margen.

Es, además, exactamente la lección que propuse al cerrar la pasada 4 —comparar contra un número de fuera del sistema— convertida en requisito. Me parece el cambio más sólido de esta ronda.

## 5 · Campaña de mutación

- [x] **RM1 · SHA.** Informe `3cdd2b79eb4e9aa2a7d69db7ddb80fb04ab8a2a3`; `git diff --stat 3cdd2b7..HEAD` devuelve **solo** `progress/current.md`, `progress/impl_F-002.md`, `progress/mutacion_F-002.md` y `specs/.../tasks.md`: ni una línea de producción ni de test.
- [x] **Totales verificados por mí**: recalculados con `harness.alcance` + `generar_mutantes` → **985 líneas y 198 mutantes**, coincidiendo fichero a fichero (porcentajes 178, cli 10, steps 5, modifier 5, text_sanitize **0**, resto 0).
- [x] **0 supervivientes, 0 timeouts, «Sin veredicto (base rota)» = 0**, sin cabecera «⚠ CAMPAÑA NO VÁLIDA», completa sin muestreo, 4 workers.
- [x] **RM2 · Tiempo.** Total **446,7 s** > 60 s: **no la he reejecutado**, y conste. Media × workers = 2,3 × 4 = **9,2 s frente a 64,6 s** de línea base (ratio 0,142); coste por mutante 446,7 × 4 ÷ 198 = **9,0 s**. «Timeout efectivo» 140 s, distinto de las campañas previas: **no comparo tiempos entre campañas**.
- [x] **RM4 · ejecutada sobre el código nuevo**: dos mutantes reales, los dos **MUEREN**. `bc3_porcentajes.py:595 entero 'campos[3] = limpio' → 'campos[4] = limpio'` —que escribiría el texto limpio **en el campo del precio**— y `:590 comparacion 'len(campos) <= 3' → '< 3'`.
- [x] **RM3** cero equivalentes declarados. **RM5 · N/A por ausencia de objeto**. **RM6**: el delta no retira ninguna guarda de `None`; el estrechamiento del filtro se analiza en el punto 2.

## Checkpoints

- **C1** [x] init.sh exit 0. **C2** [x] una sola `in_progress`; rama correcta; `current.md` al día.
- **C3** [x] la limpieza vive en `utils/` y se llama desde infraestructura; una sola implementación en el repositorio; sin `print()`, sin secretos, sin dependencias nuevas, en español. Trampa de dominio de los códigos `~D`/`~C`: comprobada en el punto 1 (207 = 207).
- **C3 bis** **N/A justificado**: el delta no toca `docs/referencia/`.
- **C4** [x] R24, R25, R26, R6 ter y R6 bis tienen test trazable y pasan; los dobles siguen cumpliendo (a)-(c); ningún test toca red, BBDD, Gemini, OpenAI ni `ocr_service`. **C4 bis** [x] arriba, con el control del cero de mutantes hecho. **C4 ter** **N/A**: no existe `harness/rutas_sensibles.json`.
- **C5** [x] T34-T45 en `[x]` con su commit; **T15, T24, T32 y T46 `[ ]` a propósito**, son del humano. Árbol limpio salvo los untracked previos a la rama.

## Lo que queda sin red (para el humano, no bloquea)

1. **`convert_to_material` no tiene ni un test, y esta ronda le ha cambiado la salida.** Es deuda previa, pero ahora tiene consecuencias: 41 líneas distintas en un presupuesto real. Propongo una feature aparte con un **test de caracterización** —fijar la salida sobre una fixture pequeña— para que el próximo cambio en `clean_text` no dependa de que alguien lo mida a mano en una review. Mientras no exista, la evidencia de que hoy no ha roto nada es la medición de esta review, no la suite.
2. **Las cifras de la ronda 4 no cuadran entre documentos.** Tú das 99,4 % / 98,4 % con 2 decimales y 76,8 % / 93,4 % con 4; el docstring del test dice 99,2 % / 100 % y 94,3 % / 80,1 %; mi medición con tolerancia de un céntimo da 99,6 % / 100 %. La **conclusión es la misma en las tres** (2 decimales reproduce mejor el `~C` que 4, y el umbral del 98 % se cumple con margen), pero convendría que el informe deje una sola cifra con su método, porque es el número que justifica haber revertido una ronda entera.
3. **T15, T24, T32 y T46**: verificaciones MANUALES del humano, pendientes a propósito. **La feature no se cierra como `done` hasta que estén anotadas**, y la de esta ronda (importar en Sigrid y confirmar que `VALV1`, `VALV4`, `VALV5` y `VALV6` ya entran con descompuesto) es la que valida la razón de ser de la ronda 5.
4. **`design.md` está en 250/250 e `impl_F-002.md` en 220/220**: en el tope exacto. La próxima ronda **obliga** a resumir y enlazar; no queda sitio para añadir una línea.
