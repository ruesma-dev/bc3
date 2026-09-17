<!-- specs/F-002-porcentuales-a-ud/requirements.md -->
# F-002 · Requisitos · Porcentuales a UD con cantidad 1

Rigor `critico`. Fuente de datos: `progress/explore_porcentuales.md` (medido
sobre `input/`) y `docs/ARCHITECTURE.md` §«Semántica de dominio», puntos 5-8.

**El matiz que define la feature (literal del humano):** *en el BC3 el
concepto porcentual no trae unitario, lo calcula Presto*. La pasada tiene que
reproducir ese cálculo y materializarlo como precio de un concepto propio.

Alcance fijado por el humano: es una **pasada previa e independiente**
(`.bc3` → `.bc3`) que **solo** toca líneas porcentuales; corre ANTES de
`convert_to_material` y su salida alimenta al resto del ETL.

## Definiciones

- **D1 · Línea porcentual**: tripleta `hijo\factor\rendimiento` de un `~D`
  cuyo `hijo`, tras `strip()`, empieza por `%` **o** cuyo `~C` declara unidad
  `%` (tras unificar). Las dos formas conviven en `input/`.
- **D2 · Importe de línea normal** = `precio(~C del hijo) × factor × rendimiento`.
- **D3 · Base acumulada de una línea** = suma de los importes (D2/D4) de las
  líneas que **le preceden** en ese mismo `~D`, en orden de fichero.
- **D4 · Importe de línea porcentual** = `factor × rendimiento × base acumulada`.
- **D5 · Clon**: concepto nuevo, propio de un par (padre, línea porcentual),
  con unidad `UD`, precio = D4 y tipo `3`.

## Requisitos

R1. El sistema debe clasificar como porcentual toda línea que cumpla D1, y
solo esas.

R2. El sistema debe calcular el importe de cada línea de un `~D` según D2
(normal) o D4 (porcentual), recorriendo las tripletas en el orden del fichero.

R3. CUANDO un `~D` contiene al menos una línea porcentual convertible, el
sistema debe sustituir cada tripleta porcentual por `codigo_clon\1\1` y dejar
intactas —texto igual al de entrada— las tripletas no porcentuales.

R4. El sistema debe emitir, por cada línea porcentual convertida, un `~C` del
clon con unidad `UD`, precio = importe D4, tipo `3`, fecha la del `~C`
porcentual original y resumen el del original (o su código si viene vacío).

R5. El código del clon debe ser `<codigo_padre>.P<n>` (n = ordinal 1-based de
la línea porcentual dentro de su `~D`), recortado por la derecha del código
del padre hasta caber en 20 caracteres, y único frente a: todos los códigos
`~C` del fichero, sus truncados a 20 caracteres y los clones ya emitidos.

R6. El sistema debe redondear el precio del clon a 2 decimales
(`ROUND_HALF_UP`) y acumular en D3 el valor **ya redondeado**, de modo que la
base usada sea la misma que se puede leer en el fichero de salida.

R7. SI el precio del clon resulta `-0`, ENTONCES el sistema debe escribirlo
como `0`, sin notación científica ni coma decimal.

R8. El sistema no debe corregir el residuo de redondeo en ninguna línea: las
líneas no porcentuales salen byte a byte como entraron.

R9. CUANDO **todas** las líneas de un `~D` son porcentuales (base 0) y el `~C`
del padre trae un precio numérico distinto de 0, el sistema debe dar al clon
de la **primera** línea porcentual ese precio del padre, aplicar las
porcentuales siguientes sobre él, y registrar el caso en el informe como
`precio_del_padre_aplicado`.

R10. CUANDO un `~D` tiene al menos una línea no porcentual y su primera línea
porcentual tiene base 0, el sistema debe dar precio `0` al clon (R9 no
aplica). Caso real: `07.02.05` de Siroco.

R11. MIENTRAS una línea no porcentual tenga rendimiento `0`, el sistema debe
dejarla intacta y contarla con importe 0 en D3: no se recalcula ni se borra.

R12. CUANDO una línea porcentual tiene rendimiento `0`, el sistema debe
convertirla igualmente, con precio de clon `0`: el importe sigue siendo 0 y el
concepto `%` desaparece del `~D`.

R13. El sistema debe eliminar del fichero de salida el `~C` de cada concepto
porcentual —y su `~T`, si lo tiene— cuando ninguna tripleta del fichero de
salida lo referencie ya.

R14. SI un concepto porcentual queda referenciado por alguna tripleta no
convertida, ENTONCES el sistema debe conservar su `~C` y su `~T` y registrar
el motivo en el informe.

R15. CUANDO un registro `~M` referencia el par `padre\porcentual` de una línea
convertida, el sistema debe reescribir el par con el código del clon. Caso
real: `33.03.01\%CC` en El Escorial.

R16. SI una línea que precede a una porcentual en su `~D` no tiene `~C` con
precio numérico, ENTONCES el sistema debe dejar ese `~D` **entero** sin
convertir, conservar sus conceptos `%` y registrarlo en el informe como
`base_indeterminada`.

R17. Todo `~D` reescrito debe terminar en `\|` (exactamente una barra antes
del cierre) y todos los códigos que aparezcan en él deben existir como `~C` en
el fichero de salida.

R18. El sistema debe emitir el fichero de salida en `latin-1` y dejar
**idéntica a la entrada** toda línea que no sea (a) un `~D` con porcentuales
convertibles, (b) el `~C`/`~T` de un porcentual eliminado o (c) un `~M`
afectado por R15. No trunca códigos, no fuerza tipos, no unifica unidades, no
intercala `CD#`, no crea clones `.1` y no limpia texto.

R19. **INVARIANTE.** Para cada `~D` del fichero, la suma de los importes
calculados sobre la ENTRADA y la calculada sobre la SALIDA deben coincidir con
tolerancia `0,01 + 0,005 × nº de líneas porcentuales del ~D`, salvo los `~D`
afectados por R9, que el informe lista uno a uno. La comparación es
entrada-contra-salida: **nunca** contra el precio del `~C` del padre (hay
padres con precio fijado a mano que no cuadra con su propio descompuesto:
`170100`, `1701010`, `07.02.01a`).

R20. El sistema debe devolver un informe con: nº de `~D` procesados, nº de
líneas convertidas, nº de conceptos `%` eliminados, y una fila por cada caso
excepcional (R9, R14, R16) con padre, código porcentual, rendimiento e importe.

R21. DONDE la bandera `Settings.porcentuales_a_ud` esté desactivada, el
sistema debe copiar el fichero sin modificar ninguna línea y no insertar el
paso en el pipeline.

R22. El sistema debe poder ejecutarse sobre un fichero suelto, sin el resto
del ETL, indicando entrada y salida.

R23. SI el fichero de entrada no existe, ENTONCES el sistema debe lanzar
`FileNotFoundError` sin crear el fichero de salida.

## Fuera de alcance

- Recalcular el precio del `~C` de los padres (lo hace Presto / el ERP).
- Tocar `convert_to_material`, los clones `.1` o la FASE 2.
- Descompuestos cuyo **padre** sea un concepto porcentual: no existen en
  `input/` (medido); si aparecieran, caen en R16.
