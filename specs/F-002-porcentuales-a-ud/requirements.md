<!-- specs/F-002-porcentuales-a-ud/requirements.md -->
# F-002 · Requisitos · Porcentuales a UD con cantidad 1

Rigor `critico`. Fuente: `progress/explore_porcentuales.md` (medido sobre
`input/`) y `docs/ARCHITECTURE.md` §«Semántica de dominio», puntos 5-8.

**El matiz que define la feature (literal del humano):** *en el BC3 el
concepto porcentual no trae unitario, lo calcula Presto*. La pasada tiene que
reproducir ese cálculo y materializarlo como precio de un concepto propio.

Alcance fijado por el humano: **pasada previa e independiente** (`.bc3` →
`.bc3`) que **solo** toca líneas porcentuales, ANTES de `convert_to_material`.

## Definiciones

- **D1 · Línea porcentual**: tripleta `hijo\factor\rendimiento` cuyo `hijo`,
  tras `strip()`, empieza por `%` **o** cuyo `~C` declara unidad `%` (tras
  unificar). Las dos formas conviven en `input/`.
- **D2 · Importe de línea normal** = `precio(~C del hijo) × factor × rendimiento`.
- **D3 · Base acumulada** = suma de los importes (D2/D4) de las líneas que **le
  preceden** en ese mismo `~D`, en orden de fichero.
- **D4 · Importe de línea porcentual** = `factor × rendimiento × base acumulada`.
- **D5 · Clon**: concepto nuevo de un par (padre, línea porcentual), unidad
  `UD`, precio = D4, tipo `3`.

## Requisitos

R1. El sistema debe clasificar como porcentual toda línea que cumpla D1, y
solo esas.

R2. El sistema debe calcular el importe de cada línea de un `~D` según D2 o D4,
recorriendo las tripletas en el orden del fichero.

R3. CUANDO un `~D` contiene al menos una línea porcentual convertible, el
sistema debe sustituir cada tripleta porcentual por `codigo_clon\1\1` y dejar
intactas —texto igual al de entrada— las no porcentuales.

R4. El sistema debe emitir, por cada línea porcentual convertida, un `~C` del
clon con unidad `UD`, precio = importe D4, tipo `3`, la fecha del `~C`
porcentual original y su resumen (o su código si viene vacío).

R5. El código del clon debe ser `<codigo_padre>.P<n>` (n = ordinal 1-based de
la porcentual dentro de su `~D`), recortado por la derecha del código del padre
hasta caber en 20 caracteres, y único frente a todos los códigos `~C` del
fichero, sus truncados a 20 y los clones ya emitidos. La línea de base de R9 es
el ordinal `0` de esa familia, `<codigo_padre>.P0`, con las mismas reglas.

R6. El sistema debe redondear el precio del clon (`ROUND_HALF_UP`) al número de
decimales que diga `Settings.porcentuales_decimales` —por defecto **4**, leído
de `.env` como `PORCENTUALES_DECIMALES`— y acumular en D3 el valor **ya
redondeado**, de modo que la base usada sea la que se puede leer en la salida.
Con 2 decimales el desvío medido llega a 165 € en Siroco y 143 € en laguna,
siempre al alza (tabla en `design.md` §Decimales del precio del clon).

R6 bis. SI `porcentuales_decimales` cae fuera del rango **2..6** o no es
numérico, ENTONCES el sistema debe usar el 4 por defecto y avisar por log. 2 es
el mínimo (lo que traen los `~C` originales) y 6 el máximo (desvío ya cero).

R7. El sistema debe escribir el precio del clon en notación decimal con punto,
sin notación científica, sin ceros de relleno a la derecha cuando el valor es
exacto (`7,41` se escribe `7.41`, no `7.4100`) y con `0` en lugar de `-0`.

R8. El sistema no debe corregir el residuo de redondeo en ninguna línea no
porcentual: esas salen byte a byte como entraron.

R9. CUANDO **todas** las líneas de un `~D` son porcentuales (base 0) y el `~C`
del padre trae un precio numérico `P` ≠ 0, el sistema debe **reconstruir la base
implícita** despejándola hacia atrás, `base = P / Π(1 + r_i)` sobre todos los
rendimientos `r_i` del `~D`, y escribir un `~D` con una **línea de base**
(`<padre>.P0`, factor 1, rendimiento 1, precio = base) más una línea por cada
porcentual, calculada con D4 sobre el acumulado; se registra como
`base_reconstruida`. El precio del padre YA es el final, con los porcentajes
dentro (motivo en `design.md` §D3).

R9 bis. El sistema debe redondear la base con el criterio de R6 (mismos
decimales, `ROUND_HALF_UP`), calcular con ella los importes porcentuales y
**absorber el residuo en la línea de base**:
`precio(.P0) = P − Σ(importes porcentuales redondeados)`. Ninguna porcentual se
retoca: la suma del `~D` da exactamente `P` con cualquier valor de decimales.

R9 ter. SI algún `(1 + r_i)` de ese `~D` es 0 o negativo —descuento del −100 %
o mayor, que hace la división imposible o absurda—, ENTONCES el sistema no debe
convertir ese `~D`: lo deja intacto, conserva sus `%` y lo registra como
`base_no_despejable`.

R10. CUANDO un `~D` tiene al menos una línea no porcentual y su primera
porcentual tiene base 0, el clon recibe precio `0` y R9 no aplica (`07.02.05`).

R11. MIENTRAS una línea no porcentual tenga rendimiento `0`, el sistema debe
dejarla intacta y contarla con importe 0 en D3: ni se recalcula ni se borra.

R12. CUANDO una línea porcentual tiene rendimiento `0`, el sistema debe
convertirla igualmente con precio de clon `0`: el importe sigue siendo 0 y el
`%` desaparece del `~D`.

R13. El sistema debe eliminar de la salida el `~C` de cada concepto porcentual
—y su `~T`, si lo tiene— cuando ninguna tripleta de la salida lo referencie ya.

R14. SI un concepto porcentual queda referenciado por alguna tripleta no
convertida, ENTONCES el sistema debe conservar su `~C` y su `~T` y registrarlo.

R15. CUANDO un `~M` referencia el par `padre\porcentual` de una línea
convertida, el par se reescribe con el clon (real: `33.03.01\%CC`, Escorial).

R16. SI una línea que precede a una porcentual en su `~D` no tiene `~C` con
precio numérico, ENTONCES el `~D` sale **entero** sin convertir, conserva sus
`%` y se registra como `base_indeterminada`.

R17. Todo `~D` reescrito debe terminar en `\|` (una sola barra antes del
cierre) y todos sus códigos deben existir como `~C` en la salida.

R18. El sistema debe emitir la salida en `latin-1` y dejar **idéntica a la
entrada** toda línea que no sea (a) un `~D` con porcentuales convertibles,
(b) el `~C`/`~T` de un porcentual eliminado o (c) un `~M` afectado por R15. No
trunca códigos, no fuerza tipos, no unifica unidades, no intercala `CD#`, no
crea clones `.1` y no limpia texto.

R19. **INVARIANTE.** Para cada `~D` del fichero, la suma de los importes
calculados sobre la ENTRADA y sobre la SALIDA deben coincidir con tolerancia
`0,01 + nº de líneas porcentuales × 10^(−d) / 2`, siendo `d` los decimales de
R6 (con `d = 2` da la tolerancia original de 0,005 por línea), salvo los `~D`
afectados por R9, que el informe lista uno a uno. La comparación es
entrada-contra-salida: **nunca** contra el precio del `~C` del padre (hay
padres con precio fijado a mano que no cuadra con su descompuesto: `170100`,
`1701010`, `07.02.01a`).

R19 bis. **INVARIANTE de los `~D` de R9.** No se pueden comparar
entrada-contra-salida (su entrada calcula 0), pero el importe calculado sobre
la **SALIDA** debe ser igual al precio `P` del `~C` del padre ± un céntimo, con
cualquier valor de `d`. Es obligatorio: es lo que caza el defecto de la R9
anterior (`ICV260` habría salido 336,39 ≠ 291,50).

R20. El sistema debe devolver un informe con: nº de `~D` procesados, líneas
convertidas, conceptos `%` eliminados, los decimales usados, y una fila por caso
excepcional (R9, R9 ter, R14, R16) con padre, código, rendimiento e importe.

R21. DONDE la bandera `Settings.porcentuales_a_ud` esté desactivada, el sistema
debe copiar el fichero sin tocar ninguna línea y no insertar el paso.

R22. El sistema debe poder ejecutarse sobre un fichero suelto, sin el resto del
ETL, indicando entrada y salida.

R23. SI el fichero de entrada no existe, ENTONCES el sistema debe lanzar
`FileNotFoundError` sin crear el fichero de salida.

## Fuera de alcance

- Recalcular el precio del `~C` de los padres (lo hace Presto / el ERP).
- Tocar `convert_to_material`, los clones `.1` o la FASE 2.
- Descompuestos cuyo **padre** sea porcentual: no existen (medido) → R16.
