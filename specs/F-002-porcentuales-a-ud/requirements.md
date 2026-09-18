<!-- specs/F-002-porcentuales-a-ud/requirements.md -->
# F-002 · Requisitos · Porcentuales a UD con cantidad 1

Rigor `critico`. Fuente: `progress/explore_porcentuales.md` (medido sobre
`input/`) y `docs/ARCHITECTURE.md` §«Semántica de dominio», puntos 5-8.

**El matiz que define la feature (literal del humano):** *en el BC3 el concepto
porcentual no trae unitario, lo calcula Presto*. La pasada reproduce ese
cálculo y lo materializa como precio de un concepto propio. Alcance: **pasada
previa e independiente** (`.bc3` → `.bc3`) que **solo** toca líneas
porcentuales, ANTES de `convert_to_material`.

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
decimales que diga `Settings.porcentuales_decimales` —por defecto **2**, leído
de `.env` como `PORCENTUALES_DECIMALES`— y acumular en D3 el valor **ya
redondeado**. El criterio NO es minimizar el error de redondeo sino
**reproducir lo que calcula Presto**, que es lo que pide la feature: contra el
precio que el `~C` de cada partida declara, 2 decimales acierta el 99,2 % de
Siroco y el 100 % de laguna, y 4 baja al 94,3 % y al 80,1 %
(`design.md` §Decimales del precio del clon).

R6 bis. SI `porcentuales_decimales` cae fuera del rango **2..6** o no es
numérico, ENTONCES el sistema debe usar el defecto y avisar por log.

R6 ter. **INVARIANTE.** Sobre cada `.bc3` de `input/`, el importe de cada `~D`
con porcentual en la SALIDA debe dar el precio del `~C` de su padre en el
**98 %** o más de las partidas, excluidas las de precio puesto a mano (los
cuatro de R9 y `07.02.01a`). Única comprobación contra un número ajeno.

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

R10. CUANDO un `~D` tiene una línea no porcentual y su primera porcentual tiene
base 0, el clon recibe precio `0` y R9 no aplica (`07.02.05`).

R11. MIENTRAS una línea no porcentual tenga rendimiento `0`, se deja intacta y
cuenta con importe 0 en D3: ni se recalcula ni se borra.

R12. CUANDO una línea porcentual tiene rendimiento `0`, se convierte igual con
precio `0`: el importe sigue siendo 0 y el `%` desaparece del `~D`.

R13. El `~C` de cada porcentual —y su `~T`— se elimina de la salida cuando
ninguna tripleta de la salida lo referencie ya.

R14. SI un porcentual sigue referenciado por una tripleta no convertida,
ENTONCES se conservan su `~C` y su `~T` y se registra.

R15. CUANDO un `~M` referencia el par `padre\porcentual` de una línea
convertida, el par se reescribe con el clon (real: `33.03.01\%CC`, Escorial).

R16. SI una línea previa a una porcentual no tiene `~C` con precio numérico,
ENTONCES el `~D` sale **entero** sin convertir, conserva sus `%` y se registra
como `base_indeterminada`.

R17. Todo `~D` reescrito debe terminar en `\|` (una sola barra antes del
cierre) y todos sus códigos deben existir como `~C` en la salida.

R18. El sistema debe emitir la salida en `latin-1` y dejar **idéntica a la
entrada** toda línea que no sea (a) un `~D` con porcentuales convertibles,
(b) el `~C`/`~T` de un porcentual eliminado o (c) un `~M` afectado por R15. No
trunca códigos, ni fuerza tipos, ni unifica unidades, ni intercala `CD#`, ni
crea clones `.1`, ni limpia texto.

R19. **INVARIANTE.** Para cada `~D`, la suma de los importes calculados sobre
la ENTRADA y sobre la SALIDA deben coincidir con tolerancia
`0,01 + nº de porcentuales × 10^(−d) / 2` (`d` = decimales de R6), salvo los
`~D` de R9, que el informe lista uno a uno. Va `~D` a `~D` y **no** partida a
partida contra su `~C`: eso es R6 ter, que por eso admite excepciones
(`170100`, `1701010`, `07.02.01a` tienen el precio puesto a mano).

R19 bis. **INVARIANTE de los `~D` de R9.** No valen entrada-contra-salida (su
entrada calcula 0): el importe sobre la **SALIDA** debe dar el precio `P` del
`~C` del padre ± un céntimo, con cualquier `d`. Es lo que caza el defecto de la
R9 anterior (`ICV260` habría salido 336,39 ≠ 291,50).

R20. El informe debe traer: nº de `~D` procesados, líneas convertidas,
conceptos `%` eliminados, decimales usados y una fila por caso excepcional
(R9, R9 ter, R14, R16) con padre, código, rendimiento e importe.

R21. DONDE `Settings.porcentuales_a_ud` esté desactivada, el sistema debe
copiar el fichero sin tocar ninguna línea y no insertar el paso.

R22. El sistema debe poder ejecutarse sobre un fichero suelto, sin el resto del
ETL, indicando entrada y salida.

R23. SI la entrada no existe, ENTONCES debe lanzar `FileNotFoundError` sin
crear el fichero de salida.

## Fuera de alcance

Recalcular el precio del `~C` de los padres (lo hace Presto / el ERP) y tocar
`convert_to_material`, los clones `.1` o la FASE 2.
