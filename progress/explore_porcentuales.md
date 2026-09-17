<!-- progress/explore_porcentuales.md -->
# Exploración · cómo calcula Presto los conceptos porcentuales (F-002)

Fecha: 2026-09-17 · Solo lectura sobre `input/`. Método: recalcular cada
descompuesto que contiene una línea porcentual y comparar con el precio del
`~C` del padre.

## 1. La base del porcentaje son las líneas ANTERIORES del mismo `~D`

Contrastadas dos hipótesis sobre 373 descompuestos con porcentual y precio
de padre no nulo:

| Hipótesis | Siroco | El Escorial |
|---|---|---|
| **A · base = líneas que le preceden en ese `~D`** | 258 / 262 | 107 / 111 |
| B · base = descompuesto entero (líneas no porcentuales) | 211 / 262 | 91 / 111 |

Hay **67 descompuestos discriminantes** (A y B dan resultados distintos) y
en todos ellos gana A. Ejemplos: `07.02.05` (padre 16,00 · A 16,00 ·
B 15,20), `30.02.06` (padre 1.104,98 · A 1.104,975 · B 1.148,25),
`28.12.1` (padre 7.288,79 · A 7.288,787 · B 7.988,787).

**Conclusión: la duda (1) de F-002 queda cerrada por datos.** El importe de
una línea porcentual es `rendimiento × (suma de importes de las líneas que
le preceden en su `~D`)`, y un porcentual posterior ve en su base el importe
del porcentual anterior (`43.15` encadena `%SUB25`, `%SUB20` y `%SUB10`).

## 2. Dos trampas que la spec debe tratar explícitamente

**a) Descompuestos cuya ÚNICA línea es un porcentual.** `31.04.03.01`
(padre 1.100,00, solo `%SUB2.5`) y `32.03.04.32` (padre 1.117,65, solo
`%SUB17`). La base es 0, así que el importe calculado es 0 y el precio del
padre **no sale de su descompuesto**: está puesto a mano. Si la
transformación deja un clon a precio 0 y algo recalcula el padre, esas dos
partidas se van a cero. Hay que decidir qué se hace y dejarlo por escrito.

**b) El precio del `~C` del padre no siempre cuadra con su descompuesto.**
`170100` (24.308,23 en el `~C`, 27.187,28 calculado), `1701010` (3.105,90 vs
3.392,51), `07.02.01a` (9,35 vs 9,82). Son precios fijados en Presto que no
se recalcularon. **Consecuencia para el test del invariante: no se compara
contra el precio del `~C` del padre, se compara el importe calculado ANTES
con el calculado DESPUÉS de la transformación.** Comparar contra el `~C`
haría fallar la suite por un dato que ya venía torcido.

El resto de desvíos observados son de redondeo (|desvío| ≤ 0,014) y fijan la
tolerancia a usar: 2 decimales por línea, con holgura de céntimo y medio al
comparar un descompuesto entero.

## 3. Detalle que confirma el formato

En estos dos presupuestos el porcentual **no lleva unidad `%`**: la unidad
viene vacía y lo que lo delata es el código (`%RF`, `%VID`, `%SUB10`,
`% BATACHES` —con espacio—). La unidad `%` sí aparece en bancos de precios
(`input/presupuesto.bc3`, conceptos `%MA0100`, `%PM0020`). La detección debe
aceptar las dos formas.

## 4. La captura de Presto de Elena Díaz (correo «BC3», 2026-09-17 11:46)

Muestra el descompuesto `1ICQ` (SALA PRODUCCIÓN CLIMATIZACIÓN) con tres
líneas porcentuales seguidas. Presto las pinta así:

| línea | cantidad | precio | importe |
|---|---|---|---|
| `ICQ070.1` (vaso de expansión) | 1,000 | 8.503,54 | 8.503,54 |
| `%%jefec` | 85,035 | 11,78 | 1.001,72 |
| `%%gast` (seinsa 4%) | 95,053 | — | 0 |
| `%%bene` (beneficio) | 95,053 | 15,40 | 1.463,81 |

Dos cosas que se leen directamente de esos números:

**a) Confirma el encadenamiento.** `85,035 × 100 = 8.503,5`, que es el importe
de la línea inmediatamente anterior; y `95,053 × 100 = 9.505,3 = 8.503,54 +
1.001,72`, es decir, la base de `%%bene` **ya incluye el importe de
`%%jefec`**. Es exactamente el comportamiento medido en `43.15` de Siroco.

**b) En la interfaz de Presto el porcentual se ve como una línea normal:**
cantidad × precio = importe, donde la **cantidad es la base dividida entre
100** y el **precio es el porcentaje** (11,78 %). O sea, Presto ya ha
resuelto el cálculo y lo muestra materializado.

**Lo que queda por comprobar, y condiciona el diseño:** cómo viaja eso al
`.bc3` exportado. Si el `~D` lleva `rendimiento = base/100` (85.035) y el `~C`
del porcentual lleva `precio = porcentaje` (11.78), entonces en ESE fichero el
importe es `precio × rendimiento` —una multiplicación normal— y aplicar la
fórmula D4 (rendimiento × base acumulada) daría un disparate. En Siroco y El
Escorial la codificación es la otra: `~C` con precio `-2` y rendimiento
`-0.02`, cuyo producto (0,04) no es el importe, y donde D4 sí acierta.

Pueden convivir dos codificaciones. Hace falta el fichero
`lagunamodificado16julio.bc3` (adjunto de ese correo) para decidirlo: no se ha
podido descargar desde el correo, lo tiene que dejar el humano en `input/`.

Nota: el otro grupo de imágenes del correo es la firma corporativa, sin
información técnica.

### 4 bis · Resuelto con el fichero de Elena (2026-09-17)

`input/lagunamodificado16julio.bc3` ya está en el repositorio y **no hay dos
codificaciones**: lo de `cantidad = base/100` es solo cómo Presto lo PINTA en
pantalla; al `.bc3` viaja igual que en Siroco y El Escorial.

El propio descompuesto de la captura:

```
~D|1000080|1000080.1\1\1\%%jefedeobrayencayGF4%\1\0.1178\%%gastosfinancieros\1\0\%%beneficioseinsa\1\0.154\|
~C|%%jefedeobrayencayGF4%||14 meses x 12.000...|11.78|150626|0|
```

El rendimiento es `0.1178` (tanto por uno) y el `~C` trae `11.78` (el
porcentaje), exactamente el patrón ya conocido. Ninguna de las 402 líneas
porcentuales del fichero tiene rendimiento > 1,5, que es lo que habría
delatado una base cableada.

Validación sobre el fichero entero: **400 de 402** descompuestos cuadran con
la hipótesis A (base = líneas anteriores) frente a 133 con la B, y hay **267
casos discriminantes**, todos a favor de A. Sumado a Siroco y El Escorial, la
regla se sostiene en 765 de 775 descompuestos, con 334 casos discriminantes y
ni uno en contra.

La aritmética de la captura sale clavada con esa regla: base 8.503,54 →
`%%jefe` 0,1178 × 8.503,54 = 1.001,72, y `%%bene` 0,154 × (8.503,54 +
1.001,72) = 1.463,81. Son los importes que muestra Presto.

**Conclusión: `requirements.md` D3/D4 quedan confirmados, la spec no cambia.**
