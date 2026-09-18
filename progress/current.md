<!-- progress/current.md -->
# Trabajo en curso

**F-002 · Convertir los descompuestos porcentuales a UD con cantidad 1
conservando el importe de Presto** · rama `feature/F-002-porcentuales-a-ud` ·
rigor `critico` · estado `in_progress`.

**Ronda 5 cerrada (T35-T43, T45): la limpieza del texto.** Elena importó
nuestra salida en Sigrid y cuatro partidas entraron **sin descompuesto**:
`VALV1`, `VALV4`, `VALV5` y `VALV6` («Válvula de bola, ½"», `1¼"`, `1½"`,
`2"`). `VALV2` («3/4"»), del mismo capítulo, entró bien: las distingue el
no-ASCII del resumen. La pasada limpia ahora el resumen de cada `~C`, el texto
de cada `~T` y el de los clones con el `clean_text` de siempre, bajo la bandera
`PORCENTUALES_LIMPIAR_TEXTO` (por defecto true) y `--sin-limpiar-texto`.
Códigos, precios, factores, rendimientos y unidades no se tocan, y con la
bandera apagada la salida sigue siendo byte a byte la de antes.

**Se ha tocado código compartido y conviene saberlo**: `clean_text`
(`utils/text_sanitize.py`) prometía ASCII y dejaba pasar `Ø`, `ø`, `ß` y la mu
de `µ`, porque NFKD no descompone esas letras y todas son `isalnum()`. Se ha
corregido **en ese único sitio**, no duplicando la limpieza, así que **también
cambia la salida de `convert_to_material`** —que escribe para el mismo
Sigrid—: 984 conceptos con descompuesto propio de `input/` salen ya limpios.

**NO se marca `done`**: faltan las CUATRO verificaciones MANUALES del humano
(T15 Siroco, T24 laguna, T32 decimales en Presto y T44 la importación en
Sigrid).

## Las cinco rondas

1. **Ronda 1** — la pasada completa. Aprobada.
2. **Ronda 2** — R9 inflaba las partidas con dos porcentuales (`ICV260`
   336,39 en vez de 291,50). Se rehízo reconstruyendo la base implícita y se
   añadió **R19 bis** (el descompuesto de la salida debe dar el precio del
   `~C` del padre).
3. **Ronda 3** — el humano importó en Presto y el convertido salía **381,64 €
   por encima**. Desglosado: en 8 de los 9 capítulos con diferencia el
   convertido da **exactamente** lo que el `~C` del capítulo declaraba y el
   original no (C18: declara 2.826.961,28, convertido 2.826.961,28, original
   2.826.618,05) — eso es fidelidad recuperada. El defecto real estaba en
   C06: `C020615` pasaba de 172,59 a 172,60 por redondeo y, con medición de
   1.833,59 m², eran 18,34 €. Ahora **los decimales del precio del clon son
   configurables** (`PORCENTUALES_DECIMALES`, por defecto 4, rango 2..6).

4. **Ronda 4** — el contraste anterior se hacía contra un simulador de cálculo
   exacto, no contra Presto. Contra el `~C` declarado, el ganador es 2.
5. **Ronda 5** — el texto: Sigrid no importa el descompuesto de un concepto
   cuyo resumen lleva no-ASCII (R24-R26).

## Pendiente del humano

1. **T15 · Siroco** — `output/siroco_sin_pct.bc3`, que hay que regenerar con
   el defecto nuevo (2). Total y `43.15`, `05.06.29`, `31.04.03.01`,
   `32.03.04.32`.
2. **T24 · laguna** — `output/laguna_sin_pct.bc3`, ídem. `ICV260` = 291,50 e
   `ICV270` = 369,50.
3. **T32 · decimales** — ya no hace falta para decidir el valor (lo decide
   R6 ter con datos), pero sigue siendo útil saber **cuántos decimales acepta
   Presto**: si alguna vez interesa subirlo, es una línea en el `.env`.
4. **T44 · Sigrid** — importar `output/laguna_sin_pct.bc3` **en Sigrid** y
   confirmar que `VALV1`, `VALV4`, `VALV5` y `VALV6` entran ya **con su
   descompuesto**. Es la verificación de la ronda 5 y la única que dice si el
   problema de Elena está resuelto de verdad.
5. **Confirmar en el Presto de Elena cuánto dice C18.** Si 2.826.961,28, el
   convertido es el fiel y la diferencia de 381,64 € es fidelidad recuperada,
   no error.
6. **R13 sobre `input/presupuesto.bc3`**: borra 42 `~C` porcentuales sin
   convertir ninguna línea (banco de precios). Sin decidir.
7. **El suelo de 0,01 de R19** (lo levanta el reviewer): el invariante general
   no ve un error de ~0,009 € por `~D`, del mismo orden que el céntimo que
   costó la ronda 3. Hoy eso lo cubren R19 bis y los tests con números de
   Presto. Si se quiere que R19 también lo cubra, hay que hacer proporcional
   su sumando fijo.

## Para `arnes-base` (regla de propagación)

1. **C4, dobles de test**: extender a los sustitutos puestos con
   `monkeypatch.setattr`, comparando `inspect.signature` con el original.
2. **Una regla que el invariante excluye por diseño necesita su propia
   comprobación**, o queda sin red (le pasó a R9).
3. **Cuando una transformación toca dinero, al menos un test debe comparar la
   salida contra un número de fuera del sistema** —el `~C` del fichero
   original, una captura del ERP— y no solo contra otro resultado de la misma
   función. Los dos defectos de esta feature los encontró el humano
   importando en Presto, y los dos eran errores de regla, no de código.
4. **Una utilidad compartida que promete algo en su docstring necesita un test
   que lo fije.** `clean_text` prometía ASCII desde hace años y no lo cumplía;
   nadie lo vio porque nadie lo había escrito como test.

## Aviso de tamaño

`design.md` 250/250 e `impl_F-002.md` 220/220: en el tope exacto
(`requirements.md`, 138/150, tiene aire). La próxima corrección obliga a
resumir y enlazar.
