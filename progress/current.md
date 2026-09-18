<!-- progress/current.md -->
# Trabajo en curso

**F-002 · Convertir los descompuestos porcentuales a UD con cantidad 1
conservando el importe de Presto** · rama `feature/F-002-porcentuales-a-ud` ·
rigor `critico` · estado `in_progress`.

**Ronda 4 en curso: se revierte el criterio de la ronda 3.** El diagnóstico de
entonces era erróneo y el dato nuevo lo tumba: contrastando contra el precio
que el `~C` de cada partida declara —que lo escribió Presto al exportar—,
**2 decimales acierta 260/262 (99,2 %) en Siroco y 402/402 (100 %) en laguna**,
mientras 4 baja a 94,3 % y 80,1 %. Presto redondea a céntimos CADA línea del
descompuesto, así que más precisión se aleja de su resultado. Y `C020615`, el
caso que motivó la ronda 3, declara **172,6** en su propio `~C`: el 172,59 de
4 decimales salía de un cálculo exacto, no de Presto.

El defecto vuelve a **2** y queda sujeto por **R6 ter**, el test que compara
contra ese número ajeno con umbral del 98 %. Toda la infraestructura de la
ronda 3 se queda: `PORCENTUALES_DECIMALES`, el rango 2..6, `--decimales` y el
formateo sin ceros de relleno.

**NO se marca `done`**: faltan las TRES verificaciones MANUALES del humano
(T15 Siroco, T24 laguna, T32 decimales en Presto).

## Las tres rondas

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

## Pendiente del humano

1. **T15 · Siroco** — `output/siroco_sin_pct.bc3`, que hay que regenerar con
   el defecto nuevo (2). Total y `43.15`, `05.06.29`, `31.04.03.01`,
   `32.03.04.32`.
2. **T24 · laguna** — `output/laguna_sin_pct.bc3`, ídem. `ICV260` = 291,50 e
   `ICV270` = 369,50.
3. **T32 · decimales** — ya no hace falta para decidir el valor (lo decide
   R6 ter con datos), pero sigue siendo útil saber **cuántos decimales acepta
   Presto**: si alguna vez interesa subirlo, es una línea en el `.env`.
4. **Confirmar en el Presto de Elena cuánto dice C18.** Si 2.826.961,28, el
   convertido es el fiel y la diferencia de 381,64 € es fidelidad recuperada,
   no error.
5. **R13 sobre `input/presupuesto.bc3`**: borra 42 `~C` porcentuales sin
   convertir ninguna línea (banco de precios). Sin decidir.
6. **El suelo de 0,01 de R19** (lo levanta el reviewer): el invariante general
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

## Aviso de tamaño

`requirements.md` 150/150 y `design.md` 250/250: en el tope exacto. La
próxima corrección obliga a resumir y enlazar.
