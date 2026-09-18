<!-- progress/current.md -->
# Trabajo en curso

**F-002 · Porcentuales a UD** · rama `feature/F-002-porcentuales-a-ud` ·
rigor `critico` · estado `in_progress`.

**Reviewer: APPROVED** en la pasada 5 (incremental desde `12fbb9e`).
`bash harness/init.sh` en verde: 538 tests, cobertura 98,9 % de 454 líneas
cambiadas, campaña 198/198 sin supervivientes.

**NO se marca `done`**: faltan las CUATRO verificaciones MANUALES del humano
(T15 Siroco, T24 laguna, T32 decimales, T46 Sigrid).

## Cinco rondas, y qué las provocó

| Ronda | Qué pasó | Quién lo detectó |
|---|---|---|
| 1 | La pasada completa | — |
| 2 | R9 inflaba las partidas con dos porcentuales (`ICV260` 336,39 vs 291,50) | el líder, verificando la salida |
| 3 | Subir a 4 decimales · **diagnóstico erróneo del líder** | — |
| 4 | Vuelta a 2 decimales: reproduce el `~C` declarado por Presto en el 99,4 % frente al 76,8 % de 4 | el líder, al contrastar contra el `~C` |
| 5 | Limpieza de texto: Sigrid no importa el descompuesto de los conceptos con no-ASCII en el resumen | **Elena Díaz, importando en Sigrid** |

Las tres veces que algo estaba mal, lo encontró alguien mirando el resultado
real, no la suite. Es la lección que va a `arnes-base`.

## El cambio en código compartido (ronda 5)

`utils/text_sanitize.clean_text` prometía ASCII y no lo cumplía: NFKD no
descompone `Ø`, `ø`, `æ`, `ß` y pasa `µ` a mu griega. Arreglado en su sitio,
así que **cambia también la salida de `convert_to_material`**. El reviewer lo
midió sobre El Escorial: 41 líneas distintas, **38 caracteres no-ASCII → 0**, y
sin romper referencias (207 códigos de `~D` sin `~C`, antes y después).
Evidencia de que el fichero ya salía sucio del ETL:
`input/COSTE_250128_Siroco_Rv4mlo_limpio.bc3` conserva 7 `Ø`.

## Pendiente del humano

1. **T15 · Siroco** — `output/siroco_sin_pct.bc3`.
2. **T24 · laguna** — `output/laguna_sin_pct.bc3`: `ICV260` = 291,50.
3. **T32 · decimales** — confirmar que Presto acepta los precios tal cual.
4. **T46 · Sigrid** — importar y confirmar que `VALV1`, `VALV4`, `VALV5` y
   `VALV6` entran **con** su descompuesto. Valida la ronda 5.
5. **R13 sobre `input/presupuesto.bc3`**: borra 42 `~C` porcentuales sin
   convertir ninguna línea (banco de precios). Sin decidir.
6. **El suelo de 0,01 de R19**: el invariante general no ve errores de
   ~0,009 € por `~D`. Hoy eso lo cubren R19 bis y los tests con números de
   Presto.

## Deuda que deja esta feature (propuestas del reviewer)

1. **`convert_to_material` no tiene ni un test** y esta ronda le ha cambiado
   la salida. Feature aparte con un **test de caracterización** sobre fixture
   pequeña: hoy la evidencia de que no ha roto nada es una medición manual de
   review, no la suite.
2. **Las cifras de la ronda 4 no cuadran entre documentos** (99,4 % del
   líder con igualdad exacta, 99,2 % del docstring, 99,6 % del reviewer con
   tolerancia de céntimo). Misma conclusión, tres métodos: conviene dejar una
   sola cifra con su método, porque es el número que justificó revertir una
   ronda entera.
3. **`design.md` 250/250 e `impl_F-002.md` 220/220**: en el tope exacto. La
   próxima ronda obliga a resumir y enlazar.

## Para `arnes-base` (regla de propagación)

1. **C4, dobles de test**: extender a los sustitutos puestos con
   `monkeypatch.setattr`, comparando `inspect.signature` con el original.
2. **Una regla que el invariante excluye por diseño necesita su propia
   comprobación**, o queda sin red (le pasó a R9).
3. **Cuando una transformación toca dinero, al menos un test debe comparar
   contra un número de fuera del sistema** —el `~C` del fichero, una captura
   del ERP—, no contra otro resultado de la misma función.
