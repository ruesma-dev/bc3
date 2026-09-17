<!-- progress/current.md -->
# Trabajo en curso

**F-002 · Convertir los descompuestos porcentuales a UD con cantidad 1
conservando el importe de Presto** · rama `feature/F-002-porcentuales-a-ud` ·
rigor `critico` · estado `in_progress`.

**Reviewer: APPROVED** en la pasada 3 (incremental desde `4af79bf`) →
`progress/review_F-002.md`. Implementación: `progress/impl_F-002.md`.
`bash harness/init.sh` en verde: **457 tests**, cobertura **98,7 %** de las
389 líneas cambiadas, campaña de mutación completa **168/168 sin
supervivientes**.

**NO se marca `done`**: faltan las dos verificaciones MANUALES del humano
(T15, T24 y T32). Es la condición que pusieron el reviewer y el líder.

## Las dos rondas, y por qué hubo una segunda

La ronda 1 se aprobó con una R9 que solo era correcta cuando el `~D` tenía
UNA línea porcentual. Con dos, el precio del padre —que ya es el final, con
los porcentajes dentro— se asignaba al primer clon y el segundo porcentaje se
volvía a aplicar encima: `ICV260` salía **336,39** en vez de 291,50, un
+15,4 %. Lo detectó el líder verificando la salida real, no la suite.

R9 se rehízo **reconstruyendo la base implícita** (`base = P / Π(1 + r_i)`,
línea `<padre>.P0` más una línea por porcentual, residuo absorbido en la
base), y se añadió **R19 bis**, que es la comprobación que faltaba: el importe
calculado sobre la SALIDA tiene que ser igual al precio del `~C` del padre.
El reviewer reprodujo el experimento restaurando el código viejo con los tests
nuevos y confirmó el rojo `ICV260: 336.39 != 291.50`.

## Verificación independiente del líder (2026-09-18, sobre la salida real)

| Presupuesto | Resto del presupuesto (entrada → salida) | Desvío |
|---|---|---|
| Siroco | 7.814.258,82 → 7.814.258,85 | **+0,03 €** |
| laguna (Elena) | 24.052.016,95 → 24.052.017,12 | **+0,17 €** |

Céntimos de redondeo sobre 7,8 M€ y 24 M€. Y las cuatro partidas de R9, que
antes no cuadraban con su propio descompuesto, ahora sí:

| Partida | Descompuesto de la salida | Precio del `~C` |
|---|---|---|
| `31.04.03.01` | 1.100,00 | 1.100,00 |
| `32.03.04.32` | 1.117,65 | 1.117,65 |
| `ICV260` | 291,50 | 291,50 |
| `ICV270` | 369,50 | 369,50 |

## Pendiente del humano

1. **T15 · Siroco.** Fichero ya generado con la regla NUEVA:
   `output/siroco_sin_pct.bc3` (262 `~D` con porcentual, 319 líneas
   convertidas, 21 conceptos eliminados). Importar en Presto y confirmar el
   total y `43.15`, `05.06.29`, `31.04.03.01`, `32.03.04.32`.
2. **T24 · laguna.** Fichero ya generado: `output/laguna_sin_pct.bc3` (406
   `~D`, 948 líneas convertidas). Confirmar `ICV260` = 291,50 (no 336,39) e
   `ICV270` = 369,50.
3. **T32 · decimales que traga Presto.** La salida ya sale con **4 decimales**
   en el precio de los clones (`PORCENTUALES_DECIMALES`, por defecto 4). Al
   importar en Presto hay que anotar **cuántos decimales acepta**: si admite 6,
   se sube el valor en el `.env` —una línea, sin tocar código ni spec— y se
   repite la importación. Con 2 decimales el desvío medido era de +165 € en
   Siroco y +143 € en laguna; con 4 baja a +1,50 € y +3,39 €, y con 6 es cero.
   Caso de referencia: `C020615` de laguna, 172,59 con 4 decimales frente a
   172,60 con 2, sobre una medición de 1.833,59 m².
4. **R13 sobre `input/presupuesto.bc3`**: borra 42 `~C` porcentuales sin
   convertir ninguna línea, por ser un banco de precios. Sin decidir.
5. **Porte a `arnes-base`** de las dos lecciones del arnés que deja esta
   feature (ver abajo). Sin hacer.
6. **La semántica de dominio de `docs/ARCHITECTURE.md`** sigue sin validación
   humana formal.

## Para `arnes-base` (regla de propagación del CLAUDE.md del ecosistema)

1. **C4, dobles de test**: extender el punto a los sustitutos instalados con
   `monkeypatch.setattr`, comparando `inspect.signature` con la del símbolo
   sustituido. Aquí un `**kwargs` de más dejó sin probar la única línea que
   corre en producción, y ni la cobertura del 98 % ni la mutación lo
   delataron.
2. **Lección de la ronda 2**: una regla que el invariante excluye por diseño
   necesita su propia comprobación, o queda sin red. R9 estaba fuera de R19
   y por eso el defecto sobrevivió a una implementación y a una revisión
   completas.

## Incidencias de la sesión

Dos agentes se colgaron (watchdog a los 10 min): el implementer durante la
primera campaña de mutación de la ronda 2 —dejó cuatro worktrees huérfanos en
`%TEMP%`, limpiados con `git worktree prune`— y el reviewer al empezar la
pasada 3. Los dos se retomaron sin perder trabajo.
