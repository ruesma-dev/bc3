<!-- specs/SPECS.md -->
# Formato de especificaciones (SDD)

Cada feature con `sdd: true` tiene una carpeta `specs/F-XXX-slug/` con tres
ficheros. El spec-author los crea; el humano los aprueba; el implementer los
ejecuta; el reviewer valida contra ellos.

## Topes de tamaño (obligatorios)

Cada línea de una spec se paga **tres veces**: la escribe el spec-author, la lee
el implementer y la relee el reviewer. Por eso el papeleo tiene tope:

| Fichero | Tope |
|---|---|
| `requirements.md` | **150** líneas |
| `design.md` | **250** líneas |
| `tasks.md` | sin tope duro, pero **una tarea por línea** |
| `progress/impl_F-XXX.md` | **220** líneas |
| `progress/review_F-XXX.md` | **140** líneas |

Los números viven en el bloque `tamano` de `harness/rigor.json` y los mide
`python -m harness.tamano --feature F-XXX`, que `bash harness/init.sh` ejecuta
sobre la feature en curso (sección 7 quater): pasarse **pone el portero en
rojo**.

Los cuatro se **ampliaron el 2026-08-20** (eran 120 / 200 / 150 / 100). En el
repositorio donde se calibraron, la mediana histórica era ~484 líneas en los
informes de implementer y ~475 en los de review: los valores originales
recortaban un ~70 % y se notó —el primer implementer que los aplicó entregó
150/150 y su reviewer 100/100, los dos clavados en el límite—. Con los actuales
el recorte sigue siendo del ~55 %, así que el ahorro se mantiene, pero deja
aire para lo que no conviene que nadie resuma: las **trazas de fase RED** y el
**análisis de supervivientes**.

Son **topes, no objetivos**, y recortar no puede significar tirar evidencia:
**lo que no cabe se resume y se enlaza** al fichero donde vive el detalle (el
informe de mutación, la traza completa, el documento de referencia). Un tope no
justifica omitir una traza de fase RED ni el análisis de un superviviente.

## 1. requirements.md — notación EARS (tope: 150 líneas)

Cada requisito con id `R1, R2...` y una de estas plantillas:

- **Ubicuo**: "El sistema debe <comportamiento>."
- **Dirigido por evento**: "CUANDO <evento>, el sistema debe <respuesta>."
- **Dirigido por estado**: "MIENTRAS <estado>, el sistema debe <respuesta>."
- **Comportamiento no deseado**: "SI <condición de error>, ENTONCES el sistema
  debe <respuesta>."
- **Opcional**: "DONDE <feature está activa>, el sistema debe <respuesta>."

Regla de oro: cada R se traduce a >= 1 test. Si no puedes imaginar el test,
el requisito está mal escrito.

Ejemplo:
> R1. CUANDO el usuario ejecuta `python main.py version`, el sistema debe
> imprimir la versión semántica y salir con código 0.

## 2. design.md — diseño técnico (tope: 250 líneas)

Secciones obligatorias:

- **Ficheros a crear** (ruta exacta) y **ficheros a modificar** (ruta + qué
  cambia).
- **Ficheros que NO se tocan** (los colindantes que podrían tentarte).
- **Clases/funciones**: firma y responsabilidad, capa hexagonal a la que
  pertenecen (domain / application / infrastructure).
- **SQL** (si aplica): capa/esquema al que pertenece, numeración del
  fichero siguiendo la convención `NN_nombre.sql`, tablas/vistas afectadas.
- **Riesgos y decisiones**: alternativas descartadas y por qué.

## 3. tasks.md — lista de tareas atómicas (una tarea por línea)

```
- [ ] T1: <acción concreta>  |  Verificación: <test o comando>
- [ ] T2: ...
```

- Ordenadas por dependencia. Tests antes o junto a la implementación.
- Cada tarea = un commit (`F-XXX Tn: ...`).
- La última tarea es siempre: "Ejecutar `bash harness/init.sh` en verde".
- Si algo solo puede verificarse contra BBDD real, marcarlo
  `Verificación: MANUAL (humano)` y describir el comando exacto.
