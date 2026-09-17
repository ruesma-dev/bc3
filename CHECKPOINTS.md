<!-- CHECKPOINTS.md -->
# CHECKPOINTS — Evaluación del estado final

> No se evalúa el camino, se evalúa el destino. El reviewer recorre estos
> checkboxes al cerrar cada feature y rechaza el cierre si queda alguno
> vacío en C1–C5. El humano puede usarlos igual antes de mergear a dev.

> **Features `sdd=false`.** No tienen `specs/F-XXX-slug/`, así que léase
> `acceptance` de `harness/features.json` donde estos checkpoints digan
> «requisito EARS» o «spec», y `F-XXX: <descripción>` como formato mínimo de
> mensaje de commit donde digan `F-XXX Tn:`. Lo que dependa de `tasks.md` es
> N/A. Marcar N/A es legítimo, pero **hay que justificarlo por escrito** en el
> informe de review: un N/A sin motivo se trata como checkbox vacío.

> **La regla del N/A vale también para las puertas de verificación.** Ninguna
> de las puertas de C4 bis —campaña de **mutación**, **fase RED** y
> **cobertura** de las líneas cambiadas— puede saltarse marcando N/A a secas.
> Para declararlas N/A hay que justificar por escrito en el informe de review
> *por qué* no aplican en esta feature (por ejemplo: nivel `documental`, o la
> puerta de cobertura se declaró N/A con su motivo impreso por `init.sh`). Un
> N/A sin motivo escrito se trata como checkbox vacío, y un checkbox vacío en
> C1–C5 es CHANGES_REQUESTED. Omitir la herramienta —no instalar `coverage`,
> no lanzar la campaña— no es un motivo: es el hueco que esto viene a tapar.

## Niveles de rigor

No todas las features merecen la misma vigilancia. Cada una declara su nivel
en el campo `rigor` de su entrada de `harness/features.json`; lo que exige
cada nivel vive en `harness/rigor.json` y lo valida `bash harness/init.sh`.

**Si una feature no declara nivel se le aplica el `nivel_por_defecto` de
`harness/rigor.json`**, hoy `estandar`: con puertas de fase RED, cobertura y
mutación, pero sin la exigencia de cero supervivientes. Omitirlo no es gratis
—se sigue exigiendo evidencia— y tampoco arrastra el modo más caro a features
que no lo necesitan. Lo `critico` se declara: no se hereda por descuido.

| Nivel | Para qué features | Exige |
|---|---|---|
| **documental** | Solo documentación, specs o notas de sesión: ni código ni SQL | C1–C3, C3 bis y C5. **Sin** fase RED, **sin** cobertura y **sin** mutación: una feature documental no puede requerir mutation testing |
| **estandar** | Código sin riesgo sobre sistemas compartidos | Todo lo anterior + tests trazables (C4) + **fase RED** en los requisitos centrales + **cobertura** de las líneas cambiadas ≥ umbral + **campaña de mutación** con los supervivientes documentados y analizados |
| **critico** | Infraestructura compartida, producción, seguridad o dinero | Todo lo de `estandar` + **cero supervivientes** sin justificación escrita aceptada por el humano + verificaciones `MANUAL (humano)` listadas con su comando exacto y su resultado real |

Comandos de las puertas:

```bash
bash harness/init.sh                        # cobertura de las líneas cambiadas
                                            # y topes de tamaño del papeleo
python -m harness.mutacion --feature F-XXX  # campaña de mutación
python -m harness.tamano --feature F-XXX    # solo los topes de tamaño
```

Esas herramientas son **solo para proyectos Python**. En un proyecto de otro
lenguaje, `init.sh` declara cada puerta N/A **con su motivo impreso** —no en
silencio— y el nivel de rigor se sigue usando para exigir fase RED y
evidencias: lo que se pierde es la medición automática, no la disciplina.

## C1 — El arnés está completo y en verde

- [ ] `bash harness/init.sh` termina con exit code 0.
- [ ] Existen `CLAUDE.md`, `harness/features.json`, `specs/SPECS.md`,
      `progress/current.md`, `progress/history.md`, `docs/ARCHITECTURE.md`,
      `docs/CONVENTIONS.md`.

## C2 — El estado es coherente

- [ ] Como mucho UNA feature en `in_progress` en `harness/features.json`.
- [ ] La rama actual es `feature/F-XXX-slug` de la feature en curso
      (nunca `main`).
- [ ] `progress/current.md` describe SOLO la sesión activa (sin restos de
      sesiones anteriores) o está en plantilla vacía.
- [ ] Toda feature `done` tiene su resumen en `progress/history.md`.

## C3 — El código respeta arquitectura y convenciones

- [ ] Arquitectura hexagonal respetada: dominio sin imports de
      infraestructura; adaptadores solo en infrastructure.
- [ ] Primera línea de cada fichero de código: comentario con su ruta
      relativa.
- [ ] Sin `print()` de debug, sin TODOs sin contexto, sin secretos
      hardcodeados, sin dependencias nuevas no previstas en la spec.
- [ ] Reglas de dominio propias del proyecto respetadas según
      `docs/ARCHITECTURE.md`. Trampas que el reviewer vigila SIEMPRE:
      1. **Los conceptos `%` no son materiales.** Su importe es un
         porcentaje sobre el acumulado de las líneas anteriores de ese mismo
         `~D`, no precio × rendimiento. Tratarlos como una línea más da
         cifras plausibles y falsas.
      2. **Todo `~D` termina en `\|`** (una sola barra antes del cierre) y
         los códigos del `~D` son los mismos, ya truncados, que los del
         `~C`. Una tripleta que apunte a un código inexistente rompe la
         importación en Presto.
      3. **Los rendimientos a 0 se respetan**: son alternativas
         deliberadamente desactivadas, no errores que haya que limpiar.
      4. **El importe total del presupuesto no cambia** salvo que la feature
         diga explícitamente que cambia: cualquier transformación se
         comprueba comparando el total antes y después contra un BC3 de
         `input/`.

## C3 bis — Los documentos que entran de fuera son seguros

Aplica a toda feature que añada o modifique ficheros en `docs/referencia/`.
Si no toca ninguno, es N/A.

- [ ] Cada documento nuevo lleva cabecera con **origen y fecha** del original,
      según la plantilla de `docs/referencia/README.md`.
- [ ] Los originales en PDF u ofimática **no** están en el repositorio ni en
      el árbol de trabajo (compruébalo también con `git log --diff-filter=A`:
      no basta con que no estén ahora).
- [ ] Se ha ejecutado un **barrido de datos sensibles** sobre los documentos
      nuevos —correos, IPs, GUID de suscripción o tenant, credenciales,
      tokens, cadenas tipo clave— y **su resultado consta en el informe de
      review**, con los patrones usados. El barrido lo ejecuta el reviewer:
      no vale darlo por bueno leyendo el informe del implementer.
- [ ] Lo que se haya redactado está anotado en la cabecera del documento.

## C4 — La verificación es real

- [ ] Cada requisito EARS de la spec (o cada criterio `acceptance`) tiene
      >= 1 test trazable (`test_fXXX_rN_*`) y todos pasan.
- [ ] Los unit tests no tocan red ni BBDD (mocks/fixtures).
- [ ] Las verificaciones `MANUAL (humano)` están listadas en
      `progress/current.md` con su comando exacto, pendientes de que el
      humano las ejecute.
- [ ] **Los dobles de test se comparan con el original, y no a ojo.** Un
      `Fake`/`Stub` que imita una clase real y declara un método que esa clase
      **no tiene** produce VERDE FALSO: la suite pasa entera sobre código que no
      puede funcionar, y el fallo sale en producción. **Leyendo no se caza**: el
      doble es coherente por su lado y el código que lo llama por el suyo, y la
      incoherencia solo aparece al CRUZAR los dos ficheros. Por eso el arnés no
      lo pide como lectura sino como comprobación automática sobre los dobles
      del árbol —encontrados por barrido, nunca por una lista escrita a mano,
      que envejece—, que exige tres cosas: (a) todo método público de un doble
      existe en la clase imitada; (b) ningún doble acepta llamadas que el
      original rechazaría (más posicionales, un `keyword-only` pasado por
      posición, otro nombre en el mismo hueco); y (c) al revés, todo lo que el
      código de producción invoca sobre esa clase existe en ella. El caso que
      lo motiva: `datamart-seg-anual`, nocturna del 2026-09-05, muerta con
      `AttributeError` sobre **3.308 tests en verde** por un método que se
      añadió a tres dobles y nunca al cliente real; cuatro pasadas de reviewer
      tampoco lo vieron.

## C4 bis — El rigor declarado se cumple

Comprobar que los tests son de verdad, no solo que pasan. El reviewer resuelve
primero el nivel de la feature (campo `rigor`, o el `nivel_por_defecto` de
`harness/rigor.json` si no lo declara) y recorre estos puntos **contra ese
nivel**.

Los cuatro puntos marcados **RM** son las reglas de revisión de campañas
acordadas con el humano el 2026-08-19. Bloquean el cierre, pero **ninguna es
puerta automática de `init.sh`**: exigen juicio (¿ha crecido la rama desde que
se midió?, ¿es equivalente este mutante?). Lo que la herramienta garantiza son
los DATOS sobre los que se juzga: el SHA, la línea base y la media por mutante,
que el informe de mutación imprime siempre. RM3 (un equivalente no puede salir
muerto) y RM4 (reejecutar el subconjunto de tests sobre una copia) viven en
`.claude/agents/reviewer.md` como criterio, sin checkbox.

- [ ] La feature declara `rigor` en `harness/features.json` con un valor
      válido, o consta por escrito qué nivel por defecto se le aplicó.
- [ ] **Fase RED** (niveles `estandar` y `critico`): el informe
      `progress/impl_F-XXX.md` contiene, para los requisitos centrales, la
      **salida real** del fallo del test antes de existir el código. No vale
      «se hizo TDD»: vale la traza pegada. Si el entregable de la feature es
      el propio test (no hay código de producción cuyo fallo previo enseñar),
      la fase RED se demuestra rompiendo deliberadamente —en una copia
      aislada, nunca en el árbol real— lo que el test vigila, y pegando la
      traza de ese fallo.
- [ ] **Cobertura** (niveles `estandar` y `critico`): la puerta de
      `bash harness/init.sh` sale en `[OK]` con el porcentaje de las líneas
      cambiadas, o en `N/A` **con el motivo impreso**.
- [ ] **Mutación** (niveles `estandar` y `critico`): existe
      `progress/mutacion_F-XXX.md`, generado por la herramienta, con sus
      totales reales, **verificados de forma independiente por el reviewer**
      (alcance y nº de mutantes recalculados con `harness.alcance` y
      `harness.mutacion`; cálculo puro, sin ejecutar la suite).
- [ ] **Los muertos están comprobados, no solo contados.** Si el «Tiempo
      total» que declara el informe de mutación es **inferior a 60 segundos**,
      el reviewer **reejecuta la campaña** con
      `python -m harness.mutacion --feature F-XXX --salida <ruta fuera de
      progress/>` y compara los totales. La salida no puede escribirse en
      `progress/` (pisaría el informe del implementer) y el árbol debe quedar
      limpio después (`git status`). Si la campaña pasa de 60 segundos, vale el
      recálculo puro más los puntos siguientes, pero el informe de review **lo
      dice explícitamente**. Recalcular alcance y nº de mutantes no demuestra
      que los muertos lo estén: unos «N muertos» inventados pasarían ese
      control.
- [ ] **La campaña tardó lo que tenía que tardar.** Evaluar un mutante es
      **ejecutar entera la suite del servicio**, así que el coste por mutante
      no puede bajar de lo que tarda esa suite. Se calcula:

      > coste por mutante = «Tiempo total» × nº de workers ÷ nº de mutantes

      El factor de workers **no es opcional**: la campaña es paralela por
      defecto y su «Tiempo total» es tiempo de reloj, no de CPU. Sin corregir,
      toda campaña paralela sana parece sospechosa. Por eso el implementer
      declara en **«Evidencias»** con cuántos workers la lanzó; si fue en
      serie (`--workers 1`), el factor es 1.

      Si ese coste sale **por debajo de un segundo**, la campaña es
      **sospechosa por construcción**: no da tiempo a arrancar el intérprete,
      importar el proyecto y recorrer los tests. Lo normal es que la suite ni
      siquiera se estuviera ejecutando de verdad —un árbol con bytecode
      envenenado, una caché que devuelve el veredicto anterior, un fallo de
      importación que mata a todos los mutantes por la misma razón—. **Se
      relanza con la caché limpia** (`__pycache__` y `.pytest_cache` borrados)
      y se comparan los totales; si cambian, el informe válido es el segundo y
      el primero se descarta por escrito.

      Complementa la regla de los 60 segundos: aquella mira el total, esta
      mira el coste por mutante, y una campaña grande y rápida solo la caza la
      segunda. **Cuándo un coste bajo pero mayor que un segundo es sospechoso
      lo decide RM2**, más abajo, que ya trabaja sobre la línea base y la media
      que el propio informe imprime: aquí no se juzga eso a ojo.
- [ ] **El informe de mutación NO lleva la cabecera «⚠ CAMPAÑA NO VÁLIDA» ni
      una fila «Sin veredicto (base rota)» distinta de cero.** Cualquiera de
      las dos significa que la propia herramienta declara que sus números no
      valen: se arregla la línea base y se repite la campaña. Un cero de
      supervivientes medido sobre una suite que ya fallaba es el defecto que
      arregló el arnés 1.6.0, y antes de él se colaba entero.
- [ ] **RM1 · El informe de mutación declara el SHA completo de HEAD** contra
      el que se midió (fila «SHA de HEAD medido»), y el reviewer comprueba que
      el alcance medido es el que está revisando. En F-034 la rama creció de 56
      a 1.057 líneas después de medir y el informe seguía pareciendo válido.
- [ ] **RM2 · El tiempo del informe es internamente coherente:** lo que se
      rechaza **sin reejecutar nada** es un **salto de orden de magnitud** —una
      «Media por mutante evaluado (s)» que no llega ni a la décima parte de la
      «Línea base (s)», o un «Tiempo total» que no cuadra con
      `mutantes × media`—. Que la media quede **por debajo** de la línea base no
      es sospechoso por sí solo: la campaña evalúa con `-x` y el mutante que
      muere aborta la suite en el primer fallo, así que a más muertos, más baja
      la media (caso legítimo de libro: base 52,1 s, media 36,4 s, 19 de 20
      muertos). El caso que esta regla caza es F-034: 18 mutantes en 111 s
      cuando la realidad eran 63 minutos.
      **Con W workers la aritmética cambia, y el informe declara W** (fila
      «Workers», nueva desde la 1.7.2). La «Media por mutante evaluado (s)» es
      tiempo de PARED dividido entre los mutantes, así que **ya viene dividida
      entre W**: `mutantes × media` ES el «Tiempo total» por construcción y
      comparar esos dos no descubre nada. Lo que sí se compara es el coste real
      de juzgar un mutante, `media × W`, contra la «Línea base (s)» —el mismo
      número de la regla del coste por mutante de más arriba—. Sin corregir por
      W, cuatro workers hunden la media a la cuarta parte y la regla marca como
      inventada una campaña legítima (medido: base 64,9 s, media 16,8 s,
      **4 workers** → 67 s reales por mutante, coherente).
      Lee también el «Timeout efectivo por mutante (s)», y **no compares
      tiempos entre campañas con timeouts efectivos distintos**: desde la 1.7.2
      el timeout se deriva de la línea base medida en vez de ser un fijo de
      `rigor.json`, así que las campañas anteriores **no son comparables** con
      las posteriores.
- [ ] **RM5 · Solo en rigor `critico`:** cada superviviente declarado
      «equivalente» trae demostración ejecutable, y el reviewer reproduce **una
      muestra de UNO**, elegido por él. En rigor `estandar` basta la
      justificación escrita, y este punto es N/A por nivel (justificado).
- [ ] **RM6 · Si para matar un mutante se quitó código defensivo**, el
      invariante está verificado en QUIEN CONSTRUYE EL DATO y consta por
      escrito. Borrar una guarda `x is None` para que muera un mutante es
      exactamente la ausencia de defensa que causó F-019 y F-027.
- [ ] **Si la campaña automática dio 0 mutantes y se sustituyó por una
      MANUAL**: el informe trae una tabla con **una fila por mutante** y, en
      cada fila, el fichero y la línea, el **texto exacto original → mutado**
      de la sustitución, y el resultado con su **número de fallos**. Sin ese
      texto exacto el punto NO se marca: describir la mutación con palabras
      («se invierte la guarda») no la hace reproducible, y una campaña manual
      que nadie puede repetir no es evidencia, es un párrafo. El reviewer
      reproduce al menos dos filas al pie de la letra.
- [ ] Cada superviviente de esa campaña tiene su sección de análisis
      **completada** (ninguna en `PENDIENTE`). En nivel `critico`, además,
      cero supervivientes salvo justificación escrita aceptada por el humano.
- [ ] El informe del implementer trae la sección **«Evidencias»** con los
      cuatro números: tests ejecutados y resultado, cobertura de las líneas
      cambiadas, mutantes generados y supervivientes, y tiempo de la suite.
      Con la campaña de mutación, además, **el nº de workers con que se lanzó**:
      sin él no se puede calcular el coste por mutante del punto anterior.
- [ ] Ningún punto de este bloque marcado N/A sin justificación escrita.

## C4 ter — Las verificaciones extra por rutas sensibles están hechas

Hay ficheros cuyo cambio no lo cubre ningún test unitario por bien escrito que
esté: un prompt de IA, un schema que un modelo rellena, el cliente de un
proveedor externo, una migración, un fichero de infraestructura. Un repositorio
puede declararlos en `harness/rutas_sensibles.json` junto con la verificación
extra que exigen y el informe que la demuestra (esquema y ejemplo en
`harness/rutas_sensibles.ejemplo.json`).

**Sin esa declaración este bloque es N/A y no hay nada que justificar**: es la
configuración del caso mayoritario. Con declaración presente, y **solo si la
puerta de `bash harness/init.sh` señaló rutas tocadas** por el diff de la
feature:

- [ ] Existe el informe declarado para esta feature (el campo `informe` de la
      verificación, con `{feature}` resuelto).
- [ ] El informe cumple TODAS las líneas que la declaración exige en
      `exige_lineas`. El reviewer las comprueba leyendo el fichero, no
      fiándose del resumen del implementer.
- [ ] El informe es **FRESCO**: su commit pertenece a la rama de la feature y
      es POSTERIOR al último commit que tocó una ruta sensible. Un informe
      verde de antes del cambio no demuestra nada. Esto no lo automatiza la
      puerta —el informe vive en `progress/` y su commit mueve HEAD—: es
      responsabilidad del reviewer, igual que la verificación independiente de
      la mutación en C4 bis.
- [ ] Si la exigencia declarada es `aviso` y la evidencia falta, el motivo
      consta por escrito en el informe de review. `aviso` no significa
      «ignorable»: significa «todavía no bloquea el arnés».

## C5 — La sesión se cerró bien

- [ ] `tasks.md` de la spec con todas las tareas `[x]` y un commit
      `F-XXX Tn: ...` por tarea.
- [ ] Sin ficheros temporales ni artefactos sin trackear sospechosos.
- [ ] `features.json` refleja el estado real de la feature.
