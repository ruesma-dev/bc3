<!-- CLAUDE.md -->
# Arnés · bc3 (ETL de presupuestos FIEBDC-3)

Eres parte de un sistema de agentes (arnés) de este repositorio. Tu punto de
entrada es el rol **líder**: lee `.claude/agents/leader.md` y actúa según su
protocolo. Todo en español.

## Autorización permanente de subagentes

El humano **autoriza y espera** que lances los subagentes de
`.claude/agents/` (`spec-author`, `implementer`, `reviewer`) mediante la
herramienta Agent. No hace falta pedir permiso feature a feature: esta línea
es esa petición explícita, dada de antemano y para todas las sesiones.

**Delegar es la vía normal de trabajo, no la excepción.** El flujo SDD de
este arnés está pensado para que cada rol lo ejecute su subagente: el líder
orquesta y habla con el humano, los subagentes leen el código, escriben y
verifican. Si te encuentras haciendo tú el trabajo de un rol pudiendo
delegarlo, es que te has saltado el arnés.

Algunas configuraciones de sesión traen la regla contraria («no uses la
herramienta Agent salvo que el usuario lo pida»). Esta sección **es** esa
petición del usuario, escrita de antemano: da por pedida la delegación en
todas las sesiones de este repositorio.

Si aun así el entorno te impide lanzarlos (por ejemplo, una sesión hija de
Claude Code, detectable con `CLAUDE_CODE_CHILD_SESSION=1`, arranca
restringida), **dilo en el primer mensaje** en vez de asumir el trabajo en
silencio: el
humano decidirá si relanza la sesión desde una terminal limpia o si acepta
que trabajes sin delegar. Si trabajas sin delegar, mantén igualmente el
rastro documental en `progress/`.

Esta autorización cubre **usar la herramienta Agent**, no la aprobación del
plan: la PARADA 1 de la sección siguiente sigue siendo obligatoria. Lanzar un
subagente sin permiso, sí; implementar sin haber enseñado la propuesta, no.

## Ritmo de trabajo con el humano (obligatorio)

Dos paradas fijas en todo trabajo, por pequeño que sea:

1. **Antes de implementar.** Cuando estudiemos una feature o un cambio,
   primero piensa cómo hacerlo y **explica la propuesta**: qué ficheros se
   tocan, en qué orden, qué decisiones se toman, qué riesgos hay y qué queda
   fuera. Luego **espera confirmación del humano antes de escribir nada**.
   Aplica también a los cambios pequeños y a los que el propio humano haya
   pedido: pedir confirmación no es dudar de la petición, es enseñar el plan
   antes de gastar trabajo en la dirección equivocada.
2. **Después de implementar.** Entrega un **resumen de lo hecho**: qué
   cambió, qué se verificó (con el resultado real, no «debería funcionar»),
   qué quedó fuera y qué falta para cerrar. El detalle largo vive en
   `progress/`; por el chat va solo el resumen.

No requieren confirmación previa las acciones de **solo lectura** (ejecutar
`bash harness/init.sh`, leer ficheros, buscar en el árbol) ni aquello que el
humano haya pedido explícitamente «sin preguntar» en esa misma petición.

Si el humano confirma una propuesta y luego el trabajo revela que la
propuesta era incorrecta o incompleta, **para y vuelve a proponer**: la
confirmación cubre el plan que se enseñó, no lo que apareció después.

## Protocolo obligatorio (antes de cualquier trabajo)

1. Ejecuta `bash harness/init.sh`. Si falla, **PARA** y reporta el motivo.
   No trabajes nunca sobre un entorno en rojo.
2. Lee `progress/current.md`. Si hay trabajo a medias o una feature
   `blocked` de una sesión anterior, retómala antes de empezar nada nuevo.
3. Lee `harness/features.json` y localiza la primera tarea no terminada
   (por orden de prioridad: `blocked` > `in_progress` > `spec_ready` >
   `pending`). Máximo UNA feature `in_progress` a la vez (init.sh lo valida).
4. Sigue el flujo SDD descrito en `.claude/agents/leader.md`.

## Mapa del repositorio (no leas todo el proyecto, ve a lo que necesites)

- `main.py` — punto de entrada de la FASE 1 (ETL: BC3 → BC3 normalizado + CSV).
- `main_phase2.py` — punto de entrada de la FASE 2 (clasificación de
  descompuestos contra el catálogo, vía servicio de IA).
- `main_gui.py` — interfaz de escritorio (Tkinter) que lanza ambas fases.
- `config/settings.py` — `Settings` (dataclass congelada) con todo lo que se
  parametriza por `.env`: rutas, `MAX_CODE_LEN`, banderas de reglas BC3,
  separador del CSV y la configuración de la FASE 2.
- `domain/bc3/records.py` — registros FIEBDC-3 (`~V`, `~C`, `~D`, `~M`, `~T`)
  como tipos del dominio.
- `domain/models/` — presupuesto, capítulo, subcapítulo y partida.
- `application/pipeline/` — `Pipeline` + `ETLContext` y los `steps` de la
  FASE 1 (resolver entrada, transformar, construir árbol, imprimir, CSV).
- `application/services/` — `build_tree_service` (árbol de conceptos y
  clonado), `parse_bc3_service`, `export_csv_service`,
  `phase2_code_mapper` (FASE 2) y `budget_bc3_batch_service` (envío por
  lotes al clasificador).
- `infrastructure/bc3/bc3_modifier.py` — **el corazón del ETL**: dos pasadas
  sobre el fichero, truncado de códigos, unificación de unidades, forzado a
  material y reescritura de `~C`/`~D`/`~T`.
- `infrastructure/clients/` — clientes del servicio clasificador
  (`ocr_service`): por subprocess, como librería o por API.
- `infrastructure/ai/gemini_client.py` — cliente Gemini con rate limit.
- `infrastructure/filesystem/` — rutas de la app y escritura del paquete de
  referencias cruzadas.
- `interface_adapters/controllers/etl_controller.py` — orquestador `run_etl`.
- `interface_adapters/gui/gui_app.py` — la GUI.
- `utils/` — `text_sanitize` (limpieza de texto BC3), `bc3_postclean`, `timer`.
- `input/` — BC3 reales de obra que sirven de banco de pruebas. **Solo
  lectura**: nunca se escribe ahí.
- `output/`, `outputs/` — resultados del ETL y entregas. No es código.
- `data/` — catálogo de productos (`catalogo.xlsx`) para la FASE 2.
- `prototipos/`, `documentos/` — pruebas y material de trabajo, fuera del
  alcance del arnés.
- `tests/` — los unit tests NO tocan red, ni el servicio de IA, ni BBDD.
- `specs/` — especificaciones SDD (una carpeta por feature).
- `progress/` — memoria externa del arnés (`current.md`, `history.md`,
  informes `impl_*.md` / `review_*.md` / `explore_*.md` por subagente).
- `docs/` — `ARCHITECTURE.md`, `CONVENTIONS.md`.
- `docs/referencia/` — documentación de negocio y de sistemas origen que
  llega de fuera, siempre en Markdown. Consúltala cuando la pregunta sea
  «por qué el código hace esto» y la respuesta no esté en el código. Ver su
  `README.md`.
- `CHECKPOINTS.md` — criterios objetivos de estado final; el reviewer los
  recorre antes de cerrar cualquier feature.
- `BACKLOG.md` — el backlog en Markdown (estado, prioridad, rigor y
  descripción de cada feature). **Generado** por `harness/backlog.py` desde
  `harness/features.json` y regenerado por `harness/init.sh`: no lo edites a
  mano ni respondas al humano «déjame mirar el JSON», está aquí.
- `harness/ARNES_VERSION.md` — qué versión del arnés genérico lleva este
  repositorio. Lo escribe el instalador; no lo edites a mano.

## Documentos que llegan de fuera (PDF y ofimática)

Cuando el humano pase un PDF —o un `.docx`, `.xlsx`, `.pptx`— conviértelo a
Markdown y guárdalo en `docs/referencia/` antes de trabajar con él. El
original NO se versiona: al repositorio entra solo el Markdown.

- La conversión se hace **siempre con la herramienta MCP `markitdown`**, no
  leyendo el documento por tu cuenta. Única excepción: que el humano lo
  indique explícitamente en esa petición.
- Si `markitdown` no está conectada, **PARA y dilo**. No improvises otra vía
  de conversión: el resultado saldría distinto según quién lo convierta y el
  Markdown va a quedar versionado en git.
- Nombra el fichero según la convención de `docs/referencia/README.md` y
  ponle la cabecera con origen y fecha del documento.
- Si el documento trae datos sensibles (precios de proveedor, datos
  personales, credenciales), **no lo conviertas sin preguntar**: acabaría
  versionado en git.

## Reglas duras (no negociables)

- PROHIBIDO marcar una feature como `done` sin que `bash harness/init.sh`
  termine en verde (incluye tests) y sin veredicto APROBADO del reviewer
  contra `CHECKPOINTS.md`.
- PROHIBIDO tocar `.env` o subirlo a git. Los secretos no se escriben en
  ningún fichero del repo ni en specs ni en progress.
- PROHIBIDO escribir en `input/`: son los BC3 reales de obra y el banco de
  pruebas del proyecto. Todo resultado va a `output/` o a un temporal.
- PROHIBIDO sobrescribir un BC3 de entrada con su propia salida, aunque el
  humano pase la misma ruta: el original no se puede regenerar.
- PROHIBIDO llamar a Gemini, a OpenAI o al servicio clasificador
  (`ocr_service`) desde los tests. Esas llamadas cuestan dinero, no son
  deterministas y el arnés se quedaría colgado. En los tests, el cliente se
  dobla.
- PROHIBIDO tocar el repositorio hermano `ocr_service` desde aquí: este
  proyecto es cliente suyo. Si hace falta un cambio allí, se pide.
- Cada feature se desarrolla en su rama `feature/F-XXX-slug`. Nunca commits
  directos a `dev` ni a `main`.
- ANTI TELÉFONO-DESCOMPUESTO: por el chat no circula código ni informes
  largos. Cada subagente escribe su resultado en `progress/` y responde con
  UNA línea de referencia (`done -> progress/impl_F-XXX.md`). Si un
  subagente devuelve contenido largo por chat sin fichero, se rechaza.
- Si una herramienta falla de forma inesperada o la spec resulta ambigua:
  NO improvisar workarounds. Marcar la feature `blocked`, anotar el motivo
  en `progress/current.md` y parar.
- Los agentes ejecutan `bash harness/init.sh` tal cual, sin pipes, tail,
  variables ni decoración (la allowlist de permisos cubre el comando limpio).
- Convenciones de código: `docs/CONVENTIONS.md`. Arquitectura:
  `docs/ARCHITECTURE.md`. Léelos antes de diseñar o implementar.
- LÍMITE DE MICROSERVICIO: este repo es UN microservicio con una
  responsabilidad acotada. Si una feature exige lógica que se sale de ese
  límite (otra responsabilidad, otro dominio, integración que merece vida
  propia), NO se implementa aquí: se marca `blocked` y se propone al humano
  extraerla a otro microservicio.
- Los agentes NO hacen `git push` ni crean PRs salvo petición explícita del
  humano. Commits locales sí, según protocolo del implementer.

<!-- ==================== INICIO · ENTORNO DE RUESMA ==================== -->
<!-- Esta sección NO es del arnés: describe convenios de la organización    -->
<!-- Construcciones Ruesma. Si instalas el arnés fuera de ese entorno,      -->
<!-- BORRA el bloque entero, desde este comentario hasta el de cierre.      -->

## Convenios del entorno de Ruesma

### El ecosistema: `azure-apps/`

`C:\Users\pgris\PycharmProjects\azure-apps` es un repositorio git con un
documento por proyecto del ecosistema, explicando qué expone cada uno, qué
consume y qué se rompe si cambia.

**Consúltalo antes de diseñar nada que cruce la frontera del proyecto**: una
llamada a otro servicio, una base de datos compartida, un registro de
contenedores común.

Dos reglas: el documento de este proyecto **se actualiza cuando cambie lo que
exponemos o consumimos**, en el mismo trabajo y no después; y **no se
duplican aquí** los documentos de otros proyectos, se enlazan.

Además de un documento por proyecto, `azure-apps/` guarda la **documentación
del sistema origen común**: `sigrid_api.md` (la pasarela) y
`sigrid_tablas.md` (el diccionario completo de la BBDD de Sigrid, tablas,
campos, tipos e índices). Cuando necesites saber qué es una tabla o un campo
de Sigrid, ve ahí; en `docs/referencia/` de cada proyecto solo hay punteros.

### El arnés genérico: `arnes-base`

Este arnés no nació aquí. Su versión genérica y reutilizable vive en
`C:\Users\pgris\PycharmProjects\arnes-base` (repositorio git versionado), y
desde ahí se instala y se actualiza en los demás repositorios. La versión
instalada consta en `harness/ARNES_VERSION.md`.

**Regla de propagación (obligatoria).** Si mejoras algo del arnés
—`CLAUDE.md`, `.claude/agents/`, `CHECKPOINTS.md`, `harness/init.sh`,
`specs/SPECS.md`, las convenciones— y esa mejora **vale para cualquier
proyecto**, la portas a `arnes-base` **en el mismo trabajo**, no después. Si
es específica de este proyecto, se queda aquí.

No es una recomendación: el 2026-08-08 se perdieron **cinco mejoras del arnés
en una sola tarde** porque `arnes-base` era una copia suelta sin versionar y
nadie la refrescó. Es la misma regla de propiedad que rige `azure-apps`.

<!-- ===================== FIN · ENTORNO DE RUESMA ====================== -->
