<!-- docs/referencia/README.md -->
# Información adicional de referencia

Documentación de apoyo que **no** describe el código de este repositorio,
sino el negocio y los sistemas origen: manuales de la aplicación de la que
se leen los datos, criterios de negocio, definiciones funcionales,
especificaciones que llegan de fuera.

Sirve para responder «¿por qué el código hace esto?» cuando la respuesta no
está en el código sino en una norma de negocio.

## Qué va aquí y qué no

| Aquí | No aquí |
|---|---|
| Manuales y documentación de los sistemas origen | Arquitectura del proyecto → `docs/ARCHITECTURE.md` |
| Criterios de negocio y definiciones funcionales | Convenciones de código → `docs/CONVENTIONS.md` |
| Documentos que llegan en PDF, Word o Excel, convertidos a Markdown | Especificaciones de features → `specs/` |
| Capturas o extractos de informes de referencia | Notas de trabajo de una sesión → `progress/` |

### Documentación compartida por varios proyectos: una sola copia

Si un documento describe algo **común a varios repositorios** —el diccionario
de una base de datos que todos consumen, la API de un servicio compartido, el
diseño de red de la organización— **no va aquí**: va al repositorio de
documentación del ecosistema (en el entorno de Ruesma, `azure-apps/`) como
única copia, y en este directorio queda solo un **puntero** de pocas líneas
con la ruta, el motivo y lo imprescindible para no tener que abrir el otro
repositorio. Dos copias del mismo documento divergen siempre; el puntero
evita además arrastrar ficheros de megas a cada proyecto que instale el arnés.
Convención del puntero: mismo nombre `NN_tema.md`, título terminado en
«— vive en `<repositorio>`, no aquí» y una nota fechada de cuándo se movió.

## Índice

Una fila por documento incorporado. Mantenerlo al día es parte de añadir un
documento, no una tarea posterior.

Hoy (2026-09-17) no hay ninguno. El primer candidato natural es la
especificación **FIEBDC-3** y la documentación de Presto sobre conceptos
porcentuales: hasta que entren, la referencia del formato es la sección
«Semántica de dominio imprescindible» de `docs/ARCHITECTURE.md`.

| Fichero | Qué es |
|---|---|
| — | — |

## Formato

Todo en **Markdown**. Los documentos que lleguen en PDF u ofimática se
convierten al entrar con la herramienta MCP `markitdown` (ver la regla en
`CLAUDE.md`), para que sean legibles, buscables con grep y diffeables entre
versiones, y para que el resultado sea el mismo lo convierta quien lo
convierta.

Convención de nombres: `NN_tema.md` cuando haya un orden natural de lectura,
o `tema.md` si no. El original queda **fuera del repositorio**: aquí solo
entra el Markdown. Anota en la cabecera de cada fichero de dónde salió y de
qué fecha es, porque un manual desactualizado que parece vigente hace más
daño que no tenerlo.

Cabecera obligatoria. La primera línea del bloque de origen es siempre la
misma; la segunda depende de cómo llegó el documento.

**Caso 1 — llegó en PDF u ofimática y se convirtió:**

```markdown
<!-- docs/referencia/NN_tema.md -->
# Título

> Origen: <fichero o sistema de procedencia> · Fecha del documento: AAAA-MM-DD
> Convertido a Markdown el AAAA-MM-DD con la herramienta MCP `markitdown`.
> El original vive fuera del repositorio.
```

**Caso 2 — llegó ya en Markdown:**

```markdown
> Origen: <fichero o sistema de procedencia> · Fecha del documento: AAAA-MM-DD
> Incorporado a `docs/referencia/` el AAAA-MM-DD.
> Llegó ya en Markdown: no requirió conversión con `markitdown`.
```

No escribas «convertido» si no hubo conversión: la trazabilidad de cómo entró
un documento es justo lo que un reviewer no puede reconstruir después.

**Tercer bloque, obligatorio si se ha redactado algo.** Cuando el documento
traiga material sensible que se sustituya por marcadores, dilo en la cabecera
y di exactamente qué se sustituyó:

```markdown
> **Redactado.** Se han sustituido por marcadores <qué: rangos de red, IDs de
> suscripción, correos…>. El detalle está en el original, fuera del repositorio.
```

Si el original impone restricciones de uso (confidencialidad de un proveedor,
por ejemplo), cítalas también en la cabecera.

## Antes de commitear un documento nuevo

El checkpoint **C3 bis** de `CHECKPOINTS.md` es la lista de verificación
formal, y el reviewer la recorre. En resumen: cabecera con origen y fecha,
original fuera del repositorio (compruébalo también en el historial, no solo
en el árbol), barrido de datos sensibles ejecutado y anotado, y lo redactado
declarado en la cabecera.
