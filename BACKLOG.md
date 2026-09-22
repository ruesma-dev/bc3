<!-- BACKLOG.md -->
# Backlog

**Fichero generado por `harness/backlog.py` a partir de `harness/features.json`. No lo edites a mano**: edita el JSON y vuelve a generarlo (lo hace solo `bash harness/init.sh`).

Resumen: **5 features**, 5 abiertas, 0 terminadas.

En curso: **F-002**.

## Trabajo abierto

| # | Feature | Prioridad | Estado | Rigor | Rama |
|---|---|---|---|---|---|
| F-001 | Inventario de conceptos porcentuales de un BC3 | 1 | pendiente | estandar | `feature/F-001-inventario-porcentuales` |
| F-002 | Convertir los descompuestos porcentuales a UD con cantidad 1 conservando el importe de Presto | 2 | en curso | critico | `feature/F-002-porcentuales-a-ud` |
| F-003 | Aplicación web de limpieza de BC3 (backend + front, en local) | 3 | pendiente | estandar | `feature/F-003-web-limpieza` |
| F-004 | Despliegue en Azure y activación de la tarjeta del portal | 4 | pendiente | critico | `feature/F-004-despliegue-portal` |
| F-005 | Comparación de dos BC3 en la web | 5 | pendiente | estandar | `feature/F-005-web-comparar` |

## Terminadas

_Todavía no hay features terminadas._

## Detalle

### F-001 · Inventario de conceptos porcentuales de un BC3

estado **pendiente** · prioridad 1 · rigor `estandar` · SDD no · rama `feature/F-001-inventario-porcentuales`

Feature de calentamiento: valida el circuito completo del arnés (rama, acceptance, implementer, reviewer, cierre) con una tarea pequeña y de solo lectura. Añade una función que, dado un .bc3, devuelva la lista de líneas de descompuesto cuyo hijo es un concepto porcentual, con el padre, el código del hijo, el rendimiento y la base acumulada de las líneas que le preceden. No modifica ningún fichero: solo informa. Sirve además de diagnóstico previo a F-002, que es quien transformará esas líneas.

### F-002 · Convertir los descompuestos porcentuales a UD con cantidad 1 conservando el importe de Presto

estado **en curso** · prioridad 2 · rigor `critico` · SDD sí · rama `feature/F-002-porcentuales-a-ud`

En los presupuestos reales (Siroco, El Escorial) hay descompuestos que no son un material sino un porcentaje: su código empieza por '%', su unidad viene vacía o como '%', el precio de su ~C es el porcentaje en tanto por ciento (-3 = -3 %) y en el ~D del padre el rendimiento es ese mismo porcentaje en tanto por uno (-0.03). Su importe NO es precio x rendimiento: es rendimiento x (suma de los importes de las líneas que le preceden en ese mismo ~D). El ERP de destino no entiende esa semántica, así que hay que dejarlos como una línea normal: unidad UD, cantidad 1 y precio igual al importe que Presto habría calculado. Como el mismo concepto porcentual se reutiliza en decenas de padres con bases distintas (%RF, %VID, %VALCOM...), no se le puede poner un precio único: hay que clonarlo por padre, igual que ya se hace con los clones '.1' de las partidas sin hijos. La transformación es correcta si y solo si el importe de cada partida y el total del presupuesto no se mueven. DUDAS ABIERTAS que la spec debe cerrar con el humano antes de implementar: (1) si la base del porcentaje son solo las líneas anteriores del mismo ~D -verificado contra input/, pero en los casos comprobados las líneas posteriores valían 0, así que no es concluyente- o el descompuesto entero; (2) qué código reciben los clones y cómo se garantiza que siguen cabiendo en 20 caracteres sin colisionar; (3) si el concepto porcentual original se conserva en el fichero como huérfano o se elimina; (4) a cuántos decimales se redondea el precio del clon y si el redondeo se acumula o se corrige en la última línea.

### F-003 · Aplicación web de limpieza de BC3 (backend + front, en local)

estado **pendiente** · prioridad 3 · rigor `estandar` · SDD sí · rama `feature/F-003-web-limpieza`

Un adaptador de entrada web para la limpieza que ya hace F-002, junto a la GUI y el CLI que este proyecto ya tiene: FastAPI + Jinja2 + JS vanilla servido por el propio backend, que es el patrón del ecosistema (ver azure-apps/dedicacion.md, el ejemplar más limpio). El usuario sube un .bc3, elige qué limpiar y se descarga el resultado. Las opciones visibles son las banderas que la pasada ya expone: convertir porcentuales a UD, limpiar el texto y número de decimales. NO se guarda nada: el fichero se procesa y se devuelve; ni base de datos, ni blob, ni SharePoint. Esta feature deja la aplicación funcionando EN LOCAL; el despliegue y el alta en el portal son F-004. Los proyectos hermanos se miran como referencia, no se copian: de `comprare_bc3` interesa cómo estructura la comparación y de `postventa-incidencias` (azure-apps/postventa_incidencias.md §8) el circuito de subir y procesar ficheros del usuario, pero el humano ha pedido explícitamente mejorarlo, no replicarlo. ESTILO (pedido explícito del humano): fiel al front de `postventa-incidencias` (services/postventa-front). Es decir Tailwind por CDN y Alpine.js con la versión FIJADA (allí es 3.14.1, con un comentario explicando por qué no se deja en 3.x: un cambio del CDN rompería el front), hoja propia mínima con `--ruesma-burdeos: #ad1833`, fondo `bg-slate-50` y texto `text-slate-800`, cabecera con título, subtítulo y un indicador del estado del backend (punto verde / rojo / gris pulsante) más la versión. Esto matiza el 'Jinja2 + JS vanilla' del patrón general del ecosistema: manda el parecido con postventa.

### F-004 · Despliegue en Azure y activación de la tarjeta del portal

estado **pendiente** · prioridad 4 · rigor `critico` · SDD sí · rama `feature/F-004-despliegue-portal`

Llevar la web de F-003 a una Container App y encender la tarjeta 'Gestión BC3' que YA existe en el portal (front-portal/public/assets/js/catalog.js, hoy con url vacía, comingSoon true y el GUID del grupo en placeholder REEMPLAZAR_OBJECT_ID_bc3_usuarios). Patrón del ecosistema: Container App con ingress externo y Easy Auth de Entra, autorización por grupo `bc3-usuarios` con asignación requerida en la Enterprise App; registro `acralbaranesdev` con pull por identidad gestionada; tags de imagen fechados rAAAAMMDD-HHmm que NO se reescriben nunca, imagen base por digest y requirements-lock (azure-apps/mcp_bbdd.md §5 fija estas prácticas). Sin base de datos: esta app no es inquilina del PostgreSQL compartido, así que no toca su contrato de buena vecindad. Ojo al reparto de dueños: el documento del portal lo mantiene front-portal, pero `azure-apps/bc3.md` es de ESTE proyecto y hay que crearlo como copia de docs/INTEGRACION.md con su cabecera de origen y fecha.

### F-005 · Comparación de dos BC3 en la web

estado **pendiente** · prioridad 5 · rigor `estandar` · SDD sí · rama `feature/F-005-web-comparar`

Segunda función de la tarjeta: subir dos .bc3 y obtener un informe de diferencias. La lógica de referencia está en el proyecto `comprare_bc3` (application/services/diff_service.py, 332 líneas: precios, cantidades, importes, altas y bajas, y descripciones largas con tolerancias configurables; e infrastructure/exporters/excel_workbook_combined_min.py, que genera un Excel de seis hojas con los cambios de texto resaltados). El humano ha pedido traer lo que interese PERO mejorándolo, no copiarlo tal cual: hay que revisar las tolerancias, el trato de los conceptos que no existen en uno de los dos ficheros y la legibilidad del informe. Ambos proyectos ya leen BC3 con la misma librería (`bc3-lib`, del repositorio bc3_reader), así que hablan el mismo formato.
