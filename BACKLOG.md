<!-- BACKLOG.md -->
# Backlog

**Fichero generado por `harness/backlog.py` a partir de `harness/features.json`. No lo edites a mano**: edita el JSON y vuelve a generarlo (lo hace solo `bash harness/init.sh`).

Resumen: **2 features**, 2 abiertas, 0 terminadas.

En curso: **F-002**.

## Trabajo abierto

| # | Feature | Prioridad | Estado | Rigor | Rama |
|---|---|---|---|---|---|
| F-001 | Inventario de conceptos porcentuales de un BC3 | 1 | pendiente | estandar | `feature/F-001-inventario-porcentuales` |
| F-002 | Convertir los descompuestos porcentuales a UD con cantidad 1 conservando el importe de Presto | 2 | en curso | critico | `feature/F-002-porcentuales-a-ud` |

## Terminadas

_Todavía no hay features terminadas._

## Detalle

### F-001 · Inventario de conceptos porcentuales de un BC3

estado **pendiente** · prioridad 1 · rigor `estandar` · SDD no · rama `feature/F-001-inventario-porcentuales`

Feature de calentamiento: valida el circuito completo del arnés (rama, acceptance, implementer, reviewer, cierre) con una tarea pequeña y de solo lectura. Añade una función que, dado un .bc3, devuelva la lista de líneas de descompuesto cuyo hijo es un concepto porcentual, con el padre, el código del hijo, el rendimiento y la base acumulada de las líneas que le preceden. No modifica ningún fichero: solo informa. Sirve además de diagnóstico previo a F-002, que es quien transformará esas líneas.

### F-002 · Convertir los descompuestos porcentuales a UD con cantidad 1 conservando el importe de Presto

estado **en curso** · prioridad 2 · rigor `critico` · SDD sí · rama `feature/F-002-porcentuales-a-ud`

En los presupuestos reales (Siroco, El Escorial) hay descompuestos que no son un material sino un porcentaje: su código empieza por '%', su unidad viene vacía o como '%', el precio de su ~C es el porcentaje en tanto por ciento (-3 = -3 %) y en el ~D del padre el rendimiento es ese mismo porcentaje en tanto por uno (-0.03). Su importe NO es precio x rendimiento: es rendimiento x (suma de los importes de las líneas que le preceden en ese mismo ~D). El ERP de destino no entiende esa semántica, así que hay que dejarlos como una línea normal: unidad UD, cantidad 1 y precio igual al importe que Presto habría calculado. Como el mismo concepto porcentual se reutiliza en decenas de padres con bases distintas (%RF, %VID, %VALCOM...), no se le puede poner un precio único: hay que clonarlo por padre, igual que ya se hace con los clones '.1' de las partidas sin hijos. La transformación es correcta si y solo si el importe de cada partida y el total del presupuesto no se mueven. DUDAS ABIERTAS que la spec debe cerrar con el humano antes de implementar: (1) si la base del porcentaje son solo las líneas anteriores del mismo ~D -verificado contra input/, pero en los casos comprobados las líneas posteriores valían 0, así que no es concluyente- o el descompuesto entero; (2) qué código reciben los clones y cómo se garantiza que siguen cabiendo en 20 caracteres sin colisionar; (3) si el concepto porcentual original se conserva en el fichero como huérfano o se elimina; (4) a cuántos decimales se redondea el precio del clon y si el redondeo se acumula o se corrige en la última línea.
