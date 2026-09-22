<!-- progress/current.md -->
# Trabajo en curso

(ninguna feature en ejecución)

F-002 cerrada el 2026-09-22 → ver `progress/history.md`.

Siguiente por prioridad: **F-003 · Aplicación web de limpieza de BC3**
(backend + front en local), luego F-004 (despliegue y alta de la tarjeta del
portal) y F-005 (comparación de dos BC3).

Pendientes que no bloquean y siguen sin decidir:

- **R13 sobre `input/presupuesto.bc3`**: la pasada borra 42 `~C` porcentuales
  sin convertir ninguna línea, por ser un banco de precios cuyos `%` no usa
  ningún `~D`. Revertirlo es una línea.
- **Test de caracterización de `convert_to_material`** (deuda que deja F-002).
- **Porte de las tres lecciones a `arnes-base`.**
- **T15**: Siroco no se llegó a importar con el código final.
