<!-- progress/current.md -->
# Trabajo en curso

**F-002 · Convertir los descompuestos porcentuales a UD con cantidad 1
conservando el importe de Presto** · rama `feature/F-002-porcentuales-a-ud`
· rigor `critico` · SDD sí.

Estado: spec **aprobada** por el humano e **implementación en curso**
(implementer). El informe con las evidencias va en `progress/impl_F-002.md`.

## Decisiones que el humano ya cerró antes de implementar

- **R9 aprobado tal cual**: en un `~D` cuyas líneas son TODAS porcentuales y
  cuyo padre tiene precio ≠ 0, el clon de la primera línea recibe el precio
  del padre y las siguientes se aplican sobre él. Son cuatro partidas reales:
  `ICV260` (291,50) e `ICV270` (369,50) en `input/lagunamodificado16julio.bc3`,
  y `31.04.03.01` (1.100,00) y `32.03.04.32` (1.117,65) en Siroco.
- **No hay dos codificaciones de porcentual**: el `~D` lleva el rendimiento en
  tanto por uno y el `~C` el porcentaje. Lo de «cantidad = base/100» es solo
  cómo lo pinta Presto en pantalla (ver `progress/explore_porcentuales.md`
  §4 bis). La fórmula D3/D4 de `requirements.md` es la buena.
- El alcance es el de la spec: pasada previa independiente que **solo** toca
  líneas porcentuales.

## Estado de las 16 tareas

T1-T14 hechas y commiteadas (una por tarea). T16 (`bash harness/init.sh`) en
verde. Queda **T15, que es MANUAL del humano**.

## T15 · Verificación MANUAL pendiente (humano)

```
python -m interface_adapters.cli.porcentuales_cli "input/COSTE_250128_Siroco_Rv4mlo.bc3" "output/siroco_sin_pct.bc3"
```

Luego importar `output/siroco_sin_pct.bc3` en Presto y confirmar que el total
del presupuesto y el precio de `43.15`, `05.06.29`, `31.04.03.01` y
`32.03.04.32` son los que Presto mostraba antes. El resultado se anota aquí.

**Resultado: PENDIENTE.** Nadie lo ha ejecutado todavía; el agente no puede
darlo por hecho.

## Nota de instalación · 2026-09-17

Arnés v1.7.10 instalado y adaptado; `bash harness/init.sh` en **ENTORNO
LISTO**. `config/settings.py` restaurado (tenía pegada encima una copia de
`budget_bc3_batch_service.py`). `dev` y `master` avanzados hasta el commit
del arnés; sin push.

**Pendiente de validación humana:** la sección «Semántica de dominio
imprescindible» de `docs/ARCHITECTURE.md`.
