<!-- progress/current.md -->
# Trabajo en curso

(ninguna feature en ejecución)

## Nota de instalación · 2026-09-17

Arnés v1.7.10 instalado en este repositorio (modo `instalar`, no pisó nada).
Marcas `[ADAPTAR]` resueltas: `CLAUDE.md`, `docs/ARCHITECTURE.md`,
`docs/CONVENTIONS.md`, `docs/referencia/README.md`, `CHECKPOINTS.md`,
`harness/init.sh` (cabecera y sección 9) y `harness/features.json`.
`bash harness/init.sh` termina en **ENTORNO LISTO**.

Pendiente de decisión del humano:

1. **`docs/ARCHITECTURE.md`, sección «Semántica de dominio imprescindible»:
   sin validar todavía.** Está deducida del código y verificada
   numéricamente contra los BC3 de `input/`, pero la regla del arnés es que
   esa sección la valide una persona antes del primer uso real.
2. **`config/settings.py` está machacado en el árbol de trabajo**: contiene
   una copia de `application/services/budget_bc3_batch_service.py` en vez de
   la clase `Settings`. El cambio no está commiteado; la versión buena sigue
   en `HEAD`. Mientras no se restaure, el ETL no arranca.
3. **Rama.** La instalación se hizo sobre `compilar`, sin commit. `dev` y
   `master` están hoy en el mismo commit; `RAMA_BASE=dev` en `init.sh`.
