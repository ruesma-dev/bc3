<!-- harness/ARNES_VERSION.md -->
# Version del arnes instalada en este repositorio

Lo escribe `instalar_arnes.ps1`. **No lo edites a mano.**

| Dato | Valor |
|---|---|
| Version del arnes | `1.7.10` |
| Fecha de la version | 2026-09-06 |
| Instalado/actualizado el | 2026-09-17 13:48 |
| Modo | instalar |
| Origen | `arnes-base` |

Para actualizar a una version posterior, desde el repositorio `arnes-base`:

```powershell
.\instalar_arnes.ps1 -Destino "C:\Users\pgris\PycharmProjects\bc3" -Modo actualizar
```

Que hace el instalador por su cuenta al actualizar, sin que tengas que
vigilarlo fichero a fichero:

- **No toca el estado de este proyecto**: `harness/features.json`,
  `docs/ARCHITECTURE.md`, `progress/`, `specs/`, `docs/referencia/`,
  `BACKLOG.md` y la configuracion local. Ni con `-Forzar`.
- **Aplica sin preguntar** los ficheros genericos del arnes (los agentes,
  `harness/*.py`, `specs/SPECS.md`...), guardando antes una copia de la
  version que tenias.
- **Pregunta solo** por los que mezclan lo generico con lo tuyo (`CLAUDE.md`,
  `CHECKPOINTS.md`, `harness/init.sh`, `docs/CONVENTIONS.md`,
  `.claude/settings.json`), y la opcion por defecto es CONSERVAR el tuyo.

La clasificacion completa vive en `politica_ficheros.json`, en `arnes-base`.
Detalle en `GUIA_INSTALACION.md`.