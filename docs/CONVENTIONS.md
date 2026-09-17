<!-- docs/CONVENTIONS.md -->
# Convenciones de código

> Documento normativo: el reviewer valida contra él. Las secciones que no
> apliquen al lenguaje del proyecto se borran; no se dejan «por si acaso».

## Estructura y estilo

- Primera línea de CADA fichero de código: comentario con su ruta relativa.
  Ej.: `# application/steps/mi_step.py`
- Arquitectura hexagonal (Ports & Adapters): domain sin dependencias,
  application orquesta, infrastructure adapta. Patrón pipeline + steps con
  objeto contexto para procesos encadenados; la composición del pipeline se
  hace en el punto de entrada, no dentro de los steps.
- Configuración leída del entorno (`.env` en local), parametrizaciones en
  ficheros de datos versionados. **Ningún secreto en código.**
- Logging estructurado. Prohibido `print()` (o equivalente) en código de
  producción; permitido en scripts puntuales.
- Errores transitorios de red: reintentos con backoff, nunca bucle desnudo.

### Python

- Python 3.12, PEP8, type hints en firmas públicas.
- `from __future__ import annotations` en cabecera, como el resto del repo.
- Configuración: `config/settings.py`, dataclass **congelada**
  (`frozen=True`) alimentada por `.env`. Para variar un valor se clona con
  `dataclasses.replace`, nunca se asigna.
- Rutas con `pathlib.Path`, nunca concatenando cadenas.
- Todo fichero BC3 se abre con codificación `latin-1`.
- Dependencias del ETL: `pandas` y `openpyxl`. Añadir una dependencia nueva
  se decide en la spec, no sobre la marcha.

> **Deuda conocida:** el código actual de `infrastructure/bc3/` usa `print()`
> con prefijo `[BC3 DEBUG]` en vez de logging. No se exige limpiarlo de golpe,
> pero **el código nuevo no añade `print()`**: usa `logging`.

## Tests

- Los unit tests no tocan red ni BBDD: mocks y fixtures.
- Un requisito EARS (o criterio `acceptance`) => al menos un test con nombre
  trazable (`test_fXXX_rN_...`).
- Nada de credenciales en los tests, ni siquiera de entornos de desarrollo.

## Git

- Ramas: `master` (estable; este repo no tiene `main`) ← `dev`
  (integración) ← `feature/F-XXX-slug`.
- Commits: `F-XXX Tn: descripción` (tareas) o `F-XXX: descripción` (ajustes).
- Los agentes solo hacen commit local en ramas feature. Push, merge a dev y
  PRs: siempre el humano.
- Los originales en PDF u ofimática no se versionan (ver `.gitignore`).

<!-- ==================== INICIO · ENTORNO DE RUESMA ==================== -->
<!-- Convenios de la organización, no del arnés. Fuera de ese entorno,     -->
<!-- borra el bloque entero hasta el comentario de cierre.                 -->

## Convenios del entorno de Ruesma

- **Todo en español**, incluidos comentarios de código y mensajes de commit.
- CSV de salida: UTF-8 BOM, `;` como separador, coma decimal (Excel ES).
- PowerShell: UTF-8 con BOM y CRLF (salvo excepciones documentadas).
- Medidas DAX sin tildes.
- Azure: imágenes con tags fechados (`rYYYYMMDD-HHmm`), nunca reescribir
  tags; secretos como secrets del recurso, jamás desplegar `.env`.

<!-- ===================== FIN · ENTORNO DE RUESMA ====================== -->
