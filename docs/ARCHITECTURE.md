<!-- docs/ARCHITECTURE.md -->
# Arquitectura · bc3 (ETL de presupuestos FIEBDC-3)

> Este documento es NORMATIVO: el spec-author diseña contra él y el
> reviewer rechaza lo que lo incumpla. Si no está aquí, no es un requisito.
>
> **La sección «Semántica de dominio imprescindible» la valida el humano.**
> Lo escrito abajo está deducido del código y verificado numéricamente
> contra los BC3 de `input/`, pero cualquier regla que el humano corrija
> manda sobre esto.

## Qué hace este proyecto

Transforma presupuestos de obra en formato **FIEBDC-3 (`.bc3`)** para que
puedan cargarse en el ERP y compararse con el catálogo de productos de
Ruesma. Se usa desde escritorio (`main_gui.py`) o por consola, en el puesto
de quien prepara el coste de una obra; no se despliega en Azure. Tiene dos
fases independientes:

- **FASE 1 (ETL, `main.py`)** — lee un `.bc3` de `input/`, escribe en
  `output/` una copia normalizada (`presupuesto_material.bc3`) y un CSV
  plano del árbol de conceptos.
- **FASE 2 (`main_phase2.py`)** — sobre la salida de la FASE 1, clasifica
  cada descompuesto contra `data/catalogo.xlsx` delegando en el servicio
  clasificador del proyecto hermano `ocr_service`, y reescribe el BC3 con
  los códigos del catálogo.

## Capas y estructura

Hexagonal, con el flujo de la FASE 1 montado como pipeline de steps:

- `domain/` — registros FIEBDC-3 (`domain/bc3/records.py`) y modelos de
  presupuesto. Sin dependencias externas.
- `application/pipeline/` — `Pipeline`, `ETLContext` y los steps, en este
  orden: `ResolveInputStep` → `ConvertirPorcentualesStep` (opcional, bandera
  `PORCENTUALES_A_UD`) → `TransformBC3Step` → `BuildTreeStep` →
  `PrintTreeStep` (opcional) → `ExportCsvStep` (opcional).
- `application/services/` — `build_tree_service` (árbol + clones),
  `export_csv_service`, `parse_bc3_service`, `phase2_code_mapper` (FASE 2) y
  `budget_bc3_batch_service` (troceado en lotes hacia el clasificador).
- `infrastructure/bc3/bc3_modifier.py` — la transformación real del fichero,
  en dos pasadas (ver abajo).
- `infrastructure/bc3/bc3_porcentajes.py` — pasada previa e independiente
  (`.bc3` → `.bc3`, F-002) que convierte los descompuestos porcentuales a
  `UD` con cantidad 1 materializando el importe que calcula Presto. Corre
  ANTES de `convert_to_material` y su salida alimenta al resto del ETL;
  también se puede lanzar sola con
  `python -m interface_adapters.cli.porcentuales_cli entrada.bc3 salida.bc3`.
- `infrastructure/clients/`, `infrastructure/ai/` — clientes del servicio
  clasificador y de Gemini.
- `interface_adapters/` — `run_etl` y `construir_pipeline` (controlador), la
  GUI Tkinter y `cli/porcentuales_cli.py`.
- `config/settings.py` — `Settings`, dataclass **congelada** (`frozen=True`):
  para variar un valor se clona con `dataclasses.replace`, no se asigna.

`bc3_modifier.convert_to_material` hace **dos pasadas** sobre el fichero y
ese orden es parte del diseño:

1. `_collect_info` recorre el BC3 entero y construye `code_map` (códigos
   largos → truncados únicos), `tipo_map`, `children_map`, `price_map` y
   `meas_pair_map`. Nada se puede decidir en una sola pasada porque un `~D`
   puede referirse a un `~C` que aparece más abajo.
2. La reescritura aplica las reglas y emite el fichero de salida.

## Semántica de dominio imprescindible

1. **Formato del fichero.** Codificación `latin-1`. Una línea por registro,
   campos separados por `|`, subcampos por `\`. Registros usados: `~V`
   (cabecera), `~C` (concepto), `~D` (descomposición), `~M` (medición),
   `~T` (texto largo).
2. **`~C|codigo|unidad|resumen|precio|fecha|tipo|`.** El `tipo` es
   `0` = partida o capítulo, `1` = mano de obra, `2` = maquinaria,
   `3` = material.
3. **El código dice la jerarquía.** Un código con `##` es el supercapítulo
   (raíz); con `#`, un capítulo; sin `#` y con tipo `0`, una partida real.
   Esa distinción, y no el tipo, es la que decide qué es estructura y qué es
   precio.
4. **`~D|padre|hijo\factor\rendimiento\...|`** — tripletas. La línea debe
   terminar en `\|`: exactamente una barra antes del cierre. Un `~D` sin esa
   barra final rompe la importación en Presto, y por eso existe
   `_ensure_d_trailing_backslash`.
5. **Importe de una línea de descompuesto = precio del hijo × rendimiento**
   (el factor es un multiplicador adicional, normalmente `1`). El precio del
   padre es la suma de los importes de sus hijos.
6. **Conceptos porcentuales.** Su código empieza por `%`; la unidad viene
   vacía (Siroco, El Escorial) o como `%` (bancos de precios). En su `~C`, el
   campo *precio* es el porcentaje **en tanto por ciento** (`-3` = −3 %), y
   en el `~D` del padre el *rendimiento* es ese mismo porcentaje **en tanto
   por uno** (`-0.03`). Su importe **no** es precio × rendimiento: es
   `rendimiento × (suma de los importes de las líneas que le preceden en ese
   mismo ~D)`. Es una línea que modifica a las anteriores, no un material.
   Verificado contra `input/`: en `05.06.29`, `73 × 1 = 73` y `%VID` con
   `-0.02` da `73 − 1,46 = 71,54`, el precio exacto del padre.
7. **El mismo concepto `%` se reutiliza en muchos padres** (`%RF` aparece en
   decenas de descompuestos). Su importe depende de la base de cada padre,
   así que **no se le puede asignar un precio único en el `~C`** sin
   clonarlo por padre.
8. **Los rendimientos a 0 son deliberados.** Un descompuesto con rendimiento
   `0` es una alternativa que se dejó a la vista pero no se usa (otra oferta
   de proveedor, otra solución constructiva). No se borra y no se «corrige».
9. **Reglas propias del ETL de este proyecto** (FASE 1): códigos truncados a
   20 caracteres con desambiguación sin colisiones; descompuestos de tipo
   1/2/3 forzados a `3` (material); toda la rama bajo una partida real
   forzada a material; unidades unificadas a 13 formas canónicas
   (`%, CM, H, KG, T, L, M, M2, M3, PA, PLANTA, UD, VIV`), y lo que no
   encaje cae a `UD`; se intercala un concepto `CD#` («COSTE DIRECTO») entre
   el supercapítulo y sus hijos; las partidas sin hijos reciben un clon `.1`
   que hereda su precio.

## Acceso a datos y sistemas externos

- **Ficheros locales.** `input/` es **solo lectura**: son los BC3 reales de
  obra. Todo resultado va a `output/`. Un BC3 de entrada no se sobrescribe
  nunca con su propia salida.
- **Servicio clasificador (`ocr_service`).** Se invoca por subprocess (o como
  librería / API, según cliente) con las rutas de `.env`
  (`OCR_SERVICE_*`, `BC3_SERVICE_*`). Es un repositorio hermano: desde aquí
  **no se modifica**.
- **Gemini / OpenAI.** Claves en `.env`, con límite por RPM. **PROHIBIDO
  llamarlos desde los tests**: cuestan dinero, no son deterministas y
  colgarían al arnés. En tests, el cliente se dobla.
- **`.env`** no se versiona ni se imprime. Ninguna clave entra en specs,
  progress ni commits.

## Infra y despliegue

No hay despliegue en la nube. La entrega es un ejecutable de escritorio
construido con **PyInstaller** (`build/`, `dist/`; ambos fuera del alcance
del arnés). Las rutas en runtime se resuelven con
`infrastructure/filesystem/app_paths.py`, que distingue el caso congelado
(`sys.frozen`) del caso script.
