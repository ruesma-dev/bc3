# BC3 ETL Pipeline

## Descripción

Este proyecto implementa un **ETL** (Extract‑Transform‑Load) para ficheros de presupuestos en formato **FIEBDC‑3 / BC3**.

* **Extract**  → se lee un BC3 colocado en `input/`.
* **Transform** → se genera una copia limpia en `output/` aplicando reglas de negocio (truncado de códigos, limpieza de texto, conver­sión de tipos, etc.) y se construye un árbol lógico de conceptos.
* **Load / Export**  → se imprime el árbol por consola y se exporta a CSV (`output/presupuesto_tree.csv`).

\## Arquitectura

```
├── application
│   └── services
│       ├── build_tree_service.py   # Parser + modelo de nodos
│       └── export_csv_service.py   # Árbol → CSV
├── infrastructure
│   └── bc3
│       └── bc3_modifier.py         # 2‑pass cleaner / transformer
├── interface_adapters
│   └── controllers
│       └── etl_controller.py       # Orquestador principal
├── utils
│   └── text_sanitize.py            # Limpieza de texto
└── input / output                  # Entradas y resultados
```

El diseño sigue principios de **Arquitectura Limpia / Hexagonal**: ‐ capa *application* contiene la lógica de dominio (parsear y exportar).
‐ capa *infrastructure* interactúa con el sistema de ficheros.
‐ capa *interface\_adapters* expone un controlador simple (`run_etl`).

\## Instalación

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

> Solo se necesita **pandas** para la exportación CSV.

\## Uso

1. Copia tu fichero BC3 original en la carpeta \`\` y nómbralo, por defecto, `presupuesto.bc3`.
2. Ejecuta:

```bash
python main.py            # o lanza run_etl() desde tu IDE
```

3. Revisa los resultados en \`\`:

   * `presupuesto_material.bc3` → copia limpia (sin tildes, códigos ≤20 car, T=3 donde aplique).
   * `presupuesto_tree.csv`      → tabla plana para análisis en Excel / BI.

\## Reglas de transformación

| Regla | Descripción                                                                                                        |     |
| ----- | ------------------------------------------------------------------------------------------------------------------ | --- |
| R1    | Códigos de concepto se truncan a 20 caracteres (se mantiene coherencia en todo el archivo).                        |     |
| R2    | Descompuestos (`T = 1,2,3`) se fuerzan a `T = 3` (Material).                                                       |     |
| R3    | Unidades vacías en partidas o descompuestos se rellenan con `UD`.                                                  |     |
| R4    | Todo el texto se normaliza: se eliminan tildes y caracteres no imprimibles, pero se conservan los separadores \`\~ | \`. |
| R5    | La sub‑rama completa de cada partida (`T = 0`, sin `#`) también se marca como Material.                            |     |

\## Salida CSV

El CSV contiene:

| Columna            | Contenido                                                          |
| ------------------ | ------------------------------------------------------------------ |
| tipo               | supercapítulo / capítulo / partida / des\_mo / des\_maq / des\_mat |
| codigo             | Código normalizado (≤ 20 car.)                                     |
| descripcion\_corta | Descripción del campo \~C                                          |
| descripcion\_larga | Primer texto asociado (\~T)                                        |
| unidad             | Unidad de medida (o `UD`)                                          |
| precio             | Precio unitario (\~C)                                              |
| cantidad\_pres     | Cantidad presupuestada (\~D)                                       |
| importe\_pres      | Precio × cantidad                                                  |
| hijos              | Códigos hijos directos, separados por `,`                          |
| mediciones         | Líneas completas \~M asociadas, separadas por `⏎`                  |

---

© 2025 ‐ Proyecto ETL BC3   |  Autor: Equipo Dev Construcción
