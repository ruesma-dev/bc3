<!-- specs/F-002-porcentuales-a-ud/design.md -->
# F-002 · Diseño técnico · Porcentuales a UD

Encaje en `docs/ARCHITECTURE.md`: la transformación es una **pasada previa
independiente** `.bc3` → `.bc3`, adaptador de infraestructura, con su propio
step de pipeline delante de `TransformBC3Step`. No entra en
`bc3_modifier.convert_to_material` (decisión del humano, §Riesgos D5).

## Ficheros a crear

- `infrastructure/bc3/bc3_porcentajes.py` — la pasada completa.
- `interface_adapters/cli/__init__.py` y `.../cli/porcentuales_cli.py` (R22):
  `python -m interface_adapters.cli.porcentuales_cli <entrada.bc3> <salida.bc3>`.
- `tests/test_f002_porcentuales.py` — unitarios (R1-R18, R20-R23).
- `tests/test_f002_invariante.py` — R19 sobre fixtures e `input/` (`skip` si la
  carpeta está vacía).
- `tests/fixtures/f002_*.bc3` — fixtures pequeñas (§Fixtures).

## Ficheros a modificar

- `config/settings.py` — campos nuevos
  `porcentuales_a_ud: bool = _env_bool("PORCENTUALES_A_UD", "true")` y
  `porcentuales_decimales: int = _env_int("PORCENTUALES_DECIMALES", "4")`,
  saneado al rango 2..6 (R6 bis). Dataclass congelada: en tests, `replace`.
- `application/pipeline/pipeline.py` — nuevo campo en `ETLContext`:
  `preprocessed_path: Optional[Path] = None`.
- `application/pipeline/steps.py` — nuevo `ConvertirPorcentualesStep` y, en
  `TransformBC3Step`, `src = ctx.preprocessed_path or ctx.original_path`.
- `interface_adapters/controllers/etl_controller.py` — composición:
  `ResolveInputStep → ConvertirPorcentualesStep → TransformBC3Step → ...`.
- `infrastructure/bc3/bc3_modifier.py` — **un único cambio**: keyword
  `forzar_unicidad: bool = False` en `_shorten_code_unique` (§Códigos de clon);
  con el valor por defecto su comportamiento actual no cambia.

## Ficheros que NO se tocan

`convert_to_material` y el resto de `bc3_modifier.py`; `build_tree_service.py`
(los clones `.1` siguen igual); `export_csv_service`, `parse_bc3_service`, todo
lo de FASE 2 y de `infrastructure/ai|clients`; `input/` (solo lectura).

## Módulo `infrastructure/bc3/bc3_porcentajes.py`

Capa: **infrastructure**. Sin dependencias nuevas: `pathlib`, `decimal`,
`dataclasses`, `logging` y el helper de `bc3_modifier`. Nada de `print()`
(CONVENTIONS): `logging.getLogger(__name__)`.

```python
@dataclass(frozen=True)
class CasoPorcentual:   # fila del informe (R20)
    padre: str; codigo: str; motivo: str; rendimiento: float; importe: float

@dataclass
class InformePorcentuales:  # contadores, decimales usados y list[CasoPorcentual]

def es_porcentual(codigo: str, unidad: str) -> bool            # R1
def calcular_importes(triples, precios, es_pct, decimales) -> list[Decimal]
        # D2/D4 en orden; ÚNICA implementación del cálculo, para pasada y tests
def planificar(src: Path, *, decimales: int) -> Plan           # PASADA 1
def convertir_porcentuales(src, dst, *, encoding="latin-1", activo=True,
                           decimales: int = 4) -> InformePorcentuales
        # PASADA 2: reescribe; si activo=False copia sin tocar (R21)
```

### Pasada 1 — `planificar`

Recorre el fichero una vez y construye:

- `precios` y `unidades` por código, desde los `~C` (campos 3 y 1).
- `triples_por_padre: dict[codigo, list[tuple[hijo, factor, rend]]]` desde los
  `~D`, **conservando el orden** (`children_map` de `bc3_modifier` no sirve:
  pierde factor y rendimiento). Cada línea `~D` se planifica por separado.
- `codigos_ocupados` = todos los códigos `~C` **más** su truncado `codigo[:20]`,
  para que el clon no choque con un código largo que `convert_to_material`
  recortará luego (R5, §Riesgos D6).

Con eso decide, por cada línea `~D`: sin porcentuales → sin plan; línea previa a
una porcentual sin precio numérico → `base_indeterminada`, sin plan (R16);
**todas** porcentuales y padre con precio `P` ≠ 0 → base reconstruida (R9),
motivo `base_reconstruida`, salvo que algún `(1 + r_i)` ≤ 0, que deja el `~D`
sin plan como `base_no_despejable` (R9 ter); resto → plan normal.

El plan de un `~D` es una lista de `(indice_tripleta, codigo_clon, precio_clon,
resumen, fecha)`, más una línea de base opcional en cabeza.

### Cálculo (R2, R6, R9, D3)

Todo en `decimal.Decimal` desde el **texto** del BC3 (nunca desde `float`), con
`quantize(Decimal(1).scaleb(-d), ROUND_HALF_UP)` al fijar el precio del clon,
siendo `d = settings.porcentuales_decimales`. La base acumulada suma el importe
**ya redondeado**, así que es la que se puede releer en la salida. El número se
escribe con `f"{valor:f}"` + `normalize()`, sin exponentes ni relleno (R7).

### Decimales del precio del clon (R6, R6 bis, R6 ter)

`d` es configurable —**2 por defecto**—, y el criterio no es minimizar el error
de redondeo sino **reproducir lo que calcula Presto**, que es lo que pide la
feature. Se contrasta contra el precio que el `~C` de cada partida declara,
escrito por Presto al exportar; fuera, las de precio a mano (R9 y `07.02.01a`):

| `d` | Siroco | laguna |
|---|---|---|
| **2** | **260/262 (99,2 %)** | **402/402 (100 %)** |
| 3 | 237/262 (90,5 %) | 321/402 (79,9 %) |
| 4 | 247/262 (94,3 %) | 322/402 (80,1 %) |
| 6 | 247/262 (94,3 %) | 320/402 (79,6 %) |

Presto redondea a céntimos **cada línea**, así que más precisión se aleja de su
resultado en vez de acercarse. Con `d = 2` (`43.15`): `41,18 × 1,2 = 49,416` →
`12,35` → `12,35` (sobre 61,766) → `7,41`; suma 81,526. El `~C` de `C020615`
declara 172,60, que es lo que da `d = 2`: el 172,59 de `d = 4` es el cálculo
exacto, no el de Presto. Subir `d` sigue siendo tocar `PORCENTUALES_DECIMALES`.

### Base reconstruida del `~D` solo-porcentual (R9, R9 bis, R9 ter)

`base = P / Π(1 + r_i)` con `Decimal`, redondeada con los mismos `d` decimales
de R6; luego los importes porcentuales sobre esa base; y por último
`precio(.P0) = P − Σ(importes porcentuales)`, que absorbe el residuo en la línea
de base y deja la suma clavada en `P` (R19 bis) sea cual sea `d`. Los cuatro
casos reales, calculados con `Decimal` y que van tal cual a los tests
(base · clones, con `d = 4` por defecto y `d = 2` entre paréntesis):

- `ICV260` (P 291,50 · `0,1178` y `0,154`) → 225,9792 · 26,6204 · 38,9004
  (225,98 · 26,62 · 38,90).
- `ICV270` (P 369,50 · ídem) → 286,4471 · 33,7435 · 49,3094 (286,45 · 33,74 · 49,31).
- `31.04.03.01` (P 1.100 · `0,025`) → 1.073,1707 · 26,8293 (1.073,17 · 26,83).
- `32.03.04.32` (P 1.117,65 · `0,17`) → 955,2564 · 162,3936 (955,26 · 162,39).

La línea de base lleva unidad `UD`, factor 1, rendimiento 1, tipo `3`, y la
fecha y el resumen del `~C` del padre (precedente: el clon `.1`).

### Códigos de clon (R5)

`f"{padre}.P{n}"`, y `f"{padre}.P0"` para la línea de base de R9: mismo sufijo
de tres caracteres, misma familia visible en Presto y ordena delante de sus
porcentuales. Si pasa de 20, se recorta el padre:
`padre[:20 - len(sufijo)] + sufijo`; medido en `input/`, el padre más largo con
porcentuales tiene 19 caracteres, así que el recorte es real y hay que probarlo.
La unicidad **reutiliza** `bc3_modifier._shorten_code_unique` y su escalera
naive → 19+último → sufijo `#i`, con `forzar_unicidad=True` porque hoy devuelve
tal cual cualquier código de 20 o menos sin mirar `used`; con el `False` por
defecto los llamantes actuales no cambian. Descartado duplicar la escalera: dos
copias divergen. El clon **no** empieza por `%` ni lleva unidad `%`, así que una
segunda pasada no vuelve a convertirlo (idempotencia, con test).

### Pasada 2 — reescritura

Una sola lectura línea a línea, escribiendo según el plan:

- `~C` de un porcentual a eliminar (R13) → no se escribe; su `~T` tampoco.
- `~D` con plan → tripletas reconstruidas: las normales con su texto original,
  las porcentuales como `clon\1\1` y, en el caso R9, la línea de base `.P0\1\1`
  en cabeza; se cierra con `_format_d_triplets` de `bc3_modifier` (garantiza el
  `\|` de R17). Los `~C` de los clones y el de la base van justo **antes**.
- `~M` cuyo par sea `padre\porcentual` convertido → el hijo pasa a ser el clon
  (R15). Cualquier otra línea se copia tal cual, sin `clean_text` (R18).

## Fixtures (`tests/fixtures/`)

Ficheros mínimos (una decena de líneas), en `latin-1`, con números reales de
`input/`. Una por caso, con el requisito que cubre entre paréntesis:

- `f002_cadena.bc3` — `43.15`, tres porcentuales encadenados (R6).
- `f002_negativo.bc3` — `05.06.29`: `73 × 1` y `%VID -0,02` → `-1,46` (R4).
- `f002_ceros.bc3` — `07.02.05`: dos rendimientos 0, `%MAVEN` con base 0 y una
  línea normal posterior (R7, R10, R11).
- `f002_solo_pct.bc3` — `31.04.03.01` y `32.03.04.32`: R9 con una porcentual.
- `f002_solo_pct_doble.bc3` — `ICV260` e `ICV270` de
  `input/lagunamodificado16julio.bc3`, dos porcentuales y ninguna línea normal:
  el caso que destapó el defecto (R9, R9 bis, R19 bis).
- `f002_descuento_total.bc3` — solo-porcentual con `r = -1` (R9 ter).
- `f002_unidad_pct.bc3` — porcentual por unidad `%` y `~M` sobre el par (R1, R15).
- `f002_sin_precio.bc3` — línea previa sin `~C` (R16); `f002_codigo_largo.bc3` —
  padre de 19-20 caracteres y un código ya ocupado, que fuerza la escalera (R5).

## Tests: qué se prueba y cómo

- Nombres trazables `test_f002_rN_...` (CONVENTIONS). Sin red, sin BBDD, sin
  Gemini/OpenAI/`ocr_service`: la pasada no los usa.
- **El invariante no puede ser tautológico.** Comparar entrada y salida con la
  misma `calcular_importes` las mide con el mismo error. Por eso R19 se prueba
  en dos planos: (a) entrada-vs-salida sobre cada `~D` de las fixtures y de cada
  `.bc3` de `input/`; y (b) tests con **números literales** de Presto (81,5364 ·
  71,54 · −1,46 · 16,00), que fallan si el cálculo yerra en los dos lados.
- **R19 y R19 bis se comprueban con `d = 2` y con `d = 4`** (`Settings` clonado
  con `replace`), porque la tolerancia depende de `d` y el residuo de R9 bis
  también. Además, un test fija la salida esperada de `43.15` en los dos casos.
- **R19 bis es obligatorio y sobre datos reales**: por cada `~D` solo-porcentual
  de `input/` (los cuatro de §Base reconstruida), el importe de la SALIDA debe
  dar el precio del `~C` del padre ± 0,01. Es lo único que caza el +15,4 %.
- Test de orden: permutar dos tripletas de un `~D` cambia el resultado (lo exige
  el `acceptance` de `harness/features.json`).
- Test de salida válida (todo código de `~D` existe como `~C` y todo `~D` acaba
  en `\|`) y test de idempotencia (repasarla no cambia nada).

## Verificación MANUAL (humano)

1. `python -m interface_adapters.cli.porcentuales_cli "input/COSTE_250128_Siroco_Rv4mlo.bc3" "output/siroco_sin_pct.bc3"`
2. Importarlo en Presto: el total y el precio de `43.15`, `05.06.29`,
   `31.04.03.01` y `32.03.04.32` deben ser los de antes. Ídem con
   `input/lagunamodificado16julio.bc3`: `ICV260` 291,50 (no 336,39), `ICV270`
   369,50.
3. **Cuántos decimales acepta Presto al importar** (R6): comprobarlo con la
   salida de 4; si acepta 6, subir `PORCENTUALES_DECIMALES` y repetir.
4. Contrastar con la **captura de ejemplos de Presto** que el humano localice:
   (a) el unitario de una porcentual es `rendimiento × base previa`, y (b) en un
   solo-porcentual el precio del padre es el final (base de D3).

## Riesgos y decisiones

- **D1 · Decimales del precio del clon: 4 por defecto y configurables**
  (**corrección del 2026-09-18, decidida por el humano** tras medir el impacto
  en euros; tabla en §Decimales del precio del clon). Los 2 decimales de la
  primera versión costaban 165 € en Siroco y 143 € en laguna, con sesgo al
  alza. Configurable y no cableado porque los `~C` originales no pasan de 2 y
  aún no se sabe cuántos admite Presto: si admite 6, el desvío es cero y el
  cambio es una línea de `.env`. Sigue sin corregirse el residuo en líneas no
  porcentuales (falsearía una línea ajena, e inestable si es un rendimiento 0).
- **D2 · El `~C` porcentual se elimina** (R13) en lugar de quedar huérfano:
  el criterio `acceptance` pide que no quede unidad `%`; un `%` huérfano lo
  vería la FASE 2 y lo mandaría a clasificar contra el catálogo (coste y
  ruido); y nada lo referencia, así que borrarlo no mueve ningún importe. Se
  conserva si algo lo sigue apuntando (R14).
- **D3 · Descompuesto solo-porcentual: se reconstruye la base** (R9). La lectura
  literal del invariante diría precio 0, pero el precio de esas partidas está
  puesto a mano en el `~C` y un ERP que recalculase desde el descompuesto se
  llevaría a cero 1.100 €, 1.117,65 €, 291,50 € y 369,50 €. **Corrección del
  2026-09-17, decidida por el humano**: la primera versión daba al primer clon
  el precio del padre y aplicaba encima los porcentajes siguientes, lo que con
  DOS porcentuales cobraba el beneficio dos veces (`ICV260` salía a 336,39, un
  +15,4 %). Con `P` como precio final se despeja la base hacia atrás, el `~D`
  cuadra exactamente en `P` con cualquier número de porcentuales y queda con
  sentido de negocio: la hornacina cuesta 1.073,17 más su 2,5 %.
- **D4 · Los porcentuales a rendimiento 0 sí se convierten** (R12), con precio
  0. La regla «los rendimientos 0 no se tocan» protege alternativas normales
  desactivadas; aquí el importe sigue en 0 y no convertirlas dejaría un `%` vivo.
- **D5 · Pasada propia y no un paso dentro de `convert_to_material`**: requisito
  del humano. Ventaja: el invariante se mide entrada-contra-salida de la propia
  pasada, sin truncados, `CD#` ni clones `.1` de por medio.
- **D6 · Colisión con el truncado posterior.** `convert_to_material` recorta
  códigos > 20 y registra los cortos según los va leyendo, así que un código
  largo podría recortarse sobre un clon nuestro definido más abajo. Se mitiga
  reservando en `codigos_ocupados` los `codigo[:20]` de todos los `~C`; riesgo
  residual documentado, ese comportamiento no se arregla aquí.
- **D7 · `TransformBC3Step` llama a `convert_to_material` dentro de un
  `try/except TypeError`** cuyo `try` hoy **siempre** falla (esos keywords no
  existen): la rama viva es el `except`, y un `TypeError` de dentro de la
  función se tragaría en silencio. No se arregla en F-002.
- **Límite de microservicio**: ninguno. Lo que necesita la pasada está en el
  propio `.bc3`; no consulta catálogo, ni ERP, ni servicios hermanos.
