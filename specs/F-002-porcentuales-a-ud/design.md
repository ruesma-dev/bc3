<!-- specs/F-002-porcentuales-a-ud/design.md -->
# F-002 · Diseño técnico · Porcentuales a UD

Encaje en `docs/ARCHITECTURE.md`: la transformación es una **pasada previa
independiente** `.bc3` → `.bc3`, adaptador de infraestructura, con su propio
step de pipeline delante de `TransformBC3Step`. No entra en
`bc3_modifier.convert_to_material` (decisión del humano, ver §Riesgos D5).

## Ficheros a crear

- `infrastructure/bc3/bc3_porcentajes.py` — la pasada completa.
- `interface_adapters/cli/__init__.py` y `.../cli/porcentuales_cli.py` (R22):
  `python -m interface_adapters.cli.porcentuales_cli <entrada.bc3> <salida.bc3>`.
- `tests/test_f002_porcentuales.py` — tests unitarios (R1-R18, R20-R23).
- `tests/test_f002_invariante.py` — invariante sobre fixtures y sobre `input/`
  (R19), este último `skip` si no hay ficheros en `input/`.
- `tests/fixtures/f002_*.bc3` — fixtures pequeñas (ver §Fixtures).

## Ficheros a modificar

- `config/settings.py` — campo nuevo
  `porcentuales_a_ud: bool = _env_bool("PORCENTUALES_A_UD", "true")` (dataclass
  congelada: en tests se clona con `dataclasses.replace`).
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

`convert_to_material` y el resto de `bc3_modifier.py`;
`application/services/build_tree_service.py` (los clones `.1` siguen igual);
`export_csv_service`, `parse_bc3_service`, todo lo de FASE 2 y de
`infrastructure/ai|clients`; `input/` (solo lectura).

## Módulo `infrastructure/bc3/bc3_porcentajes.py`

Capa: **infrastructure** (adaptador de fichero). Sin dependencias nuevas:
`pathlib`, `decimal`, `dataclasses`, `logging` y el helper de `bc3_modifier`.
Nada de `print()` (CONVENTIONS): `logging.getLogger(__name__)`.

```python
@dataclass(frozen=True)
class CasoPorcentual:   # fila del informe (R20)
    padre: str; codigo: str; motivo: str; rendimiento: float; importe: float

@dataclass
class InformePorcentuales:   # contadores + casos: list[CasoPorcentual]
    descompuestos: int = 0; lineas_convertidas: int = 0
    conceptos_eliminados: int = 0; casos: list[CasoPorcentual] = ...

def es_porcentual(codigo: str, unidad: str) -> bool            # R1
def calcular_importes(triples, precios, es_pct) -> list[Decimal]
        # D2/D4 en orden, un importe por tripleta; ÚNICA implementación del
        # cálculo, usada por la pasada y por los tests
def planificar(src: Path, encoding="latin-1") -> Plan          # PASADA 1
def convertir_porcentuales(src: Path, dst: Path, *, encoding="latin-1",
                           activo: bool = True) -> InformePorcentuales
        # PASADA 2: reescribe; si activo=False copia sin tocar (R21)
```

### Pasada 1 — `planificar`

Recorre el fichero una vez y construye:

- `precios: dict[codigo, Decimal | None]` y `unidades: dict[codigo, str]`
  desde los `~C` (campo 3 y campo 1).
- `triples_por_padre: dict[codigo, list[tuple[hijo, factor, rend]]]` desde los
  `~D`, **conservando el orden** (`children_map` de `bc3_modifier` no sirve:
  pierde factor y rendimiento). Cada línea `~D` se planifica por separado.
- `codigos_ocupados: set[str]` = todos los códigos `~C` **más** su truncado
  `codigo[:20]`. Incluir el truncado evita que el clon choque después con un
  código largo que `convert_to_material` recortará (R5, ver §Riesgos D6).

Con eso decide, por cada línea `~D`:

1. Si no tiene porcentuales → sin plan (la línea sale igual).
2. Si alguna línea previa a una porcentual no tiene precio numérico → motivo
   `base_indeterminada`, sin plan (R16).
3. Si **todas** sus líneas son porcentuales y el padre tiene precio `P` ≠ 0 →
   plan con base reconstruida (R9), motivo `base_reconstruida`; si algún
   `(1 + r_i)` ≤ 0, sin plan y motivo `base_no_despejable` (R9 ter).
4. Resto → plan normal.

El plan de una línea `~D` es una lista de `(indice_tripleta, codigo_clon,
precio_clon, resumen, fecha)`, más una línea de base opcional en cabeza (caso 3).

### Cálculo (R2, R6, R9, D3)

Todo en `decimal.Decimal` desde el **texto** del BC3 (nunca desde `float`), con
`quantize(Decimal("0.01"), ROUND_HALF_UP)` al fijar el precio del clon. La base
acumulada suma el importe **ya redondeado**, así que es la que se puede releer
en la salida: el ERP recalcula lo mismo que calculamos nosotros.

Ejemplo real (Siroco `43.15`): `OPTIMIZADOR MPP 41,18 × 1,2 = 49,416` →
`%SUB25` `12,35` → `%SUB20` sobre `61,766` → `12,35` → `%SUB10` sobre `74,116`
→ `7,41`. Suma 81,526 frente a 81,5364 antes: desvío 0,0104, dentro de la
tolerancia de R19 (0,025).

### Base reconstruida del `~D` solo-porcentual (R9, R9 bis, R9 ter)

`base = P / Π(1 + r_i)` con `Decimal`, `quantize(0.01, ROUND_HALF_UP)`; luego
los importes porcentuales según R6 sobre esa base; y por último
`precio(.P0) = P − Σ(importes porcentuales)`, que absorbe el residuo en la
línea de base y deja la suma clavada en `P` (R19 bis). Los cuatro casos reales,
comprobados con `Decimal` al escribir esto y que van tal cual a los tests:

- `ICV260` (P 291,50 · `0,1178` y `0,154`) → base 225,98 · 26,62 · 38,90.
- `ICV270` (P 369,50 · mismos rendimientos) → base 286,45 · 33,74 · 49,31.
- `31.04.03.01` (P 1.100,00 · `0,025`) → base 1.073,17 · 26,83.
- `32.03.04.32` (P 1.117,65 · `0,17`) → base 955,26 · 162,39.

La línea de base lleva unidad `UD`, factor 1, rendimiento 1, tipo `3`, la fecha
del `~C` del padre y su mismo resumen (precedente: el clon `.1` de
`build_tree_service` hereda la descripción del padre).

### Códigos de clon (R5)

`f"{padre}.P{n}"`, y `f"{padre}.P0"` para la línea de base de R9: mismo sufijo
de tres caracteres, misma familia visible en Presto y ordena delante de sus
porcentuales. Si pasa de 20, se recorta el padre:
`padre[:20 - len(sufijo)] + sufijo`. Medido en `input/`: el padre más largo con
porcentuales tiene 19 caracteres, así que el recorte es real y hay que
probarlo. La unicidad **reutiliza** `bc3_modifier._shorten_code_unique`, que ya
implementa la escalera naive → 19+último → sufijo `#i`. Se le añade
`forzar_unicidad=True` porque hoy devuelve tal cual cualquier código de 20 o
menos sin mirar `used`, que es justo lo que aquí no vale; con el `False` por
defecto los llamantes actuales no cambian. Descartado duplicar la escalera en
el módulo nuevo: dos copias divergen.

El clon **no** empieza por `%` ni lleva unidad `%`, así que una segunda pasada
sobre la salida no vuelve a convertirlo (idempotencia, verificable con un test).

### Pasada 2 — reescritura

Una sola lectura línea a línea, escribiendo según el plan:

- `~C` de un porcentual a eliminar (R13) → no se escribe; su `~T` tampoco.
- `~D` con plan → se reconstruyen las tripletas: las normales con su texto
  original, las porcentuales como `clon\1\1` y, en el caso R9, la línea de base
  `.P0\1\1` en cabeza; la línea se cierra con `_format_d_triplets` de
  `bc3_modifier` (garantiza el `\|` de R17). Los `~C` de los clones y el de la
  línea de base se escriben justo **antes** de esa línea `~D`.
- `~M` cuyo par sea `padre\porcentual` convertido → se sustituye el hijo por
  el clon (R15).
- Cualquier otra línea → se copia tal cual, sin `clean_text` (R18).

## Fixtures (`tests/fixtures/`)

Ficheros mínimos (una decena de líneas), en `latin-1`, con números reales
tomados de `input/`:

- `f002_cadena.bc3` — `43.15`, tres porcentuales encadenados (R6, base que
  incluye al anterior).
- `f002_negativo.bc3` — Escorial `05.06.29`: `73 × 1` y `%VID -0,02` → `-1,46`,
  total `71,54` (R4, descuento).
- `f002_ceros.bc3` — `07.02.05`: dos líneas a rendimiento 0, `%MAVEN -0,05`
  con base 0 y una línea normal posterior (R10, R11, R7).
- `f002_solo_pct.bc3` — `31.04.03.01` (padre 1.100, único hijo `%SUB2.5`) y
  `32.03.04.32` (R9 con una sola porcentual).
- `f002_solo_pct_doble.bc3` — `ICV260` e `ICV270` de
  `input/lagunamodificado16julio.bc3`: dos porcentuales (`0,1178` y `0,154`) y
  ninguna línea normal. El caso que destapó el defecto (R9, R9 bis, R19 bis).
- `f002_descuento_total.bc3` — solo-porcentual con `r = -1` →
  `base_no_despejable`, el `~D` sale intacto (R9 ter).
- `f002_unidad_pct.bc3` — porcentual detectado por unidad `%` y no por código
  (`~C|% DESC. FACHADA SEGO|%|...`), más un `~M` sobre el par (R1, R15).
- `f002_sin_precio.bc3` — línea previa sin `~C` (R16).
- `f002_codigo_largo.bc3` — padre de 19-20 caracteres y un código ya ocupado
  que fuerza la escalera de desambiguación (R5).

## Tests: qué se prueba y cómo

- Nombres trazables `test_f002_rN_...` (CONVENTIONS). Sin red, sin BBDD, sin
  Gemini/OpenAI/`ocr_service`: la pasada no los usa.
- **El invariante no puede ser tautológico.** Comparar entrada y salida con la
  misma `calcular_importes` las mide a las dos con el mismo error. Por eso R19
  se prueba en dos planos: (a) entrada-vs-salida sobre cada `~D` de las
  fixtures y de cada `.bc3` de `input/`; y (b) tests con **números literales**
  escritos a mano desde Presto (81,5364 · 71,54 · −1,46 · 16,00), que fallan si
  `calcular_importes` se equivoca en la misma dirección en ambos lados.
- **R19 bis es obligatorio y se prueba sobre datos reales**: por cada `~D`
  solo-porcentual de `input/` (los cuatro de §Base reconstruida), el importe
  calculado sobre la SALIDA debe dar el precio del `~C` del padre con
  tolerancia de un céntimo. Es lo único que caza el +15,4 % de `ICV260`.
- Test de orden: permutar dos tripletas de un `~D` cambia el resultado (lo
  exige el criterio `acceptance` de `harness/features.json`).
- Test de que la salida sigue siendo válida: todo código de `~D` existe como
  `~C`, y todo `~D` acaba en `\|`.
- Test de idempotencia: aplicar la pasada a su propia salida no cambia nada.

## Verificación MANUAL (humano)

1. `python -m interface_adapters.cli.porcentuales_cli "input/COSTE_250128_Siroco_Rv4mlo.bc3" "output/siroco_sin_pct.bc3"`
2. Importar `output/siroco_sin_pct.bc3` en Presto: el total y el precio de
   `43.15`, `05.06.29`, `31.04.03.01` y `32.03.04.32` deben ser los de antes.
3. Ídem con `input/lagunamodificado16julio.bc3`: `ICV260` debe seguir valiendo
   291,50 (no 336,39) e `ICV270` 369,50.
4. Contrastar con la **captura de ejemplos de Presto** que el humano va a
   localizar: confirmar (a) que el unitario de una línea porcentual es
   `rendimiento × base previa` redondeado a 2 decimales y no a más, y (b) que
   en un solo-porcentual el precio del padre es el precio final, con los
   porcentajes ya dentro (base de la decisión D3).

## Riesgos y decisiones

- **D1 · Precio del clon a 2 decimales, sin corregir el residuo.** Medido: los
  desvíos por redondeo en `input/` son ≤ 0,014. Corregir el residuo en la
  última línea falsearía una línea que no es porcentual y sería inestable
  cuando esa línea es una alternativa a rendimiento 0. Descartado.
- **D2 · El `~C` porcentual se elimina** (R13) en lugar de quedar huérfano.
  Motivos: el criterio `acceptance` pide que no quede unidad `%` en el
  fichero; un concepto `%` huérfano lo vería la FASE 2 y lo mandaría a
  clasificar contra el catálogo (coste y ruido); y nada lo referencia, así que
  borrarlo no mueve ningún importe. Se conserva si algo lo sigue apuntando
  (R14).
- **D3 · Descompuesto solo-porcentual: se reconstruye la base** (R9). La
  lectura literal del invariante diría precio 0 —el importe calculado hoy es
  0—, pero el precio de esas partidas está puesto a mano en el `~C` y un ERP
  que recalculase desde el descompuesto se llevaría a cero 1.100 €, 1.117,65 €,
  291,50 € y 369,50 €. **Corrección del 2026-09-17, decidida por el humano**:
  la primera versión daba al primer clon el precio del padre y aplicaba encima
  los porcentajes siguientes, lo que con DOS porcentuales cobraba el beneficio
  dos veces (`ICV260` salía a 336,39, un +15,4 %). Con `P` entendido como
  precio final se despeja la base hacia atrás, el `~D` cuadra exactamente en `P`
  con cualquier número de porcentuales y queda con sentido de negocio: la
  hornacina cuesta 1.073,17 más su 2,5 %.
- **D4 · Los porcentuales a rendimiento 0 sí se convierten** (R12), con precio
  0. La regla «los rendimientos 0 no se tocan» protege alternativas normales
  desactivadas; aquí el importe sigue siendo 0 y no convertirlas dejaría un
  concepto `%` vivo.
- **D5 · Pasada propia y no un paso dentro de `convert_to_material`**:
  requisito del humano. Ventaja: el invariante se mide entrada-contra-salida
  de la propia pasada, sin truncados, `CD#` ni clones `.1` de por medio.
- **D6 · Colisión con el truncado posterior.** `convert_to_material` recorta
  códigos > 20 y registra los cortos según los va leyendo, así que un código
  largo podría recortarse sobre un clon nuestro definido más abajo. Se mitiga
  reservando en `codigos_ocupados` también los `codigo[:20]` de todos los `~C`.
  Riesgo residual documentado, no arreglamos ese comportamiento aquí.
- **D7 · `TransformBC3Step` llama a `convert_to_material` dentro de un
  `try/except TypeError`** cuyo `try` hoy **siempre** falla (esos keywords no
  existen): la rama viva es el `except`, y un `TypeError` lanzado dentro de la
  función se tragaría en silencio. No se arregla en F-002.
- **Límite de microservicio**: ninguno. Todo lo que necesita la pasada está en
  el propio `.bc3`; no consulta catálogo, ni ERP, ni servicios hermanos.
