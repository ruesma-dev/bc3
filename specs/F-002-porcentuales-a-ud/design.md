<!-- specs/F-002-porcentuales-a-ud/design.md -->
# F-002 · Diseño técnico · Porcentuales a UD

Encaje en `docs/ARCHITECTURE.md`: la transformación es una **pasada previa
independiente** `.bc3` → `.bc3`, adaptador de infraestructura, con su propio
step de pipeline delante de `TransformBC3Step`. No entra en
`bc3_modifier.convert_to_material` (decisión del humano, ver §Riesgos D5).

## Ficheros a crear

- `infrastructure/bc3/bc3_porcentajes.py` — la pasada completa.
- `interface_adapters/cli/__init__.py` y
  `interface_adapters/cli/porcentuales_cli.py` — invocación suelta (R22):
  `python -m interface_adapters.cli.porcentuales_cli <entrada.bc3> <salida.bc3>`.
- `tests/test_f002_porcentuales.py` — tests unitarios (R1-R18, R20-R23).
- `tests/test_f002_invariante.py` — invariante sobre fixtures y sobre `input/`
  (R19), este último `skip` si no hay ficheros en `input/`.
- `tests/fixtures/f002_*.bc3` — fixtures pequeñas (ver §Fixtures).

## Ficheros a modificar

- `config/settings.py` — nuevo campo
  `porcentuales_a_ud: bool = _env_bool("PORCENTUALES_A_UD", "true")`.
  Dataclass congelada: en tests se clona con `dataclasses.replace`.
- `application/pipeline/pipeline.py` — nuevo campo en `ETLContext`:
  `preprocessed_path: Optional[Path] = None`.
- `application/pipeline/steps.py` — nuevo `ConvertirPorcentualesStep` y, en
  `TransformBC3Step`, `src = ctx.preprocessed_path or ctx.original_path`.
- `interface_adapters/controllers/etl_controller.py` — composición:
  `ResolveInputStep → ConvertirPorcentualesStep → TransformBC3Step → ...`.
- `infrastructure/bc3/bc3_modifier.py` — **un único cambio**: a
  `_shorten_code_unique` se le añade el keyword `forzar_unicidad: bool = False`
  (ver §Códigos de clon). Con el valor por defecto su comportamiento actual no
  cambia en nada.

## Ficheros que NO se tocan

`convert_to_material` y el resto de `bc3_modifier.py`;
`application/services/build_tree_service.py` (los clones `.1` siguen igual);
`export_csv_service`, `parse_bc3_service`, todo lo de FASE 2 y de
`infrastructure/ai|clients`; `input/` (solo lectura).

## Módulo `infrastructure/bc3/bc3_porcentajes.py`

Capa: **infrastructure** (adaptador de fichero). Sin dependencias nuevas:
`pathlib`, `decimal`, `dataclasses`, `logging` y el helper importado de
`bc3_modifier`. Nada de `print()` (CONVENTIONS): `logging.getLogger(__name__)`.

```python
@dataclass(frozen=True)
class CasoPorcentual:          # fila del informe (R20)
    padre: str; codigo: str; motivo: str
    rendimiento: float; importe: float

@dataclass
class InformePorcentuales:
    descompuestos: int = 0; lineas_convertidas: int = 0
    conceptos_eliminados: int = 0; casos: list[CasoPorcentual] = ...

def es_porcentual(codigo: str, unidad: str) -> bool            # R1
def importe_linea(precio, factor, rendimiento) -> Decimal      # R2 (D2)
def importe_porcentual(factor, rendimiento, base) -> Decimal   # R2 (D4)
def calcular_importes(triples, precios, es_pct) -> list[Decimal]
        # recorre en orden y devuelve un importe por tripleta; es la ÚNICA
        # implementación del cálculo, usada por la pasada y por los tests
def planificar(src: Path, encoding="latin-1") -> Plan
        # PASADA 1: lee el fichero entero y decide qué se convierte
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
  pierde factor y rendimiento). Segunda aparición de un mismo `~D`: se procesa
  cada línea `~D` por separado, con su propio plan.
- `codigos_ocupados: set[str]` = todos los códigos `~C` **más** su truncado
  `codigo[:20]`. Incluir el truncado evita que el clon choque después con un
  código largo que `convert_to_material` recortará (R5, ver §Riesgos D6).

Con eso decide, por cada línea `~D`:

1. Si no tiene porcentuales → sin plan (la línea sale igual).
2. Si alguna línea previa a una porcentual no tiene precio numérico → motivo
   `base_indeterminada`, sin plan (R16).
3. Si **todas** sus líneas son porcentuales y el padre tiene precio ≠ 0 →
   plan con `precio_base_padre` (R9), motivo `precio_del_padre_aplicado`.
4. Resto → plan normal.

El plan de una línea `~D` es una lista de
`(indice_tripleta, codigo_clon, precio_clon, resumen, fecha)`.

### Cálculo (R2, R6, R9, D3)

Todo en `decimal.Decimal` construido desde el **texto** del BC3 (nunca desde
`float`), con `quantize(Decimal("0.01"), ROUND_HALF_UP)` al fijar el precio
del clon. La base acumulada suma el importe **ya redondeado** del clon, así
que la base usada es la que se puede releer en la salida: el fichero queda
internamente coherente y el ERP recalcula lo mismo que calculamos nosotros.

Ejemplo real (Siroco `43.15`, verificado al escribir esta spec):
`OPTIMIZADOR MPP 41,18 × 1,2 = 49,416` → `%SUB25` `12,35` → `%SUB20` sobre
`61,766` → `12,35` → `%SUB10` sobre `74,116` → `7,41`. Suma 81,526 frente a
81,5364 antes: desvío 0,0104, dentro de la tolerancia de R19 (0,025).

### Códigos de clon (R5)

`f"{padre}.P{n}"`; si pasa de 20, se recorta el padre:
`padre[:20 - len(sufijo)] + sufijo`. Medido en `input/`: el código de padre más
largo con porcentuales tiene 19 caracteres, así que el recorte es real y hay
que probarlo. La unicidad se resuelve **reutilizando**
`bc3_modifier._shorten_code_unique`, que ya implementa la escalera
naive → 19+último → sufijo `#i`. Se le añade `forzar_unicidad=True` porque hoy
devuelve tal cual cualquier código de 20 o menos sin mirar `used`, que es justo
lo que aquí no vale; con el `False` por defecto los llamantes actuales no
cambian. Alternativa descartada: duplicar la escalera en el módulo nuevo (dos
copias divergen y el reviewer no puede comparar).

El clon **no** empieza por `%` ni lleva unidad `%`, así que una segunda pasada
sobre la salida no vuelve a convertirlo (idempotencia, verificable con un test).

### Pasada 2 — reescritura

Una sola lectura línea a línea, escribiendo según el plan:

- `~C` de un porcentual a eliminar (R13) → no se escribe; su `~T` tampoco.
- `~D` con plan → se reconstruyen las tripletas: las normales con su texto
  original, las porcentuales como `clon\1\1`; la línea se cierra con
  `_format_d_triplets` de `bc3_modifier` (ya garantiza el `\|` de R17).
  Justo **antes** de la línea `~D` se escriben los `~C` de sus clones.
- `~M` cuyo par sea `padre\porcentual` convertido → se sustituye el hijo por
  el clon (R15).
- Cualquier otra línea → se copia tal cual, sin `clean_text` (R18).

## Fixtures (`tests/fixtures/`)

Ficheros mínimos (una decena de líneas), en `latin-1`, con números reales
tomados de `input/`:

- `f002_cadena.bc3` — `43.15` con sus tres porcentuales encadenados (R6, dos o
  más porcentuales, base que incluye al anterior).
- `f002_negativo.bc3` — El Escorial `05.06.29`: `73 × 1` y `%VID -0,02` →
  `-1,46`, total `71,54` (R4, descuento).
- `f002_ceros.bc3` — `07.02.05`: dos líneas a rendimiento 0, `%MAVEN -0,05`
  con base 0 y una línea normal posterior (R10, R11, R7).
- `f002_solo_pct.bc3` — `31.04.03.01` (padre 1.100, único hijo `%SUB2.5`) y
  `32.03.04.32` (R9).
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
- Test de orden: permutar dos tripletas de un `~D` cambia el resultado (lo
  exige el criterio `acceptance` de `harness/features.json`).
- Test de que la salida sigue siendo válida: todo código de `~D` existe como
  `~C`, y todo `~D` acaba en `\|`.
- Test de idempotencia: aplicar la pasada a su propia salida no cambia nada.

## Verificación MANUAL (humano)

1. `python -m interface_adapters.cli.porcentuales_cli "input/COSTE_250128_Siroco_Rv4mlo.bc3" "output/siroco_sin_pct.bc3"`
2. Importar `output/siroco_sin_pct.bc3` en Presto y comprobar que el total del
   presupuesto y el precio de `43.15`, `05.06.29`, `31.04.03.01` y
   `32.03.04.32` son los que Presto mostraba antes.
3. Contrastar con la **captura de ejemplos de Presto** que el humano va a
   localizar: lo que hay que confirmar con ella es (a) que el unitario que
   Presto muestra en una línea porcentual es `rendimiento × base previa`
   redondeado a 2 decimales y no a más, y (b) qué unitario enseña Presto en un
   descompuesto solo-porcentual (decisión D3 de abajo).

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
- **D3 · Descompuesto solo-porcentual** (`31.04.03.01` → 1.100 €,
  `32.03.04.32` → 1.117,65 €): el clon toma el precio del `~C` del padre
  (R9). La lectura literal del invariante diría precio 0 —el importe calculado
  hoy es 0—, pero el precio real de esas partidas está puesto a mano en el
  `~C` y un ERP que recalcule desde el descompuesto se llevaría 2.217,65 € a
  cero. Es el caso con más riesgo de la feature y por eso sale listado en el
  informe. **Necesita el visto bueno del humano.**
- **D4 · Los porcentuales a rendimiento 0 sí se convierten** (R12), con precio
  0. La regla «los rendimientos 0 no se tocan» protege líneas normales que son
  alternativas desactivadas; aquí el importe sigue siendo 0 y, si no se
  convirtiera, quedaría un concepto `%` vivo en el `~D`.
- **D5 · Pasada propia y no un paso dentro de `convert_to_material`**:
  requisito del humano. Ventaja comprobada al diseñar: el invariante se mide
  sobre entrada-contra-salida de la propia pasada, sin truncados, `CD#` ni
  clones `.1` de por medio.
- **D6 · Colisión con el truncado posterior.** `convert_to_material` recorta
  códigos > 20 y registra los cortos según los va leyendo, así que un código
  largo podría recortarse sobre un clon nuestro definido más abajo. Se mitiga
  reservando en `codigos_ocupados` también los `codigo[:20]` de todos los `~C`.
  Riesgo residual documentado, no arreglamos ese comportamiento aquí.
- **D7 · `TransformBC3Step` llama a `convert_to_material` dentro de un
  `try/except TypeError`** cuyo `try` hoy **siempre** falla (esos keywords no
  existen) y cuya rama viva es el `except`. No se arregla en F-002, pero el
  implementer debe saber que la rama que se ejecuta es la segunda y que un
  `TypeError` lanzado dentro de la función se tragaría en silencio.
- **Límite de microservicio**: ninguno. Todo lo que necesita la pasada está en
  el propio `.bc3`; no consulta catálogo, ni ERP, ni servicios hermanos.
