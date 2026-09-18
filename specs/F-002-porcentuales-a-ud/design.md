<!-- specs/F-002-porcentuales-a-ud/design.md -->
# F-002 · Diseño técnico · Porcentuales a UD

Encaje en `docs/ARCHITECTURE.md`: **pasada previa independiente** `.bc3` →
`.bc3`, adaptador de infraestructura, con su step de pipeline delante de
`TransformBC3Step`. No entra en `convert_to_material` (del humano, §Riesgos D5).

## Ficheros a crear

- `infrastructure/bc3/bc3_porcentajes.py` — la pasada completa.
- `interface_adapters/cli/__init__.py` y `.../cli/porcentuales_cli.py` (R22):
  `python -m interface_adapters.cli.porcentuales_cli <entrada.bc3> <salida.bc3>`.
- `tests/test_f002_porcentuales.py` (unitarios R1-R18, R20-R26),
  `tests/test_f002_invariante.py` (R19 sobre fixtures e `input/`, `skip` si está
  vacía), `tests/fixtures/f002_*.bc3` (§Fixtures).

## Ficheros a modificar

- `config/settings.py` — campos nuevos `porcentuales_a_ud` (`PORCENTUALES_A_UD`,
  true), `porcentuales_decimales` (`PORCENTUALES_DECIMALES`, 2, saneado a 2..6
  por R6 bis) y `porcentuales_limpiar_texto` (`PORCENTUALES_LIMPIAR_TEXTO`,
  true, R24). Dataclass congelada: en tests, `replace`.
- `application/pipeline/pipeline.py` — campo `preprocessed_path` en `ETLContext`.
- `application/pipeline/steps.py` — nuevo `ConvertirPorcentualesStep` y, en
  `TransformBC3Step`, `src = ctx.preprocessed_path or ctx.original_path`.
- `interface_adapters/controllers/etl_controller.py` — composición:
  `ResolveInputStep → ConvertirPorcentualesStep → TransformBC3Step → ...`.
- `infrastructure/bc3/bc3_modifier.py` — **un único cambio**: keyword
  `forzar_unicidad: bool = False` en `_shorten_code_unique` (§Códigos de clon),
  que con su valor por defecto no cambia el comportamiento actual.

## Ficheros que NO se tocan

`convert_to_material` y el resto de `bc3_modifier.py`; `build_tree_service.py`
(los clones `.1` siguen igual); `export_csv_service`, `parse_bc3_service`, todo
lo de FASE 2 y de `infrastructure/ai|clients`; `input/` (solo lectura); y
`utils/text_sanitize.py`, que se **usa** (`clean_text`) pero no se modifica.

## Módulo `infrastructure/bc3/bc3_porcentajes.py`

Capa: **infrastructure**. Sin dependencias nuevas: `pathlib`, `decimal`,
`dataclasses`, `logging`, el helper de `bc3_modifier` y `utils.text_sanitize`.
Sin `print()` (CONVENTIONS): `logging.getLogger(__name__)`.

```python
@dataclass(frozen=True)
class CasoPorcentual:   # fila del informe (R20)
    padre: str; codigo: str; motivo: str; rendimiento: float; importe: float

@dataclass
class InformePorcentuales:  # contadores, decimales y list[CasoPorcentual]

def es_porcentual(codigo: str, unidad: str) -> bool            # R1
def calcular_importes(triples, precios, es_pct, decimales) -> list[Decimal]
        # D2/D4 en orden; ÚNICA implementación del cálculo, para pasada y tests
def planificar(src: Path, *, decimales: int) -> Plan           # PASADA 1
def convertir_porcentuales(src, dst, *, encoding="latin-1", activo=True,
        decimales: int = 2, limpiar_texto: bool = True) -> InformePorcentuales
        # PASADA 2: reescribe; si activo=False copia sin tocar (R21)
```

### Pasada 1 — `planificar`

Recorre el fichero una vez y construye `precios` y `unidades` por código (de los
`~C`); `triples_por_padre: dict[codigo, list[tuple[hijo, factor, rend]]]` de los
`~D` **conservando el orden** (`children_map` de `bc3_modifier` no sirve: pierde
factor y rendimiento) y planificando cada línea `~D` por separado; y
`codigos_ocupados` = todos los códigos `~C` **más** su truncado `codigo[:20]`,
para que el clon no choque con un código largo que `convert_to_material`
recortará luego (R5, §Riesgos D6).

Con eso decide, por cada línea `~D`: sin porcentuales → sin plan; línea previa a
una porcentual sin precio numérico → `base_indeterminada`, sin plan (R16);
**todas** porcentuales y padre con precio `P` ≠ 0 → base reconstruida (R9),
motivo `base_reconstruida`, salvo que algún `(1 + r_i)` ≤ 0, que deja el `~D`
sin plan como `base_no_despejable` (R9 ter); resto → plan normal. El plan es una
lista de `(indice_tripleta, codigo_clon, precio_clon, resumen, fecha)`, más una
línea de base opcional en cabeza.

### Cálculo y decimales (R2, R6, R6 bis, R6 ter, R9, D3)

Todo en `decimal.Decimal` desde el **texto** del BC3 (nunca desde `float`), con
`quantize(Decimal(1).scaleb(-d), ROUND_HALF_UP)` al fijar el precio del clon,
siendo `d = settings.porcentuales_decimales`. La base acumulada suma el importe
**ya redondeado**, así que es la que se puede releer en la salida; el número se
escribe con `f"{valor:f}"` + `normalize()`, sin exponentes ni relleno (R7). `d`
es configurable —**2 por defecto**— y el criterio no es minimizar el error
de redondeo sino **reproducir lo que calcula Presto**. Se contrasta contra el
precio que el `~C` de cada partida declara, fuera las de precio a mano (R9 y
`07.02.01a`); partidas acertadas en Siroco / laguna — **`d = 2`: 260/262 (99,2 %)
y 402/402 (100 %)**; `d = 3`: 90,5 % y 79,9 %; `d = 4`: 94,3 % y 80,1 %;
`d = 6`: 94,3 % y 79,6 %. Presto redondea a céntimos **cada línea**, así que más
precisión se aleja de su resultado. Con `d = 2` (`43.15`): `41,18 × 1,2 =
49,416` → `12,35` → `12,35` (sobre 61,766) → `7,41`; suma 81,526. El `~C` de
`C020615` declara 172,60, lo que da `d = 2`: el 172,59 de `d = 4` es el cálculo
exacto, no el de Presto.

### Base reconstruida del `~D` solo-porcentual (R9, R9 bis, R9 ter)

`base = P / Π(1 + r_i)` con `Decimal`, redondeada con los mismos `d` decimales
de R6; luego los importes porcentuales sobre esa base; y por último
`precio(.P0) = P − Σ(importes porcentuales)`, que absorbe el residuo en la línea
de base y deja la suma clavada en `P` (R19 bis) sea cual sea `d`. Los cuatro
casos reales que van tal cual a los tests (base · clones, con `d = 2` por
defecto y `d = 4` entre paréntesis): `ICV260` (P 291,50 · `0,1178` y
`0,154`) → 225,98 · 26,62 · 38,90 (225,9792 ·
26,6204 · 38,9004); `ICV270` (P 369,50 · ídem) → 286,45 · 33,74 · 49,31
(286,4471 · 33,7435 · 49,3094); `31.04.03.01` (P 1.100 · `0,025`) → 1.073,17 ·
26,83 (1.073,1707 · 26,8293); `32.03.04.32` (P 1.117,65 · `0,17`) → 955,26 ·
162,39 (955,2564 · 162,3936). La línea de base lleva unidad `UD`, factor 1,
rendimiento 1, tipo `3`, y la fecha y el resumen del `~C` del padre
(precedente: el clon `.1`).

### Códigos de clon (R5)

`f"{padre}.P{n}"`, y `f"{padre}.P0"` para la línea de base de R9: misma familia
en Presto y ordena delante de sus porcentuales. Si pasa de 20 se recorta el
padre, `padre[:20 - len(sufijo)] + sufijo`; medido en `input/`, el padre más
largo con porcentuales tiene 19 caracteres, así que el recorte es real y hay que
probarlo. La unicidad **reutiliza** `bc3_modifier._shorten_code_unique` y su
escalera naive → 19+último → sufijo `#i`, con `forzar_unicidad=True` porque hoy
devuelve tal cual cualquier código de 20 o menos sin mirar `used` (con `False`,
los llamantes actuales no cambian); duplicar la escalera se descartó, dos copias
divergen. El clon no empieza por `%` ni lleva unidad `%`: una segunda pasada no
vuelve a convertirlo (idempotencia, con test).

### Pasada 2 — reescritura

Una sola lectura línea a línea, escribiendo según el plan: el `~C` de un
porcentual a eliminar (R13) no se escribe, ni su `~T`; un `~D` con plan se
reconstruye con las tripletas normales en su texto original, las porcentuales
como `clon\1\1` y, en el caso R9, la línea de base `.P0\1\1` en cabeza, y se
cierra con `_format_d_triplets` de `bc3_modifier` (garantiza el `\|` de R17),
con los `~C` de clones y base justo **antes**; un `~C`/`~T` no porcentual se
copia, con el texto limpiado si R24 está activa; un `~M` cuyo par sea
`padre\porcentual` convertido pasa a apuntar al clon (R15); y cualquier otra
línea se copia tal cual (R18).

### Limpieza de texto (R24-R26)

Se **reutiliza** `utils.text_sanitize.clean_text`, el mismo de
`convert_to_material`: una sola limpieza en el repositorio, porque dos divergen
y el resultado dependería de por dónde hubiera pasado el fichero. Entra en el
resumen del `~C`, el texto del `~T` y el resumen de clones y líneas de base
(heredan el del padre y arrastran su `Ñ`). **No** toca unidad, códigos ni campos
numéricos: limpiar la unidad sería unificarla, y eso es de
`convert_to_material`. Motivo: Sigrid no importó el descompuesto de los cuatro
`VALV` —las únicas del capítulo con no-ASCII en el resumen («Válvula», `½"`,
`1¼"`)— mientras las ASCII sí entraron, y en ese mismo presupuesto 9 conceptos
ya limpios (`114"`, `112"`) funcionan y 5 sucios no. La entrada trae 523
conceptos con no-ASCII, 173 con descompuesto propio
(`output/conceptos_con_acentos.csv`); no lo causa la pasada, lo causa que ella
no limpiara (R18) mientras el ETL completo sí.

**Limitación conocida:** `clean_text` deja `½"` en `12"` y `1¼"` en `114"`, que
parecen 12 o 114 pulgadas. Ambiguo, pero es lo que ya hace el ETL y lo que
tienen los conceptos que hoy sí importan: se mantiene por coherencia. Mejorarlo
(`½` → `1/2`) tocaría también `convert_to_material` y las salidas ya cargadas:
es otra feature, no un apaño de esta.

## Fixtures (`tests/fixtures/`)

En `latin-1`, con números reales de `input/`; una por caso, con su requisito:

- `f002_cadena.bc3` — `43.15`, tres porcentuales encadenados (R6);
  `f002_negativo.bc3` — `05.06.29`: `73 × 1` y `%VID -0,02` → `-1,46` (R4);
  `f002_ceros.bc3` — `07.02.05`: dos rendimientos 0, `%MAVEN` con base 0 y una
  línea normal posterior (R7, R10, R11).
- `f002_solo_pct.bc3` — `31.04.03.01` y `32.03.04.32`: R9 con una porcentual;
  `f002_solo_pct_doble.bc3` — `ICV260` e `ICV270` de
  `input/lagunamodificado16julio.bc3`, dos porcentuales y ninguna línea normal,
  el caso que destapó el defecto (R9, R9 bis, R19 bis).
- `f002_descuento_total.bc3` — solo-porcentual con `r = -1` (R9 ter);
  `f002_unidad_pct.bc3` — porcentual por unidad `%` y `~M` sobre el par (R1,
  R15); `f002_sin_precio.bc3` — línea previa sin `~C` (R16);
  `f002_codigo_largo.bc3` — padre de 19-20 caracteres y un código ocupado (R5).
- `f002_acentos.bc3` — los cuatro `VALV` con su resumen real («Válvula de bola,
  ½"», `1¼"`…), un `~T` con acentos y un clon que hereda una `Ñ` (R24, R25).

## Tests: qué se prueba y cómo

- Nombres trazables `test_f002_rN_...` (CONVENTIONS). Sin red, sin BBDD, sin
  Gemini/OpenAI/`ocr_service`: la pasada no los usa.
- **El invariante no puede ser tautológico**: medir entrada y salida con la
  misma `calcular_importes` arrastra el mismo error en los dos lados. R19 se
  prueba en dos planos: (a) entrada-vs-salida por cada `~D` de fixtures e
  `input/`; (b) **números literales** de Presto (81,5364 · 71,54 · −1,46 · 16).
- **R19 y R19 bis se comprueban con `d = 2` y con `d = 4`** (`Settings` clonado
  con `replace`) y **con la limpieza de R24 encendida y apagada** (R26): la
  tolerancia depende de `d`, y limpiar texto no puede mover un importe.
- **R19 bis es obligatorio y sobre datos reales**: por cada `~D` solo-porcentual
  de `input/` (los cuatro de §Base reconstruida), el importe de la SALIDA da el
  precio del `~C` del padre ± 0,01. Es lo único que caza el +15,4 %.
- **Limpieza (R24, R25)**: resumen de los cuatro `VALV` antes y después; con la
  bandera apagada, salida **byte a byte** igual a la entrada; con ella
  encendida, ningún código, precio, factor ni rendimiento cambia; y sobre cada
  `.bc3` de `input/`, ningún `~C` con descompuesto conserva no-ASCII.
- Orden: permutar dos tripletas de un `~D` cambia el resultado (lo exige el
  `acceptance`). Salida válida (códigos del `~D` existentes como `~C`, `~D`
  acabado en `\|`) e idempotencia.

## Verificación MANUAL (humano)

1. `python -m interface_adapters.cli.porcentuales_cli "input/COSTE_250128_Siroco_Rv4mlo.bc3" "output/siroco_sin_pct.bc3"`
2. Importarlo en Presto: el total y el precio de `43.15`, `05.06.29`,
   `31.04.03.01` y `32.03.04.32` deben ser los de antes. Ídem con
   `input/lagunamodificado16julio.bc3`: `ICV260` 291,50, `ICV270` 369,50.
3. **Importar en Sigrid** y confirmar que `VALV1`, `VALV4`, `VALV5` y `VALV6`
   entran ya **con su descompuesto** (R25), y que el resumen se lee aceptable
   pese a la limitación `½"` → `12"`.
4. Contrastar con la **captura de Presto** que el humano localice: el unitario
   de una porcentual es `rendimiento × base previa` y, en un solo-porcentual,
   el precio del padre es el final (base de D3).

## Riesgos y decisiones

- **D1 · Decimales del precio del clon: 2 por defecto y configurables**
  (§Cálculo y decimales). El objetivo es reproducir a Presto, que redondea a
  céntimos cada línea, no minimizar el error aritmético: por eso 2 acierta más
  que 4 o 6. El residuo no se corrige en líneas no porcentuales: falsearía una
  línea ajena, e inestable si es un rendimiento 0.
- **D2 · El `~C` porcentual se elimina** (R13) en vez de quedar huérfano: el
  `acceptance` pide que no quede unidad `%`; un `%` huérfano lo mandaría la
  FASE 2 a clasificar contra el catálogo (coste y ruido); y nada lo referencia,
  así que borrarlo no mueve importes. Se conserva si algo lo apunta (R14).
- **D3 · Descompuesto solo-porcentual: se reconstruye la base** (R9). El
  invariante literal diría precio 0, pero el precio de esas partidas está puesto
  a mano en el `~C` y un ERP que recalculase se llevaría a cero 1.100 €,
  1.117,65 €, 291,50 € y 369,50 €. **Corrección del 2026-09-17**: la primera
  versión daba al primer clon el precio del padre y aplicaba encima los demás
  porcentajes, cobrando el beneficio dos veces (`ICV260` a 336,39, +15,4 %). Con
  `P` como precio final se despeja la base hacia atrás y el `~D` cuadra en `P`.
- **D4 · Los porcentuales a rendimiento 0 sí se convierten** (R12), con precio
  0. La regla «los rendimientos 0 no se tocan» protege alternativas normales
  desactivadas; aquí el importe sigue en 0 y no convertirlas dejaría un `%` vivo.
- **D5 · Pasada propia, no un paso de `convert_to_material`** (del humano): el
  invariante se mide entrada-contra-salida, sin truncados, `CD#` ni clones `.1`.
- **D8 · La limpieza de texto entra en esta pasada, con bandera** (decisión del
  humano del 2026-09-18; descartada una pasada aparte). Motivo, alcance y
  limitación conocida, en §Limpieza de texto.
- **D6 · Colisión con el truncado posterior.** `convert_to_material` recorta
  códigos > 20 y registra los cortos según los va leyendo, así que un código
  largo podría recortarse sobre un clon definido más abajo. Se mitiga reservando
  en `codigos_ocupados` los `codigo[:20]` de todos los `~C`; riesgo residual
  documentado, ese comportamiento no se arregla aquí.
- **D7 · `TransformBC3Step` llama a `convert_to_material` dentro de un
  `try/except TypeError`** cuyo `try` hoy **siempre** falla: la rama viva es el
  `except`, y un `TypeError` de dentro se tragaría en silencio. No se arregla.
- **Límite de microservicio**: ninguno. Lo que necesita la pasada está en el
  propio `.bc3`; no consulta catálogo, ni ERP, ni servicios hermanos.
