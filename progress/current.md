<!-- progress/current.md -->
# Trabajo en curso

**F-002 · Convertir los descompuestos porcentuales a UD con cantidad 1
conservando el importe de Presto** · rama `feature/F-002-porcentuales-a-ud` ·
rigor `critico` · estado `in_progress`.

**Ronda 2 en curso.** El reviewer aprobó la pasada 2, pero al verificar la
salida real sobre `input/lagunamodificado16julio.bc3` apareció un defecto **en
la R9 tal como estaba escrita**: con DOS líneas porcentuales inflaba la
partida (`ICV260` daba 336,39 con un `~C` de 291,50, el beneficio cobrado dos
veces). El humano decidió la corrección, el spec-author actualizó la spec
(R9, R9 bis, R9 ter, R19 bis) y el implementer ha hecho T17-T23 y T25.

**La regla nueva**: cuando todas las líneas de un `~D` son porcentuales y el
padre tiene precio `P` ≠ 0, se despeja la base hacia atrás
(`base = P / Π(1 + r_i)`) y el `~D` pasa a tener una línea `<padre>.P0` con esa
base más una línea por porcentual; el residuo de redondeo se absorbe en la
línea de base. Los cuatro casos reales vuelven a sumar exactamente el precio
de su `~C`: `ICV260` 291,50 · `ICV270` 369,50 · `31.04.03.01` 1.100,00 ·
`32.03.04.32` 1.117,65.

Implementación y evidencias: `progress/impl_F-002.md`.

**NO se marca `done`**: faltan T15 y T24, las dos verificaciones MANUALES del
humano.

## T24 · pendiente del humano (regla nueva)

```
python -m interface_adapters.cli.porcentuales_cli "input/lagunamodificado16julio.bc3" "output/laguna_sin_pct.bc3"
```

Importar en Presto y confirmar `ICV260` = 291,50 (no 336,39) e `ICV270` =
369,50. **Resultado: PENDIENTE.**

## T15 · pendiente del humano

El fichero ya está generado y listo para importar:

```
python -m interface_adapters.cli.porcentuales_cli "input/COSTE_250128_Siroco_Rv4mlo.bc3" "output/siroco_sin_pct.bc3"
```

Resultado de esa ejecución: 262 `~D` con porcentual, 319 líneas convertidas,
21 conceptos eliminados, 2 casos excepcionales (los de R9). Informe en
`output/informe_porcentuales.csv`.

Queda importar `output/siroco_sin_pct.bc3` en Presto y confirmar el total y
el precio de `43.15`, `05.06.29`, `31.04.03.01` y `32.03.04.32`.
**Resultado: PENDIENTE.**

## Comprobación independiente del líder (2026-09-17, regla R9 ANTERIOR)

Hecha sobre la salida real, al margen de los tests del implementer:

| Medida | Entrada | Salida | Desvío |
|---|---|---|---|
| Todos los `~D` salvo los 2 de R9 | 7.814.258,82 | 7.814.258,85 | **+0,03 €** |
| Los 2 `~D` de R9 | 0,00 | 2.217,65 | +2.217,65 € |

El desvío de 3 céntimos sobre 7,8 M€ es el redondeo acumulado de 319 líneas
convertidas; el peor `~D` individual es `43.15`, con −0,0104 €. Los 2.217,65 €
son exactamente `1.100,00 + 1.117,65`. Con la R9 nueva esos dos `~D` siguen
sumando eso mismo —el precio de su `~C`, ahora repartido entre la línea de
base y la porcentual—, y los dos de laguna dejan de inflarse. En la salida
quedan **0** conceptos porcentuales y **0** líneas `~D` mal cerradas.

## Decisiones abiertas del humano

1. **T15 y T24**, arriba.
2. **R13 sobre `input/presupuesto.bc3`**: borra 42 `~C` porcentuales sin
   convertir ninguna línea, por ser un banco de precios cuyos `%` no usa
   ningún `~D`. Es la regla tal como se aprobó; conservarlos cuando no ha
   habido ninguna conversión es una línea en `_decidir_conceptos_a_eliminar`.
3. **Porte a `arnes-base`** de la mejora de `CHECKPOINTS.md` que propone el
   reviewer en las dos pasadas: extender el punto C4 de los dobles a los
   sustitutos instalados con `monkeypatch.setattr`, comparando
   `inspect.signature` con la del símbolo sustituido. Vale para cualquier
   proyecto, así que por la regla de propagación del `CLAUDE.md` del
   ecosistema va a `arnes-base` en este mismo trabajo. Sin hacer.
4. **La semántica de dominio de `docs/ARCHITECTURE.md`** sigue sin validación
   humana formal, aunque F-002 la ha confirmado con datos.
