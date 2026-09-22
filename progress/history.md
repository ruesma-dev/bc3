<!-- progress/history.md -->
# Histórico del arnés

Registro append-only. El líder mueve aquí el resumen de cada feature terminada.

---

## F-002 · Porcentuales a UD con cantidad 1 · **done** (2026-09-22)

Rama `feature/F-002-porcentuales-a-ud`. Rigor `critico`. Cinco rondas,
APPROVED del reviewer en la pasada 5.

**Qué hace.** Una pasada previa e independiente (`.bc3` → `.bc3`) que solo
toca las líneas de porcentaje: las convierte en un concepto propio del padre
con unidad `UD`, cantidad 1 y el precio ya calculado en euros, reproduciendo
el cálculo de Presto. Incluye la reconstrucción de la base implícita cuando
el descompuesto es solo porcentual, el redondeo a 2 decimales y la limpieza
de texto. Step propio del pipeline, CLI suelto y banderas en `Settings`
(`PORCENTUALES_A_UD`, `PORCENTUALES_DECIMALES`, `PORCENTUALES_LIMPIAR_TEXTO`).

**Cierre.** El humano acepta el resultado sobre `output/laguna_sin_pct.bc3`:
«el ultimo bc3 de laguna esta perfecto, ese es el objetivo». Cubre T24
(Presto), T32 (decimales) y T44 (Sigrid). **T15 (Siroco) quedó sin ejecutar**
y así está marcada.

**Evidencias finales.** 538 tests, cobertura 98,9 % de las líneas cambiadas,
campaña de mutación 198/198 sin supervivientes, `bash harness/init.sh` en
verde. Sobre el fichero de Elena Díaz: 948 líneas porcentuales → 0, 173
conceptos con texto sucio → 0, y 354/354 descompuestos cuadran con el precio
declarado en su `~C`.

**Lo que costó, y por qué importa.** Los tres defectos reales los encontró
alguien mirando el resultado importado, no la suite:

1. R9 inflaba un 15,4 % las partidas con dos porcentuales (`ICV260` 336,39 en
   vez de 291,50). Error de regla en la spec, no de código.
2. Subir a 4 decimales, que fue un diagnóstico erróneo del líder: 2 decimales
   reproduce el `~C` declarado por Presto en el 99,4 % frente al 76,8 %.
3. Sigrid no importa el descompuesto de los conceptos con caracteres no ASCII
   en el resumen (lo detectó Elena Díaz). Al arreglarlo apareció que
   `clean_text` prometía ASCII y no lo cumplía: `Ø`, `ø`, `æ`, `ß` y `µ`
   pasaban el filtro. Arreglado en su sitio, así que **cambia también la
   salida de `convert_to_material`** (medido: 41 líneas, 38 caracteres
   no-ASCII → 0, sin romper referencias).

**Deuda que deja:** `convert_to_material` no tiene ni un test y esta feature
le cambió la salida; la evidencia de que no rompió nada es una medición de
review, no la suite. Propuesta del reviewer: feature aparte con un test de
caracterización.

**Para `arnes-base`** (regla de propagación, sin portar todavía): extender el
checkpoint de los dobles a `monkeypatch.setattr` comparando firmas; una regla
que el invariante excluye por diseño necesita su propia comprobación; y cuando
una transformación toca dinero, al menos un test debe comparar contra un
número de fuera del sistema.
