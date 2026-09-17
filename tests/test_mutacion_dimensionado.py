# tests/test_mutacion_dimensionado.py
"""F-040 · R1–R10: la campaña se dimensiona sola en vez de adivinar.

Dos números que el arnés se inventaba, y lo que se mide de verdad en su lugar:

- **El timeout por mutante** era un fijo de `rigor.json`. Ahora sale de la
  LÍNEA BASE que la campaña ya corre dentro de cada worktree, con los W workers
  compitiendo: la medición correcta ya estaba hecha y se tiraba. El valor
  configurado pasa a ser un SUELO.
- **Los workers por defecto** eran `núcleos - 2` con tope 16, que supone que el
  cuello de botella es la CPU. No lo es: cada worker arranca una suite completa
  —intérprete, importaciones, E/S—, así que el recurso escaso es la máquina.

Los dos números salen de la campaña real del 2026-08-21: suite en reposo ~51 s,
97,5 s con 1 worker, 119–122 s con 3, contra un timeout configurado de 120 s.
Con el default de 16 workers no cabía ni una línea base.

Ningún test de este fichero ejecuta una suite: todos usan ejecutores dobles.
"""

from __future__ import annotations

import json
import math
import time
from pathlib import Path

import pytest

from harness.alcance import Alcance
from harness.mutacion import (
    FACTOR_HOLGURA_BASE,
    FILAS_DE_RELOJ,
    MARGEN_TIMEOUT,
    MUERTO,
    TOPE_WORKERS,
    BaseRota,
    InformeMutacion,
    ResultadoSuite,
    ejecutar_campania,
    escribir_informe,
    lineas_comparables,
    resolver_workers,
    timeout_de_linea_base,
    timeout_derivado,
    workers_por_defecto,
)
from harness.mutacion_paralela import fusionar
from harness.rigor import RUTA_RIGOR, cargar_rigor, workers_mutacion

# --- R8: el default deja de suponer que el cuello es la CPU -----------------


def test_el_tope_de_workers_calculados_es_cuatro() -> None:
    """El único punto medido y VERDE son 3 workers, y ya ahí la suite sube 25 %.

    4 es un paso sobre lo medido, no un salto. Por encima nadie ha medido, y el
    arnés viaja a máquinas más pequeñas que ésta, donde un tope alto no es
    optimista sino dañino.
    """
    assert TOPE_WORKERS == 4


@pytest.mark.parametrize(
    ("nucleos", "esperados"),
    [
        (1, 1),
        (2, 1),
        (4, 1),
        (6, 2),
        (8, 3),
        (10, 4),
        (22, 4),
        (128, 4),
    ],
)
def test_workers_por_defecto_reservan_dos_nucleos_por_worker(
    nucleos: int, esperados: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`min(max(1, (núcleos - 2) // 2), TOPE_WORKERS)`.

    Los dos primeros núcleos son los de siempre —la máquina y el coordinador—;
    el `// 2` reserva del orden de dos por suite, porque pytest no es monohilo:
    importa, compila y escribe caché mientras corre.
    """
    monkeypatch.setattr("harness.mutacion.os.cpu_count", lambda: nucleos)

    assert workers_por_defecto() == esperados


def test_workers_por_defecto_nunca_bajan_de_uno(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cero workers no es una campaña más barata: es ninguna campaña."""
    monkeypatch.setattr("harness.mutacion.os.cpu_count", lambda: None)

    assert workers_por_defecto() == 1


def test_en_la_maquina_de_la_medicion_el_default_baja_de_16_a_4(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """22 núcleos: el default que hacía la campaña inutilizable, y el nuevo."""
    monkeypatch.setattr("harness.mutacion.os.cpu_count", lambda: 22)

    assert workers_por_defecto() == 4
    assert min(max(1, 22 - 2), 16) == 16, "el default viejo, para que se vea el salto"


# --- R9: el límite de una máquina no se cablea en un arnés que viaja --------


def test_rigor_json_no_declara_la_clave_workers() -> None:
    bloque = json.loads(RUTA_RIGOR.read_text(encoding="utf-8"))["mutacion"]

    assert "workers" not in bloque, (
        "declarar 'mutacion.workers' cablearía el límite de ESTA máquina en un "
        "arnés que se instala en cinco proyectos (decisión del 2026-08-20)"
    )


def test_sin_la_clave_workers_manda_el_default_por_nucleos() -> None:
    assert workers_mutacion(cargar_rigor(RUTA_RIGOR)) is None
    assert resolver_workers(None, None) == workers_por_defecto()


def test_la_precedencia_no_cambia() -> None:
    """`--workers` > `mutacion.workers` > default, intacta."""
    assert resolver_workers(3, 8) == 3
    assert resolver_workers(None, 8) == 8


# --- R10: el tope calculado no limita lo que se pida a mano ----------------


@pytest.mark.parametrize("pedidos", [5, 12, 32])
def test_workers_pedidos_a_mano_no_se_recortan(pedidos: int) -> None:
    """Subir el tope es una decisión CON DATOS, y aquí es donde se producen."""
    assert resolver_workers(pedidos, None) == pedidos
    assert resolver_workers(pedidos, 2) == pedidos


def test_tampoco_se_recorta_lo_declarado_en_rigor_json() -> None:
    """Un proyecto que sí quiera declarar `workers` puede pasarse del tope."""
    assert resolver_workers(None, 12) == 12


# --- R1: el timeout por mutante sale de la línea base ya medida -------------

#: Los tres tiempos de línea base de la campaña real del 2026-08-21, uno por
#: worktree, con tres workers compitiendo por la misma máquina.
BASE_MEDIDA = {"wk_0": 119.3, "wk_1": 121.6, "wk_2": 119.5}

#: `mutacion.timeout_por_mutante_s` de este repositorio. No se toca: pasa a ser
#: un suelo, y un mutante nunca recibe menos que hoy.
SUELO = 120


def test_el_margen_es_dos() -> None:
    """Dos, no diez: un margen generoso deja pasar mutantes que cuelgan."""
    assert MARGEN_TIMEOUT == 2.0


def test_con_la_medicion_real_el_timeout_sale_244_s() -> None:
    """`max(120, ceil(121,6 × 2)) = 244`. Manda el peor worker, no la media."""
    assert timeout_derivado(SUELO, BASE_MEDIDA) == 244


def test_manda_el_PEOR_de_los_tiempos_medidos() -> None:
    """Un timeout que solo le vale al worker más rápido no le vale a nadie."""
    assert timeout_derivado(SUELO, {"lento": 200.0, "rapido": 1.0}) == 400


def test_el_valor_configurado_es_un_SUELO_y_nunca_un_techo() -> None:
    """Una suite rápida no baja el timeout por debajo de lo configurado."""
    assert timeout_derivado(SUELO, {"wk_0": 3.0}) == SUELO
    assert timeout_derivado(SUELO, {"wk_0": 59.9}) == SUELO
    assert timeout_derivado(SUELO, {"wk_0": 60.1}) > SUELO


def test_sin_medicion_se_queda_el_suelo() -> None:
    """Ejecutores dobles, campaña sin línea base: no hay nada de lo que derivar."""
    assert timeout_derivado(SUELO, {}) == SUELO


def test_el_derivado_se_redondea_HACIA_ARRIBA() -> None:
    """Redondear a la baja regalaría el segundo que faltaba justo al peor caso."""
    assert timeout_derivado(1, {"wk_0": 60.01}) == math.ceil(60.01 * 2) == 121


def test_el_margen_se_puede_inyectar_para_probarlo() -> None:
    assert timeout_derivado(1, {"wk_0": 50.0}, margen=3.0) == 150


def test_el_derivado_siempre_es_un_entero() -> None:
    derivado = timeout_derivado(SUELO, BASE_MEDIDA)

    assert isinstance(derivado, int) and not isinstance(derivado, bool)


# --- R2: la línea base tiene su propio timeout, más holgado ----------------


def test_el_factor_de_holgura_de_la_base_es_cinco() -> None:
    assert FACTOR_HOLGURA_BASE == 5


def test_la_linea_base_recibe_el_suelo_por_el_factor() -> None:
    """Huevo y gallina: la base necesita un timeout para poder medirse.

    Se le concede el suyo, holgado y aparte, y es defendible porque se paga UNA
    VEZ POR WORKER, no una por mutante. Si ni con diez minutos cabe la suite
    limpia, el problema ya no es el reloj.
    """
    assert timeout_de_linea_base(SUELO) == 600


def test_la_base_siempre_recibe_mas_tiempo_que_un_mutante() -> None:
    for suelo in (30, 120, 300):
        assert timeout_de_linea_base(suelo) > suelo


# --- R3, R5, R6: la campaña deriva, lo dice, y se deja anular ---------------

#: Lo que tarda la línea base del doble. Por encima de medio segundo a
#: propósito: con un suelo de 1 s, `ceil(espera × 2)` sale por encima del suelo
#: y se ve que la derivación ha ocurrido de verdad.
ESPERA_BASE = 1.2

#: Suelo diminuto para que la derivación se note sin esperar dos minutos.
SUELO_DE_JUGUETE = 1

#: Fuente con dos comparaciones que mutar, para que la campaña tenga trabajo.
FUENTE = "def clasifica(a, b):\n    if a == b:\n        return a > b\n    return None\n"


class EjecutorCronometrado:
    """Doble que tarda lo que se le diga y apunta con qué timeout se le llamó."""

    def __init__(self, espera: float = 0.0, base: ResultadoSuite | None = None) -> None:
        self.raiz = "wk_doble"
        self.espera = espera
        self.base = base or ResultadoSuite(codigo=0)
        self.timeouts_de_base: list[int] = []
        self.timeouts_de_mutante: list[int] = []

    def linea_base(self, timeout_s: int) -> ResultadoSuite:
        self.timeouts_de_base.append(timeout_s)
        time.sleep(self.espera)
        return self.base

    def ejecutar(self, timeout_s: int) -> str:
        self.timeouts_de_mutante.append(timeout_s)
        return MUERTO


@pytest.fixture
def arbol(tmp_path: Path) -> tuple[Alcance, str]:
    """Un fichero con código mutable y su alcance, sin git de por medio."""
    (tmp_path / "codigo.py").write_text(FUENTE, encoding="utf-8")
    alcance = Alcance(
        feature="F-040",
        origen="rama",
        ref_diff=("dev", "feature/x"),
        lineas={"codigo.py": {2, 3}},
    )
    return (alcance, str(tmp_path))


def _campania(alcance, raiz, ejecutor, eco=None, **extra):
    return ejecutar_campania(
        alcance,
        ejecutor,
        timeout_s=extra.pop("timeout_s", SUELO_DE_JUGUETE),
        raiz=raiz,
        eco=eco,
        comprobar_arbol=False,
        **extra,
    )


def test_la_campania_juzga_con_el_timeout_DERIVADO_no_con_el_suelo(
    arbol: tuple[Alcance, str],
) -> None:
    """El cambio que hace la feature: el mutante recibe lo medido, no el fijo."""
    alcance, raiz = arbol
    doble = EjecutorCronometrado(espera=ESPERA_BASE)

    informe = _campania(alcance, raiz, doble)

    assert informe.timeout_efectivo > SUELO_DE_JUGUETE, (
        "con una base de 1,2 s y suelo 1 s, el derivado tiene que subir"
    )
    assert informe.timeout_efectivo == timeout_derivado(
        SUELO_DE_JUGUETE, informe.segundos_linea_base
    )
    assert doble.timeouts_de_mutante, "no se juzgó ningún mutante"
    assert set(doble.timeouts_de_mutante) == {informe.timeout_efectivo}


def test_la_linea_base_recibe_su_timeout_holgado_no_el_del_mutante(
    arbol: tuple[Alcance, str],
) -> None:
    alcance, raiz = arbol
    doble = EjecutorCronometrado(espera=0.0)

    _campania(alcance, raiz, doble)

    assert doble.timeouts_de_base[0] == timeout_de_linea_base(SUELO_DE_JUGUETE)


def test_la_campania_dice_por_pantalla_de_donde_sale_el_timeout(
    arbol: tuple[Alcance, str],
) -> None:
    """Un número derivado que no se explica es tan opaco como uno inventado."""
    alcance, raiz = arbol
    dicho: list[str] = []

    informe = _campania(
        alcance, raiz, EjecutorCronometrado(espera=ESPERA_BASE), eco=dicho.append
    )

    anuncio = [linea for linea in dicho if "timeout" in linea.lower()]
    assert anuncio, f"la campaña no anunció el timeout: {dicho}"
    texto = "\n".join(anuncio)
    assert str(informe.timeout_efectivo) in texto
    assert "base" in texto.lower(), "hay que decir que sale de la línea base…"
    assert "2" in texto, "…y con qué margen"


def test_con_timeout_fijado_no_se_deriva_nada(
    arbol: tuple[Alcance, str],
) -> None:
    """`--timeout N` manda: N es N, aunque la base medida pidiera más."""
    alcance, raiz = arbol
    doble = EjecutorCronometrado(espera=ESPERA_BASE)

    informe = _campania(alcance, raiz, doble, timeout_fijado=True)

    assert informe.timeout_efectivo == SUELO_DE_JUGUETE
    assert set(doble.timeouts_de_mutante) == {SUELO_DE_JUGUETE}
    assert informe.timeout_fijado is True


def test_una_base_rapida_no_baja_el_timeout_del_suelo(
    arbol: tuple[Alcance, str],
) -> None:
    """El valor configurado es un SUELO: un mutante nunca recibe menos que hoy."""
    alcance, raiz = arbol
    doble = EjecutorCronometrado(espera=0.0)

    informe = _campania(alcance, raiz, doble, timeout_s=300)

    assert informe.timeout_efectivo == 300
    assert set(doble.timeouts_de_mutante) == {300}


def test_una_base_que_expira_aborta_nombrando_workers_y_holgura(
    arbol: tuple[Alcance, str],
) -> None:
    """El aborto tiene que decir qué tocar. «Sube el timeout» no basta.

    Con W workers compitiendo, la respuesta suele ser bajar W, no subir el
    reloj; y el suelo que se sube tiene nombre y fichero.
    """
    alcance, raiz = arbol
    expirada = ResultadoSuite(codigo=-1, salida="", expirado=True)

    with pytest.raises(BaseRota) as error:
        _campania(
            alcance,
            raiz,
            EjecutorCronometrado(base=expirada),
            timeout_s=120,
            workers=3,
        )

    mensaje = str(error.value)
    assert "3 worker" in mensaje, "el mensaje tiene que nombrar los workers en juego"
    assert str(timeout_de_linea_base(120)) in mensaje, "…y el timeout concedido"
    assert "timeout_por_mutante_s" in mensaje
    assert "--workers" in mensaje


def test_el_aborto_por_base_expirada_no_deja_el_arbol_mutado(
    arbol: tuple[Alcance, str],
) -> None:
    alcance, raiz = arbol
    expirada = ResultadoSuite(codigo=-1, salida="", expirado=True)

    with pytest.raises(BaseRota):
        _campania(alcance, raiz, EjecutorCronometrado(base=expirada), workers=2)

    assert (Path(raiz) / "codigo.py").read_text(encoding="utf-8") == FUENTE


def test_el_informe_recuerda_con_que_workers_se_midio(
    arbol: tuple[Alcance, str],
) -> None:
    alcance, raiz = arbol

    informe = _campania(alcance, raiz, EjecutorCronometrado(), workers=3)

    assert informe.workers == 3
    assert informe.timeout_suelo == SUELO_DE_JUGUETE


# --- R1 y R4 en la campaña PARALELA: manda el peor worker ------------------


def _parcial(alcance: Alcance, efectivo: int, suelo: int = 120) -> InformeMutacion:
    return InformeMutacion(
        feature=alcance.feature,
        alcance=alcance,
        timeout_efectivo=efectivo,
        timeout_suelo=suelo,
    )


def test_el_informe_paralelo_declara_el_timeout_del_PEOR_worker(
    arbol: tuple[Alcance, str],
) -> None:
    """Tres worktrees, tres líneas base, tres relojes. El informe declara uno.

    Tiene que ser el más largo: es el que explica el peor caso. La media de
    tres relojes distintos no es ningún reloj.
    """
    alcance, _ = arbol

    informe = fusionar(
        alcance,
        [_parcial(alcance, 240), _parcial(alcance, 244), _parcial(alcance, 239)],
        generados=9,
        segundos=100.0,
        workers=3,
    )

    assert informe.timeout_efectivo == 244
    assert informe.timeout_suelo == 120
    assert informe.workers == 3


def test_sin_parciales_el_timeout_del_informe_es_desconocido(
    arbol: tuple[Alcance, str],
) -> None:
    """Mejor `None` —que el informe imprime `n/d`— que un cero que se lee mal."""
    alcance, _ = arbol

    informe = fusionar(alcance, [], generados=0, segundos=0.0, workers=2)

    assert informe.timeout_efectivo is None
    assert informe.timeout_suelo is None
    assert informe.workers == 2


def test_el_timeout_fijado_sobrevive_a_la_fusion(
    arbol: tuple[Alcance, str],
) -> None:
    alcance, _ = arbol
    parcial = _parcial(alcance, 90, suelo=90)
    parcial.timeout_fijado = True

    informe = fusionar(alcance, [parcial], generados=1, segundos=1.0, workers=1)

    assert informe.timeout_fijado is True
    assert informe.timeout_efectivo == 90


# --- R4: el informe declara con qué reloj y con cuántos workers se midió ----


def _fila(texto: str, prefijo: str) -> str:
    return next(linea for linea in texto.splitlines() if linea.startswith(prefijo))


def test_el_informe_declara_timeout_efectivo_suelo_y_workers(
    arbol: tuple[Alcance, str], tmp_path: Path
) -> None:
    """Sin estos tres números, dos campañas dejan de ser comparables en silencio.

    Antes de F-040 el timeout era 120 s fijos para todo el mundo; ahora es un
    derivado (~244 s en esta máquina). Un reviewer que compare el «Tiempo
    total» de una campaña vieja con el de una nueva sin saberlo está comparando
    peras con manzanas.
    """
    alcance, _ = arbol
    informe = InformeMutacion(feature="F-040", alcance=alcance, generados=4)
    informe.timeout_efectivo = 244
    informe.timeout_suelo = 120
    informe.workers = 3
    ruta = tmp_path / "informe.md"

    escribir_informe(informe, ruta)
    texto = ruta.read_text(encoding="utf-8")

    assert "244" in _fila(texto, "| Timeout efectivo por mutante")
    assert "120" in _fila(texto, "| Suelo configurado")
    assert "3" in _fila(texto, "| Workers")


def test_lo_que_no_se_sabe_se_dice_n_d_y_no_cero(
    arbol: tuple[Alcance, str], tmp_path: Path
) -> None:
    """Un cero se lee como medición; una fila ausente, como descuido."""
    alcance, _ = arbol
    ruta = tmp_path / "informe.md"

    escribir_informe(InformeMutacion(feature="F-040", alcance=alcance), ruta)
    texto = ruta.read_text(encoding="utf-8")

    assert "n/d" in _fila(texto, "| Timeout efectivo por mutante")
    assert "n/d" in _fila(texto, "| Suelo configurado")
    assert "n/d" in _fila(texto, "| Workers")


def test_el_informe_dice_cuando_el_timeout_se_fijo_a_mano(
    arbol: tuple[Alcance, str], tmp_path: Path
) -> None:
    """Un número fijado a mano no es un número medido, y el informe lo separa."""
    alcance, _ = arbol
    informe = InformeMutacion(feature="F-040", alcance=alcance, generados=4)
    informe.timeout_efectivo = 90
    informe.timeout_suelo = 90
    informe.timeout_fijado = True
    ruta = tmp_path / "informe.md"

    escribir_informe(informe, ruta)

    fila = _fila(ruta.read_text(encoding="utf-8"), "| Timeout efectivo por mutante")
    assert "fijado" in fila.lower()
    assert "--timeout" in fila


def test_el_timeout_y_los_workers_NO_entran_en_la_comparacion(
    arbol: tuple[Alcance, str], tmp_path: Path
) -> None:
    """Dependen de CÓMO se corrió la campaña, no de lo que midió.

    Una campaña en serie y una paralela sobre el mismo commit tienen que dar
    informes comparables (F-039 R4), y sus workers y su timeout derivado no
    coinciden nunca. Si estas filas entraran en la comparación, el test de
    paridad de F-012 se rompería en cuanto la máquina respirase distinto.
    """
    alcance, _ = arbol

    def _escribir(efectivo: int, workers: int, nombre: str) -> list[str]:
        informe = InformeMutacion(feature="F-040", alcance=alcance, generados=4)
        informe.timeout_efectivo = efectivo
        informe.timeout_suelo = 120
        informe.workers = workers
        ruta = tmp_path / nombre
        escribir_informe(informe, ruta)
        return lineas_comparables(ruta.read_text(encoding="utf-8"))

    assert _escribir(120, 1, "serie.md") == _escribir(244, 3, "paralelo.md")
    assert "| Timeout efectivo por mutante" in " ".join(FILAS_DE_RELOJ) or any(
        prefijo.startswith("| Timeout efectivo") for prefijo in FILAS_DE_RELOJ
    )


def test_el_suelo_configurado_SI_entra_en_la_comparacion(
    arbol: tuple[Alcance, str], tmp_path: Path
) -> None:
    """El suelo sale de `rigor.json`, no del reloj: dos campañas equivalentes
    tienen que declararlo igual, y que deje de coincidir es una diferencia real."""
    alcance, _ = arbol

    def _escribir(suelo: int, nombre: str) -> list[str]:
        informe = InformeMutacion(feature="F-040", alcance=alcance, generados=4)
        informe.timeout_suelo = suelo
        ruta = tmp_path / nombre
        escribir_informe(informe, ruta)
        return lineas_comparables(ruta.read_text(encoding="utf-8"))

    assert _escribir(120, "a.md") != _escribir(300, "b.md")


# --- R7: la configuración tiene que DECIR que es un suelo -------------------


def test_el_doc_de_rigor_json_dice_que_el_timeout_es_un_SUELO() -> None:
    """La semántica cambió sin que el valor cambie: 120 significa otra cosa.

    Quien lea `"timeout_por_mutante_s": 120` y no encuentre escrito que es un
    suelo seguirá creyendo que ha puesto un techo, y no entenderá por qué el
    informe declara 244.
    """
    doc = json.loads(RUTA_RIGOR.read_text(encoding="utf-8"))["mutacion"]["$doc"]

    assert "SUELO" in doc or "suelo" in doc
    assert "linea base" in doc.lower() or "línea base" in doc.lower()


def test_el_doc_de_rigor_json_trae_el_default_nuevo_de_workers() -> None:
    doc = json.loads(RUTA_RIGOR.read_text(encoding="utf-8"))["mutacion"]["$doc"]

    assert "// 2" in doc, "el $doc sigue anunciando la fórmula vieja de workers"
    assert "16" not in doc.split("workers", 1)[-1], (
        "el tope 16 ya no existe: dejarlo escrito manda a la gente a esperar 16 "
        "workers que nunca se van a calcular"
    )


# --- R3 y R6 desde el CLI: el anuncio del timeout --------------------------


@pytest.fixture
def cli_con_campania_falsa(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    """Sustituye la campaña por un doble que apunta con qué timeout se la llamó."""
    recibidos: list[dict] = []
    alcance = Alcance(
        feature="F-040",
        origen="rama",
        ref_diff=("dev", "feature/x"),
        lineas={"harness/mutacion.py": {1, 2, 3}},
    )

    def _campania(alc, *_args: object, **kwargs: object):
        recibidos.append(dict(kwargs))
        return InformeMutacion(feature=alc.feature, alcance=alc, generados=4)

    monkeypatch.setattr("harness.mutacion.alcance_de_feature", lambda *_a, **_k: alcance)
    monkeypatch.setattr("harness.mutacion.ejecutar_campania", _campania)
    return recibidos


def test_el_cli_avisa_de_que_timeout_anula_el_calculo(
    cli_con_campania_falsa: list[dict],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Sin este aviso, quien pasa `--timeout` lee el informe creyendo que el
    número está medido."""
    from harness.mutacion import main

    main(
        [
            "--feature", "F-040", "--workers", "1", "--timeout", "90",
            "--salida", str(tmp_path / "informe.md"),
        ]
    )

    salida = capsys.readouterr().out
    assert "90" in salida
    assert "ANULADO" in salida or "anulado" in salida
    assert cli_con_campania_falsa[0]["timeout_fijado"] is True
    assert cli_con_campania_falsa[0]["timeout_s"] == 90


def test_sin_timeout_el_cli_anuncia_que_lo_va_a_derivar(
    cli_con_campania_falsa: list[dict],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from harness.mutacion import main

    main(
        [
            "--feature", "F-040", "--workers", "1",
            "--salida", str(tmp_path / "informe.md"),
        ]
    )

    salida = capsys.readouterr().out
    assert "derivar" in salida
    assert "suelo" in salida
    assert cli_con_campania_falsa[0]["timeout_fijado"] is False
    assert cli_con_campania_falsa[0]["timeout_base_s"] == timeout_de_linea_base(
        cli_con_campania_falsa[0]["timeout_s"]
    )


# --- El default de `timeout_fijado` en la campaña paralela ------------------


def test_la_campania_paralela_deriva_por_defecto(tmp_path: Path) -> None:
    """Superviviente de la campaña de F-040: `timeout_fijado: bool = False`.

    Sobrevivía porque el único llamador de producción (`main`) siempre lo pasa
    explícito, y los dobles de los demás tests no saben correr una línea base,
    así que sin medición el derivado coincide con el suelo. Con el default
    puesto a `True` la derivación se apagaría entera y ningún test lo notaría:
    el hueco es real, no un mutante equivalente.
    """
    import subprocess

    from harness.mutacion_paralela import ejecutar_campania_paralela

    def _git(*args: str) -> None:
        subprocess.run(
            ["git", "-C", str(tmp_path), *args], capture_output=True, check=True
        )

    # Repositorio de juguete: con un solo worker la campaña paralela muta IN
    # SITU, y ahí la guardia de árbol limpio vuelve a aplicar.
    (tmp_path / "codigo.py").write_text(FUENTE, encoding="utf-8")
    _git("init", "-q")
    _git("config", "user.email", "arnes@ejemplo.invalid")
    _git("config", "user.name", "Arnes")
    _git("add", "-A")
    _git("commit", "-q", "-m", "base")
    alcance = Alcance(
        feature="F-040",
        origen="rama",
        ref_diff=("dev", "feature/x"),
        lineas={"codigo.py": {2, 3}},
    )
    doble = EjecutorCronometrado(espera=ESPERA_BASE)

    informe = ejecutar_campania_paralela(
        alcance,
        servicios=[],
        timeout_s=SUELO_DE_JUGUETE,
        raiz=str(tmp_path),
        workers=1,
        fabrica=lambda _fichero, _raiz: doble,
    )

    assert informe.timeout_fijado is False, (
        "sin `--timeout`, la campaña paralela tiene que DERIVAR el timeout"
    )
    assert informe.timeout_efectivo > SUELO_DE_JUGUETE
    assert set(doble.timeouts_de_mutante) == {informe.timeout_efectivo}
