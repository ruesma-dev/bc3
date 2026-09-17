# tests/test_mutacion_repaso_timeouts.py
"""Un `timeout` no es un veredicto sobre el mutante: es un reintento pendiente.

Medido el 2026-09-02 en un proyecto real con 22 CPUs:
la misma feature, el mismo commit y tres campañas dieron **15, 27 y 0 timeouts**
sobre mutantes distintos cada vez. La causa no era ningún cuelgue, sino la
contención: con 16 suites simultáneas cada una pasaba de 38,7 s a 131,6 s,
contra un tope de 120 s por mutante. Mutantes que una campaña daba por muertos
salían `timeout` en la siguiente, y al revés.

Por eso la campaña paralela repasa **en serie** los mutantes en `timeout` antes
de dar el informe: sin concurrencia, el reloj mide al mutante y no a la máquina.
Lo que siga en `timeout` después del repaso ya sí es señal de un cuelgue real, y
el informe tiene que poder decirlo.
"""

from __future__ import annotations

import subprocess
import threading
from pathlib import Path

import pytest

from harness.alcance import Alcance
from harness.mutacion import (
    MUERTO,
    SUPERVIVIENTE,
    TIMEOUT,
    InformeMutacion,
    Mutante,
    ResultadoSuite,
)
from harness.mutacion_paralela import (
    clave_estable,
    ejecutar_campania_paralela,
    reemplazar_timeouts,
)

ALCANCE = Alcance(
    feature="F-000", origen="rama", ref_diff=("dev", "rama"), lineas={"codigo.py": {2}}
)


def mutante(linea: int, operador: str = "comparacion") -> Mutante:
    return Mutante(
        fichero="codigo.py",
        linea=linea,
        col=0,
        original="==",
        mutado="!=",
        operador=operador,
    )


# --- La función pura: cómo se reparte lo que el repaso ha aclarado -----------


def informe_con_timeouts(*mutantes: Mutante) -> InformeMutacion:
    """Informe de una campaña que dejó `mutantes` sin veredicto real."""
    evaluados = [*mutantes, mutante(99)]
    return InformeMutacion(
        feature="F-000",
        alcance=ALCANCE,
        generados=len(evaluados),
        muertos=1,
        supervivientes=[],
        timeouts=list(mutantes),
        mutantes_evaluados=evaluados,
        segundos=100.0,
    )


def reintento(
    muertos: int = 0,
    supervivientes: list[Mutante] | None = None,
    timeouts: list[Mutante] | None = None,
    segundos: float = 7.0,
) -> InformeMutacion:
    """El informe que devuelve el repaso en serie sobre los mutantes en timeout."""
    return InformeMutacion(
        feature="F-000",
        alcance=ALCANCE,
        generados=muertos + len(supervivientes or []) + len(timeouts or []),
        muertos=muertos,
        supervivientes=list(supervivientes or []),
        timeouts=list(timeouts or []),
        mutantes_evaluados=[],
        segundos=segundos,
    )


def test_un_timeout_que_al_repasarlo_muere_pasa_a_la_cuenta_de_muertos() -> None:
    informe = informe_con_timeouts(mutante(2), mutante(3))
    corregido = reemplazar_timeouts(informe, reintento(muertos=2))

    assert corregido.muertos == 3  # el muerto de la campaña más los dos repasados
    assert corregido.timeouts == []
    assert corregido.supervivientes == []


def test_un_timeout_que_al_repasarlo_sobrevive_entra_en_supervivientes() -> None:
    """El caso que más duele: un hueco de la suite que el timeout estaba tapando."""
    vivo = mutante(3)
    informe = informe_con_timeouts(mutante(2), vivo)
    corregido = reemplazar_timeouts(informe, reintento(muertos=1, supervivientes=[vivo]))

    assert corregido.supervivientes == [vivo]
    assert corregido.muertos == 2
    assert corregido.timeouts == []


def test_lo_que_sigue_en_timeout_tras_el_repaso_se_queda_en_timeout() -> None:
    """Sin concurrencia que lo explique, un timeout ya sí señala un cuelgue real."""
    colgado = mutante(2)
    informe = informe_con_timeouts(colgado, mutante(3))
    corregido = reemplazar_timeouts(
        informe, reintento(muertos=1, timeouts=[colgado])
    )

    assert corregido.timeouts == [colgado]
    assert corregido.muertos == 2


def test_el_repaso_no_cambia_ni_los_generados_ni_los_evaluados() -> None:
    """Solo cambia CÓMO se reparten: nadie se evalúa dos veces ni desaparece."""
    informe = informe_con_timeouts(mutante(2), mutante(3))
    antes_evaluados = list(informe.mutantes_evaluados)

    corregido = reemplazar_timeouts(informe, reintento(muertos=2))

    assert corregido.generados == informe.generados
    assert corregido.mutantes_evaluados == antes_evaluados
    assert corregido.evaluados == len(antes_evaluados)
    total = corregido.muertos + len(corregido.supervivientes) + len(corregido.timeouts)
    assert total == corregido.evaluados


def test_las_listas_del_informe_corregido_salen_ordenadas() -> None:
    """Mismo criterio que `fusionar`: el informe no delata en qué orden se repasó."""
    tarde, pronto = mutante(30), mutante(4)
    informe = informe_con_timeouts(pronto, tarde)
    corregido = reemplazar_timeouts(
        informe, reintento(supervivientes=[tarde, pronto])
    )

    assert corregido.supervivientes == sorted(
        corregido.supervivientes, key=clave_estable
    )
    assert corregido.supervivientes == [pronto, tarde]


def test_el_tiempo_del_repaso_se_suma_al_de_la_campania() -> None:
    """El repaso cuesta minutos reales; ocultarlos falsea la media por mutante."""
    informe = informe_con_timeouts(mutante(2))
    corregido = reemplazar_timeouts(informe, reintento(muertos=1, segundos=7.5))

    assert corregido.segundos == pytest.approx(107.5)


def test_el_informe_registra_cuantos_timeouts_se_repasaron() -> None:
    """Sin el dato, nadie puede distinguir «no hubo timeouts» de «se arreglaron»."""
    informe = informe_con_timeouts(mutante(2), mutante(3), mutante(4))
    corregido = reemplazar_timeouts(
        informe, reintento(muertos=2, timeouts=[mutante(4)])
    )

    assert corregido.timeouts_repasados == 3
    assert corregido.timeouts_resueltos == 2


def test_sin_timeouts_el_informe_sale_intacto() -> None:
    informe = InformeMutacion(
        feature="F-000", alcance=ALCANCE, generados=1, muertos=1, segundos=3.0
    )
    corregido = reemplazar_timeouts(informe, reintento())

    assert corregido.timeouts_repasados == 0
    assert corregido.muertos == 1
    assert corregido.segundos == pytest.approx(3.0)


# --- El coordinador: que el repaso ocurra de verdad, y dónde ----------------

FUENTE = "def clasifica(a, b):\n    if a == b:\n        return a > b\n    return None\n"


class EjecutorQueSeAtragantaLaPrimeraVez:
    """Da `timeout` la primera vez que ve cada mutante y su veredicto la segunda.

    Es exactamente lo que hace la máquina real: bajo contención el reloj se
    agota antes de que la suite llegue al test que mata, y el mismo mutante
    juzgado a solas muere sin despeinarse. El doble identifica al mutante por el
    código que encuentra en SU worktree, así que no necesita saber nada del
    reparto entre workers.
    """

    def __init__(
        self,
        raiz: str,
        vistos: dict[str, int],
        cerrojo: threading.Lock,
        raices: list[str],
        veredicto_al_repasar: str = MUERTO,
    ) -> None:
        self._raiz = raiz
        self._vistos = vistos
        self._cerrojo = cerrojo
        self._raices = raices
        self._veredicto = veredicto_al_repasar

    def linea_base(self, _timeout_s: int) -> ResultadoSuite:
        """La suite SIN mutar nada: verde, y no cuenta como visita de un mutante."""
        return ResultadoSuite(codigo=0)

    def ejecutar(self, _timeout_s: int) -> str:
        codigo = (Path(self._raiz) / "codigo.py").read_text(encoding="utf-8")
        with self._cerrojo:
            self._raices.append(self._raiz)
            self._vistos[codigo] = self._vistos.get(codigo, 0) + 1
            primera = self._vistos[codigo] == 1
        return TIMEOUT if primera else self._veredicto


@pytest.fixture
def repo(tmp_path: Path) -> tuple[Alcance, str]:
    """Repositorio de juguete con código mutable y su commit inicial."""

    def _git(*args: str) -> None:
        subprocess.run(
            ["git", "-C", str(tmp_path), *args], capture_output=True, check=True
        )

    (tmp_path / "codigo.py").write_text(FUENTE, encoding="utf-8")
    _git("init", "-q")
    _git("config", "user.email", "arnes@ejemplo.invalid")
    _git("config", "user.name", "Arnes")
    _git("add", "codigo.py")
    _git("commit", "-q", "-m", "base")
    alcance = Alcance(
        feature="F-000",
        origen="rama",
        ref_diff=("dev", "feature/x"),
        lineas={"codigo.py": {2, 3}},
    )
    return (alcance, str(tmp_path))


def campania(
    repo: tuple[Alcance, str], veredicto_al_repasar: str = MUERTO
) -> tuple[InformeMutacion, list[str]]:
    """Campaña paralela de verdad, con dos worktrees, sobre el repo de juguete."""
    alcance, raiz = repo
    vistos: dict[str, int] = {}
    cerrojo = threading.Lock()
    raices: list[str] = []
    informe = ejecutar_campania_paralela(
        alcance,
        servicios=[],
        timeout_s=5,
        raiz=raiz,
        workers=2,
        fabrica=lambda _fichero, raiz_worker: EjecutorQueSeAtragantaLaPrimeraVez(
            raiz_worker, vistos, cerrojo, raices, veredicto_al_repasar
        ),
    )
    return (informe, raices)


def test_la_campania_paralela_repasa_los_timeouts_antes_de_dar_el_informe(
    repo: tuple[Alcance, str],
) -> None:
    """Todos se atragantan a la primera; ninguno debe quedar como `timeout`."""
    informe, _ = campania(repo)

    assert informe.evaluados > 0, "sin mutantes el escenario no prueba nada"
    assert informe.timeouts == []
    assert informe.timeouts_repasados == informe.evaluados
    assert informe.muertos == informe.evaluados


def test_el_repaso_destapa_a_los_supervivientes_que_el_timeout_escondia(
    repo: tuple[Alcance, str],
) -> None:
    informe, _ = campania(repo, veredicto_al_repasar=SUPERVIVIENTE)

    assert informe.timeouts == []
    assert len(informe.supervivientes) == informe.evaluados
    assert informe.supervivientes == sorted(informe.supervivientes, key=clave_estable)


def test_el_repaso_nunca_muta_el_arbol_principal(repo: tuple[Alcance, str]) -> None:
    """Garantía dura del modo paralelo: el árbol que ves en disco no se toca."""
    _, raiz = repo
    _, raices = campania(repo)

    assert raices, "el doble no llegó a ejecutarse"
    assert raiz not in raices, (
        f"el repaso juzgó sobre el árbol principal: {sorted(set(raices))}"
    )
    assert (Path(raiz) / "codigo.py").read_text(encoding="utf-8") == FUENTE
    porcelain = subprocess.run(
        ["git", "-C", raiz, "status", "--porcelain"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert porcelain.stdout.strip() == ""


def test_el_repaso_se_hace_en_un_solo_worktree(repo: tuple[Alcance, str]) -> None:
    """En serie quiere decir en serie: si el repaso se repartiera, no serviría.

    La campaña reparte entre dos worktrees; el repaso, que va después, tiene que
    caber entero en uno solo. Son las últimas `timeouts_repasados` ejecuciones.
    """
    informe, raices = campania(repo)

    repasadas = raices[-informe.timeouts_repasados :]
    assert len(repasadas) >= 2, "el escenario necesita más de un mutante repasado"
    assert len(set(repasadas)) == 1, f"el repaso se repartió entre {set(repasadas)}"
