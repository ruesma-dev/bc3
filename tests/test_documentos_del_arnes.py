# tests/test_documentos_del_arnes.py
"""Lo que el arnés le pide por escrito a cada agente.

Los topes de tamaño, el umbral de reejecución y la revisión incremental no son
código: son reglas que viven en cuatro documentos y que ningún test vigilaba.
La consecuencia conocida es que se pierden —las reglas RM5 y RM6 se acordaron
con el humano y solo existían en `progress/current.md`, que es memoria de
sesión— o se contradicen entre documentos.

Estos tests son baratos y evitan exactamente eso: que una regla escrita hoy
desaparezca de un documento en la próxima reescritura sin que nadie se entere.
"""

from __future__ import annotations

from pathlib import Path

SPECS = Path("specs/SPECS.md")
SPEC_AUTHOR = Path(".claude/agents/spec-author.md")
IMPLEMENTER = Path(".claude/agents/implementer.md")


def _texto(ruta: Path) -> str:
    return ruta.read_text(encoding="utf-8", errors="replace")


# --- R17: los topes de tamaño, en los documentos que los tienen que aplicar --


def test_specs_md_declara_los_topes_de_requirements_y_design() -> None:
    texto = _texto(SPECS)

    assert "150" in texto, "tope de requirements.md"
    assert "250" in texto, "tope de design.md"


def test_specs_md_pide_una_tarea_por_linea_en_tasks() -> None:
    assert "una tarea por línea" in _texto(SPECS).lower()


def test_specs_md_dice_que_lo_que_no_cabe_se_resume_y_se_enlaza() -> None:
    """Son topes, no objetivos: recortar no puede significar tirar evidencia."""
    assert "se resume y se enlaza" in _texto(SPECS)


def test_el_spec_author_conoce_los_topes_de_su_producto() -> None:
    texto = _texto(SPEC_AUTHOR)

    assert "150" in texto and "250" in texto
    assert "se resume y se enlaza" in texto


def test_el_implementer_conoce_el_tope_de_su_informe() -> None:
    texto = _texto(IMPLEMENTER)

    assert "220" in texto, "tope de progress/impl_F-XXX.md"
    assert "se resume y se enlaza" in texto


# --- R18, R19, R20: lo que cambia en el reviewer ----------------------------

REVIEWER = Path(".claude/agents/reviewer.md")


def test_el_reviewer_conoce_el_tope_de_su_informe() -> None:
    texto = _texto(REVIEWER)

    assert "140" in texto, "tope de progress/review_F-XXX.md"
    assert "se resume y se enlaza" in texto


def test_el_umbral_de_reejecucion_baja_a_60_segundos() -> None:
    """Sigue cubriendo el fraude barato y deja de duplicar las campañas caras."""
    texto = _texto(REVIEWER)

    assert "60 segundos" in texto or "60 s" in texto
    assert "inferior a 5 minutos" not in texto, "el umbral viejo, ya retirado"


def test_el_reviewer_revisa_incremental_y_declara_desde_que_sha() -> None:
    texto = _texto(REVIEWER)

    assert "incremental" in texto.lower()
    assert "último commit aprobado" in texto
    assert "SHA" in texto


def test_el_reviewer_recoge_las_seis_reglas_de_campania() -> None:
    texto = _texto(REVIEWER)

    for regla in ("RM1", "RM2", "RM3", "RM4", "RM5", "RM6"):
        assert regla in texto, regla


def test_rm2_salta_por_orden_de_magnitud_no_por_cualquier_diferencia() -> None:
    """Redactada «muy inferior» a secas, RM2 marcaba como sospechosa la campaña
    legítima de F-038: línea base 52,1 s y media 36,4 s. La media baja porque la
    campaña corre con `-x` y un mutante que muere aborta la suite antes de
    terminarla. La alarma es el salto de orden de magnitud, no la diferencia."""
    bloque = _texto(REVIEWER).split("**RM2", 1)[1].split("**RM3", 1)[0]

    assert "orden de magnitud" in bloque
    assert "-x" in bloque, "el motivo por el que la media baja legítimamente"
    assert "111" in bloque, "el caso real de F-034 debe seguir cazándose"


def test_checkpoints_recoge_rm2_con_el_mismo_matiz_que_el_reviewer() -> None:
    """Dos redacciones distintas de la misma regla es peor que no tener ninguna."""
    bloque = _texto(CHECKPOINTS).split("## C4 bis", 1)[1].split("## C4 ter", 1)[0]
    rm2 = bloque.split("**RM2", 1)[1].split("- [ ]", 1)[0]

    assert "orden de magnitud" in rm2
    assert "-x" in rm2


def test_rm5_solo_se_exige_en_rigor_critico_y_con_una_muestra() -> None:
    """Es la única regla que sube el coste: va acotada por decisión del humano."""
    bloque = _texto(REVIEWER).split("**RM5", 1)[1].split("**RM6", 1)[0]

    assert "critico" in bloque or "crítico" in bloque
    assert "muestra" in bloque or "UNO" in bloque or "uno" in bloque


# --- R20, R21: lo que bloquea el cierre y lo que NO automatiza el portero ----

CHECKPOINTS = Path("CHECKPOINTS.md")
INIT = Path("harness/init.sh")


def test_checkpoints_usa_el_mismo_umbral_de_60_segundos() -> None:
    """Dos umbrales distintos en dos documentos es peor que no tener ninguno."""
    texto = _texto(CHECKPOINTS)

    assert "60 segundos" in texto or "60 s" in texto
    assert "inferior a 5 minutos" not in texto


def test_c4_bis_tiene_un_checkbox_por_rm1_rm2_rm5_y_rm6() -> None:
    bloque = _texto(CHECKPOINTS).split("## C4 bis", 1)[1].split("## C4 ter", 1)[0]

    for regla in ("RM1", "RM2", "RM5", "RM6"):
        assert f"{regla}" in bloque, regla
        assert any(
            linea.strip().startswith("- [ ]") and regla in linea
            for linea in bloque.splitlines()
        ), f"{regla} debe ser un checkbox, no un párrafo"


def test_rm3_y_rm4_no_son_checkbox_sino_criterio_del_reviewer() -> None:
    """No hay forma barata de decidir equivalencia ni de exigir una técnica."""
    for linea in _texto(CHECKPOINTS).splitlines():
        if linea.strip().startswith("- [ ]"):
            assert "RM3" not in linea and "RM4" not in linea, linea


def test_el_checkbox_de_rm5_esta_condicionado_a_rigor_critico() -> None:
    bloque = _texto(CHECKPOINTS).split("## C4 bis", 1)[1].split("## C4 ter", 1)[0]
    linea = next(l for l in bloque.splitlines() if "RM5" in l)
    resto = bloque.split(linea, 1)[1].split("- [ ]", 1)[0]

    assert "critico" in linea + resto or "crítico" in linea + resto


def test_ninguna_regla_de_campania_es_puerta_automatica() -> None:
    """Lo que init.sh gana es el tope de tamaño; RM1–RM6 exigen juicio."""
    portero = _texto(INIT)

    for regla in ("RM1", "RM2", "RM3", "RM4", "RM5", "RM6"):
        assert regla not in portero, f"{regla} no puede ser puerta automática"
