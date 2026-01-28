# interface_adapters/gui/gui_app.py
from __future__ import annotations

import os
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Any

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

# --- Núcleo Fase 1 (limpieza) ---
from infrastructure.bc3.bc3_modifier import convert_to_material
from application.services.build_tree_service import build_tree
from application.services.export_csv_service import export_to_csv

# --- Fase 2 (IA) opcional ---
try:
    from application.services.phase2_code_mapper import run_phase2  # type: ignore
    HAS_PHASE2 = True
except Exception:
    HAS_PHASE2 = False


# --------------------------------------------------------------------------- #
#  Configuración visual y presets                                             #
# --------------------------------------------------------------------------- #
APP_VERSION = "0.995 referencia cruzada"
APP_TITLE = "Limpieza de BC3"

# Nombres de fichero aceptados por defecto para el catálogo IA externo
DEFAULT_CATALOG_BASENAMES = ["catalog.xlsx", "catalogo_productos.xlsx", "catalogo.xlsx"]

# Plantillas REFCRU (exportadas desde BC) típicas
DEFAULT_TEMPLATE_BASENAMES = ["REFCRU_template.xlsx", "REFCRU.xlsx"]

# Rutas “típicas” en desarrollo (cuando NO es ejecutable PyInstaller)
DEV_LOGO_PATHS = [
    "interface_adapters/gui/assets/logo.png",
    "resources/logo.png",
]
DEV_CATALOG_PATHS = [
    "config/catalogo_productos.xlsx",
    "resources/catalogo_productos.xlsx",
    "config/catalogo.xlsx",
    "resources/catalogo.xlsx",
    "config/catalog.xlsx",
    "resources/catalog.xlsx",
]
DEV_TEMPLATE_PATHS = [
    "resources/REFCRU_template.xlsx",
    "resources/REFCRU.xlsx",
    "config/REFCRU_template.xlsx",
    "config/REFCRU.xlsx",
]

# Modelos Gemini (free tier) y límites orientativos
MODEL_PRESETS = {
    "gemini-3-pro-preview": {"RPM": 5, "TPM": 250_000, "RPD": 100},
    "gemini-2.5-pro": {"RPM": 5, "TPM": 250_000, "RPD": 100},
    "gemini-2.5-flash": {"RPM": 10, "TPM": 250_000, "RPD": 250},
    "gemini-2.5-flash-lite": {"RPM": 15, "TPM": 250_000, "RPD": 1000},
    "gemini-2.0-flash": {"RPM": 15, "TPM": 1_000_000, "RPD": 200},
    "gemini-2.0-flash-lite": {"RPM": 30, "TPM": 1_000_000, "RPD": 200},
}
DEFAULT_MODEL = "gemini-3-pro-preview"


# --------------------------------------------------------------------------- #
#  Utilidades de recursos                                                     #
# --------------------------------------------------------------------------- #
def _is_frozen() -> bool:
    """True si se ejecuta como .exe empaquetado (PyInstaller)."""
    return getattr(sys, "frozen", False)


def _runtime_base_dir() -> Path:
    """
    Directorio base “real” donde vive el exe en despliegue.
    - frozen: carpeta del exe
    - dev: carpeta del script lanzador
    """
    if _is_frozen():
        return Path(sys.executable).resolve().parent
    return Path(sys.argv[0]).resolve().parent


def _project_root() -> Path:
    """
    Raíz del proyecto (modo dev) o carpeta del exe en modo empaquetado.
    Si es onefile, _MEIPASS existe y sirve para recursos embebidos,
    pero para data/ externo (onefolder) necesitamos el dir del exe.
    """
    if hasattr(sys, "_MEIPASS"):
        # recursos internos onefile
        return Path(sys._MEIPASS)
    if _is_frozen():
        # onefolder: recursos externos junto al exe
        return _runtime_base_dir()
    return Path(__file__).resolve().parents[2]


def _candidate_catalog_paths() -> List[Path]:
    """
    Posibles ubicaciones del catálogo IA (en orden de prioridad):
      1) Variable de entorno BC3_CATALOG_PATH
      2) Variable de entorno PHASE2_CATALOG_XLSX (nuevo run_phase2)
      3) Estructura desplegada: <exe_dir>/data/catalog/<basename>
      4) Junto al exe o subcarpetas legacy {resources, catalog, config}
      5) En empaquetado onefile: <_MEIPASS>/resources/<basename>
      6) Rutas relativas típicas de desarrollo (DEV_CATALOG_PATHS)
    """
    cands: List[Path] = []

    # (1) ENV legacy
    env_path = os.getenv("BC3_CATALOG_PATH", "").strip()
    if env_path:
        cands.append(Path(env_path))

    # (2) ENV nuevo
    env_path2 = os.getenv("PHASE2_CATALOG_XLSX", "").strip()
    if env_path2:
        cands.append(Path(env_path2))

    exe_dir = _runtime_base_dir()
    data_dir = exe_dir / "data" / "catalog"

    # (3) estructura recomendada
    for b in DEFAULT_CATALOG_BASENAMES:
        cands.append(data_dir / b)

    # (4) legacy al lado del exe
    for b in DEFAULT_CATALOG_BASENAMES:
        cands.append(exe_dir / b)
        cands.append(exe_dir / "resources" / b)
        cands.append(exe_dir / "catalog" / b)
        cands.append(exe_dir / "config" / b)

    # (5) onefile
    if hasattr(sys, "_MEIPASS"):
        base_pack = Path(sys._MEIPASS)
        for b in DEFAULT_CATALOG_BASENAMES:
            cands.append(base_pack / "resources" / b)

    # (6) desarrollo
    root = _project_root()
    for p in DEV_CATALOG_PATHS:
        cands.append(root / p)

    return cands


def _candidate_template_paths() -> List[Path]:
    """
    Posibles ubicaciones del template REFCRU (en orden de prioridad):
      1) ENV BC3_REFCRU_TEMPLATE_PATH (GUI)
      2) ENV PHASE2_REFCRU_TEMPLATE_XLSX (nuevo run_phase2)
      3) Estructura desplegada: <exe_dir>/data/templates/<basename>
      4) Legacy: <exe_dir>/{resources,templates,config}/<basename>
      5) onefile: <_MEIPASS>/resources/<basename>
      6) Desarrollo: DEV_TEMPLATE_PATHS
    """
    cands: List[Path] = []

    env_path = os.getenv("BC3_REFCRU_TEMPLATE_PATH", "").strip()
    if env_path:
        cands.append(Path(env_path))

    env_path2 = os.getenv("PHASE2_REFCRU_TEMPLATE_XLSX", "").strip()
    if env_path2:
        cands.append(Path(env_path2))

    exe_dir = _runtime_base_dir()
    data_dir = exe_dir / "data" / "templates"

    for b in DEFAULT_TEMPLATE_BASENAMES:
        cands.append(data_dir / b)

    for b in DEFAULT_TEMPLATE_BASENAMES:
        cands.append(exe_dir / b)
        cands.append(exe_dir / "resources" / b)
        cands.append(exe_dir / "templates" / b)
        cands.append(exe_dir / "config" / b)

    if hasattr(sys, "_MEIPASS"):
        base_pack = Path(sys._MEIPASS)
        for b in DEFAULT_TEMPLATE_BASENAMES:
            cands.append(base_pack / "resources" / b)

    root = _project_root()
    for p in DEV_TEMPLATE_PATHS:
        cands.append(root / p)

    return cands


# --------------------------------------------------------------------------- #
#  Aplicación                                                                 #
# --------------------------------------------------------------------------- #
class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1000x720")
        self.minsize(920, 620)

        # Estado
        self.input_path: Optional[Path] = None
        self.catalog_path: Optional[Path] = None
        self.refcru_template_path: Optional[Path] = None
        self.last_output_dir: Optional[Path] = None
        self.model_name: str = DEFAULT_MODEL
        self.model_limits = MODEL_PRESETS[self.model_name].copy()

        # Estilos
        self._init_styles()

        # Layout raíz
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        self._build_header()
        self._build_body()
        self._build_footer()

        # ENV para fase 2
        self._apply_model_env()

        # Precarga de catálogo IA + template REFCRU
        self._auto_load_catalog()
        self._auto_load_refcru_template()

    # --------------------------- estilos / header --------------------------- #
    def _init_styles(self) -> None:
        self.style = ttk.Style(self)
        try:
            self.style.theme_use("clam")
        except Exception:
            pass
        self.style.configure("Primary.TButton", padding=10, font=("Segoe UI", 11, "bold"))
        self.style.configure("Secondary.TButton", padding=8, font=("Segoe UI", 10))
        self.style.configure("Card.TFrame", relief="groove", borderwidth=1)
        self.style.configure("BannerSuccess.TLabel", font=("Segoe UI", 14, "bold"), foreground="#0F8F3B")
        self.style.configure("BannerFail.TLabel", font=("Segoe UI", 14, "bold"), foreground="#B00020")

    def _build_header(self) -> None:
        header = ttk.Frame(self, padding=(14, 10))
        header.grid(row=0, column=0, sticky="ew")

        # columnas: 0=título/subtítulo | 1=expansor | 2=versión | 3=logo
        header.columnconfigure(0, weight=1)
        header.columnconfigure(1, weight=1)
        header.columnconfigure(2, weight=0)
        header.columnconfigure(3, weight=0)

        tk.Label(header, text=APP_TITLE, font=("Segoe UI", 18, "bold")).grid(row=0, column=0, sticky="w")
        tk.Label(
            header,
            text="1ª pasada: Limpieza •  2ª pasada: Asignación de productos (IA)",
            font=("Segoe UI", 10),
        ).grid(row=1, column=0, sticky="w")

        tk.Label(header, text=f"v{APP_VERSION}", font=("Segoe UI", 10)).grid(row=0, column=2, sticky="ne", padx=(0, 8))

        logo_path = self._resolve_resource_first(DEV_LOGO_PATHS, packaged_subdir="resources")
        if logo_path and logo_path.exists():
            try:
                self.logo_img = self._load_logo_small(logo_path, max_w=200, max_h=48)
                tk.Label(header, image=self.logo_img).grid(row=0, column=3, rowspan=2, sticky="ne")
            except Exception:
                pass

    def _load_logo_small(self, path: Path, max_w: int = 200, max_h: int = 48) -> tk.PhotoImage:
        img = tk.PhotoImage(file=str(path))
        w, h = img.width(), img.height()
        if w <= max_w and h <= max_h:
            return img
        fx = (w + max_w - 1) // max_w
        fy = (h + max_h - 1) // max_h
        f = max(1, fx, fy)
        return img.subsample(f, f)

    def _resolve_resource_first(self, candidates: List[str], packaged_subdir: Optional[str] = None) -> Optional[Path]:
        base = _project_root()

        if hasattr(sys, "_MEIPASS") and packaged_subdir:
            for c in candidates:
                p = base / packaged_subdir / Path(c).name
                if p.exists():
                    return p

        for c in candidates:
            p = (base / c).resolve()
            if p.exists():
                return p

        return None

    # ------------------------------ body ----------------------------------- #
    def _build_body(self) -> None:
        body = ttk.Frame(self, padding=(14, 0))
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(0, weight=1)
        body.rowconfigure(2, weight=1)

        # --- Card entradas ---
        card = ttk.Frame(body, style="Card.TFrame", padding=12)
        card.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        for i in range(8):
            card.columnconfigure(i, weight=0)
        card.columnconfigure(1, weight=1)
        card.columnconfigure(4, weight=1)

        # Row 0: BC3 + Catálogo
        ttk.Label(card, text="Fichero BC3:", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w", padx=(4, 8))
        self.entry_input = ttk.Entry(card)
        self.entry_input.grid(row=0, column=1, sticky="ew", padx=(0, 8))
        self.btn_browse_input = ttk.Button(card, text="Buscar…", command=self._on_browse_input)
        self.btn_browse_input.grid(row=0, column=2, sticky="ew")

        ttk.Label(card, text="Catálogo (IA):", font=("Segoe UI", 10, "bold")).grid(row=0, column=3, sticky="w", padx=(16, 8))
        self.entry_catalog = ttk.Entry(card)
        self.entry_catalog.grid(row=0, column=4, sticky="ew", padx=(0, 8))
        self.btn_browse_catalog = ttk.Button(card, text="Buscar…", command=self._on_browse_catalog)
        self.btn_browse_catalog.grid(row=0, column=5, sticky="ew")

        # Row 1: Template REFCRU
        ttk.Label(card, text="Template (REFCRU):", font=("Segoe UI", 10, "bold")).grid(row=1, column=0, sticky="w", padx=(4, 8), pady=(10, 0))
        self.entry_refcru_template = ttk.Entry(card)
        self.entry_refcru_template.grid(row=1, column=1, sticky="ew", padx=(0, 8), pady=(10, 0))
        self.btn_browse_refcru_template = ttk.Button(card, text="Buscar…", command=self._on_browse_refcru_template)
        self.btn_browse_refcru_template.grid(row=1, column=2, sticky="ew", pady=(10, 0))

        ttk.Label(card, text="(Exportado desde BC)", font=("Segoe UI", 9)).grid(row=1, column=3, sticky="w", padx=(16, 8), pady=(10, 0))

        # Row 2: Modelo + límites
        ttk.Label(card, text="Modelo IA:", font=("Segoe UI", 10, "bold")).grid(row=2, column=0, sticky="w", padx=(4, 8), pady=(10, 0))
        self.model_var = tk.StringVar(value=self.model_name)
        self.cmb_model = ttk.Combobox(
            card,
            textvariable=self.model_var,
            state="readonly",
            values=list(MODEL_PRESETS.keys()),
            width=24,
        )
        self.cmb_model.grid(row=2, column=1, sticky="w", pady=(10, 0))
        self.cmb_model.bind("<<ComboboxSelected>>", self._on_model_change)

        self.lbl_limits = ttk.Label(card, text=self._limits_text(), font=("Segoe UI", 9))
        self.lbl_limits.grid(row=2, column=2, columnspan=4, sticky="w", pady=(10, 0), padx=(10, 0))

        # --- Acciones ---
        actions = ttk.Frame(body, padding=(0, 4))
        actions.grid(row=1, column=0, sticky="e")

        self.btn_clean = ttk.Button(
            actions,
            text="LIMPIAR (1ª pasada)",
            command=self._run_clean_clicked,
            style="Primary.TButton",
        )
        self.btn_clean.grid(row=0, column=0, padx=(0, 8))

        self.btn_phase2 = ttk.Button(
            actions,
            text="ASIGNAR PRODUCTOS (IA)",
            command=self._run_phase2_clicked,
            style="Secondary.TButton",
            state=("normal" if HAS_PHASE2 else "disabled"),
        )
        self.btn_phase2.grid(row=0, column=1)

        # --- Log ---
        log_card = ttk.Frame(body, style="Card.TFrame", padding=12)
        log_card.grid(row=2, column=0, sticky="nsew")
        log_card.columnconfigure(0, weight=1)
        log_card.rowconfigure(1, weight=1)

        ttk.Label(log_card, text="Log de ejecución", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w")
        self.txt_log = tk.Text(log_card, height=18, wrap="word", font=("Consolas", 10))
        self.txt_log.grid(row=1, column=0, sticky="nsew", pady=(6, 6))
        self._init_log_tags()

        log_buttons = ttk.Frame(log_card)
        log_buttons.grid(row=2, column=0, sticky="e")
        ttk.Button(log_buttons, text="Abrir carpeta output", command=self._open_output_folder).grid(row=0, column=0)

    def _build_footer(self) -> None:
        footer = ttk.Frame(self, padding=(14, 8))
        footer.grid(row=2, column=0, sticky="ew")
        ttk.Label(
            footer,
            text="Hecho para presupuestos BC3 • Arquitectura limpia • Python 3.12",
            font=("Segoe UI", 9),
        ).pack(side="left")

    # ----------------------------- utilidades -------------------------------- #
    def _limits_text(self) -> str:
        L = self.model_limits
        return f"Límites (Free): RPM={L['RPM']}, TPM={L['TPM']:,}, RPD={L['RPD']}"

    def _apply_model_env(self) -> None:
        os.environ["GEMINI_MODEL_NAME"] = self.model_name
        os.environ["GEMINI_RPM"] = str(self.model_limits["RPM"])

    def _init_log_tags(self) -> None:
        self.txt_log.tag_configure("time", foreground="#555")
        self.txt_log.tag_configure("ok", foreground="#0F8F3B")
        self.txt_log.tag_configure("err", foreground="#B00020")
        self.txt_log.tag_configure(
            "banner_ok",
            font=("Segoe UI", 16, "bold"),
            foreground="#0F8F3B",
            spacing1=10,
            spacing3=10,
            justify="center",
        )
        self.txt_log.tag_configure(
            "banner_fail",
            font=("Segoe UI", 16, "bold"),
            foreground="#B00020",
            spacing1=10,
            spacing3=10,
            justify="center",
        )

    def _append(self, msg: str, tag: Optional[str] = None) -> None:
        ts = datetime.now().strftime("[%H:%M:%S] ")
        self.txt_log.insert("end", ts, ("time",))
        self.txt_log.insert("end", msg + "\n", (tag,) if tag else ())
        self.txt_log.see("end")

    def _append_async(self, msg: str, tag: Optional[str] = None) -> None:
        self.after(0, lambda: self._append(msg, tag))

    def _append_banner(self, text: str, ok: bool = True) -> None:
        self.txt_log.insert("end", "\n")
        self.txt_log.insert("end", text + "\n", ("banner_ok" if ok else "banner_fail",))
        self.txt_log.insert("end", "\n")
        self.txt_log.see("end")

    def _append_banner_async(self, text: str, ok: bool = True) -> None:
        self.after(0, lambda: self._append_banner(text, ok))

    def _clear_log(self) -> None:
        self.txt_log.delete("1.0", "end")

    def _disable_actions(self) -> None:
        self.btn_clean.config(state="disabled")
        self.btn_phase2.config(state="disabled")
        self.btn_browse_input.config(state="disabled")
        self.btn_browse_catalog.config(state="disabled")
        self.btn_browse_refcru_template.config(state="disabled")
        self.cmb_model.config(state="disabled")

    def _enable_actions(self) -> None:
        self.btn_clean.config(state="normal")
        if HAS_PHASE2:
            self.btn_phase2.config(state="normal")
        self.btn_browse_input.config(state="normal")
        self.btn_browse_catalog.config(state="normal")
        self.btn_browse_refcru_template.config(state="normal")
        self.cmb_model.config(state="readonly")

    # ----------------------- carga automática catálogo ----------------------- #
    def _auto_load_catalog(self) -> None:
        current = self.entry_catalog.get().strip()
        if current and Path(current).exists():
            self.catalog_path = Path(current)
            return

        for path in _candidate_catalog_paths():
            if path.exists():
                self.catalog_path = path
                self.entry_catalog.delete(0, "end")
                self.entry_catalog.insert(0, str(path))
                self._append(f"Catálogo IA precargado: {path}", tag="ok")
                return

        tried = "\n  - " + "\n  - ".join(str(p) for p in _candidate_catalog_paths())
        self._append(
            "Aviso: no se encontró el catálogo IA por defecto. "
            "Puedes seleccionarlo manualmente desde 'Buscar…' o colocarlo en:\n"
            "  <carpeta_del_exe>/data/catalog/\n"
            "Rutas probadas:" + tried,
            tag="err",
        )

    def _auto_load_refcru_template(self) -> None:
        current = self.entry_refcru_template.get().strip()
        if current and Path(current).exists():
            self.refcru_template_path = Path(current)
            return

        for path in _candidate_template_paths():
            if path.exists():
                self.refcru_template_path = path
                self.entry_refcru_template.delete(0, "end")
                self.entry_refcru_template.insert(0, str(path))
                self._append(f"Template REFCRU precargado: {path}", tag="ok")
                return

        tried = "\n  - " + "\n  - ".join(str(p) for p in _candidate_template_paths())
        self._append(
            "Aviso: no se encontró el template REFCRU por defecto. "
            "Puedes seleccionarlo manualmente desde 'Buscar…' o colocarlo en:\n"
            "  <carpeta_del_exe>/data/templates/\n"
            "Rutas probadas:" + tried,
            tag="err",
        )

    # ------------------------------ eventos UI ------------------------------ #
    def _on_browse_input(self) -> None:
        fn = filedialog.askopenfilename(
            title="Selecciona un fichero BC3",
            filetypes=[("Ficheros BC3", "*.bc3")],
        )
        if fn:
            self.input_path = Path(fn)
            self.entry_input.delete(0, "end")
            self.entry_input.insert(0, str(self.input_path))
            self.last_output_dir = self.input_path.parent

    def _on_browse_catalog(self) -> None:
        fn = filedialog.askopenfilename(
            title="Selecciona catálogo (Excel/CSV)",
            filetypes=[
                ("Excel", "*.xlsx"),
                ("Excel (97-2003)", "*.xls"),
                ("CSV", "*.csv"),
                ("Todos", "*.*"),
            ],
        )
        if fn:
            self.catalog_path = Path(fn)
            self.entry_catalog.delete(0, "end")
            self.entry_catalog.insert(0, str(self.catalog_path))

    def _on_browse_refcru_template(self) -> None:
        fn = filedialog.askopenfilename(
            title="Selecciona template REFCRU (exportado desde Business Central)",
            filetypes=[
                ("Excel", "*.xlsx"),
                ("Todos", "*.*"),
            ],
        )
        if fn:
            self.refcru_template_path = Path(fn)
            self.entry_refcru_template.delete(0, "end")
            self.entry_refcru_template.insert(0, str(self.refcru_template_path))

    def _on_model_change(self, _evt=None) -> None:
        sel = self.model_var.get()
        if sel in MODEL_PRESETS:
            self.model_name = sel
            self.model_limits = MODEL_PRESETS[sel].copy()
            self.lbl_limits.config(text=self._limits_text())
            self._apply_model_env()
            self._append(f"Modelo IA seleccionado: {sel}  →  {self._limits_text()}")

    # ------------------------------- acciones -------------------------------- #
    def _run_clean_clicked(self) -> None:
        if not self._ensure_input():
            return
        self._clear_log()
        self._append("Iniciando 1ª pasada (Limpieza BC3)…")
        self._disable_actions()
        threading.Thread(target=self._run_clean_thread, daemon=True).start()

    def _run_phase2_clicked(self) -> None:
        if not self._ensure_input():
            return
        cleaned = self._cleaned_bc3_path()
        if not cleaned.exists():
            messagebox.showerror("Fase 2 (IA)", f"No encuentro el BC3 limpio:\n{cleaned}\n\nEjecuta antes la 1ª pasada.")
            return
        if not self._ensure_catalog():
            return

        # Template REFCRU es recomendable pero no obligatorio (run_phase2 ya avisará)
        self._ensure_refcru_template_soft()

        self._clear_log()
        self._append(f"Iniciando 2ª pasada (IA) con modelo {self.model_name} …")
        self._disable_actions()
        threading.Thread(target=self._run_phase2_thread, args=(cleaned,), daemon=True).start()

    # --------------------------- lógica de ejecución ------------------------ #
    def _run_clean_thread(self) -> None:
        ok = True
        try:
            src = self.input_path
            out_dir = src.parent
            self.last_output_dir = out_dir

            cleaned_bc3 = self._cleaned_bc3_path()
            tree_csv = out_dir / f"{src.stem}_tree.csv"

            self._append_async(f"Normalizando BC3 → {cleaned_bc3.name}")
            convert_to_material(src, cleaned_bc3)

            self._append_async(f"Construyendo árbol y exportando CSV → {tree_csv.name}")
            roots = build_tree(cleaned_bc3)
            export_to_csv(roots, tree_csv)

            self._append_async(f"Guardado: {cleaned_bc3}")
            self._append_async(f"Guardado: {tree_csv}")

        except Exception as e:
            ok = False
            self._append_async(f"ERROR: {e}", tag="err")
        finally:
            self._enable_actions()
            self._append_banner_async("TERMINADO" if ok else "FAIL", ok=ok)

    # ---- helpers para progreso IA ----
    @staticmethod
    def _count_descompuestos_in_bc3(path: Path) -> int:
        total = 0
        with path.open("r", encoding="latin-1", errors="ignore") as fh:
            for raw in fh:
                if raw.startswith("~C|"):
                    parts = raw.rstrip("\n").split("|")
                    if len(parts) >= 7:
                        tipo = parts[6]
                        if tipo in {"1", "2", "3"}:
                            total += 1
        return total

    @staticmethod
    def _format_progress_event(ev: Any, idx: int, total: int) -> str:
        prefix = f"IA {idx}/{total} | "
        try:
            if isinstance(ev, str):
                return prefix + ev
            if isinstance(ev, dict):
                oldc = ev.get("old_code") or ev.get("code") or ev.get("from") or "?"
                newc = ev.get("new_code") or ev.get("mapped_code") or ev.get("to") or "?"
                conf = ev.get("confidence")
                if conf is None:
                    return f"{prefix}{oldc} → {newc}"
                return f"{prefix}{oldc} → {newc} ({float(conf):.2f})"
            if isinstance(ev, (tuple, list)) and len(ev) >= 2:
                oldc = ev[0]
                newc = ev[1]
                conf = ev[2] if len(ev) >= 3 else None
                if conf is None:
                    return f"{prefix}{oldc} → {newc}"
                return f"{prefix}{oldc} → {newc} ({float(conf):.2f})"
        except Exception:
            pass
        return prefix + repr(ev)

    def _run_phase2_thread(self, cleaned_bc3: Path) -> None:
        ok = True
        try:
            out_phase2 = cleaned_bc3.with_name(cleaned_bc3.stem + "_clasificado.bc3")
            total = self._count_descompuestos_in_bc3(cleaned_bc3)
            self._append_async(f"Detectados {total} descompuestos a procesar.")

            processed = 0

            def progress(ev: Any):
                nonlocal processed
                processed += 1
                line = self._format_progress_event(ev, processed, total if total else processed)
                self._append_async(line)

            self._append_async(f"Asignando productos → {out_phase2.name}")

            limits = self.model_limits
            used = False

            # --- kwargs base ---
            base_kwargs = {
                "input_bc3": cleaned_bc3,
                "catalog_path": self.catalog_path,
                "output_bc3": out_phase2,
                "model_name": self.model_name,
                "rpm_limit": limits["RPM"],
                "tpm_limit": limits["TPM"],
                "rpd_limit": limits["RPD"],
                # NUEVO: template REFCRU
                "refcru_template_xlsx": self.refcru_template_path,
                "emit_refcru_xlsx": True,
            }

            # Intentamos pasar callback con varios nombres habituales
            for pname in ("progress_cb", "on_progress", "progress_callback", "callback", "logger"):
                try:
                    kwargs = dict(base_kwargs)
                    kwargs[pname] = progress
                    run_phase2(**kwargs)  # type: ignore[misc]
                    used = True
                    break
                except TypeError:
                    # API antigua: no acepta alguno de los kwargs (ej. refcru_template_xlsx)
                    continue

            if not used:
                # Fallback sin callback (API antigua)
                try:
                    run_phase2(**base_kwargs)  # type: ignore[misc]
                    used = True
                except TypeError:
                    # Último fallback: llamada posicional antigua
                    run_phase2(cleaned_bc3, self.catalog_path, out_phase2)  # type: ignore[misc]

            self._append_async(f"Guardado: {out_phase2}")
            # Si template estaba, run_phase2 generará también _REFCRU.xlsx y lo logueará por callback/print

        except Exception as e:
            ok = False
            self._append_async(f"ERROR: {e}", tag="err")
        finally:
            self._enable_actions()
            self._append_banner_async("TERMINADO" if ok else "FAIL", ok=ok)

    # ---------------------------- validaciones ------------------------------ #
    def _ensure_input(self) -> bool:
        p = self.entry_input.get().strip()
        if not p:
            messagebox.showerror("Entrada", "Selecciona primero un fichero BC3.")
            return False
        self.input_path = Path(p)
        if not self.input_path.exists():
            messagebox.showerror("Entrada", f"No existe: {self.input_path}")
            return False
        if self.input_path.suffix.lower() != ".bc3":
            messagebox.showerror("Entrada", "El archivo debe ser *.bc3")
            return False
        return True

    def _ensure_catalog(self) -> bool:
        p = self.entry_catalog.get().strip()
        if not p:
            messagebox.showerror(
                "Catálogo",
                "No se ha encontrado el catálogo IA. Selecciónalo o colócalo en:\n"
                "  <carpeta_del_exe>/data/catalog/",
            )
            return False
        self.catalog_path = Path(p)
        if not self.catalog_path.exists():
            messagebox.showerror("Catálogo", f"No existe: {self.catalog_path}")
            return False
        if self.catalog_path.suffix.lower() not in {".xlsx", ".xls", ".csv"}:
            messagebox.showwarning("Catálogo", "Formato no estándar. Se esperan .xlsx/.xls/.csv")
        return True

    def _ensure_refcru_template_soft(self) -> None:
        """
        No bloquea: solo intenta registrar la ruta si existe.
        run_phase2 ya puede avisar si falta.
        """
        p = self.entry_refcru_template.get().strip()
        if not p:
            self.refcru_template_path = None
            return
        pp = Path(p)
        if pp.exists() and pp.suffix.lower() == ".xlsx":
            self.refcru_template_path = pp
        else:
            self.refcru_template_path = None

    def _cleaned_bc3_path(self) -> Path:
        src = self.input_path
        return src.with_name(f"{src.stem}_limpio.bc3")

    def _open_output_folder(self) -> None:
        target = self.last_output_dir or (self.input_path.parent if self.input_path else Path.cwd())
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(target))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                os.system(f'open "{target}"')
            else:
                os.system(f'xdg-open "{target}"')
        except Exception as e:
            messagebox.showerror("Abrir carpeta", f"No se pudo abrir la carpeta:\n{target}\n\n{e}")


# --------------------------------------------------------------------------- #
#  Entradas públicas                                                          #
# --------------------------------------------------------------------------- #
def run() -> None:
    app = App()
    app.mainloop()


def run_gui() -> None:
    """Alias para compatibilidad con main_gui.py."""
    run()


if __name__ == "__main__":
    run()
