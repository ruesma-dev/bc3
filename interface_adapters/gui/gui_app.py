# interface_adapters/gui/gui_app.py
from __future__ import annotations

"""
GUI para BC3 ETL (Windows/Linux/Mac)
- Selector de fichero .bc3 (solo muestra *.bc3 / *.BC3)
- Botón grande "CONVERTIR"
- Barra de progreso indeterminada
- Área de log con banners finales: VERDE "TERMINADO" / ROJO "FAIL"
- Abre la carpeta del input con "Abrir output"
- Salida junto al input: <base>_limpio.bc3 y <base>_tree.csv
- Trabaja en directorio temporal (no ensucia el proyecto)
- Versión mostrada abajo a la derecha (v0.92)
- Carga robusta del logo PNG tanto en desarrollo como congelado (PyInstaller onefile/onedir)
"""

import io
import os
import queue
import shutil
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
import tkinter as tk
from tkinter import (
    filedialog,
    messagebox,
    Tk,
    StringVar,
    Text,
    END,
    DISABLED,
    NORMAL,
)
from tkinter import ttk

from interface_adapters.controllers.etl_controller import run_etl

VERSION = "0.93"

# Pillow opcional (para mejor reescalado del PNG)
try:
    from PIL import Image, ImageTk  # type: ignore
    _PIL_OK = True
except Exception:
    _PIL_OK = False


def _find_logo_path() -> Path | None:
    """
    Intenta localizar 'logo.png' en varios destinos:
      - Desarrollo: interface_adapters/gui/assets/logo.png (relativo a este archivo)
      - PyInstaller onefile: {sys._MEIPASS}/interface_adapters/gui/assets/logo.png
      - PyInstaller onedir: {carpeta del exe}/interface_adapters/gui/assets/logo.png
      - (fallback) .../assets/logo.png en las dos ubicaciones anteriores
    Devuelve la primera ruta existente o None.
    """
    candidates: list[Path] = []

    # 1) Desarrollo: junto al código fuente
    here = Path(__file__).parent
    candidates.append(here / "assets" / "logo.png")

    # 2) Congelado (onefile): carpeta temporal _MEIPASS
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        base = Path(meipass)
        candidates.append(base / "interface_adapters" / "gui" / "assets" / "logo.png")
        candidates.append(base / "assets" / "logo.png")

    # 3) Congelado (onedir): junto al ejecutable
    if getattr(sys, "frozen", False):
        exedir = Path(sys.executable).parent
        candidates.append(exedir / "interface_adapters" / "gui" / "assets" / "logo.png")
        candidates.append(exedir / "assets" / "logo.png")

    for p in candidates:
        if p.exists():
            return p
    return None


@dataclass
class AppPaths:
    project_root: Path
    input_dir: Path
    output_dir: Path

    @staticmethod
    def discover() -> "AppPaths":
        root = Path.cwd()
        return AppPaths(
            project_root=root,
            input_dir=root / "input",
            output_dir=root / "output",
        )


class StdCapture:
    """Captura stdout/stderr y lo envía a una cola para mostrar en la UI."""

    def __init__(self, q: queue.Queue[str], tee: bool = True):
        self.q = q
        self.buf = io.StringIO()
        self.tee = tee
        self.old_out = None
        the_old_err = None  # no se usa, pero mantenemos la estructura
        self.old_err = the_old_err

    def write(self, data: str) -> None:
        # buffer + encolar por líneas
        self.buf.write(data)
        for chunk in data.splitlines(True):
            self.q.put(chunk)
        if self.tee and self.old_out:
            self.old_out.write(data)

    def flush(self) -> None:
        if self.old_out:
            self.old_out.flush()

    def __enter__(self):
        self.old_out = sys.stdout
        self.old_err = sys.stderr
        sys.stdout = self
        sys.stderr = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self.old_out
        sys.stderr = self.old_err


class BC3GUI:
    def __init__(self, root: Tk):
        self.root = root
        self.root.title("BC3 ETL – Conversor")
        self.root.minsize(840, 560)

        # Paths internos (el ETL trabaja en tmp; no dejamos nada en el proyecto)
        self.paths = AppPaths.discover()
        self.paths.input_dir.mkdir(parents=True, exist_ok=True)
        self.paths.output_dir.mkdir(parents=True, exist_ok=True)

        # Estado
        self.selected_path = StringVar(value="")
        self.status_text = StringVar(value="Listo.")
        self.is_running = False
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.last_dest_dir: Path | None = None  # carpeta del input seleccionado
        self._logo_image = None  # mantener referencia para evitar GC
        self._logo_path: Path | None = _find_logo_path()

        # UI
        self._build_style()
        self._build_ui()

        # Poll logs
        self._poll_log_queue()

    # ---------- UI ----------

    def _build_style(self) -> None:
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Primary.TButton", font=("Segoe UI", 14, "bold"), padding=12)
        style.configure("Title.TLabel", font=("Segoe UI", 16, "bold"))
        style.configure("Status.TLabel", foreground="#555")

    def _build_ui(self) -> None:
        # Header con logo y título
        header = ttk.Frame(self.root, padding=(12, 10))
        header.pack(fill="x")

        logo_lbl = self._maybe_add_logo(header, height_px=42)
        if logo_lbl is not None:
            logo_lbl.pack(side="left", padx=(0, 10))

        ttk.Label(header, text="BC3 ETL – Conversor", style="Title.TLabel").pack(
            side="left"
        )

        # Top: selector de fichero
        top = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        top.pack(fill="x")

        ttk.Label(top, text="Fichero BC3:", width=12).pack(side="left")
        self.entry_path = ttk.Entry(top, textvariable=self.selected_path)
        self.entry_path.pack(side="left", fill="x", expand=True, padx=(0, 8))
        ttk.Button(top, text="Seleccionar…", command=self._on_browse).pack(
            side="left"
        )

        # Middle: acciones
        mid = ttk.Frame(self.root, padding=(12, 6))
        mid.pack(fill="x")

        self.btn_convert = ttk.Button(
            mid,
            text="CONVERTIR",
            style="Primary.TButton",
            command=self._on_convert_clicked,
        )
        self.btn_convert.pack(side="left")

        self.progress = ttk.Progressbar(mid, mode="indeterminate")
        self.progress.pack(side="left", fill="x", expand=True, padx=12)

        ttk.Button(mid, text="Abrir output", command=self._open_output).pack(
            side="right"
        )

        # Log area
        log_frame = ttk.LabelFrame(self.root, text="Ejecución", padding=8)
        log_frame.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        self.txt_log: Text = Text(log_frame, wrap="word", height=16)
        self.txt_log.pack(fill="both", expand=True)
        self.txt_log.configure(state=DISABLED)

        # Tags para banners de éxito / error y centrado
        self.txt_log.tag_configure(
            "success",
            foreground="#2e7d32",
            font=("Segoe UI", 20, "bold"),
            justify="center",
        )
        self.txt_log.tag_configure(
            "error",
            foreground="#c62828",
            font=("Segoe UI", 20, "bold"),
            justify="center",
        )
        self.txt_log.tag_configure("center", justify="center")

        # Status bar
        status = ttk.Frame(self.root, padding=(12, 6))
        status.pack(fill="x")
        self.lbl_status = ttk.Label(
            status, textvariable=self.status_text, style="Status.TLabel"
        )
        self.lbl_status.pack(side="left")
        ttk.Label(
            status, text=f"v{VERSION}", style="Status.TLabel", anchor="e", width=10
        ).pack(side="right")

    # ---------- Logo ----------

    def _maybe_add_logo(self, parent: ttk.Frame, height_px: int = 40):
        """
        Intenta cargar y mostrar un logo PNG. Devuelve el Label si se pudo crear; si no, None.
        """
        try:
            if not self._logo_path or not self._logo_path.exists():
                return None

            if _PIL_OK:
                img = Image.open(self._logo_path)
                # mantener relación de aspecto, reescalar a altura deseada
                if img.height != height_px:
                    ratio = height_px / max(1, img.height)
                    new_size = (max(1, int(img.width * ratio)), height_px)
                    img = img.resize(new_size, Image.LANCZOS)
                self._logo_image = ImageTk.PhotoImage(img)
            else:
                # Fallback sin PIL (Tk 8.6 soporta PNG)
                self._logo_image = tk.PhotoImage(file=str(self._logo_path))
                # Si es muy grande, hacer subsample entero aproximado
                h = self._logo_image.height()
                if h > height_px:
                    factor = max(1, round(h / height_px))
                    self._logo_image = self._logo_image.subsample(factor, factor)

            return ttk.Label(parent, image=self._logo_image)

        except Exception:
            return None

    # ---------- Actions ----------

    def _on_browse(self) -> None:
        # Solo mostrar *.bc3 / *.BC3
        fp = filedialog.askopenfilename(
            title="Selecciona un fichero BC3",
            filetypes=[("FIEBDC-3 / BC3", "*.bc3 *.BC3")],
            defaultextension=".bc3",
        )
        if fp:
            self.selected_path.set(fp)
            self.last_dest_dir = Path(fp).parent
            self._append_log(f"Seleccionado: {fp}\n")

    def _clear_log(self) -> None:
        """Limpia el área de logs y reinicia la cola de mensajes."""
        self.txt_log.configure(state=NORMAL)
        self.txt_log.delete("1.0", END)
        self.txt_log.configure(state=DISABLED)
        self.log_queue = queue.Queue()

    def _on_convert_clicked(self) -> None:
        if self.is_running:
            return
        src = self.selected_path.get().strip()
        if not src:
            messagebox.showwarning("BC3 ETL", "Selecciona primero un fichero .bc3")
            return
        if not Path(src).exists():
            messagebox.showerror("BC3 ETL", "La ruta seleccionada no existe.")
            return

        # Limpiar log antes de una nueva ejecución
        self._clear_log()

        # Lanzar en hilo para no bloquear la UI
        t = threading.Thread(target=self._run_pipeline_thread, args=(Path(src),), daemon=True)
        self.is_running = True
        self.progress.start(10)
        self.status_text.set("Procesando…")
        self.btn_convert.configure(state=DISABLED)
        t.start()

    def _open_output(self) -> None:
        try:
            # Abrir SIEMPRE la carpeta del input seleccionado
            dest_dir = self.last_dest_dir or (
                Path(self.selected_path.get()).parent if self.selected_path.get() else None
            )
            if not dest_dir:
                messagebox.showinfo(
                    "BC3 ETL",
                    "Selecciona primero un fichero para conocer su carpeta.",
                )
                return
            path = str(dest_dir.resolve())
            if sys.platform.startswith("win"):
                os.startfile(path)  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                os.system(f'open "{path}"')
            else:
                os.system(f'xdg-open "{path}"')
        except Exception as exc:
            messagebox.showinfo("BC3 ETL", f"No se pudo abrir la carpeta:\n{exc}")

    # ---------- Pipeline thread ----------

    def _run_pipeline_thread(self, source_path: Path) -> None:
        """
        Ejecuta el ETL en un directorio temporal aislado (con subcarpetas input/output),
        después copia los resultados a la misma carpeta del fichero seleccionado:
          - <base>_limpio.bc3
          - <base>_tree.csv
        """
        try:
            dest_dir = source_path.parent
            self.last_dest_dir = dest_dir
            base = source_path.stem

            with StdCapture(self.log_queue, tee=False):
                print(f"Origen: {source_path}")

                with tempfile.TemporaryDirectory(prefix="bc3gui_") as tmpdir:
                    tmp_root = Path(tmpdir)
                    tmp_input = tmp_root / "input"
                    tmp_output = tmp_root / "output"
                    tmp_input.mkdir(parents=True, exist_ok=True)
                    tmp_output.mkdir(parents=True, exist_ok=True)

                    # Copiamos al input temporal con el nombre que espera el ETL
                    tmp_bc3 = tmp_input / "presupuesto.bc3"
                    shutil.copyfile(source_path, tmp_bc3)
                    print(f"Trabajo temporal: {tmp_root}")

                    # Ejecutar ETL desde el tmp_root
                    old_cwd = Path.cwd()
                    try:
                        os.chdir(tmp_root)
                        start = time.time()
                        run_etl(input_filename=tmp_bc3.name)
                        elapsed = time.time() - start
                        print(f"✅ Conversión finalizada en {elapsed:.2f}s")
                    finally:
                        os.chdir(old_cwd)

                    # Outputs internos generados por el ETL
                    internal_bc3 = tmp_output / "presupuesto_material.bc3"
                    internal_csv = tmp_output / "presupuesto_tree.csv"

                    # Destino final (misma carpeta que el input seleccionado)
                    dest_bc3 = dest_dir / f"{base}_limpio.bc3"
                    dest_csv = dest_dir / f"{base}_tree.csv"

                    copied_any = False
                    if internal_bc3.exists():
                        shutil.copyfile(internal_bc3, dest_bc3)
                        print(f"Guardado: {dest_bc3}")
                        copied_any = True
                    else:
                        print("⚠ No se encontró 'presupuesto_material.bc3' en trabajo temporal.")

                    if internal_csv.exists():
                        shutil.copyfile(internal_csv, dest_csv)
                        print(f"Guardado: {dest_csv}")
                        copied_any = True
                    else:
                        print("⚠ No se encontró 'presupuesto_tree.csv' en trabajo temporal.")

                    if copied_any:
                        print(f"📁 Output: {dest_dir}")

            # Fin correcto → banner verde
            self.root.after(0, self._on_pipeline_done, True, None)

        except Exception as exc:
            # Error → banner rojo
            self.log_queue.put(f"\n[ERROR] {exc}\n")
            self.root.after(0, self._on_pipeline_done, False, exc)

    def _on_pipeline_done(self, ok: bool, exc: Exception | None) -> None:
        self.is_running = False
        self.progress.stop()
        self.btn_convert.configure(state=NORMAL)

        # Mostrar banner en el log
        if ok:
            self.status_text.set("Completado.")
            self._append_banner("TERMINADO", tag="success")
        else:
            self.status_text.set("Error en la conversión.")
            self._append_banner("FAIL", tag="error")
            messagebox.showerror("BC3 ETL", f"Ocurrió un error:\n{exc}")

    # ---------- Logging ----------

    def _append_log(self, text: str) -> None:
        self.txt_log.configure(state=NORMAL)
        self.txt_log.insert(END, text)
        self.txt_log.see(END)
        self.txt_log.configure(state=DISABLED)

    def _append_banner(self, text: str, tag: str) -> None:
        self.txt_log.configure(state=NORMAL)
        self.txt_log.insert(END, "\n\n")
        self.txt_log.insert(END, f"{text}\n", (tag, "center"))
        self.txt_log.see(END)
        self.txt_log.configure(state=DISABLED)

    def _poll_log_queue(self) -> None:
        try:
            while True:
                chunk = self.log_queue.get_nowait()
                self._append_log(chunk)
        except queue.Empty:
            pass
        self.root.after(60, self._poll_log_queue)


def run_gui() -> None:
    root = Tk()
    app = BC3GUI(root)
    root.mainloop()
