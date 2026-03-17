# main_gui.py
from __future__ import annotations

from config.runtime_env import load_runtime_dotenv

load_runtime_dotenv()

from interface_adapters.gui.gui_app import run_gui


if __name__ == "__main__":
    run_gui()
