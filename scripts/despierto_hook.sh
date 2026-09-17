#!/usr/bin/env bash
# scripts/despierto_hook.sh
#
# Envoltorio de los hooks SessionStart / SessionEnd del arnés: arranca y para
# el guardián que impide que Windows suspenda el equipo mientras hay una
# sesión de Claude Code trabajando.
#
# Uso (lo invoca .claude/settings.json, no hace falta llamarlo a mano):
#   echo "$JSON_DEL_HOOK" | scripts/despierto_hook.sh arrancar
#   echo "$JSON_DEL_HOOK" | scripts/despierto_hook.sh parar
#
# Lee el `session_id` del JSON que el hook entrega por stdin y lo usa para
# etiquetar el proceso: cada sesión gestiona SU guardián. Así dos sesiones
# abiertas a la vez no se pisan, y cerrar una no desprotege a la otra.
#
# Nunca falla de forma ruidosa: si algo no está en su sitio, sale con 0 y en
# silencio. Un hook que rompe el arranque de la sesión es peor que un equipo
# que se suspende.

set -u

accion="${1:-}"
entrada="$(cat 2>/dev/null || true)"

# session_id del JSON del hook. Sin jq, que no está garantizado en Windows.
sesion="$(printf '%s' "$entrada" \
    | sed -n 's/.*"session_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
    | head -1)"
[ -z "$sesion" ] && sesion="sin-sesion"

# Solo caracteres seguros: el identificador acaba en un nombre de fichero.
sesion="$(printf '%s' "$sesion" | tr -c 'A-Za-z0-9._-' '_')"

dir_scripts="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ps1="$dir_scripts/mantener_despierto.ps1"
[ -f "$ps1" ] || exit 0

temp_win="$(printf '%s' "${TEMP:-${TMP:-/tmp}}")"
temp_unix="$(cygpath -u "$temp_win" 2>/dev/null || printf '%s' "$temp_win")"
fichero_pid="$temp_unix/claude_despierto_${sesion}.pid"

case "$accion" in
  arrancar)
    # Idempotente: si ya hay un guardián vivo de esta sesión, no arranca otro.
    if [ -f "$fichero_pid" ]; then
        vpid="$(tr -d '[:space:]' < "$fichero_pid" 2>/dev/null || true)"
        if [ -n "$vpid" ] && powershell -NoProfile -Command "exit (Get-Process -Id $vpid -ErrorAction SilentlyContinue) -eq \$null" >/dev/null 2>&1; then
            exit 0
        fi
        rm -f "$fichero_pid"
    fi
    ps1_win="$(cygpath -w "$ps1" 2>/dev/null || printf '%s' "$ps1")"
    powershell -NoProfile -ExecutionPolicy Bypass -Command \
      "Start-Process -FilePath 'powershell' -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File','$ps1_win','-SesionId','$sesion' -WindowStyle Hidden" \
      >/dev/null 2>&1 || true
    ;;
  parar)
    [ -f "$fichero_pid" ] || exit 0
    vpid="$(tr -d '[:space:]' < "$fichero_pid" 2>/dev/null || true)"
    if [ -n "$vpid" ]; then
        powershell -NoProfile -Command "Stop-Process -Id $vpid -Force -ErrorAction SilentlyContinue" >/dev/null 2>&1 || true
    fi
    rm -f "$fichero_pid"
    ;;
  *)
    ;;
esac

exit 0
