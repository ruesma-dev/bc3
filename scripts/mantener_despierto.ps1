# scripts/mantener_despierto.ps1
# Impide que Windows suspenda o hiberne el equipo MIENTRAS esta ventana siga
# abierta. Pensado para no cortar ejecuciones largas del arnes (cargas
# iniciales, subagentes, despliegues).
#
# Uso:
#   powershell -ExecutionPolicy Bypass -File .\scripts\mantener_despierto.ps1
#   powershell -ExecutionPolicy Bypass -File .\scripts\mantener_despierto.ps1 -ConPantalla
#   powershell -ExecutionPolicy Bypass -File .\scripts\mantener_despierto.ps1 -Prueba
#
# Ctrl+C, o cerrar la ventana, devuelve el comportamiento normal de energia.
#
# COMO FUNCIONA: SetThreadExecutionState marca un requisito de energia ligado
# AL HILO que hace la llamada. Por eso hace falta un proceso vivo y no sirve
# lanzarlo desde init.sh, que arranca y termina. Al morir el proceso, Windows
# libera el requisito solo: cerrar la ventana con la X es seguro aunque el
# bloque finally no llegue a ejecutarse.
#
# LO QUE ESTO NO CUBRE (que nadie se lleve una sorpresa a mitad de una carga):
#   - Cerrar la tapa de un portatil, si la accion configurada es suspender.
#   - Una directiva de grupo que fuerce la suspension.
#   - Hibernacion por bateria critica.
#
# Verificacion: `powercfg /requests` muestra la peticion activa. Requiere
# permisos de administrador; sin ellos el script avisa y sigue funcionando.

[CmdletBinding()]
param(
    # Mantiene tambien la pantalla encendida. Por defecto la pantalla puede
    # apagarse: el equipo sigue trabajando y no se gasta de mas.
    [switch]$ConPantalla,
    # Comprueba que la llamada funciona y sale. No deja nada activo.
    [switch]$Prueba,
    # Identificador de la sesion que lo arranca. Con el se escribe un fichero
    # PID en TEMP para poder pararlo despues. Lo usa el hook del arnes; a mano
    # no hace falta. Cada sesion tiene su propio guardian: asi dos sesiones a
    # la vez no se pisan y cerrar una no desprotege a la otra.
    [string]$SesionId
)

$ErrorActionPreference = 'Stop'

Add-Type -Name Power -Namespace Win32 -MemberDefinition @'
[DllImport("kernel32.dll", SetLastError = true)]
public static extern uint SetThreadExecutionState(uint esFlags);
'@

# OJO, dos trampas de PowerShell 5.1 y las dos dan el mismo error confuso
# ("no se puede convertir -2147483648 al tipo System.UInt32"):
#   1. El literal 0x80000000 se parsea como Int32, o sea -2147483648. Hay que
#      escribirlo en decimal para que sea Int64 y quepa sin signo.
#   2. -bor sobre [uint32] promociona a entero CON SIGNO. Por eso las banderas
#      se combinan en 64 bits y se convierten a UInt32 al final.
# Verificado: sistema = 2147483649, sistema+pantalla = 2147483651.
$ES_CONTINUOUS       = [uint32]2147483648   # 0x80000000
$ES_SYSTEM_REQUIRED  = [uint32]1            # 0x00000001
$ES_DISPLAY_REQUIRED = [uint32]2            # 0x00000002

$acumulado = [int64]$ES_CONTINUOUS -bor [int64]$ES_SYSTEM_REQUIRED
if ($ConPantalla) { $acumulado = $acumulado -bor [int64]$ES_DISPLAY_REQUIRED }
$flags = [uint32]$acumulado

function Mostrar-Peticiones {
    # powercfg /requests necesita administrador. Sin permisos no es un fallo:
    # el requisito de energia sigue activo, solo que no podemos ensenarlo.
    try {
        $salida = & powercfg /requests 2>&1 | Out-String
        if ($salida -match 'privilegio|privilege|denegado|denied') {
            Write-Host "  (no puedo mostrar 'powercfg /requests': necesita administrador)" -ForegroundColor DarkGray
            return
        }
        $bloque = ($salida -split "`r?`n") | Where-Object { $_ -match '^\s*\S' }
        $sistema = $false
        foreach ($linea in $bloque) {
            if ($linea -match '^SYSTEM') { $sistema = $true; continue }
            if ($sistema) {
                if ($linea -match '^[A-Z]+:?$') { break }
                Write-Host "  powercfg SYSTEM -> $($linea.Trim())" -ForegroundColor DarkGray
                break
            }
        }
    } catch {
        Write-Host "  (no puedo consultar powercfg: $($_.Exception.Message))" -ForegroundColor DarkGray
    }
}

$anterior = [Win32.Power]::SetThreadExecutionState($flags)
if ($anterior -eq 0) {
    Write-Error "SetThreadExecutionState fallo (codigo $([Runtime.InteropServices.Marshal]::GetLastWin32Error())). El equipo NO esta protegido."
    exit 1
}

$que = if ($ConPantalla) { "equipo y pantalla" } else { "equipo (la pantalla puede apagarse)" }

if ($Prueba) {
    Write-Host "PRUEBA: requisito de energia activado sobre $que." -ForegroundColor Cyan
    Mostrar-Peticiones
    [Win32.Power]::SetThreadExecutionState($ES_CONTINUOUS) | Out-Null
    Write-Host "PRUEBA: restaurado. No queda nada activo." -ForegroundColor Cyan
    exit 0
}

$inicio = Get-Date
$ficheroPid = $null
if ($SesionId) {
    $ficheroPid = Join-Path $env:TEMP ("claude_despierto_{0}.pid" -f $SesionId)
    Set-Content -Path $ficheroPid -Value $PID -Encoding ascii
}

Write-Host ""
Write-Host "Manteniendo despierto el $que." -ForegroundColor Green
Write-Host "Deja esta ventana abierta. Ctrl+C para terminar." -ForegroundColor Green
Mostrar-Peticiones
Write-Host ""

try {
    while ($true) {
        Start-Sleep -Seconds 300
        $t = (Get-Date) - $inicio
        $transcurrido = '{0:00}h {1:00}m' -f [int]$t.TotalHours, $t.Minutes
        Write-Host ("[{0}] Despierto desde hace {1}." -f (Get-Date -Format 'HH:mm'), $transcurrido)
    }
}
finally {
    [Win32.Power]::SetThreadExecutionState($ES_CONTINUOUS) | Out-Null
    if ($ficheroPid -and (Test-Path $ficheroPid)) {
        Remove-Item $ficheroPid -Force -ErrorAction SilentlyContinue
    }
    $t = (Get-Date) - $inicio
    $transcurrido = '{0:00}h {1:00}m' -f [int]$t.TotalHours, $t.Minutes
    Write-Host ""
    Write-Host ("Restaurado el comportamiento normal de energia. Total: {0}." -f $transcurrido) -ForegroundColor Yellow
}
