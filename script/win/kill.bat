@echo off
setlocal
rem Emergency forced termination only. Prefer stop.bat for normal shutdown.
rem Use --no-pause in automation; the exit code is 0 on success, 1 on failure.
set "MIXTURE_CONTROL_SCRIPT=%~f0"
powershell.exe -NoLogo -NoProfile -NonInteractive -Command "$source = [IO.File]::ReadAllText($env:MIXTURE_CONTROL_SCRIPT); & ([scriptblock]::Create(($source -split '(?m)^#__POWERSHELL__\r?$', 2)[1]))"
set "close_result=%errorlevel%"
if /I "%~1"=="--no-pause" exit /b %close_result%
pause
exit /b %close_result%
#__POWERSHELL__
$ErrorActionPreference = 'Stop'
try {
    # Force termination does not guarantee defer/ServerDown cleanup.
    $exePath = [IO.Path]::GetFullPath((Join-Path (Split-Path -Parent $env:MIXTURE_CONTROL_SCRIPT) '..\..\bin\winMixture.exe'))
    $candidates = @(Get-CimInstance Win32_Process -Filter "Name = 'winMixture.exe'")
    if (@($candidates | Where-Object { -not $_.ExecutablePath }).Count -gt 0) {
        throw 'Cannot verify an executable path. Run with the same privileges as the server.'
    }
    $targets = @($candidates | Where-Object { $_.ExecutablePath -eq $exePath })
    if ($targets.Count -eq 0) {
        Write-Host 'No matching project processes. Nothing to kill.'
        exit 0
    }
    $failed = $false
    foreach ($candidate in $targets) {
        try {
            $target = Get-Process -Id $candidate.ProcessId -ErrorAction SilentlyContinue
            if ($null -eq $target) { continue }
            if ($target.Path -ne $exePath) { throw 'Process identity changed; not terminated.' }
            $target.Kill()
            if (-not $target.WaitForExit(15000)) { throw 'Timed out waiting for forced termination.' }
            Write-Host "Close Success: PID $($candidate.ProcessId) forcefully terminated (cleanup not guaranteed)."
        } catch {
            $failed = $true
            Write-Host "Close Fail: PID $($candidate.ProcessId): $($_.Exception.Message)"
        }
    }
    if ($failed) { exit 1 }
    exit 0
} catch {
    Write-Host "Close Fail: $($_.Exception.Message)"
    exit 1
}
