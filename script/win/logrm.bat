@echo off
setlocal
rem Remove the entire project log directory, only while the server is stopped.
rem Use --no-pause in automation; exit code 0 means success, 1 means failure.
set "MIXTURE_CONTROL_SCRIPT=%~f0"
powershell.exe -NoLogo -NoProfile -NonInteractive -Command "$source = [IO.File]::ReadAllText($env:MIXTURE_CONTROL_SCRIPT); & ([scriptblock]::Create(($source -split '(?m)^#__POWERSHELL__\r?$', 2)[1]))"
set "clean_result=%errorlevel%"
if /I "%~1"=="--no-pause" exit /b %clean_result%
pause
exit /b %clean_result%
#__POWERSHELL__
$ErrorActionPreference = 'Stop'
try {
    # Resolve from the script, never from the caller's current directory.
    $projectDir = [IO.Path]::GetFullPath((Join-Path (Split-Path -Parent $env:MIXTURE_CONTROL_SCRIPT) '..\..'))
    $logDir = [IO.Path]::GetFullPath((Join-Path $projectDir 'log'))
    if ((Split-Path -Parent $logDir) -ne $projectDir -or (Split-Path -Leaf $logDir) -ne 'log') {
        throw 'Unsafe log directory; nothing deleted.'
    }
    $exePath = Join-Path $projectDir 'bin\winMixture.exe'
    $candidates = @(Get-CimInstance Win32_Process -Filter "Name = 'winMixture.exe'")
    if (@($candidates | Where-Object { -not $_.ExecutablePath }).Count -gt 0) {
        throw 'Cannot verify an executable path. Run with the same privileges as the server.'
    }
    if (@($candidates | Where-Object { $_.ExecutablePath -eq $exePath }).Count -gt 0) {
        throw 'Project server is running. Stop it before deleting logs.'
    }
    if (-not (Test-Path -LiteralPath $logDir)) {
        Write-Host 'No log directory. Nothing to delete.'
        exit 0
    }
    # Reject links/junctions before recursive deletion; never traverse their targets.
    $pending = New-Object 'System.Collections.Generic.Stack[string]'
    $pending.Push($logDir)
    while ($pending.Count -gt 0) {
        $item = Get-Item -LiteralPath $pending.Pop() -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Link/junction detected: $($item.FullName). Nothing deleted."
        }
        if ($item.PSIsContainer) {
            foreach ($child in Get-ChildItem -LiteralPath $item.FullName -Force) {
                $pending.Push($child.FullName)
            }
        }
    }
    if (-not (Get-Item -LiteralPath $logDir -Force).PSIsContainer) {
        throw 'Expected a log directory, but found a file. Nothing deleted.'
    }
    Remove-Item -LiteralPath $logDir -Recurse -Force
    if (Test-Path -LiteralPath $logDir) { throw 'Log directory still exists.' }
    Write-Host "Clean Success: removed $logDir"
    exit 0
} catch {
    Write-Host "Clean Fail: $($_.Exception.Message)"
    exit 1
}
