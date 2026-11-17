@echo off
setlocal
rem Send CTRL+C and wait per process; never fall back to forced termination.
rem Override MIXTURE_STOP_TIMEOUT_SECONDS (1-300); use --no-pause in automation.
set "MIXTURE_CONTROL_SCRIPT=%~f0"
if not defined MIXTURE_STOP_TIMEOUT_SECONDS set "MIXTURE_STOP_TIMEOUT_SECONDS=15"
powershell.exe -NoLogo -NoProfile -NonInteractive -Command "$source = [IO.File]::ReadAllText($env:MIXTURE_CONTROL_SCRIPT); & ([scriptblock]::Create(($source -split '(?m)^#__POWERSHELL__\r?$', 2)[1]))"
set "close_result=%errorlevel%"
if /I "%~1"=="--no-pause" exit /b %close_result%
pause
exit /b %close_result%
#__POWERSHELL__
$ErrorActionPreference = 'Stop'
try {
    # Only control this project's executable, regardless of the working directory.
    $exePath = [IO.Path]::GetFullPath((Join-Path (Split-Path -Parent $env:MIXTURE_CONTROL_SCRIPT) '..\..\bin\winMixture.exe'))
    $timeoutSeconds = 0
    if (-not [int]::TryParse($env:MIXTURE_STOP_TIMEOUT_SECONDS, [ref]$timeoutSeconds) -or $timeoutSeconds -lt 1 -or $timeoutSeconds -gt 300) {
        throw 'MIXTURE_STOP_TIMEOUT_SECONDS must be between 1 and 300.'
    }
    $candidates = @(Get-CimInstance Win32_Process -Filter "Name = 'winMixture.exe'")
    if (@($candidates | Where-Object { -not $_.ExecutablePath }).Count -gt 0) {
        throw 'Cannot verify an executable path. Run with the same privileges as the server.'
    }
    $targets = @($candidates | Where-Object { $_.ExecutablePath -eq $exePath })
    if ($targets.Count -eq 0) {
        Write-Host 'No matching project processes. Nothing to stop.'
        exit 0
    }
    # A helper attaches to the server console without detaching the batch window.
    # CTRL+C is console-wide, so shared consoles must be rejected before sending.
    $helper = @'
$ErrorActionPreference = 'Stop'
$status = 1
$message = ''
$attached = $false
try {
    Add-Type -TypeDefinition @"
using System.Runtime.InteropServices;
public static class MixtureConsole {
    [DllImport("kernel32.dll", SetLastError=true)] public static extern bool FreeConsole();
    [DllImport("kernel32.dll", SetLastError=true)] public static extern bool AttachConsole(uint processId);
    [DllImport("kernel32.dll", SetLastError=true)] public static extern bool SetConsoleCtrlHandler(System.IntPtr handler, bool add);
    [DllImport("kernel32.dll", SetLastError=true)] public static extern bool GenerateConsoleCtrlEvent(uint controlEvent, uint processGroupId);
    [DllImport("kernel32.dll", SetLastError=true)] public static extern uint GetConsoleProcessList([Out] uint[] processes, uint count);
}
"@
    $target = Get-Process -Id $config.ProcessID -ErrorAction SilentlyContinue
    if ($null -eq $target) {
        $message = 'Already exited.'
    } else {
        if ($target.Path -ne $config.ExePath -or $target.StartTime.ToUniversalTime().Ticks.ToString() -ne $config.StartTicks) {
            throw 'Process identity changed; no signal sent.'
        }
        [void][MixtureConsole]::FreeConsole()
        if (-not [MixtureConsole]::AttachConsole([uint32]$config.ProcessID)) {
            $nativeError = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
            throw "AttachConsole failed ($nativeError). Use Ctrl+C in the server console, or kill.bat for forced termination."
        }
        $attached = $true
        # AttachConsole resets handlers; ignore CTRL+C only after attaching.
        if (-not [MixtureConsole]::SetConsoleCtrlHandler([IntPtr]::Zero, $true)) {
            throw 'Cannot protect the signal helper from CTRL+C.'
        }
        $consoleProcesses = New-Object uint32[] 64
        $count = [MixtureConsole]::GetConsoleProcessList($consoleProcesses, 64)
        if ($count -eq 0 -or $count -gt 64) { throw 'Cannot safely enumerate the target console.' }
        for ($index = 0; $index -lt $count; $index++) {
            if ($consoleProcesses[$index] -ne $PID -and $consoleProcesses[$index] -ne $config.ProcessID) {
                throw 'Shared console detected; no signal sent. Use Ctrl+C in that console.'
            }
        }
        if (-not [MixtureConsole]::GenerateConsoleCtrlEvent(0, 0)) { throw 'GenerateConsoleCtrlEvent failed.' }
        Start-Sleep -Milliseconds 200
        $message = 'CTRL+C sent.'
    }
    $status = 0
} catch {
    $message = $_.Exception.Message
} finally {
    if ($attached) { [void][MixtureConsole]::FreeConsole() }
    [IO.File]::WriteAllText($config.ResultFile, $message)
}
exit $status
'@
    $failed = $false
    foreach ($candidate in $targets) {
        $resultFile = [IO.Path]::GetTempFileName()
        try {
            $target = Get-Process -Id $candidate.ProcessId -ErrorAction SilentlyContinue
            if ($null -eq $target) { continue }
            if ($target.Path -ne $exePath) { throw 'Process identity changed; no signal sent.' }
            $config = @{
                ProcessID = $target.Id
                ExePath = $exePath
                StartTicks = $target.StartTime.ToUniversalTime().Ticks.ToString()
                ResultFile = $resultFile
            } | ConvertTo-Json -Compress
            $encodedConfig = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($config))
            $command = '$config = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String("' + $encodedConfig + '")) | ConvertFrom-Json' + "`n" + $helper
            $encodedCommand = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($command))
            $sender = Start-Process -FilePath "$PSHOME\powershell.exe" -ArgumentList @('-NoLogo', '-NoProfile', '-NonInteractive', '-EncodedCommand', $encodedCommand) -WindowStyle Hidden -PassThru -Wait
            if ($sender.ExitCode -ne 0) { throw ([IO.File]::ReadAllText($resultFile)) }
            if (-not $target.WaitForExit($timeoutSeconds * 1000)) {
                throw "Timed out after $timeoutSeconds seconds. Process was NOT force-killed; use kill.bat if needed."
            }
            Write-Host "Close Success: PID $($candidate.ProcessId) exited after stop request."
        } catch {
            $failed = $true
            Write-Host "Close Fail: PID $($candidate.ProcessId): $($_.Exception.Message)"
        } finally {
            Remove-Item -LiteralPath $resultFile -ErrorAction SilentlyContinue
        }
    }
    if ($failed) { exit 1 }
    exit 0
} catch {
    Write-Host "Close Fail: $($_.Exception.Message)"
    exit 1
}
