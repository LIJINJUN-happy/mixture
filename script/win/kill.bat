@echo off
setlocal enabledelayedexpansion

call :close_process winMixture.exe
pause
exit

:close_process
set process_name=%~1
taskkill /IM %process_name% 2>NUL
if %errorlevel% == 0 (
    echo Close Success
    timeout /t 3
    exit
) else (
    echo Close Fail: %errorlevel%
    pause
)
goto :eof