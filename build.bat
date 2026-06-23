@echo off
REM ====================================================================
REM  build.bat - compile dupcleaner.exe from source (Windows)
REM  Usage:  build.bat
REM  Output: dupcleaner.exe in this folder
REM ====================================================================
setlocal

where go >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Go is not installed or not on PATH.
    echo         Install it from https://go.dev/dl/ and re-run this script.
    exit /b 1
)

echo [1/2] Building dupcleaner.exe ...
go build -o dupcleaner.exe .
if errorlevel 1 (
    echo [ERROR] Build failed - see the messages above.
    exit /b 1
)

for %%I in (dupcleaner.exe) do set "SIZE=%%~zI"
echo [2/2] Done. Built dupcleaner.exe (%SIZE% bytes).
echo.
echo Run it:              dupcleaner.exe
echo Run with debug logs: dupcleaner.exe        ^(debug is the default^)
echo Run quietly:         set DUPCLEANER_LOG_LEVEL=info ^&^& dupcleaner.exe
endlocal
