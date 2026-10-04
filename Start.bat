@echo off
cd /d "%~dp0"
where py >nul 2>nul
if %errorlevel% equ 0 (
  py -3 scripts\setup_wizard.py
) else (
  python scripts\setup_wizard.py
)
if errorlevel 1 pause
