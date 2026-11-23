@echo off
setlocal
cd /d "%~dp0.."

where py >nul 2>&1
if %errorlevel%==0 (set PY=py -3) else (set PY=python)

start "broker" cmd /k %PY% src\broker.py
timeout /t 2 /nobreak >nul

start "ResponseTeam" cmd /k %PY% src\subscriber.py --as ResponseTeam
start "PublicShield" cmd /k %PY% src\subscriber.py --as PublicShield
start "ResearchArchive" cmd /k %PY% src\subscriber.py --as ResearchArchive

start "SightingReports" cmd /k %PY% src\publisher.py --as SightingReports
start "PoltergeistActivity" cmd /k %PY% src\publisher.py --as PoltergeistActivity
start "ParanormalReadings" cmd /k %PY% src\publisher.py --as ParanormalReadings

timeout /t 2 /nobreak >nul
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0windows-demo.ps1"

endlocal
