@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "APP_URL=http://127.0.0.1:7448/"
set "APP_PORT=7448"
set "LOG_DIR=%~dp0logs"
set "LOG_FILE=%LOG_DIR%\refinerGUI-launch.log"

if defined R_SCRIPT (
  if exist "%R_SCRIPT%" goto have_rscript
)

set "R_SCRIPT=%ProgramFiles%\R\R-4.6.0\bin\Rscript.exe"
if exist "%R_SCRIPT%" goto have_rscript

for /f "delims=" %%I in ('where Rscript 2^>nul') do (
  set "R_SCRIPT=%%I"
  goto have_rscript
)

echo ERROR: Rscript was not found.
echo Install R 4.6.0, add Rscript to PATH, or set R_SCRIPT to a valid Rscript.exe path.
echo.
pause
exit /b 1

:have_rscript
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
pushd "%~dp0"

rem If something is already listening, only open it when it responds like refinerGUI.
netstat -ano | findstr ":%APP_PORT%" | findstr "LISTENING" >nul
if not errorlevel 1 (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r = Invoke-WebRequest -UseBasicParsing -TimeoutSec 3 '%APP_URL%'; if ($r.Content -match 'refineR GUI|refineR') { exit 0 } else { exit 2 } } catch { exit 1 }"
  if not errorlevel 1 (
    start "" "%APP_URL%"
    popd
    exit /b 0
  )
  echo ERROR: Port %APP_PORT% is already in use, but it does not look like refinerGUI.
  echo Close the process using this port or edit APP_PORT in this launcher.
  echo.
  pause
  popd
  exit /b 1
)

rem Launch app in a detached process so double-click works without keeping this window open.
echo Starting refinerGUI at %DATE% %TIME% > "%LOG_FILE%"
start "refinerGUI" /min cmd /c ""%R_SCRIPT%" -e "options(shiny.launch.browser = FALSE); shiny::runApp('.', host = '127.0.0.1', port = 7448)" >> "%LOG_FILE%" 2>&1"

set /a tries=0
:wait_for_app
set /a tries+=1
powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r = Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 '%APP_URL%'; if ($r.Content -match 'refineR GUI|refineR') { exit 0 } else { exit 2 } } catch { exit 1 }" >nul 2>nul
if not errorlevel 1 goto app_ready

if !tries! geq 20 goto app_timeout
timeout /t 1 /nobreak >nul
goto wait_for_app

:app_ready
start "" "%APP_URL%"
popd
exit /b 0

:app_timeout
echo ERROR: App did not start on port %APP_PORT% within 20 seconds.
echo If another app is using this port, close it and try again.
echo Startup log: %LOG_FILE%
echo.
pause
popd
exit /b 1
