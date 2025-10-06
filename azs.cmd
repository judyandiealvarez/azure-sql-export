@echo off
REM Windows launcher for azs unified CLI
REM Prefer installed entry point if available; otherwise use module

REM Prefer installed azs on PATH, but avoid infinite recursion with this script
set AZS_FOUND=
for /f "usebackq delims=" %%I in (`where azs 2^>nul`) do (
  if /i not "%%~fI"=="%~f0" set AZS_FOUND=%%~fI
)
if not defined AZS_FOUND (
  for /f "usebackq delims=" %%I in (`where azs.exe 2^>nul`) do (
    set AZS_FOUND=%%~fI
  )
)
if defined AZS_FOUND (
  "%AZS_FOUND%" %*
  exit /b %ERRORLEVEL%
)

REM Use Python to run the CLI module directly; ensure deps installed
setlocal ENABLEDELAYEDEXPANSION
set PY=
where py >nul 2>nul && set PY=py -3
if not defined PY where python >nul 2>nul && set PY=python
if not defined PY where python3 >nul 2>nul && set PY=python3
if not defined PY (
  echo Python interpreter not found in PATH 1>&2
  exit /b 127
)

for %%M in (yaml pandas flask pytds sqlparse sqlglot werkzeug) do (
  %PY% -c "import %%M" 1>nul 2>nul || set NEED_INSTALL=1
)

if defined NEED_INSTALL (
  echo [azs] Dependencies missing. Attempting to install from requirements.txt...
  if exist requirements.txt (
    %PY% -m pip install -r requirements.txt --disable-pip-version-check || (
      echo [azs] System install failed (PEP 668 or permissions). Trying --user...
      %PY% -m pip install --user -r requirements.txt --disable-pip-version-check
    )
  ) else (
    echo [azs] requirements.txt not found 1>&2
  )
)

REM Re-check; if still missing, offer creating a venv
set NEED_INSTALL=
for %%M in (yaml pandas flask pytds sqlparse sqlglot werkzeug) do (
  %PY% -c "import %%M" 1>nul 2>nul || set NEED_INSTALL=1
)

if defined NEED_INSTALL (
  set /p ANSWER=[azs] Create a local virtual environment (.venv) and install deps? [Y/n] 
  if /i "%ANSWER%"=="" set ANSWER=Y
  if /i not "%ANSWER%"=="N" if /i not "%ANSWER%"=="No" (
    echo [azs] Creating .venv and installing deps...
    %PY% -m venv .venv
    if %ERRORLEVEL% NEQ 0 goto after_venv
    call .venv\Scripts\activate
    if %ERRORLEVEL% NEQ 0 goto after_venv
    .venv\Scripts\python -m pip install -U pip
    if %ERRORLEVEL% NEQ 0 goto after_venv
    .venv\Scripts\python -m pip install -r requirements.txt
    if %ERRORLEVEL% EQU 0 (
      set PY=.venv\Scripts\python
      echo [azs] Using virtual environment at .venv
    )
    :after_venv
  ) else (
    echo [azs] Skipping venv creation. You may run: %PY% -m pip install -r requirements.txt
  )
)

%PY% -m pyazs.cli %*
exit /b %ERRORLEVEL%


