@echo off
REM — switch to the script's directory
cd /d "%~dp0"

REM — activate the venv
call .venv13\Scripts\activate.bat

REM — run the Almanac app with correct port (8072) and debug=False
python runalmanac.py --port 8072 --no-debug