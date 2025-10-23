@echo off
cd /d "%~dp0"
set PYTHONPATH=%CD%

python SRC\otpusk.py
pause