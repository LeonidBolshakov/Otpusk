@echo off

cd /d "%~dp0"

rem === include centralized config ===
call "galaktika.cmd"
rem ==================================


%GALAKTIKA_EXE%\\asql.exe uchrabvr_update.lot /c:galaktika.cfg