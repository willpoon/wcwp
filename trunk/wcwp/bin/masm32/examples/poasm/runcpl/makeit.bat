@echo off

if exist runcpl.obj del runcpl.obj
if exist runcpl.exe del runcpl.exe

: -----------------------------------------
: assemble runcpl.asm into an OBJ file
: -----------------------------------------
\masm32\BIN\poasm /V2 runcpl.asm
if errorlevel 1 goto errasm

: -----------------------
: link the main OBJ file
: -----------------------
\masm32\BIN\PoLink.exe /SUBSYSTEM:WINDOWS /merge:.data=.text runcpl.obj > nul
if errorlevel 1 goto errlink
dir runcpl.*
goto TheEnd

:errlink
: ----------------------------------------------------
: display message if there is an error during linking
: ----------------------------------------------------
echo.
echo There has been an error while linking this project.
echo.
goto TheEnd

:errasm
: -----------------------------------------------------
: display message if there is an error during assembly
: -----------------------------------------------------
echo.
echo There has been an error while assembling this project.
echo.
goto TheEnd

:TheEnd

pause
