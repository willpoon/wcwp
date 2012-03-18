@echo off

if not exist rsrc.rc goto over1
\masm32\bin\rc /v rsrc.rc
\masm32\bin\cvtres /machine:ix86 rsrc.res
:over1
 
if exist "fda2.obj" del "fda2.obj"
if exist "fda2.exe" del "fda2.exe"

\masm32\bin\ml /c /coff "fda2.asm"
if errorlevel 1 goto errasm

if not exist rsrc.obj goto nores

\masm32\bin\PoLink /SUBSYSTEM:WINDOWS /merge:.data=.text "fda2.obj" rsrc.res > nul
if errorlevel 1 goto errlink

dir "fda2.*"
goto TheEnd

:nores
\masm32\bin\PoLink /SUBSYSTEM:WINDOWS /merge:.data=.text "fda2.obj" > nul
if errorlevel 1 goto errlink
dir "fda2.*"
goto TheEnd

:errlink
echo _
echo Link error
goto TheEnd

:errasm
echo _
echo Assembly Error
goto TheEnd

:TheEnd
 
pause
