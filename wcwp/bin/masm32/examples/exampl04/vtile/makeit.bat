@echo off

if not exist rsrc.rc goto over1
\MASM32\BIN\Rc.exe /v rsrc.rc
\MASM32\BIN\Cvtres.exe /machine:ix86 rsrc.res
:over1

if exist %1.obj del vtile.obj
if exist %1.exe del vtile.exe

\MASM32\BIN\Ml.exe /c /coff vtile.asm
if errorlevel 1 goto errasm

if not exist rsrc.obj goto nores

\MASM32\BIN\Link.exe /SUBSYSTEM:WINDOWS vtile.obj rsrc.obj
if errorlevel 1 goto errlink

dir vtile.*
goto TheEnd

:nores
\MASM32\BIN\Link.exe /SUBSYSTEM:WINDOWS vtile.obj
if errorlevel 1 goto errlink
dir %1
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

