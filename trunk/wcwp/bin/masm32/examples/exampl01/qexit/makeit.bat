@echo off

if exist qexit.obj del qexit.obj
if exist qexit.exe del qexit.exe

\masm32\bin\ml /c /coff /nologo qexit.asm
\masm32\bin\Link /SUBSYSTEM:WINDOWS /MERGE:.rdata=.text qexit.obj > nul

dir qexit.*

pause
