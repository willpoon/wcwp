@echo off
rem d:\masm32\bin\rc /v rsrc.rc
rem d:\masm32\bin\cvtres /machine:ix86 rsrc.res

\masm32\bin\ml /c /coff /Cp TDITBHook.asm
\masm32\bin\Link /SECTION:.bss,S  /DLL /DEF:TDITBHook.def /SUBSYSTEM:WINDOWS /LIBPATH:\masm32\lib TDITBHook.obj

copy tditbhook.dll ..

pause