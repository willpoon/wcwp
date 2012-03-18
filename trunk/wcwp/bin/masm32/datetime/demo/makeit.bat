@echo off
\masm32\bin\ml /c /coff "WorldTimes.asm"
\masm32\bin\link /SUBSYSTEM:CONSOLE /OUT:WorldTimes.exe WorldTimes.obj 
del *.obj
dir WorldTimes.*
pause
