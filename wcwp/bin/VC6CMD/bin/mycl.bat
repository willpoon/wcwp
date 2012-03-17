@echo off

set CLDir=C:\poon\wcwp\wcwp\bin\VC6CMD

PATH=%CLDir%\BIN;
set INCLUDE=%CLDir%\ATL\INCLUDE;%CLDir%\INCLUDE;%CLDir%\MFC\INCLUDE
set LIB=%CLDir%\LIB;%CLDir%\MFC\LIB

set CLDir=


echo.
path
echo.
set include
echo.
set lib



cl /DEFAULTLIB kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib  %1
del *.obj > nul 2>&1 
del *.idb > nul 2>&1 
del *.pdb > nul 2>&1
