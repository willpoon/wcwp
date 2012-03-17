set CL_HOME=C:\poon\wcwp\wcwp\bin\vc6cmdline

echo off
cls
color 0A

set INCLUDE=%CL_HOME%/include
set LIB=%CL_HOME%/lib

set path=%CL_HOME%;%path%

call cmd
