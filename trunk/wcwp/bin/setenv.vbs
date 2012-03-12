'http://stackoverflow.com/questions/588321/can-system-environment-variables-be-set-via-windows-logon-scripts
'This approach yields variables that are immediately available via a CMD window. No reboot is required like the batch file registry writes.
' work in win7

Set WSHShell = WScript.CreateObject("WScript.Shell")
Set WshEnv = WshShell.Environment("SYSTEM")
if InstrRev(WshEnv("Path"),"wcwp") then
msgbox("wcwp already in path")
else
WshEnv("Path") = WshEnv("Path") & ";C:\poon\wcwp\wcwp\bin"
end if
'masm
if InstrRev(WshEnv("Path"),"masm615") then
msgbox("MASM615 already in path")
else
WshEnv("Path") = WshEnv("Path") & ";c:\poon\wcwp\wcwp\bin\masm615\bin"
end if

'masm sdk

if InstrRev(WshEnv("Path"),"masm32") then
msgbox("masm32 already in path")
else
WshEnv("Path") = WshEnv("Path") & ";c:\masm32\bin;c:\masm32\lib;c:\masm32\include"
end if
'masm sdk %include%
if InstrRev(WshEnv("include"),"masm32") then
msgbox("masm32\include already in %include%")
else
WshEnv("include") = "c:\masm32\include;" & WshEnv("include")
end if

'masm sdk %lib%
if InstrRev(WshEnv("lib"),"masm32") then
msgbox("masm32\lib already in %lib%")
else
WshEnv("lib") = "c:\masm32\lib;" & WshEnv("lib")
end if

'C:\poon\wcwp\wcwp\bin\nasm-2.09.10

if InstrRev(WshEnv("Path"),"nasm") then
msgbox("nasm already in %Path%")
else
WshEnv("Path") = "C:\poon\wcwp\wcwp\bin\nasm-2.09.10;" & WshEnv("Path")
end if

'c:\Dev-Cpp\bin
if InstrRev(WshEnv("Path"),"Dev-Cpp") then
msgbox("Dev-Cpp already in %Path%")
else
WshEnv("Path") = "c:\Dev-Cpp\bin;" & WshEnv("Path")
end if

'C:\poon\wcwp\wcwp\bin\radasm
if InstrRev(WshEnv("Path"),"radasm") then
msgbox("radasm already in %Path%")
else
WshEnv("Path") = "C:\poon\wcwp\wcwp\bin\radasm;" & WshEnv("Path")
end if

'java bin
if InstrRev(WshEnv("Path"),"jdk1.7") then
msgbox("jdk1.7 already in %Path%")
else
WshEnv("Path") = "C:\Program Files\Java\jdk1.7.0_01\bin;" & WshEnv("Path")
end if


'javaclasspath
if InstrRev(WshEnv("classpath"),"jdk1.7") then
msgbox("jdk1.7 already in %classpath%")
else
WshEnv("classpath") = "C:\Program Files\Java\jdk1.7.0_01\bin;" & WshEnv("classpath")
end if


'SET PATH=C:\poon\wcwp\wcwp\bin\masm615\BIN;C:\poon\wcwp\wcwp\bin\masm615\BINR;c:\winnt\system32
'SET LIB=C:\poon\wcwp\wcwp\bin\masm615\LIB
'SET INCLUDE=C:\poon\wcwp\wcwp\bin\masm615\INCLUDE
'SET INIT=E:\PROGRAMS
'SET HELPFILES=C:\poon\wcwp\wcwp\bin\masm615\HELP\*.HLP
'SET TMP=C:\WINDOWS\TEMP





