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


'SET PATH=C:\poon\wcwp\wcwp\bin\masm615\BIN;C:\poon\wcwp\wcwp\bin\masm615\BINR;c:\winnt\system32
'SET LIB=C:\poon\wcwp\wcwp\bin\masm615\LIB
'SET INCLUDE=C:\poon\wcwp\wcwp\bin\masm615\INCLUDE
'SET INIT=E:\PROGRAMS
'SET HELPFILES=C:\poon\wcwp\wcwp\bin\masm615\HELP\*.HLP
'SET TMP=C:\WINDOWS\TEMP





