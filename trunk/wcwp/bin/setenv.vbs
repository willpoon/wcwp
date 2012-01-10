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