#$language = "VBScript"
#$interface = "1.0"

crt.Screen.Synchronous = True

' This automatically generated script may need to be
' edited in order to work correctly.

Sub Main
	crt.Screen.Send "ifboss" & chr(13)
	crt.Screen.WaitForString "Password: "
	crt.Screen.Send "ifboss" & chr(13)
	crt.Screen.WaitForString "$ "
	crt.Screen.Send "ksh" & chr(13)
	crt.Screen.WaitForString "$ "
	crt.Screen.Send "set -o vi" & chr(13)
	crt.Screen.WaitForString "$ "
	crt.Screen.Send "stty erase '^H'" & chr(13)
	crt.Screen.WaitForString "$ "
	crt.Screen.Send "export LANG=zh" & chr(13)
	crt.Screen.WaitForString "$ "
	crt.Screen.Send "ls -lrt" & chr(13)
End Sub
