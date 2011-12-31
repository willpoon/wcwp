#$language = "VBScript"
#$interface = "1.0"

crt.Screen.Synchronous = True

' This automatically generated script may need to be
' edited in order to work correctly.

Sub Main
	crt.Screen.Send "app" & chr(13)
	crt.Screen.WaitForString "Password: "
	crt.Screen.Send "app" & chr(13)
	crt.Screen.WaitForString "$ "
	crt.Screen.Send "ksh -o vi" & chr(13)
	crt.Screen.WaitForString "$ "
	crt.Screen.Send ". /bassapp/bihome/panzw/.profile" & chr(13)
	crt.Screen.WaitForString "$ "
	crt.Screen.Send ". /bassapp/bihome/panzw/config/cds" & chr(13)
	crt.Screen.WaitForString "$ "
	crt.Screen.Send "pzh" & chr(13)
End Sub
