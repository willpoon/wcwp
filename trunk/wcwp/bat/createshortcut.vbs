set shell = createobject("wscript.shell")
desktoppath = shell.specialfolders("desktop")
set link = shell.createshortcut(desktoppath & "\masm32 editor.lnk")
link.description = "masm32 editor"
link.targetpath = "c:\masm32\qeditor.exe"
link.windowstyle = 3
link.workingdirectory = "c:\masm32"
link.save


