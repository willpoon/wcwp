'http://stackoverflow.com/questions/588321/can-system-environment-variables-be-set-via-windows-logon-scripts
'This approach yields variables that are immediately available via a CMD window. No reboot is required like the batch file registry writes.
' work in win7
'LIB 似乎不能用同样的方法设置，需要到系统-环境变量中设置

dim WCWP_PATH
WCWP_PATH="C:\poon\wcwp\wcwp\bin"

Set WSHShell = WScript.CreateObject("WScript.Shell")
Set WshEnv = WshShell.Environment("SYSTEM")
'~ WshEnv("Path")="c:\windows\system32\windowspowershell\v1.0\;c:\windows\system32\wbem;c:\windows\system32;c:\windows;c:\progra~1\ibm\sqllib\function;c:\progra~1\ibm\sqllib\bin;c:\program files\windows7master;c:\program files\tortoisesvn\bin;c:\program files\tencent\qqpcmgr\6.6.2136.201;c:\program files\mysql\mysql server 5.5\bin;c:\program files\microsoft visual studio\vc98\bin;c:\program files\microsoft visual studio\common\tools\winnt;c:\program files\microsoft visual studio\common\tools;c:\program files\microsoft visual studio\common\msdev98\bin;c:\program files\lua\5.1\clibs;c:\program files\lua\5.1;c:\program files\java\jdk1.7.0_01\bin;c:\program files\dell\dw wlan card;c:\poon\wcwp\wcwp\bin\radasm;c:\poon\wcwp\wcwp\bin\nasm-2.09.10;c:\poon\wcwp\wcwp\bin\masm615\bin;c:\poon\wcwp\wcwp\bin;c:\ora10instantclient;c:\masm32\lib;c:\masm32\include;c:\masm32\bin;c:\dev-cpp\bin"

'~ WshEnv("LIB")="c:\masm32\lib;C:\Program Files\Microsoft Visual Studio\VC98\mfc\lib;C:\Program Files\Microsoft Visual Studio\VC98\lib"

msgbox(WshEnv("LIB"))
msgbox(WshEnv("path"))


if InstrRev(WshEnv("Path"),"wcwp") then
'msgbox("wcwp already in path")
else
WshEnv("Path") = WshEnv("Path") & ";" & WCWP_PATH
end if


'2012-03-17
'C:\poon\wcwp\wcwp\bin\VC6CMD
if InstrRev(WshEnv("Path"),"VC6CMD") then
'msgbox("VC6CMD already in path")
else
WshEnv("Path") = WshEnv("Path") & ";" & WCWP_PATH & "\VC6CMD\bin"
end if


'masm
if InstrRev(WshEnv("Path"),"masm615") then
'msgbox("MASM615 already in path")
else
WshEnv("Path") = WshEnv("Path") & ";" & WCWP_PATH & "\masm615\bin"
end if

'masm sdk

if InstrRev(WshEnv("Path"),"masm32") then
'msgbox("masm32 already in path")
else
WshEnv("Path") = WshEnv("Path") & ";c:\masm32\bin"
end if
'masm sdk %include%
if InstrRev(WshEnv("include"),"masm32") then
'msgbox("masm32\include already in %include%")
else
WshEnv("include") = "c:\masm32\include;" & WshEnv("include")
end if

'masm sdk %lib%
if InstrRev(WshEnv("lib"),"masm32") then
'msgbox("masm32\lib already in %lib%")
else
WshEnv("lib") = "c:\masm32\lib;" & WshEnv("lib")
end if

'WCWP_PATH\nasm-2.09.10

if InstrRev(WshEnv("Path"),"nasm") then
'msgbox("nasm already in %Path%")
else
WshEnv("Path") = WCWP_PATH & "\nasm-2.09.10;" & WshEnv("Path")
end if

'c:\Dev-Cpp\bin
if InstrRev(WshEnv("Path"),"Dev-Cpp") then
'msgbox("Dev-Cpp already in %Path%")
else
WshEnv("Path") = "c:\Dev-Cpp\bin;" & WshEnv("Path")
end if

'WCWP_PATH\radasm
if InstrRev(WshEnv("Path"),"radasm") then
'msgbox("radasm already in %Path%")
else
WshEnv("Path") = WCWP_PATH & "\radasm;" & WshEnv("Path")
end if

'java bin
if InstrRev(WshEnv("Path"),"jdk1.7") then
'msgbox("jdk1.7 already in %Path%")
else
WshEnv("Path") = "C:\Program Files\Java\jdk1.7.0_01\bin;" & WshEnv("Path")
end if


'javaclasspath
if InstrRev(WshEnv("classpath"),"jdk1.7") then
'msgbox("jdk1.7 already in %classpath%")
else
WshEnv("classpath") = "C:\Program Files\Java\jdk1.7.0_01\bin;" & WshEnv("classpath")
end if

msgbox(WshEnv("path"))