'http://stackoverflow.com/questions/588321/can-system-environment-variables-be-set-via-windows-logon-scripts
'This approach yields variables that are immediately available via a CMD window. No reboot is required like the batch file registry writes.
' work in win7
'LIB 似乎不能用同样的方法设置，需要到系统-环境变量中设置

dim WCWP_PATH
WCWP_PATH="C:\poon\wcwp\wcwp\bin"
MINGW_BIN="D:\Soft_remove\CodeBlocks\MinGW\bin"
Set WSHShell = WScript.CreateObject("WScript.Shell")
Set WshEnv = WshShell.Environment("SYSTEM")
'~ WshEnv("Path")="c:\windows\system32\windowspowershell\v1.0\;c:\windows\system32\wbem;c:\windows\system32;c:\windows;c:\progra~1\ibm\sqllib\function;c:\progra~1\ibm\sqllib\bin;c:\program files\windows7master;c:\program files\tortoisesvn\bin;c:\program files\tencent\qqpcmgr\6.6.2136.201;c:\program files\mysql\mysql server 5.5\bin;c:\program files\microsoft visual studio\vc98\bin;c:\program files\microsoft visual studio\common\tools\winnt;c:\program files\microsoft visual studio\common\tools;c:\program files\microsoft visual studio\common\msdev98\bin;c:\program files\lua\5.1\clibs;c:\program files\lua\5.1;c:\program files\java\jdk1.7.0_01\bin;c:\program files\dell\dw wlan card;c:\poon\wcwp\wcwp\bin\radasm;c:\poon\wcwp\wcwp\bin\nasm-2.09.10;c:\poon\wcwp\wcwp\bin\masm615\bin;c:\poon\wcwp\wcwp\bin;c:\ora10instantclient;c:\masm32\lib;c:\masm32\include;c:\masm32\bin;c:\dev-cpp\bin"

'~ WshEnv("LIB")="c:\masm32\lib;C:\Program Files\Microsoft Visual Studio\VC98\mfc\lib;C:\Program Files\Microsoft Visual Studio\VC98\lib"

msgbox(WshEnv("LIB"))
msgbox(WshEnv("path"))
SYSPATH="C:\Windows\system32;C:\Windows;C:\Windows\System32\Wbem;C:\Windows\System32\WindowsPowerShell\v1.0\;C:\Program Files\TortoiseSVN\bin;C:\Program Files\IBM\SQLLIB\BIN;C:\Program Files\IBM\SQLLIB\FUNCTION;d:\Soft_remove\instantclient_11_2;c:\IBM\SQLLIB\BIN"


MYPATH=WCWP_PATH _
& ";" & WCWP_PATH & "\VC6CMD\bin" _
& ";" & WCWP_PATH & "\masm32\bin" _
& ";" & WCWP_PATH & "\nasm-2.09.10" _
& ";" & WCWP_PATH & "\radasm" _
& ";" & MINGW_BIN _
& ";" & SYSPATH
WshEnv("Path") =  MYPATH

SYSINC=""
MYINC=WCWP_PATH & "\masm32\include" _
& ";" & WCWP_PATH & "\VC6CMD\include" _
& ";" & WCWP_PATH & "\VC6CMD\ATL\include" _
& ";" & WCWP_PATH & "\VC6CMD\MFC\include" _
& ";" & "c:\IBM\SQLLIB\include" _
& ";" & SYSINC

WshEnv("include") =  MYINC

SYSLIB=""
MYLIB=WCWP_PATH &"\masm32\lib" _
& ";" & WCWP_PATH & "\VC6CMD\lib" _
& ";" & WCWP_PATH & "\VC6CMD\MFC\lib" _
& ";" & "c:\IBM\SQLLIB\lib" _
& ";" & SYSLIB

WshEnv("lib") = MYLIB


oracle_home="d:\Soft_remove\instantclient_11_2"
  
WshEnv("ORACLE_HOME") = oracle_home
WshEnv("LD_LIBRARY_PATH") = oracle_home
WshEnv("NLS_LANG") = "SIMPLIFIED CHINESE_CHINA.ZHS16GBK"
WshEnv("SQL_PATH") = oracle_home
WshEnv("TNS_ADMIN") = oracle_home
WshEnv("DB2CODEPAGE") = 1386
WshEnv("DB2INSTANCE") = "db2inst1"
'~ WshEnv("CLASSPATH") = DB2

'~ if InstrRev(WshEnv("Path"),"wcwp") then
'~ 'msgbox("wcwp already in path")
'~ else
'~ WshEnv("Path") = WshEnv("Path") & ";" & WCWP_PATH
'~ end if


'2012-03-17
'~ 'C:\poon\wcwp\wcwp\bin\VC6CMD
'~ if InstrRev(WshEnv("Path"),"VC6CMD") then
'~ 'msgbox("VC6CMD already in path")
'~ else
'~ WshEnv("Path") = WshEnv("Path") & ";" & WCWP_PATH & "\VC6CMD\bin"
'~ end if


'~ 'masm
'~ if InstrRev(WshEnv("Path"),"masm615") then
'~ 'msgbox("MASM615 already in path")
'~ else
'~ WshEnv("Path") = WshEnv("Path") & ";" & WCWP_PATH & "\masm615\bin"
'~ end if

'masm sdk

'~ if InstrRev(WshEnv("Path"),"masm32") then
'~ 'msgbox("masm32 already in path")
'~ else
'~ WshEnv("Path") = WshEnv("Path") & ";" & WCWP_PATH & "\masm32\bin"
'~ end if
'masm sdk %include%
'~ if InstrRev(WshEnv("include"),"masm32") then
'~ 'msgbox("masm32\include already in %include%")
'~ else
'~ WshEnv("include") = WCWP_PATH & "\masm32\include;" & WshEnv("include")
'~ end if



'WCWP_PATH\nasm-2.09.10

'~ if InstrRev(WshEnv("Path"),"nasm") then
'~ 'msgbox("nasm already in %Path%")
'~ else
'~ WshEnv("Path") = WCWP_PATH & "\nasm-2.09.10;" & WshEnv("Path")
'~ end if

'~ 'WCWP_PATH\radasm
'~ if InstrRev(WshEnv("Path"),"radasm") then
'~ 'msgbox("radasm already in %Path%")
'~ else
'~ WshEnv("Path") = WCWP_PATH & "\radasm;" & WshEnv("Path")
'~ end if

'c:\Dev-Cpp\bin
'~ if InstrRev(WshEnv("Path"),"Dev-Cpp") then
'~ 'msgbox("Dev-Cpp already in %Path%")
'~ else
'~ WshEnv("Path") = "c:\Dev-Cpp\bin;" & WshEnv("Path")
'~ end if



'~ 'java bin
'~ if InstrRev(WshEnv("Path"),"jdk1.7") then
'~ 'msgbox("jdk1.7 already in %Path%")
'~ else
'~ WshEnv("Path") = "C:\Program Files\Java\jdk1.7.0_01\bin;" & WshEnv("Path")
'~ end if


'~ 'javaclasspath
'~ if InstrRev(WshEnv("classpath"),"jdk1.7") then
'~ 'msgbox("jdk1.7 already in %classpath%")
'~ else
'~ WshEnv("classpath") = "C:\Program Files\Java\jdk1.7.0_01\bin;" & WshEnv("classpath")
'~ end if


'masm sdk %lib%
'~ if InstrRev(WshEnv("lib"),"masm32") then
'~ 'msgbox("masm32\lib already in %lib%")
'~ else
'~ WshEnv("lib") = WCWP_PATH &"\masm32\lib;" & WshEnv("lib")
'~ end if

msgbox(WshEnv("path"))
msgbox(WshEnv("include"))
msgbox(WshEnv("lib"))

