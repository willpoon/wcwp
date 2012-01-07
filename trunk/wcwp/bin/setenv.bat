:http://bbs.wuyou.com/viewthread.php?tid=123186&page=1
:REM 4.1 autoexec.bat - 来自系统盘根目录
:REM 优点：可动态定义变量，可在DOS/Windows全系列系统下使用
:REM 缺点：需要重启才能生效，需要修改系统文件
:REM echo set path=d:\batch;%path%>> c:\autoexec.bat
:REM 在2K/XP/03中是否解析autoexec.bat中的变量与以下注册表项相关
:REM User Key: [HKEY_CURRENT_USER\Software\Microsoft\Windows NT\CurrentVersion\Winlogon]
:REM Value Name: ParseAutoexec
:REM Data Type: REG_SZ (String Value)
:REM Value Data: (0 = disabled, 1 = enabled)



:for /f %%i in ('cd') do set CURR_PATH=%%i
:set PATH=%PATH%;%CURR_PATH%
:sqlite3
echo set path=%cd%;%path% >> c:\autoexec.bat
pause