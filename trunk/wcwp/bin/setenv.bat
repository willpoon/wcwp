:http://bbs.wuyou.com/viewthread.php?tid=123186&page=1
:REM 4.1 autoexec.bat - ����ϵͳ�̸�Ŀ¼
:REM �ŵ㣺�ɶ�̬�������������DOS/Windowsȫϵ��ϵͳ��ʹ��
:REM ȱ�㣺��Ҫ����������Ч����Ҫ�޸�ϵͳ�ļ�
:REM echo set path=d:\batch;%path%>> c:\autoexec.bat
:REM ��2K/XP/03���Ƿ����autoexec.bat�еı���������ע��������
:REM User Key: [HKEY_CURRENT_USER\Software\Microsoft\Windows NT\CurrentVersion\Winlogon]
:REM Value Name: ParseAutoexec
:REM Data Type: REG_SZ (String Value)
:REM Value Data: (0 = disabled, 1 = enabled)



:for /f %%i in ('cd') do set CURR_PATH=%%i
:set PATH=%PATH%;%CURR_PATH%
:sqlite3
echo set path=%cd%;%path% >> c:\autoexec.bat
pause