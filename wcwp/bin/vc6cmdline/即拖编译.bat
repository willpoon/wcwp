set CL_HOME=C:\poon\wcwp\wcwp\bin\vc6cmdline

echo off
cls
color 0A

echo 设定环境变量...
set INCLUDE=%CL_HOME%/include
set LIB=%CL_HOME%/lib

echo ===============================================
echo ■开始编译%1...
%CL_HOME%/cl %1

echo ===============================================
echo ■清理多余文件...
del *.obj /Q
echo 完成.
echo ===============================================
echo ■编译完成，如成功则输出exe文件。
echo ★制作：t.qq.com/xiaostone
PAUSE