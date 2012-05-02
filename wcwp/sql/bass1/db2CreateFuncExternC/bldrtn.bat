@echo off
rem (c) Copyright IBM Corp. 2007 All rights reserved.
rem 
rem The following sample of source code ("Sample") is owned by International 
rem Business Machines Corporation or one of its subsidiaries ("IBM") and is 
rem copyrighted and licensed, not sold. You may use, copy, modify, and 
rem distribute the Sample in any form without payment to IBM, for the purpose of 
rem assisting you in the development of your applications.
rem 
rem The Sample code is provided to you on an "AS IS" basis, without warranty of 
rem any kind. IBM HEREBY EXPRESSLY DISCLAIMS ALL WARRANTIES, EITHER EXPRESS OR 
rem IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF 
rem MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. Some jurisdictions do 
rem not allow for the exclusion or limitation of implied warranties, so the above 
rem limitations or exclusions may not apply to you. IBM shall not be liable for 
rem any damages you suffer as a result of using, copying, modifying or 
rem distributing the Sample, even if IBM has been advised of the possibility of 
rem such damages.

rem BATCH FILE: bldrtn.bat 
rem Builds C routines (stored procedures and UDFs) on Windows
rem Usage: bldrtn prog_name [ db_name ]

rem Default compiler is set to Microsoft Visual C++
rem To use a different compiler, comment out 'set BLDCOMP=cl'
rem and uncomment 'set BLDCOMP=icl' or 'set BLDCOMP=ecl'
rem Microsoft C/C++ Compiler
set BLDCOMP=cl

rem Intel C++ Compiler for 32-bit and 64-bit applications
rem set BLDCOMP=icl

rem Older Intel C++ Compiler for Itanium 64-bit applications
rem set BLDCOMP=ecl

rem Uncomment the next line if building 32-bit applications using
rem a DB2 64-bit product on Windows 64-bit
rem set BITWIDTH=32onWin64

rem The INCLUDE path environment variable must have 
rem %DB2PATH%\include ahead of any Microsoft Platform SDK
rem include directories.
rem set INCLUDE=%DB2PATH%\include;%INCLUDE%

rem On ntx64 use bufferoverflowU.lib library during linking
set BUFFEROVERFLOWLIB=
if "%CPU%" == "AMD64" set BUFFEROVERFLOWLIB=bufferoverflowU.lib

if "%BITWIDTH%" == "32onWin64" set LIB=%DB2PATH%\lib\Win32;%LIB%

if exist "%1.sqc" goto embedded
if exist "%1.sqx" goto embedded
goto compile

:embedded
rem Precompile and bind the program.
call embprep %1 %2

:compile
rem Compile the program.
if exist "%1.cxx" goto cpp
%BLDCOMP% -Zi -Od -c -W2 -DWIN32 -MD %1.c
goto link_step
:cpp
%BLDCOMP% -Zi -Od -c -W2 -DWIN32 -MD %1.cxx

:link_step
rem Link the program.
link -noentry -debug -out:%1.dll -dll %1.obj db2api.lib -def:%1.def %BUFFEROVERFLOWLIB%
if EXIST %1.dll.manifest MT -manifest %1.dll.manifest -outputresource:%1.dll;#2

rem Copy the routine DLL to the 'function' directory
echo %DB2PATH%\function
copy %1.dll "%DB2PATH%\function"
@echo on
