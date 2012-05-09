//----------------------------------------------------------
#pragma hdrstop

//----------------------------------------------------------
#include "c_add_log.h"

//----------------------------------------------------------

//TCCriticalSection TCAddLog::csLog;

//==========================================================================
// 函数 : TCAddLog::Add()
// 用途 : 写运行日志
// 原型 : Add(const TCString& sLog)
// 参数 : sLog -- 要写入日志的内容
// 返回 : 无
// 说明 :
//==========================================================================
void TCAddLog::Add(const TCString& sLog)
{
    TCString     sFileName;
    TCString     sLogPath;
    TCString     sAppName;
    TCString     sBuff;
    TCString     sTime;
    TCFileStream cFile;

//    csLog.Enter();

    try
    {
        sAppName = Application.GetAppName();
        sLogPath = TAppPath::AppLog() + sAppName;
        ForceDirectories(sLogPath);
        sFileName = TCAppLog::GetDailyLogFileNameOfApplication();
        cFile.Open(sFileName,omAppend);
        sTime  = TCTime::Now();

        sBuff  = '\n'+Left(sTime,4)+'/'+Mid(sTime,5,2)+'/'+Mid(sTime,7,2)
        	 +' '+Mid(sTime,9,2)+':'+Mid(sTime,11,2)+':'+Right(sTime,2)+' ';

        sBuff += sLog;

        printf((char*)sBuff);

        cFile.WriteLn(sBuff);
        cFile.Close();
    }
    catch(TCException& e)
    {
#ifdef __TEST__
    	printf("\nLog Exception:%s",(char*)e.GetMessage());
#endif
//    	csLog.Leave();
    }
//    csLog.Leave();
}

//==========================================================================
// 函数 :TCAddLog::Add
// 用途 : 写运行日志
// 原型 : Add(const TCString& sLog)
// 参数 : sLog -- 要写入日志的内容
// 返回 : 无
// 说明 :
//==========================================================================
void TCAddLog::Add(char* sLog)
{
    Add(TCString(sLog));
}
