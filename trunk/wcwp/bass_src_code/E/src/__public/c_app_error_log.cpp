//---------------------------------------------------------------------------
#include "cmpublic.h"

#pragma hdrstop

#include "c_app_error_log.h"
//---------------------------------------------------------------------------

//==========================================================================
// 函数 : TCAppErrorLog::AddLog
// 用途 : 记录错误日志
// 原型 : static void AddLog(TEAppErrorSeverity aeAppErrorSeverity,
//              TCString sErrorType, TCString sErrorTag, TCString sErrorDes);
// 参数 : aeAppErrorSeverity - 错误严重程度，分为
//            警告 : 局数据错误或性能较低等，但不影响程序的正常运行
//            错误 : 产生的错误需要干预后才能正常处理
//        sErrorType         - 错误类型串，一个错误类型下可有多种错误
//        sErrorTag          - 错误标志串，唯一标志每一种错误
//        sErrorDes          - 文字详细描述
// 返回 : 无
// 说明 : 如果错误日志文件过大(超过10000条记录)，则截短1/3
//==========================================================================
void TCAppErrorLog::AddLog(TEAppErrorSeverity aeAppErrorSeverity,
            TCString sErrorType, TCString sErrorTag, TCString sErrorDes)
{
    //======= 1. 得到错误日志文件名 ======
    TCString sLogFileName;

    if (aeAppErrorSeverity == aeHint)
        sLogFileName = MergePathAndFile(TAppPath::AppLog(), "sys_hint.mlg");
    else
        sLogFileName = MergePathAndFile(TAppPath::AppLog(), "warn_err.mlg");

    //======= 2. 如果文件不存在, 则创建之 =====
    if (!FileExists(sLogFileName))
    {   TCDBFCreate dcCreate(sLogFileName, 7);
        dcCreate.AddField("program",    'C', 16);
        dcCreate.AddField("time",       'C', 14);
        dcCreate.AddField("severity",   'C', 8);
        dcCreate.AddField("err_type",   'C', 16);
        dcCreate.AddField("err_tag",    'C', 16);
        dcCreate.AddField("err_des",    'C', 255);
        dcCreate.AddField("fetched",    'C', 1);
        dcCreate.CreateDBF();
    }

    //====== 3. 增加日志项 =========
    TCString sProgram, dtTime, sSeverity;
    char cFetched;
    sProgram = Application.GetAppName();
    dtTime = TCTime::Now();

    switch (aeAppErrorSeverity)
    {
    case aeWarning:
        sSeverity = "WARN";
        break;
    case aeError:
        sSeverity = "ERROR";
        break;
    case aeHint:
        sSeverity = "HINT";
        break;
    default:
        throw TCException("switch (aeAppErrorSeverity) : "
                "Something must error.");
    }

    cFetched = 'N';

#ifdef __TEST__
    printf("Add Error Log : (Severity)%s  (ErrorType)%s  (ErrorTag)%s  "
            "(ErrorDes)%s", (char *)sSeverity, (char *)sErrorType,
            (char *)sErrorTag, (char *)sErrorDes);
#endif

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(sLogFileName);
    fdFoxDBF.DBBind("program", sProgram);
    fdFoxDBF.DBBind("time", dtTime);
    fdFoxDBF.DBBind("severity", sSeverity);
    fdFoxDBF.DBBind("err_type", sErrorType);
    fdFoxDBF.DBBind("err_tag", sErrorTag);
    fdFoxDBF.DBBind("err_des", sErrorDes);
    fdFoxDBF.DBBind("fetched", cFetched);

    fdFoxDBF.Append();

    //====== 4. 如果文件过大，则截短1/3 =====
    if (fdFoxDBF.RecCount() >= 10000)
    {
        long i;
        for (i = 1; i <= fdFoxDBF.RecCount() / 3; i++)
        {
            fdFoxDBF.Go(i);
            fdFoxDBF.Delete();
        }

        fdFoxDBF.Pack();
    }

    fdFoxDBF.CloseDBF();
}


//==========================================================================
// 函数 : TCAppErrorLog::AddAppWarnMsgLog
// 用途 : 记录错误日志
// 原型 : static void AddAppWarnMsgLog(TCString sAppName,TCString sWarnMsg);
// 参数 : sAppName  - 进程名称
//        sWarnMsg  - 错误信息
// 返回 : 无
// 说明 : 
//==========================================================================
void TCAppErrorLog::AddAppWarnMsgLog(TCString sAppName,TCString sWarnMsg)
{
	FILE *fp;
	long nFileSize;
	TCString sLogFileName;
	sLogFileName = MergePathAndFile(TAppPath::AppLog(),sAppName+".warn!");

	if((fp=fopen(sLogFileName,"a+"))==NULL) {
		printf("can't open logfile\n");
		return;
	}

	fseek(fp,0L,SEEK_END);
	fgetpos(fp,&nFileSize);
	if(nFileSize >= 2000000)
    	{
		fclose(fp);
        	DeleteFile(sLogFileName);

		if((fp=fopen(sLogFileName,"a+"))==NULL) {
			printf("can't open logfile\n");
			return ;
		}
	}

    	TCString sLogBuf;
    	TCString sStrNow;

    	sStrNow = TCTime::Now();
    	sLogBuf = Left(sStrNow,4) + "-" +
        Mid(sStrNow,5,2) + "-" +
        Mid(sStrNow,7,2) + " " +
        Mid(sStrNow,9,2) + ":" +
        Mid(sStrNow,11,2) + ":" +
        Right(sStrNow,2) + " " +
      	"进程"+sAppName+"告警信息:"+sWarnMsg;

	fputs(sLogBuf,fp);
	fclose(fp);

	return;
}


