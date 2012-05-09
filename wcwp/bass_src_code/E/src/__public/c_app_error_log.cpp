//---------------------------------------------------------------------------
#include "cmpublic.h"

#pragma hdrstop

#include "c_app_error_log.h"
//---------------------------------------------------------------------------

//==========================================================================
// ���� : TCAppErrorLog::AddLog
// ��; : ��¼������־
// ԭ�� : static void AddLog(TEAppErrorSeverity aeAppErrorSeverity,
//              TCString sErrorType, TCString sErrorTag, TCString sErrorDes);
// ���� : aeAppErrorSeverity - �������س̶ȣ���Ϊ
//            ���� : �����ݴ�������ܽϵ͵ȣ�����Ӱ��������������
//            ���� : �����Ĵ�����Ҫ��Ԥ�������������
//        sErrorType         - �������ʹ���һ�����������¿��ж��ִ���
//        sErrorTag          - �����־����Ψһ��־ÿһ�ִ���
//        sErrorDes          - ������ϸ����
// ���� : ��
// ˵�� : ���������־�ļ�����(����10000����¼)����ض�1/3
//==========================================================================
void TCAppErrorLog::AddLog(TEAppErrorSeverity aeAppErrorSeverity,
            TCString sErrorType, TCString sErrorTag, TCString sErrorDes)
{
    //======= 1. �õ�������־�ļ��� ======
    TCString sLogFileName;

    if (aeAppErrorSeverity == aeHint)
        sLogFileName = MergePathAndFile(TAppPath::AppLog(), "sys_hint.mlg");
    else
        sLogFileName = MergePathAndFile(TAppPath::AppLog(), "warn_err.mlg");

    //======= 2. ����ļ�������, �򴴽�֮ =====
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

    //====== 3. ������־�� =========
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

    //====== 4. ����ļ�������ض�1/3 =====
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
// ���� : TCAppErrorLog::AddAppWarnMsgLog
// ��; : ��¼������־
// ԭ�� : static void AddAppWarnMsgLog(TCString sAppName,TCString sWarnMsg);
// ���� : sAppName  - ��������
//        sWarnMsg  - ������Ϣ
// ���� : ��
// ˵�� : 
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
      	"����"+sAppName+"�澯��Ϣ:"+sWarnMsg;

	fputs(sLogBuf,fp);
	fclose(fp);

	return;
}


