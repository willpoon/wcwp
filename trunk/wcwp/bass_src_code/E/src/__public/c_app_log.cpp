//---------------------------------------------------------------------------
#include "cmpublic.h"
#pragma hdrstop

#include "c_app_log.h"
//---------------------------------------------------------------------------

//==========================================================================
// ���� : TCAppLog::GetDailyLogFileNameOfApplication
// ��; : �õ�ÿ��һ���ļ���Ӧ�ó�����־�ļ�����
// ԭ�� : TCString GetDailyLogFileNameOfApplication();
// ���� : ��
// ���� : �ļ�ȫ��
// ˵�� : 1. ���Ŀ¼�����ڣ��򴴽���Ŀ¼
//        2. ��־�ļ�������־��Ŀ¼����Ӧ�ó���������Ŀ¼����������
//           <Ӧ�ó�����>+<YYYYMMDD>.mlg
//==========================================================================
TCString TCAppLog::GetDailyLogFileNameOfApplication(TCString sAppName)
{
    TCString sLogPath;
    TCString sLogFileName;

    if (sAppName == TCString("") )
        sAppName = Application.GetAppName();

    sLogPath = TAppPath::AppLog() + sAppName;
    if (!ForceDirectories(sLogPath))
        throw TCException("CREATE LOG PATH ERROR : "+sLogPath);

    TCAppLog::CheckOutLogFile(sLogPath,sAppName);   //%%%ADD 2000/11/18

    sLogFileName = MergePathAndFile(sLogPath,
            sAppName+TCTime::Today()+".mlg");
    return sLogFileName;
}

//==========================================================================
// ���� : TCAppLog::GetDailyLogFileNameWithTag
// ��; : �õ����б�ʶ����ÿ����־�ļ���
// ԭ�� : TCString GetDailyLogFileNameWithTag(TCString sTagName,
//              TCString sAppName);
// ���� : ��ʶ����Ӧ�ó�����
// ���� : ���б�ʶ������־�ļ���(���ļ����ƺ����"_��ʶ��")
// ˵�� :
//==========================================================================
TCString TCAppLog::GetDailyLogFileNameWithTag(TCString sTagName,
            TCString sAppName)
{
    ASSERT(Length(sTagName) > 0);

    TCString sDailyLogFileName;
    sDailyLogFileName = GetDailyLogFileNameOfApplication(sAppName);

    ASSERT(Length(sDailyLogFileName) > 4);

    TCString sResult;
    sResult = CutFileExt(sDailyLogFileName);
    sResult += "_" + sTagName;
    sResult += ExtractFileExt(sDailyLogFileName);

    return sResult;
}

void TCAppLog::CheckOutLogFile(TCString sLogPath,TCString sAppName)
{
    long nCurrentTimeSection;
    static long nLastTimeSection;
    static bool s_bRunFirstTime = true;
    const long LOG_CHECKOUT_DELAY = 12 * 60 ;

    if (s_bRunFirstTime)
    {
        nLastTimeSection
                = TCTime::GetTimeSection(TCTime::Now(), LOG_CHECKOUT_DELAY,1);
        s_bRunFirstTime = false;
    }
    nCurrentTimeSection
                = TCTime::GetTimeSection(TCTime::Now(), LOG_CHECKOUT_DELAY,1);
    if (nCurrentTimeSection != nLastTimeSection)
    {
       nLastTimeSection = nCurrentTimeSection;

       TCString slFilter,sCheckOutName;
       TCStringList slFilesName;
       long i;
       slFilter = sAppName +"2???????*"+".mlg" ;
       GetDirFileList(slFilesName,sLogPath, slFilter);
       slFilesName.Sort();

       //90����ǰ����־��Ϣ��ɾ��
       sCheckOutName = sAppName +
              TCTime::GetDateStringByTimeT(time(NULL)- 90 * 24 *3600)+".mlg";

       for( i = 0 ; i < slFilesName.GetCount() ; i++)
       {  if( sCheckOutName > slFilesName[i] )       //Albert
             DeleteFile(MergePathAndFile(sLogPath,slFilesName[i]));
          else
             break;
       }
    }
}

void TCAppLog::WriteLogFile(TCString sLogStr)
{
	FILE *fp;
	long nFileSize;
    TCString sLogFileName;

    sLogFileName = TAppPath::AppLog() + Application.GetAppName() + ".log";
	if((fp=fopen(sLogFileName,"a+"))==NULL) {
		printf("can't open logfile\n");
		return;
	}

	fseek(fp,0L,SEEK_END);
	fgetpos(fp,&nFileSize);
	if(nFileSize >= MAXLOGFILELEN)
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
        sLogStr;

#ifdef _DEBUG
    printf("%s", (char *)sLogBuf);
#endif
	fputs(sLogBuf,fp);
	fclose(fp);

	return;
}

