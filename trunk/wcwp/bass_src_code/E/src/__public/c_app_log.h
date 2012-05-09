//---------------------------------------------------------------------------

#ifndef c_app_logH
#define c_app_logH
//---------------------------------------------------------------------------

#define MAXLOGFILELEN   2000000

class TCAppLog
{
public:
    static TCString GetDailyLogFileNameOfApplication(TCString sAppName = "");
    static TCString GetDailyLogFileNameWithTag(TCString sTagName,
            TCString sAppName = "");
    static void CheckOutLogFile(TCString sLogPath,TCString sAppName);

    static void WriteLogFile(TCString sLogStr);
};

#endif
