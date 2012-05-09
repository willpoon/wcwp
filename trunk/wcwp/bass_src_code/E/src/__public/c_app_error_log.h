//---------------------------------------------------------------------------

#ifndef c_app_error_logH
#define c_app_error_logH
//---------------------------------------------------------------------------

enum TEAppErrorSeverity { aeHint, aeWarning, aeError };

class TCAppErrorLog
{
public:
    static void AddLog(TEAppErrorSeverity aeAppErrorSeverity,
            TCString sErrorType, TCString sErrorTag, TCString sErrorDes);
    
    //========Begin add.HMY.20021202========================
    //===由于使用dbf文件格式浏览日志文件时,不能很好的浏览===
    //===日志文件中的全部内容,所以,增加该函数===============
    static void AddAppWarnMsgLog(TCString sAppName,TCString sWarnMsg="Unknown message!");
    //========End add=======================================
};

#endif
