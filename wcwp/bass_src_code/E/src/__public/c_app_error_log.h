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
    //===����ʹ��dbf�ļ���ʽ�����־�ļ�ʱ,���ܺܺõ����===
    //===��־�ļ��е�ȫ������,����,���Ӹú���===============
    static void AddAppWarnMsgLog(TCString sAppName,TCString sWarnMsg="Unknown message!");
    //========End add=======================================
};

#endif
