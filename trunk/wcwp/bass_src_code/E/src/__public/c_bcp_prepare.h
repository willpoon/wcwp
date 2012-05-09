//---------------------------------------------------------------------------

#ifndef c_bcp_prepareH
#define c_bcp_prepareH
//---------------------------------------------------------------------------

class TCBCPPrepare
{
public:
    // �����LIST����һ������
    static void  AddToXBcpList(TCString sFileName, TCString sTableName,
                bool bSameFieldSeq = true, bool bSameFieldName = true);

    static TCString GetArchiveDir();    // �õ�����Ŀ¼
    static long GetBcpSyncDelay();
    static TCString GetWorkingDir();    // �õ�����Ŀ¼
    static TCString GetBCPDir();        // �õ�����ļ����Ŀ¼
    static void     SubmitWorkingFileToBcp(TCString sFileName,
            TCString sTableName, TCString sDealDate,
            bool bSameFieldSeq = true, bool bSameFieldName = true);
    static void     SyncWorkingFileToBcp(TCString sFileName,
            TCString sTableName, bool bSameFieldSeq = true,
            bool bSameFieldName = true);
};

#endif
