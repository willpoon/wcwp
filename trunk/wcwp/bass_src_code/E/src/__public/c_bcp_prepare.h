//---------------------------------------------------------------------------

#ifndef c_bcp_prepareH
#define c_bcp_prepareH
//---------------------------------------------------------------------------

class TCBCPPrepare
{
public:
    // 在入库LIST加入一项请求
    static void  AddToXBcpList(TCString sFileName, TCString sTableName,
                bool bSameFieldSeq = true, bool bSameFieldName = true);

    static TCString GetArchiveDir();    // 得到备份目录
    static long GetBcpSyncDelay();
    static TCString GetWorkingDir();    // 得到工作目录
    static TCString GetBCPDir();        // 得到入库文件存放目录
    static void     SubmitWorkingFileToBcp(TCString sFileName,
            TCString sTableName, TCString sDealDate,
            bool bSameFieldSeq = true, bool bSameFieldName = true);
    static void     SyncWorkingFileToBcp(TCString sFileName,
            TCString sTableName, bool bSameFieldSeq = true,
            bool bSameFieldName = true);
};

#endif
