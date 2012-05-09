//---------------------------------------------------------------------------

#ifndef c_file_transH
#define c_file_transH
//---------------------------------------------------------------------------
#include "c_ftp_client.h"
#include <iostream.h>


const long MAX_DIR = 100;
static long s_nLastGetFileTime;

class TCFileTrans
{
private:

    TCString    m_sFileDate;
    TCString    m_sCurFileName;
    TCString 	m_sFileNamePostfix[MAX_DIR];
    TCString 	m_sSubDir[MAX_DIR];	

    TCFtpClient m_tcFtp;

    void     AddListInfo();
    void     Connect();
    void     GetOneFile(TCString sSrcFile);
    long     GetInterval();
    TCString GetSrcDir();
    TCStringList GetSrcDirList();       // 得到源目录列表  20020204
    TCStringList GetDestDirList();      // 得到目的目录列表 20020325   
    TCStringList GetSrcBackUpList();    // 得到原目录备份目录列表 20020904    
    TCString GetTmpDir();
    TCString GetCollectType();
    TCString GetHostName();
    TCString GetUserName();
    TCString GetPassword();
    TCString GetFileFilter();
    TCString GetTransMode();
    bool     IsAccordFileDate();
    bool     IsNeedAddList();
    bool     IsNeedDeleteSource();
    bool     IsNeedUncompress();
    //add at 20020903
    bool     IsNeedSrcBackUp();
    //add at 20050322
    bool     IsAlertNoFile();

    void     UploadOneFile(TCString sSrcFile, TCString sDestFile);
    void     WriteFileDate();
    void     GetLastFileDate();
    void     WriteFileTranLog(TCString sFileName, long nFileSize,
                TCString dtStartTime, TCString dtStopTime);
    //add by chang at 20030114 for gprs分目录采集
    bool IsDealSubdirectory();
    void GetFtpFileNameAndSubDir();
    void UpdateFtpFileNameAndSubDir(long i,TCString sSubDir, TCString sFileNamePostfix);
    long GetFtpFileName(TCStringList tsFtpFileList, TCString sFileNamePostfix);
    long GetFtpSubDir(TCStringList tsSubDir, TCString tsSubDirName);
    bool CheckFtpFileName(TCString sFileName,long nPostfixLength);
    TCString GetSubDirFileName(TCString sSrcDir, TCString& sSubDir, TCString& sPostfix);
    bool IsFtpAll(TCString sSrcDir, TCString sSubDir,TCString sNextDir,TCString sPostfix);
    TCString IntToStrn(long nSrc, long nLen);

public:
    void DoGetFile();
    void DoPutFile();
};

#endif

