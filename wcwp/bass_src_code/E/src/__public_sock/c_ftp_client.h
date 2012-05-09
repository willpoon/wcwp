//---------------------------------------------------------------------------

#ifndef c_ftpClientH
#define c_ftpClientH
#include "c_power_socket.h"
//---------------------------------------------------------------------------

enum TEFtpMode{fmASCII,fmIMAGE,fmBYTE};
struct TSFtpFileInfo
{
  TCString m_sFileName;
  long m_nFileSize;
//  TCString m_sDateTime;
  bool m_bIsDirectory;
};

class TCFtpClient
{
private:
    TCPowerSocket m_CmdSock;
    TCPowerSocket m_DataSock;
    TCString m_sResponse;
    TEFtpMode m_eFtpMode;
    bool m_bLogin;
    TCStringList m_lsFileInfo;
    TSFtpFileInfo m_fiFileInfo;

    void DoCommand(TCString sCommand);
    void IfExecuteSuccess();
    void PrepareDataTransfers(TCString sCommand);
    bool IsIPString();
public:
    TCString m_sUserID;
    TCString m_sPassword;
    TCString m_sHost;
    long m_nPort;
    bool m_bShowMsg;

    void SetTimeOut(long nValue);
    void SetConnectType(TEConnectType eConnectType);
    bool IfConnected();

public:
    TCFtpClient();
    ~TCFtpClient();

    void Open();
    void Close();
    void Mode(TEFtpMode eFtpMode);
    void Mode(TCString sMode);
    void Download(TCString sRemoteFile, TCString sLocalFile="");
    void Upload(TCString sLocalFile,TCString sRemoteFile="");   // 以共享方式打开文件
    void UploadFile(TCString sLocalFile,TCString sRemoteFile="");//打开文件并锁定
    void ChangeDir(TCString sDirName);
    void MakeDirectory(TCString sDirectoryName);
    void RemoveDir(TCString sDirectoryName);
    long Size(TCString sFileName);
    void DeleteFile(TCString sSrcFileName);
    void Rename(TCString sSrcFilename,TCString sDestFileName);

    long GetFileCount() {return m_lsFileInfo.GetCount();}
    long List(TCString sFilter="*");
    void NameList(TCStringList& lsName,TCString sFilter="*");
    TSFtpFileInfo & GetFileInfo(long nIndex);
    TCString GetFileDescString(long nIndex);

    TCString GetResponseStr();
};


enum TEFTPErrorCode
{ 
  ftpLoginError,
  ftpConnectError,
  ftpTransferError,
  ftpCommandError,
  ftpListSyntaxError,
  ftpReturnSyntaxError
};

class TCFTPException : public TCException
{
private:
    TEFTPErrorCode m_ftpErrorStatus;
public:
    TCFTPException(TEFTPErrorCode ftpErrorStatus, TCString sMsg);
    TEFTPErrorCode GetFTPErrorCode()
    {
       return m_ftpErrorStatus;
    }
    TCString GetErrMessage()
    {
        return m_sMessage;
    }
};


#endif
