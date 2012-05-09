//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_ftp_client.h"

//---------------------------------------------------------------------------

#pragma package(smart_init)
//==========================================================================
// ���� :TCFtpClient
// ��; :FTP�ļ�����ͻ��˻��๹�캯��
// ԭ�� :TCFtpClient::TCFtpClient()
// ���� :��
// ���� :��
// ˵�� :��ʼ���������������Ӷ˿ڡ����ò���ʾ����˻ظ�����Ϣ�����ó�ʱʱ��
//==========================================================================
TCFtpClient::TCFtpClient()
{
    m_bLogin = false;
    m_bShowMsg = false;
    m_nPort = 21;
    m_eFtpMode = fmIMAGE;
    SetTimeOut(60000);
}

//==========================================================================
// ���� :~TCFtpClient
// ��; :FTP�ļ�����ͻ��˻�����������
// ԭ�� :TCFtpClient::~TCFtpClient()
// ���� :��
// ���� :��
// ˵�� :�ر����е�����
//==========================================================================
TCFtpClient::~TCFtpClient()
{
    Close();
}

//==========================================================================
// ���� :Close
// ��; :�ر�����
// ԭ�� :void TCFtpClient::Close()
// ���� :��
// ���� :��
// ˵�� :
//==========================================================================
void TCFtpClient::Close()
{
  if( IfConnected() )
      DoCommand("QUIT");
  m_CmdSock.Close();
  m_DataSock.Close();
  m_bLogin = false;
}

//==========================================================================
// ���� :ChangeDir
// ��; :����FTP����Ŀ¼
// ԭ�� :void TCFtpClient::ChangeDir(TCString sDirName)
// ���� :����Ŀ¼
// ���� :��
// ˵�� :�д�����DoCommand���׳�����
//==========================================================================
void TCFtpClient::ChangeDir(TCString sDirName)
{
  DoCommand("CWD "+ sDirName);
}

//==========================================================================
// ���� :DoCommand
// ��; :ִ��һ��FTPָ������������ݲ������͵�ָ��
// ԭ�� :void TCFtpClient::DoCommand(TCString sCommand)
// ���� :���
// ���� :��
// ˵�� :��ִ��ʧ�����׳�����
//==========================================================================
void TCFtpClient::DoCommand(TCString sCommand)
{
   sCommand = sCommand + "\r\n";
   if( !m_CmdSock.Write((char *)sCommand,Length(sCommand)) )
       throw TCFTPException(ftpTransferError,"-����������жϣ���������״̬");
   IfExecuteSuccess();
}

//==========================================================================
// ���� :DeleteFile
// ��; :ɾ��Զ�������ϵ��ļ�
// ԭ�� :void TCFtpClient::DeleteFile(TCString sSrcFileName)
// ���� :��Ҫɾ�����ļ���
// ���� :��
// ˵�� :��ִ��ʧ����DoCommand���׳�����
//==========================================================================
void TCFtpClient::DeleteFile(TCString sSrcFileName)
{
   DoCommand("DELE "+ sSrcFileName);
}

//==========================================================================
// ���� :Download
// ��; : �����ļ�
// ԭ�� :void TCFtpClient::Download(TCString sRemoteFile, TCString sLocalFile)
// ���� :Զ���ļ����������ļ���
// ���� :��
// ˵�� :���ع����д���������
//==========================================================================
void TCFtpClient::Download(TCString sRemoteFile, TCString sLocalFile)
{
   TCFileStream cNewFile;
   long nSize;
   cNewFile.Open(sLocalFile,omWrite);
   PrepareDataTransfers("RETR "+sRemoteFile);
   nSize = m_DataSock.Read(m_DataSock.m_sBuf,sizeof(m_DataSock.m_sBuf));
   while( nSize > 0 )
   {
       if( m_bShowMsg )
           printf(".");
       cNewFile.Write(m_DataSock.m_sBuf,nSize);
       nSize = m_DataSock.Read(m_DataSock.m_sBuf,sizeof(m_DataSock.m_sBuf));
   }
   m_DataSock.Close();
   if( m_bShowMsg )
       printf("\r\n");
   IfExecuteSuccess();
}

//==========================================================================
// ���� : GetFileInfo
// ��; : ȡ����ϸ�ļ��б���ĳһ�ļ���Ϣ
// ԭ�� : TSFtpFileInfo & TCFtpClient::GetFileInfo(long nIndex)
// ���� : ��ϸ�б������
// ���� : FTP�ļ���Ϣ�ṹ���ã�TSFtpFileInfo &��
// ˵�� : ���ʽ�����׳����⣬Ŀǰֻ��д���ļ����ƺ��ļ���С�Լ��Ƿ�ΪĿ¼��
//        ���ļ�������δ��ת��
//==========================================================================
TSFtpFileInfo & TCFtpClient::GetFileInfo(long nIndex)
{
  if( (nIndex<0)||(nIndex>=m_lsFileInfo.GetCount()) )
       throw TCException("Error TCFtpClient::GetFileInfo()---����nIndexԽ��");
       
  TCStringList lsField;
  long i;
  lsField.CommaText(m_lsFileInfo[nIndex],' ');
  for( i = 0;i<lsField.GetCount();)
  {
    if( lsField[i] == TCString("") )
        lsField.Delete(i);
    else
        i++;
  }
  
  //for test
  for( i = 0;i<lsField.GetCount();i++)
  {
    PrintTestMsg(lsField[i]);
  }
  PrintTestMsg("cols count is:"+IntToStr(lsField.GetCount()));
  
  #ifdef __SUN58__
  #else
	  if( lsField.GetCount() < 8 )
	      throw TCFTPException(ftpListSyntaxError,"-���ص��ļ���������ʽ�д���{"+\
	                                                 m_lsFileInfo[nIndex]+"}");
  	  m_fiFileInfo.m_nFileSize=StrToInt(lsField[4]);
  #endif

//--------ȡ�ļ���---------------------------------------------------
  if( lsField.GetCount()<=9 )
       m_fiFileInfo.m_sFileName = lsField[lsField.GetCount()-1];
  else
       m_fiFileInfo.m_sFileName = Mid(m_lsFileInfo[nIndex],60);
  if(Pos(m_fiFileInfo.m_sFileName,"\n" ) != 0)
       m_fiFileInfo.m_sFileName = Left(m_fiFileInfo.m_sFileName, Length(m_fiFileInfo.m_sFileName)-1); 

//--------�ж��Ƿ�ΪĿ¼---------------------------------------------
  if( lsField[0][1] == '-' )
      m_fiFileInfo.m_bIsDirectory = false;
  else
      m_fiFileInfo.m_bIsDirectory = true;
  #ifdef __SUN58__
	if(Pos(m_lsFileInfo[nIndex],"<DIR>" )>0)
		m_fiFileInfo.m_bIsDirectory = true;
	else
		m_fiFileInfo.m_bIsDirectory = false;
  #endif
  
  return m_fiFileInfo;
}

//==========================================================================
// ���� : GetFileDescString
// ��; : ȡ���ļ���ϸ�б��е�һ��
// ԭ�� : TCString TCFtpClient::GetFileDescString(long nIndex)
// ���� : ��ϸ�б�����
// ���� : �ļ���Ϣ��
// ˵�� : �������Խ�����׳�����
//==========================================================================
TCString TCFtpClient::GetFileDescString(long nIndex)
{
    if( (nIndex<0)||(nIndex>=m_lsFileInfo.GetCount()) )
       throw TCException("Error TCFtpClient::GetFileInfo()---����nIndexԽ��");
    return m_lsFileInfo[nIndex];
}

//==========================================================================
// ���� : GetResponseStr
// ��; : ȡ��FTPִ������Ľ����Ӧ��
// ԭ�� : TCString TCFtpClient::GetResponseStr()
// ���� : ��
// ���� : FTPִ������Ľ����Ӧ��
// ˵�� :
//==========================================================================
TCString TCFtpClient::GetResponseStr()
{
   return  m_sResponse;
}

//==========================================================================
// ���� :IfConnected
// ��; :�ж��Ƿ��Ѿ��ͷ���˽�������
// ԭ�� :bool TCFtpClient::IfConnected()
// ���� :��
// ���� :����ֵ
// ˵�� :��---�Ѿ����ӣ���---δ����
//==========================================================================
bool TCFtpClient::IfConnected()
{
    if( !m_CmdSock.IfConnected() )
        m_bLogin = false;
    return m_CmdSock.IfConnected();
}

//==========================================================================
// ���� :IsIPString
// ��; :�ж������HOSTֵ�Ƿ�ΪIP��ַ��
// ԭ�� :bool TCFtpClient::IsIPString()
// ���� :��
// ���� :����ֵ
// ˵�� :��-ֵΪIP��ַ������-ֵΪ��������
//==========================================================================
bool TCFtpClient::IsIPString()
{
    int i,nCount = 0;
    if( (Length(m_sHost) < 7) )
         return false;
    for( i = 0 ;i<Length(m_sHost); i++)
    {
      if( m_sHost[i+1] == '.' )
          nCount++;
      else
      {  if( (m_sHost[i+1] < '0')||(m_sHost[i+1] > '9') )
            return false;
      }
    }
    if( nCount == 3 )
        return true;
    else
        return false;
}

//==========================================================================
// ���� :IfExecuteSuccess
// ��; :�ж�FTP�����Ӧ������Ϣ���壬�Ա������ִ���Ƿ�ɹ���
// ԭ�� :void TCFtpClient::IfExecuteSuccess()
// ���� :��
// ���� :
// ˵�� :����д�����������
//==========================================================================
void TCFtpClient::IfExecuteSuccess()
{
    int nErrorCode;
    m_sResponse = m_CmdSock.ReadLine();
    if( !IfConnected() )
        throw TCFTPException(ftpTransferError,"-δ��������");
    if( m_bShowMsg )
        printf("%s\r\n",(char *)m_sResponse);
    if( Length(m_sResponse) < 3 )
        throw TCFTPException(ftpReturnSyntaxError,"-��Ӧ��ʽ����{"+m_sResponse+"}");
    else
    {
        nErrorCode = StrToInt(Mid(m_sResponse,1,3));
        if( nErrorCode==230 )  m_bLogin = true;
        if( nErrorCode==221 )  m_bLogin = false;
       	if ( (nErrorCode <= 0)||(m_sResponse[1] == '4') || (m_sResponse[1] == '5') )
             throw TCFTPException(ftpCommandError,"-ִ������ʧ��{"+m_sResponse+"}");
    }

}

//==========================================================================
// ���� : List
// ��; : �г�Զ���������ļ�����ϸ��Ϣ
// ԭ�� : long TCFtpClient::List(TCString sFilter)
// ���� : �ļ����������������磺"A??.*")
// ���� : �б�����
// ˵�� : ��ִ��ʧ�����׳����⣬�б��������˽�г�Աm_lsFileInfo����
//==========================================================================
long TCFtpClient::List(TCString sFilter)
{
    TCString sLine;
    m_lsFileInfo.Clear();
    TCString sCmd;
    if(Length(sFilter) == 0)
        sCmd = "LIST";
    else
        sCmd = "LIST " + sFilter;

    PrepareDataTransfers(sCmd);
    while(true)
    {
        sLine = m_DataSock.ReadLine();
        if( sLine != TCString("") )
        {
            #ifdef __SUN58__
            	if (Left(sLine,5)!="total" && Pos(sLine,"No such file or directory")==0)
            		m_lsFileInfo.Add(sLine);
            #else
            	m_lsFileInfo.Add(sLine);
            #endif
            if( m_bShowMsg )
               printf("%s\r\n",(char*)sLine);
               
            //for test
            PrintTestMsg(sLine);
        }
        else break;
    }
    IfExecuteSuccess();
    return m_lsFileInfo.GetCount();
}

//==========================================================================
// ���� :Mode
// ��; :�����ļ�����ģʽ
// ԭ�� :void TCFtpClient::Mode(TEFtpMode eFtpMode)
// ���� :ö��TEFtpMode{fmASCII,fmIMAGE,fmBYTE}
// ���� :
// ˵�� :
//==========================================================================
void TCFtpClient::Mode(TEFtpMode eFtpMode)
{
    char cType;
    m_eFtpMode = eFtpMode;
    switch(eFtpMode)
    {
      case fmASCII : cType = 'A';
      case fmIMAGE : cType = 'I';
      case fmBYTE  : cType = 'B';
      default: cType = 'I';
    }
    DoCommand(TCString("TYPE ")+cType);
}

//==========================================================================
// ���� :Mode
// ��; :�����ļ�����ģʽ
// ԭ�� :void TCFtpClient::Mode(TCString sMode)
// ���� :����ģʽ
// ���� :
// ˵�� :
//==========================================================================
void TCFtpClient::Mode(TCString sMode)
{
    char cType;
    ASSERT(sMode == TCString("A") || sMode == TCString("B") || sMode == TCString("I") ) ;
    cType = sMode[1];
    switch(cType)
    {
        case 'A':   m_eFtpMode = fmASCII;
                    break;
        case 'B':   m_eFtpMode = fmBYTE;
                    break;
        case 'I':   m_eFtpMode = fmIMAGE;
                    break;
        default:    m_eFtpMode = fmIMAGE;
                    break;
    }
}

//==========================================================================
// ���� :MakeDirectory
// ��; :��Զ�������Ͻ���Ŀ¼
// ԭ�� :void TCFtpClient::MakeDirectory(TCString sDirectoryName)
// ���� :������Ŀ¼��
// ���� :��
// ˵�� :��ִ��ʧ����DoCommand���׳�����
//==========================================================================
void TCFtpClient::MakeDirectory(TCString sDirectoryName)
{
   DoCommand("MKD "+ sDirectoryName);
}

//==========================================================================
// ���� : NameList
// ��; : �г�Զ�������ϵ�ǰĿ¼���ļ�����
// ԭ�� : void TCFtpClient::NameList(TCStringList& lsName,TCString sFilter)
// ���� : �б��Ŷ����ļ����������������磺"A??.*")
// ���� : ��
// ˵�� : ��ִ��ʧ�����׳�����
//==========================================================================
void TCFtpClient::NameList(TCStringList& lsName,TCString sFilter)
{
    TCString sLine;
    TCString sCmd;
    if(Length(sFilter) == 0)
        sCmd = "NLST";
    else
        sCmd = "NLST " + sFilter;

    PrepareDataTransfers(sCmd);
    TCString sFileNameBuf;
    while(true)
    {
        sLine = m_DataSock.ReadLine();
        if( sLine != TCString("") )
        {
           sFileNameBuf += sLine;
           if( m_bShowMsg )
               printf("%s\r\n",(char*)sLine);
        }
        else break;
    }
    if(sFileNameBuf != TCString("") )
        lsName.SetText(sFileNameBuf);
    IfExecuteSuccess();
}

//==========================================================================
// ���� :Open
// ��; :��¼��FTP�����
// ԭ�� :void TCFtpClient::Open()
// ���� :��
// ���� :
// ˵�� :����֮ǰ���������������ƻ��ߵ�ַ�Լ��û����Ϳ��
//==========================================================================
void TCFtpClient::Open()
{
    Close();
    if( m_sHost == TCString("") )
        throw TCFTPException(ftpLoginError,"-δ����FTP��������");
    if( IsIPString() )
        m_CmdSock.m_sAddress = m_sHost;
    else
        m_CmdSock.m_sAddress = TCPowerSocket::GetHostIP(m_sHost);
    m_CmdSock.m_nPort = m_nPort;
    if( !m_CmdSock.Open() )
        throw TCFTPException(ftpConnectError,"-�޷��������ӣ������������ú�����״̬");
    try
    {
        IfExecuteSuccess();
        DoCommand("USER " + m_sUserID );
        DoCommand("PASS " + m_sPassword );
        while( m_CmdSock.WaitForData(100) )
            IfExecuteSuccess();
    }
    catch(TCFTPException &eftp)
    {
        throw TCFTPException(ftpLoginError,"-��¼ʧ�ܣ������û����Ϳ���");
    }
    Mode(m_eFtpMode);
}

//==========================================================================
// ���� :PrepareDataTransfers
// ��; :׼���������ݣ��������������������ͨ��
// ԭ�� :void TCFtpClient::PrepareDataTransfers(TCString sCommand)
// ���� :ִ�е�����---�����������ִ�к���Ҫ�������ݵ�����
// ���� :
// ˵�� :��������ݴ���ĵ�ַ�Ͷ˿ڣ���ʽΪ(h1,h2,h3,h4,p1,p2)
//==========================================================================
void TCFtpClient::PrepareDataTransfers(TCString sCommand)
{
   long nStart,nEnd;
   TCStringList lsDataStr;
   if( !m_bLogin )
       throw TCFTPException(ftpCommandError,"-δ��¼");
   DoCommand("PASV");
   nStart = m_sResponse.Find('(');
   nEnd = m_sResponse.ReverseFind(')');
   if( ( nStart == -1 )||( nEnd == -1 ) )
        throw TCFTPException(ftpReturnSyntaxError,"-��Ӧ��ʽ����{"+m_sResponse+"}");
   lsDataStr.CommaText(m_sResponse.Mid(nStart+1,nEnd-nStart),',');
   if( lsDataStr.GetCount() != 6 )
        throw TCFTPException(ftpReturnSyntaxError,"-��Ӧ��ʽ����{"+m_sResponse+"}");
   m_DataSock.m_sAddress = lsDataStr[0]+'.'+lsDataStr[1]+'.'+lsDataStr[2]+'.'+lsDataStr[3];
   m_DataSock.m_nPort = StrToInt(lsDataStr[4])*256+StrToInt(lsDataStr[5]);

   sCommand = sCommand + "\r\n";
   if( !m_CmdSock.Write((char *)sCommand,Length(sCommand)) )
        throw TCFTPException(ftpTransferError,"-����ָ�������ж�{"+sCommand+"}");
   m_DataSock.Close();
   m_DataSock.Open(false);
   IfExecuteSuccess();
}

//==========================================================================
// ���� : RemoveDir
// ��; : ɾ��Զ��������һĿ¼����Ŀ¼�����ǿ�Ŀ¼
// ԭ�� : void TCFtpClient::RemoveDir(TCString sDirectoryName)
// ���� : Ҫɾ����Ŀ¼����
// ���� : ��
// ˵�� : ��ִ��ʧ����DoCommand���׳�����
//==========================================================================
void TCFtpClient::RemoveDir(TCString sDirectoryName)
{
   DoCommand("RMD "+ sDirectoryName);
}

//==========================================================================
// ���� : Rename
// ��; : ����Զ�������ϵ��ļ���
// ԭ�� : void TCFtpClient::Rename(TCString sSrcFilename,TCString sDestFileName)
// ���� : Դ�ļ����ƣ�Ŀ���ļ�����
// ���� : ��
// ˵�� : ��ִ��ʧ����DoCommand���׳�����
//==========================================================================
void TCFtpClient::Rename(TCString sSrcFilename,TCString sDestFileName)
{
  DoCommand("RNFR "+sSrcFilename);
  DoCommand("RNTO "+sDestFileName);
}

//==========================================================================
// ���� :SetConnectType
// ��; :�������ӷ�ʽ
// ԭ�� :void TCFtpClient::SetConnectType(TEConnectType eConnectType)
// ���� :���ӷ�ʽ---��������ʽ��������ʽ
// ���� :��
// ˵�� : ����ֵ---ctNonBlocking����ctBlocking
//==========================================================================
void TCFtpClient::SetConnectType(TEConnectType eConnectType)
{
    m_CmdSock.SetConnectType(eConnectType);
    m_DataSock.SetConnectType(eConnectType);
}

//==========================================================================
// ���� :SetTimeOut
// ��; :���ó�ʱʱ��
// ԭ�� :void TCFtpClient::SetTimeOut(long nValue)
// ���� :��ʱʱ����ֵ
// ���� :��
// ˵�� :��λ����
//==========================================================================
void TCFtpClient::SetTimeOut(long nValue)
{
    m_CmdSock.m_Timeout = nValue;
    m_DataSock.m_Timeout = nValue;
}

//==========================================================================
// ���� : Size
// ��; : �õ�Զ�������ϵ��ļ���С
// ԭ�� : long TCFtpClient::Size(TCString sFilename)
// ���� : �ļ�����
// ���� : ��
// ˵�� : ��ִ��ʧ����DoCommand���׳�����
//==========================================================================
long TCFtpClient::Size(TCString sFilename)
{
    DoCommand("SIZE "+sFilename);

    TCStringList slResponseList;
    slResponseList.CommaText(m_sResponse, ' ');
    if(slResponseList.GetCount() != 2 )
        throw TCFTPException(ftpTransferError,"���صĸ�ʽ����ȷ!");
    if(!IsNumber(slResponseList[1]))
        throw TCFTPException(ftpTransferError,"���صĸ�ʽ����ȷ!");
    long nSize = StrToInt(slResponseList[1]);
    return nSize;
}

//==========================================================================
// ���� :Upload
// ��; :�ϴ��ļ�
// ԭ�� :void TCFtpClient::Upload(TCString sLocalFile,TCString sRemoteFile)
// ���� :�����ļ�����Զ�������ļ���
// ���� :��
// ˵�� :���ϴ������д���������
//==========================================================================
void TCFtpClient::Upload(TCString sLocalFile,TCString sRemoteFile)
{
   TCFileStream cSrcFile;
   long nSize;
   if( sRemoteFile = "" )
       sRemoteFile = ExtractFileName(sLocalFile);
   cSrcFile.Open(sLocalFile,omRead|omShared);
   PrepareDataTransfers("STOR "+sRemoteFile);
   nSize = cSrcFile.Read(m_DataSock.m_sBuf,sizeof(m_DataSock.m_sBuf));
   while( nSize > 0 )
   {
       if( m_bShowMsg )
           printf(".");
       if( !m_DataSock.Write(m_DataSock.m_sBuf,nSize) )
            throw TCFTPException(ftpTransferError,"-�����ϴ������жϣ���������״̬");
       nSize = cSrcFile.Read(m_DataSock.m_sBuf,sizeof(m_DataSock.m_sBuf));
   }
   m_DataSock.Close();
   if( m_bShowMsg )
       printf("���ݴ������\r\n");
   IfExecuteSuccess();
}

//==========================================================================
// ���� :UploadFile
// ��; :�ϴ��ļ�,�ϴ�ʱ���ļ�������
// ԭ�� :void TCFtpClient::UploadFile(TCString sLocalFile,TCString sRemoteFile)
// ���� :�����ļ�����Զ�������ļ���
// ���� :��
// ˵�� :���ϴ������д���������
//==========================================================================
void TCFtpClient::UploadFile(TCString sLocalFile,TCString sRemoteFile)
{
   TCFileStream cSrcFile;
   long nSize;
   if( sRemoteFile = "" )
       sRemoteFile = ExtractFileName(sLocalFile);
   cSrcFile.Open(sLocalFile,omRead|omExclusive_Waiting);
   PrepareDataTransfers("STOR "+sRemoteFile);
   nSize = cSrcFile.Read(m_DataSock.m_sBuf,sizeof(m_DataSock.m_sBuf));
   while( nSize > 0 )
   {
       if( m_bShowMsg )
           printf(".");
       if( !m_DataSock.Write(m_DataSock.m_sBuf,nSize) )
            throw TCFTPException(ftpTransferError,"-�����ϴ������жϣ���������״̬");
       nSize = cSrcFile.Read(m_DataSock.m_sBuf,sizeof(m_DataSock.m_sBuf));
   }
   m_DataSock.Close();
   if( m_bShowMsg )
       printf("���ݴ������\r\n");
   IfExecuteSuccess();
}

//==========================================================================
// ���� : TCFTPException
// ��; : FTP���������Ĺ��캯��
// ԭ�� : TCFTPException::TCFTPException(TEFTPErrorCode ftpErrorStatus, TCString sMsg)
// ���� : �������ͣ�TEFTPErrorCode����������Ϣ������
// ���� : ��
// ˵�� :
//==========================================================================
TCFTPException::TCFTPException(TEFTPErrorCode ftpErrorStatus, TCString sMsg):TCException(sMsg)
{
   m_ftpErrorStatus = ftpErrorStatus;
}
