//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_ftp_client.h"

//---------------------------------------------------------------------------

#pragma package(smart_init)
//==========================================================================
// 函数 :TCFtpClient
// 用途 :FTP文件传输客户端基类构造函数
// 原型 :TCFtpClient::TCFtpClient()
// 参数 :无
// 返回 :无
// 说明 :初始化变量，设置连接端口、设置不显示服务端回复的信息，设置超时时间
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
// 函数 :~TCFtpClient
// 用途 :FTP文件传输客户端基类析构函数
// 原型 :TCFtpClient::~TCFtpClient()
// 参数 :无
// 返回 :无
// 说明 :关闭所有的连接
//==========================================================================
TCFtpClient::~TCFtpClient()
{
    Close();
}

//==========================================================================
// 函数 :Close
// 用途 :关闭连接
// 原型 :void TCFtpClient::Close()
// 参数 :无
// 返回 :无
// 说明 :
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
// 函数 :ChangeDir
// 用途 :更改FTP工作目录
// 原型 :void TCFtpClient::ChangeDir(TCString sDirName)
// 参数 :工作目录
// 返回 :无
// 说明 :有错误则DoCommand中抛出例外
//==========================================================================
void TCFtpClient::ChangeDir(TCString sDirName)
{
  DoCommand("CWD "+ sDirName);
}

//==========================================================================
// 函数 :DoCommand
// 用途 :执行一条FTP指令，必须是无数据操作类型的指令
// 原型 :void TCFtpClient::DoCommand(TCString sCommand)
// 参数 :命令串
// 返回 :无
// 说明 :如执行失败则抛出例外
//==========================================================================
void TCFtpClient::DoCommand(TCString sCommand)
{
   sCommand = sCommand + "\r\n";
   if( !m_CmdSock.Write((char *)sCommand,Length(sCommand)) )
       throw TCFTPException(ftpTransferError,"-命令传输意外中断，请检查网络状态");
   IfExecuteSuccess();
}

//==========================================================================
// 函数 :DeleteFile
// 用途 :删除远端主机上的文件
// 原型 :void TCFtpClient::DeleteFile(TCString sSrcFileName)
// 参数 :需要删除的文件名
// 返回 :无
// 说明 :如执行失败则DoCommand中抛出例外
//==========================================================================
void TCFtpClient::DeleteFile(TCString sSrcFileName)
{
   DoCommand("DELE "+ sSrcFileName);
}

//==========================================================================
// 函数 :Download
// 用途 : 下载文件
// 原型 :void TCFtpClient::Download(TCString sRemoteFile, TCString sLocalFile)
// 参数 :远程文件名，本地文件名
// 返回 :无
// 说明 :下载过程有错则抛例外
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
// 函数 : GetFileInfo
// 用途 : 取得详细文件列表中某一文件信息
// 原型 : TSFtpFileInfo & TCFtpClient::GetFileInfo(long nIndex)
// 参数 : 详细列表的索引
// 返回 : FTP文件信息结构引用（TSFtpFileInfo &）
// 说明 : 如格式错误将抛出例外，目前只填写了文件名称和文件大小以及是否为目录，
//        对文件的日期未做转换
//==========================================================================
TSFtpFileInfo & TCFtpClient::GetFileInfo(long nIndex)
{
  if( (nIndex<0)||(nIndex>=m_lsFileInfo.GetCount()) )
       throw TCException("Error TCFtpClient::GetFileInfo()---参数nIndex越界");
       
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
	      throw TCFTPException(ftpListSyntaxError,"-返回的文件描述串格式有错误{"+\
	                                                 m_lsFileInfo[nIndex]+"}");
  	  m_fiFileInfo.m_nFileSize=StrToInt(lsField[4]);
  #endif

//--------取文件名---------------------------------------------------
  if( lsField.GetCount()<=9 )
       m_fiFileInfo.m_sFileName = lsField[lsField.GetCount()-1];
  else
       m_fiFileInfo.m_sFileName = Mid(m_lsFileInfo[nIndex],60);
  if(Pos(m_fiFileInfo.m_sFileName,"\n" ) != 0)
       m_fiFileInfo.m_sFileName = Left(m_fiFileInfo.m_sFileName, Length(m_fiFileInfo.m_sFileName)-1); 

//--------判断是否为目录---------------------------------------------
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
// 函数 : GetFileDescString
// 用途 : 取得文件详细列表中的一行
// 原型 : TCString TCFtpClient::GetFileDescString(long nIndex)
// 参数 : 详细列表索引
// 返回 : 文件信息串
// 说明 : 如果索引越界则抛出例外
//==========================================================================
TCString TCFtpClient::GetFileDescString(long nIndex)
{
    if( (nIndex<0)||(nIndex>=m_lsFileInfo.GetCount()) )
       throw TCException("Error TCFtpClient::GetFileInfo()---参数nIndex越界");
    return m_lsFileInfo[nIndex];
}

//==========================================================================
// 函数 : GetResponseStr
// 用途 : 取得FTP执行命令的结果回应串
// 原型 : TCString TCFtpClient::GetResponseStr()
// 参数 : 无
// 返回 : FTP执行命令的结果回应串
// 说明 :
//==========================================================================
TCString TCFtpClient::GetResponseStr()
{
   return  m_sResponse;
}

//==========================================================================
// 函数 :IfConnected
// 用途 :判断是否已经和服务端建立连接
// 原型 :bool TCFtpClient::IfConnected()
// 参数 :无
// 返回 :布尔值
// 说明 :真---已经连接，假---未连接
//==========================================================================
bool TCFtpClient::IfConnected()
{
    if( !m_CmdSock.IfConnected() )
        m_bLogin = false;
    return m_CmdSock.IfConnected();
}

//==========================================================================
// 函数 :IsIPString
// 用途 :判断输入的HOST值是否为IP地址串
// 原型 :bool TCFtpClient::IsIPString()
// 参数 :无
// 返回 :布尔值
// 说明 :真-值为IP地址串，假-值为主机名称
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
// 函数 :IfExecuteSuccess
// 用途 :判断FTP命令回应串的信息含义，以辨别命令执行是否成功。
// 原型 :void TCFtpClient::IfExecuteSuccess()
// 参数 :无
// 返回 :
// 说明 :如果有错误则抛例外
//==========================================================================
void TCFtpClient::IfExecuteSuccess()
{
    int nErrorCode;
    m_sResponse = m_CmdSock.ReadLine();
    if( !IfConnected() )
        throw TCFTPException(ftpTransferError,"-未建立连接");
    if( m_bShowMsg )
        printf("%s\r\n",(char *)m_sResponse);
    if( Length(m_sResponse) < 3 )
        throw TCFTPException(ftpReturnSyntaxError,"-回应格式错误{"+m_sResponse+"}");
    else
    {
        nErrorCode = StrToInt(Mid(m_sResponse,1,3));
        if( nErrorCode==230 )  m_bLogin = true;
        if( nErrorCode==221 )  m_bLogin = false;
       	if ( (nErrorCode <= 0)||(m_sResponse[1] == '4') || (m_sResponse[1] == '5') )
             throw TCFTPException(ftpCommandError,"-执行命令失败{"+m_sResponse+"}");
    }

}

//==========================================================================
// 函数 : List
// 用途 : 列出远端主机上文件的详细信息
// 原型 : long TCFtpClient::List(TCString sFilter)
// 参数 : 文件名过滤条件串（如："A??.*")
// 返回 : 列表行数
// 说明 : 如执行失败则抛出例外，列表将存在类的私有成员m_lsFileInfo当中
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
// 函数 :Mode
// 用途 :设置文件传输模式
// 原型 :void TCFtpClient::Mode(TEFtpMode eFtpMode)
// 参数 :枚举TEFtpMode{fmASCII,fmIMAGE,fmBYTE}
// 返回 :
// 说明 :
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
// 函数 :Mode
// 用途 :设置文件传输模式
// 原型 :void TCFtpClient::Mode(TCString sMode)
// 参数 :传输模式
// 返回 :
// 说明 :
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
// 函数 :MakeDirectory
// 用途 :在远端主机上建立目录
// 原型 :void TCFtpClient::MakeDirectory(TCString sDirectoryName)
// 参数 :建立的目录名
// 返回 :无
// 说明 :如执行失败则DoCommand中抛出例外
//==========================================================================
void TCFtpClient::MakeDirectory(TCString sDirectoryName)
{
   DoCommand("MKD "+ sDirectoryName);
}

//==========================================================================
// 函数 : NameList
// 用途 : 列出远端主机上当前目录的文件名称
// 原型 : void TCFtpClient::NameList(TCStringList& lsName,TCString sFilter)
// 参数 : 列表存放对象，文件名过滤条件串（如："A??.*")
// 返回 : 无
// 说明 : 如执行失败则抛出例外
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
// 函数 :Open
// 用途 :登录到FTP服务端
// 原型 :void TCFtpClient::Open()
// 参数 :无
// 返回 :
// 说明 :调用之前须先设置主机名称或者地址以及用户名和口令。
//==========================================================================
void TCFtpClient::Open()
{
    Close();
    if( m_sHost == TCString("") )
        throw TCFTPException(ftpLoginError,"-未设置FTP主机错误");
    if( IsIPString() )
        m_CmdSock.m_sAddress = m_sHost;
    else
        m_CmdSock.m_sAddress = TCPowerSocket::GetHostIP(m_sHost);
    m_CmdSock.m_nPort = m_nPort;
    if( !m_CmdSock.Open() )
        throw TCFTPException(ftpConnectError,"-无法建立连接，请检查主机设置和网络状态");
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
        throw TCFTPException(ftpLoginError,"-登录失败，请检查用户名和口令");
    }
    Mode(m_eFtpMode);
}

//==========================================================================
// 函数 :PrepareDataTransfers
// 用途 :准备传输数据，发送命令并建立数据连接通道
// 原型 :void TCFtpClient::PrepareDataTransfers(TCString sCommand)
// 参数 :执行的命令---该命令必须是执行后需要处理数据的命令
// 返回 :
// 说明 :命令返回数据传输的地址和端口，格式为(h1,h2,h3,h4,p1,p2)
//==========================================================================
void TCFtpClient::PrepareDataTransfers(TCString sCommand)
{
   long nStart,nEnd;
   TCStringList lsDataStr;
   if( !m_bLogin )
       throw TCFTPException(ftpCommandError,"-未登录");
   DoCommand("PASV");
   nStart = m_sResponse.Find('(');
   nEnd = m_sResponse.ReverseFind(')');
   if( ( nStart == -1 )||( nEnd == -1 ) )
        throw TCFTPException(ftpReturnSyntaxError,"-回应格式错误{"+m_sResponse+"}");
   lsDataStr.CommaText(m_sResponse.Mid(nStart+1,nEnd-nStart),',');
   if( lsDataStr.GetCount() != 6 )
        throw TCFTPException(ftpReturnSyntaxError,"-回应格式错误{"+m_sResponse+"}");
   m_DataSock.m_sAddress = lsDataStr[0]+'.'+lsDataStr[1]+'.'+lsDataStr[2]+'.'+lsDataStr[3];
   m_DataSock.m_nPort = StrToInt(lsDataStr[4])*256+StrToInt(lsDataStr[5]);

   sCommand = sCommand + "\r\n";
   if( !m_CmdSock.Write((char *)sCommand,Length(sCommand)) )
        throw TCFTPException(ftpTransferError,"-发送指令意外中断{"+sCommand+"}");
   m_DataSock.Close();
   m_DataSock.Open(false);
   IfExecuteSuccess();
}

//==========================================================================
// 函数 : RemoveDir
// 用途 : 删除远程主机上一目录，该目录必须是空目录
// 原型 : void TCFtpClient::RemoveDir(TCString sDirectoryName)
// 参数 : 要删除的目录名称
// 返回 : 无
// 说明 : 如执行失败则DoCommand中抛出例外
//==========================================================================
void TCFtpClient::RemoveDir(TCString sDirectoryName)
{
   DoCommand("RMD "+ sDirectoryName);
}

//==========================================================================
// 函数 : Rename
// 用途 : 更改远程主机上的文件名
// 原型 : void TCFtpClient::Rename(TCString sSrcFilename,TCString sDestFileName)
// 参数 : 源文件名称，目标文件名称
// 返回 : 无
// 说明 : 如执行失败则DoCommand中抛出例外
//==========================================================================
void TCFtpClient::Rename(TCString sSrcFilename,TCString sDestFileName)
{
  DoCommand("RNFR "+sSrcFilename);
  DoCommand("RNTO "+sDestFileName);
}

//==========================================================================
// 函数 :SetConnectType
// 用途 :设置连接方式
// 原型 :void TCFtpClient::SetConnectType(TEConnectType eConnectType)
// 参数 :连接方式---非阻塞方式或阻塞方式
// 返回 :无
// 说明 : 参数值---ctNonBlocking或者ctBlocking
//==========================================================================
void TCFtpClient::SetConnectType(TEConnectType eConnectType)
{
    m_CmdSock.SetConnectType(eConnectType);
    m_DataSock.SetConnectType(eConnectType);
}

//==========================================================================
// 函数 :SetTimeOut
// 用途 :设置超时时间
// 原型 :void TCFtpClient::SetTimeOut(long nValue)
// 参数 :超时时间数值
// 返回 :无
// 说明 :单位毫秒
//==========================================================================
void TCFtpClient::SetTimeOut(long nValue)
{
    m_CmdSock.m_Timeout = nValue;
    m_DataSock.m_Timeout = nValue;
}

//==========================================================================
// 函数 : Size
// 用途 : 得到远程主机上的文件大小
// 原型 : long TCFtpClient::Size(TCString sFilename)
// 参数 : 文件名称
// 返回 : 无
// 说明 : 如执行失败则DoCommand中抛出例外
//==========================================================================
long TCFtpClient::Size(TCString sFilename)
{
    DoCommand("SIZE "+sFilename);

    TCStringList slResponseList;
    slResponseList.CommaText(m_sResponse, ' ');
    if(slResponseList.GetCount() != 2 )
        throw TCFTPException(ftpTransferError,"返回的格式不正确!");
    if(!IsNumber(slResponseList[1]))
        throw TCFTPException(ftpTransferError,"返回的格式不正确!");
    long nSize = StrToInt(slResponseList[1]);
    return nSize;
}

//==========================================================================
// 函数 :Upload
// 用途 :上传文件
// 原型 :void TCFtpClient::Upload(TCString sLocalFile,TCString sRemoteFile)
// 参数 :本地文件名，远端主机文件名
// 返回 :无
// 说明 :如上传过程有错则抛例外
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
            throw TCFTPException(ftpTransferError,"-数据上传意外中断，请检查网络状态");
       nSize = cSrcFile.Read(m_DataSock.m_sBuf,sizeof(m_DataSock.m_sBuf));
   }
   m_DataSock.Close();
   if( m_bShowMsg )
       printf("数据传输完毕\r\n");
   IfExecuteSuccess();
}

//==========================================================================
// 函数 :UploadFile
// 用途 :上传文件,上传时打开文件并锁定
// 原型 :void TCFtpClient::UploadFile(TCString sLocalFile,TCString sRemoteFile)
// 参数 :本地文件名，远端主机文件名
// 返回 :无
// 说明 :如上传过程有错则抛例外
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
            throw TCFTPException(ftpTransferError,"-数据上传意外中断，请检查网络状态");
       nSize = cSrcFile.Read(m_DataSock.m_sBuf,sizeof(m_DataSock.m_sBuf));
   }
   m_DataSock.Close();
   if( m_bShowMsg )
       printf("数据传输完毕\r\n");
   IfExecuteSuccess();
}

//==========================================================================
// 函数 : TCFTPException
// 用途 : FTP例外管理类的构造函数
// 原型 : TCFTPException::TCFTPException(TEFTPErrorCode ftpErrorStatus, TCString sMsg)
// 参数 : 错误类型（TEFTPErrorCode），错误信息描述串
// 返回 : 无
// 说明 :
//==========================================================================
TCFTPException::TCFTPException(TEFTPErrorCode ftpErrorStatus, TCString sMsg):TCException(sMsg)
{
   m_ftpErrorStatus = ftpErrorStatus;
}
