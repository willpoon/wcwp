//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_power_socket.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)


TCString TCPowerSocket::HostName()
{
  TCString sHostName ;
  char sStr[256] ;
  if (gethostname(sStr,sizeof(sStr)-1)!= 0)
     throw TCException("unknown host name!!!");
  sHostName = sStr ;
  return sHostName ;
}

TCString TCPowerSocket::GetHostIP(TCString sHostName)
{
  TCString IPStr ;
  char sStr[15] ;
  struct hostent *hostin ;
  if( sHostName == TCString("") )
      sHostName = HostName();
  if( (hostin = gethostbyname(sHostName)) == NULL)
     throw TCException("Error gethostbyname()");
  sprintf(sStr ,"%u.%u.%u.%u",BYTE(hostin->h_addr[0]),BYTE(hostin->h_addr[1]),
          BYTE(hostin->h_addr[2]),BYTE(hostin->h_addr[3])) ;
  IPStr = sStr ;
  return IPStr ;
}

TCPowerSocket::TCPowerSocket(int sock)
{
   InitSocket();
   m_Connected = false ;
   m_sock = sock ;
   m_Timeout = 60000 ;
   m_nBytesSent = 0;
   m_nBytesRecvd = 0;
   m_ConnectType = ctNonBlocking;
}

TCPowerSocket::~TCPowerSocket()
{
   Disconnect();
}

//==========================================================================
// 函数 : Open()
// 用途 : 建立SOCKET连接
// 原型 : bool TCPowerSocket::Open(bool bSetLinger = true)
// 参数 :
// 返回 : 是否连接成功
// 说明 :
//==========================================================================
bool TCPowerSocket::Open(bool bSetLinger)
{
   if( m_sock == -1 )
       ConnectTo(bSetLinger);
   CheckError();
   return m_Connected;
}

//==========================================================================
// 函数 : Close()
// 用途 : 关闭连接
// 原型 : void TCPowerSocket::Close()
// 参数 :
// 返回 :
// 说明 :
//==========================================================================
void TCPowerSocket::Close()
{
   Disconnect() ;
   m_nBytesSent = 0;
   m_nBytesRecvd = 0;
}

//==========================================================================
// 函数 : BytesSent()
// 用途 : 取得发送的总字节数
// 原型 : long TCPowerSocket::BytesSent()
// 参数 :
// 返回 : 发送的总字节数
// 说明 :
//==========================================================================
long TCPowerSocket::BytesSent()
{
  return m_nBytesSent;
}
//==========================================================================
// 函数 : BytesRecvd()
// 用途 : 取得接收的总字节数
// 原型 : long TCPowerSocket::BytesRecvd()
// 参数 :
// 返回 : 接收的总字节数
// 说明 :
//==========================================================================
long TCPowerSocket::BytesRecvd()
{
  return m_nBytesRecvd;
}

//==========================================================================
// 函数 : GetSock()
// 用途 : 取得sock描述符
// 原型 : int TCPowerSocket::GetSock()
// 参数 :
// 返回 : sock描述符
// 说明 :
//==========================================================================
int TCPowerSocket::GetSock()
{
  return m_sock;
}

//==========================================================================
// 函数 : SetConnectType
// 用途 : 接收数据
// 原型 : void TCPowerSocket::SetConnectType(TEConnectType eConnectType)
// 参数 :
// 返回 :
// 说明 :
//==========================================================================
void TCPowerSocket::SetConnectType(TEConnectType eConnectType)
{
   if( m_Connected )
       throw TCException("Error SetConnectType() - Connected");
   m_ConnectType = eConnectType;
}
//==========================================================================
// 函数 : ReadLine
// 用途 : 接收数据
// 原型 : TCString TCPowerSocket::ReadLine()
// 参数 :
// 返回 : 字符串
// 说明 : 读一以回车换行结束的串，如果没有接收到则断开连接
//==========================================================================
TCString TCPowerSocket::ReadLine()
{
  char * pBuf;
  pBuf = m_sBuf;
  while( m_Connected )
  {
      if( !WaitForData(m_Timeout) )
         break ;
      if( recv(m_sock,pBuf,1,0) == 1 )
      {
        if( *pBuf == '\n' )
        {  if( *(pBuf-1) == '\r' )
                return TCString(m_sBuf,pBuf - m_sBuf - 1);
           else return TCString(m_sBuf,pBuf - m_sBuf + 1);
        }
        else
        {
          pBuf = pBuf + 1;
          if( (pBuf - m_sBuf) >= sizeof(m_sBuf) )
               break;
        }
      }
      else break ;
  }
  Disconnect();
  return "";
}
//==========================================================================
// 函数 : Read
// 用途 : 接收数据
// 原型 : long TCPowerSocket::Read(void * pBuf,int nSize)
// 参数 :
// 返回 : 接收的字节数
// 说明 : 如果连接断开则返回-1
//==========================================================================
long TCPowerSocket::Read(void * pBuf,int nSize)
{
  int nMaxCountPerRead,nTemp,nCount,nSent;
  //每次最多读100个字节
  nMaxCountPerRead = 100;
  nSent = nSize;
  while( m_Connected )
  {
      if( !WaitForData(m_Timeout) )
         break ;
      if( nSize > nMaxCountPerRead )
         nCount = nMaxCountPerRead;
      else nCount = nSize;
      if( m_ConnectType == ctNonBlocking)
      {   nTemp = recv(m_sock,(char*)pBuf,nSize,0);
          m_nBytesRecvd = m_nBytesRecvd + nTemp;
          return nTemp;
      }
      else
          nTemp = recv(m_sock,(char*)pBuf,nCount,0);
      if( nTemp > 0 )
      {
        nSize = nSize-nTemp;
        m_nBytesRecvd = m_nBytesRecvd + nTemp;
        if( nSize <= 0 )
            return nSent ;
        pBuf = (char*)pBuf + nTemp;
      }
      else break ;
  }
  Disconnect();
  return -1;
}

//==========================================================================
// 函数 : Write
// 用途 : 发送数据
// 原型 : long TCPowerSocket::Write(void * pBuf,int nSize)
// 参数 :
// 返回 : 布尔值，真值：成功发送，假值：发送失败
// 说明 : 如果连接断开则返回假 ，本函数循环发送完所有请求的数据
//==========================================================================
bool TCPowerSocket::Write(void * pBuf,int nSize)
{
  int nCount ;
  while( m_Connected )
  {
      if( !WaitForSend(m_Timeout) )
          break ;
      if( nSize > 1024 )
          nCount = 1024 ;
      else nCount = nSize ;
      nCount = send(m_sock,(char*)pBuf,nCount,0 );
      //nCount = write(m_sock,(char*)pBuf,nCount);
      if (nCount < 0) break ;
      nSize = nSize-nCount;
      if( nSize <= 0 )
      {
         m_nBytesSent = m_nBytesSent+nSize;
         return true;
      }
      else pBuf = (char *)pBuf + nCount;
  }
  Disconnect();
  return false;
}

bool TCPowerSocket::WaitForData(unsigned long nTimeout)
{
  fd_set FDSet ;
  struct timeval timeout;
  timeout.tv_sec = nTimeout/1000;
  timeout.tv_usec = (nTimeout%1000) * 1000;
  FD_ZERO(&FDSet);
  FD_SET((unsigned int)m_sock, &FDSet);
  return ( select(m_sock+1, &FDSet, NULL,NULL, &timeout)>0 ) ;
}

bool TCPowerSocket::WaitForSend(unsigned long nTimeout)
{
  fd_set FDSet ;
  struct timeval timeout;
  timeout.tv_sec = nTimeout/1000;
  timeout.tv_usec = (nTimeout%1000) * 1000;
  FD_ZERO(&FDSet);
  FD_SET((unsigned int)m_sock, &FDSet);
  return (select(m_sock+1,NULL,&FDSet,NULL, &timeout)>0);
}

void TCPowerSocket::CheckError()
{
  if( (m_sock == -1)||(!m_Connected) ) return;

  int nErrorCode,nLen ;
  nLen = sizeof(nErrorCode) ;
  if( getsockopt(m_sock,SOL_SOCKET,SO_ERROR,(char*)&nErrorCode,&nLen) != 0 )
      Disconnect();
  if( nErrorCode != 0 )
      Disconnect();
}

void TCPowerSocket::SetLinger()
{
   struct linger s_linger;
   s_linger.l_onoff = 1;
   s_linger.l_linger = 0;
   setsockopt(m_sock,SOL_SOCKET,SO_LINGER,(char *)&s_linger,sizeof(s_linger));
}

void TCPowerSocket::ConnectTo(bool bSetLinger)
{
  struct sockaddr_in addr;
  memset(&addr,'\0',sizeof(addr));
  m_sock = socket(AF_INET,SOCK_STREAM,0) ;
  if( m_sock < 0 )
  {  m_sock = -1;
     return;
  }
  addr.sin_family = AF_INET;
  addr.sin_port = htons(m_nPort) ;
#ifdef __WIN32__
  if ((addr.sin_addr.s_addr = inet_addr(m_sAddress)) == INADDR_NONE ) // INADDR_ANY ;
  {  closesocket(m_sock);
#else
  if ((addr.sin_addr.s_addr = inet_addr(m_sAddress)) == (in_addr_t)-1 )
  {  close(m_sock);
#endif
     m_sock = -1;
     return;
  }
  if ( connect(m_sock,(struct sockaddr *)&addr,sizeof(addr)) != 0)
  {  Disconnect();
  }
  else
  {
#ifdef __WIN32__
     unsigned long smode = 0 ;
     if( m_ConnectType == ctNonBlocking )
        smode = 1;
     if (ioctlsocket(m_sock, FIONBIO, &smode) != 0)
#else
     int status ;
     status = fcntl(m_sock, F_GETFL) ;
     if( m_ConnectType == ctNonBlocking )
         status = status|O_NONBLOCK;
     else
         status = (status&~O_NONBLOCK)&~O_NDELAY;
     if(fcntl(m_sock, F_SETFL,status) != 0)
#endif
     {   Disconnect();
         return;
     }
     else
     {
         m_Connected = true ;
         if( bSetLinger )
             SetLinger();
     }
  }
}

void TCPowerSocket::Disconnect()
{
  if(m_sock != -1)
  {
#ifdef __WIN32__
    closesocket(m_sock);
#else
    shutdown(m_sock,SHUT_RDWR);
    close(m_sock) ;
#endif
  }
  m_sock = -1 ;
  m_Connected = false ;

}

void  InitSocket()
{
#ifdef __WIN32__
  static bRunInitSocket = true;
  if( bRunInitSocket )
  {
    WSADATA WsaData ;
    if ( WSAStartup(2,&WsaData)!=0 )
       throw TCException("Initialier socket error!!!");
    bRunInitSocket = false;
  }
#else
  return;
#endif
}

void CleanupSocket()
{
#ifdef __WIN32__
  WSACleanup();
#else
  return;
#endif
}


