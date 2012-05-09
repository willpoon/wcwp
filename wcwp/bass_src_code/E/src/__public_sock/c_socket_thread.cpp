//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_socket_thread.h"
#include "c_power_socket.h"

//---------------------------------------------------------------------------
#pragma package(smart_init)

TCSocketThread::TCSocketThread()
{
   InitSocket();
   m_Connected = false ;
   m_sock = -1 ;
   m_Timeout = 60000 ;
   m_timer = time(NULL);
}

TCSocketThread::~TCSocketThread()
{
   Disconnect() ;
}

void TCSocketThread::ClientCreate(TCString sAddress,unsigned short nPort,int sock)
{
   m_sAddress = (char *)sAddress ;
   m_Port = nPort ;
   m_sock = sock ;
   if (m_sock != -1)
      m_Connected = true ;
   Create();
}

void TCSocketThread::CheckError()
{
  if( (m_sock == -1)||(!m_Connected) ){ Terminate(); return; }

  int nErrorCode,nLen ;
  nLen = sizeof(nErrorCode) ;
  if( getsockopt(m_sock,SOL_SOCKET,SO_ERROR,(char*)&nErrorCode,&nLen) != 0 )
     Terminate();
  if( nErrorCode != 0 )
      Terminate();
}

bool TCSocketThread::Read(void * pBuf,int nSize)
{
  int nMaxCountPerRead,nTemp,nCount ;
  //每次最多读100个字节
  nMaxCountPerRead = 100;
  while( (!gTerminateProgramFlag)&&(!Terminated)&& m_Connected )
  {
      if( !WaitForData(m_Timeout) )
         break ;
      if( nSize > nMaxCountPerRead )
         nCount = nMaxCountPerRead;
      else nCount = nSize;
      nTemp = recv(m_sock,(char*)pBuf,nCount,0);
      if( nTemp > 0 )
      {
        nSize = nSize-nTemp;
        if( nSize <= 0 )
        {
            m_timer = time(NULL);
            return true ;
        }
        pBuf = (char*)pBuf + nTemp;
      }
      else break ;
  }
  Terminate();
  return false;
}

bool TCSocketThread::Write(void * pBuf,int nSize)
{
  int nCount ;
  while( (!gTerminateProgramFlag)&&(!Terminated)&& m_Connected )
  {
      if( !WaitForSend(m_Timeout) )
          break ;
      if( nSize > 1024 )
          nCount = 1024 ;
      else nCount = nSize ;
      nCount = send(m_sock,(char*)pBuf,nCount,0 );
      if (nCount < 0) break ;
      nSize = nSize-nCount;
      if( nSize <= 0 )
      {
         m_timer = time(NULL);
         return true;
      }
      else pBuf = (char *)pBuf + nCount;
  }
  Terminate();
  return false;
}

bool TCSocketThread::WaitForData(unsigned long nTimeout)
{
  fd_set FDSet ;
  struct timeval timeout;
  timeout.tv_sec = nTimeout/1000;
  timeout.tv_usec = (nTimeout%1000) * 1000;
  FD_ZERO(&FDSet);
  FD_SET((unsigned int)m_sock, &FDSet);
  return ( select(m_sock+1, &FDSet, NULL,NULL, &timeout)>0 ) ;
}

bool TCSocketThread::WaitForSend(unsigned long nTimeout)
{
  fd_set FDSet ;
  struct timeval timeout;
  timeout.tv_sec = nTimeout/1000;
  timeout.tv_usec = (nTimeout%1000) * 1000;
  FD_ZERO(&FDSet);
  FD_SET((unsigned int)m_sock, &FDSet);
  return (select(m_sock+1,NULL,&FDSet,NULL, &timeout)>0);
}

void TCSocketThread::ConnectTo()
{
  struct sockaddr_in addr;
  memset(&addr,'\0',sizeof(addr));
  m_sock = socket(AF_INET,SOCK_STREAM,0) ;
  if( m_sock < 0 )
  {  m_sock = -1;
     return;
  }
  addr.sin_family = AF_INET;
  addr.sin_port = htons(m_Port) ;
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
     if (ioctlsocket(m_sock, FIONBIO, &smode) != 0)
#else
     int status ;
     status = fcntl(m_sock, F_GETFL) ;
     if(fcntl(m_sock, F_SETFL,(status&~O_NONBLOCK)&~O_NDELAY) != 0)
#endif
     {   Disconnect();
         return;
     }
     else
     {
         m_Connected = true ;
         SetLinger();
     }
  }
}

void TCSocketThread::SetLinger()
{
   struct linger s_linger;
   s_linger.l_onoff = 1;
   s_linger.l_linger = 0;
   setsockopt(m_sock,SOL_SOCKET,SO_LINGER,(char *)&s_linger,sizeof(s_linger));
}

void TCSocketThread::Disconnect()
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

void TCSocketThread::Execute()
{
   if( !m_Connected )
        ConnectTo();
   CheckError();
   ClientExecute();      //%%%%%%
   Disconnect();
}

void TCSocketThread::ClientExecute()
{

}


