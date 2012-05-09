//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_server_socket.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)

TCServerSocket g_ServerSocket;

TCServerSocket::TCServerSocket()
{
   InitSocket();
   m_Port = 8110 ;
   m_sock = -1 ;
   m_baklog = 10 ;
   m_ConnectType = ctBlocking;
}

TCServerSocket::~TCServerSocket()
{
  Close() ;
}

void TCServerSocket::Open()
{
  if( m_sock == -1 )
  {
    struct sockaddr_in addr;
    memset(&addr,'\0',sizeof(addr));
    m_sock = socket(AF_INET,SOCK_STREAM,0) ;
    if( m_sock < 0 )
       throw TCException("Cannot create socket !!!");
    addr.sin_family = AF_INET;
    addr.sin_port = htons(m_Port) ;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
/*
#ifdef __WIN32__
    if ((addr.sin_addr.s_addr = inet_addr(TCServerSocket::GetHostIP())) == INADDR_NONE ) // INADDR_ANY ;
#else
    if ((addr.sin_addr.s_addr = inet_addr(TCServerSocket::GetHostIP())) == (in_addr_t)-1 )
#endif
       throw TCException("Error inet_addr()");
*/
    if ( bind(m_sock,(struct sockaddr *)&addr,sizeof(addr)) != 0)
       throw TCException("Error bind()");
    if (listen(m_sock,m_baklog) != 0)
       throw TCException("Error listen()");
#ifdef __WIN32__
    unsigned long smode = 1 ;
    if (ioctlsocket(m_sock, FIONBIO, &smode) != 0)
#else
    int status ;
    status = fcntl(m_sock, F_GETFL) ;
    if(fcntl(m_sock, F_SETFL,status|O_NONBLOCK) != 0)
#endif
       throw TCException("Error socket mode");
    CheckError();
    SetLinger(m_sock);
  }
}
void TCServerSocket::CheckError()
{
    int nErrorCode,nLen ;
    nLen = sizeof(nErrorCode) ;
    if( getsockopt(m_sock,SOL_SOCKET,SO_ERROR,(char*)&nErrorCode,&nLen) != 0 )
       throw TCException("Error getsockopt()");
    if( nErrorCode != 0 )
       throw TCException("Error getsockopt(),ErrorCode = "+IntToStr(nErrorCode));
}
void TCServerSocket::Close()
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
}

void TCServerSocket::SetPort(unsigned short nPort)
{
  if ( m_sock == -1 )
     m_Port = nPort ;
  else
     throw TCException("Error SetPort()");
}

void TCServerSocket::SetLinger(int nsock)
{
   struct linger s_linger;
   s_linger.l_onoff = 1;
   s_linger.l_linger = 0;
   setsockopt(nsock,SOL_SOCKET,SO_LINGER,(char *)&s_linger,sizeof(s_linger));
}

void TCServerSocket::WaitForConnection(void (*Method)(TCString,unsigned short,int))
{
   int csock,nLen ;
   struct sockaddr_in addr ;
   nLen = sizeof(addr) ;
   if( m_sock != -1 )
   {
     csock = accept(m_sock,(struct sockaddr *)&addr,&nLen) ;
     if( csock > 0 )
     {
#ifdef __WIN32__
       unsigned long smode = 0 ;
       if( m_ConnectType == ctNonBlocking )
           smode = 1;
       if (ioctlsocket(csock, FIONBIO, &smode) != 0)
       {   closesocket(m_sock);
           return;
       }
#else
       int status ;
       status = fcntl(csock, F_GETFL) ;
       if( m_ConnectType == ctNonBlocking )
           status = status|O_NONBLOCK;
       else
           status = (status&~O_NONBLOCK)&~O_NDELAY;
       if(fcntl(m_sock, F_SETFL,status) != 0)
       {  shutdown(csock,SHUT_RDWR);
          close(csock) ;
          return;
       }
#endif
       SetLinger(csock);

       char * szAddr;
       szAddr = inet_ntoa(addr.sin_addr);
       Method(szAddr,m_Port,csock) ;
    }
  }
}

