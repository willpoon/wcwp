//---------------------------------------------------------------------------

#ifndef c_server_socketH
#define c_server_socketH
#include "c_power_socket.h"

//---------------------------------------------------------------------------

class TCServerSocket
{

  private :
    int m_sock ;
    unsigned short m_Port ;
    int m_baklog ;
    void CheckError();
    void SetLinger(int nsock);
  public :
    TEConnectType  m_ConnectType;

    TCServerSocket();
    ~TCServerSocket();
    void Open();
    void Close();
    void SetPort(unsigned short nPort);
    void WaitForConnection(void (*Method)(TCString,unsigned short,int)) ;
};

extern TCServerSocket g_ServerSocket;


#endif

