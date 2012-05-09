//---------------------------------------------------------------------------

#ifndef c_socket_threadH
#define c_socket_threadH
#include "c_thread.h"
//---------------------------------------------------------------------------
class TCSocketThread : public TCThread
{
  protected :
    int m_sock ;
    TCString m_sAddress ;
    unsigned short m_Port ;
    bool m_Connected ;
    unsigned long m_Timeout ;

  public :
    TCSocketThread();
    ~TCSocketThread();
    void ClientCreate(TCString sAddress,unsigned short nPort,int sock = -1) ;

  protected :
    time_t m_timer;
    void CheckError();
    void SetLinger();
    bool Read(void * pBuf,int nSize) ;
    bool Write(void * pBuf,int nSize);
    bool WaitForData(unsigned long nTimeout);
    bool WaitForSend(unsigned long nTimeout);
    void ConnectTo() ;
    void Disconnect() ;
    virtual void Execute() ;
    virtual void ClientExecute() ;
};


#endif
