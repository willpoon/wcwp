//---------------------------------------------------------------------------

#ifndef c_power_socketH
#define c_power_socketH

#ifdef __WIN32__
#include <windows.h>
#else
#include <synch.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <signal.h>
#include <fcntl.h>
#endif
#include "cmpublic.h"
#include <errno.h>
//---------------------------------------------------------------------------
//��   : TCPowerSocket
//�������ڷ�װһ������TCP/IP��SCOKET����Ӧ�á�
//���� ��
//    . �������������߷�������ʽ����
//    . �����÷��ͻ��߽��յĳ�ʱʱ��
//    . �򵥵���Open��Close�����������Ӻ͹ر�����
//    . �Է�������ʽ�������ݣ�������ʽ���Ӷ�дһ�����ڶ��߳�Ӧ�ã���ο�TCSocketThread��
//    . ѭ������ָ��������ֱ�������͵����ݷ������
//    . ��¼���ͺͽ��յ����ֽ���
//    . ��ѯ�˿��Ƿ������ݿɶ�����Ϊ��д״̬
//    . ��WINDOWSӦ�����Զ���ʼ��SOCKET���ӿ�
//���÷�ʽ����:
//  main(int argc, char* argv[])
//  {


//  }

enum TEConnectType { ctNonBlocking, ctBlocking };

class TCPowerSocket
{
  public :
    static TCString HostName();
    static TCString GetHostIP(TCString sHostName = "");
   protected :
     int m_sock ;
     long m_nBytesSent;
     long m_nBytesRecvd;
     TEConnectType  m_ConnectType;  //ָ�����������߷�����ģʽ����
     bool m_Connected ;

   public:
     TCString m_sAddress ;          //���ӵ�IP��ַ
     unsigned short m_nPort ;        //���ӵĶ˿�
     unsigned long m_Timeout ;      //��ʱʱ��
     char m_sBuf[MAX_LINE];
     bool IfConnected(){ return m_Connected; };
   public :
     TCPowerSocket(int sock = -1);
     ~TCPowerSocket();
     bool Open(bool bSetLinger = true);
     void Close();
     void SetConnectType(TEConnectType eConnectType);

     virtual TCString ReadLine() ;
     virtual long Read(void * pBuf,int nSize) ;
     virtual bool Write(void * pBuf,int nSize);
     bool WaitForData(unsigned long nTimeout);
     bool WaitForSend(unsigned long nTimeout);
     long BytesSent();
     long BytesRecvd();
     int GetSock();
   protected :
     void CheckError();
     void SetLinger();
     void ConnectTo(bool bSetLinger) ;
     void Disconnect() ;
};
void  InitSocket();
void  CleanupSocket();
#endif
