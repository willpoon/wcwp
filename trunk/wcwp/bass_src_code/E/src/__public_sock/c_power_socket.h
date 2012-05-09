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
//类   : TCPowerSocket
//该类用于封装一个基于TCP/IP的SCOKET连接应用。
//功能 ：
//    . 设置以阻塞或者非阻塞方式连接
//    . 可设置发送或者接收的超时时间
//    . 简单调用Open和Close方法建立连接和关闭连接
//    . 以非阻塞方式接收数据，阻塞方式连接读写一般用于多线程应用，请参考TCSocketThread类
//    . 循环发送指定的数据直到请求发送的数据发送完毕
//    . 记录发送和接收的总字节数
//    . 查询端口是否有数据可读或者为可写状态
//    . 在WINDOWS应用中自动初始化SOCKET连接库
//调用方式举例:
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
     TEConnectType  m_ConnectType;  //指定以阻塞或者非阻塞模式相连
     bool m_Connected ;

   public:
     TCString m_sAddress ;          //连接的IP地址
     unsigned short m_nPort ;        //连接的端口
     unsigned long m_Timeout ;      //超时时间
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
