#ifndef c_comm_packetH
#define c_comm_packetH
#define SOCK_MAX_MSG_SIZE   0x2000
#define SOCK_TAIL_FLAG    '$'

#include "cmpublic.h"
#include "c_socket_thread.h"

//通讯包头结构
struct TSPacketHeader
{
char Signature[2] ;    // The Packet header's Signature must be 0xf00e;
char PacketType[4] ;   // 0x100 表示为注册包
                       // 0x101 为注册回应包
                       // 0x1000为报警事件包
                       // 0x1001为报警事件回应包
                       // 0x2000为系统控制包
                       // 0x2001为系统控制回应包
                       // 0x3000为消息通知包
                       // 0x3001为消息回应包
                       // 0x5000为事件描述包
                       // 0x5001为事件描述应答包
                       // 0x9000为同步包
char PacketSize[4];
};


//基本事件消息结构：
struct TSEventInfo
{
    char DestNodeID[4];    // 目标节点
    char SrcNodeID[4];     // 源节点
    char EventCode[4];     // 事件编码
    char Order[4];
};

struct TSNPacketInfo
{
    long PacketType;
    long PacketSize;
    long DestNodeID;    // 目标节点
    long SrcNodeID;     // 源节点
    long EventCode;     // 事件编码
    long Order;
};

#define  DS_Register        0x100  //注册包
#define  DS_RegisterAsk     0x101  //注册回应包
#define  DS_Monitor         0x1000 //监控事件包
#define  DS_MonitorAsk      0x1001 //监控事件回应包
#define  DS_SysControl      0x2000 //系统控制包
#define  DS_SysControlAsk   0x2001 //系统控制回应包
#define  DS_NotifyEvent     0x3000 //消息事件通知请求包
#define  DS_NotifyEventAsk  0x3001 //消息回应包
#define  DS_ItemInfo        0x5000 //项目描述包
#define  DS_ItemInfoAsk     0x5001 //项目描述应答包
#define  DS_UploadFile      0x6000 //上传文件
#define  DS_DownloadFile    0x6002 //下载文件
#define  DS_DownloadFileAsk 0x6003 //下载文件
#define  DS_Heartbeat       0x9000 //同步包



#define  _INSPECT_NODE     0x110
#define  _OBJECT_NODE      0x100
#define  _ALONE_NODE       0x111


#define _MAX_ITEM_EVENT_CODE   0x8000
//针对项目列表的事件编码等于该项目在列表中的位置，
//远不可能达到或者大于0x8000,所以有其他特殊的事件编码可以定义在0x8000之后，

//DS_DownloadFile  文件下载请求
//1。请求发送文件 EventCode = 文件模式 文本文件（omText）二进制文件（omBinary）
//             Order = 开始下载位置
//             请求下载的带路径的文件名称
//DS_DownloadFileAsk
//2。回应         EventCode = _NOTIFY_SUCCESS,_NOTIFY_FAIL
//             Order = 该文件最大字节数
//             如果成功则 接着是发送的序号串"\0"数据内容在文件的位置串"\0"，接着是
//             文件数据内容
//DS_DownloadFileAsk
//3。回应         EventCode = _NOTIFY_SUCCESS,_NOTIFY_FAIL
//             Order = 开始下载位置
//             如果成功 则这个时候接收方比较开始下载的位置是否与当前的指针相同如果相同
//             且文件数据未发送完则继续第二个步骤 ，如果不同则发送一失败消息然后结束本次
//             文件下载操作

#define _NOTIFY_NODE_STATUS  0x8001
#define _NOTIFY_BUSY         0x8002
#define _NOTIFY_CANCEL       0x8003
#define _NOTIFY_FAIL         0x8004
#define _NOTIFY_SUCCESS      0x8005
#define _NOTIFY_REFRESH_ITEM 0x8006

#define _CONTROL_RESET_SERVER 0x8888

//---------------------------------------------------------------------------

class TCCommPacket:public TCSocketThread
{
  private:
    char m_SBuffer[SOCK_MAX_MSG_SIZE] ;
    TSPacketHeader * m_pPacketHeader;
    TSEventInfo * m_pEventInfo;
  protected:
    char * m_pSBuf ;
    long m_nDataLen ;
    long m_nNodeID;
    long m_DestNodeID;
    TSNPacketInfo m_snPacketInfo;
  public:
    TCCommPacket();
    ~TCCommPacket();
    long GetMaxFillSize();
    long GetNodeID() { return m_nNodeID; }

  protected:

    void FillPacketHeader(long PacketType,long nDataSize,
                                       bool bIncludeEventPacketSize = true);
    void FillEventInfo(long EventCode,long Order,long nSrcNodeID,long nDestNodeID);
    bool CheckPacketHeader();
    void SendHeartbeat();
    bool CheckTailFlag();

    void ReceiveEvent();
    void SendEvent();

    virtual void FillRegisterInfo();
    virtual void RegisterToCenter();
    virtual void DoRegister();

};

#endif
