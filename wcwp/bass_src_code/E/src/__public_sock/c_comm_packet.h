#ifndef c_comm_packetH
#define c_comm_packetH
#define SOCK_MAX_MSG_SIZE   0x2000
#define SOCK_TAIL_FLAG    '$'

#include "cmpublic.h"
#include "c_socket_thread.h"

//ͨѶ��ͷ�ṹ
struct TSPacketHeader
{
char Signature[2] ;    // The Packet header's Signature must be 0xf00e;
char PacketType[4] ;   // 0x100 ��ʾΪע���
                       // 0x101 Ϊע���Ӧ��
                       // 0x1000Ϊ�����¼���
                       // 0x1001Ϊ�����¼���Ӧ��
                       // 0x2000Ϊϵͳ���ư�
                       // 0x2001Ϊϵͳ���ƻ�Ӧ��
                       // 0x3000Ϊ��Ϣ֪ͨ��
                       // 0x3001Ϊ��Ϣ��Ӧ��
                       // 0x5000Ϊ�¼�������
                       // 0x5001Ϊ�¼�����Ӧ���
                       // 0x9000Ϊͬ����
char PacketSize[4];
};


//�����¼���Ϣ�ṹ��
struct TSEventInfo
{
    char DestNodeID[4];    // Ŀ��ڵ�
    char SrcNodeID[4];     // Դ�ڵ�
    char EventCode[4];     // �¼�����
    char Order[4];
};

struct TSNPacketInfo
{
    long PacketType;
    long PacketSize;
    long DestNodeID;    // Ŀ��ڵ�
    long SrcNodeID;     // Դ�ڵ�
    long EventCode;     // �¼�����
    long Order;
};

#define  DS_Register        0x100  //ע���
#define  DS_RegisterAsk     0x101  //ע���Ӧ��
#define  DS_Monitor         0x1000 //����¼���
#define  DS_MonitorAsk      0x1001 //����¼���Ӧ��
#define  DS_SysControl      0x2000 //ϵͳ���ư�
#define  DS_SysControlAsk   0x2001 //ϵͳ���ƻ�Ӧ��
#define  DS_NotifyEvent     0x3000 //��Ϣ�¼�֪ͨ�����
#define  DS_NotifyEventAsk  0x3001 //��Ϣ��Ӧ��
#define  DS_ItemInfo        0x5000 //��Ŀ������
#define  DS_ItemInfoAsk     0x5001 //��Ŀ����Ӧ���
#define  DS_UploadFile      0x6000 //�ϴ��ļ�
#define  DS_DownloadFile    0x6002 //�����ļ�
#define  DS_DownloadFileAsk 0x6003 //�����ļ�
#define  DS_Heartbeat       0x9000 //ͬ����



#define  _INSPECT_NODE     0x110
#define  _OBJECT_NODE      0x100
#define  _ALONE_NODE       0x111


#define _MAX_ITEM_EVENT_CODE   0x8000
//�����Ŀ�б���¼�������ڸ���Ŀ���б��е�λ�ã�
//Զ�����ܴﵽ���ߴ���0x8000,����������������¼�������Զ�����0x8000֮��

//DS_DownloadFile  �ļ���������
//1���������ļ� EventCode = �ļ�ģʽ �ı��ļ���omText���������ļ���omBinary��
//             Order = ��ʼ����λ��
//             �������صĴ�·�����ļ�����
//DS_DownloadFileAsk
//2����Ӧ         EventCode = _NOTIFY_SUCCESS,_NOTIFY_FAIL
//             Order = ���ļ�����ֽ���
//             ����ɹ��� �����Ƿ��͵���Ŵ�"\0"�����������ļ���λ�ô�"\0"��������
//             �ļ���������
//DS_DownloadFileAsk
//3����Ӧ         EventCode = _NOTIFY_SUCCESS,_NOTIFY_FAIL
//             Order = ��ʼ����λ��
//             ����ɹ� �����ʱ����շ��ȽϿ�ʼ���ص�λ���Ƿ��뵱ǰ��ָ����ͬ�����ͬ
//             ���ļ�����δ������������ڶ������� �������ͬ����һʧ����ϢȻ���������
//             �ļ����ز���

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
