//---------------------------------------------------------------------------

#ifndef c_monitor_item_analyzeH
#define c_monitor_item_analyzeH
#include "cmpublic.h"
#include "c_thread.h"
//---------------------------------------------------------------------------

struct TSMItemInfo
{
   TCString sName ;
   TCString sTypeName ;
   TCString sSource ;
   TCString sTime_Section ;
   TCString sConfig ;
   TCString sCommand ;
   TCString Status;
   TCString Attribute ;
   TCString sFieldNames;
   TCString sInfoField ;
   TCString sInfoData;
   time_t report_time;
};

class TCMItemAnalyze ;

class TCMonitorConfig
{
//------------2001-2-19 ADD-----------------------------------------------------
  private:
    TCList m_lMItemAnalyze;
    void FreeAll();
//------------------------------------------------------------------------------

  public:
    static void GetConnectAddress(TCString& sAddress,unsigned short& nPort);

//------------2001-2-19 ADD-----------------------------------------------------
    static void GetNodeNames(TCStringList & slNode);
//------------------------------------------------------------------------------

    static TCString GetPassword();
    static unsigned short GetListenPort();
    static void GetIncludeList(TCStringList& sItemList,TCString sNodeName);
    static void GetItemListByInclude(TCString sItemName,TCStringList& sSubItemList);
    static void GetItemInfo(TCString sItemName,TCString sSubItemName,
                        TSMItemInfo * pItemInfo);
    static long GetDefaultWarnTime();
    static void IfNullStringTerminate(TCString szItemName,TCString szValue);
//------------2001-2-19 ADD-----------------------------------------------------
  public :
    TCMonitorConfig();
    ~TCMonitorConfig();
    long LoadAll();
    TCMItemAnalyze * GetByIndex(long nIndex);
    long GetCount();
//------------------------------------------------------------------------------

};



//��Ŀ�б������� ��
//        ע��ɹ�ʱ�ɷ���˷��͵�ǰ�Ѿ����յĶ��г��ȣ�
//        �ͻ��˿ɼ����Ƿ��Ѿ�ȫ��������ϣ���������ɡ�
//        ����ͻ�������������֪ͨ��������µǼǱ��ڵ�
//        ��Ŀ�б�
//        ��������Ϣ����ʱ��Ҳ��֪ͨ��������µǼǱ��ڵ�
//        ��Ŀ�б�

class TCMItemAnalyze : public TCList
{
   private:
      pthread_mutex_t m_mut;

   public:
     long m_nNodeID;
     TCString m_sNodeName;
     TCString m_sPassword;
     TCString m_sAddress;

 //ָ����Ŀ״̬ˢ�µ�ʱ��
     time_t mtRefreshTime;
 //ָ����Ӧ�Ľڵ������״̬����ʱ�䣬���ڼ�ط�����
     time_t mtReference;
     bool m_bActive;

     void mut_lock();
     void mut_unlock();
     long GetWarnLevel(TCString sStatus);

   public:
     time_t m_tmCreate;
     TCMItemAnalyze();
     ~TCMItemAnalyze();
     //��INI�����ļ�ȡ���������е���Ŀ
     void Load(TCString sNodeName);
     //����Ŀ���ݴ��û��з������Ա㷢��
     long FillToBuffer(long & nCount,char *pBuf,long nMaxSize);

     TSMItemInfo * GetItem(long nIndex);
     //�����յ�����Ŀ���ݴ��������
     bool AddToQueue(char *pBuf);

    //����ʱ��ָ��״̬�Ƿ�ı�
     bool FillStatus(long nIndex,char *pBuf);

     void Clear();


};

#endif
