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



//项目列表传输流程 ：
//        注册成功时由服务端发送当前已经接收的队列长度，
//        客户端可检验是否已经全部传送完毕，并接续完成。
//        如果客户程序新启动则通知服务端重新登记本节点
//        项目列表。
//        当配置信息更改时则也需通知服务端重新登记本节点
//        项目列表。

class TCMItemAnalyze : public TCList
{
   private:
      pthread_mutex_t m_mut;

   public:
     long m_nNodeID;
     TCString m_sNodeName;
     TCString m_sPassword;
     TCString m_sAddress;

 //指出项目状态刷新的时间
     time_t mtRefreshTime;
 //指出对应的节点的连接状态更改时间，用于监控服务器
     time_t mtReference;
     bool m_bActive;

     void mut_lock();
     void mut_unlock();
     long GetWarnLevel(TCString sStatus);

   public:
     time_t m_tmCreate;
     TCMItemAnalyze();
     ~TCMItemAnalyze();
     //从INI配置文件取出读出所有的项目
     void Load(TCString sNodeName);
     //将项目数据串用换行符相连以便发送
     long FillToBuffer(long & nCount,char *pBuf,long nMaxSize);

     TSMItemInfo * GetItem(long nIndex);
     //将接收到的项目数据串放入队列
     bool AddToQueue(char *pBuf);

    //返回时候指出状态是否改变
     bool FillStatus(long nIndex,char *pBuf);

     void Clear();


};

#endif
