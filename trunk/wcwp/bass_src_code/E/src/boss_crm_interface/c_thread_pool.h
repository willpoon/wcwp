//---------------------------------------------------------------------------

#ifndef c_thread_poolH
#define c_thread_poolH
//---------------------------------------------------------------------------
#include "c_thread_run.h"
#include "cmpublic_db.h"

//---------------------------------------------------------------------------
// 类 　: TCThreadPool
// 用途 : 实现程序的线程池，目前没有实现线程池的动态调整
//---------------------------------------------------------------------------
class TCThreadPool
{
private:
    void DeleteThread(long nIdx);

    void Monitor();
protected:
    long mnMaxThread;                   //最大线程数
    bool mbRecordRunTime;               //记录脚本中SQL的运行时间
    bool mbSendErrorMsg;               //单个脚本中SQL执行发生的错误时发送短信给相关人员

    TCCriticalSection   m_csListLock;
    TCList m_lstThreadList;

    void AddThread(TCThreadRun *pThreadRun);
public:
    TCThreadPool();
    ~TCThreadPool();
    
    void Initialize();
    void Clear();
    void ClearFinishedThread();

    virtual TCThreadRun *GetNewAThread()=0;
    virtual void DestroyAThread(TCThreadRun *pThreadRun)=0;

    TCThreadRun * GetThreadRun(long nIdx);
    //long GetThreadCount();
    //void SetThreadCount(long nMaxThread, long nMinThread);
};

#endif
 