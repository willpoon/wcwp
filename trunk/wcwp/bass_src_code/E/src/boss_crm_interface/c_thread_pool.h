//---------------------------------------------------------------------------

#ifndef c_thread_poolH
#define c_thread_poolH
//---------------------------------------------------------------------------
#include "c_thread_run.h"
#include "cmpublic_db.h"

//---------------------------------------------------------------------------
// �� ��: TCThreadPool
// ��; : ʵ�ֳ�����̳߳أ�Ŀǰû��ʵ���̳߳صĶ�̬����
//---------------------------------------------------------------------------
class TCThreadPool
{
private:
    void DeleteThread(long nIdx);

    void Monitor();
protected:
    long mnMaxThread;                   //����߳���
    bool mbRecordRunTime;               //��¼�ű���SQL������ʱ��
    bool mbSendErrorMsg;               //�����ű���SQLִ�з����Ĵ���ʱ���Ͷ��Ÿ������Ա

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
 