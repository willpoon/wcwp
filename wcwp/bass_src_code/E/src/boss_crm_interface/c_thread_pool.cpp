//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_thread_pool.h"
//---------------------------------------------------------------------------

#pragma package(smart_init)

//==========================================================================
// ���� : TCThreadPool::TCThreadPool
// ��; : ���캯��
// ԭ�� :
// ���� :
// ���� :
// ˵�� :
//==========================================================================
TCThreadPool::TCThreadPool()
{
    //��������߳���
    mnMaxThread=14;
    mbRecordRunTime=false;
    mbSendErrorMsg=false;
}

//==========================================================================
// ���� : TCThreadPool::~TCThreadPool
// ��; : ��������������̳߳�
// ԭ�� :
// ���� : ��
// ���� :
// ˵�� :
//==========================================================================
TCThreadPool::~TCThreadPool()
{
    Clear();
}

//==========================================================================
// ���� : TCThreadPool::AddThread
// ��; : ����һ���̳߳�
// ԭ�� : AddThread(TCThreadRun *pThreadRun)
// ���� : pThreadRun -- �߳����ָ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCThreadPool::AddThread(TCThreadRun *pThreadRun)
{
    m_csListLock.Enter();
    try
    {
        if (m_lstThreadList.IndexOf(pThreadRun) < 0)
        {
            m_lstThreadList.Add(pThreadRun);
        }
    }
    catch(TCException &e)
    {
        m_csListLock.Leave();
        throw e;
    }
    catch(...)
    {
        m_csListLock.Leave();
        throw TCException("TCThreadPool::AddThread Error:"
                        "Unknown Exception.");
    }
    m_csListLock.Leave();
}

//==========================================================================
// ���� : TCThreadPool::DeleteThread
// ��; : ɾ��һ���߳�
// ԭ�� : void DeleteThread(long nIdx)
// ���� : nIdx -- �߳�����
// ���� : ��
// ˵�� :
//==========================================================================
void TCThreadPool::DeleteThread(long nIdx)
{
    m_csListLock.Enter();
    try
    {
        TCThreadRun *pThreadRun =
            (TCThreadRun *) m_lstThreadList[nIdx];
        pThreadRun->SetFreeOnTerminate(false);
        pThreadRun->Terminate();
        pThreadRun->WaitFor();
        m_lstThreadList.Delete(nIdx);
        DestroyAThread(pThreadRun);
    }
    catch(TCException &e)
    {
        m_csListLock.Leave();
        throw e;
    }
    catch(...)
    {
        m_csListLock.Leave();
        throw TCException("TCThreadPool::DeleteThread Error:"
                        "Unknow Exception.");;
    }
    m_csListLock.Leave();
}

//==========================================================================
// ���� :  TCThreadPool::GetThreadServer
// ��; :  �õ�һ�������̵߳�ָ��
// ԭ�� :  TCThreadStat * GetThreadServer(long nIdx)
// ���� :  nIdx -- �����߳�����
// ���� :  �����̵߳�ָ��
// ˵�� :
//==========================================================================
TCThreadRun * TCThreadPool::GetThreadRun(long nIdx)
{
    TCThreadRun * pThreadRun;
    m_csListLock.Enter();
    try
    {
        pThreadRun = (TCThreadRun *)m_lstThreadList[nIdx];
    }
    catch(TCException &e)
    {
        m_csListLock.Leave();
        throw e;
    }
    catch(...)
    {
        m_csListLock.Leave();
        throw TCException("TCThreadPool::GetThreadRun Error:"
                        "Unknown Exception.");
    }
    return pThreadRun;
}

//==========================================================================
// ���� : TCThreadPool::ClearFinishedThread
// ��; : ���������̲߳��ͷ���Դ
// ԭ�� : void ClearFinishedThread()
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCThreadPool::ClearFinishedThread()
{
    m_csListLock.Enter();
    try
    {
        TCThreadRun * pThreadRun;
        long i, nCount;
        nCount = m_lstThreadList.GetCount();
        for (i=nCount - 1;i>=0;i--)
        {
            pThreadRun = (TCThreadRun *)m_lstThreadList[i];

            if (pThreadRun->GetFinished())
            {
                //==== 1 �����߳�FreeOnTerminateΪfalse =======
                pThreadRun->SetFreeOnTerminate(false);

                //==== 2 ʹ�߳��˳����� ====
                pThreadRun->Terminate();

                //===== 3 �ȴ��߳��˳� =========
                pThreadRun->WaitFor();

                m_lstThreadList.Remove(pThreadRun);

                //===== 4 �ͷ��߳�ָ�� ======
                DestroyAThread(pThreadRun);
            }
        }

    }
    catch(TCException &e)
    {
        m_csListLock.Leave();
        throw e;
    }
    catch(...)
    {
        m_csListLock.Leave();
        throw TCException("TCThreadPool::ClearFinishedThread Error:"
                        "Unknown Exception.");
    }
    m_csListLock.Leave();
}


//==========================================================================
// ���� : TCThreadPool::Clear
// ��; : ����̳߳ز��ͷ���Դ
// ԭ�� : void Clear()
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCThreadPool::Clear()
{
    m_csListLock.Enter();
    try
    {
        TCThreadRun * pThreadRun;
        long i, nCount;
        nCount = m_lstThreadList.GetCount();
        for (i=0; i<nCount;i++)
        {
            pThreadRun = (TCThreadRun *)m_lstThreadList[i];

            //==== 1 �����߳�FreeOnTerminateΪfalse =======
            pThreadRun->SetFreeOnTerminate(false);

            //==== 2 ʹ�߳��˳����� ====
            pThreadRun->Terminate();
        }

        while (m_lstThreadList.GetCount() > 0)
        {
            pThreadRun = (TCThreadRun *)m_lstThreadList.Last();

            //===== 3 �ȴ��߳��˳� =========
            pThreadRun->WaitFor();

            m_lstThreadList.Remove(pThreadRun);

            //===== 4 �ͷ��߳�ָ�� ======
            DestroyAThread(pThreadRun);
        }
    }
    catch(TCException &e)
    {
        m_csListLock.Leave();
        throw e;
    }
    catch(...)
    {
        m_csListLock.Leave();
        throw TCException("TCThreadPool::Clear Error:"
                        "Unknown Exception.");
    }
    m_csListLock.Leave();
}

//==========================================================================
// ���� : TCThreadPool::Initialize
// ��; : ��Ա������ʼ��
// ԭ�� : Initialize(TCString sHostName, TCString sService,
//                       long nPort, long nQueue,
//                       long nMinThread, long nMaxThread)
// ���� :
// ���� :
// ˵�� :
//==========================================================================
void TCThreadPool::Initialize()
{
//
}

//==========================================================================
// ���� : TCThreadPool::Monitor
// ��; : �̳߳ؼ�أ���ûʵ��
// ԭ�� : void Monitor()
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCThreadPool::Monitor()
{
//   
}








