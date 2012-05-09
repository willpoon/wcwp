//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_thread_pool.h"
//---------------------------------------------------------------------------

#pragma package(smart_init)

//==========================================================================
// 函数 : TCThreadPool::TCThreadPool
// 用途 : 构造函数
// 原型 :
// 参数 :
// 返回 :
// 说明 :
//==========================================================================
TCThreadPool::TCThreadPool()
{
    //设置最大线程数
    mnMaxThread=14;
    mbRecordRunTime=false;
    mbSendErrorMsg=false;
}

//==========================================================================
// 函数 : TCThreadPool::~TCThreadPool
// 用途 : 析构函数，清除线程池
// 原型 :
// 参数 : 无
// 返回 :
// 说明 :
//==========================================================================
TCThreadPool::~TCThreadPool()
{
    Clear();
}

//==========================================================================
// 函数 : TCThreadPool::AddThread
// 用途 : 增加一个线程池
// 原型 : AddThread(TCThreadRun *pThreadRun)
// 参数 : pThreadRun -- 线程类的指针
// 返回 : 无
// 说明 :
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
// 函数 : TCThreadPool::DeleteThread
// 用途 : 删除一个线程
// 原型 : void DeleteThread(long nIdx)
// 参数 : nIdx -- 线程索引
// 返回 : 无
// 说明 :
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
// 函数 :  TCThreadPool::GetThreadServer
// 用途 :  得到一个服务线程的指针
// 原型 :  TCThreadStat * GetThreadServer(long nIdx)
// 参数 :  nIdx -- 服务线程索引
// 返回 :  服务线程的指针
// 说明 :
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
// 函数 : TCThreadPool::ClearFinishedThread
// 用途 : 清除已完成线程并释放资源
// 原型 : void ClearFinishedThread()
// 参数 : 无
// 返回 : 无
// 说明 :
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
                //==== 1 设置线程FreeOnTerminate为false =======
                pThreadRun->SetFreeOnTerminate(false);

                //==== 2 使线程退出处理 ====
                pThreadRun->Terminate();

                //===== 3 等待线程退出 =========
                pThreadRun->WaitFor();

                m_lstThreadList.Remove(pThreadRun);

                //===== 4 释放线程指针 ======
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
// 函数 : TCThreadPool::Clear
// 用途 : 清除线程池并释放资源
// 原型 : void Clear()
// 参数 : 无
// 返回 : 无
// 说明 :
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

            //==== 1 设置线程FreeOnTerminate为false =======
            pThreadRun->SetFreeOnTerminate(false);

            //==== 2 使线程退出处理 ====
            pThreadRun->Terminate();
        }

        while (m_lstThreadList.GetCount() > 0)
        {
            pThreadRun = (TCThreadRun *)m_lstThreadList.Last();

            //===== 3 等待线程退出 =========
            pThreadRun->WaitFor();

            m_lstThreadList.Remove(pThreadRun);

            //===== 4 释放线程指针 ======
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
// 函数 : TCThreadPool::Initialize
// 用途 : 成员变量初始化
// 原型 : Initialize(TCString sHostName, TCString sService,
//                       long nPort, long nQueue,
//                       long nMinThread, long nMaxThread)
// 参数 :
// 返回 :
// 说明 :
//==========================================================================
void TCThreadPool::Initialize()
{
//
}

//==========================================================================
// 函数 : TCThreadPool::Monitor
// 用途 : 线程池监控，暂没实现
// 原型 : void Monitor()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCThreadPool::Monitor()
{
//   
}








