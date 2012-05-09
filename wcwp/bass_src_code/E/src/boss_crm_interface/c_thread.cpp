//---------------------------------------------------------------------------
#include "cmpublic.h"
#include "c_critical_section.h"

#pragma hdrstop

#include "c_thread.h"

#ifdef __WIN32__
#include <windows.h>
#else
#include <pthread.h>
#endif

//------------------------------------------------------------------------
// 结构 : TSSyncProc
// 用途 : 同步处理结构，用于同步函数调用
//------------------------------------------------------------------------
struct TSSyncProc
{
    TCThread * m_pThread;           // 线程类指针
#ifdef __TH_WINDOWS__
    DWORD m_nSignal;                // Windows信号
#endif

#ifdef __TH_UNIX__
    pthread_cond_t m_tcSignal;      // UNIX信号
#endif
};

//---------------------------------------------------------------------------
// 函数声明
//---------------------------------------------------------------------------
#ifdef __TH_UNIX__
DWORD BeginThread(pthread_attr_t * pThreadAttr,
        void * (*pfuncThreadFunc)(void * pParameter), void * pParameter,
        DWORD * pThreadID);
#endif
#ifdef __TH_WINDOWS__
void EndThread(long nExitCode);
#endif
#ifdef __TH_UNIX__
DWORD GetCurrentThreadID();
void EndThread(long nExitCode);
#endif

void AddThread();
void RemoveThread();
DWORD ThreadProc(TCThread * pThread);
#ifdef __TH_WINDOWS__
DWORD WINAPI ThreadWinAPIProc(void * pThread);
#endif
#ifdef __TH_UNIX__
void * ThreadUnixProc(void * pThread);
#endif

//---------------------------------------------------------------------------

#ifdef __TH_UNIX__
//--------------------------------------------------------------------------
// 函数指针 : STA__funcEndThreadProc
// 用途     : 可安装的结束线程函数句柄
//--------------------------------------------------------------------------
void (*STA__funcEndThreadProc)(long nExitCode) = NULL;
#endif

//==========================================================================
// 函数 : BeginThread
// 用途 : 封装UNIX中的线程创建
// 原型 : DWORD BeginThread(pthread_attr_t * pThreadAttr,
//              void * (*pfuncThreadFunc)(void * pParameter),
//              void * pParameter, DWORD * pThreadID);
// 参数 : pThreadAttr     - 线程属性
//        pfuncThreadFunc - 线程开始执行时调用的函数
//        pParameter      - start_routine占用一个参数，它是一个指向void的指针
//        pThreadID       - 创建的线程的ID
// 返回 : 成功时返回0，不成功时为一个非零的错误代码
// 说明 : 该函数封装了pthread_create的实现
//==========================================================================
#ifdef __TH_UNIX__
DWORD BeginThread(pthread_attr_t * pThreadAttr,
        void * (*pfuncThreadFunc)(void * pParameter), void * pParameter,
        pthread_t * pThreadID)
{
    return pthread_create(pThreadID, pThreadAttr, pfuncThreadFunc, pParameter);
}
#endif
/*
In function `DWORD BeginThread(pthread_attr_t *, void * (*)(void *),
        void *, DWORD *)'
    passing `DWORD *' as argument 1 of `pthread_create(pthread_t *,
    const pthread_attr_t *, void * (*)(void *), void *)'
    */
#ifdef __TH_WINDOWS__
//==========================================================================
// 函数 : EndThread
// 用途 : Windows环境下结束一个线程的实现
// 原型 : void EndThread(long nExitCode);
// 参数 : 退出码
// 返回 : 无
// 说明 :
//==========================================================================
void EndThread(long nExitCode)
{
    ExitThread(nExitCode);
}
#endif

#ifdef __TH_UNIX__
//==========================================================================
// 函数 : GetCurrentThreadID
// 用途 : 封装UNIX得到自身线程ID
// 原型 : DWORD GetCurrentThreadID();
// 参数 : 无
// 返回 : 线程ID
// 说明 :
//==========================================================================
DWORD GetCurrentThreadID()
{
    return (DWORD)pthread_self();
}

//==========================================================================
// 函数 : EndThread
// 用途 : UNIX环境下结束一个线程的实现
// 原型 : void EndThread(long nExitCode);
// 参数 : 退出码
// 返回 : 无
// 说明 :
//==========================================================================
void EndThread(long nExitCode)
{
    if (STA__funcEndThreadProc != NULL)
        STA__funcEndThreadProc(nExitCode);

    pthread_detach((pthread_t)GetCurrentThreadID());
    pthread_exit((void *)nExitCode);
}
#endif

//---------------------------------------------------------------------------

bool                STA__bProcPosted;           // 已安装同步过程
TCList *            STA__pSyncList = NULL;      // 同步过程列表
TCCriticalSection   STA__csThreadLock;          // 互斥锁
long                STA__nThreadCount;          // 线程数量

void DeleteSyncList()
{
    if (STA__pSyncList != NULL)
    {
        delete STA__pSyncList;
        STA__pSyncList = NULL;
    }
}
//==========================================================================
// 函数 : AddThread
// 用途 : 增加一个线程，线程计数加1
// 原型 : void AddThread()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void AddThread()
{
    STA__csThreadLock.Enter();
    try
    {
        if (STA__nThreadCount == 0 && STA__pSyncList == NULL)
        {
            STA__pSyncList = new TCList;
            Application.InstallExitHandle(DeleteSyncList);
        }

        STA__nThreadCount ++;
    }
    catch(...)
    {
        STA__csThreadLock.Leave();
        throw;
    }

    STA__csThreadLock.Leave();
}

//==========================================================================
// 函数 : RemoveThread
// 用途 : 删除一个线程，线程计数减1
// 原型 : void RemoveThread()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void RemoveThread()
{
    STA__csThreadLock.Enter();
    try
    {
        STA__nThreadCount --;
    }
    catch(...)
    {
        STA__csThreadLock.Leave();
        throw;
    }

    STA__csThreadLock.Leave();
}

//==========================================================================
// 函数 : ThreadProc
// 用途 : 线程的执行函数
// 原型 : DWORD ThreadProc(TCThread * pThread);
// 参数 : 线程类的指针
// 返回 : 取pThread的m_nReturnValue做为返回值
// 说明 : 由于SUN的Solaris中，当线程全速运行时，要等到一个线程退出时，另一个
//        线程才能启动。这样，在一个线程启动前延时一秒以尽量避免此类问题的产
//        生。另外，在线程的实际处理过程中，应尽量多地运用SleepMilliSeconds
//        方法。
//==========================================================================
DWORD ThreadProc(TCThread * pThread)
{
    bool bFreeThread;
    long nRet;

#ifdef __TH_UNIX__
    //========= 1. (UNIX)如果挂起，则等待 =======
    if (pThread->m_bSuspended)
        sem_wait(&pThread->m_semCreateSuspendedSem);
#endif

    try
    {
        //======= 2. 运行线程 ========
        if (!pThread->IsTerminated())
        {
            try
            {
                //--------- 增加下面一条语句，以避免Solaris中的线程问题 ----
                pThread->SleepMilliSeconds(1);
                //----------- 增加结束 ---------
                
                pThread->Execute();
            }
            catch (TCException & e)
            {
                pThread->m_pFatalException = new TCException(e);
            }
        }
    }

    catch (...)
    {
        //========= 3. 做一些释放任务 ========
        bFreeThread = pThread->m_bFreeOnTerminate;
        nRet = pThread->m_nReturnValue;
        pThread->m_bFinished = true;
        pThread->DoTerminate();
        if (bFreeThread)
        {
            delete pThread;
            pThread = NULL;
        }

#ifdef __TH_WINDOWS__
        EndThread(nRet);
#endif

#ifdef __TH_UNIX__
        // 直接通过EndThread调用pthread_exit会detach该线程，引起
        // TCThread::WaitFor中的pthread_join调用出错
        if (STA__funcEndThreadProc != NULL)
            (*STA__funcEndThreadProc)(nRet);
        pthread_exit(&nRet);
#endif
        throw;
    }

    //========= 4. 与上一个catch(...)完全相同，达到finally的效果 ======
    bFreeThread = pThread->m_bFreeOnTerminate;
    nRet = pThread->m_nReturnValue;
    pThread->m_bFinished = true;
    pThread->DoTerminate();
    if (bFreeThread)
    {
        delete pThread;
        pThread = NULL;
    }

#ifdef __TH_WINDOWS__
    EndThread(nRet);
#endif

#ifdef __TH_UNIX__
    if (STA__funcEndThreadProc != NULL)
        (*STA__funcEndThreadProc)(nRet);
    pthread_exit(&nRet);
#endif

    return nRet;
}

#ifdef __TH_WINDOWS__
//==========================================================================
// 函数 : ThreadWinAPIProc
// 用途 : 以WINAPI的形式封装，以利于CreateThread的调用
// 原型 : DWORD WINAPI ThreadWinAPIProc(void * pThread);
// 参数 : 线程类的指针
// 返回 : 取pThread的m_nReturnValue做为返回值
// 说明 : 该函数直接调用ThreadProc
//==========================================================================
DWORD WINAPI ThreadWinAPIProc(void * pThread)
{
    return ThreadProc((TCThread *)pThread);
}
#endif

#ifdef __TH_UNIX__
//==========================================================================
// 函数 : ThreadUnixProc
// 用途 : 以UNIX的形式封装，以利于BeginThread的调用
// 原型 : void * ThreadUnixProc(void * pThread);
// 参数 : 线程类的指针
// 返回 : 取pThread的m_nReturnValue做为返回值
// 说明 : 该函数直接调用ThreadProc
//==========================================================================
void * ThreadUnixProc(void * pThread)
{
    return (void *)ThreadProc((TCThread *)pThread);
}
#endif

//==========================================================================
// 函数 : GetTotalThreadCount
// 用途 : 得到总线程数（不包括主线程）
// 原型 : long GetTotalThreadCount();
// 参数 : 无
// 返回 : 总线程数（不包括主线程）
// 说明 : 
//==========================================================================
long GetTotalThreadCount()
{
    return STA__nThreadCount;
}

//==========================================================================
// 函数 : CheckSynchronize
// 用途 : 检查并运行同步事件
// 原型 : bool CheckSynchronize();
// 参数 : 无
// 返回 : 如果有方法被同步运行，则返回真；如果什么都没做，则返回假。
// 说明 : 在主线程中周期性的执行CheckSynchronize，可以在主线程中对后台线程
//        执行同步任务。
//==========================================================================
bool CheckSynchronize()
{
    TSSyncProc * pSyncProc;
    bool bResult;

    if (STA__bProcPosted)
    {
        STA__csThreadLock.Enter();
        try
        {
            bResult = (STA__pSyncList != NULL
                    && STA__pSyncList->GetCount() > 0);
            if (bResult)
            {
                while (STA__pSyncList->GetCount() > 0)
                {
                    pSyncProc = (TSSyncProc *)(*STA__pSyncList)[0];
                    STA__pSyncList->Delete(0);
                    try
                    {
                        pSyncProc->m_pThread->m_funcSyncMethod
                                (pSyncProc->m_pThread->m_pSyncMethodParam);
                    }
                    catch (TCException & e)
                    {
                        pSyncProc->m_pThread->m_pSynchronizeException
                                = new TCException(e);
                    }
#ifdef __TH_WINDOWS__
                    SetEvent((void *)pSyncProc->m_nSignal);
#endif
#ifdef __TH_UNIX__
                    pthread_cond_signal(&pSyncProc->m_tcSignal);
#endif
                }
                STA__bProcPosted = false;
            }
        }
        catch (...)
        {
            STA__csThreadLock.Leave();
            throw;
        }
        STA__csThreadLock.Leave();
    }
    else
        bResult = false;

    return bResult;
}

//==========================================================================
// 函数 : TCThread::TCThread
// 用途 : 构造函数
// 原型 : TCThread();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
TCThread::TCThread()
{
    m_nHandle = 0;
    m_nThreadID = 0;
#ifdef __TH_UNIX__
    m_bInitialSuspendDone = false;
#endif

    m_bCreateSuspended = false;
    m_bSuspended = false;

    m_bTerminated = false;
    m_bFreeOnTerminate = false;

    m_nReturnValue = 0;
    m_bFinished = false;

    m_funcSyncMethod = NULL;
    m_pSyncMethodParam = NULL;

    m_pSynchronizeException = NULL;
    m_pFatalException = NULL;
}

//==========================================================================
// 函数 : TCThread::~TCThread
// 用途 : 析构函数
// 原型 : ~TCThread();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
TCThread::~TCThread()
{
    //======== 1. 如果线程未结束，则发结束请求，并等待线程结束 ========
    if (m_nThreadID != 0 && !m_bFinished)
    {
        Terminate();

        if (m_bCreateSuspended)
            Resume();

        WaitFor();
    }

    //======== 2. 释放句柄、信号量 ===========
#ifdef __TH_WINDOWS__
    if (m_nHandle != 0)
        CloseHandle((void *)m_nHandle);
#endif

#ifdef __TH_UNIX__
    if (m_nThreadID != 0)
        pthread_detach(m_nThreadID);
    sem_destroy(&m_semCreateSuspendedSem);
#endif

    //======= 3. 释放FatalException指针 ========
    delete m_pFatalException;
    m_pFatalException = NULL;

    //======= 4. 删除线程 =======
    RemoveThread();
}

//==========================================================================
// 函数 : TCThread::AfterConstruction
// 用途 : 在建立线程后，根据条件唤醒线程
// 原型 : void TCThread::AfterConstruction();
// 参数 : 无
// 返回 : 无
// 说明 : 通过Start()函数调用
//==========================================================================
void TCThread::AfterConstruction()
{
    if (!m_bCreateSuspended)
        Resume();
}

//==========================================================================
// 函数 : TCThread::CheckThreadError
// 用途 : 检查线程调用返回值，如果出错则抛出例外
// 原型 : private void CheckThreadError(long nErrCode);
// 参数 : 线程调用返回的错误码
// 返回 : 无
// 说明 : 如果返回的错误码非零，则抛出例外
//==========================================================================
void TCThread::CheckThreadError(long nErrCode)
{
    if (nErrCode != 0)
        throw TCThreadException("Thread Error - "
                + TCSystem::SysErrorMessage(nErrCode));
}

//==========================================================================
// 函数 : TCThread::CheckThreadErrorSuccess
// 用途 : 检查线程调用返回值，如果为假则抛出例外
// 原型 : private void CheckThreadErrorSuccess(long nErrCode);
// 参数 : 线程调用返回的是否成功信息
// 返回 : 无
// 说明 : 如果返回的错误码为假（零、不成功），则抛出例外
//==========================================================================
void TCThread::CheckThreadErrorSuccess(bool bSuccess)
{
    if (!bSuccess)
        CheckThreadError(TCSystem::GetLastError());
}

//==========================================================================
// 函数 : TCThread::DoTerminate
// 用途 : 线程退出时调用该函数，子类可重载该函数以完成具体实现
// 原型 : virtual void DoTerminate();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCThread::DoTerminate()
{
}

//==========================================================================
// 函数 : TCThread::GetFatalException
// 用途 : 得到致使错误引起的例外类指针
// 原型 : TCException * TCThread::GetFatalException();
// 参数 : 无
// 返回 : 例外类的指针
// 说明 :
//==========================================================================
TCException * TCThread::GetFatalException()
{
    return m_pFatalException;
}

//==========================================================================
// 函数 : TCThread::GetHandle
// 用途 : 得到线程句柄
// 原型 : DWORD GetHandle();
// 参数 : 无
// 返回 : 线程句柄
// 说明 :
//==========================================================================
DWORD TCThread::GetHandle()
{
    return m_nHandle;
}

#ifdef __TH_WINDOWS__
//--------------------------------------------------------------------------
// 数组 : STA__Priorities (Windows Implemetation)
// 用途 : 线程优先级的常量定义。以完成枚举型与常量之间的互相转换
//--------------------------------------------------------------------------
int STA__Priorities[tpTimeCritical + 1]
        = { THREAD_PRIORITY_IDLE,
            THREAD_PRIORITY_LOWEST,
            THREAD_PRIORITY_BELOW_NORMAL,
            THREAD_PRIORITY_NORMAL,
            THREAD_PRIORITY_ABOVE_NORMAL,
            THREAD_PRIORITY_HIGHEST,
            THREAD_PRIORITY_TIME_CRITICAL
          };

//==========================================================================
// 函数 : TCThread::GetPriority (Windows Implemetation)
// 用途 : 得到线程的优先级
// 原型 : TEThreadPriority GetPriority();
// 参数 : 无
// 返回 : 线程的优先级枚举
// 说明 :
//==========================================================================
TEThreadPriority TCThread::GetPriority()
{
    //====== 1. 得到线程的优先级 =======
    int nPriority;
    nPriority = GetThreadPriority((void *)m_nHandle);
    CheckThreadErrorSuccess(nPriority != THREAD_PRIORITY_ERROR_RETURN);

    //======= 2. 得到定义的枚举类型 =======
    TEThreadPriority tpRet;
    long i;
    tpRet = tpNormal;

    for (i = 0; i <= tpTimeCritical; i++)
    {
        if (STA__Priorities[i] == nPriority)
        {
            tpRet = (TEThreadPriority)i;
            break;
        }
    }

    return tpRet;
}
#endif

#ifdef __TH_UNIX__
//==========================================================================
// 函数 : TCThread::GetPolicy (UNIX Implemetation)
// 用途 : 得到线程的策略
// 原型 : int GetPolicy();
// 参数 : 无
// 返回 : 线程的策略
// 说明 :
//==========================================================================
int TCThread::GetPolicy()
{
    sched_param J;
    int nRet;
    CheckThreadError(pthread_getschedparam(m_nThreadID, &nRet, &J));
    return nRet;
}

//==========================================================================
// 函数 : TCThread::GetPriority (UNIX Implemetation)
// 用途 : 得到线程的优先级
// 原型 : TEThreadPriority GetPriority();
// 参数 : 无
// 返回 : 线程的优先级整型值
// 说明 :
//    Unix的优先级是以计划策略为基础实现的。共有三种策略。
//
//          策略          类型           优先级
//      ----------      --------       --------
//      SCHED_RR        RealTime         0-99
//      SCHED_FIFO      RealTime         0-99
//      SCHED_OTHER     Regular           0
//
//    SCHED_RR与SCHED_FIFO只能被root用户设置。
//==========================================================================
int TCThread::GetPriority()
{
    int nP;
    sched_param spJ;
    pthread_getschedparam(m_nThreadID, &nP, &spJ);
    return spJ.sched_priority;
}
#endif

//==========================================================================
// 函数 : TCThread::GetReturnValue
// 用途 : 得到线程的返回值
// 原型 : long GetReturnValue();
// 参数 : 无
// 返回 : 线程的返回值
// 说明 :
//==========================================================================
long TCThread::GetReturnValue()
{
    return m_nReturnValue;
}

//==========================================================================
// 函数 : TCThread::GetThreadID
// 用途 : 得到线程ID
// 原型 : DWORD GetThreadID();
// 参数 : 无
// 返回 : 线程ID
// 说明 :
//==========================================================================
DWORD TCThread::GetThreadID()
{
    return (DWORD)m_nThreadID;
}

//==========================================================================
// 函数 : TCThread::IsFreeOnTerminate
// 用途 : 得到是否线程退出时自动释放线程类指针
// 原型 : bool IsFreeOnTerminate();
// 参数 : 无
// 返回 : 是否线程退出时自动释放线程类指针
// 说明 :
//==========================================================================
bool TCThread::IsFreeOnTerminate()
{
    return m_bFreeOnTerminate;
}

//==========================================================================
// 函数 : TCThread::IsSuspended
// 用途 : 得到当前线程是否挂起
// 原型 : bool IsSuspended();
// 参数 : 无
// 返回 : 当前线程是否挂起
// 说明 :
//==========================================================================
bool TCThread::IsSuspended()
{
    return m_bSuspended;
}

//==========================================================================
// 函数 : TCThread::IsTerminated
// 用途 : 得到当前线程是否中止
// 原型 : bool IsTerminated();
// 参数 : 无
// 返回 : 当前线程是否中止
// 说明 :
//==========================================================================
bool TCThread::IsTerminated()
{
    return m_bTerminated;
}

//==========================================================================
// 函数 : TCThread::Resume
// 用途 : 唤醒线程
// 原型 : void Resume();
// 参数 : 无
// 返回 : 无
// 说明 :
//      关于Suspend与Resume。POSIX不支持suspending/resuming一个线程。由于不能
//      保证在何处线程被挂起，Suspend一个线程被认为是一个危险的动作。挂起的线
//      程可能也获得一个锁，互斥量，或它可能正在一个互斥段中。为了模仿挂起/唤
//      醒操作，我们用到了信号。用SIGSTOP去唤醒挂起一个线程，用SIGCONT去挂起
//      一个线程。这种方法只适用于Linux，因为根据POSIX标准，如果一个线程接收
//      到SIGSTOP，则整个进程都将停止运行。但Linux不完全采用POSIX标准。如果它
//      完全遵从POSIX标准，那么suspend与resume将无法工作。
//==========================================================================
void TCThread::Resume()
{
#ifdef __TH_WINDOWS__
    long nSuspendCount;

    nSuspendCount = ResumeThread((void *)m_nHandle);
    CheckThreadErrorSuccess(nSuspendCount >= 0);
    if (nSuspendCount == 1)
        m_bSuspended = false;
#endif

#ifdef __TH_UNIX__
    if (!m_bInitialSuspendDone)
    {
        m_bInitialSuspendDone = true;
        sem_post(& m_semCreateSuspendedSem);
    }
    else
        CheckThreadError(pthread_kill(m_nThreadID, SIGCONT));

    m_bSuspended = false;
#endif
}

//==========================================================================
// 函数 : TCThread::SetFreeOnTerminate
// 用途 : 设置是否线程退出时自动释放线程类指针
// 原型 : void SetFreeOnTerminate(bool bFreeOnTerminate);
// 参数 : 是否线程退出时自动释放线程类指针
// 返回 : 无
// 说明 :
//==========================================================================
void TCThread::SetFreeOnTerminate(bool bFreeOnTerminate)
{
    m_bFreeOnTerminate = bFreeOnTerminate;
}

#ifdef __TH_WINDOWS__
//==========================================================================
// 函数 : TCThread::SetPriority (Windows Implemetation)
// 用途 : 设置线程的优先级
// 原型 : void SetPriority(TEThreadPriority tpValue);
// 参数 : 线程的优先级枚举
// 返回 : 无
// 说明 :
//==========================================================================
void TCThread::SetPriority(TEThreadPriority tpValue)
{
    CheckThreadErrorSuccess(SetThreadPriority((void *)m_nHandle,
            STA__Priorities[tpValue]));
}
#endif

#ifdef __TH_UNIX__
//==========================================================================
// 函数 : TCThread::SetPolicy (UNIX Implemetation)
// 用途 : 设置线程的策略
// 原型 : void SetPolicy(int nValue);
// 参数 : 线程的策略
// 返回 : 无
// 说明 :
//==========================================================================
void TCThread::SetPolicy(int nValue)
{
    sched_param P;

    if (nValue != GetPolicy())
    {
        P.sched_priority = GetPriority();
        CheckThreadError(pthread_setschedparam(m_nThreadID, nValue, &P));
    }
}

//==========================================================================
// 函数 : TCThread::SetPriority (UNIX Implemetation)
// 用途 : 设置线程的优先级
// 原型 : void TCThread::SetPriority(int nValue);
// 参数 : 线程的优先级整型值
// 返回 : 无
// 说明 :
//==========================================================================
void TCThread::SetPriority(int nValue)
{
    sched_param spP;

    if (nValue != GetPriority())
    {
        spP.sched_priority = nValue;
        CheckThreadError(pthread_setschedparam(m_nThreadID, GetPolicy(), &spP));
    }
}
#endif

//==========================================================================
// 函数 : TCThread::SetReturnValue
// 用途 : 设置线程的返回值
// 原型 : void SetReturnValue(long nValue);
// 参数 : 线程的返回值
// 返回 : 无
// 说明 :
//==========================================================================
void TCThread::SetReturnValue(long nValue)
{
    m_nReturnValue = nValue;
}

//==========================================================================
// 函数 : TCThread::SetSuspended
// 用途 : 设置当前线程是否挂起
// 原型 : void SetSuspended(bool bValue);
// 参数 : 当前线程是否挂起
// 返回 : 无
// 说明 :
//==========================================================================
void TCThread::SetSuspended(bool bValue)
{
    if (bValue != m_bSuspended)
    {
        if (bValue)
            Suspend();
        else
            Resume();
    }
}

void TCThread::SleepMilliSeconds(long nMilliSeconds)
{
#ifdef __TH_WINDOWS__
    ::Sleep(nMilliSeconds);
#endif
#ifdef __TH_UNIX__
	struct timeval timeout;
	timeout.tv_sec=nMilliSeconds / 1000;
	timeout.tv_usec=(nMilliSeconds % 1000) * 1000;
	select(0,(fd_set*)0,(fd_set*)0,(fd_set*)0,&timeout);
	return;
#endif
}

//==========================================================================
// 函数 : TCThread::Start
// 用途 : 创建一个线程，并运行
// 原型 : void TCThread::Start(bool bCreateSuspended);
// 参数 : 创建线程时是否初始于挂起状态
// 返回 : 无
// 说明 :
//==========================================================================
void TCThread::Start(bool bCreateSuspended)
{
    //======= 1. 调用AddThread使线程计数增1 =======
    AddThread();

    //====== 2. 赋值标识是否挂起的成员变量 ==========
    m_bSuspended = bCreateSuspended;
    m_bCreateSuspended = bCreateSuspended;

#ifdef __TH_WINDOWS__
    //======= 3. (Windows)创建线程，并处于挂起状态 =======
    m_nHandle = (DWORD)CreateThread(NULL, 0, ThreadWinAPIProc, this,
            CREATE_SUSPENDED, &m_nThreadID);
    if (m_nHandle == 0)
        throw TCThreadException("Thread Create Error - "
                + TCSystem::SysErrorMessage(TCSystem::GetLastError()));
#endif

#ifdef __TH_UNIX__
    //======= 4. (UNIX)创建线程，并处于挂起状态 =======
    long nErrCode;
    sem_init(& m_semCreateSuspendedSem, false, 0);
    nErrCode = BeginThread(NULL, ThreadUnixProc, this, &m_nThreadID);
    if (nErrCode != 0)
        throw TCThreadException(TCString("Thread Create Error - ")
                + TCSystem::SysErrorMessage(TCSystem::GetLastError()));
#endif

    //===== 5. 调用线程创建后处理，可能做线程唤醒任务 ======
    AfterConstruction();
}

//==========================================================================
// 函数 : TCThread::Suspend
// 用途 : 挂起线程
// 原型 : void Suspend();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCThread::Suspend()
{
#ifdef __TH_WINDOWS__
    m_bSuspended = true;
    CheckThreadErrorSuccess(SuspendThread((void *)m_nHandle) >= 0);
#endif

#ifdef __TH_UNIX__
    m_bSuspended = true;
    CheckThreadError(pthread_kill(m_nThreadID, SIGCONT));
#endif
}

//==========================================================================
// 函数 : TCThread::Synchronize
// 用途 : 同步方法调用
// 原型 : void Synchronize(void (*funcNeedSync)());
// 参数 : 同步调用方法指针
// 返回 : 无
// 说明 :
//==========================================================================
void TCThread::Synchronize(void (*funcNeedSync)(void *), void * pSyncParam)
{
    TSSyncProc spSyncProc;

#ifdef __TH_WINDOWS__
    //======= 1. (Windows) 创建事件 =========
    spSyncProc.m_nSignal = (DWORD)CreateEvent(NULL, true, false, NULL);
    try
    {
#endif

#ifdef __TH_UNIX__
        //======== 2. (UNIX) 这一步同时也初始化了条件变量 =======
        memset(&spSyncProc, '\0', sizeof(spSyncProc));
#endif

        STA__csThreadLock.Enter();
        try
        {
            //======= 3. 加入同步过程列表 =======
            m_pSynchronizeException = NULL;
            m_funcSyncMethod = funcNeedSync;
            m_pSyncMethodParam = pSyncParam;
            spSyncProc.m_pThread = this;

            STA__pSyncList->Add(&spSyncProc);
            STA__bProcPosted = true;

#ifdef __TH_WINDOWS__
            //======= 4. (Windows)等待同步事件完成通知 ======
            STA__csThreadLock.Leave();
            try
            {
                WaitForSingleObject((void *)spSyncProc.m_nSignal, INFINITE);
            }
            catch (...)
            {
                STA__csThreadLock.Enter();
                throw;
            }
            STA__csThreadLock.Enter();
#endif

#ifdef __TH_UNIX__
            //======= 5. (UNIX)等待同步条件变量 =====
            pthread_cond_wait(&spSyncProc.m_tcSignal,
                    (pthread_mutex_t *)&STA__csThreadLock);
#endif
        }
        catch (...)
        {
            STA__csThreadLock.Leave();
            throw;
        }
        STA__csThreadLock.Leave();

#ifdef __TH_WINDOWS__
    }
    catch (...)
    {
        CloseHandle((void *)spSyncProc.m_nSignal);
        throw;
    }
    CloseHandle((void *)spSyncProc.m_nSignal);
#endif

    //====== 6. 如果同步事件有例外赋值，则在线程内抛出该例外 ======
    if (m_pSynchronizeException != NULL)
        throw *m_pSynchronizeException;
}

//==========================================================================
// 函数 : TCThread::Terminate
// 用途 : 通知线程退出
// 原型 : void Terminate()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCThread::Terminate()
{
    m_bTerminated = true;
}

//==========================================================================
// 函数 : TCThread::WaitFor
// 用途 : 等待线程退出
// 原型 : DWORD TCThread::WaitFor();
// 参数 : 无
// 返回 : 线程的退出码
// 说明 :
//==========================================================================
DWORD TCThread::WaitFor()
{
    DWORD nRet;

#ifdef __TH_WINDOWS__
    DWORD nH;
    nH = m_nHandle;
    WaitForSingleObject((void *)nH, INFINITE);
    CheckThreadErrorSuccess(GetExitCodeThread((void *)nH, &nRet));
#endif

#ifdef __TH_UNIX__
    DWORD nID;
    nID = (DWORD)m_nThreadID;

    void *pX;
    pX = &nRet;
    CheckThreadError(pthread_join((pthread_t)nID, &pX));
#endif

    m_nThreadID = 0;

    return nRet;
}

//==========================================================================
// 函数 : TCThreadException::TCThreadException
// 用途 : 构造函数
// 原型 : TCThreadException(TCString sMsg);
// 参数 : 例外情况描述串
// 返回 : 无
// 说明 :
//==========================================================================
TCThreadException::TCThreadException(TCString sMsg) : TCException(sMsg)
{
}

#ifdef __TEST__

//==========================================================================
// 函数 : DisplayTestThreadPrompt
// 用途 : 显示提示信息
//==========================================================================
void DisplayTestThreadPrompt()
{
    printf("\n\n==== Test Thread ====\n\n");
#ifdef __TH_UNIX__
    printf("U. Unix Simple Thread Test (No Delay)\n");
    printf("D. Unix Simple Thread Test (Delay Before Execute)\n");
    printf("I. Unix Simple Thread Test (Delay Inter Execute)\n");
#endif
    printf("0. Simple Print Thread\n");
    printf("1. Thread Pointer Access\n");
    printf("2. Terminate Thread Sample\n");
    printf("3. Thread Priority Sample\n");
    printf("4. Thread Exception Sample\n");
    printf("5. Thread Suspend & Resume Sample\n");
    printf("6. Thread CriticalSection Sample\n");
    printf("7. Thread Synchronize Sample\n");

    printf("\nQ. Quit\n\n");
}

#ifdef __TH_UNIX__
const long TEST_NUM_THREADS = 5;            // 生成的测试线程数
pthread_t STA__tid[TEST_NUM_THREADS];       // 线程ID数组

//==========================================================================
// 函数 : TestThreadUUnixSimpleThread__ThreadExec
// 用途 : 线程运行函数
//==========================================================================
void * TestThreadUUnixSimpleThread__ThreadExec(void *arg)
{
    long sleep_time;
    long nThreadDelayMode;

    nThreadDelayMode = (long)arg;

    if (nThreadDelayMode == 1)
    {   printf("*");
        fflush(stdout);
        TCSystem::DelayMicroSeconds(1);
    }

    sleep_time = 10;

    for (long i = 0; i < 30; i++)
	{
	    for (long j = 0; j < 10000000; j++);
		printf("Thread: %d    Progress: %d\n", pthread_self(), i);
        if (nThreadDelayMode == 2)
        {   printf("*");
            fflush(stdout);
            TCSystem::DelayMicroSeconds(1);            
        }
    }

    printf("thread %d sleeping %d seconds ...\n", pthread_self(), sleep_time);
#pragma warn -8053
    sleep(sleep_time);
#pragma warn +8053
    printf("\nthread %d awakening\n", pthread_self());
    return (NULL);
}

//==========================================================================
// 函数 : TestThreadUUnixSimpleThread
// 用途 : 测试简单的Unix线程（非类的方式）
//==========================================================================
void TestThreadUUnixSimpleThread(long nThreadDelayMode)
{
    long i;

    for (i = 0; i < TEST_NUM_THREADS; i++)
        pthread_create(&STA__tid[i], NULL,
                TestThreadUUnixSimpleThread__ThreadExec,
                (void *)nThreadDelayMode);

    printf("TestThreadUUnixSimpleThread() reporting that all %d "
            "threads have started\n", TEST_NUM_THREADS);

    for ( i = 0; i < TEST_NUM_THREADS; i++)
        pthread_join(STA__tid[i], NULL);

    printf("TestThreadUUnixSimpleThread() reporting that all %d "
            "threads have terminated\n", TEST_NUM_THREADS);
}

#endif



//--------------------------------------------------------------------------
// 类   : TCPrintThread
// 用途 : 继承自TCThread，用以演示简单的线程实现
//--------------------------------------------------------------------------
class TCPrintThread : public TCThread
{
public:
    void Execute();
};

void TCPrintThread::Execute()
{
    long i;

    for (i = 0; i < 1024; i++)
    {
#ifdef __TH_UNIX__
        for (long j = 0; j < 400000; j++);
#endif
        printf("Total: %d - ThreadID: %d - Progress: %d\n",
                GetTotalThreadCount(), GetThreadID(), i);
        fflush(stdout);
    }
    printf("*** Thread \"%d\" Execute finished ***\n", GetThreadID());
}

//==========================================================================
// 函数 : TestThread0PrintThread
// 用途 : 测试简单的线程
// 说明 : 由于线程的释放析构要等待线程的退出，所以要等到所有线程退出后，函数
//        才返回
//==========================================================================
void TestThread0PrintThread()
{
    TCPrintThread thPrint1, thPrint2, thPrint3;
    thPrint1.Start(false);
    thPrint2.Start(false);
    thPrint3.Start(false);
    TCSystem::ReadStringFromConsole();
}

//==========================================================================
// 函数 : TestThread0PrintThread
// 用途 : 通过申请指针的方式使用线程，线程结束后自动释放指针
// 说明 : 可以通过连续的命令，起动多个线程。通过该方式，起动120个线程，证明
//        是成功的。
//==========================================================================
void TestThread1PointerAccess()
{
    TCPrintThread * thPrint1, * thPrint2, * thPrint3;
    thPrint1 = new TCPrintThread;
    thPrint2 = new TCPrintThread;
    thPrint3 = new TCPrintThread;

    thPrint1->SetFreeOnTerminate(true);
    thPrint2->SetFreeOnTerminate(true);
    thPrint3->SetFreeOnTerminate(true);

    thPrint1->Start(false);
    thPrint2->Start(false);
    thPrint3->Start(false);
    TCSystem::ReadStringFromConsole();
}

//--------------------------------------------------------------------------
// 类   : TCTerminateThread
// 用途 : 继承自TCThread，用以测试Terminate
//--------------------------------------------------------------------------
class TCTerminateThread : public TCThread
{
public:
    void Execute();
};

void TCTerminateThread::Execute()
{
    long i;

    for (i = 0; i < 1024; i++)
    {
#ifdef __TH_UNIX__
        for (long j = 0; j < 200000; j++);
#endif
        if (IsTerminated())
        {
            printf("*** Thread \"%d\" terminated ***\n", GetThreadID());
            return;
        }

        printf("ThreadID: %d - Progress: %d\n", GetThreadID(), i);
        fflush(stdout);
    }
    printf("*** Thread \"%d\" Execute finished ***\n", GetThreadID());
}

//==========================================================================
// 函数 : TestThread2Terminate
// 用途 : 测试Terminate
// 说明 : 在调用Terminate()以后，线程的Execute()判断IsTerminated, 然后进行
//        退出处理
//==========================================================================
void TestThread2Terminate()
{
    TCTerminateThread thPrint1, thPrint2, thPrint3;
    thPrint1.Start(false);
    thPrint2.Start(false);
    thPrint3.Start(false);
    TCSystem::ReadStringFromConsole();
    thPrint1.Terminate();
    thPrint2.Terminate();
    thPrint3.Terminate();
}

//--------------------------------------------------------------------------
// 类   : TCPriorityThread
// 用途 : 继承自TCThread，用以测试线程优先级
//--------------------------------------------------------------------------
class TCPriorityThread : public TCThread
{
private:
    char m_cDisplayChar;
public:
    TCPriorityThread();
    void Execute();
    char GetDisplayChar();
    void SetDisplayChar(char cDisplayChar);
};

TCPriorityThread::TCPriorityThread()
{
    m_cDisplayChar = '*';
}

void TCPriorityThread::Execute()
{
    long i;

    while (true)
    {
        if (IsTerminated())
        {
            printf("\n*** Thread \"%d\" (%c) terminated ***", GetThreadID(),
                    m_cDisplayChar);
            return;
        }

        for (i = 0; i < 1000000; i++);

        printf("%c", m_cDisplayChar);
        fflush(stdout);
    }
}

char TCPriorityThread::GetDisplayChar()
{
    return m_cDisplayChar;
}

void TCPriorityThread::SetDisplayChar(char cDisplayChar)
{
    m_cDisplayChar = cDisplayChar;
}

//==========================================================================
// 函数 : TestThread3Priority__GetDes
// 用途 : 得到优先级的描述说明
//==========================================================================
#ifdef __TH_WINDOWS__
TCString TestThread3Priority__GetDes(TEThreadPriority thPriority)
{
    switch (thPriority)
    {
        case tpIdle:
            return "Idle";
        case tpLowest:
            return "Lowest";
        case tpLower:
            return "Lower";
        case tpNormal:
            return "Normal";
        case tpHigher:
            return "Higher";
        case tpHighest:
            return "Highest";
        case tpTimeCritical:
            return "TimeCritical";
        default:
            return "*Uncertain*";
    }
}
#endif

//==========================================================================
// 函数 : TestThread3Priority__RearrangePriority
// 用途 : 按随机方式设置进程的优先级
//==========================================================================
void TestThread3Priority__Rearrange(TCPriorityThread & thPrint1,
        TCPriorityThread &thPrint2, TCPriorityThread &thPrint3)
{
    long nPriority1, nPriority2, nPriority3;

#ifdef __TH_WINDOWS__
    nPriority1 = RandLong(tpTimeCritical + 1) - 1;
    nPriority2 = RandLong(tpTimeCritical + 1) - 1;
    nPriority3 = RandLong(tpTimeCritical + 1) - 1;

    printf("\nRearrange Priority: \n"
            "        Thread1: %c %d %s \n"
            "        Thread2: %c %d %s \n"
            "        Thread3: %c %d %s \n",
            thPrint1.GetDisplayChar(),
            nPriority1,
            (char *)TestThread3Priority__GetDes((TEThreadPriority)nPriority1),

            thPrint2.GetDisplayChar(),
            nPriority2,
            (char *)TestThread3Priority__GetDes((TEThreadPriority)nPriority2),

            thPrint3.GetDisplayChar(),
            nPriority3,
            (char *)TestThread3Priority__GetDes((TEThreadPriority)nPriority3)
            );
    thPrint1.SetPriority((TEThreadPriority)nPriority1);
    thPrint2.SetPriority((TEThreadPriority)nPriority2);
    thPrint3.SetPriority((TEThreadPriority)nPriority3);
#endif

#ifdef __TH_UNIX__
    nPriority1 = RandLong(100) - 1;
    nPriority2 = RandLong(100) - 1;
    nPriority3 = RandLong(100) - 1;

    printf("\nRearrange Priority: \n"
            "        Thread1: %c %d \n"
            "        Thread2: %c %d \n"
            "        Thread3: %c %d \n",
            thPrint1.GetDisplayChar(), nPriority1,
            thPrint2.GetDisplayChar(), nPriority2,
            thPrint3.GetDisplayChar(), nPriority3);
    thPrint1.SetPriority(nPriority1);
    thPrint2.SetPriority(nPriority2);
    thPrint3.SetPriority(nPriority3);
#endif
}

//==========================================================================
// 函数 : TestThread3Priority
// 用途 : 测试Priority
// 说明 : 几个线程同时运行。如果按下"P"键，则按随机方式设置进程的优先级
//==========================================================================
void TestThread3Priority()
{
    printf("        Press <P> key to rearrange priority, \n");
    printf("        Press other Key to terminate threads. \n\n");
    printf("        Press <Return> Key to Continue...\n");
    TCSystem::ReadStringFromConsole();

    TCPriorityThread thPrint1, thPrint2, thPrint3;
    thPrint1.SetDisplayChar('.');
    thPrint2.SetDisplayChar('-');
    thPrint3.SetDisplayChar('*');

    thPrint1.Start(false);
    thPrint2.Start(false);
    thPrint3.Start(false);

    while (true)
    {
        TCString sInput;
        sInput = Left(TCSystem::ReadStringFromConsole(), 1);
        if (sInput == "P" || sInput == "p")
            TestThread3Priority__Rearrange(thPrint1, thPrint2,
                    thPrint3);
        else
            break;
    }

    thPrint1.Terminate();
    thPrint2.Terminate();
    thPrint3.Terminate();
}

//--------------------------------------------------------------------------
// 类   : TCExceptionThread
// 用途 : 继承自TCThread，用以测试线程内的例外
//--------------------------------------------------------------------------
class TCExceptionThread : public TCThread
{
public:
    void Execute();
};

void TCExceptionThread::Execute()
{
    long i;

    for (i = 0; i < 1024; i++)
    {
#ifdef __TH_UNIX__
        for (long j = 0; j < 200000; j++);
#endif

        if (IsTerminated())
        {
            printf("*** Thread \"%d\" terminated ***\n", GetThreadID());
            return;
        }

        if (RandLong(512) == 2)
        {
            printf((char *)("*** Throw Exception at \""
                    + IntToStr(i) + "\" ***\n"));
            throw TCException("Throw Exception at \"" + IntToStr(i) + "\"");
        }

        printf("ThreadID: %d - Progress: %d\n", GetThreadID(), i);
        fflush(stdout);
    }
    printf("*** Thread \"%d\" Execute finished ***\n", GetThreadID());
}

//==========================================================================
// 函数 : TestThread4Exception
// 用途 : 测试线程内的例外
// 说明 : 线程内随机产生例外，测试线程例外机制
//==========================================================================
void TestThread4Exception()
{
    TCExceptionThread thPrint1, thPrint2, thPrint3;
    thPrint1.Start(false);
    thPrint2.Start(false);
    thPrint3.Start(false);
    TCSystem::ReadStringFromConsole();

    if (thPrint1.GetFatalException() != NULL)
        printf("Thread1 %d Result : Exception Msg ----- %s\n",
                thPrint1.GetThreadID(),
                (char *)thPrint1.GetFatalException()->GetExceptionMessage());
    else
        printf("Thread1 %d Result : Terminate Normally\n",
                thPrint1.GetThreadID());

    if (thPrint2.GetFatalException() != NULL)
        printf("Thread2 %d Result : Exception Msg ----- %s\n",
                thPrint2.GetThreadID(),
                (char *)thPrint2.GetFatalException()->GetExceptionMessage());
    else
        printf("Thread2 %d Result : Terminate Normally\n",
                thPrint2.GetThreadID());

    if (thPrint3.GetFatalException() != NULL)
        printf("Thread3 %d Result : Exception Msg ----- %s\n",
                thPrint3.GetThreadID(),
                (char *)thPrint3.GetFatalException()->GetExceptionMessage());
    else
        printf("Thread3 %d Result : Terminate Normally\n",
                thPrint3.GetThreadID());
}

//==========================================================================
// 函数 : TestThread5Suspend
// 用途 : 测试Suspend和Resume
// 说明 : 线程Suspend的状态下，无法有效地退出。如果有线程被Suspend，由于线
//        程内无法执行到Terminate, 所以析构函数将无限期地等待下去
//==========================================================================
void TestThread5Suspend()
{
    printf("        Press 1, 2, 3 key to suspend thread 1, 2, 3 \n");
    printf("        Press 6, 7, 8 key to resume thread 1, 2, 3 \n");
    printf("        Press other Key to terminate threads. \n\n");
    printf("        Press <Return> Key to Continue...\n");
    TCSystem::ReadStringFromConsole();

    TCPriorityThread thPrint1, thPrint2, thPrint3;
    thPrint1.SetDisplayChar('.');
    thPrint2.SetDisplayChar('-');
    thPrint3.SetDisplayChar('*');

    thPrint1.Start(false);
    thPrint2.Start(false);
    thPrint3.Start(false);

    while (true)
    {
        TCString sInput;
        char cInput;

        sInput = Left(TCSystem::ReadStringFromConsole(), 1);

        if (Length(sInput) < 1)
            break;
        cInput = sInput[1];

        if (cInput == '1')      thPrint1.Suspend();
        else if (cInput == '2') thPrint2.Suspend();
        else if (cInput == '3') thPrint3.Suspend();
        else if (cInput == '6') thPrint1.Resume();
        else if (cInput == '7') thPrint2.Resume();
        else if (cInput == '8') thPrint3.Resume();
        else
            break;
    }

    thPrint1.Terminate();
    thPrint2.Terminate();
    thPrint3.Terminate();
}

//--------------------------------------------------------------------------
// 类   : TCCriticalThread
// 用途 : 继承自TCThread，用以测试CriticalSection
//--------------------------------------------------------------------------
class TCCriticalThread : public TCThread
{
public:
    bool m_bUseCriticalSection;
    TCCriticalThread()
    {   m_bUseCriticalSection = false;
    };
    void Execute();
};

TCCriticalSection STA__LockForTestThread;

void TCCriticalThread::Execute()
{
    long i, j;
    bool bUseCriticalSection;

    for (i = 0; i < 1024; i++)
    {
        if (IsTerminated())
        {
            printf("*** Thread \"%d\" terminated ***\n", GetThreadID());
            return;
        }

        char buffer[128];
        sprintf(buffer, "ThreadID: %d - Progress: %d\n", GetThreadID(), i);

        bUseCriticalSection = m_bUseCriticalSection;

#ifdef __TH_WINDOWS__
        for (long nTmp = 0; nTmp < 10000000; nTmp++);
#endif

#ifdef __TH_UNIX__
        for (long nTmp = 0; nTmp < 1000000; nTmp++);       
#endif

        if (bUseCriticalSection)
            STA__LockForTestThread.Enter();

        for (j = 0; j < (long)strlen(buffer); j++)
        {
#ifdef __TH_UNIX__
            for (long k = 0; k < 100000; k++);
#endif
            printf("%c", buffer[j]);
            fflush(stdout);
        }

        if (bUseCriticalSection)
            STA__LockForTestThread.Leave();
    }
    printf("*** Thread \"%d\" Execute finished ***\n", GetThreadID());
}

//==========================================================================
// 函数 : TestThread6Critical
// 用途 : 测试CriticalSection与Thread的配合使用
//==========================================================================
void TestThread6Critical()
{
    printf("        Press \"C\" key to toggle critical mode \n");
    printf("        Press <Return> Key to Continue...\n");
    TCSystem::ReadStringFromConsole();

    TCCriticalThread thPrint1, thPrint2, thPrint3;
    thPrint1.Start(false);
    thPrint2.Start(false);
    thPrint3.Start(false);

    while (true)
    {
        TCString sInput;

        sInput = TCSystem::ReadStringFromConsole();

        if (Left(sInput, 1) == "c" || Left(sInput, 1) == "C")
        {
            thPrint1.m_bUseCriticalSection = !thPrint1.m_bUseCriticalSection;
            thPrint2.m_bUseCriticalSection = !thPrint2.m_bUseCriticalSection;
            thPrint3.m_bUseCriticalSection = !thPrint3.m_bUseCriticalSection;
        }
        else
            break;
    }

    thPrint1.Terminate();
    thPrint2.Terminate();
    thPrint3.Terminate();
}

//--------------------------------------------------------------------------
// 类   : TCSynchronizeThread
// 用途 : 继承自TCThread，用以测试Synchronize
//--------------------------------------------------------------------------
class TCSynchronizeThread : public TCThread
{
public:
    bool m_bUseSynchronizeMethod;
    long m_nProgress;
    TCSynchronizeThread()
    {   m_bUseSynchronizeMethod = false;
    };
    void Execute();
};

void ShowMessage(void * pArg)
{
    TCSynchronizeThread * pThread;

    pThread = (TCSynchronizeThread *)pArg;

    long i;

    char buffer[128];
    sprintf(buffer, "ThreadID: %d - Progress: %d\n", pThread->GetThreadID(),
            pThread->m_nProgress);

    for (i = 0; i < (long)strlen(buffer); i++)
    {
#ifdef __TH_UNIX__
        for (long j = 0; j < 100000; j++);
#endif
        printf("%c", buffer[i]);
        fflush(stdout);
    }
}

void TCSynchronizeThread::Execute()
{
    long i;

    for (i = 0; i < 1024; i++)
    {
#ifdef __TH_WINDOWS__
        for (long nTmp = 0; nTmp < 10000000; nTmp++);
#endif

        if (IsTerminated())
        {
            printf("*** Thread \"%d\" terminated ***\n", GetThreadID());
            return;
        }

        m_nProgress = i;

        if (m_bUseSynchronizeMethod)
            Synchronize(ShowMessage, this);
        else
            ShowMessage(this);
    }

    printf("*** Thread \"%d\" Execute finished ***\n", GetThreadID());
}

class TCCheckSyncThread : public TCThread
{
public:
    void Execute()
    {
        while (true)
        {
            CheckSynchronize();
            if (IsTerminated())
            {
                CheckSynchronize();
                printf("*** CheckSyncThread \"%d\" terminated ***\n",
                        GetThreadID());
                return;
            }
            SleepMilliSeconds(1);
        }
    };

    void DoTerminate()
    {
        //==== 注: 由于必须把Synchronize需要完成的东西做完其它线程才能析构 ===
        while (GetTotalThreadCount() > 1)
        {
            CheckSynchronize();
            SleepMilliSeconds(1);
        }
    };
};

//==========================================================================
// 函数 : TestThread7Synchronize
// 用途 : 测试线程的Synchronize方法调用
//==========================================================================
void TestThread7Synchronize()
{
    printf("        Press \"S\" key to toggle synchronize mode \n");
    printf("        Press <Return> key to Continue...\n");
    TCSystem::ReadStringFromConsole();

    TCCheckSyncThread thCheckSync;
    thCheckSync.Start(false);

    TCSynchronizeThread thPrint1, thPrint2, thPrint3;
    thPrint1.Start(false);
    thPrint2.Start(false);
    thPrint3.Start(false);

    while (true)
    {
        TCString sInput;

        sInput = TCSystem::ReadStringFromConsole();

        if (Left(sInput, 1) == "s" || Left(sInput, 1) == "S")
        {
            thPrint1.m_bUseSynchronizeMethod
                    = !thPrint1.m_bUseSynchronizeMethod;
            thPrint2.m_bUseSynchronizeMethod
                    = !thPrint2.m_bUseSynchronizeMethod;
            thPrint3.m_bUseSynchronizeMethod
                    = !thPrint3.m_bUseSynchronizeMethod;
        }
        else
            break;
    }

    thPrint1.Terminate();
    thPrint2.Terminate();
    thPrint3.Terminate();

    thCheckSync.Terminate();
}

//==========================================================================
// 函数 : TestThreadMainFunc
// 用途 : 测试主函数
//==========================================================================
void TestThreadMainFunc()
{
    int cChar;

    DisplayTestThreadPrompt();

    while (true)
    {
        cChar = getchar();

        switch (cChar)
        {
            case 'Q':
            case 'q':
            case 0x1B:
                return;

#ifdef __TH_UNIX__
            case 'U':
            case 'u':
                TestThreadUUnixSimpleThread(0);
                break;

            case 'D':
            case 'd':
                TestThreadUUnixSimpleThread(1);
                break;

            case 'I':
            case 'i':
                TestThreadUUnixSimpleThread(2);
                break;
#endif

            case '0':
                TestThread0PrintThread();
                break;

            case '1':
                TestThread1PointerAccess();
                break;

            case '2':
                TestThread2Terminate();
                break;

            case '3':
                TestThread3Priority();
                break;

            case '4':
                TestThread4Exception();
                break;

            case '5':
                TestThread5Suspend();
                break;

            case '6':
                TestThread6Critical();
                break;

            case '7':
                TestThread7Synchronize();
                break;

            default:
                continue;
        }
        DisplayTestThreadPrompt();
    }
}

#endif




