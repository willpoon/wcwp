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
// �ṹ : TSSyncProc
// ��; : ͬ������ṹ������ͬ����������
//------------------------------------------------------------------------
struct TSSyncProc
{
    TCThread * m_pThread;           // �߳���ָ��
#ifdef __TH_WINDOWS__
    DWORD m_nSignal;                // Windows�ź�
#endif

#ifdef __TH_UNIX__
    pthread_cond_t m_tcSignal;      // UNIX�ź�
#endif
};

//---------------------------------------------------------------------------
// ��������
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
// ����ָ�� : STA__funcEndThreadProc
// ��;     : �ɰ�װ�Ľ����̺߳������
//--------------------------------------------------------------------------
void (*STA__funcEndThreadProc)(long nExitCode) = NULL;
#endif

//==========================================================================
// ���� : BeginThread
// ��; : ��װUNIX�е��̴߳���
// ԭ�� : DWORD BeginThread(pthread_attr_t * pThreadAttr,
//              void * (*pfuncThreadFunc)(void * pParameter),
//              void * pParameter, DWORD * pThreadID);
// ���� : pThreadAttr     - �߳�����
//        pfuncThreadFunc - �߳̿�ʼִ��ʱ���õĺ���
//        pParameter      - start_routineռ��һ������������һ��ָ��void��ָ��
//        pThreadID       - �������̵߳�ID
// ���� : �ɹ�ʱ����0�����ɹ�ʱΪһ������Ĵ������
// ˵�� : �ú�����װ��pthread_create��ʵ��
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
// ���� : EndThread
// ��; : Windows�����½���һ���̵߳�ʵ��
// ԭ�� : void EndThread(long nExitCode);
// ���� : �˳���
// ���� : ��
// ˵�� :
//==========================================================================
void EndThread(long nExitCode)
{
    ExitThread(nExitCode);
}
#endif

#ifdef __TH_UNIX__
//==========================================================================
// ���� : GetCurrentThreadID
// ��; : ��װUNIX�õ������߳�ID
// ԭ�� : DWORD GetCurrentThreadID();
// ���� : ��
// ���� : �߳�ID
// ˵�� :
//==========================================================================
DWORD GetCurrentThreadID()
{
    return (DWORD)pthread_self();
}

//==========================================================================
// ���� : EndThread
// ��; : UNIX�����½���һ���̵߳�ʵ��
// ԭ�� : void EndThread(long nExitCode);
// ���� : �˳���
// ���� : ��
// ˵�� :
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

bool                STA__bProcPosted;           // �Ѱ�װͬ������
TCList *            STA__pSyncList = NULL;      // ͬ�������б�
TCCriticalSection   STA__csThreadLock;          // ������
long                STA__nThreadCount;          // �߳�����

void DeleteSyncList()
{
    if (STA__pSyncList != NULL)
    {
        delete STA__pSyncList;
        STA__pSyncList = NULL;
    }
}
//==========================================================================
// ���� : AddThread
// ��; : ����һ���̣߳��̼߳�����1
// ԭ�� : void AddThread()
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : RemoveThread
// ��; : ɾ��һ���̣߳��̼߳�����1
// ԭ�� : void RemoveThread()
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : ThreadProc
// ��; : �̵߳�ִ�к���
// ԭ�� : DWORD ThreadProc(TCThread * pThread);
// ���� : �߳����ָ��
// ���� : ȡpThread��m_nReturnValue��Ϊ����ֵ
// ˵�� : ����SUN��Solaris�У����߳�ȫ������ʱ��Ҫ�ȵ�һ���߳��˳�ʱ����һ��
//        �̲߳�����������������һ���߳�����ǰ��ʱһ���Ծ��������������Ĳ�
//        �������⣬���̵߳�ʵ�ʴ�������У�Ӧ�����������SleepMilliSeconds
//        ������
//==========================================================================
DWORD ThreadProc(TCThread * pThread)
{
    bool bFreeThread;
    long nRet;

#ifdef __TH_UNIX__
    //========= 1. (UNIX)���������ȴ� =======
    if (pThread->m_bSuspended)
        sem_wait(&pThread->m_semCreateSuspendedSem);
#endif

    try
    {
        //======= 2. �����߳� ========
        if (!pThread->IsTerminated())
        {
            try
            {
                //--------- ��������һ����䣬�Ա���Solaris�е��߳����� ----
                pThread->SleepMilliSeconds(1);
                //----------- ���ӽ��� ---------
                
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
        //========= 3. ��һЩ�ͷ����� ========
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
        // ֱ��ͨ��EndThread����pthread_exit��detach���̣߳�����
        // TCThread::WaitFor�е�pthread_join���ó���
        if (STA__funcEndThreadProc != NULL)
            (*STA__funcEndThreadProc)(nRet);
        pthread_exit(&nRet);
#endif
        throw;
    }

    //========= 4. ����һ��catch(...)��ȫ��ͬ���ﵽfinally��Ч�� ======
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
// ���� : ThreadWinAPIProc
// ��; : ��WINAPI����ʽ��װ��������CreateThread�ĵ���
// ԭ�� : DWORD WINAPI ThreadWinAPIProc(void * pThread);
// ���� : �߳����ָ��
// ���� : ȡpThread��m_nReturnValue��Ϊ����ֵ
// ˵�� : �ú���ֱ�ӵ���ThreadProc
//==========================================================================
DWORD WINAPI ThreadWinAPIProc(void * pThread)
{
    return ThreadProc((TCThread *)pThread);
}
#endif

#ifdef __TH_UNIX__
//==========================================================================
// ���� : ThreadUnixProc
// ��; : ��UNIX����ʽ��װ��������BeginThread�ĵ���
// ԭ�� : void * ThreadUnixProc(void * pThread);
// ���� : �߳����ָ��
// ���� : ȡpThread��m_nReturnValue��Ϊ����ֵ
// ˵�� : �ú���ֱ�ӵ���ThreadProc
//==========================================================================
void * ThreadUnixProc(void * pThread)
{
    return (void *)ThreadProc((TCThread *)pThread);
}
#endif

//==========================================================================
// ���� : GetTotalThreadCount
// ��; : �õ����߳��������������̣߳�
// ԭ�� : long GetTotalThreadCount();
// ���� : ��
// ���� : ���߳��������������̣߳�
// ˵�� : 
//==========================================================================
long GetTotalThreadCount()
{
    return STA__nThreadCount;
}

//==========================================================================
// ���� : CheckSynchronize
// ��; : ��鲢����ͬ���¼�
// ԭ�� : bool CheckSynchronize();
// ���� : ��
// ���� : ����з�����ͬ�����У��򷵻��棻���ʲô��û�����򷵻ؼ١�
// ˵�� : �����߳��������Ե�ִ��CheckSynchronize�����������߳��жԺ�̨�߳�
//        ִ��ͬ������
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
// ���� : TCThread::TCThread
// ��; : ���캯��
// ԭ�� : TCThread();
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : TCThread::~TCThread
// ��; : ��������
// ԭ�� : ~TCThread();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
TCThread::~TCThread()
{
    //======== 1. ����߳�δ�������򷢽������󣬲��ȴ��߳̽��� ========
    if (m_nThreadID != 0 && !m_bFinished)
    {
        Terminate();

        if (m_bCreateSuspended)
            Resume();

        WaitFor();
    }

    //======== 2. �ͷž�����ź��� ===========
#ifdef __TH_WINDOWS__
    if (m_nHandle != 0)
        CloseHandle((void *)m_nHandle);
#endif

#ifdef __TH_UNIX__
    if (m_nThreadID != 0)
        pthread_detach(m_nThreadID);
    sem_destroy(&m_semCreateSuspendedSem);
#endif

    //======= 3. �ͷ�FatalExceptionָ�� ========
    delete m_pFatalException;
    m_pFatalException = NULL;

    //======= 4. ɾ���߳� =======
    RemoveThread();
}

//==========================================================================
// ���� : TCThread::AfterConstruction
// ��; : �ڽ����̺߳󣬸������������߳�
// ԭ�� : void TCThread::AfterConstruction();
// ���� : ��
// ���� : ��
// ˵�� : ͨ��Start()��������
//==========================================================================
void TCThread::AfterConstruction()
{
    if (!m_bCreateSuspended)
        Resume();
}

//==========================================================================
// ���� : TCThread::CheckThreadError
// ��; : ����̵߳��÷���ֵ������������׳�����
// ԭ�� : private void CheckThreadError(long nErrCode);
// ���� : �̵߳��÷��صĴ�����
// ���� : ��
// ˵�� : ������صĴ�������㣬���׳�����
//==========================================================================
void TCThread::CheckThreadError(long nErrCode)
{
    if (nErrCode != 0)
        throw TCThreadException("Thread Error - "
                + TCSystem::SysErrorMessage(nErrCode));
}

//==========================================================================
// ���� : TCThread::CheckThreadErrorSuccess
// ��; : ����̵߳��÷���ֵ�����Ϊ�����׳�����
// ԭ�� : private void CheckThreadErrorSuccess(long nErrCode);
// ���� : �̵߳��÷��ص��Ƿ�ɹ���Ϣ
// ���� : ��
// ˵�� : ������صĴ�����Ϊ�٣��㡢���ɹ��������׳�����
//==========================================================================
void TCThread::CheckThreadErrorSuccess(bool bSuccess)
{
    if (!bSuccess)
        CheckThreadError(TCSystem::GetLastError());
}

//==========================================================================
// ���� : TCThread::DoTerminate
// ��; : �߳��˳�ʱ���øú�������������ظú�������ɾ���ʵ��
// ԭ�� : virtual void DoTerminate();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCThread::DoTerminate()
{
}

//==========================================================================
// ���� : TCThread::GetFatalException
// ��; : �õ���ʹ���������������ָ��
// ԭ�� : TCException * TCThread::GetFatalException();
// ���� : ��
// ���� : �������ָ��
// ˵�� :
//==========================================================================
TCException * TCThread::GetFatalException()
{
    return m_pFatalException;
}

//==========================================================================
// ���� : TCThread::GetHandle
// ��; : �õ��߳̾��
// ԭ�� : DWORD GetHandle();
// ���� : ��
// ���� : �߳̾��
// ˵�� :
//==========================================================================
DWORD TCThread::GetHandle()
{
    return m_nHandle;
}

#ifdef __TH_WINDOWS__
//--------------------------------------------------------------------------
// ���� : STA__Priorities (Windows Implemetation)
// ��; : �߳����ȼ��ĳ������塣�����ö�����볣��֮��Ļ���ת��
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
// ���� : TCThread::GetPriority (Windows Implemetation)
// ��; : �õ��̵߳����ȼ�
// ԭ�� : TEThreadPriority GetPriority();
// ���� : ��
// ���� : �̵߳����ȼ�ö��
// ˵�� :
//==========================================================================
TEThreadPriority TCThread::GetPriority()
{
    //====== 1. �õ��̵߳����ȼ� =======
    int nPriority;
    nPriority = GetThreadPriority((void *)m_nHandle);
    CheckThreadErrorSuccess(nPriority != THREAD_PRIORITY_ERROR_RETURN);

    //======= 2. �õ������ö������ =======
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
// ���� : TCThread::GetPolicy (UNIX Implemetation)
// ��; : �õ��̵߳Ĳ���
// ԭ�� : int GetPolicy();
// ���� : ��
// ���� : �̵߳Ĳ���
// ˵�� :
//==========================================================================
int TCThread::GetPolicy()
{
    sched_param J;
    int nRet;
    CheckThreadError(pthread_getschedparam(m_nThreadID, &nRet, &J));
    return nRet;
}

//==========================================================================
// ���� : TCThread::GetPriority (UNIX Implemetation)
// ��; : �õ��̵߳����ȼ�
// ԭ�� : TEThreadPriority GetPriority();
// ���� : ��
// ���� : �̵߳����ȼ�����ֵ
// ˵�� :
//    Unix�����ȼ����Լƻ�����Ϊ����ʵ�ֵġ��������ֲ��ԡ�
//
//          ����          ����           ���ȼ�
//      ----------      --------       --------
//      SCHED_RR        RealTime         0-99
//      SCHED_FIFO      RealTime         0-99
//      SCHED_OTHER     Regular           0
//
//    SCHED_RR��SCHED_FIFOֻ�ܱ�root�û����á�
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
// ���� : TCThread::GetReturnValue
// ��; : �õ��̵߳ķ���ֵ
// ԭ�� : long GetReturnValue();
// ���� : ��
// ���� : �̵߳ķ���ֵ
// ˵�� :
//==========================================================================
long TCThread::GetReturnValue()
{
    return m_nReturnValue;
}

//==========================================================================
// ���� : TCThread::GetThreadID
// ��; : �õ��߳�ID
// ԭ�� : DWORD GetThreadID();
// ���� : ��
// ���� : �߳�ID
// ˵�� :
//==========================================================================
DWORD TCThread::GetThreadID()
{
    return (DWORD)m_nThreadID;
}

//==========================================================================
// ���� : TCThread::IsFreeOnTerminate
// ��; : �õ��Ƿ��߳��˳�ʱ�Զ��ͷ��߳���ָ��
// ԭ�� : bool IsFreeOnTerminate();
// ���� : ��
// ���� : �Ƿ��߳��˳�ʱ�Զ��ͷ��߳���ָ��
// ˵�� :
//==========================================================================
bool TCThread::IsFreeOnTerminate()
{
    return m_bFreeOnTerminate;
}

//==========================================================================
// ���� : TCThread::IsSuspended
// ��; : �õ���ǰ�߳��Ƿ����
// ԭ�� : bool IsSuspended();
// ���� : ��
// ���� : ��ǰ�߳��Ƿ����
// ˵�� :
//==========================================================================
bool TCThread::IsSuspended()
{
    return m_bSuspended;
}

//==========================================================================
// ���� : TCThread::IsTerminated
// ��; : �õ���ǰ�߳��Ƿ���ֹ
// ԭ�� : bool IsTerminated();
// ���� : ��
// ���� : ��ǰ�߳��Ƿ���ֹ
// ˵�� :
//==========================================================================
bool TCThread::IsTerminated()
{
    return m_bTerminated;
}

//==========================================================================
// ���� : TCThread::Resume
// ��; : �����߳�
// ԭ�� : void Resume();
// ���� : ��
// ���� : ��
// ˵�� :
//      ����Suspend��Resume��POSIX��֧��suspending/resumingһ���̡߳����ڲ���
//      ��֤�ںδ��̱߳�����Suspendһ���̱߳���Ϊ��һ��Σ�յĶ������������
//      �̿���Ҳ���һ��������������������������һ��������С�Ϊ��ģ�¹���/��
//      �Ѳ����������õ����źš���SIGSTOPȥ���ѹ���һ���̣߳���SIGCONTȥ����
//      һ���̡߳����ַ���ֻ������Linux����Ϊ����POSIX��׼�����һ���߳̽���
//      ��SIGSTOP�����������̶���ֹͣ���С���Linux����ȫ����POSIX��׼�������
//      ��ȫ���POSIX��׼����ôsuspend��resume���޷�������
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
// ���� : TCThread::SetFreeOnTerminate
// ��; : �����Ƿ��߳��˳�ʱ�Զ��ͷ��߳���ָ��
// ԭ�� : void SetFreeOnTerminate(bool bFreeOnTerminate);
// ���� : �Ƿ��߳��˳�ʱ�Զ��ͷ��߳���ָ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCThread::SetFreeOnTerminate(bool bFreeOnTerminate)
{
    m_bFreeOnTerminate = bFreeOnTerminate;
}

#ifdef __TH_WINDOWS__
//==========================================================================
// ���� : TCThread::SetPriority (Windows Implemetation)
// ��; : �����̵߳����ȼ�
// ԭ�� : void SetPriority(TEThreadPriority tpValue);
// ���� : �̵߳����ȼ�ö��
// ���� : ��
// ˵�� :
//==========================================================================
void TCThread::SetPriority(TEThreadPriority tpValue)
{
    CheckThreadErrorSuccess(SetThreadPriority((void *)m_nHandle,
            STA__Priorities[tpValue]));
}
#endif

#ifdef __TH_UNIX__
//==========================================================================
// ���� : TCThread::SetPolicy (UNIX Implemetation)
// ��; : �����̵߳Ĳ���
// ԭ�� : void SetPolicy(int nValue);
// ���� : �̵߳Ĳ���
// ���� : ��
// ˵�� :
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
// ���� : TCThread::SetPriority (UNIX Implemetation)
// ��; : �����̵߳����ȼ�
// ԭ�� : void TCThread::SetPriority(int nValue);
// ���� : �̵߳����ȼ�����ֵ
// ���� : ��
// ˵�� :
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
// ���� : TCThread::SetReturnValue
// ��; : �����̵߳ķ���ֵ
// ԭ�� : void SetReturnValue(long nValue);
// ���� : �̵߳ķ���ֵ
// ���� : ��
// ˵�� :
//==========================================================================
void TCThread::SetReturnValue(long nValue)
{
    m_nReturnValue = nValue;
}

//==========================================================================
// ���� : TCThread::SetSuspended
// ��; : ���õ�ǰ�߳��Ƿ����
// ԭ�� : void SetSuspended(bool bValue);
// ���� : ��ǰ�߳��Ƿ����
// ���� : ��
// ˵�� :
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
// ���� : TCThread::Start
// ��; : ����һ���̣߳�������
// ԭ�� : void TCThread::Start(bool bCreateSuspended);
// ���� : �����߳�ʱ�Ƿ��ʼ�ڹ���״̬
// ���� : ��
// ˵�� :
//==========================================================================
void TCThread::Start(bool bCreateSuspended)
{
    //======= 1. ����AddThreadʹ�̼߳�����1 =======
    AddThread();

    //====== 2. ��ֵ��ʶ�Ƿ����ĳ�Ա���� ==========
    m_bSuspended = bCreateSuspended;
    m_bCreateSuspended = bCreateSuspended;

#ifdef __TH_WINDOWS__
    //======= 3. (Windows)�����̣߳������ڹ���״̬ =======
    m_nHandle = (DWORD)CreateThread(NULL, 0, ThreadWinAPIProc, this,
            CREATE_SUSPENDED, &m_nThreadID);
    if (m_nHandle == 0)
        throw TCThreadException("Thread Create Error - "
                + TCSystem::SysErrorMessage(TCSystem::GetLastError()));
#endif

#ifdef __TH_UNIX__
    //======= 4. (UNIX)�����̣߳������ڹ���״̬ =======
    long nErrCode;
    sem_init(& m_semCreateSuspendedSem, false, 0);
    nErrCode = BeginThread(NULL, ThreadUnixProc, this, &m_nThreadID);
    if (nErrCode != 0)
        throw TCThreadException(TCString("Thread Create Error - ")
                + TCSystem::SysErrorMessage(TCSystem::GetLastError()));
#endif

    //===== 5. �����̴߳��������������̻߳������� ======
    AfterConstruction();
}

//==========================================================================
// ���� : TCThread::Suspend
// ��; : �����߳�
// ԭ�� : void Suspend();
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : TCThread::Synchronize
// ��; : ͬ����������
// ԭ�� : void Synchronize(void (*funcNeedSync)());
// ���� : ͬ�����÷���ָ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCThread::Synchronize(void (*funcNeedSync)(void *), void * pSyncParam)
{
    TSSyncProc spSyncProc;

#ifdef __TH_WINDOWS__
    //======= 1. (Windows) �����¼� =========
    spSyncProc.m_nSignal = (DWORD)CreateEvent(NULL, true, false, NULL);
    try
    {
#endif

#ifdef __TH_UNIX__
        //======== 2. (UNIX) ��һ��ͬʱҲ��ʼ������������ =======
        memset(&spSyncProc, '\0', sizeof(spSyncProc));
#endif

        STA__csThreadLock.Enter();
        try
        {
            //======= 3. ����ͬ�������б� =======
            m_pSynchronizeException = NULL;
            m_funcSyncMethod = funcNeedSync;
            m_pSyncMethodParam = pSyncParam;
            spSyncProc.m_pThread = this;

            STA__pSyncList->Add(&spSyncProc);
            STA__bProcPosted = true;

#ifdef __TH_WINDOWS__
            //======= 4. (Windows)�ȴ�ͬ���¼����֪ͨ ======
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
            //======= 5. (UNIX)�ȴ�ͬ���������� =====
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

    //====== 6. ���ͬ���¼������⸳ֵ�������߳����׳������� ======
    if (m_pSynchronizeException != NULL)
        throw *m_pSynchronizeException;
}

//==========================================================================
// ���� : TCThread::Terminate
// ��; : ֪ͨ�߳��˳�
// ԭ�� : void Terminate()
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCThread::Terminate()
{
    m_bTerminated = true;
}

//==========================================================================
// ���� : TCThread::WaitFor
// ��; : �ȴ��߳��˳�
// ԭ�� : DWORD TCThread::WaitFor();
// ���� : ��
// ���� : �̵߳��˳���
// ˵�� :
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
// ���� : TCThreadException::TCThreadException
// ��; : ���캯��
// ԭ�� : TCThreadException(TCString sMsg);
// ���� : �������������
// ���� : ��
// ˵�� :
//==========================================================================
TCThreadException::TCThreadException(TCString sMsg) : TCException(sMsg)
{
}

#ifdef __TEST__

//==========================================================================
// ���� : DisplayTestThreadPrompt
// ��; : ��ʾ��ʾ��Ϣ
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
const long TEST_NUM_THREADS = 5;            // ���ɵĲ����߳���
pthread_t STA__tid[TEST_NUM_THREADS];       // �߳�ID����

//==========================================================================
// ���� : TestThreadUUnixSimpleThread__ThreadExec
// ��; : �߳����к���
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
// ���� : TestThreadUUnixSimpleThread
// ��; : ���Լ򵥵�Unix�̣߳�����ķ�ʽ��
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
// ��   : TCPrintThread
// ��; : �̳���TCThread��������ʾ�򵥵��߳�ʵ��
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
// ���� : TestThread0PrintThread
// ��; : ���Լ򵥵��߳�
// ˵�� : �����̵߳��ͷ�����Ҫ�ȴ��̵߳��˳�������Ҫ�ȵ������߳��˳��󣬺���
//        �ŷ���
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
// ���� : TestThread0PrintThread
// ��; : ͨ������ָ��ķ�ʽʹ���̣߳��߳̽������Զ��ͷ�ָ��
// ˵�� : ����ͨ������������𶯶���̡߳�ͨ���÷�ʽ����120���̣߳�֤��
//        �ǳɹ��ġ�
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
// ��   : TCTerminateThread
// ��; : �̳���TCThread�����Բ���Terminate
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
// ���� : TestThread2Terminate
// ��; : ����Terminate
// ˵�� : �ڵ���Terminate()�Ժ��̵߳�Execute()�ж�IsTerminated, Ȼ�����
//        �˳�����
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
// ��   : TCPriorityThread
// ��; : �̳���TCThread�����Բ����߳����ȼ�
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
// ���� : TestThread3Priority__GetDes
// ��; : �õ����ȼ�������˵��
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
// ���� : TestThread3Priority__RearrangePriority
// ��; : �������ʽ���ý��̵����ȼ�
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
// ���� : TestThread3Priority
// ��; : ����Priority
// ˵�� : �����߳�ͬʱ���С��������"P"�����������ʽ���ý��̵����ȼ�
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
// ��   : TCExceptionThread
// ��; : �̳���TCThread�����Բ����߳��ڵ�����
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
// ���� : TestThread4Exception
// ��; : �����߳��ڵ�����
// ˵�� : �߳�������������⣬�����߳��������
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
// ���� : TestThread5Suspend
// ��; : ����Suspend��Resume
// ˵�� : �߳�Suspend��״̬�£��޷���Ч���˳���������̱߳�Suspend��������
//        �����޷�ִ�е�Terminate, �������������������ڵصȴ���ȥ
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
// ��   : TCCriticalThread
// ��; : �̳���TCThread�����Բ���CriticalSection
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
// ���� : TestThread6Critical
// ��; : ����CriticalSection��Thread�����ʹ��
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
// ��   : TCSynchronizeThread
// ��; : �̳���TCThread�����Բ���Synchronize
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
        //==== ע: ���ڱ����Synchronize��Ҫ��ɵĶ������������̲߳������� ===
        while (GetTotalThreadCount() > 1)
        {
            CheckSynchronize();
            SleepMilliSeconds(1);
        }
    };
};

//==========================================================================
// ���� : TestThread7Synchronize
// ��; : �����̵߳�Synchronize��������
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
// ���� : TestThreadMainFunc
// ��; : ����������
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




