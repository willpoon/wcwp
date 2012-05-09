//---------------------------------------------------------------------------

#ifndef c_threadH
#define c_threadH
//---------------------------------------------------------------------------
#include "cmpublic.h"
#include "c_critical_section.h"

#ifndef __WIN32__
#include <semaphore.h>
#endif

//---------------------------------------------------------------------------
// __TH_UNIX__      : UNIX�����µ�ʵ��
// __TH_FAKE_UNIX__ : ��Win32������ģ��UNIX��ʵ�ֺ���
// __TH_WINDOWS__   : Windows�����µ�ʵ��
//
// ����
// ��UNIX�����£�
//        #define __TH_UNIX__         ;
//        #undef  __TH_FAKE_UNIX__    ; ��ΪUNIX���������д˿⺯��
//        #undef  __TH_WINDOWS__      ;
//
// ��Windows�����£�Ҫ����UNIX������ʵ�֣�
//        #define __TH_UNIX__         ; ��Ҫ����UNIX����ʵ��
//        #define __TH_FAKE_UNIX__    ; ������Windows������ģ���UNIXʵ�ֺ���
//        #undef  __TH_WINDOWS__      ; ������Ӧ��Windowsʵ��
//
// ������Windows�����£�
//        #undef  __TH_UNIX__         ;
//        #undef  __TH_FAKE_UNIX__    ;
//        #define __TH_WINDOWS__      ;
//---------------------------------------------------------------------------
#define __TEST_UNIX_IN_WINDOWS__ 0

#ifdef __WIN32__
#if __TEST_UNIX_IN_WINDOWS__ == 1
#define __TH_UNIX__
#define __TH_FAKE_UNIX__
#undef  __TH_WINDOWS__
#else
#undef  __TH_UNIX__
#undef  __TH_FAKE_UNIX__
#define __TH_WINDOWS__
#endif
#else
#define __TH_UNIX__
#undef  __TH_FAKE_UNIX__
#undef  __TH_WINDOWS__
#endif

#ifdef __TH_FAKE_UNIX__

const int SIGCONT = 12;
const int SIGSTOP = 13;

typedef int sem_t;

int sem_init(sem_t *sem, int pshared, unsigned int value) { return 0; };

int sem_destroy(sem_t * sem) { return 0; };
int sem_wait(sem_t * sem) { return 0; };
int sem_post(sem_t * sem) { return 0; };
int sem_getvalue(sem_t *sem, int *sval);

typedef unsigned int pthread_t;
typedef int pthread_attr_t;

int  pthread_create(pthread_t *tid, const pthread_attr_t *attr,
        void *(*start_routine)(void *), void *arg) { return 0; };
int  pthread_kill(pthread_t thread, int nSignal) { return 0; };
void pthread_exit(void *value_ptr) { };
int  pthread_join(pthread_t thread, void **value_ptr) { return 0; };
void pthread_detach(pthread_t thread) { };
pthread_t  pthread_self() { return 0; };

typedef struct
    {   int sched_priority;
    } sched_param;
int pthread_getschedparam(pthread_t thread, int *p, sched_param *j)
{ return 0; };
int pthread_setschedparam(pthread_t thread, int nPolicy, sched_param *p)
{ return 0; };

typedef int pthread_cond_t;
int pthread_cond_wait(pthread_cond_t * cond, pthread_mutex_t * mutex)
{ return 0; };
int pthread_cond_signal(pthread_cond_t * cond)
{ return 0; };
#endif

#ifdef __TH_WINDOWS__
//------------------------------------------------------------------------
// ö�� : TEThreadPriority
// ��; : �̵߳����ȼ�
//------------------------------------------------------------------------
enum TEThreadPriority
{   tpIdle,
    tpLowest,
    tpLower,
    tpNormal,
    tpHigher,
    tpHighest,
    tpTimeCritical
};
#endif

long GetTotalThreadCount();
bool CheckSynchronize();

//------------------------------------------------------------------------
// ��   : TCThread
// ��; : �߳���
//------------------------------------------------------------------------
class TCThread
{
private:
    unsigned long   m_nHandle;              // �߳̾����ʹ��ʱתΪvoid *

#ifdef __TH_WINDOWS__
    unsigned long    m_nThreadID;            // �߳�ID
#endif

#ifdef __TH_UNIX__
    pthread_t       m_nThreadID;
#endif

#ifdef __TH_UNIX__
    sem_t       m_semCreateSuspendedSem;    // �̴߳���ʱ������ź���
    bool        m_bInitialSuspendDone;      // �̳߳��������ѱ�����
#endif

    bool            m_bCreateSuspended;     // �̴߳���ʱ�Ĺ���״̬
    bool            m_bSuspended;           // �̹߳���״̬

    bool            m_bTerminated;          // �Ƿ���Ҫ�˳�
    bool            m_bFreeOnTerminate;    // �߳��˳�ʱ�Ƿ��ͷ�

    long            m_nReturnValue;         // �̷߳���ֵ
    //bool            m_bFinished;            // �߳��Ƿ��ѽ�������

    void            (*m_funcSyncMethod)(void *);    // ͬ�����ú���ָ��
    void            *m_pSyncMethodParam;            // ͬ�����ò���ָ��

    TCException *   m_pSynchronizeException;        // ͬ������
    TCException *   m_pFatalException;              // �߳�����

    void AfterConstruction();               // �߳����к�Ĵ���

    void CheckThreadError(long nErrCode);           // ����̴߳���(����)
    void CheckThreadErrorSuccess(bool bSuccess);    // ����̴߳���(����)

protected:
    bool            m_bFinished;            // �߳��Ƿ��ѽ�������

    virtual void    DoTerminate();                  // �߳��˳�ʱ�Ĺ���
    virtual void    Execute() = 0;                  // �߳�ʵ�����к���
    bool            IsTerminated();                 // �߳��Ƿ��˳�

    long            GetReturnValue();               // �õ��̷߳���ֵ
    void            SetReturnValue(long nValue);    // �����̷߳���ֵ

public:
    TCThread();                                     // ���캯��
    virtual ~TCThread();                            // ��������

    void            Start(bool bCreateSuspended);   // �����߳�

    void            Resume();                       // �����߳�
    void            Suspend();                      // �����߳�

    void            Terminate();                    // ��Ϣ�˳��߳�
    DWORD           WaitFor();                      // �ȴ��˳��߳�

    TCException *   GetFatalException();            // ����߳�����

    bool IsFreeOnTerminate();                          // ����߳��˳�ʱ�ͷ�
    void SetFreeOnTerminate(bool bFreeOnTerminate);   // �����߳��˳�ʱ�ͷ�


    DWORD           GetHandle();                    // ����߳̾��

#ifdef __TH_WINDOWS__
    TEThreadPriority    GetPriority();                          // ������ȼ�
    void                SetPriority(TEThreadPriority tpValue);  // �������ȼ�
#endif

#ifdef __TH_UNIX__
    int                 GetPriority();              // ������ȼ�
    void                SetPriority(int nValue);    // �������ȼ�
    int                 GetPolicy();                // ��ò���
    void                SetPolicy(int nValue);      // ���ò���
#endif

    bool                IsSuspended();              // �Ƿ����
    void                SetSuspended(bool bValue);  // ���ù���

    DWORD   GetThreadID();                                  // ����߳�ID
    void    Synchronize(void (*funcNeedSync)(void *),       // ͬ����������
                    void *pSyncParam);

    void    SleepMilliSeconds(long nMilliSeconds);

friend DWORD ThreadProc(TCThread * pThread);
friend bool CheckSynchronize();
};

//------------------------------------------------------------------------
// ��   : TCThreadException
// ��; : �߳�������
//------------------------------------------------------------------------
class TCThreadException : public TCException
{
public:
    TCThreadException(TCString sMsg);
};

#ifdef __TEST__
    void TestThreadMainFunc();
#endif

#endif


