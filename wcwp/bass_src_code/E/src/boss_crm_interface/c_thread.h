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
// __TH_UNIX__      : UNIX环境下的实现
// __TH_FAKE_UNIX__ : 在Win32环境下模拟UNIX的实现函数
// __TH_WINDOWS__   : Windows环境下的实现
//
// 所以
// 在UNIX环境下，
//        #define __TH_UNIX__         ;
//        #undef  __TH_FAKE_UNIX__    ; 因为UNIX环境下已有此库函数
//        #undef  __TH_WINDOWS__      ;
//
// 在Windows环境下，要测试UNIX环境的实现，
//        #define __TH_UNIX__         ; 需要测试UNIX环境实现
//        #define __TH_FAKE_UNIX__    ; 必须在Windows环境下模拟出UNIX实现函数
//        #undef  __TH_WINDOWS__      ; 屏蔽相应的Windows实现
//
// 真正的Windows环境下，
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
// 枚举 : TEThreadPriority
// 用途 : 线程的优先级
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
// 类   : TCThread
// 用途 : 线程类
//------------------------------------------------------------------------
class TCThread
{
private:
    unsigned long   m_nHandle;              // 线程句柄，使用时转为void *

#ifdef __TH_WINDOWS__
    unsigned long    m_nThreadID;            // 线程ID
#endif

#ifdef __TH_UNIX__
    pthread_t       m_nThreadID;
#endif

#ifdef __TH_UNIX__
    sem_t       m_semCreateSuspendedSem;    // 线程创建时挂起的信号量
    bool        m_bInitialSuspendDone;      // 线程初创挂起已被唤醒
#endif

    bool            m_bCreateSuspended;     // 线程创建时的挂起状态
    bool            m_bSuspended;           // 线程挂起状态

    bool            m_bTerminated;          // 是否需要退出
    bool            m_bFreeOnTerminate;    // 线程退出时是否释放

    long            m_nReturnValue;         // 线程返回值
    //bool            m_bFinished;            // 线程是否已结束运行

    void            (*m_funcSyncMethod)(void *);    // 同步调用函数指针
    void            *m_pSyncMethodParam;            // 同步调用参数指针

    TCException *   m_pSynchronizeException;        // 同步例外
    TCException *   m_pFatalException;              // 线程例外

    void AfterConstruction();               // 线程运行后的处理

    void CheckThreadError(long nErrCode);           // 检查线程错误(整型)
    void CheckThreadErrorSuccess(bool bSuccess);    // 检查线程错误(布尔)

protected:
    bool            m_bFinished;            // 线程是否已结束运行

    virtual void    DoTerminate();                  // 线程退出时的工作
    virtual void    Execute() = 0;                  // 线程实际运行函数
    bool            IsTerminated();                 // 线程是否退出

    long            GetReturnValue();               // 得到线程返回值
    void            SetReturnValue(long nValue);    // 设置线程返回值

public:
    TCThread();                                     // 构造函数
    virtual ~TCThread();                            // 析构函数

    void            Start(bool bCreateSuspended);   // 运行线程

    void            Resume();                       // 唤醒线程
    void            Suspend();                      // 挂起线程

    void            Terminate();                    // 消息退出线程
    DWORD           WaitFor();                      // 等待退出线程

    TCException *   GetFatalException();            // 获得线程例外

    bool IsFreeOnTerminate();                          // 获得线程退出时释放
    void SetFreeOnTerminate(bool bFreeOnTerminate);   // 设置线程退出时释放


    DWORD           GetHandle();                    // 获得线程句柄

#ifdef __TH_WINDOWS__
    TEThreadPriority    GetPriority();                          // 获得优先级
    void                SetPriority(TEThreadPriority tpValue);  // 设置优先级
#endif

#ifdef __TH_UNIX__
    int                 GetPriority();              // 获得优先级
    void                SetPriority(int nValue);    // 设置优先级
    int                 GetPolicy();                // 获得策略
    void                SetPolicy(int nValue);      // 设置策略
#endif

    bool                IsSuspended();              // 是否挂起
    void                SetSuspended(bool bValue);  // 设置挂起

    DWORD   GetThreadID();                                  // 获得线程ID
    void    Synchronize(void (*funcNeedSync)(void *),       // 同步方法调用
                    void *pSyncParam);

    void    SleepMilliSeconds(long nMilliSeconds);

friend DWORD ThreadProc(TCThread * pThread);
friend bool CheckSynchronize();
};

//------------------------------------------------------------------------
// 类   : TCThreadException
// 用途 : 线程例外类
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


