//---------------------------------------------------------------------------

#ifndef c_critical_sectionH
#define c_critical_sectionH
//---------------------------------------------------------------------------

#include "cmpublic.h"

#ifndef __WIN32__
#ifndef _REENTRANT
#define _REENTRANT   //mutithread
#endif

#ifndef __alpha
#include <synch.h>
#endif

#include <signal.h>
#endif

#ifdef __WIN32__
  struct pthread_mutex_t
  {
    long Lock;
    long Owner;

    long m_nLockTimes;      // ADD 20010827.HJJ
  };
#endif

//---------------------------------------------------------------------------
// 类   : TCCriticalSection
// 用途 : 临界区类，在多线程环境中只允许一个线程运行临界区内的语句，阻塞其它
//        线程的执行
//---------------------------------------------------------------------------
class TCCriticalSection
{
private:
    pthread_mutex_t m_mutex;
public:
    TCCriticalSection();
    ~TCCriticalSection();
    void Acquire();
    void Release();
    void Enter();
    void Leave();
    bool TryLock();
};

#endif

