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
// ��   : TCCriticalSection
// ��; : �ٽ����࣬�ڶ��̻߳�����ֻ����һ���߳������ٽ����ڵ���䣬��������
//        �̵߳�ִ��
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

