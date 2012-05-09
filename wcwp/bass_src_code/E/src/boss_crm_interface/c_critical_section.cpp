//---------------------------------------------------------------------------

#pragma hdrstop

#ifdef __WIN32__
#include <windows.h>
#else
#include <pthread.h>
#endif

#include "cmpublic.h"
#include <errno.h>

#include "c_critical_section.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)

#ifdef __WIN32__

//======== 定义了以下五个UNIX函数，以兼容UNIX操作 ========
void pthread_mutex_init(pthread_mutex_t *mutex,void * pNull);
void pthread_mutex_destroy(pthread_mutex_t *mutex);
int pthread_mutex_lock(pthread_mutex_t *mutex);
int pthread_mutex_unlock(pthread_mutex_t *mutex);
int pthread_mutex_trylock(pthread_mutex_t *mutex);

#pragma argsused
void pthread_mutex_init(pthread_mutex_t *mutex,void * pNull)
{
  setmem(mutex, sizeof(pthread_mutex_t), 0);
}

void pthread_mutex_destroy(pthread_mutex_t *mutex)
{
  pthread_mutex_lock(mutex);
  pthread_mutex_unlock(mutex);
}

int pthread_mutex_lock(pthread_mutex_t *mutex)
{
  time_t start_time;
  start_time = time(NULL);
  while(time(NULL)<start_time + 1800)
  {
   if(mutex->Lock )
   {
     if( mutex->Owner == (long)GetCurrentThreadId() )
     {
        mutex->m_nLockTimes ++;         // ADD 20010827.HJJ
        return 0;
     }
   }
   else
     if(mutex->Owner == 0)
     {
        InterlockedIncrement(&mutex->Lock);
        if( mutex->Lock == 1 )
        {
          InterlockedExchange(&mutex->Owner,GetCurrentThreadId());
          mutex->m_nLockTimes ++;       // ADD 20010827.HJJ
          return 0;
        }
        else InterlockedDecrement(&mutex->Lock);
     }
   TCSystem::DelayMicroSeconds(100);
  }
  return -1;
}

int pthread_mutex_unlock(pthread_mutex_t *mutex)
{
   if( mutex->Lock == 1 )
   {
     mutex->m_nLockTimes --;            // ADD 20010827.HJJ
     if (mutex->m_nLockTimes < 0)       // ADD 20010827.HJJ
        return -1;                      // ADD 20010827.HJJ
     if (mutex->m_nLockTimes >= 1)      // ADD 20010827.HJJ
        return 0;                       // ADD 20010827.HJJ

     if( mutex->Owner != (long)GetCurrentThreadId() )
         return -1;
     InterlockedExchange(&mutex->Owner,0);
     InterlockedExchange(&mutex->Lock,0);
     return 0;
   }
   return -1;
}

int pthread_mutex_trylock(pthread_mutex_t *mutex)
{
    long i;
    for(i = 0; i<5; i++)
    {
        if(mutex->Lock )
        {
            if( mutex->Owner == (long)GetCurrentThreadId() )
            {
                mutex->m_nLockTimes ++;
                return 0;
            }
        }
        else
            if(mutex->Owner == 0)
            {
                InterlockedIncrement(&mutex->Lock);
                if( mutex->Lock == 1 )
                {
                    InterlockedExchange(&mutex->Owner,GetCurrentThreadId());
                    mutex->m_nLockTimes ++; 
                    return 0;
                }
                else InterlockedDecrement(&mutex->Lock);
            }
        TCSystem::DelayMicroSeconds(100);
    }
    return -1;
}
#endif

//==========================================================================
// 函数 : TCCriticalSection::TCCriticalSection
// 用途 : 构造函数
// 原型 : TCCriticalSection();
// 参数 : 无
// 返回 : 无
// 说明 : 初始化mutex
//==========================================================================
TCCriticalSection::TCCriticalSection()
{
    pthread_mutex_init(&m_mutex, NULL);
}

//==========================================================================
// 函数 : TCCriticalSection::TCCriticalSection
// 用途 : 构造函数
// 原型 : TCCriticalSection();
// 参数 : 无
// 返回 : 无
// 说明 : 删除释放mutex
//==========================================================================
TCCriticalSection::~TCCriticalSection()
{
    pthread_mutex_destroy(&m_mutex);
}

//==========================================================================
// 函数 : TCCriticalSection::Acquire
// 用途 : 进入线程敏感区，并阻塞其它线程进入
// 原型 : void Acquire();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCCriticalSection::Acquire()
{
    int nRet;

    nRet = pthread_mutex_lock(&m_mutex);

    if (nRet == -1)
        throw TCException("TCCritialSection::Acquire() : function "
                "pthread_mutex_lock() not succeed.");
}

//==========================================================================
// 函数 : TCCriticalSection::Enter
// 用途 : 进入线程敏感区，并阻塞其它线程进入
// 原型 : void Enter();
// 参数 : 无
// 返回 : 无
// 说明 : 与Acquire完成相同的功能，一般情况下，Enter与Leave合用，Acquire与
//        Release合用
//==========================================================================
void TCCriticalSection::Enter()
{
    Acquire();
}

//==========================================================================
// 函数 : TCCriticalSection::Leave
// 用途 : 允许其它线程使用临界区
// 原型 : void Leave();
// 参数 : 无
// 返回 : 无
// 说明 : 
//==========================================================================
void TCCriticalSection::Leave()
{
    Release();
}

//==========================================================================
// 函数 : TCCriticalSection::Release
// 用途 : 允许其它线程使用临界区
// 原型 : void Release();
// 参数 : 无
// 返回 : 无
// 说明 : 
//==========================================================================
void TCCriticalSection::Release()
{
    int nRet;

    nRet = pthread_mutex_unlock(&m_mutex);

    if (nRet == -1)
        throw TCException("TCCritialSection::Release() : function "
                "pthread_mutex_unlock() not succeed.");
}

bool TCCriticalSection::TryLock()
{
    int nRet;

    nRet = pthread_mutex_trylock(&m_mutex);
    if (nRet == 0)
        return true;
    else
    {
#ifndef __WIN32__
        TCSystem::DelayMicroSeconds(100);
#endif
        return false;
    }
}






