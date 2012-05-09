//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_thread.h"

//---------------------------------------------------------------------------
#pragma package(smart_init)

TCList gSyncFunctionList ;
long gRunningThreadCount = 0;
bool gTerminateProgramFlag = false ;

#ifdef __WIN32__
pthread_mutex_t PTHREAD_MUTEX_INITIALIZER = {0,0};
#endif

pthread_mutex_t gmThreadMut = PTHREAD_MUTEX_INITIALIZER ;

#ifdef __WIN32__
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
         return 0;
   }
   else
     if(mutex->Owner == 0)
     {
        InterlockedIncrement(&mutex->Lock);
        if( mutex->Lock == 1 )
        {
          InterlockedExchange(&mutex->Owner,GetCurrentThreadId());
          return 0;
        }
        else InterlockedDecrement(&mutex->Lock);
     }
   Sleep(1);
  }
  return -1;
}

int pthread_mutex_unlock(pthread_mutex_t *mutex)
{
   if( mutex->Lock == 1 )
   {
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
         return 0;
   }
   else
     if(mutex->Owner == 0)
     {
        InterlockedIncrement(&mutex->Lock);
        if( mutex->Lock == 1 )
        {
          InterlockedExchange(&mutex->Owner,GetCurrentThreadId());
          return 0;
        }
        else InterlockedDecrement(&mutex->Lock);
     }
   Sleep(1);
  }
  return -1;
}
#endif


#ifdef __WIN32__
DWORD WINAPI ThreadProc(void * p)
#else
void * ThreadProc(void * p)
#endif
{
   TCThread * pThread ;
   pThread = (TCThread *)p ;
   pThread->Execute() ;
   if (pThread->m_FreeOnTerminate)
      delete pThread ;
#ifdef __WIN32__
   Sleep(1);
#else
   TCSystem::DelayMicroSeconds(1) ;
#endif
   pthread_mutex_lock(&gmThreadMut);
   gRunningThreadCount -- ;
   pthread_mutex_unlock(&gmThreadMut);
#ifdef __WIN32__
   ExitThread(0);
#else
   pthread_exit(0);
#endif
   return NULL;
}

TCThread::TCThread()
{
  Terminated = false ;
  m_FreeOnTerminate = true ;
  m_ThreadID = 0 ;
#ifdef __WIN32__
  m_Handle = 0 ;
#endif
}

TCThread::~TCThread()
{
  Terminate() ;
  m_FreeOnTerminate = false ;
#ifdef __WIN32__
  if (m_Handle != 0)
  {
     CloseHandle(m_Handle);
     m_Handle = 0 ;
  }
#else
  if (m_ThreadID != 0)
     pthread_detach(m_ThreadID) ;
#endif
}

#ifdef __WIN32__
unsigned long TCThread::GetThreadID()
#else
unsigned int TCThread::GetThreadID()
#endif
{
  return m_ThreadID ;
}


void TCThread::Create()
{
   pthread_mutex_lock(&gmThreadMut);
   gRunningThreadCount ++ ;
   pthread_mutex_unlock(&gmThreadMut);
#ifdef __WIN32__
   m_Handle = CreateThread(NULL,0, ThreadProc, this,0,&m_ThreadID) ;
   if (m_Handle == NULL)
   {
      pthread_mutex_lock(&gmThreadMut);
      gRunningThreadCount -- ;
      pthread_mutex_unlock(&gmThreadMut);
      throw TCException("Cannot create thread,Running thread count " +
                         IntToStr(gRunningThreadCount) + "!!!") ;
   }
#else
   if( pthread_create((pthread_t *)&m_ThreadID,NULL,ThreadProc,this) != 0 )
   {
      pthread_mutex_lock(&gmThreadMut);
      gRunningThreadCount -- ;
      pthread_mutex_unlock(&gmThreadMut);
      throw TCException("Cannot create thread,Running thread count " +
                         IntToStr(gRunningThreadCount) + "!!!") ;
   }
#endif

}

void TCThread::Terminate()
{
  Terminated = true ;
}

void TCThread::Execute()
{
}

void TCThread::asynchronism( void (*Method)(void *),void * pParam )
{
   TSFunction * pSFunc ;
   pSFunc = new TSFunction ;
   pSFunc->Method = Method ;
   pSFunc->pParam = pParam ;
   pSFunc->bExecuted = 1 ;
   PushSyncEvent(pSFunc) ;
}

void TCThread::Synchronize( void(*Method)(void *),void * pParam )
{
   TSFunction * pSFunc ;
   pSFunc = new TSFunction ;
   pSFunc->Method = Method ;
   pSFunc->pParam = pParam ;
   pSFunc->bExecuted = 0;
   PushSyncEvent(pSFunc) ;
   while(!pSFunc->bExecuted)
   {
#ifdef __WIN32__
     Sleep(1);
#else
     TCSystem::DelayMicroSeconds(1);
#endif
     if (Terminated)
         break ;
   }
   delete[] pSFunc ;
}

void PushSyncEvent(void *P)
{
  pthread_mutex_lock(&gmThreadMut);
  gSyncFunctionList.Add(P) ;
  pthread_mutex_unlock(&gmThreadMut);
}

TCThreadException::TCThreadException(TCString sMsg) : TCException(sMsg)
{
}


void Method_Mutex( void (*Method)(void *),void *pParam ,pthread_mutex_t *pMutex )
{
  if (pMutex != NULL)
  {
    pthread_mutex_lock(pMutex);
    Method(pParam);
    pthread_mutex_unlock(pMutex) ;
  }
}


void MainSyncEvent()
{
  TCList sRunList;
  TSFunction * pfRun;
  pthread_mutex_lock(&gmThreadMut);
  sRunList = gSyncFunctionList;
  gSyncFunctionList.Clear() ;
  pthread_mutex_unlock(&gmThreadMut) ;

  int i = 0 ;

  for( i=0;i<sRunList.GetCount(); i++ )
  {
    pfRun = (TSFunction *)(sRunList[i]);
    pfRun->Method(pfRun->pParam) ;
    if( pfRun->bExecuted  )
         delete[] pfRun ;
    else
         pfRun->bExecuted = 1 ;
  }
}

void WaitForThreadTeminated()
{
  long nCount;
  gTerminateProgramFlag = true ;
#ifdef __WIN32__
  Sleep(10);
#else
  TCSystem::DelayMicroSeconds(10) ;
#endif
  do
  {
    pthread_mutex_lock(&gmThreadMut);
    nCount = gRunningThreadCount;
    pthread_mutex_unlock(&gmThreadMut);
    MainSyncEvent();
#ifdef __WIN32__
    Sleep(1);
#else
    TCSystem::DelayMicroSeconds(1) ;
#endif
  }while(nCount>0);
#ifdef __WIN32__
  Sleep(1000);
#else
  TCSystem::DelayMicroSeconds(1000) ;
#endif
  MainSyncEvent();
}


