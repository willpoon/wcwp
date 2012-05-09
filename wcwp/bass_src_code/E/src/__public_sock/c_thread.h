//---------------------------------------------------------------------------

#ifndef c_threadH
#define c_threadH
#ifdef __WIN32__
#include <windows.h>
#else
#ifndef _REENTRANT
#define _REENTRANT   //mutithread
#endif
#include <pthread.h>
#include <synch.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <signal.h>
#include <fcntl.h>
#endif
#include "cmpublic.h"
#include <errno.h>
//---------------------------------------------------------------------------
#ifdef __WIN32__
  struct pthread_mutex_t
  {
    long Lock;
    long Owner;
  };
  void pthread_mutex_init(pthread_mutex_t *mutex,void * pNull);
  void pthread_mutex_destroy(pthread_mutex_t *mutex);
  int pthread_mutex_lock(pthread_mutex_t *mutex);
  int pthread_mutex_unlock(pthread_mutex_t *mutex);
  int pthread_mutex_trylock(pthread_mutex_t *mutex);

  extern pthread_mutex_t gmThreadMut ;
#endif

extern TCList gSyncFunctionList ;
extern long gRunningThreadCount ;
extern bool gTerminateProgramFlag ;

struct TSFunction
{
  void (*Method)(void *) ;
  void * pParam ;
  char bExecuted ;
} ;

extern pthread_mutex_t gmThreadMut ;

class TCThread
{
  public :
    TCThread() ;
    ~TCThread() ;
    bool m_FreeOnTerminate ;
#ifdef __WIN32__
    unsigned long GetThreadID() ;
#else
    unsigned int GetThreadID() ;
#endif
    virtual void Terminate() ;
    virtual void Execute() ;
  protected :
    bool Terminated ;
#ifdef __WIN32__
    unsigned long m_ThreadID ;
    HANDLE m_Handle ;
#else
    unsigned int m_ThreadID ;
#endif
    void asynchronism( void (*Method)(void *),void * pParam ) ;
    void Synchronize( void (*Method)(void *),void * pParam );
    void Create() ;
};

class TCThreadException : public TCException
{
public:
    TCThreadException(TCString sMsg);
};

#ifdef __WIN32__
DWORD WINAPI ThreadProc(void * p) ;
#else
void * ThreadProc(void * p) ;
#endif

void Method_Mutex( void(*Method)(void*),void *pParam ,pthread_mutex_t *pMutex = &gmThreadMut) ;

void PushSyncEvent(void *P) ;
void MainSyncEvent() ;

void WaitForThreadTeminated();

#endif








