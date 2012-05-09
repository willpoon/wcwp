//---------------------------------------------------------------------------
#ifndef c_file_streamH
#define c_file_streamH

#include "cmpublic.h"

//---------------------------------------------------------------------------
const long BUFFER_BLOCK = 8192 ;

enum e_FileOpenMode
{
  omRead = 1,
  omWrite = 2,
  omAppend = 4 ,
  omText = 8,
  omBinary = 16 ,
  omShared = 32,
  omExclusive = 64 ,
  omExclusive_Waiting = 128
//  omCompress = 256,
//  omExpand = 512
};

const e_FileOpenMode fomDefaultShareMode = omExclusive_Waiting ;

class TCFileStream
{
   private:
     TCString m_FileName ;
     void * m_pBuffer ;
     FILE * m_fp ;
     bool m_IsWrite ;
     bool m_IsLocked ;
     void * AllocBuffer(long nSize) ;

     void FreeBuffer(void) ;
     TCString GetOpenModeString(int e_Mode) ;
     int LockFile() ;
     int UnLockFile() ;
     int OpenFile(char * s_pMode,bool b_Shared) ;
     int OpenFileLockWaiting(char * s_pMode) ;
     void ExpandFile() ;

   public :
     TCFileStream() ;
     ~TCFileStream() ;
     bool HasOpened() ;
     TCString GetFileName() ;
     virtual int Close(void) ;
     virtual int Open(const TCString FileName,int e_Mode) ;
     virtual void * GetBuffer(long Count) ;
     virtual void * GetBuffer() ;
     virtual long Read(void *pBuffer, long Count) ;
     virtual long Write(const void *pBuffer, long Count) ;
     long  InBufferRead(long Count) ;
     long  InBufferWrite(long Count) ;
     virtual long Seek(long Offset, int Origin = SEEK_SET);

     virtual long FileSize(void) ;
     virtual long GetPos(void) ;
     virtual long GetHandle(void) ;
     virtual TCString FileModifyDateTimeString(void); //%%%ADD

     virtual char * GetString(TCString& S) ;
     virtual long  WriteLn(const TCString S) ;

     void Truncate(unsigned long NewSize);
     bool Eof();

     void Flush();
};

#endif
