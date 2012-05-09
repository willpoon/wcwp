//---------------------------------------------------------------------------
#pragma hdrstop

#include "cmpublic.h"

#ifdef __WIN32__
#include <sys/locking.h>
#endif

#include "c_mzcompress.h"

//---------------------------------------------------------------------------
#pragma package(smart_init)

static long s_init_pbuf[] = {0,0};

TCString TCFileStream::GetOpenModeString(int e_Mode)
{
   char s_mode[5] ;
   int len = 1;
   s_mode[1] = '+' ;
   m_IsWrite = true ;
   if ( (((e_Mode&omAppend)!=0)&&((e_Mode&omRead)!=0))||
        (((e_Mode&omAppend)!=0)&&((e_Mode&omWrite)!=0))||
        (((e_Mode&omText)!=0)&&((e_Mode&omBinary)!=0)) )
          throw TCException(m_FileName + " Error open file mode!") ;
   if ( (((e_Mode&omShared)!=0)&&((e_Mode&omExclusive)!=0))||
        (((e_Mode&omShared)!=0)&&((e_Mode&omExclusive_Waiting)!=0 )) )
          throw TCException(m_FileName + " Error open file share mode!") ;
   if ( ((e_Mode&omText)!=0)&&((e_Mode&omBinary)!=0) )
          throw TCException(m_FileName + " Error open mode : omTEXT|omBinary ");
   if ( (e_Mode&omAppend)!=0 )
   {
          s_mode[0] = 'a' ;
          len = 2;
   }
   else if ( ((e_Mode&omRead)!=0)&&((e_Mode&omWrite)!=0) )
        {
          s_mode[0] = 'r' ;
          len = 2 ;
        }
   else if ( (e_Mode&omWrite)!=0 )
        {
          s_mode[0] = 'w' ;
          len = 2 ;
        }
   else if ( (e_Mode&omRead)!=0 )
        {
          m_IsWrite = false ;
          s_mode[0] = 'r' ;
          if ( (((e_Mode&omShared) == 0)&&(fomDefaultShareMode == omExclusive))
              ||((e_Mode&omShared) == 0)&&(fomDefaultShareMode == omExclusive_Waiting))
              len = 2 ;
          else
              if ( ((e_Mode&omExclusive_Waiting)!=0)||((e_Mode&omExclusive)!=0) )
                 len = 2;
        }
   else throw TCException(m_FileName + " Error open file mode!") ;
   if ( (e_Mode&omText)!=0 )
        s_mode[len] = 't' ;
   else s_mode[len] = 'b' ;
   s_mode[len+1] = '\0' ;
   return TCString(s_mode) ;
}
//================= 解压文件，暂时不需要===============
/*
void TCFileStream::ExpandFile()
{
  if ( FileExists(m_FileName) )
    if( ExtractFileExt(m_FileName) == ".MZ" )
    {
        TCMZCompress::SetPutcHandle(NULL);
        TCMZCompress::ExpandMZFile( m_FileName ) ;  //解压文件
    }
  if( ExtractFileExt(m_FileName) == ".MZ" )
  {
    int nPos =  m_FileName.ReverseFind('.') ;
    if ( nPos > 0 )
       m_FileName =  m_FileName.Mid(1,nPos-1) ;
  }
}
*/
int TCFileStream::Open(const TCString sFileName,int e_Mode)
{
   int ifSuc = -1,i_def ;
   if ( m_fp != NULL )
      TCFileStream::Close() ;
   i_def = fomDefaultShareMode ;
   m_FileName = RegulatePathFile(sFileName) ;
/*   if ( (e_Mode&omCompress)!=0 )    //压缩文件，暂时不需要
        m_IsCompress = true ;
   //====检查是否需要解压文件！=====
   if ( (e_Mode&omExpand)!=0  )
        ExpandFile() ;
*/
  //======== 打开文件==============
   TCString sMode ;
   sMode = GetOpenModeString(e_Mode) ;
   if ( (e_Mode&omShared)!=0 )
       ifSuc = OpenFile((char *)sMode , true) ;
   else if ( (e_Mode&omExclusive)!=0)
       ifSuc = OpenFile((char *)sMode , false) ;
   else if ( (e_Mode&omExclusive_Waiting)!=0  )
          ifSuc = OpenFileLockWaiting((char *)sMode) ;
   else if (i_def == omShared)
           ifSuc = OpenFile((char *)sMode , true) ;
   else if (i_def == omExclusive)
           ifSuc = OpenFile((char *)sMode , false) ;
   else if (i_def == omExclusive_Waiting)
           ifSuc = OpenFileLockWaiting((char *)sMode) ;

   if (ifSuc != 0)
       throw TCException(" Error open file - " + m_FileName) ;

   return ifSuc ;
}

int TCFileStream::OpenFile(char * s_pMode , bool b_Shared)
{
   int i ,ifSuc = -1 ;
   for (i = 0;i < 10;i++)
   {
      m_fp = fopen((char *)m_FileName ,s_pMode ) ;
      if (m_fp != NULL )
      {
         if ( ! b_Shared  )
         {
           ifSuc = LockFile() ;
           if (ifSuc != 0)
               fclose(m_fp) ;
         }
         else ifSuc = 0 ;
         break ;
      }
      if ( !FileExists(m_FileName) )
            break ;
      TCSystem::DelayMicroSeconds(100) ;
   }
   if (ifSuc != 0)
       m_fp = NULL ;
   return ifSuc ;
}

int TCFileStream::OpenFileLockWaiting(char * s_pMode)
{
  int ifSuc = -1 ;
  time_t  OldTime = time(NULL) ;
  while( ifSuc != 0 )
  {
    m_fp = fopen((char *)m_FileName ,s_pMode ) ;
    if (m_fp != NULL )
    {
       ifSuc = LockFile() ;
       if (ifSuc != 0)
          fclose(m_fp) ;
       continue ;
     }
     if ( !FileExists(m_FileName) )
           break ;
     TCSystem::DelayMicroSeconds(10) ;
     if ( time(NULL) > ( OldTime + 20*60 ) )
         break ;
  }

  if ( ifSuc != 0 )
       m_fp = NULL ;
  return ifSuc ;
}


int TCFileStream::LockFile()
{
    int ifSuc = -1 ;

    ASSERT( m_fp != NULL ) ;
    if (m_fp != NULL)
    {
#ifdef __WIN32__
       ifSuc = locking(GetHandle(),LK_LOCK,0) ;
#else
       for (int i = 0; i < 100 ;i++)
       {
          if ( lockf(GetHandle(),F_TEST,0) )
               TCSystem::DelayMicroSeconds(2) ;
          ifSuc = lockf(GetHandle(),F_TLOCK,0) ; 
          if( ifSuc == 0 )
             break ;
       }
#endif
    }
   if (ifSuc == 0)
        m_IsLocked = true ;
   else m_IsLocked = false ;
   return ifSuc ;
}

int TCFileStream::UnLockFile()
{
   ASSERT( m_fp!=NULL ) ;
#ifndef __WIN32__
   int ifSuc = -1 ;
   if( m_fp != NULL )
     for (int i = 0; i<1000; i++)
     {
       ifSuc = lockf(GetHandle(),F_ULOCK,0) ;
       if ( ifSuc == 0 )
            break;
       else
           TCSystem::DelayMicroSeconds(1);
     }
   return ifSuc ;
#else
   return 0 ;
#endif

}

TCFileStream::TCFileStream()
{
  m_FileName = "" ;
  m_fp = NULL ;
  m_pBuffer = &s_init_pbuf ;
//  m_IsCompress = false ;
  m_IsLocked = false ;
}

TCFileStream::~TCFileStream()
{
  TCFileStream::Close() ;
}

bool TCFileStream::HasOpened()
{
  if (m_fp == NULL)
     return false ;
  else
     return true ;
}

TCString TCFileStream::GetFileName()
{
   return m_FileName ;
}

void * TCFileStream::AllocBuffer(long nSize)
{
   ASSERT( m_pBuffer == &s_init_pbuf ) ;
   ASSERT( nSize > 0 ) ;
   if ( nSize > 0 )
   {
      m_pBuffer = new char[ nSize+ sizeof(long) ] ;
      ASSERT( m_pBuffer != NULL ) ;
      if ( m_pBuffer != NULL )
      {
         memcpy(m_pBuffer,&nSize,sizeof(long)) ;
      }
      else
      {
          m_pBuffer = &s_init_pbuf ;
          throw TCException(m_FileName + " Alloc mem error!");
      }
   }
   return ((char *)m_pBuffer + sizeof(long)) ;
}

void TCFileStream::FreeBuffer()
{
   if ( m_pBuffer != &s_init_pbuf )
      delete[] (char *)m_pBuffer ;
   m_pBuffer = &s_init_pbuf ;
}

void * TCFileStream::GetBuffer(long Count)
{
   ASSERT( Count > 0 ) ;
   FreeBuffer() ;
   return AllocBuffer( Count ) ;
}

void * TCFileStream::GetBuffer()
{
  if ( m_pBuffer != &s_init_pbuf )
       return ((char *)m_pBuffer + sizeof(long)) ;
  else return (NULL) ;
}

int TCFileStream::Close(void)
{
  int ifclose = -1 ,ifUnLockSuc = 0 ;

  TCFileStream::FreeBuffer() ;
  if ( m_fp != NULL )
  {
     fflush(m_fp); // Add by kjb 20020120
     if (m_IsLocked)
        ifUnLockSuc = UnLockFile() ;
     ifclose = fclose(m_fp) ;
     if (ifclose != 0)
        throw TCException("TCFileStream::Close() Error : FileName - "
                + m_FileName);
     ASSERT(ifUnLockSuc == 0) ;
     m_fp = NULL ;
/*     if(m_IsCompress)   //==========压缩文件============
     {
       TCMZCompress::SetPutcHandle(NULL) ;
       TCMZCompress::CompressMZFile(m_FileName) ;
     }
*/
  }
  m_fp = NULL ;
  if (ifUnLockSuc != 0)
      throw TCException(m_FileName + " Cannot UnLockFile!!!") ;
  m_IsLocked = false ;
//  m_IsCompress = false ;
  return ifclose ;
}

long TCFileStream::Read(void *pBuffer, long Count)
{
   long nSize = 0 ;
   ASSERT((m_fp != NULL)&&(pBuffer != NULL)) ;
   if ( (m_fp != NULL)&&(pBuffer != NULL) )
   {
      nSize = fread(pBuffer,1,Count,m_fp) ;
   }
  return nSize ;
}

long TCFileStream::Write(const void *pBuffer, long Count)
{
   long nSize = 0 ;
   ASSERT((m_fp != NULL)&&(pBuffer != NULL)&&m_IsWrite) ;
   if ( (m_fp != NULL)&&(pBuffer != NULL)&&m_IsWrite )
   {
       nSize = fwrite( pBuffer,1,Count,m_fp ) ;

       if (nSize != Count)
            throw TCException(m_FileName + " TCFileStream::Write() Error - "
                    "Write Buffer not success.");
   }
   return nSize ;
}

long  TCFileStream::InBufferRead(long Count)
{
   long nSize = 0 ;
   ASSERT( m_fp != NULL ) ;
   if ( m_fp != NULL )
   {
     if ( (Count+2) > *((long*)m_pBuffer) )
     {
        GetBuffer( Count + 2 ) ;
     }
     if ( m_pBuffer != &s_init_pbuf )
         nSize = fread(GetBuffer(),1,Count,m_fp) ;
     if( nSize < 0 ) nSize = 0;
     if ( m_pBuffer != &s_init_pbuf )
        memset((char*)GetBuffer() + nSize,0,1);
   }
   return nSize ;
}

long  TCFileStream::InBufferWrite(long Count)
{
   long nSize = 0 ;
   ASSERT( m_fp != NULL ) ;
   ASSERT( Count <= *((long*)m_pBuffer) ) ;
   if ( (m_fp != NULL)&&(Count <= *((long*)m_pBuffer))&&m_IsWrite )
   {
      nSize = fwrite( (long*)m_pBuffer + 1 ,1,Count,m_fp ) ;
   }
   return nSize ;
}

long TCFileStream::Seek(long Offset, int Origin)
{
   long nSize = -1 ;
   ASSERT(m_fp != NULL) ;
   if ( m_fp != NULL )
   {
     nSize = fseek(m_fp ,Offset ,Origin) ;
   }
   return nSize ;
}

long TCFileStream::FileSize(void)
{
   long nSize = -1 ;
   struct stat statbuf ;
   ASSERT(m_fp != NULL) ;
   if ( m_fp != NULL )
     if ( fstat(GetHandle(),&statbuf) == 0 )
        nSize = statbuf.st_size ;
   return nSize ;
}

//%%%%%%%%%ADD
TCString TCFileStream::FileModifyDateTimeString(void)
{
   TCString sDateTime;
   struct stat statbuf ;
   ASSERT(m_fp != NULL) ;
   if ( m_fp != NULL )
     if ( fstat(GetHandle(),&statbuf) == 0 )
         sDateTime = TCTime::GetDatetimeStringByTimeT(statbuf.st_mtime) ;
   return sDateTime ;
}
//%%%%%%%%%ADD


long TCFileStream::GetHandle()
{
  ASSERT(m_fp != NULL) ;
  return fileno(m_fp) ;
}

long TCFileStream::GetPos()
{
  ASSERT(m_fp != NULL) ;
  return ftell(m_fp) ;
}

char * TCFileStream::GetString(TCString& S)
{
  ASSERT( m_fp != NULL ) ;
  char * lpsz ;
  if ( m_fp != NULL )
  {
     if ( MAX_LINE > *((long*)m_pBuffer) )
     {
        GetBuffer( MAX_LINE + 1 ) ;
     }
    lpsz = (char *)m_pBuffer + sizeof(long) ;
    lpsz = fgets(lpsz, MAX_LINE, m_fp ) ;
    if ( lpsz != NULL )
    {
       S = TCString(lpsz) ;
       S.CutNewLineChar() ;
    }
  }
  return lpsz ;
}

long  TCFileStream::WriteLn(const TCString S)
{
  ASSERT(m_fp != NULL) ;
  TCString sWithNewLine;
  sWithNewLine = S + "\n";
  if (m_fp != NULL)
  {
    return fputs((char *)sWithNewLine,m_fp) ;
  }
  return EOF ;
}

void TCFileStream::Truncate(unsigned long NewSize)
{
  ASSERT(m_fp != NULL) ;
  if (m_fp != NULL)
  {
#ifdef __WIN32__
    chsize(GetHandle(),NewSize) ;
#else
    ftruncate(GetHandle(),NewSize) ;
#endif
  }
}

bool TCFileStream::Eof()
{
  ASSERT(m_fp != NULL) ;
  if (m_fp != NULL)
    if (feof(m_fp) != 0)
       return true ;
  return false ;
}

void TCFileStream::Flush()
{
    ASSERT(m_fp != NULL);

    if (fflush(m_fp) != 0)
        throw TCException(m_FileName + " Error Flush of TCFileStream.");
}

