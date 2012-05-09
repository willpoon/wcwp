#include "cmpublic.h"

#ifdef __MULTI_THREAD__
static long sa_InitData[] = { -1, 0, 0, 0, 0 };
#else
static long sa_InitData[] = { -1, 0, 0, 0 };
#endif
static TSStringData* pStringDataNil = (TSStringData*)&sa_InitData;
static char * pStringNil = (char * )(((char *)&sa_InitData)+sizeof(TSStringData));

#ifdef __MULTI_THREAD__
    //TCCriticalSection TCString::s_Lock;
#endif

// special function to make EmptyString work even during initialization

const TCString& GetEmptyString()
{
  return *(TCString*)&pStringNil;
}

TCString::TCString()
{
    Init();
}

TCString::TCString(const TCString& stringSrc)
{
	ASSERT(stringSrc.GetData()->nRefs != 0);
	if (stringSrc.GetData()->nRefs >= 0)
	{
		ASSERT(stringSrc.GetData() != pStringDataNil);
        Increment(stringSrc.GetData());
		m_pchData = stringSrc.m_pchData;
//   	InterlockedIncrement(&GetData()->nRefs);
      //GetData()->nRefs += 1 ;
      //Increment(GetData());
	}
	else
	{
        Init();
		*this = stringSrc.m_pchData;
	}
}

//////////////////////////////////////////////////////////////////////////////
// More sophisticated construction

TCString::TCString(char ch)
{
   Init();
   long nLen = 1 ;
   AllocBuffer(nLen) ;
   m_pchData[0] = ch ;
}


TCString::TCString(char c, long nCount)
{
    Init();

    if (nCount != 0)
    {
        AllocBuffer(nCount);
        memset(m_pchData, c, nCount);
    }
}

TCString::TCString(char * lpsz)
{
	Init();
	long nLen = SafeStrlen(lpsz);
	if (nLen != 0)
	{
	    AllocBuffer(nLen);
	    memcpy(m_pchData, lpsz, nLen*sizeof(char));
	}
}

TCString::TCString(char * lpsz,long nLen)
{
    Init();
    if (nLen > 0)
    {
      AllocBuffer(nLen);
      memcpy(m_pchData, lpsz, nLen);
    }
}

void TCString::AllocBuffer(long nLen)
// always allocate one extra character for '\0' termination
// assumes [optimistically] that data length will equal allocation length
{
	ASSERT(nLen >= 0);
	ASSERT(nLen <= LONG_MAX-1);    // max size (enough room for 1 extra)

    long nNewAllocSize;
	if (nLen == 0)
		Init();
	else
	{
            if (nLen > 64)
               nNewAllocSize = nLen + nLen / 4;
            else
               if (nLen > 8)
                 nNewAllocSize = nLen + 16;
               else
                 nNewAllocSize = nLen + 4;

		TSStringData* pData =
			(TSStringData*)new char[sizeof(TSStringData) + (nNewAllocSize+1)*sizeof(char)];
        if( pData == NULL )
        {
           Init();
           throw TCException("Alloc mem error!");
        }
		pData->nRefs = 1;
		pData->data()[nLen] = '\0';
		pData->nDataLength = nLen;
		pData->nAllocLength = nNewAllocSize;
#ifdef __MULTI_THREAD__
        pData->m_pLock = new TCCriticalSection;
        if (pData->m_pLock == NULL)
            throw TCException("can not create string lock.");
#endif
		m_pchData = pData->data();
	}
}

void TCString::Release()
{
 if (GetData() != pStringDataNil)
 {
      ASSERT(GetData()->nRefs != 0);
      //GetData()->nRefs -= 1 ;
//    if (InterlockedDecrement(&GetData()->nRefs) <= 0)
      if (Decrement(GetData()) <= 0)
      //if (GetData()->nRefs <= 0 )
      {
#ifdef __MULTI_THREAD__
          if (GetData()->m_pLock)
          {
            delete GetData()->m_pLock;
            GetData()->m_pLock = NULL;
          }
#endif
          delete[] (char *)GetData();
      }
      Init();
 }
}

void TCString::Release(TSStringData* pData)
{
   if (pData != pStringDataNil)
   {
		ASSERT(pData->nRefs != 0);
//		if (InterlockedDecrement(&pData->nRefs) <= 0)
      //pData->nRefs -= 1 ;
      //if (pData->nRefs <= 0)
      if (Decrement(pData) <= 0)
      {
#ifdef __MULTI_THREAD__
         if (pData->m_pLock)
         {
            delete pData->m_pLock;
            pData->m_pLock = NULL;
         }
#endif
         delete[] (char *)pData;
      }
   }
}

void TCString::Empty()
{
	if (GetData()->nRefs >= 0)
		Release();
	else
		*this = pStringNil;
	ASSERT(GetData()->nDataLength == 0);
	ASSERT(GetData()->nRefs < 0 || GetData()->nAllocLength == 0);
}

void TCString::CopyBeforeWrite()
{
	if (GetData()->nRefs > 1)
	{
		TSStringData* pData = GetData();
        AllocBuffer(pData->nDataLength);
        memcpy(m_pchData, pData->data(), (pData->nDataLength+1)*sizeof(char));
        Release(pData);
 	}
	ASSERT(GetData()->nRefs <= 1);
}

void TCString::AllocBeforeWrite(long nLen)
{
	if (GetData()->nRefs > 1 || nLen > GetData()->nAllocLength)
	{
		Release();
		AllocBuffer(nLen);
	}
	ASSERT(GetData()->nRefs <= 1);
}

TCString::~TCString()
//  free any attached data
{
	if (GetData() != pStringDataNil)
	{
//		if (InterlockedDecrement(&GetData()->nRefs) <= 0)
      //GetData()->nRefs -= 1 ;
      //if (GetData()->nRefs <= 0)
      if (Decrement(GetData()) <= 0)
      {
#ifdef __MULTI_THREAD__
          if (GetData()->m_pLock)
          {
            delete GetData()->m_pLock;
            GetData()->m_pLock = NULL;
          }
#endif
      	delete[] (char*)GetData();
      }
	}
}

//////////////////////////////////////////////////////////////////////////////
// Helpers for the rest of the implementation

void TCString::AllocCopy(TCString& dest, long nCopyLen, long nCopyIndex,
	 long nExtraLen) const
{
	// will clone the data attached to this string
	// allocating 'nExtraLen' characters
	// Places results in uninitialized string 'dest'
	// Will copy the part or all of original data to start of new string

	long nNewLen = nCopyLen + nExtraLen;
	if (nNewLen == 0)
	{
		dest.Init();
	}
	else
	{
		dest.AllocBuffer(nNewLen);
		memcpy(dest.m_pchData, m_pchData+nCopyIndex, nCopyLen*sizeof(char));
	}
}


//////////////////////////////////////////////////////////////////////////////
// Assignment operators
//  All assign a new value to the string
//      (a) first see if the buffer is big enough
//      (b) if enough room, copy on top of old buffer, set size and type
//      (c) otherwise free old string data, and create a new one
//
//  All routines return the new string (but as a 'const TCString&' so that
//      assigning it again will cause a copy, eg: s1 = s2 = "hi there".
//

void TCString::AssignCopy(long nSrcLen, char * lpszSrcData)
{
	AllocBeforeWrite(nSrcLen);
	memcpy(m_pchData, lpszSrcData, nSrcLen*sizeof(char));
	GetData()->nDataLength = nSrcLen;
	m_pchData[nSrcLen] = '\0';
}

const TCString& TCString::operator=(const TCString& stringSrc)
{
	if (m_pchData != stringSrc.m_pchData)
	{
		if ((GetData()->nRefs < 0 && GetData() != pStringDataNil) ||
			stringSrc.GetData()->nRefs < 0)
		{
			// actual copy necessary since one of the strings is locked
			AssignCopy(stringSrc.GetData()->nDataLength, stringSrc.m_pchData);
		}
		else
		{
			// can just copy references around
			Release();
			ASSERT(stringSrc.GetData() != pStringDataNil);
            Increment(stringSrc.GetData());
			m_pchData = stringSrc.m_pchData;
//       InterlockedIncrement(&GetData()->nRefs);
         //GetData()->nRefs += 1 ;
         //Increment(GetData());
		}
	}
	return *this;
}

const TCString& TCString::operator=(char * lpsz)
{
      ASSERT(lpsz != NULL);
      AssignCopy(SafeStrlen(lpsz), lpsz);
      return *this;
}

const TCString& TCString::operator=(char ch)
{
//	ASSERT(!_istlead(ch));     can't set single lead byte
	AssignCopy(1, &ch);
	return *this;
}

TCString  operator+(const TCString& string1, char ch)
{
	TCString s;
	s.ConcatCopy(string1.GetData()->nDataLength, string1.m_pchData, 1, &ch);
	return s;
}

TCString  operator+(char ch, const TCString& string)
{
	TCString s;
	s.ConcatCopy(1, &ch, string.GetData()->nDataLength, string.m_pchData);
	return s;
}

//////////////////////////////////////////////////////////////////////////////
// Very simple sub-string extraction

TCString TCString::Mid(long nFirst) const
{
	return Mid(nFirst, GetData()->nDataLength - nFirst + 1);
}

TCString TCString::Mid(long nFirst, long nCount) const
{
	// out-of-bounds requests return sensible things
        ASSERT( nFirst > 0 );
	if (nFirst <= 0)
		nFirst = 1;
	if (nCount < 0)
		nCount = 0;
        nFirst -= 1;
	if (nFirst + nCount > GetData()->nDataLength)
		nCount = GetData()->nDataLength - nFirst ;
	if (nFirst > GetData()->nDataLength)
		nCount = 0;

	TCString dest;
	AllocCopy(dest, nCount, nFirst, 0);
	return dest;
}

TCString TCString::Right(long nCount) const
{
	if (nCount < 0)
		nCount = 0;
	else if (nCount > GetData()->nDataLength)
		nCount = GetData()->nDataLength;

	TCString dest;
	AllocCopy(dest, nCount, GetData()->nDataLength-nCount, 0);
	return dest;
}

TCString TCString::Left(long nCount) const
{
	if (nCount < 0)
		nCount = 0;
	else if (nCount > GetData()->nDataLength)
		nCount = GetData()->nDataLength;

	TCString dest;
	AllocCopy(dest, nCount, 0, 0);
	return dest;
}


void TCString::ConcatCopy(long nSrc1Len, char * lpszSrc1Data,
     long nSrc2Len, char * lpszSrc2Data)
{
  // -- master concatenation routine
  // Concatenate two sources
  // -- assume that 'this' is a new TCString object

	long nNewLen = nSrc1Len + nSrc2Len;
	if (nNewLen != 0)
	{
		AllocBuffer(nNewLen);
		memcpy(m_pchData, lpszSrc1Data, nSrc1Len*sizeof(char));
		memcpy(m_pchData+nSrc1Len, lpszSrc2Data, nSrc2Len*sizeof(char));
	}
}

TCString operator+(const TCString& string1, const TCString& string2)
{
	TCString s;
   s.ConcatCopy(string1.GetData()->nDataLength, string1.m_pchData,
		string2.GetData()->nDataLength, string2.m_pchData);
	return s;
}

TCString operator+(const TCString& string, char * lpsz)
{
	ASSERT(lpsz != NULL);
	TCString s;
	s.ConcatCopy(string.GetData()->nDataLength, string.m_pchData,
    TCString::SafeStrlen(lpsz), lpsz);
	return s;
}

TCString operator+(char * lpsz, const TCString& string)
{
	ASSERT(lpsz != NULL);
	TCString s;
	s.ConcatCopy(TCString::SafeStrlen(lpsz), lpsz, string.GetData()->nDataLength,
	            	string.m_pchData);
	return s;
}

//////////////////////////////////////////////////////////////////////////////
// concatenate in place

void TCString::ConcatInPlace(long nSrcLen, char * lpszSrcData)
{
	//  -- the main routine for += operators

	// concatenating an empty string is a no-op!
	if (nSrcLen == 0)
		return;

	// if the buffer is too small, or we have a width mis-match, just
	//   allocate a new buffer (slow but sure)
	if (GetData()->nRefs > 1 || (GetData()->nDataLength + nSrcLen) > GetData()->nAllocLength)
	{
		// we have to grow the buffer, use the ConcatCopy routine
		TSStringData* pOldData = GetData();
		ConcatCopy(GetData()->nDataLength, m_pchData, nSrcLen, lpszSrcData);
		ASSERT(pOldData != NULL);
		TCString::Release(pOldData);
	}
	else
	{
		// fast concatenation when buffer big enough
		memcpy(m_pchData+GetData()->nDataLength, lpszSrcData, nSrcLen*sizeof(char));
		GetData()->nDataLength += nSrcLen;
		ASSERT(GetData()->nDataLength <= GetData()->nAllocLength);
		m_pchData[GetData()->nDataLength] = '\0';
	}
}

const TCString& TCString::operator+=(char * lpsz)
{
	ASSERT(lpsz != NULL);
	ConcatInPlace(SafeStrlen(lpsz), lpsz);
	return *this;
}

const TCString& TCString::operator+=(char ch)
{
	ConcatInPlace(1, &ch);
	return *this;
}

const TCString& TCString::operator+=(const TCString& m_string)
{
	ConcatInPlace(m_string.GetData()->nDataLength, m_string.m_pchData);
	return *this;
}

///////////////////////////////////////////////////////////////////////////////
// Advanced direct buffer access

char * TCString::GetBuffer(long nMinBufLength)
{
	ASSERT(nMinBufLength >= 0);

	if (GetData()->nRefs > 1 || nMinBufLength > GetData()->nAllocLength)
	{
		// we have to grow the buffer
		TSStringData* pOldData = GetData();
		int nOldLen = GetData()->nDataLength;   // AllocBuffer will tromp it
		if (nMinBufLength < nOldLen)
			nMinBufLength = nOldLen;
  	    AllocBuffer(nMinBufLength);
	    memcpy(m_pchData, pOldData->data(), (nOldLen+1)*sizeof(char));
		GetData()->nDataLength = nOldLen;
		TCString::Release(pOldData);
	}
	ASSERT(GetData()->nRefs <= 1);

	// return a pointer to the character storage for this string
	ASSERT(m_pchData != NULL);
	return m_pchData;
}

void TCString::ReleaseBuffer(long nNewLength)
{
	CopyBeforeWrite();  // just in case GetBuffer was not called

	if (nNewLength == -1)
		nNewLength = strlen(m_pchData); // zero terminated

	ASSERT(nNewLength <= GetData()->nAllocLength);
	GetData()->nDataLength = nNewLength;
	m_pchData[nNewLength] = '\0';
}

char * TCString::GetBufferSetLength(long nNewLength)
{
	ASSERT(nNewLength >= 0);

	GetBuffer(nNewLength);
	GetData()->nDataLength = nNewLength;
	m_pchData[nNewLength] = '\0';
	return m_pchData;
}

void TCString::FreeExtra()
{
	ASSERT(GetData()->nDataLength <= GetData()->nAllocLength);
	if (GetData()->nDataLength != GetData()->nAllocLength)
	{
		TSStringData* pOldData = GetData();
        AllocBuffer(GetData()->nDataLength);
	    memcpy(m_pchData, pOldData->data(), pOldData->nDataLength*sizeof(char));
	    ASSERT(m_pchData[GetData()->nDataLength] == '\0');
	    TCString::Release(pOldData);
	}
	ASSERT(GetData() != NULL);
}

char * TCString::LockBuffer()
{
	char * lpsz = GetBuffer(0);
	GetData()->nRefs = -1;
	return lpsz;
}

void TCString::UnlockBuffer()
{
	ASSERT(GetData()->nRefs == -1);
	if (GetData() != pStringDataNil)
		GetData()->nRefs = 1;
}

///////////////////////////////////////////////////////////////////////////////
// Commonly used routines (rarely used routines in STREX.CPP)

long TCString::Find(char ch) const
{
	// find first single character
	char * lpsz = strchr(m_pchData, ch);

	// return -1 if not found and index otherwise
	return (lpsz == NULL) ? -1 : (long)(lpsz - m_pchData + 1);
}

long TCString::ReverseFind(char ch) const
{
	// find last single character
	char * lpsz = strrchr(m_pchData, ch);

	// return -1 if not found, distance from beginning otherwise
	return (lpsz == NULL) ? -1 : (long)(lpsz - m_pchData + 1);
}

// find a sub-string (like strstr)
long TCString::Find(char * lpszSub) const
{
	ASSERT(lpszSub != NULL);

	// find first matching substring
	char * lpsz = strstr(m_pchData, lpszSub);

	// return -1 for not found, distance from beginning otherwise
	return (lpsz == NULL) ? -1 : (long)(lpsz - m_pchData + 1);
}


long TCString::FindOneOf(char * lpszCharSet) const
{
	ASSERT(lpszCharSet != NULL);
	char * lpsz = strpbrk(m_pchData, lpszCharSet);
	return (lpsz == NULL) ? -1 : (long)(lpsz - m_pchData+1);
}

void TCString::MakeUpper()
{
    CopyBeforeWrite();
    for (long i = 0;i< GetLength() ; i++)
    {
      if ((*(m_pchData+i) >= 'a')&&*(m_pchData+i) <= 'z')
      {
        *(m_pchData+i) = char(*(m_pchData+i) + 'A' -'a') ; 
      }
    }
}

void TCString::MakeLower()
{
    CopyBeforeWrite();
    for (long i = 0;i< GetLength() ; i++)
    {
      if ((*(m_pchData+i) >= 'A')&&*(m_pchData+i) <= 'Z')
      {
        *(m_pchData+i) = char(*(m_pchData+i) + 'a' -'A') ; 
      }
    }

}

void TCString::MakeReverse()
{
    CopyBeforeWrite();
    char ch ;
    for (long i=0;i<GetLength()/2;i++)
    {
      ch = *(m_pchData+i) ;
      *(m_pchData+i) = *(m_pchData+GetLength()-i-1);
      *(m_pchData+GetLength()-i-1) = ch ;
    }
}

void TCString::SetAt(long nIndex, char ch)
{
	ASSERT(nIndex > 0);
	ASSERT(nIndex <= GetData()->nDataLength);

	CopyBeforeWrite();
	m_pchData[nIndex - 1] = ch;
}

void TCString::TrimRight()
{
	CopyBeforeWrite();

	// find beginning of trailing spaces by starting at beginning (DBCS aware)
	char * lpsz = m_pchData;
	char * lpszLast = NULL;
	while (*lpsz != '\0')
	{
		if ( (*lpsz == 0x20)||(*lpsz == 0x09) )
		{
			if (lpszLast == NULL)
				lpszLast = lpsz;
		}
		else
			lpszLast = NULL;
		lpsz = lpsz + 1 ;
	}

	if (lpszLast != NULL)
	{
		// truncate at trailing space start
		*lpszLast = '\0';
		GetData()->nDataLength = lpszLast - m_pchData;
	}
}

void TCString::TrimLeft()
{
	CopyBeforeWrite();

	// find first non-space character
	char * lpsz = m_pchData;
	while ( (*lpsz == 0x20)||(*lpsz == 0x09) )
		lpsz = lpsz + 1;

	// fix up data and length
	int nDataLength = GetData()->nDataLength - (lpsz - m_pchData);
	memmove(m_pchData, lpsz, (nDataLength+1)*sizeof(char));
	GetData()->nDataLength = nDataLength;
}

long TCString::GetAllocLength() const
{
  return GetData()->nAllocLength;
}

TCString::operator char*() const
{
 return m_pchData;
}

long TCString::SafeStrlen(char * lpsz)
{
  return (lpsz == NULL) ? 0 : strlen(lpsz);
}

TSStringData* TCString::GetData() const
{
	//²âÊÔ´úÂë£¬add by cac 20030710
//	if( m_pchData == NULL ) 
//	{
//           int i ,j;
//           i=0;
//           j= 2/i;
//        } 
	ASSERT(m_pchData != NULL);
 	return ((TSStringData*)m_pchData)-1;
}

void TCString::Init()
{
 m_pchData = EmptyString.m_pchData;
}

char TCString::GetAt(long nIndex) const
{
	ASSERT(nIndex > 0);
	ASSERT(nIndex <= GetData()->nDataLength);
	return m_pchData[nIndex - 1];
}
char TCString::operator[](int nIndex) const
{
	// same as GetAt
	ASSERT(nIndex > 0);
	if (nIndex <= GetData()->nDataLength)
	    return m_pchData[nIndex - 1];
    else
    {
        ASSERT(nIndex == 1);
        return '#';
    }
}

char TCString::operator[](long nIndex) const
{
	// same as GetAt
	ASSERT(nIndex > 0);
	if (nIndex <= GetData()->nDataLength)
	    return m_pchData[nIndex - 1];
    else
    {
        ASSERT(nIndex == 1);
        return '#';
    }
}

void TCString::CutNewLineChar()
{
#ifdef __MULTI_THREAD__
  //TCString::s_Lock.Enter();
  if (GetData()->m_pLock)
    GetData()->m_pLock->Enter();
  try
  {
#endif
  if ( m_pchData[GetData()->nDataLength - 1 ] == '\n' )
  {
     m_pchData[GetData()->nDataLength-1] ='\0' ;
     GetData()->nDataLength -= 1 ;
  }
#ifdef __MULTI_THREAD__
  }
  catch(...)
  {
     //TCString::s_Lock.Leave();
     if (GetData()->m_pLock)
        GetData()->m_pLock->Leave();
     throw;
  }
  //TCString::s_Lock.Leave();
  if (GetData()->m_pLock)
    GetData()->m_pLock->Leave();
#endif
}

long Increment(TSStringData* pData)
{
#ifdef __MULTI_THREAD__
    //TCString::s_Lock.Enter();
    if (pData->m_pLock)
        pData->m_pLock->Enter();
#endif
    long ret = ++ pData->nRefs ;
#ifdef __MULTI_THREAD__
    //TCString::s_Lock.Leave();
    if (pData->m_pLock)
        pData->m_pLock->Leave();
#endif
    return ret;
}

long Decrement(TSStringData* pData)
{
#ifdef __MULTI_THREAD__
    //TCString::s_Lock.Enter();
    if (pData->m_pLock)
        pData->m_pLock->Enter();
#endif
    long ret = -- pData->nRefs ;
#ifdef __MULTI_THREAD__
    //TCString::s_Lock.Leave();
    if (pData->m_pLock)
        pData->m_pLock->Leave();
#endif
    return ret;
}









