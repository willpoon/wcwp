
//--------------------------------------------------------------------------
// 字符串类声明
//
//
// 创建 ： 林华城                 2000年6月6日
//--------------------------------------------------------------------------

#ifndef _c_stringH
#define _c_stringH

#include "cmpublic.h"

#ifdef __MULTI_THREAD__
#include "c_critical_section.h"
#endif

#define EmptyString GetEmptyString()

struct TSStringData
{
	long nRefs;            // reference count
	long nDataLength;
	long nAllocLength;
#ifdef __MULTI_THREAD__
    TCCriticalSection *m_pLock;
#endif
	// char data[nAllocLength]
	char * data()
		{ return (char *)(this+1); }

};

long Increment(TSStringData *pData);
long Decrement(TSStringData *pData);
class TCString
{
#ifdef __MULTI_THREAD__
public:
    //static TCCriticalSection   s_Lock;
#endif
public:
// Constructors
	TCString();
	TCString(const TCString& stringSrc);
	TCString(char * lpsz);
    TCString(char * lpsz,long nLen);
    TCString(char ch) ;
    TCString(char c, long nCount) ;
//	TCString(const unsigned char* psz);

// Attributes & Operations
// as an array of characters
	long GetLength() const
    {
        return GetData()->nDataLength;
    }
    inline bool IsEmpty()
    {
        return GetData()->nDataLength == 0;
    }
	void Empty();                       // free up the data

	char GetAt(long nIndex) const;      // 1 based
	char operator[](int nIndex) const; // same as GetAt
    char operator[](long nIndex) const;
	void SetAt(long nIndex, char ch);
	operator char*() const;           // as a C string

	// overloaded assignment
	const TCString& operator=(const TCString& stringSrc);
	const TCString& operator=(char ch);
	const TCString& operator=(char* lpsz);
//	const TCString& operator=(const unsigned char* psz);
	// string concatenation
	const TCString& operator+=(const TCString& m_string);
	const TCString& operator+=(char ch);
	const TCString& operator+=(char * lpsz);

	friend TCString operator+(const TCString& string1,
			                    const TCString& string2);
	friend TCString operator+(const TCString& string, char ch);
	friend TCString operator+(char ch, const TCString& string);
	friend TCString operator+(const TCString& string, char * lpsz);
	friend TCString operator+(char * lpsz, const TCString& string);

	// string comparison
	inline long Compare(char * lpsz) const         // straight character
    {
        return strcmp(m_pchData, lpsz);
    }
	inline long CompareNoCase(char * lpsz) const   // ignore case
    {
#ifdef __WIN32__
        return stricmp(m_pchData, lpsz);
#else
        return strcasecmp(m_pchData, lpsz);
#endif
    }

	// simple sub-string extraction
	TCString Mid(long nFirst, long nCount) const;
	TCString Mid(long nFirst) const;
	TCString Left(long nCount) const;
	TCString Right(long nCount) const;


	// upper/lower/reverse conversion
	void MakeUpper();
	void MakeLower();
    void MakeReverse();

	// trimming whitespace (either side)
	void TrimRight();
	void TrimLeft();

	// searching (return starting index, or -1 if not found)
	// look for a single character match
	long Find(char ch) const;        // like "C" strchr , 1 base
	long ReverseFind(char ch) const;
	long FindOneOf(char * lpszCharSet) const;


	// look for a specific sub-string
	long Find(char * lpszSub) const;        // like "C" strstr


	// Access to string implementation buffer as "C" character array
	char * GetBuffer(long nMinBufLength);
	void ReleaseBuffer(long nNewLength = -1);
	char * GetBufferSetLength(long nNewLength);
	void FreeExtra();

	// Use LockBuffer/UnlockBuffer to turn refcounting off
	char * LockBuffer();
	void UnlockBuffer();

// Implementation
public:
	~TCString();
	long GetAllocLength() const;
    void CutNewLineChar() ;

    inline long ToInt(void) const
    {
      return atol(m_pchData) ;
    }

    inline double ToFloat(void) const
    {
      return atof(m_pchData) ;
    }
    inline long SetIntToStr(long i)
    {
      char aResult[20] ;
      long nlen ;
      nlen = sprintf(aResult,"%d",i) ;
      if ( nlen > 0 )
         AssignCopy(nlen, aResult);
      return nlen ;
    }
    inline void aCharToString(char *srcChar ,long nlen)
    {
      AssignCopy(nlen, srcChar) ;
    }
protected:
	char * m_pchData;   // pointer to ref counted string data

	// implementation helpers
	TSStringData* GetData() const;
	void Init();
	void AllocCopy(TCString& dest, long nCopyLen, long nCopyIndex, long nExtraLen) const;
	void AllocBuffer(long nLen);
	void AssignCopy(long nSrcLen, char * lpszSrcData);
	void ConcatCopy(long nSrc1Len, char * lpszSrc1Data, long nSrc2Len, char * lpszSrc2Data);
	void ConcatInPlace(long nSrcLen, char * lpszSrcData);
	void CopyBeforeWrite();
	void AllocBeforeWrite(long nLen);
	void Release();
	static void Release(TSStringData* pData) ;
	static long SafeStrlen(char * lpsz);

};

const TCString& GetEmptyString();

inline bool operator==(const TCString& s1, const TCString& s2)
{
 return s1.Compare(s2) == 0;
}

inline bool operator==(const TCString& s1, char * s2)
{
 return s1.Compare(s2) == 0 ;
}

inline bool operator==(char * s1, const TCString& s2)
{
 return s2.Compare(s1) == 0;
}

inline bool operator!=(const TCString& s1, const TCString& s2)
{
 return s1.Compare(s2) != 0;
}

inline bool operator!=(const TCString& s1, char * s2)
{
 return s1.Compare(s2) != 0;
}

inline bool operator!=(char * s1, const TCString& s2)
{
 return s2.Compare(s1) != 0;
}

inline bool operator<(const TCString& s1, const TCString& s2)
{
 return s1.Compare(s2) < 0;
}

inline bool operator<(const TCString& s1, char * s2)
{
 return s1.Compare(s2) < 0;
}

inline bool operator<(char * s1, const TCString& s2)
{
 return s2.Compare(s1) > 0;
}

inline bool operator>(const TCString& s1, const TCString& s2)
{
 return s1.Compare(s2) > 0;
}

inline bool operator>(const TCString& s1, char * s2)
{
 return s1.Compare(s2) > 0;
}

inline bool operator>(char * s1, const TCString& s2)
{
 return s2.Compare(s1) < 0;
}

inline bool operator<=(const TCString& s1, const TCString& s2)
{
 return s1.Compare(s2) <= 0;
}

inline bool operator<=(const TCString& s1, char * s2)
{
 return s1.Compare(s2) <= 0;
}

inline bool operator<=(char * s1, const TCString& s2)
{
 return s2.Compare(s1) >= 0;
}

inline bool operator>=(const TCString& s1, const TCString& s2)
{
 return s1.Compare(s2) >= 0;
}

inline bool operator>=(const TCString& s1, char * s2)
{
 return s1.Compare(s2) >= 0;
}

inline bool operator>=(char * s1, const TCString& s2)
{
 return s2.Compare(s1) <= 0;
}

#endif
