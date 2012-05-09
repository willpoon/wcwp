//---------------------------------------------------------------------------
#pragma hdrstop

#include "cmpublic.h"



//---------------------------------------------------------------------------
#pragma package(smart_init)

static TCString CStringNil = GetEmptyString() ;

TCStringList::TCStringList()
{
   init();
}

TCStringList::TCStringList(TCStringList & sSrcList)
{
  init();
  long nCount = sSrcList.GetCount() ;
  for(long i = 0;i<nCount ; i++)
  {
    Add(sSrcList[i]) ;
  }
}

TCStringList::~TCStringList()
{
   Release() ;
}

void TCStringList::init()
{
   m_pStrings = &CStringNil ;
   m_StringCount = 0 ;
   m_AllocLength = 0 ;
}

//////////////////////////////////////////////////////////////////////////////

void TCStringList::Release()
{
   if ( m_pStrings != &CStringNil )
      delete[] (TCString *)m_pStrings ;
   init() ;
}

void TCStringList::Release(TCString * pListData)
{
   if (pListData != &CStringNil)
      delete[] pListData ;
}

void TCStringList::AllocBuffer(long lCount)
{
    ASSERT(lCount >= 0);
    ASSERT(lCount <= LONG_MAX-1);
    if (lCount == 0 )
    {
      init() ;
    }
    else
    {
      if( (m_pStrings = new TCString[lCount]) == NULL )
      {
        init();
        throw TCException("Alloc mem error!");
      }
      m_AllocLength = lCount ;
    }
}

void TCStringList::AllocNewBuffer(long NewLen)
{
    long OldAllocLength ;
    long GetOldCount ;
    TCString * OldList ;
    OldAllocLength = m_AllocLength ;
    GetOldCount = OldAllocLength ;
    OldList =  m_pStrings ;
    AllocBuffer(NewLen) ;
    if (OldAllocLength > m_AllocLength)
    GetOldCount = m_AllocLength ;
    for( long i = 0; i < GetOldCount ; i++)
         m_pStrings[i] = OldList[i] ;
    Release(OldList) ;
}

long TCStringList::ExtendLength()
{
  if ( m_AllocLength <= 0 )
       return InitAllocLength ;
  if ( m_AllocLength > 10000 )
       return (m_AllocLength + 5000) ;
  else return (m_AllocLength + m_AllocLength/2) ;
//  return (m_AllocLength + m_AllocLength/2) ;
}

void TCStringList::Add(const TCString & c_str)
{
   if ( m_pStrings == &CStringNil || m_AllocLength == 0 )
   {
      AllocBuffer(InitAllocLength) ;
      m_pStrings[0] = c_str ;
      m_StringCount = 1 ;
   }
   else
   {
      if  (m_StringCount >= m_AllocLength)
           AllocNewBuffer(ExtendLength()) ;
      m_pStrings[m_StringCount] = c_str ;
      m_StringCount += 1 ;
   }
}

void TCStringList::Clear(void)
{
   Release() ;
}

void TCStringList::Delete(long Index)
{
    ASSERT(Index >= 0) ;
    ASSERT(Index < m_StringCount) ;
    if ((Index < m_StringCount)&&(m_pStrings != &CStringNil))
    {
       m_pStrings[Index].Empty() ;
       if ( Index != (m_StringCount-1) )
       {
         memmove(m_pStrings + Index,m_pStrings + Index + 1,
                 (m_StringCount - Index - 1)*sizeof(TCString)) ;
         memmove(m_pStrings + m_StringCount -1,&CStringNil,sizeof(TCString)) ;
       }
       m_StringCount -= 1 ;
    }
}

void TCStringList::Insert(long Index,const TCString & S)
{
    ASSERT(Index >= 0) ;
    ASSERT(Index < m_StringCount) ;
    if ( m_StringCount >= m_AllocLength )
        AllocNewBuffer(ExtendLength()) ;
    if (Index < m_StringCount)
    {
      memmove(m_pStrings + Index + 1,m_pStrings + Index,
                (m_StringCount - Index)*sizeof(TCString)) ;
      memmove(m_pStrings + Index,&CStringNil,sizeof(TCString)) ;
      m_pStrings[Index] = S ;
      m_StringCount += 1 ;
    }
}

TCString TCStringList::GetText(void) const
{
   TCString sText ;
   long len = 0 ;
   for(long i = 0 ;i<m_StringCount; i++)
   {
     len += m_pStrings[i].GetLength() + 2 ;
   }
   if ( sText.GetBuffer(len + 1) != NULL )
   {
    for(long i = 0 ;i<m_StringCount; i++)
    {
#ifdef __WIN32__
       sText += m_pStrings[i] + "\r\n" ;
#else
       sText += m_pStrings[i] + "\n" ;  //for unix
#endif
    }
   }
   else
     throw TCException("AllocBuffer error in GetText()...") ;
   return sText ;
}

void TCStringList::SetText(const TCString srcText)
{
   char * lpsz ;
   long i , nPos = 0 ;
   TCStringList::Clear() ;
   lpsz = (char *)srcText ;
   for( i = 0; i < srcText.GetLength() ; i++)
   {
     if ( *(lpsz+i) == '\n' )
     {
       if ( (i>0)&&(*(lpsz+i-1) == '\r') )
         TCStringList::Add(srcText.Mid(nPos + 1,i-nPos-1));
       else
         TCStringList::Add(srcText.Mid(nPos + 1,i-nPos));
       nPos = i + 1 ;
     }
   }
  if (nPos < srcText.GetLength())
     TCStringList::Add(srcText.Mid(nPos+1)); 
}

void TCStringList::CommaText(const TCString srcText,char cCommaChar,
        bool bIgnoreLastEmptyString)
{
   char * lpsz ;
   long i , nPos = 0 ;
   TCStringList::Clear() ;
   lpsz = (char *)srcText ;
   for( i = 0; i < srcText.GetLength() ; i++)
   {
     if ( *(lpsz+i) == cCommaChar )
     {
       TCStringList::Add(srcText.Mid(nPos+1,i-nPos));
       nPos = i + 1 ;
     }
   }

   if (bIgnoreLastEmptyString)
   {
        if (nPos < srcText.GetLength())
            TCStringList::Add(srcText.Mid(nPos+1));
   }
   else
        TCStringList::Add(srcText.Mid(nPos+1));

}

int TCStringList::LoadFromFile(TCString srcFileName)
{
   TCString S ;
   TCFileStream srcFile ;
   TCStringList::Clear() ;
   if ( srcFile.Open(srcFileName,omRead|omExclusive_Waiting|omText) != 0 )
        return -1 ;
   while (srcFile.GetString(S) != NULL)
   {
     TCStringList::Add(S);
   }
   return 0 ;
}

int TCStringList::SaveToFile(TCString destFileName,enum e_ListSaveMode e_savemode )
{
   TCString S ;
   TCFileStream destFile ;
   switch (e_savemode )
   {
     case omCreateNew :
         if ( destFile.Open(destFileName,omWrite) != 0 )
            return -1 ;
         break ;
     case omAppendMode :
         if ( destFile.Open(destFileName,omAppend) != 0 )
            return -1 ;
         break ;
     default :
         return -1 ;
   }
   S = TCStringList::GetText() ;     
   return destFile.Write((char*)S , S.GetLength()) ;
}

TCString& TCStringList::operator[](long nIndex) const
{
   ASSERT(nIndex >= 0);
   ASSERT( nIndex < m_StringCount);
   if (nIndex < m_StringCount)
       return m_pStrings[nIndex] ;
   else return CStringNil ;
}

TCStringList& TCStringList::operator=(TCStringList & sSrcList)
{
  Clear();
  
  long nCount = sSrcList.GetCount() ;
  for(long i = 0;i<nCount ; i++)
  {
    Add(sSrcList[i]) ;
  }
  return * this ;
}

void TCStringList::Sort()
{
  if( m_StringCount > 1 )
     QuickSort( 0,m_StringCount-1) ;
}

void TCStringList::QuickSort(long nL ,long nR)
{
 long i,j,p ;
 TCString S ;
 do
 {
   i = nL ;
   j = nR ;
   p = (nL+nR)>>1 ;
   do
   {
      while ( m_pStrings[i] < m_pStrings[p] ) i++;
      while ( m_pStrings[j] > m_pStrings[p] ) j--;
      if ( i <= j )
      {
        S = m_pStrings[i] ;
        m_pStrings[i] = m_pStrings[j] ;
        m_pStrings[j] = S ;
        if ( p == i )
           p = j ;
        else if (p == j)
                p = i;
        i++ ;
        j-- ;
      }
   }while(i<=j) ;
   if (nL < j )
      QuickSort(nL, j);
   nL = i ;
 }while(i < nR) ;

}

TCString TCStringList::GetName(long nIndex)
{
    TCString sLine;
    long nPos;

    sLine = (*this)[nIndex];
    nPos = Pos(sLine, "=");
    if (nPos == 0)
        return AllTrim(sLine);
    else
        return AllTrim(Left(sLine, nPos - 1));
}

long TCStringList::IndexOfName(TCString sName)
{
    long i;

    for (i = 0; i < GetCount(); i++)
        if (GetName(i) == sName)
            return i;

    return -1;
}

TCString TCStringList::GetValue(long nIndex)
{
    TCString sLine;
    long nPos;

    sLine = (*this)[nIndex];
    nPos = Pos(sLine, "=");
    if (nPos == 0)
        return "";
    else
        return AllTrim(Mid(sLine, nPos + 1));
}

TCString TCStringList::GetValue(TCString sName)
{
    long i;

    i = IndexOfName(sName);

    if (i == -1)
        return "";

    return GetValue(i);
}

void TCStringList::SetValue(TCString sName, TCString sValue)
{
    TCString sLine;
    sLine = sName + "=" + sValue;

    long nIndex;
    nIndex = IndexOfName(sName);

    if (nIndex == -1)
        Add(sLine);
    else
        m_pStrings[nIndex] = sLine;
}

bool TCStringList::IsExist(TCString &sValue)
{
    for (long i = 0; i < GetCount(); i++)
        if (m_pStrings[i] == sValue)
            return true;
    return false;
}
