#ifndef _c_string_listH
#define _c_string_listH

#include "cmpublic.h"

#define  InitAllocLength  20

enum e_ListSaveMode{omCreateNew,omAppendMode} ;

class TCStringList
{
  public :
     TCStringList();
     TCStringList(TCStringList & sSrcList);
     ~TCStringList();
     virtual void Add(const TCString & c_str) ;
     virtual void Clear(void);
     long GetCount(){return m_StringCount;} ;
     virtual void Delete(long Index) ;
     virtual void Insert(long Index,const TCString & S) ;
     virtual void Sort() ;
     virtual void QuickSort(long nL ,long nR) ;
     virtual TCString GetText(void) const;
     virtual void SetText(const TCString SetValues);
     void CommaText(const TCString srcText,char cCommaChar = ',',
            bool bIgnoreLastEmptyString = true);
     int LoadFromFile(TCString srcFileName) ;
     int SaveToFile(TCString destFileName,
            enum e_ListSaveMode e_savemode = omCreateNew) ;

     virtual TCString GetName(long nIndex);
     virtual long IndexOfName(TCString sName);
     virtual TCString GetValue(long nIndex);
     virtual TCString GetValue(TCString sName);

     virtual void SetValue(TCString sName, TCString sValue);

     //============================= add by yxw 2002.06.24
     bool IsExist(TCString &sValue);

  protected :
     long m_StringCount;
     long m_AllocLength;
     TCString * m_pStrings ;
     virtual void AllocBuffer(long lCount) ;
     virtual void AllocNewBuffer(long NewCount) ;
     virtual long ExtendLength() ;
     virtual void init() ;
     virtual void Release() ;
     virtual void Release(TCString * pListData) ;

  public :
     TCString& operator[](long nIndex) const ;
     TCStringList & operator=(TCStringList & sSrcList);
};



#endif
