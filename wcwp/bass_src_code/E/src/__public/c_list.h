//---------------------------------------------------------------------------

#ifndef c_listH
#define c_listH
#include "cmpublic.h"

#define  InitAllocLength  20

//---------------------------------------------------------------------------
enum TEListDuplicates
{
    dupIgnore,
    dupAccept,
    dupError
};

class TCList
{
  public :
     TCList();
     virtual ~TCList();

     virtual long Add(void * P) ;
     virtual void Clear(void);
     long         GetCount(){return m_nCount;};
     virtual void Delete(long Index);
     virtual void Insert(long nIndex,void * P);
     long         IndexOf(void * pItem);
     virtual void Remove(void * pItem);

  protected :
     struct pPointer{
       void *P ;
     };
     long m_nCount;
     long m_AllocLength;
     void ** m_Pointer ;
     virtual void AllocBuffer(long lCount) ;
     virtual void AllocNewBuffer(long NewCount) ;
     virtual long ExtendLength() ;
     virtual void init() ;
     virtual void Release() ;
     virtual void Release(void ** P) ;

  protected:
     virtual void Destroy(void * P);

  public :
     void * operator[](long nIndex) const ;
     TCList& operator=(TCList & sSrcList);

     void * Get(long nIndex);
     void * Last();

  private:
    bool m_bSorted;
    TEListDuplicates m_eDuplicates;

    void InsertItem(long nIndex, void * P);

    void ExchangeItems(long nIndex1, long nIndex2);
    void QuickSort(long nL, long nR);

  protected:
    virtual long CompareValue(void *pItem1, void *pItem2);

  public:
    void Exchange(long nIndex1, long nIndex2);

    bool FindValue(void * P, long & nIndex);
    long IndexOfValue(void * pItem);

    void SetDuplicates(TEListDuplicates duplicates)
    { m_eDuplicates = duplicates; }
    TEListDuplicates GetDuplicates()
    { return m_eDuplicates; }

    void Sort();
    void SetSorted(bool bSorted);
    bool GetSorted()
    { return m_bSorted; }
};

inline void TCList::init()
{
    m_bSorted = false;
    m_Pointer = NULL ;
    m_nCount = 0 ;
    m_AllocLength = 0 ;
    m_eDuplicates = dupAccept;
}

#ifdef __TEST__
void TestNumberListMainFunc();
#endif

#endif


