//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_list.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)

//==========================================================================
// ���� : TCList::TCList
// ��; : ���캯������ʼ��˽�б���
// ԭ�� : TCList();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
TCList::TCList()
{
   init();
}

//==========================================================================
// ���� : TCList::~TCList
// ��; : ��������, ����Release()�ͷ�������ڴ�
// ԭ�� : ~TCList();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
TCList::~TCList()
{
   Release() ;
}

//==========================================================================
// ���� : TCList::Add
// ��; : ����һ��ָ��
// ԭ�� : void Add(void * P);
// ���� : P -- ָ��
// ���� : ��
// ˵�� : �����������״̬�����ȶ�λ������룬ʼ�ձ�������״̬
//==========================================================================
long TCList::Add(void * P)
{
    long nIndex;
    if (!m_bSorted)
        nIndex = m_nCount;
    else
    {
        if (FindValue(P, nIndex))
        {
            if (m_eDuplicates == dupIgnore)
                return nIndex;
            else if (m_eDuplicates == dupError)
                throw TCException("TCList::Add() : String list does not "
                        "allow duplicates");
        }
    }

    InsertItem(nIndex, P);

    return nIndex;
}

//==========================================================================
// ���� : TCList::AllocBuffer
// ��; : �����ڴ�ռ�
// ԭ�� : void AllocBuffer(long lCount);
// ���� : lCount -- ����Ĵ�С
// ���� : ��
// ˵�� :
//==========================================================================
void TCList::AllocBuffer(long lCount)
{
    ASSERT(lCount >= 0);
    ASSERT(lCount <= LONG_MAX-1);
    if (lCount == 0 )
    {
      init() ;
    }
    else
    {
      m_Pointer = (void**)new char[sizeof(struct pPointer)*lCount] ;
      if( m_Pointer == NULL )
      {
        init();
        throw TCException("Alloc mem error!");
      }
      m_AllocLength = lCount ;
    }
}

//==========================================================================
// ���� : TCList::AllocNewBuffer
// ��; : �����µ��ڴ�ռ�
// ԭ�� : void AllocNewBuffer(long NewLen);
// ���� : NewLen -- ���ڴ�ռ�Ĵ�С
// ���� : ��
// ˵�� : �ú����Ὣ�ϵ��ڴ渴�Ƶ��µĵط�
//==========================================================================
void TCList::AllocNewBuffer(long NewLen)
{
    long OldAllocLength ;
    long nOldCount ;
    void ** OldList ;
    OldAllocLength = m_AllocLength ;
    nOldCount = OldAllocLength ;
    OldList =  m_Pointer ;
    AllocBuffer(NewLen) ;
    if (OldAllocLength > m_AllocLength)
       nOldCount = m_AllocLength ;
    memcpy(m_Pointer,OldList,nOldCount*sizeof(struct pPointer));
    Release(OldList) ;
}

//==========================================================================
// ���� : TCList::Clear
// ��; : ������е�����
// ԭ�� : void Clear(void );
// ���� :  --
// ���� : ��
// ˵�� :
//==========================================================================
void TCList::Clear(void)
{
    Release() ;
}

//==========================================================================
// ���� : TCList::CompareValue
// ��; : �Ƚ�����ָ��ָ���ֵ�Ĵ�С
// ԭ�� : long CompareValue(void *pItem1, void *pItem2);
// ���� : pItem1 -- ֵ1��ָ��
//        pItem2 -- ֵ2��ָ��
// ���� : ���*pItem1 < *pItem2���򷵻ص�ֵС��0;
//        ���*pItem1 > *pItem2���򷵻ص�ֵ����0;
//        ���*pItem1 = *pItem2, �򷵻ص�ֵ����0
// ˵�� : ������������Sort����Ӧ���ظú���
// ��ʷ : 2001.12��������Sort֮��
//==========================================================================
#pragma argsused
long TCList::CompareValue(void *pItem1, void *pItem2)
{
    throw TCException("TCList::CompareValue() : CompareValue is not supported in "
            "base class.  If you want to sort, override the function in "
            "inherited classed.");
}

//==========================================================================
// ���� : TCList::Delete
// ��; : ɾ��һ����Ŀ
// ԭ�� : void Delete(long Index);
// ���� : Index -- ��ĿIndex, ��0��ʼ����
// ���� : ��
// ˵�� :
// ��ʷ : 2001.12 ����Detroy���ã����ͷ��ⲿָ��
//==========================================================================
void TCList::Delete(long Index)
{
    ASSERT(Index >= 0) ;
    ASSERT(Index < m_nCount) ;
    if ((Index < m_nCount)&&(m_Pointer != NULL))
    {
        Destroy(m_Pointer[Index]);
        if ( Index != (m_nCount-1) )
        {
            memmove((char *)(m_Pointer) + Index*sizeof(struct pPointer),
                    (char *)(m_Pointer) + (Index+1)*sizeof(struct pPointer),
                    (m_nCount - Index - 1)*sizeof(struct pPointer));
        }
        m_nCount -= 1 ;
    }
}

//==========================================================================
// ���� : TCList::Destroy
// ��; : �ͷ��ⲿָ��
// ԭ�� : void Destroy(void * P);
// ���� : P -- �ⲿָ��
// ���� : ��
// ˵�� : ���಻���κβ������̳���Ӧ���ظú��������ͷ��ⲿָ��
// ��ʷ : 2001.12 ���Ӹú�������Delete���ã��ͷ��ⲿָ��
//==========================================================================
#pragma argsused
void TCList::Destroy(void * P)
{
}

//==========================================================================
// ���� : TCList::Exchange
// ��; : ��������LIST���λ��
// ԭ�� : void Exchange(long nIndex1, long nIndex2);
// ���� : nIndex1 -- ��1��Index(��0����)
//        nIndex2 -- ��2��Index
// ���� : ��
// ˵�� : ֱ�ӵ���ExchangeItems���ʵ�ʵĲ���
//==========================================================================
void TCList::Exchange(long nIndex1, long nIndex2)
{
    ExchangeItems(nIndex1, nIndex2);
}

//==========================================================================
// ���� : TCList::ExchangeItems
// ��; : ��������LIST���λ��
// ԭ�� : void ExchangeItems(long nIndex1, long nIndex2);
// ���� : nIndex1 -- ��1��Index(��0����)
//        nIndex2 -- ��2��Index
// ���� : ��
// ˵�� :
//==========================================================================
void TCList::ExchangeItems(long nIndex1, long nIndex2)
{
    void * pTemp;

    pTemp = m_Pointer[nIndex1];

    m_Pointer[nIndex1] = m_Pointer[nIndex2];
    m_Pointer[nIndex2] = pTemp;
}

//==========================================================================
// ���� : TCList::ExtendLength
// ��; : �õ���չ�Ĵ�С
// ԭ�� : long ExtendLength();
// ���� : ��
// ���� : ��չ�Ĵ�С
// ˵�� :
//==========================================================================
long TCList::ExtendLength()
{
    if (m_AllocLength <= 0)
       return InitAllocLength;
    if (m_AllocLength > 10000)
       return m_AllocLength + 5000;
    else
        return m_AllocLength + m_AllocLength/2;
}

//==========================================================================
// ���� : TCList::FindValue
// ��; : ��ֵ�����Ƿ����
// ԭ�� : bool FindValue(void * P, long & nIndex);
// ���� : P      -- ָ��ֵ��ָ��
//        nIndex -- �ҵ�������������
// ���� : �Ƿ��ҵ�
// ˵�� :
//==========================================================================
bool TCList::FindValue(void * P, long & nIndex)
{
    long nL, nH, nI, nC;
    bool bResult;

    bResult = false;
    nL = 0;
    nH = m_nCount - 1;

    while (nL <= nH)
    {
        nI = (nL + nH) / 2;
        nC = CompareValue(Get(nI), P);
        if (nC < 0)
            nL = nI + 1;
        else
        {
            nH = nI - 1;
            if (nC == 0)
            {
                bResult = true;
                if (m_eDuplicates != dupAccept)
                    nL = nI;
            }
        }
    }

    nIndex = nL;
    return bResult;
}

//==========================================================================
// ���� : TCList::Get
// ��; : �õ�ָ���±��ָ��
// ԭ�� : void* Get(long nIndex);
// ���� : nIndex -- ָ�����±�
// ���� : ���ص�ָ��
// ˵�� :
//==========================================================================
void * TCList::Get(long nIndex)
{
    ASSERT(nIndex >= 0);
    ASSERT(nIndex < m_nCount);
    return m_Pointer[nIndex];
}

//==========================================================================
// ���� : TCList::IndexOf
// ��; : �õ�һ��ָ����±�
// ԭ�� : long IndexOf(void * pItem);
// ���� : pItem -- ָ����ָ��
// ���� : ���ص��±꣬���û���ҵ��򷵻�-1
// ˵�� :
//==========================================================================
long TCList::IndexOf(void * pItem)
{
    long nIndex = 0;
    while ((nIndex < m_nCount) && (m_Pointer[nIndex] != pItem))
        nIndex++;
    if(nIndex == m_nCount)
        nIndex = -1;
    return nIndex ;
}

//==========================================================================
// ���� : TCList::IndexOfValue
// ��; : �õ�һ��ֵ��LIST�е��±�
// ԭ�� : long IndexOfValue(void * pItem);
// ���� : pItem -- ָ��ֵ��ָ��
// ���� : ���ص��±�
// ˵�� : ��IndexOf��ͬ���ǣ�IndexOfValue�Ƚϵ���ֵ����Ҫ����Compare������
//        IndexOf�Ƚϵ���ָ��
//==========================================================================
long TCList::IndexOfValue(void * pItem)
{
    //========= 1. ��������������Find()���� ==========
    long nResult;

    if (m_bSorted) 
    {
        if (!FindValue(pItem, nResult))
            return -1;
        else
            return nResult;
    }

    //=========== 2. ���δ����, ��˳����� ==============
    long i;

    for (i = 0; i < m_nCount; i++)
    {
        if (CompareValue(pItem, Get(i)) == 0)
            return i;
    }

    return -1;
}

//==========================================================================
// ���� : TCList::Insert
// ��; : ����һ��ָ����ָ�����±�֮ǰ
// ԭ�� : void Insert(long Index, void * P);
// ���� : nIndex -- �±�
//        P      --  ָ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCList::Insert(long nIndex,void * P)
{
    ASSERT(nIndex >= 0);
    ASSERT(nIndex <= m_nCount);

    if (m_bSorted)
        throw TCException("TCList::Insert() : Operation not allowed on sorted list");

    InsertItem(nIndex, P);
}

//==========================================================================
// ���� : TCList::InsertItem
// ��; : ����һ��ָ�뵽ָ��������֮ǰ
// ԭ�� : void InsertItem(long nIndex, void * P);
// ���� : nIndex -- ����
//        P      -- ָ��
// ���� : ��
// ˵�� : �ú�����Ҫ���ڱ�Add��Insert����
//==========================================================================
void TCList::InsertItem(long nIndex, void * P)
{
    if ( m_Pointer == NULL || m_AllocLength == 0 )
    {
        AllocBuffer(InitAllocLength) ;
        m_Pointer[0] = P ;
        m_nCount = 1 ;
        return;
    }

    if  (m_nCount >= m_AllocLength)
        AllocNewBuffer(ExtendLength());

    if (nIndex < m_nCount)
        memmove((char *)(m_Pointer) + (nIndex+1)*sizeof(struct pPointer),
                (char *)(m_Pointer) + nIndex*sizeof(struct pPointer),
                (m_nCount - nIndex)*sizeof(struct pPointer)) ;
    m_Pointer[nIndex] = P ;
    m_nCount += 1 ;
}

//==========================================================================
// ���� : TCList::Last
// ��; : �õ����һ��ָ��
// ԭ�� : void* Last();
// ���� : ��
// ���� : ���һ��ָ��
// ˵�� :
//==========================================================================
void * TCList::Last()
{
    ASSERT(m_nCount > 0);
    return Get(m_nCount - 1);
}

//==========================================================================
// ���� : TCList::QuickSort
// ��; : ���������㷨
// ԭ�� : void QuickSort(long nL, long nR);
// ���� : nL -- ��ǰһ��Index
//        nR -- ���һ��Index
// ���� : ��
// ˵�� : �ú��������麯��Compare�������ֵ�ıȽ�
//==========================================================================
void TCList::QuickSort(long nL, long nR)
{
    long i, j;
    long nP;
    while (true)
    {
        i = nL;
        j = nR;
        nP = (nL + nR) / 2;

        while (true)
        {
            while (CompareValue(Get(i), Get(nP)) < 0)
                i ++;

            while (CompareValue(Get(j), Get(nP)) > 0)
                j --;

            if (i <= j)
            {
                ExchangeItems(i, j);

                if (nP == i)
                    nP = j;
                else if (nP == j)
                    nP = i;
                i ++;
                j --;

            }

            if (i > j)
                break;
        }

        if (nL < j)
            QuickSort(nL, j);
        nL = i;
        if (i >= nR)
            break;
    }
}

//==========================================================================
// ���� : TCList::Release
// ��; : �ͷ�������ڴ�
// ԭ�� : void Release();
// ���� : ��
// ���� : ��
// ˵�� :
// ��ʷ : 2001.12 ����ǰ���е�Delete(i)�����ͷ��ⲿָ��
//==========================================================================
void TCList::Release()
{
    long i;
    for (i = m_nCount - 1; i >= 0; i--)
        Delete(i);

   if ( m_Pointer != NULL )
      delete[] m_Pointer ;

   m_Pointer = NULL;

   init() ;
}

//==========================================================================
// ���� : TCList::Release
// ��; : �ͷ�һ��ָ������
// ԭ�� : void Release(void ** P);
// ���� : P -- ָ������
// ���� : ��
// ˵�� :
//==========================================================================
void TCList::Release(void ** P)
{
   if (P != NULL)
      delete[] P;

   P = NULL;
}

//==========================================================================
// ���� : TCList::Remove
// ��; : ɾ��ָ��ָ���һ����¼
// ԭ�� : void Remove(void * pItem);
// ���� : pItem -- ָ����ָ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCList::Remove(void * pItem)
{
  long nIndex ;
  nIndex = IndexOf(pItem) ;
  if (nIndex != -1)
      Delete(nIndex) ;
}

//==========================================================================
// ���� : TCList::SetSorted
// ��; : ����LISTΪ����/������״̬
// ԭ�� : void SetSorted(bool bSorted);
// ���� : bSorted -- ����״̬
// ���� : ��
// ˵�� : ����ɷ�����״̬��Ϊ����״̬��������Sort()����������������״̬�£�
//        ��λ�Ͳ��붼������ʽ����
//==========================================================================
void TCList::SetSorted(bool bSorted)
{
    if (m_bSorted != bSorted)
    {
        if (bSorted)
            Sort();
        m_bSorted = bSorted;
    }
}

//==========================================================================
// ���� : TCList::Sort
// ��; : ��LIST��������
// ԭ�� : void Sort();
// ���� : ��
// ���� : ��
// ˵�� : 
//==========================================================================
void TCList::Sort()
{
    if (!m_bSorted && m_nCount > 1)
    {
        QuickSort(0, m_nCount - 1);
    }
}

//==========================================================================
// ���� : TCList::operator[]
// ��; : []���������õ�ָ���±��ָ��
// ԭ�� : void * operator[](long nIndex) const;
// ���� : nIndex -- ָ���±�
// ���� : ��
// ˵�� :
//==========================================================================
void * TCList::operator[](long nIndex) const
{
   ASSERT(nIndex >= 0);
   ASSERT( nIndex < m_nCount);
   if (nIndex < m_nCount)
       return m_Pointer[nIndex] ;
   else return NULL ;
}

//==========================================================================
// ���� : TCList::operator=
// ��; : ��ֵ������
// ԭ�� : TCList& operator=(TCList & sSrcList);
// ���� : sSrcList -- ԴList
// ���� : ���������
// ˵�� :
//==========================================================================
TCList& TCList::operator=(TCList & sSrcList)
{
  long nCount = sSrcList.GetCount() ;
  for(long i = 0;i<nCount ; i++)
  {
    Add(sSrcList[i]) ;
  }
  return * this ;
}

#ifdef __TEST__

class TCNumberList : public TCList
{
public:
    void AddNumber(long nNumber);
    virtual void Destroy(void * P);
    ~TCNumberList();
protected:
    long CompareValue(void *pItem1, void *pItem2);
};

TCNumberList::~TCNumberList()
{
    Release();
}

void TCNumberList::AddNumber(long nNumber)
{
    long * pNumber;
    pNumber = new long;
    *pNumber = nNumber;
    Add(pNumber);
}

long TCNumberList::CompareValue(void *pItem1, void *pItem2)
{
    if (*(long *)pItem1 > *(long *)pItem2)
        return 1;
    else if (*(long *)pItem1 < *(long *)pItem2)
        return -1;
    else
        return 0;
}

void TCNumberList::Destroy(void * P)
{
    printf("^");
    fflush(stdout);
    delete (long *)P;
}

//================== ��ʾ��ʾ��Ϣ ===========
TCNumberList * pNumberList;

void DisplayTestNumberListPrompt()
{
    printf("\n\n=========== Test NumberList =========\n\n");
    printf("0. New\n");
    printf("1. Add Rand Numbers\n");
    printf("2. Browse Numbers\n");
    printf("3. Delete Number\n");
    printf("4. Sort Numbers\n");
    printf("5. Toggle Sorted\n");
    printf("6. Toggle Duplicates\n");
    printf("7. Insert\n");
    printf("8. Search\n");
    printf("9. Delete\n");
    printf("A. Clear\n");

    printf("\nQ. Quit\n\n");
}

void TestNumberList0New()
{
    printf("Deleting NumberList Pointer...\n");
    delete pNumberList;
    printf("New NumberList Pointer...\n");
    pNumberList = new TCNumberList;
}

void TestNumberList1AddRandNumber()
{
    long nTotal, i;
    printf("Input the total number you want to add : ");
    nTotal = StrToInt(TCSystem::ReadStringFromConsole());
    for (i = 0; i < nTotal; i++)
    {
        pNumberList->AddNumber(RandLong(100));
    }
}

void TestNumberList2BrowseNumber()
{
    long i;
    for (i = 0; i < pNumberList->GetCount(); i++)
    {
        printf("%d : %d\n", i + 1, *(long *)pNumberList->Get(i));
    }
}

void TestNumberList3DeleteNumber()
{
    long nIndex;
    printf("Input the index you want to delete : ");
    nIndex = StrToInt(TCSystem::ReadStringFromConsole()) - 1;
    if (nIndex < 0 || nIndex >= pNumberList->GetCount())
    {
        printf("The Index is out of scope.\n");
        return;
    }

    pNumberList->Delete(nIndex);
}

void TestNumberList4Sort()
{
    pNumberList->Sort();
}

void TestNumberList5ToggleSorted()
{
    bool bSorted;
    bSorted = pNumberList->GetSorted();
    bSorted = !bSorted;
    pNumberList->SetSorted(bSorted);
    if (bSorted)
        printf("The Number List Is Sorted.\n");
    else
        printf("The Number List Is Unsorted.\n");
}

void TestNumberList6ToggleDuplicates()
{
    TEListDuplicates eDuplicates;
    eDuplicates = pNumberList->GetDuplicates();
    eDuplicates = (TEListDuplicates)((eDuplicates + 1) % 3);
    pNumberList->SetDuplicates(eDuplicates);
    if (eDuplicates == dupIgnore)
        printf("Duplicate Mode set to DUP_IGNORE\n");
    else if (eDuplicates == dupAccept)
        printf("Duplicate Mode set to DUP_ACCEPT\n");
    else
    {
        ASSERT(eDuplicates == dupError);
        printf("Duplicate Mode set to DUP_ERROR\n");
    }
}

void TestNumberList7Insert()
{
    long nIndex, nNumber;
    printf("Input the index you want to insert before : ");
    nIndex = StrToInt(TCSystem::ReadStringFromConsole());
    printf("Input the number you want to insert : ");
    nNumber = StrToInt(TCSystem::ReadStringFromConsole());
    pNumberList->Insert(nIndex, new long(nNumber));
}

void TestNumberList8Search()
{
    long nNumber;
    printf("Input the number you want to search : ");
    nNumber = StrToInt(TCSystem::ReadStringFromConsole());

    long nIndex;
    nIndex = pNumberList->IndexOfValue(&nNumber);

    if (nIndex == -1)
        printf("Cannot Find the number\n");
    else
        printf("The index is : %d\n", nIndex + 1);

    if (pNumberList->GetSorted())
    {
        pNumberList->FindValue(&nNumber, nIndex);
        printf("Find Index is : %d", nIndex + 1);
    }
}

void TestNumberList9Delete()
{
    long nIndex;
    nIndex = StrToInt(TCSystem::ReadStringFromConsole());
    printf("Input the number you want to insert : ");
    pNumberList->Delete(nIndex - 1);
}

void TestNumberListAClear()
{
    pNumberList->Clear();
}

void TestNumberListMainFunc()
{
    int cChar;

    pNumberList = new TCNumberList;

    DisplayTestNumberListPrompt();

    while (true)
    {
        cChar = getchar();

        switch (cChar)
        {
            case 'Q':
            case 'q':
            case 0x1B:
                delete pNumberList;
                return;

            case '0':
                TestNumberList0New();
                break;

            case '1':
                TestNumberList1AddRandNumber();
                break;

            case '2':
                TestNumberList2BrowseNumber();
                break;

            case '3':
                TestNumberList3DeleteNumber();
                break;

            case '4':
                TestNumberList4Sort();
                break;

            case '5':
                TestNumberList5ToggleSorted();
                break;

            case '6':
                TestNumberList6ToggleDuplicates();
                break;

            case '7':
                TestNumberList7Insert();
                break;

            case '8':
                TestNumberList8Search();
                break;

            case '9':
                TestNumberList9Delete();
                break;

            case 'a':
            case 'A':
                TestNumberListAClear();
                break;

            default:
                continue;
        }
        DisplayTestNumberListPrompt();
    }
}

#endif


