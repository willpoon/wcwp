//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_list.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)

//==========================================================================
// 函数 : TCList::TCList
// 用途 : 构造函数，初始化私有变量
// 原型 : TCList();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
TCList::TCList()
{
   init();
}

//==========================================================================
// 函数 : TCList::~TCList
// 用途 : 析构函数, 调用Release()释放申请的内存
// 原型 : ~TCList();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
TCList::~TCList()
{
   Release() ;
}

//==========================================================================
// 函数 : TCList::Add
// 用途 : 加入一个指针
// 原型 : void Add(void * P);
// 参数 : P -- 指针
// 返回 : 无
// 说明 : 如果已在排序状态，则按先定位，后插入，始终保持排序状态
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
// 函数 : TCList::AllocBuffer
// 用途 : 申请内存空间
// 原型 : void AllocBuffer(long lCount);
// 参数 : lCount -- 申请的大小
// 返回 : 无
// 说明 :
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
// 函数 : TCList::AllocNewBuffer
// 用途 : 申请新的内存空间
// 原型 : void AllocNewBuffer(long NewLen);
// 参数 : NewLen -- 新内存空间的大小
// 返回 : 无
// 说明 : 该函数会将老的内存复制到新的地方
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
// 函数 : TCList::Clear
// 用途 : 清除所有的内容
// 原型 : void Clear(void );
// 参数 :  --
// 返回 : 无
// 说明 :
//==========================================================================
void TCList::Clear(void)
{
    Release() ;
}

//==========================================================================
// 函数 : TCList::CompareValue
// 用途 : 比较两个指针指向的值的大小
// 原型 : long CompareValue(void *pItem1, void *pItem2);
// 参数 : pItem1 -- 值1的指针
//        pItem2 -- 值2的指针
// 返回 : 如果*pItem1 < *pItem2，则返回的值小于0;
//        如果*pItem1 > *pItem2，则返回的值大于0;
//        如果*pItem1 = *pItem2, 则返回的值等于0
// 说明 : 在子类中如需Sort，则应重载该函数
// 历史 : 2001.12增加以做Sort之用
//==========================================================================
#pragma argsused
long TCList::CompareValue(void *pItem1, void *pItem2)
{
    throw TCException("TCList::CompareValue() : CompareValue is not supported in "
            "base class.  If you want to sort, override the function in "
            "inherited classed.");
}

//==========================================================================
// 函数 : TCList::Delete
// 用途 : 删除一个项目
// 原型 : void Delete(long Index);
// 参数 : Index -- 项目Index, 从0开始计数
// 返回 : 无
// 说明 :
// 历史 : 2001.12 增加Detroy调用，以释放外部指针
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
// 函数 : TCList::Destroy
// 用途 : 释放外部指针
// 原型 : void Destroy(void * P);
// 参数 : P -- 外部指针
// 返回 : 无
// 说明 : 父类不做任何操作，继承类应重载该函数，以释放外部指针
// 历史 : 2001.12 增加该函数，被Delete调用，释放外部指针
//==========================================================================
#pragma argsused
void TCList::Destroy(void * P)
{
}

//==========================================================================
// 函数 : TCList::Exchange
// 用途 : 交换两个LIST项的位置
// 原型 : void Exchange(long nIndex1, long nIndex2);
// 参数 : nIndex1 -- 项1的Index(从0计数)
//        nIndex2 -- 项2的Index
// 返回 : 无
// 说明 : 直接调用ExchangeItems完成实际的操作
//==========================================================================
void TCList::Exchange(long nIndex1, long nIndex2)
{
    ExchangeItems(nIndex1, nIndex2);
}

//==========================================================================
// 函数 : TCList::ExchangeItems
// 用途 : 交换两个LIST项的位置
// 原型 : void ExchangeItems(long nIndex1, long nIndex2);
// 参数 : nIndex1 -- 项1的Index(从0计数)
//        nIndex2 -- 项2的Index
// 返回 : 无
// 说明 :
//==========================================================================
void TCList::ExchangeItems(long nIndex1, long nIndex2)
{
    void * pTemp;

    pTemp = m_Pointer[nIndex1];

    m_Pointer[nIndex1] = m_Pointer[nIndex2];
    m_Pointer[nIndex2] = pTemp;
}

//==========================================================================
// 函数 : TCList::ExtendLength
// 用途 : 得到扩展的大小
// 原型 : long ExtendLength();
// 参数 : 无
// 返回 : 扩展的大小
// 说明 :
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
// 函数 : TCList::FindValue
// 用途 : 按值查找是否存在
// 原型 : bool FindValue(void * P, long & nIndex);
// 参数 : P      -- 指向值的指针
//        nIndex -- 找到的索引的引用
// 返回 : 是否找到
// 说明 :
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
// 函数 : TCList::Get
// 用途 : 得到指定下标的指针
// 原型 : void* Get(long nIndex);
// 参数 : nIndex -- 指定的下标
// 返回 : 返回的指针
// 说明 :
//==========================================================================
void * TCList::Get(long nIndex)
{
    ASSERT(nIndex >= 0);
    ASSERT(nIndex < m_nCount);
    return m_Pointer[nIndex];
}

//==========================================================================
// 函数 : TCList::IndexOf
// 用途 : 得到一个指针的下标
// 原型 : long IndexOf(void * pItem);
// 参数 : pItem -- 指定的指针
// 返回 : 返回的下标，如果没有找到则返回-1
// 说明 :
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
// 函数 : TCList::IndexOfValue
// 用途 : 得到一个值在LIST中的下标
// 原型 : long IndexOfValue(void * pItem);
// 参数 : pItem -- 指向值的指针
// 返回 : 返回的下标
// 说明 : 与IndexOf不同的是，IndexOfValue比较的是值（需要调用Compare），而
//        IndexOf比较的是指针
//==========================================================================
long TCList::IndexOfValue(void * pItem)
{
    //========= 1. 如果已排序，则调用Find()函数 ==========
    long nResult;

    if (m_bSorted) 
    {
        if (!FindValue(pItem, nResult))
            return -1;
        else
            return nResult;
    }

    //=========== 2. 如果未排序, 则顺序查找 ==============
    long i;

    for (i = 0; i < m_nCount; i++)
    {
        if (CompareValue(pItem, Get(i)) == 0)
            return i;
    }

    return -1;
}

//==========================================================================
// 函数 : TCList::Insert
// 用途 : 插入一个指针在指定的下标之前
// 原型 : void Insert(long Index, void * P);
// 参数 : nIndex -- 下标
//        P      --  指针
// 返回 : 无
// 说明 :
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
// 函数 : TCList::InsertItem
// 用途 : 插入一个指针到指定的索引之前
// 原型 : void InsertItem(long nIndex, void * P);
// 参数 : nIndex -- 索引
//        P      -- 指针
// 返回 : 无
// 说明 : 该函数主要用于被Add、Insert调用
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
// 函数 : TCList::Last
// 用途 : 得到最后一个指针
// 原型 : void* Last();
// 参数 : 无
// 返回 : 最后一个指针
// 说明 :
//==========================================================================
void * TCList::Last()
{
    ASSERT(m_nCount > 0);
    return Get(m_nCount - 1);
}

//==========================================================================
// 函数 : TCList::QuickSort
// 用途 : 快速排序算法
// 原型 : void QuickSort(long nL, long nR);
// 参数 : nL -- 最前一个Index
//        nR -- 最后一个Index
// 返回 : 无
// 说明 : 该函数调用虚函数Compare完成两个值的比较
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
// 函数 : TCList::Release
// 用途 : 释放申请的内存
// 原型 : void Release();
// 参数 : 无
// 返回 : 无
// 说明 :
// 历史 : 2001.12 增加前三行的Delete(i)，以释放外部指针
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
// 函数 : TCList::Release
// 用途 : 释放一个指针数组
// 原型 : void Release(void ** P);
// 参数 : P -- 指针数组
// 返回 : 无
// 说明 :
//==========================================================================
void TCList::Release(void ** P)
{
   if (P != NULL)
      delete[] P;

   P = NULL;
}

//==========================================================================
// 函数 : TCList::Remove
// 用途 : 删除指定指针的一条记录
// 原型 : void Remove(void * pItem);
// 参数 : pItem -- 指定的指针
// 返回 : 无
// 说明 :
//==========================================================================
void TCList::Remove(void * pItem)
{
  long nIndex ;
  nIndex = IndexOf(pItem) ;
  if (nIndex != -1)
      Delete(nIndex) ;
}

//==========================================================================
// 函数 : TCList::SetSorted
// 用途 : 设置LIST为排序/非排序状态
// 原型 : void SetSorted(bool bSorted);
// 参数 : bSorted -- 排序状态
// 返回 : 无
// 说明 : 如果由非排序状态设为排序状态，将调用Sort()进行排序。且在排序状态下，
//        定位和插入都按排序方式进行
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
// 函数 : TCList::Sort
// 用途 : 对LIST进行排序
// 原型 : void Sort();
// 参数 : 无
// 返回 : 无
// 说明 : 
//==========================================================================
void TCList::Sort()
{
    if (!m_bSorted && m_nCount > 1)
    {
        QuickSort(0, m_nCount - 1);
    }
}

//==========================================================================
// 函数 : TCList::operator[]
// 用途 : []操作符，得到指定下标的指针
// 原型 : void * operator[](long nIndex) const;
// 参数 : nIndex -- 指定下标
// 返回 : 无
// 说明 :
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
// 函数 : TCList::operator=
// 用途 : 赋值操作符
// 原型 : TCList& operator=(TCList & sSrcList);
// 参数 : sSrcList -- 源List
// 返回 : 本身的引用
// 说明 :
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

//================== 显示提示信息 ===========
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


