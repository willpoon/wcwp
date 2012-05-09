//---------------------------------------------------------------------------

#ifndef c_db_indexH
#define c_db_indexH

//---------------------------------------------------------------------------

#include "cmpublic.h"

const WORD DB_VERSION = 13;    // Version 1.3
//---------------------------------------------------------------------------
// 结构 : TSDBDataBuf
// 用途 : 存放数据缓冲
//---------------------------------------------------------------------------
struct TSDBDataBuf
{
    TSDBDataBuf *   m_pBufPrev;         // list中的前一个Buffer
    TSDBDataBuf *   m_pBufNext;         // list中的下一个Buffer
    WORD            m_wBufSize;         // Buffer的字节数
    DWORD           m_dwBufCurrentBlock; // 当前的逻辑块(0:文件头)
    WORD            m_wBufCurrentSize;  // 当前的Block Size
    WORD            m_wBufRecordIndex;  // 块中的当前记录指针
    char *          m_pBufData;         // 指向数据的指针
};

typedef struct TSDBDataBuf * TBuffer;

//---------------------------------------------------------------------------
// 结构 : TSDBDataFile
// 用途 : 数据文件
//---------------------------------------------------------------------------
const long INDEX_FILE_NAME_MAX = 128;

struct TSDBDataFile
{
    WORD         m_wStat;            // 数据文件的状态
    TBuffer      m_bfFileHeader;     // 数据文件的文件头
    char         m_szFileName[INDEX_FILE_NAME_MAX];      // 文件名
    DWORD        m_dwPrevBlock;      // 前一个所读的Block
    WORD         m_wPrevRecord;      // 前一个所读的Record
    WORD         m_wPrevVSize;	     // Prev var rec size
    TBuffer      m_bfBuffer;         // 数据缓冲区
    TBuffer      m_bfTmp;            // 临时数据缓冲区
    TBuffer      m_bfAux;            // 另一个临时数据缓冲区
};

typedef  struct TSDBDataFile * TDataFile;
typedef  struct TSDBDataFile * TDataSet;

//---------------------------------------------------------------------------
// 结构 : TSDBFileHeader
// 用途 : 数据文件头
//---------------------------------------------------------------------------
enum TEFileType { ftDBSeq = 1, ftDBRandom = 2, ftDBIndex = 3, ftDBVar = 4 };
enum TEFileStat { fsDBClosed = 0, fsDBOpen = 1, fsDBDupAllowed = 2 };

struct TSDBFileHeader
{
    char    m_szFileTag[16];        // 文件串"BONSON IDX FILE "
    char    m_cCPUOrder;            // I : Intel; M : Motorola
    WORD    m_wDBVersion;           // 数据库的版本号
    WORD    m_wFileType;            // 文件类型，今后应改为枚举类型
    WORD    m_wFileStat;            // 文件状态，今后应改为枚举类型
    DWORD   m_dwLastBlock;          // 最后一次分配的Block
    WORD    m_wBlockSize;           // Block的字节数
    WORD    m_wRecordSize;          // 记录字节数
    WORD    m_wControlSize;         // 一条记录中的控制部分(指向块的指针)大小
    WORD    m_wDataSize;            // 存放用户数据的部分的大小
    DWORD   m_dwRecordCount;        // 记录数
    WORD    m_wRecordsPerBlock;     // 每Block的记录数
    DWORD   m_dwRootPointer;        // 根块的块号
    WORD    m_wBaseSize;            // Random File Base Size
    WORD    m_wKeySize;             // Key的字节数
    DWORD   m_dwNextAvail;          // 第一条有效记录
    WORD    m_wAttrSize;            // Size of Attribute Block
    DWORD   m_dwVFileSize;		    // Size of VAR file data
    WORD    m_wOwnerLinkCount;		// Owner link count
    WORD    m_wMemberLinkCount;		// Member link count
    WORD    m_wKeyLinkCount;	    // Key link count
    char    m_szOtherInfo[128];      // 存放其它设置信息
};

typedef  struct TSDBFileHeader *TFileHeader;

//---------------------------------------------------------------------------
// 结构 : TSDBFreeRecord
// 用途 :
//---------------------------------------------------------------------------
enum TEFreeStat { fsFree = '0', fsInUse = '1' };

struct TSDBFreeRecord
{
    char  m_cStat;    // 状态
    DWORD m_dwNext;   // 下一个有效块号
};

typedef struct TSDBFreeRecord * TFreeRecord;

//---------------------------------------------------------------------------
// 结构 : TSDBIndexHeader
// 用途 : 索引头
//---------------------------------------------------------------------------
struct TSDBIndexHeader
{
    char  m_cStat;           // 状态
    DWORD m_dwParent;        // 父块号
    WORD  m_wRecCount;       // 索引块中的记录数
};

typedef struct TSDBIndexHeader * TIndexHeader;

//---------------------------------------------------------------------------
// 结构 : TSDBIndexRecord
// 用途 : 索引记录
//---------------------------------------------------------------------------
struct TSDBIndexRecord
{
    DWORD dwIndexPointer;             // 指向索引块
};

typedef struct TSDBIndexRecord * TIndexRecord;

//---------------------------------------------------------------------------
// 结构 : TSDBOwnerLink
// 用途 :
//---------------------------------------------------------------------------
struct TSDBOwnerLink
{
    long m_nFirst;      // rec # of first member
    long m_nLast;       // rec # of last member
};

typedef struct TSDBOwnerLink * TOLink;

//---------------------------------------------------------------------------
// 结构 : TSDBMemberLink
// 用途 :
//---------------------------------------------------------------------------
struct TSDBMemberLink
{
    long m_nPrev;       // rec # of prev member
    long m_nNext;       // rec # of next member
    long m_nOwner;      // rec # of owner
};

typedef struct TSDBMemberLink * TMLink;

//---------------------------------------------------------------------------
// 结构 : TSDBKeyLink
// 用途 :
//---------------------------------------------------------------------------
struct TSDBKeyLink
{
    long m_nOwner;          // rec # of the master
};

typedef struct TSDBKeyLink * TKLink;

struct TSBufferPoolItem
{
    long    m_nRefreshCount;
    TBuffer m_bfBuffer;
    bool    m_bUpdated;
};

const long BUFFER_POOL_MAX = 48;

enum TEDBAddResult { arSucceed, arDuplicateKey };

//---------------------------------------------------------------------------
// 类   : TCDBIndex
// 用途 : 索引数据库类
//---------------------------------------------------------------------------
class TCDBIndex
{
private:
    bool m_bOpenWithReadOnly;       // 2001.11.29 ADDED

private:
    static bool s_m_bBufferLazyWrite;
    static bool s_m_bDBAddThrowException;

    TSBufferPoolItem m_BufferPool[BUFFER_POOL_MAX];

    DWORD  m_dwDBAddBlock;
    WORD   m_wDBAddRecord;

    DWORD  m_dwDBMatchBlock;
    WORD   m_wDBMatchRecord;

    TDataFile m_df;
    TCFileStream m_fFile;

protected:
    void BUFCopyBuffer(TBuffer bfDest, TBuffer bfSrc);
    bool BUFFetch(TBuffer buf, DWORD dwBlock);
    long BUFGetIndex(DWORD dwBlock);
    long BUFOldestIndex();
    void BUFRefresh(TBuffer buf, bool bUpdate = false);
    inline long BUFRefreshCount();
    void BUFRelease();

    void BUFWriteAllUpdated();

    TEDBAddResult DBAddIDX(char *szUserData);

    TBuffer DBAllocBuf(long nSize);

    void DBCheckDataFile();

    void DBExtend(TBuffer buf);

    void DBFileHeaderCreate(TFileHeader fh);
    void DBFileHeaderOpen(TFileHeader fh);

    void DBFile1Option(TFileHeader fh, TCString sOption, TCString sSubOption);
    void DBFileOptions(TFileHeader fh, TCString sOptionStr);

    bool DBFindFirstIDX(char *szUserData, char *szKey,
            long nKeySize);
    void DBFindInsertIDX(char *szKey, long nKeySize);

    TBuffer DBFreeBuffer(TBuffer buf);
    void    DBFreeDataFile();

    void    DBGetBlock(DWORD dwBlock, TBuffer buf);

    void    DBGetNextAvail(TBuffer buf);

    bool    DBGetParentIDX();

    void    DBPutBlock(TBuffer buf);
    void    DBPutBlockToFile(TBuffer buf);

    bool    DBReadFirstIDX(DWORD dwBlock, char *szUserData);
    bool    DBReadLastIDX(DWORD dwBlock, char *szUserData);

    bool    DBReadNextIDX(char *szUserData);
    bool    DBReadPrevIDX(char *szUserData);

public:
    TCDBIndex();
    ~TCDBIndex();

    static void BUFSetLazyWrite(bool bLazy = true)
    {   s_m_bBufferLazyWrite = bLazy;
    };

    static void DBAddThrowException(bool bThrow = true)
    {   s_m_bDBAddThrowException = bThrow;
    }

    TEDBAddResult DBAdd(char *szUserData);

    void      DBClose();
    void      DBCreate(TCString sFileName, TCString sOptions,
            TCString sOtherInfo = "");

    bool      DBFind(char *szUserData, char *szKey,
            long nKeySize = 0);

    TCString  DBGetOtherInfo();

    void      DBOpen(TCString sFileName);
    void      DBOpenR(TCString sFileName);

    bool      DBReadFirst(char *szUserData);
    bool      DBReadLast(char *szUserData);

    bool      DBReadNext(char *szUserData);
    bool      DBReadPrev(char *szUserData);

    void      DBSplitBlockIDX();

    void      DBZAP();

    WORD      DataSize()
    {
        return TFileHeader(m_df->m_bfFileHeader->m_pBufData)->m_wDataSize;
    }

    void      Flush();
};

//==========================================================================
// 函数 : TCDBIndex::BUFRefreshCount
// 用途 : 得到最新的刷新记数
// 原型 : inline long BUFRefreshCount();
// 参数 : 无
// 返回 : 刷新记录
// 说明 : 1. 用刷新计数维护数据的新旧，该数越小越旧。
//        2. 如果刷新计数超出一定的值，则刷新计数及缓冲区中的刷新计数要减少。
//==========================================================================
long TCDBIndex::BUFRefreshCount()
{   static long s_nCount = 0;
    s_nCount ++;

    if (s_nCount >= 1000000000L)
    {
        long i;
        for (i = 1; i < BUFFER_POOL_MAX; i++)
        {   if (m_BufferPool[i].m_nRefreshCount == 0)
                continue;
            m_BufferPool[i].m_nRefreshCount -= 800000000L;
            if (m_BufferPool[i].m_nRefreshCount <= 0)
                m_BufferPool[i].m_nRefreshCount = 1;
        }

        s_nCount = 300000000L;
    }

    return s_nCount;
};


#ifdef __TEST__
void TestIndexFileMainFunc();
#endif


#endif

