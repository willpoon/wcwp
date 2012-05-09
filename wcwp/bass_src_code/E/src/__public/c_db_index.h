//---------------------------------------------------------------------------

#ifndef c_db_indexH
#define c_db_indexH

//---------------------------------------------------------------------------

#include "cmpublic.h"

const WORD DB_VERSION = 13;    // Version 1.3
//---------------------------------------------------------------------------
// �ṹ : TSDBDataBuf
// ��; : ������ݻ���
//---------------------------------------------------------------------------
struct TSDBDataBuf
{
    TSDBDataBuf *   m_pBufPrev;         // list�е�ǰһ��Buffer
    TSDBDataBuf *   m_pBufNext;         // list�е���һ��Buffer
    WORD            m_wBufSize;         // Buffer���ֽ���
    DWORD           m_dwBufCurrentBlock; // ��ǰ���߼���(0:�ļ�ͷ)
    WORD            m_wBufCurrentSize;  // ��ǰ��Block Size
    WORD            m_wBufRecordIndex;  // ���еĵ�ǰ��¼ָ��
    char *          m_pBufData;         // ָ�����ݵ�ָ��
};

typedef struct TSDBDataBuf * TBuffer;

//---------------------------------------------------------------------------
// �ṹ : TSDBDataFile
// ��; : �����ļ�
//---------------------------------------------------------------------------
const long INDEX_FILE_NAME_MAX = 128;

struct TSDBDataFile
{
    WORD         m_wStat;            // �����ļ���״̬
    TBuffer      m_bfFileHeader;     // �����ļ����ļ�ͷ
    char         m_szFileName[INDEX_FILE_NAME_MAX];      // �ļ���
    DWORD        m_dwPrevBlock;      // ǰһ��������Block
    WORD         m_wPrevRecord;      // ǰһ��������Record
    WORD         m_wPrevVSize;	     // Prev var rec size
    TBuffer      m_bfBuffer;         // ���ݻ�����
    TBuffer      m_bfTmp;            // ��ʱ���ݻ�����
    TBuffer      m_bfAux;            // ��һ����ʱ���ݻ�����
};

typedef  struct TSDBDataFile * TDataFile;
typedef  struct TSDBDataFile * TDataSet;

//---------------------------------------------------------------------------
// �ṹ : TSDBFileHeader
// ��; : �����ļ�ͷ
//---------------------------------------------------------------------------
enum TEFileType { ftDBSeq = 1, ftDBRandom = 2, ftDBIndex = 3, ftDBVar = 4 };
enum TEFileStat { fsDBClosed = 0, fsDBOpen = 1, fsDBDupAllowed = 2 };

struct TSDBFileHeader
{
    char    m_szFileTag[16];        // �ļ���"BONSON IDX FILE "
    char    m_cCPUOrder;            // I : Intel; M : Motorola
    WORD    m_wDBVersion;           // ���ݿ�İ汾��
    WORD    m_wFileType;            // �ļ����ͣ����Ӧ��Ϊö������
    WORD    m_wFileStat;            // �ļ�״̬�����Ӧ��Ϊö������
    DWORD   m_dwLastBlock;          // ���һ�η����Block
    WORD    m_wBlockSize;           // Block���ֽ���
    WORD    m_wRecordSize;          // ��¼�ֽ���
    WORD    m_wControlSize;         // һ����¼�еĿ��Ʋ���(ָ����ָ��)��С
    WORD    m_wDataSize;            // ����û����ݵĲ��ֵĴ�С
    DWORD   m_dwRecordCount;        // ��¼��
    WORD    m_wRecordsPerBlock;     // ÿBlock�ļ�¼��
    DWORD   m_dwRootPointer;        // ����Ŀ��
    WORD    m_wBaseSize;            // Random File Base Size
    WORD    m_wKeySize;             // Key���ֽ���
    DWORD   m_dwNextAvail;          // ��һ����Ч��¼
    WORD    m_wAttrSize;            // Size of Attribute Block
    DWORD   m_dwVFileSize;		    // Size of VAR file data
    WORD    m_wOwnerLinkCount;		// Owner link count
    WORD    m_wMemberLinkCount;		// Member link count
    WORD    m_wKeyLinkCount;	    // Key link count
    char    m_szOtherInfo[128];      // �������������Ϣ
};

typedef  struct TSDBFileHeader *TFileHeader;

//---------------------------------------------------------------------------
// �ṹ : TSDBFreeRecord
// ��; :
//---------------------------------------------------------------------------
enum TEFreeStat { fsFree = '0', fsInUse = '1' };

struct TSDBFreeRecord
{
    char  m_cStat;    // ״̬
    DWORD m_dwNext;   // ��һ����Ч���
};

typedef struct TSDBFreeRecord * TFreeRecord;

//---------------------------------------------------------------------------
// �ṹ : TSDBIndexHeader
// ��; : ����ͷ
//---------------------------------------------------------------------------
struct TSDBIndexHeader
{
    char  m_cStat;           // ״̬
    DWORD m_dwParent;        // �����
    WORD  m_wRecCount;       // �������еļ�¼��
};

typedef struct TSDBIndexHeader * TIndexHeader;

//---------------------------------------------------------------------------
// �ṹ : TSDBIndexRecord
// ��; : ������¼
//---------------------------------------------------------------------------
struct TSDBIndexRecord
{
    DWORD dwIndexPointer;             // ָ��������
};

typedef struct TSDBIndexRecord * TIndexRecord;

//---------------------------------------------------------------------------
// �ṹ : TSDBOwnerLink
// ��; :
//---------------------------------------------------------------------------
struct TSDBOwnerLink
{
    long m_nFirst;      // rec # of first member
    long m_nLast;       // rec # of last member
};

typedef struct TSDBOwnerLink * TOLink;

//---------------------------------------------------------------------------
// �ṹ : TSDBMemberLink
// ��; :
//---------------------------------------------------------------------------
struct TSDBMemberLink
{
    long m_nPrev;       // rec # of prev member
    long m_nNext;       // rec # of next member
    long m_nOwner;      // rec # of owner
};

typedef struct TSDBMemberLink * TMLink;

//---------------------------------------------------------------------------
// �ṹ : TSDBKeyLink
// ��; :
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
// ��   : TCDBIndex
// ��; : �������ݿ���
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
// ���� : TCDBIndex::BUFRefreshCount
// ��; : �õ����µ�ˢ�¼���
// ԭ�� : inline long BUFRefreshCount();
// ���� : ��
// ���� : ˢ�¼�¼
// ˵�� : 1. ��ˢ�¼���ά�����ݵ��¾ɣ�����ԽСԽ�ɡ�
//        2. ���ˢ�¼�������һ����ֵ����ˢ�¼������������е�ˢ�¼���Ҫ���١�
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

