//---------------------------------------------------------------------------
#ifdef __WIN32__
#ifdef __TEST__
#include <windows.h>
#endif
#endif

#include "cmpublic.h"

#pragma hdrstop

#include "c_db_index.h"

#pragma warn -8071
//---------------------------------------------------------------------------

bool TCDBIndex::s_m_bBufferLazyWrite = true;
bool TCDBIndex::s_m_bDBAddThrowException = true;

// �����ļ��ļ�ͷ�Ĵ�С
const long DB_FILE_HDR_SIZE = 4096;

const TCString INDEX_FILE_TAG = "BONSON IDX FILE ";

//==========================================================================
// ���� : TCDBIndex::TCDBIndex
// ��; : ���캯��
// ԭ�� : TCDBIndex::TCDBIndex()
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
TCDBIndex::TCDBIndex()
{
    m_df = NULL;

    m_dwDBAddBlock = 0;
    m_wDBAddRecord = 0;

    m_dwDBMatchBlock = 0;
    m_wDBMatchRecord = 0;

    memset(m_BufferPool, 0, sizeof(m_BufferPool));

    m_bOpenWithReadOnly = false;
}

//==========================================================================
// ���� : TCDBIndex::~TCDBIndex
// ��; : ��������
// ԭ�� : TCDBIndex::~TCDBIndex()
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
TCDBIndex::~TCDBIndex()
{
    if (m_df != NULL)
        DBClose();
}

//==========================================================================
// ���� : TCDBIndex::BUFCopyBuffer
// ��; : ����Buffer
// ԭ�� : void BUFCopyBuffer(TBuffer bfDest, TBuffer bfSrc);
// ���� : Ŀ��Bufferָ�룬ԴBufferָ��
// ���� : ��
// ˵�� : �ú�����������Buffer�����ݣ�������Buffer���ݵ����ݡ�
//==========================================================================
void TCDBIndex::BUFCopyBuffer(TBuffer bfDest, TBuffer bfSrc)
{
    memcpy(bfDest, bfSrc, sizeof(TSDBDataBuf) - sizeof(char *));
    memcpy(bfDest->m_pBufData, bfSrc->m_pBufData, bfSrc->m_wBufSize);
}

//==========================================================================
// ���� : TCDBIndex::BUFFetch
// ��; : ��һ��Ļ��������Ƶ�ָ����Buffer֮��
// ԭ�� : bool BUFFetch(TBuffer buf, DWORD dwBlock);
// ���� : Ŀ��Bufferָ�룬���
// ���� : ����ڻ�����ҵ������и��ƣ�����true; û�ҵ���Ӧ�飬����false��
// ˵�� : �ú�����������Buffer�����ݣ�������Buffer���ݵ����ݡ�
//==========================================================================
bool TCDBIndex::BUFFetch(TBuffer buf, DWORD dwBlock)
{
    long nIndex;

    nIndex = BUFGetIndex(dwBlock);

    if (nIndex == -1)
        return false;

    if (dwBlock == 0)
        if (m_BufferPool[nIndex].m_nRefreshCount == 0)
            return false;

    BUFCopyBuffer(buf, m_BufferPool[nIndex].m_bfBuffer);
    m_BufferPool[nIndex].m_nRefreshCount = BUFRefreshCount();
    return true;
}

//==========================================================================
// ���� : TCDBIndex::BUFGetIndex
// ��; : �õ�������е�ָ����Ŷ�Ӧ��Index
// ԭ�� : long BUFGetIndex(DWORD dwBlock);
// ���� : ���
// ���� : ָ����Ŷ�Ӧ��Index, �������û�У��򷵻�-1
// ˵�� :
//==========================================================================
long TCDBIndex::BUFGetIndex(DWORD dwBlock)
{
    long i;

    if (dwBlock == 0)
    {
        return 0;
    }

    for (i = 1; i < BUFFER_POOL_MAX; i++)
        if (m_BufferPool[i].m_nRefreshCount != 0)
            if (m_BufferPool[i].m_bfBuffer->m_dwBufCurrentBlock == dwBlock)
                return i;

    return -1;
}

//==========================================================================
// ���� : TCDBIndex::BUFOldestIndex
// ��; : ����BufferPool�����ϵ�һ������Index
// ԭ�� : long BUFOldestIndex();
// ���� : ��
// ���� : ���ϵ�����Index
// ˵�� : 1. �����ĳ������δ�����ڴ棬��ú�����������ڴ棬�����ظ�
//           ��������Index��
//        2. ��m_nRefreshCountά�����ݵ��¾ɣ�����ԽСԽ�ɡ�
//        3. IndexΪ0�̶�Ϊ�ļ�ͷ��
//==========================================================================
long TCDBIndex::BUFOldestIndex()
{
    long i;
    long nSmallestRefreshCount;
    long nSmallestIndex;

    ASSERT(BUFFER_POOL_MAX >= 3);

    nSmallestRefreshCount = -1;

    //======= 1. ѭ��ɨ��ÿһ�������� ========
    for (i = 1; i < BUFFER_POOL_MAX; i++)
    {
        //==== 2. ����û��廹δ��ʹ�ã��������ڴ棬�����ظ�Index ======
        if (m_BufferPool[i].m_nRefreshCount == 0)
        {
            TFileHeader fhFileHeader;

            fhFileHeader = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
            m_BufferPool[i].m_bfBuffer = DBAllocBuf(fhFileHeader->m_wBlockSize
                    + 2 * fhFileHeader->m_wRecordSize);
            m_BufferPool[i].m_nRefreshCount = BUFRefreshCount();
            return i;
        }

        //====== 3. �Ƚϵõ���ɵĻ�����Index =====
        if (nSmallestRefreshCount == -1)
        {
            nSmallestRefreshCount = m_BufferPool[i].m_nRefreshCount;
            nSmallestIndex = i;
        }
        else if (nSmallestRefreshCount > m_BufferPool[i].m_nRefreshCount)
        {
            nSmallestRefreshCount = m_BufferPool[i].m_nRefreshCount;
            nSmallestIndex = i;
        }
    }

    if (m_BufferPool[nSmallestIndex].m_bUpdated)
    {
        BUFWriteAllUpdated();
    }
    return nSmallestIndex;
}

//==========================================================================
// ���� : TCDBIndex::BUFRefresh
// ��; : ���»��������еļ�¼
// ԭ�� : void BUFRefresh(TBuffer buf);
// ���� : ������ָ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCDBIndex::BUFRefresh(TBuffer buf, bool bUpdate)
{
    long nIndex;
    nIndex = BUFGetIndex(buf->m_dwBufCurrentBlock);
    if (nIndex == -1)
    {
        nIndex = BUFOldestIndex();
    }
    else
    {
        m_BufferPool[nIndex].m_nRefreshCount = BUFRefreshCount();
    }
    BUFCopyBuffer(m_BufferPool[nIndex].m_bfBuffer, buf);
    if (s_m_bBufferLazyWrite)
        m_BufferPool[nIndex].m_bUpdated = bUpdate;
    else
    {
        m_BufferPool[nIndex].m_bUpdated = false;
        if (bUpdate)
        {
            DBPutBlockToFile(buf);
        }
    }
}

//==========================================================================
// ���� : TCDBIndex::BUFRelease
// ��; : �ͷŻ�������������Buffer
// ԭ�� : void TCDBIndex::BUFRelease();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCDBIndex::BUFRelease()
{
    BUFWriteAllUpdated();

    long i;

    for (i = 0; i < BUFFER_POOL_MAX; i++)
    {
        if (m_BufferPool[i].m_nRefreshCount == 0)
            continue;
        else
        {
            DBFreeBuffer(m_BufferPool[i].m_bfBuffer);
            m_BufferPool[i].m_nRefreshCount = 0;
        }
    }
}

//==========================================================================
// ���� : TCDBIndex::BUFWriteAllUpdated
// ��; : �����и��¹��Ŀ鶼д���ļ�
// ԭ�� : void TCDBIndex::BUFWriteAllUpdated();
// ���� : ��
// ���� : ��
// ˵�� : 1. �ú����������ͻ��������ʱ����
//        2. �ڻ��������ʱ��ʵ����Ҳ����ֻ��������Ļ���������Ϊ�˱�֤��
//           ���������������Ե��øú�����д�ļ���
//==========================================================================
void TCDBIndex::BUFWriteAllUpdated()
{
    long i;

    for (i = 0; i < BUFFER_POOL_MAX; i++)
    {
        if (m_BufferPool[i].m_nRefreshCount == 0)
            continue;
        else
            if (m_BufferPool[i].m_bUpdated)
            {
                m_BufferPool[i].m_bUpdated = false;
                DBPutBlockToFile(m_BufferPool[i].m_bfBuffer);
            }
    }
}

//==========================================================================
// ���� : TCDBIndex::DBAdd
// ��; : ����һ���û�����
// ԭ�� : void DBAdd(char *szUserData)
// ���� : �û�����
// ���� : ��
// ˵�� :
//==========================================================================
TEDBAddResult TCDBIndex::DBAdd(char *szUserData)
{
    TBuffer buf;
    DBCheckDataFile();
    buf = (TBuffer)m_df->m_bfBuffer;

    TEDBAddResult arRet;
    arRet = DBAddIDX(szUserData);
    buf->m_dwBufCurrentBlock = 0;
    buf->m_wBufRecordIndex = 0;

    return arRet;
}

//==========================================================================
// ���� : TCDBIndex::DBAddIDX
// ��; : ����һ���û�����
// ԭ�� : void DBAddIDX(char *szUserData)
// ���� : �û�����
// ���� : ��
// ˵�� : 
//==========================================================================
TEDBAddResult TCDBIndex::DBAddIDX(char *szUserData)
{
    //==== 0. ��Ҫ�ĳ�ʼ������ =====
    TFileHeader fh;
    TIndexHeader ihIndexHeader;
    TBuffer buf;
    char *szRBuf;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;//�ļ�ͷ����
    buf = m_df->m_bfBuffer;//������������
    ihIndexHeader = (TIndexHeader)buf->m_pBufData;//����ͷ����
    szRBuf = buf->m_pBufData + sizeof(TSDBIndexHeader);//ָ������ͷ֮��ĵ�һ�����ݲ����

    //====== 2. ���û�и��ڵ㣬��ȡ����һ�����ýڵ� =====
    if (fh->m_dwRootPointer == 0)
    {
        DBGetNextAvail(buf);
    }

    //====== 3. ��������������� =====
    else
    {
        DBFindInsertIDX(szUserData, fh->m_wKeySize);
        //===== 4. �������ȫƥ���Keyֵ�����ļ����ֲ������ظ��������׳����� ===
        if (m_dwDBMatchBlock != 0)
            if ((fh->m_wFileStat & fsDBDupAllowed) == 0)
            {
                if (s_m_bDBAddThrowException)
                    throw TCException("DBIndex Dup Not Allowed");
                else
                    return arDuplicateKey;
            }
    }

    //======= 5. ���еļ�¼����1, ������ӵĿ�ż���¼�� ===
    WORD wRecord;

    ihIndexHeader->m_wRecCount ++;
    wRecord = buf->m_wBufRecordIndex;

    m_dwDBAddBlock = buf->m_dwBufCurrentBlock;
    m_wDBAddRecord = buf->m_wBufRecordIndex;

    //===== 6. ����������ļ�¼�����ȿ��еļ�¼��������ƫ��һ����¼ =======
    char *szSrc, *szDest;
    WORD wCount;

    if (wRecord <= ihIndexHeader->m_wRecCount)
    {   szSrc = szRBuf + (wRecord - 1) * fh->m_wRecordSize;
        szDest = szSrc + fh->m_wRecordSize;
        wCount = (ihIndexHeader->m_wRecCount - wRecord + 1) * fh->m_wRecordSize;
        // �˴�ԭ���õ���memcpy, ���������ص�����Ϊmemmove
        memmove(szDest, szSrc, wCount);
    }

    //======= 7. ����¼ֵ(�ӽڵ�Ϊ0, �����û�����) ====
//    TIndexRecord irIndexRecord;

//    irIndexRecord = (TIndexRecord)szSrc;
//    irIndexRecord->dwIndexPointer = 0;
//    szDest = (char *)irIndexRecord + fh->m_wControlSize;
    memset(szSrc, 0, sizeof(TSDBIndexRecord));
    szDest = szSrc + fh->m_wControlSize;

    memcpy(szDest, szUserData, fh->m_wDataSize);

    //======= 8. ��¼����1, �ж��Ƿ����ҳ�棬д���ļ�ͷ =====
    fh->m_dwRecordCount ++;
    DBSplitBlockIDX();
    DBPutBlock(m_df->m_bfFileHeader);
    return arSucceed;
}

//==========================================================================
// ���� : TCDBIndex::DBAllocBuf
// ��; : ����BUFFER�ռ�
// ԭ�� : TBuffer DBAllocBuf(long nSize);
// ���� : Buffer��С
// ���� : Bufferָ��
// ˵�� : �ú�������Buffer�Ŀռ䡣���ָ����С�����ٷ������ݡ�
//==========================================================================
TBuffer TCDBIndex::DBAllocBuf(long nSize)
{
    //========= 1. ����Buffer�ռ� ==========
    TBuffer  buf;
    char   *calloc();

    buf = new TSDBDataBuf;
    memset(buf, 0, sizeof(TSDBDataBuf));

    if (buf == NULL)
        throw TCException("DBIndex Alloc Error");

    //========= 2. ���ָ����Size��Ϊ0�������m_pBufData��Ա���� =======
    if (nSize != 0)
    {
        ASSERT(nSize <= 65535);

        // ָ����Size����m_wBufSize��        
        buf->m_wBufSize = nSize;
        buf->m_pBufData = new char [nSize];
        memset(buf->m_pBufData, 0, nSize);

        if (buf->m_pBufData == NULL)
        {   delete buf;
            throw TCException("DBIndex Alloc Error");
        }
    }

    return buf;
}

//==========================================================================
// ���� : TCDBIndex::DBCheckDataFile
// ��; : �ж��Ƿ���Ч��DataFile
// ԭ�� : void TCDBIndex::DBCheckDataFile();
// ���� : DataFile
// ���� : ��
// ˵�� : ������֤Datafile�������Ա��������Ч�ԡ�����ʱDataFile���Ѵ�״̬��
//==========================================================================
void TCDBIndex::DBCheckDataFile()
{
    TFileHeader fh;

    //====== 1. dfΪ���� ==========
    ASSERT(m_df != NULL);

    //===== 2. ״̬Ϊ�Ѵ� ==========
    ASSERT((m_df->m_wStat & fsDBOpen) != 0);

    //====== 3. �ļ�ͷ��Ϊ�գ���������Ϊ�� =======

    ASSERT((m_df->m_bfFileHeader != NULL) && (m_df->m_bfBuffer != NULL));

    //====== 4. �ļ�����ΪIndexFile ========
    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;

	if ( fh->m_wFileType != ftDBIndex )
		printf(" DBFileHeaderOpen:The File is :%s\n",(char*)m_fFile.GetFileName());
	
    ASSERT(fh->m_wFileType == ftDBIndex);

    //====== 5. TmpBuffer��AuxBuffer������Ϊ�� ====
    ASSERT((m_df->m_bfTmp != NULL) && (m_df->m_bfAux != NULL));
}

//==========================================================================
// ���� : TCDBIndex::DBClose
// ��; : �ر������ļ����ͷ�����ڴ�
// ԭ�� : void DBClose();
// ���� : DataFile
// ���� : ��
// ˵�� : 
//==========================================================================
void TCDBIndex::DBClose()
{
    if (m_df == NULL)
        return;

    DBCheckDataFile();

    DBPutBlock(m_df->m_bfFileHeader);

    BUFRelease();

    m_fFile.Close();

    DBFreeDataFile();
}

//==========================================================================
// ���� : TCDBIndex::DBCreate
// ��; : �½������ļ�
// ԭ�� : TDataFile DBCreate(TCString sFileName, TCString sOptions);
// ���� : �ļ�����ѡ���ַ���
// ���� : �����ļ��ṹָ��
// ˵�� :
//==========================================================================
void TCDBIndex::DBCreate(TCString sFileName, TCString sOptions,
        TCString sOtherInfo)
{
    m_BufferPool[0].m_bfBuffer = DBAllocBuf(DB_FILE_HDR_SIZE);

    //======== 1. ����DataFile���ڴ� =======
    m_df = new TSDBDataFile;
    memset(m_df, 0, sizeof(TSDBDataFile));
    if (m_df == NULL)
        throw TCException("DBIndex Alloc Error");

    //====== 2. �����ļ�ͷ���ڴ� ========
    TFileHeader fhFileHeader;
//%%%    m_df->m_bfFileHeader = DBAllocBuf(DB_FILE_HDR_SIZE);  //COMMENT20010601
    m_df->m_bfFileHeader = m_BufferPool[0].m_bfBuffer;         //%%%ADD20010601

    //====== 3. ��������ڴ�ͬʱ����fhFileHeader�������Ժ�ķ��� ======
    fhFileHeader = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;

    //===== 4. �����ļ�ͷ�ĵ�ǰ��������СΪDB_FILE_HDR_SIZE ======
    m_df->m_bfFileHeader->m_wBufCurrentSize = DB_FILE_HDR_SIZE;

    //===== 5. ����DataFile���ļ����� =====
    ASSERT(Length(sFileName) < INDEX_FILE_NAME_MAX);
    strcpy(m_df->m_szFileName, (char *)sFileName);

    //====== 6. ���������ļ�ѡ�� ========
    DBFileOptions(fhFileHeader, sOptions);

    //====== 7. ��дfhFileHeader������������� ======
    DBFileHeaderCreate(fhFileHeader);

    FillPadr(fhFileHeader->m_szOtherInfo, sOtherInfo,
            sizeof(fhFileHeader->m_szOtherInfo));

    //====== 8. ���ļ� ===========
    m_fFile.Open(m_df->m_szFileName, omWrite);

    //======== 9. �����ļ���״̬ΪDBOpen =====
    m_df->m_wStat = fsDBOpen;

    //===== 10. ����Buffer, Tmp, Aux�ڴ� ==========
    m_df->m_bfBuffer = DBAllocBuf(fhFileHeader->m_wBlockSize
            + 2 * fhFileHeader->m_wRecordSize);

    m_df->m_bfTmp = DBAllocBuf(fhFileHeader->m_wBlockSize
            + 2 * fhFileHeader->m_wRecordSize);

    m_df->m_bfAux = DBAllocBuf(fhFileHeader->m_wBlockSize
            + 2 * fhFileHeader->m_wRecordSize);

    // ��������ڴ��С��ΪBlockSize��2����RecordSize, ��BufSize��Ա
    // �����������BlockSize��
    m_df->m_bfBuffer->m_wBufSize = fhFileHeader->m_wBlockSize;
    m_df->m_bfTmp->m_wBufSize = fhFileHeader->m_wBlockSize;
    m_df->m_bfAux->m_wBufSize = fhFileHeader->m_wBlockSize;

    //====== 11. д��DataFile�ļ� =========
    // m_wBufCurrentBlock = 0, д���ļ�ͷ
    DBPutBlock(m_df->m_bfFileHeader);

}

//==========================================================================
// ���� : TCDBIndex::DBExtend
// ��; : ��չ�ļ��洢�ռ�
// ԭ�� : void DBExtend(TBuffer buf);
// ���� : �����ļ�ָ��, Buffer
// ���� : ��
// ˵�� :
//==========================================================================
void TCDBIndex::DBExtend(TBuffer buf)
{
    TFileHeader fh;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;

    //===== 1. �����+1 ======
    fh->m_dwLastBlock ++;

    //==== 2. Buffer�еĵ�ǰ��=�����
    buf->m_dwBufCurrentBlock = fh->m_dwLastBlock;

    //====== 3. Buffer�ĵ�ǰ��С = ���С ======
    buf->m_wBufCurrentSize = fh->m_wBlockSize;

    //======= 4. ���Buffer�е����� ======
    memset(buf->m_pBufData, 0, buf->m_wBufCurrentSize);

    //===== 5. ��һ����Ч��Block��Ϊ��ǰBlock ====
    TFreeRecord frFreeRecord;
    frFreeRecord = (TFreeRecord)buf->m_pBufData;
    frFreeRecord->m_cStat = fsFree;
    frFreeRecord->m_dwNext = fh->m_dwNextAvail;
    fh->m_dwNextAvail = buf->m_dwBufCurrentBlock;

    //==== 6. д��Buffer, д���ļ�ͷ ======
    DBPutBlock(m_df->m_bfBuffer);
    DBPutBlock(m_df->m_bfFileHeader);
}

//==========================================================================
// ���� : TCDBIndex::DBFileHeaderCreate
// ��; : ����ļ�ͷTFileHeader�ṹ��������Ӧ��ֵ
// ԭ�� : void DBFileHeaderCreate(TFileHeader fh);
// ���� : �ļ�ͷ
// ���� : ��
// ˵�� :
//==========================================================================
void TCDBIndex::DBFileHeaderCreate(TFileHeader fh)
{
    long nHeaderSize;

    FillPadr(fh->m_szFileTag, INDEX_FILE_TAG, sizeof(fh->m_szFileTag));
    if (TCSystem::ThisMachineByteOrder() == boIntel)
        fh->m_cCPUOrder = 'I';
    else
        fh->m_cCPUOrder = 'M';

    //===== 1. ���汾�Ÿ�ֵ ========
    fh->m_wDBVersion = DB_VERSION;

    ASSERT(fh->m_wFileType == ftDBIndex);

    //==== 2. ����ļ��򿪱�־ ========
    fh->m_wFileStat &= ~fsDBOpen;
    fh->m_dwLastBlock = 0;

    //====== 3. ��BlockSize��ControlSize��ֵ���򸳸�ȱʡֵ =======
    if (fh->m_wBlockSize == 0)
        fh->m_wBlockSize = 512;

    if (fh->m_wControlSize == 0)
        fh->m_wControlSize = 4;

    //===== 4. ����ͷ��С = TSDBIndexHeader�Ĵ�С + m_wControlSize;
    // Ϊ���ڼ��㣬�˴�������ͷ��Сʵ���ϵ�������ͷ��С + β���Ŀ�ָ��
    nHeaderSize = sizeof(TSDBIndexHeader) + fh->m_wControlSize;

    //==== 5. ControlSize += LinksCnt * LinksSize
    fh->m_wControlSize = fh->m_wControlSize
            + fh->m_wOwnerLinkCount * sizeof(TSDBOwnerLink)
            + fh->m_wMemberLinkCount * sizeof(TSDBMemberLink)
            + fh->m_wKeyLinkCount * sizeof(TSDBKeyLink);

    //===== 6. ��¼�Ĵ�С = ControlSize + DataSize ========
    fh->m_wRecordSize = fh->m_wControlSize + fh->m_wDataSize;

    //==== 7. ����Blocksize����Ϊ�����ļ�¼��С+����ͷ��С����Ϊ512�ı��� ====
    if (fh->m_wBlockSize < 2 * fh->m_wRecordSize + nHeaderSize)
        fh->m_wBlockSize = 2 * fh->m_wRecordSize + nHeaderSize;

    fh->m_wBlockSize = (fh->m_wBlockSize + 511) / 512 * 512;

    //===== 8. ��¼��=0; ���ÿ���¼��; RootPointer=0; NextAvail=0; ====
    fh->m_dwRecordCount = 0;
    fh->m_wRecordsPerBlock = (fh->m_wBlockSize - nHeaderSize)
            / fh->m_wRecordSize;
    fh->m_dwRootPointer = 0;
    fh->m_dwNextAvail = 0;

    ASSERT(fh->m_wKeySize <= fh->m_wDataSize);
}

//==========================================================================
// ���� : TCDBIndex::DBFileHeaderOpen
// ��; : ���������ļ����ļ�ͷ����Ч��
// ԭ�� : void DBFileHeaderOpen(TFileHeader fh);
// ���� : �ļ�ͷָ��
// ���� : ��
// ˵�� : 
//==========================================================================
void TCDBIndex::DBFileHeaderOpen(TFileHeader fh)
{
    TCString sFileTag;
    sFileTag = TCString(fh->m_szFileTag, sizeof(fh->m_szFileTag));
    if ( sFileTag != INDEX_FILE_TAG )
    	//printf(" DBFileHeaderOpen failed:The File is :%s,file tag is: %s\n",(char*)m_fFile.GetFileName(),sFileTag);
    ASSERT(sFileTag == INDEX_FILE_TAG);

    if (TCSystem::ThisMachineByteOrder() == boIntel)
        ASSERT(fh->m_cCPUOrder == 'I');
    else
        ASSERT(fh->m_cCPUOrder == 'M');

    if (fh->m_wDBVersion != DB_VERSION)
        throw TCException("DBIndex Version Error");

    ASSERT(fh->m_wFileType == ftDBIndex);

    ASSERT(fh->m_wBlockSize != 0 && fh->m_wRecordSize != 0
            && fh->m_wRecordsPerBlock != 0);

    ASSERT(fh->m_wRecordSize == fh->m_wControlSize + fh->m_wDataSize);

    ASSERT(fh->m_wKeySize <= fh->m_wDataSize);
}

//==========================================================================
// ���� : TCDBIndex::DBFile1Option
// ��; : ����һ���ļ�ѡ�������FileHeader�е���Ӧ����
// ԭ�� : void DBFile1Option(TFileHeader fh, TCString sOption,
//              TCString sSubOption);
// ���� : �ļ�ͷָ�룬ѡ������ѡ��ֵ
// ���� : ��
// ˵�� : �ú�����DBFileOptions()����
//==========================================================================
void TCDBIndex::DBFile1Option(TFileHeader fh, TCString sOption,
        TCString sSubOption)
{
    if (sOption == TCString("SEQ") )
    {
        fh->m_wFileType = ftDBSeq;
        return;
    }

	if (sOption == TCString("RAN") )
    {
        fh->m_wFileType = ftDBRandom;
        return;
    }

	if (sOption == TCString("IDX") )
    {
        fh->m_wFileType = ftDBIndex;
        return;
    }

    if (sOption == TCString("VAR") )
    {
        fh->m_wFileType = ftDBVar;
        return;
    }

    if (sOption == TCString("BLK") )
    {
        fh->m_wBlockSize = StrToInt(sSubOption);
        return;
    }

    if (sOption == TCString("REC") )
    {
        fh->m_wDataSize = StrToInt(sSubOption);
        return;
    }

    if (sOption == TCString("BASE") )
    {
        fh->m_wBaseSize = StrToInt(sSubOption);
        return;
    }

	if (sOption == TCString("KEY") )
    {
        fh->m_wKeySize = StrToInt(sSubOption);
        return;
    }

    if (sOption == TCString("ATR") )
    {
        fh->m_wAttrSize = StrToInt(sSubOption);
        return;
    }

    if (sOption == TCString("MLINKS") )
    {
        fh->m_wMemberLinkCount = StrToInt(sSubOption);
        return;
    }

    if (sOption == TCString("OLINKS") )
    {
        fh->m_wOwnerLinkCount = StrToInt(sSubOption);
        return;
    }

    if (sOption == TCString("KLINKS") )
    {
        fh->m_wKeyLinkCount = StrToInt(sSubOption);
        return;
    }

    if (sOption == TCString("UNIQUE") )
    {
        fh->m_wFileStat &= ~fsDBDupAllowed;
        return;
    }

	if (sOption == TCString("DUPLICATE") )
    {
		fh->m_wFileStat |= fsDBDupAllowed;
        return;
    }

    throw TCException("DB Index : Invalid Option");
}

//==========================================================================
// ���� : TCDBIndex::DBFileOptions
// ��; : ����DB�ļ���ѡ���ȷ��ǡ���Ĵ�����ʽ����������
// ԭ�� : void DBFileOptions(TFileHeader fh, TCString sOptionStr);
// ���� : �ļ�ͷָ�룬ѡ�
// ���� : ��
// ˵�� : �ú��������������ĸ�ѡ������TFileHeader����Ӧ��Ϣ��
//==========================================================================
void TCDBIndex::DBFileOptions(TFileHeader fh, TCString sOptionStr)
{
    long i;
    TCString sOption, sSubOption;

    TCStringList slOptionList;
    slOptionList.CommaText(sOptionStr);

    for (i = 0; i < slOptionList.GetCount(); i++)
    {
        sOption = UpperCase(slOptionList.GetName(i));
        sSubOption = slOptionList.GetValue(i);

        DBFile1Option(fh, sOption, sSubOption);
    }
}

//==========================================================================
// ���� : TCDBIndex::DBFind
// ��; : ����Index�еĹؼ���
// ԭ�� : bool DBFind(char *szUserData, char *szKey,
//              long nKeySize = 0);
// ���� : �����ļ�ָ�룬�û����ݣ��ؼ��֣��ؼ��ֵĴ�С
// ���� : �Ƿ�������
// ˵�� : �������������DataFile�еĳ�Ա����m_dwPrevBlock, m_wPrevRecord
//        ������������ֵ��������������Ա�������㡣
// ��ʷ : 2001.12.3 �޸�Ϊif (!bFindIt) ���˸�"!"
//==========================================================================
bool TCDBIndex::DBFind(char *szUserData, char *szKey,
        long nKeySize)
{
    bool bFindIt;

    TFileHeader fh;
    TBuffer buf;

    DBCheckDataFile();

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = (TBuffer)m_df->m_bfBuffer;

    if (fh->m_dwRecordCount == 0)
        return false;

    if (nKeySize == 0)
        nKeySize = fh->m_wKeySize;

    bFindIt = DBFindFirstIDX(szUserData, szKey, nKeySize);

    if (!bFindIt)        // ԭ����if (bFindIt) 2001.12.03��Ϊ���ڵı��
    {   buf->m_dwBufCurrentBlock = 0;
        buf->m_wBufRecordIndex = 0;
    }

    m_df->m_dwPrevBlock = buf->m_dwBufCurrentBlock;
    m_df->m_wPrevRecord = buf->m_wBufRecordIndex;

    return bFindIt;
}

//==========================================================================
// ���� : TCDBIndex::DBFindFirstIDX
// ��; : ����Index�еĹؼ���
// ԭ�� : bool DBFindFirstIDX(char *szUserData, char *szKey,
//              long nKeySize);
// ���� : �����ļ�ָ�룬�û����ݣ��ؼ��֣��ؼ��ֵĴ�С
// ���� : �Ƿ�������
// ˵�� : ���δ�ѵ����򷵻�false�����򷵻�true, ����UserData��������Ӧ��
//        ֵ��UserDataӦ���ȷ����㹻���ڴ档
//==========================================================================
bool TCDBIndex::DBFindFirstIDX(char *szUserData, char *szKey,
        long nKeySize)
{
    TFileHeader fh;
    TBuffer buf;
    char *szRBuf;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;

    DBFindInsertIDX(szKey, nKeySize);

    if (m_dwDBMatchBlock == 0)
        return false;

    DBGetBlock(m_dwDBMatchBlock, buf);

    buf->m_wBufRecordIndex = m_wDBMatchRecord;

    szRBuf = buf->m_pBufData + sizeof(struct TSDBIndexHeader)
            + (buf->m_wBufRecordIndex - 1) * fh->m_wRecordSize
            + fh->m_wControlSize;

    memcpy(szUserData, szRBuf, fh->m_wDataSize);

    return true;
}

//==========================================================================
// ���� : TCDBIndex::DBFindInsertIDX
// ��; : ������ָ���ؼ��ֵĲ����
// ԭ�� : void DBFindInsertIDX(char *szKey, long nKeySize);
// ���� : �����ļ�ָ�룬�ؼ��֣��ؼ��ֵĴ�С
// ���� : ��
// ˵�� : �����������¼�����¼��m_dwDBMatchBlock��m_wDBMatchRecord�С�
//        ����buf�ĳ�Ա����m_dwBufCurrentBlock��m_dwBufRecordIndex�м�¼
//        ��ǰ�Ĳ����λ�á�
//==========================================================================
void TCDBIndex::DBFindInsertIDX(char *szKey, long nKeySize)
{
    TFileHeader fh;
    TIndexHeader ihIndexHeader;
//    TIndexRecord irIndexRecord;
    TSDBIndexRecord irDBIndexRecord;
    TBuffer buf;
    char *szRBuf, *szIKey;
    DWORD dwBlock;
    WORD wRecord;
    long x;

    //====== 1. ���ҵ���Block��Record����Ϊ0 ========
    m_dwDBMatchBlock = 0;
    m_wDBMatchRecord = 0;

    //===== 2. ��ʼ��������������Block��ֵ����dwBlock =====
    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;
    ihIndexHeader = (TIndexHeader)buf->m_pBufData;
    dwBlock = fh->m_dwRootPointer;

    if (nKeySize == 0)
        nKeySize = fh->m_wKeySize;

    // ========== 3. ѭ�����в��� =========
    while (dwBlock != 0)
    {
        //====== 4. ��Block����������buf =======
        DBGetBlock(dwBlock, buf);

        //==== 5. ��ü�¼���ݵ�ʵ�ʿ�ʼλ�� ======
        szRBuf = buf->m_pBufData + sizeof(struct TSDBIndexHeader);

        //======= 6. �������Block�еĸ���¼���ݲ����бȽ� =====
        for (wRecord = 1; wRecord <= ihIndexHeader->m_wRecCount; wRecord++)
        {
            //===== 7. ȡ��IndexRecord(Block) ======
//            irIndexRecord = (TIndexRecord)szRBuf;     //0918
            memcpy(&irDBIndexRecord, szRBuf, sizeof(TSDBIndexRecord));

            //===== 8. IndexKeyֵ��Ҫ����ControlSize ====
            szIKey = szRBuf + fh->m_wControlSize;

            //====== 9. �Դ����Keyֵ�뵱ǰKeyֵ��Ƚ� =====
            x = memcmp(szKey, szIKey, nKeySize);

            //===== 10. �����ͬ����ֵƥ����ĵ�ǰֵ, �ٽ������� ======
            if (x == 0)
            {
                m_dwDBMatchBlock = dwBlock;
                m_wDBMatchRecord = wRecord;

//                dwBlock = irIndexRecord->dwIndexPointer;      //0918
                dwBlock = irDBIndexRecord.dwIndexPointer;
                break;
            }

            //======= 11. ���Ҫ������Keyֵ��С���򴫳�����ٽ������� ===
            if (x < 0)
            {
//                dwBlock = irIndexRecord->dwIndexPointer;      //0918
                dwBlock = irDBIndexRecord.dwIndexPointer;
                break;
            }

            //====== 12. ���Ҫ������Keyֵ���ǽϴ���ƫ�Ƶ���һ��Record =====
            szRBuf += fh->m_wRecordSize;

            //===== 13. ������������һ����¼����������һ���ڵ�
            if (wRecord == ihIndexHeader->m_wRecCount)
            {
//                irIndexRecord = (TIndexRecord)szRBuf;     //0918
//                dwBlock = irIndexRecord->dwIndexPointer;
                memcpy(&irDBIndexRecord, szRBuf, sizeof(TSDBIndexRecord));
                dwBlock = irDBIndexRecord.dwIndexPointer;
            }
        }   // end of for (wRecord = 1; ...
    }   // end of while (dwBlock != 0)

    //== 14. ��Buffer�м��µ�ǰ��Record(��ǰ��Buffer����DBGetBlock�м�¼) ===
    buf->m_wBufRecordIndex = wRecord;
}

//==========================================================================
// ���� : TCDBIndex::DBFreeBuffer
// ��; : �ͷ�Buffer
// ԭ�� : TBuffer DBFreeBuffer(TBuffer buf);
// ���� : Ҫ�ͷŵ�Buffer
// ���� : ��һ��Ҫ�ͷŵ�Buffer
// ˵�� : �ͷ�Buffer, ����˫��������������һ��Ҫ�ͷŵ�Buffer��
// ���÷�ʽ���� :
//          while (bfBuffer != NULL)
//              bfBuffer = DBFreeBuffer(bfBuffer);
//==========================================================================
TBuffer TCDBIndex::DBFreeBuffer(TBuffer buf)
{
    TBuffer bfPrev, bfNext;

    if (buf == NULL)
        return NULL;

    bfPrev = buf->m_pBufPrev;
    bfNext = buf->m_pBufNext;

    if (bfPrev != NULL)
        bfPrev->m_pBufNext = buf->m_pBufNext;

    if (bfNext != NULL)
        bfNext->m_pBufPrev = buf->m_pBufPrev;

    if (buf->m_pBufData != NULL)
        delete [] buf->m_pBufData;

    delete buf;

    return bfNext;
}

//==========================================================================
// ���� : TCDBIndex::DBFreeDataFile
// ��; : �ͷ�DataFile�������ڴ�
// ԭ�� : void DBFreeDataFile();
// ���� : Ҫ�ͷŵ�DataFile
// ���� : ��
// ˵�� : �ú�����DBClose()����
//==========================================================================
void TCDBIndex::DBFreeDataFile()
{
    if (m_df == NULL)
        return;

//%%%    if (m_df->m_bfFileHeader != NULL)      //COMMENT20010601
//%%%        delete m_df->m_bfFileHeader;       //COMMENT20010601

    while (m_df->m_bfBuffer != NULL)
        m_df->m_bfBuffer = DBFreeBuffer(m_df->m_bfBuffer);

    if (m_df->m_bfTmp != NULL)
        DBFreeBuffer(m_df->m_bfTmp);

    if (m_df->m_bfAux != NULL)
        DBFreeBuffer(m_df->m_bfAux);

    delete m_df;

    m_df = NULL;
}

//==========================================================================
// ���� : TCDBIndex::DBGetBlock
// ��; : ȡ�������ļ�һ��Buffer
// ԭ�� : void DBGetBlock(long nBlock, TBuffer buf);
// ���� : �����ļ�ָ�룬��ţ�������Buffer
// ���� : ��
// ˵�� :
//==========================================================================
void TCDBIndex::DBGetBlock(DWORD dwBlock, TBuffer buf)
{
    if (BUFFetch(buf, dwBlock))
    {
        return;
    }

    TFileHeader fhFileHeader;
    long nOffset;
    long nCount;

    ASSERT(m_df != NULL);
    ASSERT((m_df->m_wStat & fsDBOpen) != 0);
    ASSERT(buf != NULL);

    fhFileHeader = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;

    //===== 1. ���ƫ��������nBlockΪ0����ȡ�ļ�ͷ ======
    if (dwBlock == 0)
    {   nOffset = 0;
        nCount = buf->m_wBufSize;
    }
    else
    {   nOffset = DB_FILE_HDR_SIZE + (dwBlock - 1) * fhFileHeader->m_wBlockSize;
        nCount = buf->m_wBufSize;
    }

    //=== 2. �������ݴ���buf->m_pBufData�� ========
    buf->m_dwBufCurrentBlock = 0;
    buf->m_wBufCurrentSize = 0;
    buf->m_wBufRecordIndex = 0;

    m_fFile.Seek(nOffset);
    nCount = m_fFile.Read(buf->m_pBufData, nCount);
    if (nCount < 0)
        throw TCException("DBIndex Read Error");

    if (nCount == 0)
        throw TCException("DBIndex End Of File");

    //====== 3. ����buf�еĿ�ż���С��Ϣ ======
    buf->m_dwBufCurrentBlock = dwBlock;
    buf->m_wBufCurrentSize = nCount;
    BUFRefresh(buf);
}

//==========================================================================
// ���� : TCDBIndex::DBGetNextAvail
// ��; : �õ���һ�����ÿ�
// ԭ�� : void DBGetNextAvail(TBuffer buf);
// ���� : �����ļ�ָ��, ������
// ���� : ��
// ˵�� : ���������Ч�ռ䣬��ú�������DBExtend()������չ��
//==========================================================================
void TCDBIndex::DBGetNextAvail(TBuffer buf)
{
    TFileHeader fh;
    TFreeRecord frFreeRecord;
    TIndexHeader ihIndexHeader;
    DWORD dwBlock;
    char *szRBuf;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;

    //===== 1. ����ļ���û�п��ÿռ䣬����չ֮ ====
    if (fh->m_dwNextAvail == NULL)
        DBExtend(buf);

    //==== 2. �����¿��ÿռ� =========
    dwBlock = fh->m_dwNextAvail;

    DBGetBlock(dwBlock, buf);

    //===== 3. ��ǰ��¼Ϊ1 ========
    buf->m_wBufRecordIndex = 1;
    szRBuf = buf->m_pBufData;

    //===== 4. ���µ�ǰ���״̬Ϊ����, ������һ�����ÿռ��ֵ =======
    frFreeRecord = (TFreeRecord)szRBuf;
    if (frFreeRecord->m_cStat != fsFree)
        throw TCException("DBIndex Invalid Free (IS NOT FREE STAT)");

    frFreeRecord->m_cStat = fsInUse;
    fh->m_dwNextAvail = frFreeRecord->m_dwNext;

    //==== 5. ���û�и��飬���豾��Ϊ���� ========
    if (fh->m_dwRootPointer == 0)
        fh->m_dwRootPointer = dwBlock;

    //===== 6. �����ǰ��ĸ��ڵ����Լ���¼������ =====
    ihIndexHeader = (TIndexHeader)szRBuf;
    ihIndexHeader->m_dwParent = 0;
    ihIndexHeader->m_wRecCount = 0;
}

//==========================================================================
// ���� : TCDBIndex::DBGetOtherInfo
// ��; : �õ������ļ�������������Ϣ
// ԭ�� : TCString DBGetOtherInfo();
// ���� : ��
// ���� : ������Ϣ��
// ˵�� : ������Ϣ����DBCreate������д���ļ��ġ�
//==========================================================================
TCString TCDBIndex::DBGetOtherInfo()
{
    TFileHeader fh;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;

    return AllTrim(TCString(fh->m_szOtherInfo, sizeof(fh->m_szOtherInfo)));
}

//==========================================================================
// ���� : TCDBIndex::DBGetParentIDX
// ��; : �õ����ڵ�
// ԭ�� : bool DBGetParentIDX();
// ���� : �����ļ�ָ��
// ���� : �Ƿ��ҵ���������ط����ʾ�ѵ��ļ�β��
// ˵�� : �ҵ���ǰbuffer�ĸ��ڵ㡣����buffer�У���¼�ڵ�λ�õ�Buffer��
//        m_wBufRecordIndex��Ա�����С�
//==========================================================================
bool TCDBIndex::DBGetParentIDX()
{
    TFileHeader fh;
    TBuffer buf;
    TIndexHeader ihIndexHeader;
    TSDBIndexRecord irDBIndexRecord;
    char *szRBuf;
    DWORD dwHoldBlock;
    WORD wRecord;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;

    szRBuf = buf->m_pBufData + sizeof(TSDBIndexHeader);
    ihIndexHeader = (TIndexHeader)buf->m_pBufData;

    //======= 1. ���µ�ǰ��� =====
    dwHoldBlock = buf->m_dwBufCurrentBlock;

    //==== 2. �����ǰ��û�и��飬����false ====
    if (ihIndexHeader->m_dwParent == 0)
        return false;

    //==== 3. ���븸�� ======
    DBGetBlock(ihIndexHeader->m_dwParent, buf);

    //==== 4. ���������ÿһ���ڵ㣬ƥ���ӿ�������ָ�� ======
    for (wRecord = 1; wRecord <= ihIndexHeader->m_wRecCount + 1; wRecord++)
    {
//        irIndexRecord = (TIndexRecord)szRBuf;     //0918
//        if (irIndexRecord->dwIndexPointer == dwHoldBlock)
//            break;

        memcpy(&irDBIndexRecord, szRBuf, sizeof(TSDBIndexRecord));
        if (irDBIndexRecord.dwIndexPointer == dwHoldBlock)
            break;

        szRBuf += fh->m_wRecordSize;
    }

//    if (irIndexRecord->dwIndexPointer != dwHoldBlock)     //0918
//        throw TCException("DBIndex Invalid Index");

    if (irDBIndexRecord.dwIndexPointer != dwHoldBlock)
        throw TCException("DBIndex Invalid Index");

    //====== 5. ��buffer�е�m_wBufRecordIndex���½ڵ�� ===
    buf->m_wBufRecordIndex = wRecord;

    return true;
}

//==========================================================================
// ���� : TCDBIndex::DBOpen
// ��; : �����ݿ������ļ�
// ԭ�� : TDataFile DBOpen(TCString sFileName);
// ���� : �ļ���
// ���� : �����ļ��ṹָ��
// ˵�� :
//==========================================================================
void TCDBIndex::DBOpen(TCString sFileName)
{
    if (m_df != NULL)
        DBClose();

    m_BufferPool[0].m_bfBuffer = DBAllocBuf(DB_FILE_HDR_SIZE);

    TFileHeader fhFileHeader;

    //======= 1. ����DataFile���ڴ� =========
    m_df = new TSDBDataFile;
    memset(m_df, 0, sizeof(TSDBDataFile));
    if (m_df == NULL)
        throw TCException("DBIndex Alloc Error");

    //====== 2. �����ļ�ͷ���ڴ� ========
//%%%    m_df->m_bfFileHeader = DBAllocBuf(DB_FILE_HDR_SIZE);  //COMMENT20010601
    m_df->m_bfFileHeader = m_BufferPool[0].m_bfBuffer;         //%%%ADD20010601

    //====== 3. ��������ڴ�ͬʱ����fhFileHeader�������Ժ�ķ��� ======
    fhFileHeader = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;

    //===== 4. ����DataFile���ļ����� =====
    ASSERT(Length(sFileName) < INDEX_FILE_NAME_MAX);
    strcpy(m_df->m_szFileName, (char *)sFileName);

    //===== 5. ���ļ� ========
    if (m_bOpenWithReadOnly)
        m_fFile.Open(m_df->m_szFileName, omRead | omShared);
    else
        m_fFile.Open(m_df->m_szFileName, omRead | omWrite);

    //======== 6. �����ļ���״̬ΪDBOpen =====
    m_df->m_wStat = fsDBOpen;

    //======= 7. ����FileHeader ========
    DBGetBlock(0, m_df->m_bfFileHeader);

    //===== 8. ����Buffer, Tmp, Aux�ڴ� ==========
    m_df->m_bfBuffer = DBAllocBuf(fhFileHeader->m_wBlockSize
            + 2 * fhFileHeader->m_wRecordSize);

    m_df->m_bfTmp = DBAllocBuf(fhFileHeader->m_wBlockSize
            + 2 * fhFileHeader->m_wRecordSize);

    m_df->m_bfAux = DBAllocBuf(fhFileHeader->m_wBlockSize
            + 2 * fhFileHeader->m_wRecordSize);

    // ��������ڴ��С��ΪBlockSize��2����RecordSize, ��BufSize��Ա
    // �����������BlockSize��
    m_df->m_bfBuffer->m_wBufSize = fhFileHeader->m_wBlockSize;
    m_df->m_bfTmp->m_wBufSize = fhFileHeader->m_wBlockSize;
    m_df->m_bfAux->m_wBufSize = fhFileHeader->m_wBlockSize;

    //======= 9. ���FileHeader�и���������Ч�� ========
    DBFileHeaderOpen(fhFileHeader);
}

//==========================================================================
// ���� : TCDBIndex::DBOpenR
// ��; : ��ֻ����ʽ�����ݿ������ļ�
// ԭ�� : TDataFile DBOpenR(TCString sFileName);
// ���� : �ļ���
// ���� : ��
// ˵�� :
// ��ʷ : 2001.11.29 Oldix, ���ӱ�����
//==========================================================================
void TCDBIndex::DBOpenR(TCString sFileName)
{
    m_bOpenWithReadOnly = true;
    DBOpen(sFileName);
    m_bOpenWithReadOnly = false;
}

//==========================================================================
// ���� : TCDBIndex::DBPutBlock
// ��; : ����д�뻺��
// ԭ�� : void DBPutBlock(TBuffer buf);
// ���� : Buffer
// ���� : ��
// ˵�� : ��������������ﵽ����ʱ�Ž���д�̲���
//==========================================================================
void TCDBIndex::DBPutBlock(TBuffer buf)
{
    ASSERT(buf != NULL);

    ASSERT(buf->m_wBufCurrentSize != 0);

    BUFRefresh(buf, true);
}

//==========================================================================
// ���� : TCDBIndex::DBPutBlock
// ��; : ���ļ�д��һ��Buffer
// ԭ�� : void DBPutBlock(TBuffer buf);
// ���� : Buffer
// ���� : ��
// ˵�� :
//==========================================================================
void TCDBIndex::DBPutBlockToFile(TBuffer buf)
{
    TFileHeader fhFileHeader;
    int nCount;

    long nOffset;

    ASSERT(m_df != NULL);

    ASSERT((m_df->m_wStat & fsDBOpen) != 0);

    ASSERT(buf != NULL);

    ASSERT(buf->m_wBufCurrentSize != 0);

    //===== 1. ���ָ��Buffer���ļ��е�ƫ���� =========
    fhFileHeader = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;

    if (buf->m_dwBufCurrentBlock == 0)
        nOffset = 0;
    else
        nOffset = DB_FILE_HDR_SIZE +
                (buf->m_dwBufCurrentBlock - 1) * fhFileHeader->m_wBlockSize;

    //===== 2. ��λ�ļ�ָ�� =========
    m_fFile.Seek(nOffset);

    //===== 3. д��ָ��Buffer ========
    nCount = m_fFile.Write(buf->m_pBufData, buf->m_wBufCurrentSize);
    if (nCount !=  buf->m_wBufCurrentSize)
        throw TCException("DBIndex Write Error");

}

//==========================================================================
// ���� : TCDBIndex::DBReadFirst
// ��; : ��ȡ�����ļ��ĵ�һ������
// ԭ�� : bool DBReadFirst(char *szUserData);
// ���� : �ļ�DataFile, ���ݴ��
// ���� : ��û���ҵ���¼
// ˵�� : �ҵ��ļ�¼��������szUserData��, szUserDataҪ���ȷ����㹻���ڴ档
// ���þ��� :
//          bFindIt = dbIndex.DBReadFirst(data);
//          while (bFindIt)
//          {   // ... Process Data ...
//              bFindIt = dbIndex.DBReadNext(data);
//          }
//==========================================================================
bool TCDBIndex::DBReadFirst(char *szUserData)
{
    bool bFindIt;

    TFileHeader fh;
    TBuffer buf;

    DBCheckDataFile();

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;

    //===== 1. �����¼��Ϊ0���򷵻�false =====
    if (fh->m_dwRecordCount == 0)
        return false;

    //======= 2. ����DBReadFirstIDX�Ի������ =====
    bFindIt = DBReadFirstIDX(fh->m_dwRootPointer, szUserData);
    if (!bFindIt)
    {
        buf->m_dwBufCurrentBlock = 0;
        buf->m_wBufRecordIndex = 0;
        buf->m_wBufCurrentSize = 0;
    }

    //=== 3. ����ǰλ�ü���С����m_dwPrevBlock, m_wPrevRecord, m_wPrevVSize�� ==
    m_df->m_dwPrevBlock = buf->m_dwBufCurrentBlock;
    m_df->m_wPrevRecord = buf->m_wBufRecordIndex;
    m_df->m_wPrevVSize = buf->m_wBufCurrentSize;

    return bFindIt;
}

//==========================================================================
// ���� : TCDBIndex::DBReadFirstIDX
// ��; : ��ȡָ��Block��ָ��ĵ�һ����¼
// ԭ�� : bool DBReadFirstIDX(DWORD dwBlock, char *szUserData);
// ���� : �ļ�DataFile, ��ţ����ݴ��
// ���� : ��û���ҵ���¼
// ˵�� : �ҵ��ļ�¼��������szUserData��, szUserDataҪ���ȷ����㹻���ڴ档
//==========================================================================
bool TCDBIndex::DBReadFirstIDX(DWORD dwBlock, char *szUserData)
{
    TFileHeader fh;
    TBuffer buf;
    TIndexHeader ihIndexHeader;
//    TIndexRecord irIndexRecord;       //0918
    TSDBIndexRecord irDBIndexRecord;
    char *szSrc;

    //===== 1. ������Ϊ0������false =========
    if (dwBlock == 0)
        return false;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;

    ihIndexHeader = (TIndexHeader)buf->m_pBufData;

//    irIndexRecord = (TIndexRecord)(buf->m_pBufData
//                + sizeof(TSDBIndexHeader));           //0918
    char *pIndexRecord;
    pIndexRecord = buf->m_pBufData + sizeof(TSDBIndexHeader);

    //===== 2. һֱ��������һ����¼(���Ϊ0) =====
    while (dwBlock)
    {
        DBGetBlock(dwBlock, buf);

        if (ihIndexHeader->m_wRecCount == 0)
            throw TCException("DBIndex Invalid Index");

//        dwBlock = irIndexRecord->dwIndexPointer;      //0918
        memcpy(&irDBIndexRecord, pIndexRecord, sizeof(TSDBIndexRecord));
        dwBlock = irDBIndexRecord.dwIndexPointer;
    }

    //====== 3. ������Ӧ���� ======
    szSrc = buf->m_pBufData + sizeof(TSDBIndexHeader) + fh->m_wControlSize;
    buf->m_wBufRecordIndex = 1;
    memcpy(szUserData, szSrc, fh->m_wDataSize);

    return true;
}

//==========================================================================
// ���� : TCDBIndex::DBReadLast
// ��; : ��ȡ�����ļ������һ������
// ԭ�� : bool DBReadLast(char *szUserData);
// ���� : �ļ�DataFile, ���ݴ��
// ���� : ��û���ҵ���¼
// ˵�� : �ҵ��ļ�¼��������szUserData��, szUserDataҪ���ȷ����㹻���ڴ档
// ���þ��� :
//          bFindIt = dbIndex.DBReadLast(data);
//          while (bFindIt)
//          {   // ... Process Data ...
//              bFindIt = dbIndex.DBReadPrev(data);
//          }
//==========================================================================
bool TCDBIndex::DBReadLast(char *szUserData)
{
    bool bFindIt;

    TFileHeader fh;
    TBuffer buf;

    DBCheckDataFile();

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;

    //===== 1. �����¼��Ϊ0���򷵻�false =====
    if (fh->m_dwRecordCount == 0)
        return false;

    //======= 2. ����DBReadFirstIDX�Ի������ =====
    bFindIt = DBReadLastIDX(fh->m_dwRootPointer, szUserData);
    if (!bFindIt)
    {
        buf->m_dwBufCurrentBlock = 0;
        buf->m_wBufRecordIndex = 0;
        buf->m_wBufCurrentSize = 0;
    }

    //=== 3. ����ǰλ�ü���С����m_dwPrevBlock, m_wPrevRecord, m_wPrevVSize�� ==
    m_df->m_dwPrevBlock = buf->m_dwBufCurrentBlock;
    m_df->m_wPrevRecord = buf->m_wBufRecordIndex;
    m_df->m_wPrevVSize = buf->m_wBufCurrentSize;

    return bFindIt;
}

//==========================================================================
// ���� : TCDBIndex::DBReadLastIDX
// ��; : ��ȡָ��Block��ָ������һ����¼
// ԭ�� : bool DBReadLastIDX(DWORD dwBlock, char *szUserData);
// ���� : �ļ�DataFile, ��ţ����ݴ��
// ���� : ��û���ҵ���¼
// ˵�� : �ҵ��ļ�¼��������szUserData��, szUserDataҪ���ȷ����㹻���ڴ档
//==========================================================================
bool TCDBIndex::DBReadLastIDX(DWORD dwBlock, char *szUserData)
{
    TFileHeader fh;
    TBuffer buf;
    TIndexHeader ihIndexHeader;
//    TIndexRecord irIndexRecord;       //0918
    TSDBIndexRecord irDBIndexRecord;
    char *szSrc;

    //===== 1. ������Ϊ0������false =========
    if (dwBlock == 0)
        return false;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;

    ihIndexHeader = (TIndexHeader)buf->m_pBufData;

    //===== 2. һֱ��������һ����¼(���Ϊ0) =====
    while (dwBlock)
    {
        DBGetBlock(dwBlock, buf);

        if (ihIndexHeader->m_wRecCount == 0)
            throw TCException("DBIndex Invalid Index");

//      irIndexRecord = (TIndexRecord)(buf->m_pBufData + sizeof(TSDBIndexHeader)
//              + ihIndexHeader->m_wRecCount * fh->m_wRecordSize);      //0918
        char *pIndexRecord;
        pIndexRecord = buf->m_pBufData + sizeof(TSDBIndexHeader)
                  + ihIndexHeader->m_wRecCount * fh->m_wRecordSize;
        memcpy(&irDBIndexRecord, pIndexRecord, sizeof(TSDBIndexRecord));

//        dwBlock = irIndexRecord->dwIndexPointer;      //0918
        dwBlock = irDBIndexRecord.dwIndexPointer;
    }

    //====== 3. ������Ӧ���� ======
    szSrc = buf->m_pBufData + sizeof(TSDBIndexHeader)
            + (ihIndexHeader->m_wRecCount - 1) * fh->m_wRecordSize
            + fh->m_wControlSize;
    buf->m_wBufRecordIndex = ihIndexHeader->m_wRecCount;
    memcpy(szUserData, szSrc, fh->m_wDataSize);

    return true;
}

//==========================================================================
// ���� : TCDBIndex::DBReadNext
// ��; : ��ȡ��һ����¼
// ԭ�� : bool DBReadNext(char *szUserData);
// ���� : �ļ�DataFile, ���ݴ��
// ���� : ��û���ҵ���¼
// ˵�� : 1. ����DataFile��m_dwPrevBlock, m_wPrevRecord
//        2. �ҵ��ļ�¼��������szUserData��, szUserDataҪ���ȷ����㹻���ڴ档
//==========================================================================
bool TCDBIndex::DBReadNext(char *szUserData)
{
    bool bFindIt;

    TFileHeader fh;
    TBuffer buf;

    DBCheckDataFile();

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;

    if (fh->m_dwRecordCount == 0)
        return false;

    if (m_df->m_dwPrevBlock == 0)
        throw TCException("DBIndex No Current Record");

    bFindIt = DBReadNextIDX(szUserData);

    if (!bFindIt)
    {   buf->m_dwBufCurrentBlock = 0;
        buf->m_wBufRecordIndex = 0;
        buf->m_wBufCurrentSize = 0;
    }

    m_df->m_dwPrevBlock = buf->m_dwBufCurrentBlock;
    m_df->m_wPrevRecord = buf->m_wBufRecordIndex;
    m_df->m_wPrevVSize = buf->m_wBufCurrentSize;

    return bFindIt;
}

//==========================================================================
// ���� : TCDBIndex::DBReadNextIDX
// ��; : ��ȡ��һ��������¼
// ԭ�� : bool DBReadNextIDX(char *szUserData);
// ���� : �ļ�DataFile, ���ݴ��
// ���� : ��û���ҵ���¼
// ˵�� : 1. ����DataFile�е�m_dwPrevBlock��m_wPrevRecord����һ����¼��
//        2. �ҵ��ļ�¼��������szUserData��, szUserDataҪ���ȷ����㹻���ڴ档
//==========================================================================
bool TCDBIndex::DBReadNextIDX(char *szUserData)
{
    TFileHeader fh;
    TBuffer buf;
    TIndexHeader ihIndexHeader;
//    TIndexRecord irIndexRecord;       //0918
    TSDBIndexRecord irDBIndexRecord;
    char *szSrc;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;
    ihIndexHeader = (TIndexHeader)buf->m_pBufData;

    //====== 1. ��ȡ��ǰ�� ======
    DBGetBlock(m_df->m_dwPrevBlock, buf);

    buf->m_wBufRecordIndex = m_df->m_wPrevRecord;

    //== 2. �統ǰ��¼Ϊ���е�����¼����һֱ����������ӵ����Ч���ݵĸ��ڵ� ===
    if (buf->m_wBufRecordIndex > ihIndexHeader->m_wRecCount)
    {
        while (buf->m_wBufRecordIndex > ihIndexHeader->m_wRecCount)
            if (!DBGetParentIDX())
                return false;

        szSrc = buf->m_pBufData + sizeof(TSDBIndexHeader)
                + (buf->m_wBufRecordIndex - 1) * fh->m_wRecordSize
                + fh->m_wControlSize;
        memcpy(szUserData, szSrc, fh->m_wDataSize);
        return true;
    }

    //===== 3. ��ǰ��¼��1 ========
    buf->m_wBufRecordIndex ++;

    char *pIndexRecord;
    pIndexRecord = buf->m_pBufData + sizeof(TSDBIndexHeader)
            + (buf->m_wBufRecordIndex - 1) * fh->m_wRecordSize;
    memcpy(&irDBIndexRecord, pIndexRecord, sizeof(irDBIndexRecord));
//    irIndexRecord = (TIndexRecord)(buf->m_pBufData + sizeof(TSDBIndexHeader)
//            + (buf->m_wBufRecordIndex - 1) * fh->m_wRecordSize);  //0918

    //====== 4. ������ָ�벻Ϊ�㣬���ȡ��һ����¼ ======
//    if (irIndexRecord->dwIndexPointer != 0)       //0918
//        return DBReadFirstIDX(irIndexRecord->dwIndexPointer, szUserData);
    if (irDBIndexRecord.dwIndexPointer != 0)
        return DBReadFirstIDX(irDBIndexRecord.dwIndexPointer, szUserData);

    //===== 5. �����ǰ��¼���ڱ����¼����ݹ���ñ��������������ڵ㣩 ====
    if (buf->m_wBufRecordIndex > ihIndexHeader->m_wRecCount)
    {
        m_df->m_wPrevRecord = buf->m_wBufRecordIndex;
        return DBReadNextIDX(szUserData);
    }

//    szSrc = ((char *)irIndexRecord) + fh->m_wControlSize;     //0918
    szSrc = pIndexRecord + fh->m_wControlSize;
    memcpy(szUserData, szSrc, fh->m_wDataSize);

    return true;
}

//==========================================================================
// ���� : TCDBIndex::DBReadPrev
// ��; : ��ȡ��һ����¼
// ԭ�� : bool DBReadPrev(char *szUserData);
// ���� : �ļ�DataFile, ���ݴ��
// ���� : ��û���ҵ���¼
// ˵�� : 1. ����DataFile��m_dwPrevBlock, m_wPrevRecord
//        2. �ҵ��ļ�¼��������szUserData��, szUserDataҪ���ȷ����㹻���ڴ档
//==========================================================================
bool TCDBIndex::DBReadPrev(char *szUserData)
{
    bool bFindIt;

    TFileHeader fh;
    TBuffer buf;

    DBCheckDataFile();

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;

    if (fh->m_dwRecordCount == 0)
        return false;

    if (m_df->m_dwPrevBlock == 0)
        throw TCException("DBIndex No Current Record");

    bFindIt = DBReadPrevIDX(szUserData);

    if (!bFindIt)
    {   buf->m_dwBufCurrentBlock = 0;
        buf->m_wBufRecordIndex = 0;
        buf->m_wBufCurrentSize = 0;
    }

    m_df->m_dwPrevBlock = buf->m_dwBufCurrentBlock;
    m_df->m_wPrevRecord = buf->m_wBufRecordIndex;
    m_df->m_wPrevVSize = buf->m_wBufCurrentSize;

    return bFindIt;
}

//==========================================================================
// ���� : TCDBIndex::DBReadPrevIDX
// ��; : ��ȡ��һ��������¼
// ԭ�� : bool DBReadPrevIDX(char *szUserData);
// ���� : �ļ�DataFile, ���ݴ��
// ���� : ��û���ҵ���¼
// ˵�� : 1. ����DataFile�е�m_dwPrevBlock��m_wPrevRecord����һ����¼��
//        2. �ҵ��ļ�¼��������szUserData��, szUserDataҪ���ȷ����㹻���ڴ档
//==========================================================================
bool TCDBIndex::DBReadPrevIDX(char *szUserData)
{
    TFileHeader fh;
    TBuffer buf;
//    TIndexRecord irIndexRecord;       //0918
    TSDBIndexRecord irDBIndexRecord;
    char *szSrc;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;

    //====== 1. ��ȡ��ǰ�� ======
    DBGetBlock(m_df->m_dwPrevBlock, buf);

    //===== 2. ��λ����ǰ��¼ ========
    buf->m_wBufRecordIndex = m_df->m_wPrevRecord;

//    irIndexRecord = (TIndexRecord)(buf->m_pBufData + sizeof(TSDBIndexHeader)
//            + (buf->m_wBufRecordIndex - 1) * fh->m_wRecordSize);  //0918
    char *pIndexRecord;
    pIndexRecord = buf->m_pBufData + sizeof(TSDBIndexHeader)
            + (buf->m_wBufRecordIndex - 1) * fh->m_wRecordSize;
    memcpy(&irDBIndexRecord, pIndexRecord, sizeof(TSDBIndexRecord));

//    if (buf->m_wBufRecordIndex == 1
//            && irIndexRecord->dwIndexPointer == 0)        //0918
    if (buf->m_wBufRecordIndex == 1 && irDBIndexRecord.dwIndexPointer == 0)  
    {
        //== 2. �統ǰ��¼Ϊ���еĵ�һ����¼����һֱ�������������ڵ� ===
        while (buf->m_wBufRecordIndex == 1)
            if (!DBGetParentIDX())
                return false;

        //===== 3. ��ǰ��һ����¼ ====
        buf->m_wBufRecordIndex --;

        szSrc = buf->m_pBufData + sizeof(TSDBIndexHeader)
                + (buf->m_wBufRecordIndex - 1) * fh->m_wRecordSize
                + fh->m_wControlSize;
        memcpy(szUserData, szSrc, fh->m_wDataSize);
        return true;
    }

    //====== 4. ������ָ�벻Ϊ�㣬���ȡ���һ����¼ ======
//    if (irIndexRecord->dwIndexPointer != 0)       //0918
//        return DBReadLastIDX(irIndexRecord->dwIndexPointer, szUserData);
    if (irDBIndexRecord.dwIndexPointer != 0)
        return DBReadLastIDX(irDBIndexRecord.dwIndexPointer, szUserData);

    //====== 5. ��ȡǰһ����¼ =======
    buf->m_wBufRecordIndex -- ;

//    irIndexRecord = (TIndexRecord)(buf->m_pBufData + sizeof(TSDBIndexHeader)
//            + (buf->m_wBufRecordIndex - 1) * fh->m_wRecordSize);  //0918

    pIndexRecord = buf->m_pBufData + sizeof(TSDBIndexHeader)
            + (buf->m_wBufRecordIndex - 1) * fh->m_wRecordSize;

//    szSrc = (char *)irIndexRecord + fh->m_wControlSize;
    szSrc = pIndexRecord + fh->m_wControlSize;
    
    memcpy(szUserData, szSrc, fh->m_wDataSize);

    return true;
}

//==========================================================================
// ���� : TCDBIndex::DBSplitBlockIDX
// ��; : �ж�ĳ���Ƿ�Ҫ���ѣ�������д�ļ�����
// ԭ�� : void DBSplitBlockIDX();
// ���� : �ļ�DataFile
// ���� : ��
// ˵�� : 
//==========================================================================
void  TCDBIndex::DBSplitBlockIDX()
{
    //======== 0. �ʵ��ĳ�ʼ������ ============
    TFileHeader fh;
    TBuffer bfBuffer, bfTmpBuffer;
    TIndexHeader ihIndexHeader;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    bfBuffer = m_df->m_bfBuffer;
    bfTmpBuffer = m_df->m_bfTmp;

    ihIndexHeader = (TIndexHeader)bfBuffer->m_pBufData;

    //==== 1. �����ǰ��ļ�¼��û�г������ƣ���д�ļ����˳����� =====
    if (ihIndexHeader->m_wRecCount <= fh->m_wRecordsPerBlock)
    {   DBPutBlock(bfBuffer);
        return;
    }

    //====== 2. ���ø�Buffer��ֵ������Buffer��TmpBuffer�� ===================
    TBuffer bfAuxBuffer;

    bfBuffer = m_df->m_bfTmp;
    bfTmpBuffer = m_df->m_bfBuffer;
    m_df->m_bfBuffer = bfBuffer;
    m_df->m_bfTmp = bfTmpBuffer;
    bfAuxBuffer = m_df->m_bfAux;

    //==== 3. �õ���һ�����ÿռ� ======
    DBGetNextAvail(bfBuffer);
    //========= 4. ��������ͷ��ֵ =======
    TIndexHeader ihTmpIndexHeader, ihAuxIndexHeader;
    ihIndexHeader = (TIndexHeader)bfBuffer->m_pBufData;
    ihTmpIndexHeader = (TIndexHeader)bfTmpBuffer->m_pBufData;
    ihAuxIndexHeader = (TIndexHeader)bfAuxBuffer->m_pBufData;

    //======= 5. ��ֵ�����ѵĿ�ż��·���Ŀ�� ========
    DWORD dwLeftBlock, dwRightBlock;
    dwLeftBlock = bfTmpBuffer->m_dwBufCurrentBlock;      // �����ѵĿ��
    dwRightBlock = bfBuffer->m_dwBufCurrentBlock;        // �·���Ŀ��

    //======= 6. ���ѵ��������ߵļ�¼�� =======
    WORD wCount1, wCount2;
    wCount1 = ihTmpIndexHeader->m_wRecCount / 2;
    wCount2 = ihTmpIndexHeader->m_wRecCount - (wCount1 + 1);

    //====== 7. ���·���Ŀ�ͷ��ֵ����ż���¼�� =====
    ihIndexHeader->m_dwParent = ihTmpIndexHeader->m_dwParent;
    ihIndexHeader->m_wRecCount = wCount2;

    //====== 8. �·�����Ŀ�����¼����, д���ļ� =======
    char *szSrc, *szDest;
    WORD wCount;

    szSrc = bfTmpBuffer->m_pBufData + sizeof(TSDBIndexHeader)
            + (wCount1 + 1) * fh->m_wRecordSize;
    szDest = bfBuffer->m_pBufData + sizeof(TSDBIndexHeader);
    wCount = (wCount2 + 1) * fh->m_wRecordSize;
    memcpy(szDest, szSrc, wCount);
    DBPutBlock(bfBuffer);
    //==== 9. �������Ľڼ�¼���ڷ��ѳ��Ŀ��У������AddBlock��AddRecord =====
    if (m_dwDBAddBlock == bfTmpBuffer->m_dwBufCurrentBlock)
        if (m_wDBAddRecord > wCount1 + 1)
        {
            m_dwDBAddBlock = bfBuffer->m_dwBufCurrentBlock;
            m_wDBAddRecord -= wCount1 + 1;
        }

    //=== 10. ����Ǹ��飬����չһ���������飬������ǰ��ָ����� =====
    char *szRBuf;
//    TIndexRecord irIndexRecord;       //0918
    TSDBIndexRecord irDBIndexRecord;
    WORD wRecord;

    szRBuf = bfBuffer->m_pBufData + sizeof(TSDBIndexHeader);
    if (ihTmpIndexHeader->m_dwParent == 0)
    {
        if (bfTmpBuffer->m_dwBufCurrentBlock != fh->m_dwRootPointer)
            throw TCException("DBIndex Invalid Index");
        DBGetNextAvail(bfBuffer);
        ihTmpIndexHeader->m_dwParent = bfBuffer->m_dwBufCurrentBlock;
        fh->m_dwRootPointer = bfBuffer->m_dwBufCurrentBlock;

//        irIndexRecord = (TIndexRecord)szRBuf;     //0918
        memcpy(&irDBIndexRecord, szRBuf, sizeof(TSDBIndexRecord));
        wRecord = 1;
    }
    //==== 11. ������Ǹ��飬���ȡ���ڵ�, ��ѭ�����Ҹ��ڵ��ж�Ӧ�ļ�¼λ�� ===
    else
    {
        DBGetBlock(ihTmpIndexHeader->m_dwParent, bfBuffer);
        for (wRecord = 1; wRecord <= ihIndexHeader->m_wRecCount + 1; wRecord++)
        {
//            irIndexRecord = (TIndexRecord)szRBuf;     //0918
            memcpy(&irDBIndexRecord, szRBuf, sizeof(TSDBIndexRecord));

//            if (irIndexRecord->dwIndexPointer == dwLeftBlock)
//                break;        //0918
            if (irDBIndexRecord.dwIndexPointer == dwLeftBlock)
                break;

            szRBuf += fh->m_wRecordSize;
        }

//        if (irIndexRecord->dwIndexPointer != dwLeftBlock)     //0918
        if (irDBIndexRecord.dwIndexPointer != dwLeftBlock)
            throw TCException("DBIndex Invalid Index");
    }

    //======= 12. ʹ���ڵ�ָ���·�������Ľڵ� ========
//    irIndexRecord->dwIndexPointer = dwRightBlock;         //0918
    irDBIndexRecord.dwIndexPointer = dwRightBlock;
    memcpy(szRBuf, &irDBIndexRecord, sizeof(TSDBIndexRecord));

    //====== 13. ����ڵ�ǡ�������м䣬��ýڵ㼴Ϊ���ڵ� =====
    if (m_dwDBAddBlock == bfTmpBuffer->m_dwBufCurrentBlock)
        if (m_wDBAddRecord == wCount1 + 1)
        {
            m_dwDBAddBlock = bfBuffer->m_dwBufCurrentBlock;
            m_wDBAddRecord = wRecord;
        }

    //====== 14. ���ڵ���ؼ�¼����ƶ����������¼�¼ =======
    szSrc = szRBuf;
    szDest = szRBuf + fh->m_wRecordSize;
    // �������һ��IndexRecord, ����Ҫ�ิ��һ����¼
    wCount = ((ihIndexHeader->m_wRecCount + 1) - wRecord + 1)
            * fh->m_wRecordSize;
    // ԭ���õ���memcpy, ���������ص�����Ϊmemmove
    memmove(szDest, szSrc, wCount);

    //===== 15. ���м�ڵ�������Ƶ����ڵ��� =======
    szSrc = bfTmpBuffer->m_pBufData + sizeof(TSDBIndexHeader)
            + wCount1 * fh->m_wRecordSize;
    szDest = szRBuf;
    wCount = fh->m_wRecordSize;
    memcpy(szDest, szSrc, wCount);

    //====== 16. ���ڵ���¼��ڵ�������ڵ�(�Ͻڵ�), ���м�¼��+1 ========
//    irIndexRecord->dwIndexPointer = dwLeftBlock;      //0918
    irDBIndexRecord.dwIndexPointer = dwLeftBlock;
    memcpy(szRBuf, &irDBIndexRecord, sizeof(TSDBIndexRecord));

    ihIndexHeader->m_wRecCount ++;

    //======= 17. ����ԭ�ڵ��¼����д��ԭ�ڵ� ======
    ihTmpIndexHeader->m_wRecCount = wCount1;
    DBPutBlock(bfTmpBuffer);

    //======== 18. ȡ���ҽڵ㣬���ҽڵ�ĸ��ڵ㲻�ԣ�����£�д�� ======
    DBGetBlock(dwRightBlock, bfTmpBuffer);
    if (ihTmpIndexHeader->m_dwParent != bfBuffer->m_dwBufCurrentBlock)
    {
        ihTmpIndexHeader->m_dwParent = bfBuffer->m_dwBufCurrentBlock;
        DBPutBlock(bfTmpBuffer);
    }

    //====== 19. ��һȡ���ҽ���ÿһ���ӽڵ㣬�������丸�ڵ�λ�� ====
    szRBuf = bfTmpBuffer->m_pBufData + sizeof(TSDBIndexHeader);

    for (wCount = 1; wCount <= ihTmpIndexHeader->m_wRecCount + 1; wCount++)
    {
//        irIndexRecord = (TIndexRecord)szRBuf;     //0918
        memcpy(&irDBIndexRecord, szRBuf, sizeof(TSDBIndexRecord));

//        if (irIndexRecord->dwIndexPointer != 0)
        if (irDBIndexRecord.dwIndexPointer != 0)
        {
//            DBGetBlock(irIndexRecord->dwIndexPointer, bfAuxBuffer);   //0918
            DBGetBlock(irDBIndexRecord.dwIndexPointer, bfAuxBuffer);
            ihAuxIndexHeader->m_dwParent = bfTmpBuffer->m_dwBufCurrentBlock;
            DBPutBlock(bfAuxBuffer);
        }

        szRBuf += fh->m_wRecordSize;
    }

    //====== 20. �ݹ���ã���ȷ�����ڵ��Ƿ���Ҫ���� =======
    DBSplitBlockIDX();
}

void TCDBIndex::DBZAP()
{
    TFileHeader fh;

    DBCheckDataFile();

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    fh->m_dwRecordCount = 0;
    fh->m_dwRootPointer = 0;
    fh->m_dwLastBlock = 0;

    DBPutBlockToFile(m_df->m_bfFileHeader);

    m_fFile.Truncate(DB_FILE_HDR_SIZE);
}

//==========================================================================
// ���� : TCDBIndex::Flush
// ��; : ��һЩ�仯���д��Ӳ��
// ԭ�� : void Flush();
// ���� : ��
// ���� : ��
// ˵�� : �ú���һ����һ���׶β�������Ժ���ã��Ա�֤���ݵ������ԡ�
//==========================================================================
void TCDBIndex::Flush()
{
    ASSERT(m_df != NULL);

    BUFWriteAllUpdated();

    m_fFile.Flush();
}

#pragma warn +8071

#ifdef __TEST__

#ifndef __WIN32__

long GetTickCount()
{
    return 0;
}

#endif

char     data[2040];

#ifdef __WIN32__
#define IDX_FILE "c:\\temp\\ictest.dat"
#else
#define IDX_FILE "ictest.dat"
#endif

void DisplayTestIndexFilePrompt()
{
    printf("\n\n==== Test Index File ====\n\n");
    printf("0. Create Index File\n");
    printf("1. Add Data\n");
    printf("2. Read Forward\n");
    printf("3. Read Reverse\n");
    printf("4. Find (First) & Delete\n");
    printf("5. Find Key\n");
    printf("6. Generate Src Data File\n");
    printf("7. Create Index File And Add Data From \"_src_dat\"\n");
    printf("8. ZAP Index File\n");
    printf("Q. Quit\n\n");
}

void I00CreateIndexFile()
{
    TCDBIndex dbIndex;
    long nBlockSize;

    printf("Input the block size of index file:");
    scanf("%ld", &nBlockSize);

    if (nBlockSize < 10 || nBlockSize > 10000000)
    {   printf("invalid blocksize");
        return;
    }

    sprintf(data, "idx, rec=25, blk=%ld, key=10, UNIQUE", nBlockSize);

    dbIndex.DBCreate(IDX_FILE, data, "FOXDBF IDX K0003");

    dbIndex.DBClose();

    printf("%ld", sizeof(TSDBFileHeader));
}

void I01AddData()
{
    memset(data, 0, sizeof(data));

    TCDBIndex dbIndex;

    long i;

    long nCount;
    long nStartTickCount;

    dbIndex.DBOpen(IDX_FILE);

//    printf((char *)dbIndex.DBGetOtherInfo());

    printf("Input the number of records you want to add:");
    scanf("%ld", &nCount);

    printf("...adding %d records...\n", nCount);

    nStartTickCount = GetTickCount();

    for (i=1; i <= nCount; i++)
    {
        sprintf(data,"%010ld",RandLong(99999999));

        if (dbIndex.DBAdd(data) == arDuplicateKey)
            printf("*");

        if (i % 100 == 0)
        {   printf(".");
            fflush(stdout);
        }
    }

    printf("\nTickCount : %ld\n", GetTickCount() - nStartTickCount);
    printf("...add complete...\n\n");

    dbIndex.DBClose();
}

void I02ReadForward()
{
    TCDBIndex dbIndex;

#ifdef __WIN32__
    char *szForwardFile = "c:\\temp\\ictest_readforward.dat";
#else
    char *szForwardFile = "ictest_readforward.dat";
#endif

    FILE *fpForward;
    bool bFindIt;

    long nCount = 0;

    fpForward = fopen(szForwardFile, "wt");

    dbIndex.DBOpen(IDX_FILE);

    bFindIt = dbIndex.DBReadFirst(data);

    while (bFindIt)
    {
        fprintf(fpForward, "%10s\n",data);

        nCount ++;
        if (nCount % 100 == 0)
        {   printf(".");
            fflush(stdout);
        }

        bFindIt = dbIndex.DBReadNext(data);
    }

    dbIndex.DBClose();

    fclose(fpForward);
    printf("\nwrite result to file : %s\n", szForwardFile);
}

void I03ReadReverse()
{
    TCDBIndex dbIndex;

#ifdef __WIN32__
    char *szReverseFile = "c:\\temp\\ictest_readreverse.dat";
#else
    char *szReverseFile = "ictest_readreverse.dat";
#endif

    FILE *fpReverse;
    bool bFindIt;

    long nCount = 0;

    fpReverse = fopen(szReverseFile, "wt");

    dbIndex.DBOpen(IDX_FILE);

    bFindIt = dbIndex.DBReadLast(data);

    while (bFindIt)
    {
        fprintf(fpReverse, "%10s\n",data);

        nCount ++;
        if (nCount % 100 == 0)
        {   printf(".");
            fflush(stdout);
        }

        bFindIt = dbIndex.DBReadPrev(data);
    }

    dbIndex.DBClose();

    fclose(fpReverse);
    printf("\nwrite result to file : %s\n", szReverseFile);
}

void I04FindAndDelete()
{
}

void I05FindKey()
{
    TCDBIndex dbIndex;
    long nKeyValue;

    printf("Input the Key Value to be Searched:");
    scanf("%ld", &nKeyValue);

    sprintf(data, "%010ld", nKeyValue);

    dbIndex.DBOpen(IDX_FILE);

    if (dbIndex.DBFind(data, data, 0))
        printf("Find It");
    else
        printf("Cannot Find It");

    dbIndex.DBClose();
}

void I06GenerateSrcDataFile()
{
    TCString sDirectory;
    long nCount;

    printf("Input the Directory you want to Generate Data:");
    scanf("%s", data);
    sDirectory = TCString(data);
    if (!ForceDirectories(sDirectory))
        throw TCException("directory cannot be created");

    printf("Input the number of records you want to add:");
    scanf("%ld", &nCount);

    long i;
    TCString sFile;
    FILE *fpSrcData;

    sFile = MergePathAndFile(sDirectory, "_src_dat");

    printf("...adding %d data to file %s...\n", nCount, (char *)sFile);
    fpSrcData = fopen((char *)sFile, "wt");

    for (i=1; i <= nCount; i++)
    {
        sprintf(data,"%010ld",RandLong(99999999));
        fprintf(fpSrcData, "%s\n", data);

        if (i % 100 == 0)
        {   printf(".");
            fflush(stdout);
        }
        if (i % 10000 == 0)
            printf("%ld\n", i);
    }

    printf("\n... completed ...\n\n");
    fclose(fpSrcData);
}

void I07SortSrcDataFile()
{
    //==== 1. �õ��ļ�"_src_dat"��ŵ�Ŀ¼�� =====
    TCString sDirectory;

    printf("Input the Directory the File \"_src_dat\" put:");
    scanf("%s", data);
    sDirectory = TCString(data);

    //==== 2. �õ�"_src_dat"��ŵ�ȫ·���ļ��� ====
    TCString sFileName;
    sFileName = MergePathAndFile(sDirectory, "_src_dat");
    if (!FileExists(sFileName))
    {
        printf("The File not Exists.");
        return;
    }

    //======= 3. ����Index�ļ� ======
    TCDBIndex dbIndex;
    long nBlockSize;
    TCString sGenerateOptions;
    TCString sIndexFileName;

    nBlockSize = 512;

    sGenerateOptions = "idx, rec=12, blk=" + IntToStr(nBlockSize)
            + ", key=10, DUPLICATE";

    sIndexFileName = MergePathAndFile(sDirectory, "indexf.dat");

    dbIndex.DBCreate(sIndexFileName, sGenerateOptions);

//    dbIndex.DBClose();

    printf("Index File Created.\n");

    //========= 4. ��ȡ���ݲ����в��� =======
    //=== 4.1 �������ļ��������ļ� =====
    TCFileStream fsDataFile, fsReport;
    fsDataFile.Open(sFileName, omRead|omText);
//    dbIndex.DBOpen(sIndexFileName);
    fsReport.Open(MergePathAndFile(sDirectory, "perf_report.txt"),
            omWrite | omText);

    //===== 4.2 ѭ����ȡ���ݣ������в��� ======
    long nCount = 0;
    long nTickAll, nTick10000, nTick100000, nTickCost;
    TCString sDataItem;
    TCString sOutput;

    nTickAll = nTick10000 = nTick100000 = GetTickCount();

    while (fsDataFile.GetString(sDataItem) != NULL)
    {
        dbIndex.DBAdd(sDataItem);

        nCount ++;

        if (nCount % 100 == 0)
        {   printf(".");
            fflush(stdout);
        }

        if (nCount % 10000 == 0)
        {
            nTickCost = GetTickCount() - nTick10000;
            sOutput = "10000 : " + IntToStr(nTickCost) + " ("
                    + IntToStr(10000 * 1000 / nTickCost) + ")";
            printf("%s\n", (char *)sOutput);
            fsReport.WriteLn(sOutput);
            nTick10000 = GetTickCount();
        }

        if (nCount % 100000 == 0)
        {
            nTickCost = GetTickCount() - nTick100000;
            sOutput = IntToStr(nCount) + " - 100000 : "
                    + IntToStr(nTickCost) + " ("
                    + IntToStr(100000 * 1000 / nTickCost) + ")";
            printf("%s\n", (char *)sOutput);
            fsReport.WriteLn(sOutput);
            nTick100000 = GetTickCount();
        }
    }

    nTickCost = GetTickCount() - nTickAll;
    sOutput = "All : " + IntToStr(nTickCost) + " ("
            + IntToStr(nCount * 1000 / nTickCost) + ")";

    printf("%s\n", (char *)sOutput);
    fsReport.WriteLn(sOutput);
    printf("(Buffer Pool : %ld)\n", BUFFER_POOL_MAX);
    printf("...add complete...\n");

    //====== 4.3 �ر��ļ� ==========
    dbIndex.DBClose();
    fsDataFile.Close();
    fsReport.Close();

    //====== 5. ������������ļ����� =====
    memset(data, 0, sizeof(data));

    TCString sForwardFileName;
    sForwardFileName = MergePathAndFile(sDirectory, "rforward.txt");

    TCFileStream fForward;
    fForward.Open(sForwardFileName, omWrite | omText);
    bool bFindIt;

    dbIndex.DBOpen(sIndexFileName);

    nCount = 0;

    bFindIt = dbIndex.DBReadFirst(data);

    while (bFindIt)
    {
        fForward.WriteLn(data);

        nCount ++;
        if (nCount % 1000 == 0)
        {   printf(".");
            fflush(stdout);
        }

        bFindIt = dbIndex.DBReadNext(data);
    }

    dbIndex.DBClose();
    fForward.Close();

    printf("\nwrite result to file : %s\n", (char *)sForwardFileName);

    //====== 5. ������������ļ����� =====
    memset(data, 0, sizeof(data));

    TCString sReverseFileName;
    sReverseFileName = MergePathAndFile(sDirectory, "rreverse.txt");

    TCFileStream fReverse;
    fReverse.Open(sReverseFileName, omWrite | omText);

    dbIndex.DBOpen(sIndexFileName);

    nCount = 0;

    bFindIt = dbIndex.DBReadLast(data);

    while (bFindIt)
    {
        fReverse.WriteLn(data);

        nCount ++;
        if (nCount % 1000 == 0)
        {   printf(".");
            fflush(stdout);
        }

        bFindIt = dbIndex.DBReadPrev(data);
    }

    dbIndex.DBClose();
    fReverse.Close();

    printf("\nwrite result to file : %s\n", (char *)sReverseFileName);
}

void I08ZAPIndexFile()
{
    TCDBIndex dbIndex;

    dbIndex.DBOpen(IDX_FILE);

    dbIndex.DBZAP();

    dbIndex.DBClose();
}

void TestIndexFileMainFunc()
{
    int cChar;

//    TCDBIndex::BUFSetLazyWrite(true);     //ȱʡ��Ϊtrue
    TCDBIndex::DBAddThrowException(false);

    DisplayTestIndexFilePrompt();
    while (1)
    {
        cChar = getchar();

        switch (cChar)
        {
            case 'Q':
            case 'q':
            case 0x1B:
                return;

            case '0':
                I00CreateIndexFile();
                break;

            case '1':
                I01AddData();
                break;

            case '2':
                I02ReadForward();
                break;

            case '3':
                I03ReadReverse();
                break;

            case '4':
                I04FindAndDelete();
                break;

            case '5':
                I05FindKey();
                break;

            case '6':
                I06GenerateSrcDataFile();
                break;

            case '7':
                I07SortSrcDataFile();
                break;

            case '8':
                I08ZAPIndexFile();
                break;

            default:
                continue;
        }
        DisplayTestIndexFilePrompt();
    }
}

#endif






