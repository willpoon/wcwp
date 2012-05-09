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

// 索引文件文件头的大小
const long DB_FILE_HDR_SIZE = 4096;

const TCString INDEX_FILE_TAG = "BONSON IDX FILE ";

//==========================================================================
// 函数 : TCDBIndex::TCDBIndex
// 用途 : 构造函数
// 原型 : TCDBIndex::TCDBIndex()
// 参数 : 无
// 返回 : 无
// 说明 :
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
// 函数 : TCDBIndex::~TCDBIndex
// 用途 : 析构函数
// 原型 : TCDBIndex::~TCDBIndex()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
TCDBIndex::~TCDBIndex()
{
    if (m_df != NULL)
        DBClose();
}

//==========================================================================
// 函数 : TCDBIndex::BUFCopyBuffer
// 用途 : 复制Buffer
// 原型 : void BUFCopyBuffer(TBuffer bfDest, TBuffer bfSrc);
// 参数 : 目标Buffer指针，源Buffer指针
// 返回 : 无
// 说明 : 该函数不仅复制Buffer的内容，还复制Buffer数据的内容。
//==========================================================================
void TCDBIndex::BUFCopyBuffer(TBuffer bfDest, TBuffer bfSrc)
{
    memcpy(bfDest, bfSrc, sizeof(TSDBDataBuf) - sizeof(char *));
    memcpy(bfDest->m_pBufData, bfSrc->m_pBufData, bfSrc->m_wBufSize);
}

//==========================================================================
// 函数 : TCDBIndex::BUFFetch
// 用途 : 将一块的缓冲区复制到指定的Buffer之中
// 原型 : bool BUFFetch(TBuffer buf, DWORD dwBlock);
// 参数 : 目标Buffer指针，块号
// 返回 : 如果在缓冲池找到并进行复制，返回true; 没找到相应块，返回false。
// 说明 : 该函数不仅复制Buffer的内容，还复制Buffer数据的内容。
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
// 函数 : TCDBIndex::BUFGetIndex
// 用途 : 得到缓冲池中的指定块号对应的Index
// 原型 : long BUFGetIndex(DWORD dwBlock);
// 参数 : 块号
// 返回 : 指定块号对应的Index, 如果池中没有，则返回-1
// 说明 :
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
// 函数 : TCDBIndex::BUFOldestIndex
// 用途 : 返回BufferPool中最老的一条数据Index
// 原型 : long BUFOldestIndex();
// 参数 : 无
// 返回 : 最老的数据Index
// 说明 : 1. 如果有某个缓冲未申请内存，则该函数完成申请内存，并返回该
//           缓冲区的Index。
//        2. 用m_nRefreshCount维护数据的新旧，该数越小越旧。
//        3. Index为0固定为文件头。
//==========================================================================
long TCDBIndex::BUFOldestIndex()
{
    long i;
    long nSmallestRefreshCount;
    long nSmallestIndex;

    ASSERT(BUFFER_POOL_MAX >= 3);

    nSmallestRefreshCount = -1;

    //======= 1. 循环扫描每一个缓冲区 ========
    for (i = 1; i < BUFFER_POOL_MAX; i++)
    {
        //==== 2. 如果该缓冲还未被使用，则申请内存，并返回该Index ======
        if (m_BufferPool[i].m_nRefreshCount == 0)
        {
            TFileHeader fhFileHeader;

            fhFileHeader = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
            m_BufferPool[i].m_bfBuffer = DBAllocBuf(fhFileHeader->m_wBlockSize
                    + 2 * fhFileHeader->m_wRecordSize);
            m_BufferPool[i].m_nRefreshCount = BUFRefreshCount();
            return i;
        }

        //====== 3. 比较得到最旧的缓冲区Index =====
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
// 函数 : TCDBIndex::BUFRefresh
// 用途 : 更新缓冲区池中的记录
// 原型 : void BUFRefresh(TBuffer buf);
// 参数 : 缓冲区指针
// 返回 : 无
// 说明 :
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
// 函数 : TCDBIndex::BUFRelease
// 用途 : 释放缓冲区池中所有Buffer
// 原型 : void TCDBIndex::BUFRelease();
// 参数 : 无
// 返回 : 无
// 说明 :
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
// 函数 : TCDBIndex::BUFWriteAllUpdated
// 用途 : 将所有更新过的块都写入文件
// 原型 : void TCDBIndex::BUFWriteAllUpdated();
// 参数 : 无
// 返回 : 无
// 说明 : 1. 该函数在析构和缓冲区溢出时调用
//        2. 在缓冲区溢出时，实际上也可以只更新溢出的缓冲区。但为了保证数
//           数尽量完整，所以调用该函数来写文件。
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
// 函数 : TCDBIndex::DBAdd
// 用途 : 插入一条用户数据
// 原型 : void DBAdd(char *szUserData)
// 参数 : 用户数据
// 返回 : 无
// 说明 :
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
// 函数 : TCDBIndex::DBAddIDX
// 用途 : 插入一条用户数据
// 原型 : void DBAddIDX(char *szUserData)
// 参数 : 用户数据
// 返回 : 无
// 说明 : 
//==========================================================================
TEDBAddResult TCDBIndex::DBAddIDX(char *szUserData)
{
    //==== 0. 必要的初始化工作 =====
    TFileHeader fh;
    TIndexHeader ihIndexHeader;
    TBuffer buf;
    char *szRBuf;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;//文件头数据
    buf = m_df->m_bfBuffer;//索引内容数据
    ihIndexHeader = (TIndexHeader)buf->m_pBufData;//索引头数据
    szRBuf = buf->m_pBufData + sizeof(TSDBIndexHeader);//指向索引头之后的第一个数据插入点

    //====== 2. 如果没有根节点，则取出下一个可用节点 =====
    if (fh->m_dwRootPointer == 0)
    {
        DBGetNextAvail(buf);
    }

    //====== 3. 否则，搜索到插入点 =====
    else
    {
        DBFindInsertIDX(szUserData, fh->m_wKeySize);
        //===== 4. 如果有完全匹配的Key值，而文件中又不允许重复键，则抛出例外 ===
        if (m_dwDBMatchBlock != 0)
            if ((fh->m_wFileStat & fsDBDupAllowed) == 0)
            {
                if (s_m_bDBAddThrowException)
                    throw TCException("DBIndex Dup Not Allowed");
                else
                    return arDuplicateKey;
            }
    }

    //======= 5. 块中的记录数加1, 填充增加的块号及记录号 ===
    WORD wRecord;

    ihIndexHeader->m_wRecCount ++;
    wRecord = buf->m_wBufRecordIndex;

    m_dwDBAddBlock = buf->m_dwBufCurrentBlock;
    m_wDBAddRecord = buf->m_wBufRecordIndex;

    //===== 6. 如果搜索到的记录数不比块中的记录数大，向右偏移一条记录 =======
    char *szSrc, *szDest;
    WORD wCount;

    if (wRecord <= ihIndexHeader->m_wRecCount)
    {   szSrc = szRBuf + (wRecord - 1) * fh->m_wRecordSize;
        szDest = szSrc + fh->m_wRecordSize;
        wCount = (ihIndexHeader->m_wRecCount - wRecord + 1) * fh->m_wRecordSize;
        // 此处原来用的是memcpy, 但数据有重叠，改为memmove
        memmove(szDest, szSrc, wCount);
    }

    //======= 7. 填充记录值(子节点为0, 填入用户数据) ====
//    TIndexRecord irIndexRecord;

//    irIndexRecord = (TIndexRecord)szSrc;
//    irIndexRecord->dwIndexPointer = 0;
//    szDest = (char *)irIndexRecord + fh->m_wControlSize;
    memset(szSrc, 0, sizeof(TSDBIndexRecord));
    szDest = szSrc + fh->m_wControlSize;

    memcpy(szDest, szUserData, fh->m_wDataSize);

    //======= 8. 记录数加1, 判断是否分裂页面，写入文件头 =====
    fh->m_dwRecordCount ++;
    DBSplitBlockIDX();
    DBPutBlock(m_df->m_bfFileHeader);
    return arSucceed;
}

//==========================================================================
// 函数 : TCDBIndex::DBAllocBuf
// 用途 : 分配BUFFER空间
// 原型 : TBuffer DBAllocBuf(long nSize);
// 参数 : Buffer大小
// 返回 : Buffer指针
// 说明 : 该函数分配Buffer的空间。如果指定大小，则再分配数据。
//==========================================================================
TBuffer TCDBIndex::DBAllocBuf(long nSize)
{
    //========= 1. 分配Buffer空间 ==========
    TBuffer  buf;
    char   *calloc();

    buf = new TSDBDataBuf;
    memset(buf, 0, sizeof(TSDBDataBuf));

    if (buf == NULL)
        throw TCException("DBIndex Alloc Error");

    //========= 2. 如果指定的Size不为0，则分配m_pBufData成员变量 =======
    if (nSize != 0)
    {
        ASSERT(nSize <= 65535);

        // 指定的Size填入m_wBufSize中        
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
// 函数 : TCDBIndex::DBCheckDataFile
// 用途 : 判断是否有效的DataFile
// 原型 : void TCDBIndex::DBCheckDataFile();
// 参数 : DataFile
// 返回 : 无
// 说明 : 断言验证Datafile及其各成员变量的有效性。调用时DataFile是已打开状态。
//==========================================================================
void TCDBIndex::DBCheckDataFile()
{
    TFileHeader fh;

    //====== 1. df为不空 ==========
    ASSERT(m_df != NULL);

    //===== 2. 状态为已打开 ==========
    ASSERT((m_df->m_wStat & fsDBOpen) != 0);

    //====== 3. 文件头不为空，缓冲区不为空 =======

    ASSERT((m_df->m_bfFileHeader != NULL) && (m_df->m_bfBuffer != NULL));

    //====== 4. 文件类型为IndexFile ========
    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;

	if ( fh->m_wFileType != ftDBIndex )
		printf(" DBFileHeaderOpen:The File is :%s\n",(char*)m_fFile.GetFileName());
	
    ASSERT(fh->m_wFileType == ftDBIndex);

    //====== 5. TmpBuffer及AuxBuffer都不能为空 ====
    ASSERT((m_df->m_bfTmp != NULL) && (m_df->m_bfAux != NULL));
}

//==========================================================================
// 函数 : TCDBIndex::DBClose
// 用途 : 关闭索引文件并释放相关内存
// 原型 : void DBClose();
// 参数 : DataFile
// 返回 : 无
// 说明 : 
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
// 函数 : TCDBIndex::DBCreate
// 用途 : 新建索引文件
// 原型 : TDataFile DBCreate(TCString sFileName, TCString sOptions);
// 参数 : 文件名，选项字符串
// 返回 : 索引文件结构指针
// 说明 :
//==========================================================================
void TCDBIndex::DBCreate(TCString sFileName, TCString sOptions,
        TCString sOtherInfo)
{
    m_BufferPool[0].m_bfBuffer = DBAllocBuf(DB_FILE_HDR_SIZE);

    //======== 1. 分配DataFile的内存 =======
    m_df = new TSDBDataFile;
    memset(m_df, 0, sizeof(TSDBDataFile));
    if (m_df == NULL)
        throw TCException("DBIndex Alloc Error");

    //====== 2. 分配文件头的内存 ========
    TFileHeader fhFileHeader;
//%%%    m_df->m_bfFileHeader = DBAllocBuf(DB_FILE_HDR_SIZE);  //COMMENT20010601
    m_df->m_bfFileHeader = m_BufferPool[0].m_bfBuffer;         //%%%ADD20010601

    //====== 3. 将分配的内存同时赋予fhFileHeader，便于以后的访问 ======
    fhFileHeader = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;

    //===== 4. 设置文件头的当前缓冲区大小为DB_FILE_HDR_SIZE ======
    m_df->m_bfFileHeader->m_wBufCurrentSize = DB_FILE_HDR_SIZE;

    //===== 5. 设置DataFile的文件名域 =====
    ASSERT(Length(sFileName) < INDEX_FILE_NAME_MAX);
    strcpy(m_df->m_szFileName, (char *)sFileName);

    //====== 6. 分析数据文件选项 ========
    DBFileOptions(fhFileHeader, sOptions);

    //====== 7. 填写fhFileHeader中所有域的内容 ======
    DBFileHeaderCreate(fhFileHeader);

    FillPadr(fhFileHeader->m_szOtherInfo, sOtherInfo,
            sizeof(fhFileHeader->m_szOtherInfo));

    //====== 8. 打开文件 ===========
    m_fFile.Open(m_df->m_szFileName, omWrite);

    //======== 9. 设置文件的状态为DBOpen =====
    m_df->m_wStat = fsDBOpen;

    //===== 10. 分配Buffer, Tmp, Aux内存 ==========
    m_df->m_bfBuffer = DBAllocBuf(fhFileHeader->m_wBlockSize
            + 2 * fhFileHeader->m_wRecordSize);

    m_df->m_bfTmp = DBAllocBuf(fhFileHeader->m_wBlockSize
            + 2 * fhFileHeader->m_wRecordSize);

    m_df->m_bfAux = DBAllocBuf(fhFileHeader->m_wBlockSize
            + 2 * fhFileHeader->m_wRecordSize);

    // 所分配的内存大小都为BlockSize加2倍的RecordSize, 但BufSize成员
    // 变量中填的是BlockSize。
    m_df->m_bfBuffer->m_wBufSize = fhFileHeader->m_wBlockSize;
    m_df->m_bfTmp->m_wBufSize = fhFileHeader->m_wBlockSize;
    m_df->m_bfAux->m_wBufSize = fhFileHeader->m_wBlockSize;

    //====== 11. 写入DataFile文件 =========
    // m_wBufCurrentBlock = 0, 写入文件头
    DBPutBlock(m_df->m_bfFileHeader);

}

//==========================================================================
// 函数 : TCDBIndex::DBExtend
// 用途 : 扩展文件存储空间
// 原型 : void DBExtend(TBuffer buf);
// 参数 : 索引文件指针, Buffer
// 返回 : 无
// 说明 :
//==========================================================================
void TCDBIndex::DBExtend(TBuffer buf)
{
    TFileHeader fh;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;

    //===== 1. 最后块号+1 ======
    fh->m_dwLastBlock ++;

    //==== 2. Buffer中的当前块=最后块号
    buf->m_dwBufCurrentBlock = fh->m_dwLastBlock;

    //====== 3. Buffer的当前大小 = 块大小 ======
    buf->m_wBufCurrentSize = fh->m_wBlockSize;

    //======= 4. 清空Buffer中的数据 ======
    memset(buf->m_pBufData, 0, buf->m_wBufCurrentSize);

    //===== 5. 下一个有效的Block即为当前Block ====
    TFreeRecord frFreeRecord;
    frFreeRecord = (TFreeRecord)buf->m_pBufData;
    frFreeRecord->m_cStat = fsFree;
    frFreeRecord->m_dwNext = fh->m_dwNextAvail;
    fh->m_dwNextAvail = buf->m_dwBufCurrentBlock;

    //==== 6. 写入Buffer, 写入文件头 ======
    DBPutBlock(m_df->m_bfBuffer);
    DBPutBlock(m_df->m_bfFileHeader);
}

//==========================================================================
// 函数 : TCDBIndex::DBFileHeaderCreate
// 用途 : 填充文件头TFileHeader结构，填入相应数值
// 原型 : void DBFileHeaderCreate(TFileHeader fh);
// 参数 : 文件头
// 返回 : 无
// 说明 :
//==========================================================================
void TCDBIndex::DBFileHeaderCreate(TFileHeader fh)
{
    long nHeaderSize;

    FillPadr(fh->m_szFileTag, INDEX_FILE_TAG, sizeof(fh->m_szFileTag));
    if (TCSystem::ThisMachineByteOrder() == boIntel)
        fh->m_cCPUOrder = 'I';
    else
        fh->m_cCPUOrder = 'M';

    //===== 1. 给版本号赋值 ========
    fh->m_wDBVersion = DB_VERSION;

    ASSERT(fh->m_wFileType == ftDBIndex);

    //==== 2. 清除文件打开标志 ========
    fh->m_wFileStat &= ~fsDBOpen;
    fh->m_dwLastBlock = 0;

    //====== 3. 给BlockSize和ControlSize赋值，或赋给缺省值 =======
    if (fh->m_wBlockSize == 0)
        fh->m_wBlockSize = 512;

    if (fh->m_wControlSize == 0)
        fh->m_wControlSize = 4;

    //===== 4. 索引头大小 = TSDBIndexHeader的大小 + m_wControlSize;
    // 为便于计算，此处的索引头大小实际上等于索引头大小 + 尾部的块指针
    nHeaderSize = sizeof(TSDBIndexHeader) + fh->m_wControlSize;

    //==== 5. ControlSize += LinksCnt * LinksSize
    fh->m_wControlSize = fh->m_wControlSize
            + fh->m_wOwnerLinkCount * sizeof(TSDBOwnerLink)
            + fh->m_wMemberLinkCount * sizeof(TSDBMemberLink)
            + fh->m_wKeyLinkCount * sizeof(TSDBKeyLink);

    //===== 6. 记录的大小 = ControlSize + DataSize ========
    fh->m_wRecordSize = fh->m_wControlSize + fh->m_wDataSize;

    //==== 7. 调整Blocksize至少为两倍的记录大小+索引头大小，且为512的倍数 ====
    if (fh->m_wBlockSize < 2 * fh->m_wRecordSize + nHeaderSize)
        fh->m_wBlockSize = 2 * fh->m_wRecordSize + nHeaderSize;

    fh->m_wBlockSize = (fh->m_wBlockSize + 511) / 512 * 512;

    //===== 8. 记录数=0; 求得每块记录数; RootPointer=0; NextAvail=0; ====
    fh->m_dwRecordCount = 0;
    fh->m_wRecordsPerBlock = (fh->m_wBlockSize - nHeaderSize)
            / fh->m_wRecordSize;
    fh->m_dwRootPointer = 0;
    fh->m_dwNextAvail = 0;

    ASSERT(fh->m_wKeySize <= fh->m_wDataSize);
}

//==========================================================================
// 函数 : TCDBIndex::DBFileHeaderOpen
// 用途 : 检查打开索引文件的文件头的有效性
// 原型 : void DBFileHeaderOpen(TFileHeader fh);
// 参数 : 文件头指针
// 返回 : 无
// 说明 : 
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
// 函数 : TCDBIndex::DBFile1Option
// 用途 : 解析一个文件选项，并填入FileHeader中的相应属性
// 原型 : void DBFile1Option(TFileHeader fh, TCString sOption,
//              TCString sSubOption);
// 参数 : 文件头指针，选项名，选项值
// 返回 : 无
// 说明 : 该函数被DBFileOptions()调用
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
// 函数 : TCDBIndex::DBFileOptions
// 用途 : 分析DB文件的选项，以确定恰当的创建方式及其它参数
// 原型 : void DBFileOptions(TFileHeader fh, TCString sOptionStr);
// 参数 : 文件头指针，选项串
// 返回 : 无
// 说明 : 该函数逐个分析传入的各选项，并填充TFileHeader中相应信息。
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
// 函数 : TCDBIndex::DBFind
// 用途 : 搜索Index中的关键字
// 原型 : bool DBFind(char *szUserData, char *szKey,
//              long nKeySize = 0);
// 参数 : 索引文件指针，用户数据，关键字，关键字的大小
// 返回 : 是否搜索到
// 说明 : 如果搜索到，则DataFile中的成员变量m_dwPrevBlock, m_wPrevRecord
//        填入搜索到的值。否则这两个成员变量填零。
// 历史 : 2001.12.3 修改为if (!bFindIt) 加了个"!"
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

    if (!bFindIt)        // 原来是if (bFindIt) 2001.12.03改为现在的表达
    {   buf->m_dwBufCurrentBlock = 0;
        buf->m_wBufRecordIndex = 0;
    }

    m_df->m_dwPrevBlock = buf->m_dwBufCurrentBlock;
    m_df->m_wPrevRecord = buf->m_wBufRecordIndex;

    return bFindIt;
}

//==========================================================================
// 函数 : TCDBIndex::DBFindFirstIDX
// 用途 : 搜索Index中的关键字
// 原型 : bool DBFindFirstIDX(char *szUserData, char *szKey,
//              long nKeySize);
// 参数 : 索引文件指针，用户数据，关键字，关键字的大小
// 返回 : 是否搜索到
// 说明 : 如果未搜到，则返回false。否则返回true, 并在UserData中填入相应的
//        值。UserData应事先分配足够的内存。
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
// 函数 : TCDBIndex::DBFindInsertIDX
// 用途 : 搜索到指定关键字的插入点
// 原型 : void DBFindInsertIDX(char *szKey, long nKeySize);
// 参数 : 索引文件指针，关键字，关键字的大小
// 返回 : 无
// 说明 : 如果搜索到记录，则记录到m_dwDBMatchBlock及m_wDBMatchRecord中。
//        否则，buf的成员变量m_dwBufCurrentBlock及m_dwBufRecordIndex中记录
//        当前的插入点位置。
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

    //====== 1. 将找到的Block和Record都设为0 ========
    m_dwDBMatchBlock = 0;
    m_wDBMatchRecord = 0;

    //===== 2. 初始化各变量，将根Block的值赋予dwBlock =====
    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;
    ihIndexHeader = (TIndexHeader)buf->m_pBufData;
    dwBlock = fh->m_dwRootPointer;

    if (nKeySize == 0)
        nKeySize = fh->m_wKeySize;

    // ========== 3. 循环进行查找 =========
    while (dwBlock != 0)
    {
        //====== 4. 将Block的内容填入buf =======
        DBGetBlock(dwBlock, buf);

        //==== 5. 求得记录数据的实际开始位置 ======
        szRBuf = buf->m_pBufData + sizeof(struct TSDBIndexHeader);

        //======= 6. 逐个搜索Block中的各记录数据并进行比较 =====
        for (wRecord = 1; wRecord <= ihIndexHeader->m_wRecCount; wRecord++)
        {
            //===== 7. 取出IndexRecord(Block) ======
//            irIndexRecord = (TIndexRecord)szRBuf;     //0918
            memcpy(&irDBIndexRecord, szRBuf, sizeof(TSDBIndexRecord));

            //===== 8. IndexKey值还要加上ControlSize ====
            szIKey = szRBuf + fh->m_wControlSize;

            //====== 9. 对传入的Key值与当前Key值相比较 =====
            x = memcmp(szKey, szIKey, nKeySize);

            //===== 10. 如果相同，则赋值匹配项的当前值, 再进行搜索 ======
            if (x == 0)
            {
                m_dwDBMatchBlock = dwBlock;
                m_wDBMatchRecord = wRecord;

//                dwBlock = irIndexRecord->dwIndexPointer;      //0918
                dwBlock = irDBIndexRecord.dwIndexPointer;
                break;
            }

            //======= 11. 如果要搜索的Key值较小，则传出块号再进行搜索 ===
            if (x < 0)
            {
//                dwBlock = irIndexRecord->dwIndexPointer;      //0918
                dwBlock = irDBIndexRecord.dwIndexPointer;
                break;
            }

            //====== 12. 如果要搜索的Key值还是较大，则偏移到下一个Record =====
            szRBuf += fh->m_wRecordSize;

            //===== 13. 如果搜索到最后一个记录，则连到下一个节点
            if (wRecord == ihIndexHeader->m_wRecCount)
            {
//                irIndexRecord = (TIndexRecord)szRBuf;     //0918
//                dwBlock = irIndexRecord->dwIndexPointer;
                memcpy(&irDBIndexRecord, szRBuf, sizeof(TSDBIndexRecord));
                dwBlock = irDBIndexRecord.dwIndexPointer;
            }
        }   // end of for (wRecord = 1; ...
    }   // end of while (dwBlock != 0)

    //== 14. 在Buffer中记下当前的Record(当前的Buffer已在DBGetBlock中记录) ===
    buf->m_wBufRecordIndex = wRecord;
}

//==========================================================================
// 函数 : TCDBIndex::DBFreeBuffer
// 用途 : 释放Buffer
// 原型 : TBuffer DBFreeBuffer(TBuffer buf);
// 参数 : 要释放的Buffer
// 返回 : 下一个要释放的Buffer
// 说明 : 释放Buffer, 更新双向链表，并返回下一个要释放的Buffer。
// 调用方式举例 :
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
// 函数 : TCDBIndex::DBFreeDataFile
// 用途 : 释放DataFile的所有内存
// 原型 : void DBFreeDataFile();
// 参数 : 要释放的DataFile
// 返回 : 无
// 说明 : 该函数被DBClose()调用
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
// 函数 : TCDBIndex::DBGetBlock
// 用途 : 取得索引文件一个Buffer
// 原型 : void DBGetBlock(long nBlock, TBuffer buf);
// 参数 : 索引文件指针，块号，缓冲区Buffer
// 返回 : 无
// 说明 :
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

    //===== 1. 求出偏移量，如nBlock为0，则取文件头 ======
    if (dwBlock == 0)
    {   nOffset = 0;
        nCount = buf->m_wBufSize;
    }
    else
    {   nOffset = DB_FILE_HDR_SIZE + (dwBlock - 1) * fhFileHeader->m_wBlockSize;
        nCount = buf->m_wBufSize;
    }

    //=== 2. 读出数据存于buf->m_pBufData中 ========
    buf->m_dwBufCurrentBlock = 0;
    buf->m_wBufCurrentSize = 0;
    buf->m_wBufRecordIndex = 0;

    m_fFile.Seek(nOffset);
    nCount = m_fFile.Read(buf->m_pBufData, nCount);
    if (nCount < 0)
        throw TCException("DBIndex Read Error");

    if (nCount == 0)
        throw TCException("DBIndex End Of File");

    //====== 3. 更新buf中的块号及大小信息 ======
    buf->m_dwBufCurrentBlock = dwBlock;
    buf->m_wBufCurrentSize = nCount;
    BUFRefresh(buf);
}

//==========================================================================
// 函数 : TCDBIndex::DBGetNextAvail
// 用途 : 得到下一个可用块
// 原型 : void DBGetNextAvail(TBuffer buf);
// 参数 : 索引文件指针, 缓冲区
// 返回 : 无
// 说明 : 如果已无有效空间，则该函数调用DBExtend()进行扩展。
//==========================================================================
void TCDBIndex::DBGetNextAvail(TBuffer buf)
{
    TFileHeader fh;
    TFreeRecord frFreeRecord;
    TIndexHeader ihIndexHeader;
    DWORD dwBlock;
    char *szRBuf;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;

    //===== 1. 如该文件已没有可用空间，则扩展之 ====
    if (fh->m_dwNextAvail == NULL)
        DBExtend(buf);

    //==== 2. 读入新可用空间 =========
    dwBlock = fh->m_dwNextAvail;

    DBGetBlock(dwBlock, buf);

    //===== 3. 当前记录为1 ========
    buf->m_wBufRecordIndex = 1;
    szRBuf = buf->m_pBufData;

    //===== 4. 更新当前块的状态为在用, 更新下一个可用空间的值 =======
    frFreeRecord = (TFreeRecord)szRBuf;
    if (frFreeRecord->m_cStat != fsFree)
        throw TCException("DBIndex Invalid Free (IS NOT FREE STAT)");

    frFreeRecord->m_cStat = fsInUse;
    fh->m_dwNextAvail = frFreeRecord->m_dwNext;

    //==== 5. 如果没有根块，则设本块为根块 ========
    if (fh->m_dwRootPointer == 0)
        fh->m_dwRootPointer = dwBlock;

    //===== 6. 清除当前块的父节点属性及记录数属性 =====
    ihIndexHeader = (TIndexHeader)szRBuf;
    ihIndexHeader->m_dwParent = 0;
    ihIndexHeader->m_wRecCount = 0;
}

//==========================================================================
// 函数 : TCDBIndex::DBGetOtherInfo
// 用途 : 得到索引文件的其它附加信息
// 原型 : TCString DBGetOtherInfo();
// 参数 : 无
// 返回 : 附加信息串
// 说明 : 附加信息是在DBCreate调用中写入文件的。
//==========================================================================
TCString TCDBIndex::DBGetOtherInfo()
{
    TFileHeader fh;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;

    return AllTrim(TCString(fh->m_szOtherInfo, sizeof(fh->m_szOtherInfo)));
}

//==========================================================================
// 函数 : TCDBIndex::DBGetParentIDX
// 用途 : 得到父节点
// 原型 : bool DBGetParentIDX();
// 参数 : 索引文件指针
// 返回 : 是否找到（如果返回否，则表示已到文件尾）
// 说明 : 找到当前buffer的父节点。读入buffer中，记录节点位置到Buffer的
//        m_wBufRecordIndex成员变量中。
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

    //======= 1. 记下当前块号 =====
    dwHoldBlock = buf->m_dwBufCurrentBlock;

    //==== 2. 如果当前块没有父块，返回false ====
    if (ihIndexHeader->m_dwParent == 0)
        return false;

    //==== 3. 读入父块 ======
    DBGetBlock(ihIndexHeader->m_dwParent, buf);

    //==== 4. 搜索父块的每一个节点，匹配子块索引块指针 ======
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

    //====== 5. 在buffer中的m_wBufRecordIndex记下节点号 ===
    buf->m_wBufRecordIndex = wRecord;

    return true;
}

//==========================================================================
// 函数 : TCDBIndex::DBOpen
// 用途 : 打开数据库索引文件
// 原型 : TDataFile DBOpen(TCString sFileName);
// 参数 : 文件名
// 返回 : 索引文件结构指针
// 说明 :
//==========================================================================
void TCDBIndex::DBOpen(TCString sFileName)
{
    if (m_df != NULL)
        DBClose();

    m_BufferPool[0].m_bfBuffer = DBAllocBuf(DB_FILE_HDR_SIZE);

    TFileHeader fhFileHeader;

    //======= 1. 分配DataFile的内存 =========
    m_df = new TSDBDataFile;
    memset(m_df, 0, sizeof(TSDBDataFile));
    if (m_df == NULL)
        throw TCException("DBIndex Alloc Error");

    //====== 2. 分配文件头的内存 ========
//%%%    m_df->m_bfFileHeader = DBAllocBuf(DB_FILE_HDR_SIZE);  //COMMENT20010601
    m_df->m_bfFileHeader = m_BufferPool[0].m_bfBuffer;         //%%%ADD20010601

    //====== 3. 将分配的内存同时赋予fhFileHeader，便于以后的访问 ======
    fhFileHeader = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;

    //===== 4. 设置DataFile的文件名域 =====
    ASSERT(Length(sFileName) < INDEX_FILE_NAME_MAX);
    strcpy(m_df->m_szFileName, (char *)sFileName);

    //===== 5. 打开文件 ========
    if (m_bOpenWithReadOnly)
        m_fFile.Open(m_df->m_szFileName, omRead | omShared);
    else
        m_fFile.Open(m_df->m_szFileName, omRead | omWrite);

    //======== 6. 设置文件的状态为DBOpen =====
    m_df->m_wStat = fsDBOpen;

    //======= 7. 读入FileHeader ========
    DBGetBlock(0, m_df->m_bfFileHeader);

    //===== 8. 分配Buffer, Tmp, Aux内存 ==========
    m_df->m_bfBuffer = DBAllocBuf(fhFileHeader->m_wBlockSize
            + 2 * fhFileHeader->m_wRecordSize);

    m_df->m_bfTmp = DBAllocBuf(fhFileHeader->m_wBlockSize
            + 2 * fhFileHeader->m_wRecordSize);

    m_df->m_bfAux = DBAllocBuf(fhFileHeader->m_wBlockSize
            + 2 * fhFileHeader->m_wRecordSize);

    // 所分配的内存大小都为BlockSize加2倍的RecordSize, 但BufSize成员
    // 变量中填的是BlockSize。
    m_df->m_bfBuffer->m_wBufSize = fhFileHeader->m_wBlockSize;
    m_df->m_bfTmp->m_wBufSize = fhFileHeader->m_wBlockSize;
    m_df->m_bfAux->m_wBufSize = fhFileHeader->m_wBlockSize;

    //======= 9. 检查FileHeader中各变量的有效性 ========
    DBFileHeaderOpen(fhFileHeader);
}

//==========================================================================
// 函数 : TCDBIndex::DBOpenR
// 用途 : 以只读方式打开数据库索引文件
// 原型 : TDataFile DBOpenR(TCString sFileName);
// 参数 : 文件名
// 返回 : 无
// 说明 :
// 历史 : 2001.11.29 Oldix, 增加本函数
//==========================================================================
void TCDBIndex::DBOpenR(TCString sFileName)
{
    m_bOpenWithReadOnly = true;
    DBOpen(sFileName);
    m_bOpenWithReadOnly = false;
}

//==========================================================================
// 函数 : TCDBIndex::DBPutBlock
// 用途 : 更新写入缓冲
// 原型 : void DBPutBlock(TBuffer buf);
// 参数 : Buffer
// 返回 : 无
// 说明 : 当缓冲区满，或达到检查点时才进行写盘操作
//==========================================================================
void TCDBIndex::DBPutBlock(TBuffer buf)
{
    ASSERT(buf != NULL);

    ASSERT(buf->m_wBufCurrentSize != 0);

    BUFRefresh(buf, true);
}

//==========================================================================
// 函数 : TCDBIndex::DBPutBlock
// 用途 : 向文件写入一个Buffer
// 原型 : void DBPutBlock(TBuffer buf);
// 参数 : Buffer
// 返回 : 无
// 说明 :
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

    //===== 1. 求得指定Buffer在文件中的偏移量 =========
    fhFileHeader = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;

    if (buf->m_dwBufCurrentBlock == 0)
        nOffset = 0;
    else
        nOffset = DB_FILE_HDR_SIZE +
                (buf->m_dwBufCurrentBlock - 1) * fhFileHeader->m_wBlockSize;

    //===== 2. 定位文件指针 =========
    m_fFile.Seek(nOffset);

    //===== 3. 写入指定Buffer ========
    nCount = m_fFile.Write(buf->m_pBufData, buf->m_wBufCurrentSize);
    if (nCount !=  buf->m_wBufCurrentSize)
        throw TCException("DBIndex Write Error");

}

//==========================================================================
// 函数 : TCDBIndex::DBReadFirst
// 用途 : 读取索引文件的第一条数据
// 原型 : bool DBReadFirst(char *szUserData);
// 参数 : 文件DataFile, 数据存放
// 返回 : 有没有找到记录
// 说明 : 找到的记录数据填入szUserData中, szUserData要事先分配足够的内存。
// 调用举例 :
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

    //===== 1. 如果记录数为0，则返回false =====
    if (fh->m_dwRecordCount == 0)
        return false;

    //======= 2. 调用DBReadFirstIDX以获得数据 =====
    bFindIt = DBReadFirstIDX(fh->m_dwRootPointer, szUserData);
    if (!bFindIt)
    {
        buf->m_dwBufCurrentBlock = 0;
        buf->m_wBufRecordIndex = 0;
        buf->m_wBufCurrentSize = 0;
    }

    //=== 3. 将当前位置及大小存于m_dwPrevBlock, m_wPrevRecord, m_wPrevVSize中 ==
    m_df->m_dwPrevBlock = buf->m_dwBufCurrentBlock;
    m_df->m_wPrevRecord = buf->m_wBufRecordIndex;
    m_df->m_wPrevVSize = buf->m_wBufCurrentSize;

    return bFindIt;
}

//==========================================================================
// 函数 : TCDBIndex::DBReadFirstIDX
// 用途 : 读取指定Block所指向的第一条记录
// 原型 : bool DBReadFirstIDX(DWORD dwBlock, char *szUserData);
// 参数 : 文件DataFile, 块号，数据存放
// 返回 : 有没有找到记录
// 说明 : 找到的记录数据填入szUserData中, szUserData要事先分配足够的内存。
//==========================================================================
bool TCDBIndex::DBReadFirstIDX(DWORD dwBlock, char *szUserData)
{
    TFileHeader fh;
    TBuffer buf;
    TIndexHeader ihIndexHeader;
//    TIndexRecord irIndexRecord;       //0918
    TSDBIndexRecord irDBIndexRecord;
    char *szSrc;

    //===== 1. 如果块号为0，返回false =========
    if (dwBlock == 0)
        return false;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;

    ihIndexHeader = (TIndexHeader)buf->m_pBufData;

//    irIndexRecord = (TIndexRecord)(buf->m_pBufData
//                + sizeof(TSDBIndexHeader));           //0918
    char *pIndexRecord;
    pIndexRecord = buf->m_pBufData + sizeof(TSDBIndexHeader);

    //===== 2. 一直搜索到第一条记录(块号为0) =====
    while (dwBlock)
    {
        DBGetBlock(dwBlock, buf);

        if (ihIndexHeader->m_wRecCount == 0)
            throw TCException("DBIndex Invalid Index");

//        dwBlock = irIndexRecord->dwIndexPointer;      //0918
        memcpy(&irDBIndexRecord, pIndexRecord, sizeof(TSDBIndexRecord));
        dwBlock = irDBIndexRecord.dwIndexPointer;
    }

    //====== 3. 复制相应数据 ======
    szSrc = buf->m_pBufData + sizeof(TSDBIndexHeader) + fh->m_wControlSize;
    buf->m_wBufRecordIndex = 1;
    memcpy(szUserData, szSrc, fh->m_wDataSize);

    return true;
}

//==========================================================================
// 函数 : TCDBIndex::DBReadLast
// 用途 : 读取索引文件的最后一条数据
// 原型 : bool DBReadLast(char *szUserData);
// 参数 : 文件DataFile, 数据存放
// 返回 : 有没有找到记录
// 说明 : 找到的记录数据填入szUserData中, szUserData要事先分配足够的内存。
// 调用举例 :
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

    //===== 1. 如果记录数为0，则返回false =====
    if (fh->m_dwRecordCount == 0)
        return false;

    //======= 2. 调用DBReadFirstIDX以获得数据 =====
    bFindIt = DBReadLastIDX(fh->m_dwRootPointer, szUserData);
    if (!bFindIt)
    {
        buf->m_dwBufCurrentBlock = 0;
        buf->m_wBufRecordIndex = 0;
        buf->m_wBufCurrentSize = 0;
    }

    //=== 3. 将当前位置及大小存于m_dwPrevBlock, m_wPrevRecord, m_wPrevVSize中 ==
    m_df->m_dwPrevBlock = buf->m_dwBufCurrentBlock;
    m_df->m_wPrevRecord = buf->m_wBufRecordIndex;
    m_df->m_wPrevVSize = buf->m_wBufCurrentSize;

    return bFindIt;
}

//==========================================================================
// 函数 : TCDBIndex::DBReadLastIDX
// 用途 : 读取指定Block所指向的最后一条记录
// 原型 : bool DBReadLastIDX(DWORD dwBlock, char *szUserData);
// 参数 : 文件DataFile, 块号，数据存放
// 返回 : 有没有找到记录
// 说明 : 找到的记录数据填入szUserData中, szUserData要事先分配足够的内存。
//==========================================================================
bool TCDBIndex::DBReadLastIDX(DWORD dwBlock, char *szUserData)
{
    TFileHeader fh;
    TBuffer buf;
    TIndexHeader ihIndexHeader;
//    TIndexRecord irIndexRecord;       //0918
    TSDBIndexRecord irDBIndexRecord;
    char *szSrc;

    //===== 1. 如果块号为0，返回false =========
    if (dwBlock == 0)
        return false;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    buf = m_df->m_bfBuffer;

    ihIndexHeader = (TIndexHeader)buf->m_pBufData;

    //===== 2. 一直搜索到第一条记录(块号为0) =====
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

    //====== 3. 复制相应数据 ======
    szSrc = buf->m_pBufData + sizeof(TSDBIndexHeader)
            + (ihIndexHeader->m_wRecCount - 1) * fh->m_wRecordSize
            + fh->m_wControlSize;
    buf->m_wBufRecordIndex = ihIndexHeader->m_wRecCount;
    memcpy(szUserData, szSrc, fh->m_wDataSize);

    return true;
}

//==========================================================================
// 函数 : TCDBIndex::DBReadNext
// 用途 : 读取下一条记录
// 原型 : bool DBReadNext(char *szUserData);
// 参数 : 文件DataFile, 数据存放
// 返回 : 有没有找到记录
// 说明 : 1. 更新DataFile的m_dwPrevBlock, m_wPrevRecord
//        2. 找到的记录数据填入szUserData中, szUserData要事先分配足够的内存。
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
// 函数 : TCDBIndex::DBReadNextIDX
// 用途 : 读取下一条索引记录
// 原型 : bool DBReadNextIDX(char *szUserData);
// 参数 : 文件DataFile, 数据存放
// 返回 : 有没有找到记录
// 说明 : 1. 搜索DataFile中的m_dwPrevBlock及m_wPrevRecord的下一条记录。
//        2. 找到的记录数据填入szUserData中, szUserData要事先分配足够的内存。
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

    //====== 1. 读取当前块 ======
    DBGetBlock(m_df->m_dwPrevBlock, buf);

    buf->m_wBufRecordIndex = m_df->m_wPrevRecord;

    //== 2. 如当前记录为块中的最后记录，则一直向上搜索到拥有有效数据的父节点 ===
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

    //===== 3. 当前记录加1 ========
    buf->m_wBufRecordIndex ++;

    char *pIndexRecord;
    pIndexRecord = buf->m_pBufData + sizeof(TSDBIndexHeader)
            + (buf->m_wBufRecordIndex - 1) * fh->m_wRecordSize;
    memcpy(&irDBIndexRecord, pIndexRecord, sizeof(irDBIndexRecord));
//    irIndexRecord = (TIndexRecord)(buf->m_pBufData + sizeof(TSDBIndexHeader)
//            + (buf->m_wBufRecordIndex - 1) * fh->m_wRecordSize);  //0918

    //====== 4. 如果块号指针不为零，则读取第一条记录 ======
//    if (irIndexRecord->dwIndexPointer != 0)       //0918
//        return DBReadFirstIDX(irIndexRecord->dwIndexPointer, szUserData);
    if (irDBIndexRecord.dwIndexPointer != 0)
        return DBReadFirstIDX(irDBIndexRecord.dwIndexPointer, szUserData);

    //===== 5. 如果当前记录大于本块记录，则递归调用本函数（搜索父节点） ====
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
// 函数 : TCDBIndex::DBReadPrev
// 用途 : 读取上一条记录
// 原型 : bool DBReadPrev(char *szUserData);
// 参数 : 文件DataFile, 数据存放
// 返回 : 有没有找到记录
// 说明 : 1. 更新DataFile的m_dwPrevBlock, m_wPrevRecord
//        2. 找到的记录数据填入szUserData中, szUserData要事先分配足够的内存。
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
// 函数 : TCDBIndex::DBReadPrevIDX
// 用途 : 读取上一条索引记录
// 原型 : bool DBReadPrevIDX(char *szUserData);
// 参数 : 文件DataFile, 数据存放
// 返回 : 有没有找到记录
// 说明 : 1. 搜索DataFile中的m_dwPrevBlock及m_wPrevRecord的上一条记录。
//        2. 找到的记录数据填入szUserData中, szUserData要事先分配足够的内存。
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

    //====== 1. 读取当前块 ======
    DBGetBlock(m_df->m_dwPrevBlock, buf);

    //===== 2. 定位到当前记录 ========
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
        //== 2. 如当前记录为块中的第一条记录，则一直向上搜索到父节点 ===
        while (buf->m_wBufRecordIndex == 1)
            if (!DBGetParentIDX())
                return false;

        //===== 3. 往前跳一个记录 ====
        buf->m_wBufRecordIndex --;

        szSrc = buf->m_pBufData + sizeof(TSDBIndexHeader)
                + (buf->m_wBufRecordIndex - 1) * fh->m_wRecordSize
                + fh->m_wControlSize;
        memcpy(szUserData, szSrc, fh->m_wDataSize);
        return true;
    }

    //====== 4. 如果块号指针不为零，则读取最后一条记录 ======
//    if (irIndexRecord->dwIndexPointer != 0)       //0918
//        return DBReadLastIDX(irIndexRecord->dwIndexPointer, szUserData);
    if (irDBIndexRecord.dwIndexPointer != 0)
        return DBReadLastIDX(irDBIndexRecord.dwIndexPointer, szUserData);

    //====== 5. 读取前一条记录 =======
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
// 函数 : TCDBIndex::DBSplitBlockIDX
// 用途 : 判断某块是否要分裂，并进行写文件操作
// 原型 : void DBSplitBlockIDX();
// 参数 : 文件DataFile
// 返回 : 无
// 说明 : 
//==========================================================================
void  TCDBIndex::DBSplitBlockIDX()
{
    //======== 0. 适当的初始化操作 ============
    TFileHeader fh;
    TBuffer bfBuffer, bfTmpBuffer;
    TIndexHeader ihIndexHeader;

    fh = (TFileHeader)m_df->m_bfFileHeader->m_pBufData;
    bfBuffer = m_df->m_bfBuffer;
    bfTmpBuffer = m_df->m_bfTmp;

    ihIndexHeader = (TIndexHeader)bfBuffer->m_pBufData;

    //==== 1. 如果当前块的记录数没有超出限制，则写文件并退出函数 =====
    if (ihIndexHeader->m_wRecCount <= fh->m_wRecordsPerBlock)
    {   DBPutBlock(bfBuffer);
        return;
    }

    //====== 2. 设置各Buffer的值（互换Buffer和TmpBuffer） ===================
    TBuffer bfAuxBuffer;

    bfBuffer = m_df->m_bfTmp;
    bfTmpBuffer = m_df->m_bfBuffer;
    m_df->m_bfBuffer = bfBuffer;
    m_df->m_bfTmp = bfTmpBuffer;
    bfAuxBuffer = m_df->m_bfAux;

    //==== 3. 得到下一个可用空间 ======
    DBGetNextAvail(bfBuffer);
    //========= 4. 给索引块头赋值 =======
    TIndexHeader ihTmpIndexHeader, ihAuxIndexHeader;
    ihIndexHeader = (TIndexHeader)bfBuffer->m_pBufData;
    ihTmpIndexHeader = (TIndexHeader)bfTmpBuffer->m_pBufData;
    ihAuxIndexHeader = (TIndexHeader)bfAuxBuffer->m_pBufData;

    //======= 5. 赋值欲分裂的块号及新分配的块号 ========
    DWORD dwLeftBlock, dwRightBlock;
    dwLeftBlock = bfTmpBuffer->m_dwBufCurrentBlock;      // 欲分裂的块号
    dwRightBlock = bfBuffer->m_dwBufCurrentBlock;        // 新分配的块号

    //======= 6. 分裂的左右两边的记录数 =======
    WORD wCount1, wCount2;
    wCount1 = ihTmpIndexHeader->m_wRecCount / 2;
    wCount2 = ihTmpIndexHeader->m_wRecCount - (wCount1 + 1);

    //====== 7. 给新分配的块头赋值父块号及记录数 =====
    ihIndexHeader->m_dwParent = ihTmpIndexHeader->m_dwParent;
    ihIndexHeader->m_wRecCount = wCount2;

    //====== 8. 新分配出的块填充记录数据, 写入文件 =======
    char *szSrc, *szDest;
    WORD wCount;

    szSrc = bfTmpBuffer->m_pBufData + sizeof(TSDBIndexHeader)
            + (wCount1 + 1) * fh->m_wRecordSize;
    szDest = bfBuffer->m_pBufData + sizeof(TSDBIndexHeader);
    wCount = (wCount2 + 1) * fh->m_wRecordSize;
    memcpy(szDest, szSrc, wCount);
    DBPutBlock(bfBuffer);
    //==== 9. 如果加入的节记录落在分裂出的块中，则更新AddBlock及AddRecord =====
    if (m_dwDBAddBlock == bfTmpBuffer->m_dwBufCurrentBlock)
        if (m_wDBAddRecord > wCount1 + 1)
        {
            m_dwDBAddBlock = bfBuffer->m_dwBufCurrentBlock;
            m_wDBAddRecord -= wCount1 + 1;
        }

    //=== 10. 如果是根块，则扩展一块以做根块，并将当前块指向根块 =====
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
    //==== 11. 如果不是根块，则读取父节点, 并循环查找父节点中对应的记录位置 ===
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

    //======= 12. 使父节点指向新分配出来的节点 ========
//    irIndexRecord->dwIndexPointer = dwRightBlock;         //0918
    irDBIndexRecord.dwIndexPointer = dwRightBlock;
    memcpy(szRBuf, &irDBIndexRecord, sizeof(TSDBIndexRecord));

    //====== 13. 如果节点恰好落在中间，则该节点即为父节点 =====
    if (m_dwDBAddBlock == bfTmpBuffer->m_dwBufCurrentBlock)
        if (m_wDBAddRecord == wCount1 + 1)
        {
            m_dwDBAddBlock = bfBuffer->m_dwBufCurrentBlock;
            m_wDBAddRecord = wRecord;
        }

    //====== 14. 父节点相关记录向后移动，以填入新记录 =======
    szSrc = szRBuf;
    szDest = szRBuf + fh->m_wRecordSize;
    // 因最后还有一项IndexRecord, 所以要多复制一条记录
    wCount = ((ihIndexHeader->m_wRecCount + 1) - wRecord + 1)
            * fh->m_wRecordSize;
    // 原来用的是memcpy, 因数据有重叠，改为memmove
    memmove(szDest, szSrc, wCount);

    //===== 15. 将中间节点的内容移到父节点中 =======
    szSrc = bfTmpBuffer->m_pBufData + sizeof(TSDBIndexHeader)
            + wCount1 * fh->m_wRecordSize;
    szDest = szRBuf;
    wCount = fh->m_wRecordSize;
    memcpy(szDest, szSrc, wCount);

    //====== 16. 父节点的下级节点连到左节点(老节点), 块中记录数+1 ========
//    irIndexRecord->dwIndexPointer = dwLeftBlock;      //0918
    irDBIndexRecord.dwIndexPointer = dwLeftBlock;
    memcpy(szRBuf, &irDBIndexRecord, sizeof(TSDBIndexRecord));

    ihIndexHeader->m_wRecCount ++;

    //======= 17. 更新原节点记录数，写入原节点 ======
    ihTmpIndexHeader->m_wRecCount = wCount1;
    DBPutBlock(bfTmpBuffer);

    //======== 18. 取出右节点，如右节点的父节点不对，则更新，写入 ======
    DBGetBlock(dwRightBlock, bfTmpBuffer);
    if (ihTmpIndexHeader->m_dwParent != bfBuffer->m_dwBufCurrentBlock)
    {
        ihTmpIndexHeader->m_dwParent = bfBuffer->m_dwBufCurrentBlock;
        DBPutBlock(bfTmpBuffer);
    }

    //====== 19. 逐一取出右结点的每一个子节点，并更新其父节点位置 ====
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

    //====== 20. 递归调用，以确定父节点是否需要分裂 =======
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
// 函数 : TCDBIndex::Flush
// 用途 : 将一些变化情况写入硬盘
// 原型 : void Flush();
// 参数 : 无
// 返回 : 无
// 说明 : 该函数一般在一个阶段操作完成以后调用，以保证数据的完整性。
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
    //==== 1. 得到文件"_src_dat"存放的目录名 =====
    TCString sDirectory;

    printf("Input the Directory the File \"_src_dat\" put:");
    scanf("%s", data);
    sDirectory = TCString(data);

    //==== 2. 得到"_src_dat"存放的全路径文件名 ====
    TCString sFileName;
    sFileName = MergePathAndFile(sDirectory, "_src_dat");
    if (!FileExists(sFileName))
    {
        printf("The File not Exists.");
        return;
    }

    //======= 3. 产生Index文件 ======
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

    //========= 4. 读取数据并进行插入 =======
    //=== 4.1 打开数据文件及索引文件 =====
    TCFileStream fsDataFile, fsReport;
    fsDataFile.Open(sFileName, omRead|omText);
//    dbIndex.DBOpen(sIndexFileName);
    fsReport.Open(MergePathAndFile(sDirectory, "perf_report.txt"),
            omWrite | omText);

    //===== 4.2 循环读取数据，并进行插入 ======
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

    //====== 4.3 关闭文件 ==========
    dbIndex.DBClose();
    fsDataFile.Close();
    fsReport.Close();

    //====== 5. 正序读出索引文件内容 =====
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

    //====== 5. 反序读出索引文件内容 =====
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

//    TCDBIndex::BUFSetLazyWrite(true);     //缺省即为true
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






