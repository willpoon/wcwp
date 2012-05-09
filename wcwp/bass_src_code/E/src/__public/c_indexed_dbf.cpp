//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_indexed_dbf.h"

//---------------------------------------------------------------------------
// 常数 : REC_NO_LENGTH
// 用途 : 记录号在索引文件中的长度表示, 8 => 支持不超过一亿条记录
//---------------------------------------------------------------------------
const long REC_NO_LENGTH = 8;

//---------------------------------------------------------------------------

//==========================================================================
// 函数 : TCIndexedDBF::~TCIndexedDBF
// 用途 : 析构函数
// 原型 : ~TCIndexedDBF();
// 参数 : 无
// 返回 : 无
// 说明 : 
//==========================================================================
TCIndexedDBF::~TCIndexedDBF()
{
    CloseDBF();
}

//==========================================================================
// 函数 : TCIndexedDBF::Append
// 用途 : 增加一条记录并加入索引项
// 原型 : void Append();
// 参数 : 无
// 返回 : 加入结果
// 说明 :
//==========================================================================
TEDBAddResult TCIndexedDBF::Append()
{
    long nAppendRecCount;

    PutDBFValue();
    nAppendRecCount = RecCount() + 1;

    ASSERT(nAppendRecCount < 100000000);

    memcpy(m_pIndexData, &m_pRecordBuffer[1], m_nKeyFieldsSize);
    sprintf(&m_pIndexData[m_nKeyFieldsSize], "%08ld", nAppendRecCount);
    if (m_CDBIndex.DBAdd(m_pIndexData) == arDuplicateKey)
    {
        return arDuplicateKey;
    }
    TCFoxDBF::Append();
    return arSucceed;
}

//==========================================================================
// 函数 : TCIndexedDBF::AttachFile
// 用途 : 打开DBF文件及相关索引文件
// 原型 : void AttachFile(TCString sFileName, long nKeyCount,
//              bool bUnique = true);
// 参数 : DBF文件名，关键字段数，是否唯一索引
// 返回 : 无
// 说明 : DBF文件的前面几个字段做为索引
//==========================================================================
void TCIndexedDBF::AttachFile(TCString sFileName, long nKeyCount, bool bUnique)
{
    CloseDBF();

    //===== 1. 调用父函数连到相关文件 ======
    long i;
    TCFoxDBF::AttachFile(sFileName);

    m_nKeyCount = nKeyCount;

    m_nKeyFieldsSize = 0;
    for (i = 0; i < nKeyCount; i++)
        m_nKeyFieldsSize += FieldLength(i);

    m_nIndexDataSize = m_nKeyFieldsSize + REC_NO_LENGTH;

    m_pIndexData = new char [m_nIndexDataSize + 1];
    m_pIndexData[m_nIndexDataSize] = '\0';

    //===== 2. 得到索引文件名 =====
    TCString sIndexFileName;
    sIndexFileName = ChangeFileExt(sFileName, ".idx");
    if (sFileName == sIndexFileName)
        throw TCException("TCIndexeDBF::AttachFile() Error - "
                "IndexFileName is the same as DBFFileName");
    //====== 3. 如果索引文件不存在，则创建并重建索引，否则打开索引文件 =======
    if (!FileExists(sIndexFileName))
    {
        CreateIndexFile(sIndexFileName, bUnique);
        Reindex();
    }
    else
    {
        TCString sOtherInfo;

        if (m_bOpenWithReadOnly)
        {
            m_CDBIndex.DBOpenR(sIndexFileName);
        }
        else
        {
            m_CDBIndex.DBOpen(sIndexFileName);
        }

        ASSERT(m_nIndexDataSize == m_CDBIndex.DataSize());

        sOtherInfo = m_CDBIndex.DBGetOtherInfo();

        ASSERT(Left(sOtherInfo, 10) == TCString("FOXDBF_IDX") );
        ASSERT(StrToInt(Mid(sOtherInfo, 17, 4)) == nKeyCount);
    }
}

//==========================================================================
// 函数 : TCIndexedDBF::AttachFileR
// 用途 : 以只读方式打开DBF文件及相关索引文件
// 原型 : void AttachFileR(TCString sFileName, long nKeyCount,
//              bool bUnique = true);
// 参数 : DBF文件名，关键字段数，是否唯一索引
// 返回 : 无
// 说明 : DBF文件的前面几个字段做为索引
// 历史 : 2001.11.29 增加本函数
//==========================================================================
void TCIndexedDBF::AttachFileR(TCString sFileName, long nKeyCount, bool bUnique)
{
    m_bOpenWithReadOnly = true;
    AttachFile(sFileName, nKeyCount, bUnique);
    m_bOpenWithReadOnly = false;
}

//==========================================================================
// 函数 : TCIndexedDBF::CloseDBF
// 用途 : 关闭所有文件，释放内存
// 原型 : void CloseDBF()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCIndexedDBF::CloseDBF()
{
    if (m_bFileOpened)
    {
        delete [] m_pIndexData;
        TCFoxDBF::CloseDBF();
        m_CDBIndex.DBClose();
    }
}

//==========================================================================
// 函数 : TCIndexedDBF::CreateIndexFile
// 用途 : 创建索引文件
// 原型 : void CreateIndexFile(TCString sIndexFileName, bool bUnique);
// 参数 : 索引文件名，关键字段数, 是否唯一索引
// 返回 : 无
// 说明 : 数据部分放8字节的记录号(字符串表示)
//==========================================================================
void TCIndexedDBF::CreateIndexFile(TCString sIndexFileName, bool bUnique)
{
    //==== 1. 得到Options(RecordSize, KeySize, Unique?) =====
    ASSERT(m_nKeyCount >= 1);

    TCString sOptions;

    sOptions = "idx, rec=" + IntToStr(m_nIndexDataSize) + ", blk=2048, key="
            + IntToStr(m_nKeyFieldsSize) + ", ";

    if (bUnique)
        sOptions += "UNIQUE";
    else
        sOptions += "DUPLICATE";

    //===== 2. 得到OtherInfo =====
    TCString sOtherInfo;
    TCString sKeyCount;

    sOtherInfo = "FOXDBF_IDX";      // 标志
    sOtherInfo += "V0100";          // 版本号1.00

    sKeyCount = "K" + Padl(IntToStr(m_nKeyCount), 4, '0');    // 共有多少关键字段
    ASSERT(Length(sKeyCount) == 5);

    sOtherInfo += sKeyCount;
    ASSERT(Length(sOtherInfo) == 20);

    //====== 3. 建立索引文件 =======
	printf("Creating IDX file........\n");//debug
    m_CDBIndex.DBCreate(sIndexFileName, sOptions, sOtherInfo);
}

//==========================================================================
// 函数 : TCIndexedDBF::FindIndex
// 用途 : 根据关键字组合，通过索引文件，求得所在的记录号
// 原型 : long TCIndexedDBF::FindIndex(char *KeyData);
// 参数 : 关键字组合
// 返回 : 记录号（找不到返回0）
// 说明 :
//==========================================================================
long TCIndexedDBF::FindIndex(char *KeyData)
{
    bool bFindIt;

    bFindIt = m_CDBIndex.DBFind(m_pIndexData, KeyData);

    if (!bFindIt)
        return 0;
    else
    {
        m_pIndexData[m_nIndexDataSize] = '\0';
        return atol(&m_pIndexData[m_nKeyFieldsSize]);
    }
}

//==========================================================================
// 函数 : TCIndexedDBF::Flush
// 用途 : 将一些变化情况写入硬盘
// 原型 : void Flush();
// 参数 : 无
// 返回 : 无
// 说明 : 该函数一般在一个阶段操作完成以后调用，以保证数据的完整性。
//==========================================================================
void TCIndexedDBF::Flush()
{
    ASSERT(m_bFileOpened == true);

    TCFoxDBF::Flush();
    m_CDBIndex.Flush();
}

//==========================================================================
// 函数 : TCIndexedDBF::Locate
// 用途 : 根据绑定变量，通过索引文件，求得所在的记录号
// 原型 : long TCIndexedDBF::Locate()；
// 参数 : 无
// 返回 : 记录号（找不到返回0）
// 说明 :
//==========================================================================
long TCIndexedDBF::Locate()
{
#ifdef DEBUG
    long i;

    for (i = 0; i < m_nKeyCount; i++)
        if (m_fbpFoxBinds[i].bp == NULL)
            throw TCException("TCIndexedDBF::Locate() - The Key Field "
                    "has not been binded.");
#endif

    PutDBFValue(m_nKeyCount);

    return FindIndex(&m_pRecordBuffer[1]);
}

//==========================================================================
// 函数 : TCIndexedDBF::Reindex
// 用途 : 重建索引
// 原型 : void Reindex();
// 参数 : 无
// 返回 : 无
// 说明 : 清空索引并进行重建
//==========================================================================
void TCIndexedDBF::Reindex()
{
    long i;

    ASSERT(RecordLength() > m_nKeyFieldsSize);

    m_CDBIndex.DBZAP();

    for (i = 1; i <= RecCount(); i++)
    {
        Go(i);

        ReadRecordToBuffer();

        memcpy(m_pIndexData, &m_pRecordBuffer[1], m_nKeyFieldsSize);
        sprintf(&m_pIndexData[m_nKeyFieldsSize], "%08ld", i);

        m_CDBIndex.DBAdd(m_pIndexData);
    }
}

//==========================================================================
// 函数 : TCIndexedDBF::ZAP
// 用途 : 清空DBF文件及索引文件
// 原型 : void ZAP();
// 参数 : 无
// 返回 : 无
// 说明 : 
//==========================================================================
void TCIndexedDBF::ZAP()
{
    TCFoxDBF::ZAP();
    m_CDBIndex.DBZAP();
}


//==========================================================================
// 函数 : TCIndexedDBF::DropDBFFile
// 用途 : 删除DBF文件及索引文件
// 原型 : static void DropDBFFile(TCString sFileName);
// 参数 : DBF文件名
// 返回 : 无
// 说明 : DBF文件及索引文件必须都存在
//==========================================================================
void TCIndexedDBF::DropDBFFile(TCString sFileName)
{
    TCString sIndexFile;
    sIndexFile = ChangeFileExt(sFileName, ".idx");

    if (!FileExists(sFileName))
        throw TCException("DropDBFFile() : File Not Exists - " + sFileName);

    if (!FileExists(sIndexFile))
        throw TCException("DropDBFFile() : File Not Exists - " + sIndexFile);

    if (!DeleteFile(sFileName))
        throw TCException("DropDBFFile() : Delete File Error - " + sFileName);

    if (!DeleteFile(sIndexFile))
        throw TCException("DropDBFFile() : Delete File Error - " + sIndexFile);
}

long TCIndexedDBF::LocateFirst()
{
    bool bFindIt;

    bFindIt = m_CDBIndex.DBReadFirst(m_pIndexData);

    if (!bFindIt)
        return 0;
    else
    {
        m_pIndexData[m_nIndexDataSize] = '\0';
        return atol(&m_pIndexData[m_nKeyFieldsSize]);
    }
}

long TCIndexedDBF::LocateNext()
{
    bool bFindIt;

    bFindIt = m_CDBIndex.DBReadNext(m_pIndexData);

    if (!bFindIt)
        return 0;
    else
    {
        m_pIndexData[m_nIndexDataSize] = '\0';
        return atol(&m_pIndexData[m_nKeyFieldsSize]);
    }
}

#ifdef __TEST__

void DisplayTestIndexedDBFPrompt()
{
    printf("\n\n==== Test Indexed DBF ====\n\n");
    printf("0. Attach File\n");
    printf("Q. Quit\n\n");
}

void ID00AttachFile()
{
    TCIndexedDBF id;
    id.AttachFile("c:\\temp\\person1.dbf", 2, true);
    id.CloseDBF();
}

void TestIndexedDBFMainFunc()
{
    int cChar;

    DisplayTestIndexedDBFPrompt();
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
                ID00AttachFile();
                break;

            default:
                continue;
        }
        DisplayTestIndexedDBFPrompt();
    }
}

#endif


