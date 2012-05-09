//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_indexed_dbf.h"

//---------------------------------------------------------------------------
// ���� : REC_NO_LENGTH
// ��; : ��¼���������ļ��еĳ��ȱ�ʾ, 8 => ֧�ֲ�����һ������¼
//---------------------------------------------------------------------------
const long REC_NO_LENGTH = 8;

//---------------------------------------------------------------------------

//==========================================================================
// ���� : TCIndexedDBF::~TCIndexedDBF
// ��; : ��������
// ԭ�� : ~TCIndexedDBF();
// ���� : ��
// ���� : ��
// ˵�� : 
//==========================================================================
TCIndexedDBF::~TCIndexedDBF()
{
    CloseDBF();
}

//==========================================================================
// ���� : TCIndexedDBF::Append
// ��; : ����һ����¼������������
// ԭ�� : void Append();
// ���� : ��
// ���� : ������
// ˵�� :
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
// ���� : TCIndexedDBF::AttachFile
// ��; : ��DBF�ļ�����������ļ�
// ԭ�� : void AttachFile(TCString sFileName, long nKeyCount,
//              bool bUnique = true);
// ���� : DBF�ļ������ؼ��ֶ������Ƿ�Ψһ����
// ���� : ��
// ˵�� : DBF�ļ���ǰ�漸���ֶ���Ϊ����
//==========================================================================
void TCIndexedDBF::AttachFile(TCString sFileName, long nKeyCount, bool bUnique)
{
    CloseDBF();

    //===== 1. ���ø�������������ļ� ======
    long i;
    TCFoxDBF::AttachFile(sFileName);

    m_nKeyCount = nKeyCount;

    m_nKeyFieldsSize = 0;
    for (i = 0; i < nKeyCount; i++)
        m_nKeyFieldsSize += FieldLength(i);

    m_nIndexDataSize = m_nKeyFieldsSize + REC_NO_LENGTH;

    m_pIndexData = new char [m_nIndexDataSize + 1];
    m_pIndexData[m_nIndexDataSize] = '\0';

    //===== 2. �õ������ļ��� =====
    TCString sIndexFileName;
    sIndexFileName = ChangeFileExt(sFileName, ".idx");
    if (sFileName == sIndexFileName)
        throw TCException("TCIndexeDBF::AttachFile() Error - "
                "IndexFileName is the same as DBFFileName");
    //====== 3. ��������ļ������ڣ��򴴽����ؽ�����������������ļ� =======
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
// ���� : TCIndexedDBF::AttachFileR
// ��; : ��ֻ����ʽ��DBF�ļ�����������ļ�
// ԭ�� : void AttachFileR(TCString sFileName, long nKeyCount,
//              bool bUnique = true);
// ���� : DBF�ļ������ؼ��ֶ������Ƿ�Ψһ����
// ���� : ��
// ˵�� : DBF�ļ���ǰ�漸���ֶ���Ϊ����
// ��ʷ : 2001.11.29 ���ӱ�����
//==========================================================================
void TCIndexedDBF::AttachFileR(TCString sFileName, long nKeyCount, bool bUnique)
{
    m_bOpenWithReadOnly = true;
    AttachFile(sFileName, nKeyCount, bUnique);
    m_bOpenWithReadOnly = false;
}

//==========================================================================
// ���� : TCIndexedDBF::CloseDBF
// ��; : �ر������ļ����ͷ��ڴ�
// ԭ�� : void CloseDBF()
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : TCIndexedDBF::CreateIndexFile
// ��; : ���������ļ�
// ԭ�� : void CreateIndexFile(TCString sIndexFileName, bool bUnique);
// ���� : �����ļ������ؼ��ֶ���, �Ƿ�Ψһ����
// ���� : ��
// ˵�� : ���ݲ��ַ�8�ֽڵļ�¼��(�ַ�����ʾ)
//==========================================================================
void TCIndexedDBF::CreateIndexFile(TCString sIndexFileName, bool bUnique)
{
    //==== 1. �õ�Options(RecordSize, KeySize, Unique?) =====
    ASSERT(m_nKeyCount >= 1);

    TCString sOptions;

    sOptions = "idx, rec=" + IntToStr(m_nIndexDataSize) + ", blk=2048, key="
            + IntToStr(m_nKeyFieldsSize) + ", ";

    if (bUnique)
        sOptions += "UNIQUE";
    else
        sOptions += "DUPLICATE";

    //===== 2. �õ�OtherInfo =====
    TCString sOtherInfo;
    TCString sKeyCount;

    sOtherInfo = "FOXDBF_IDX";      // ��־
    sOtherInfo += "V0100";          // �汾��1.00

    sKeyCount = "K" + Padl(IntToStr(m_nKeyCount), 4, '0');    // ���ж��ٹؼ��ֶ�
    ASSERT(Length(sKeyCount) == 5);

    sOtherInfo += sKeyCount;
    ASSERT(Length(sOtherInfo) == 20);

    //====== 3. ���������ļ� =======
	printf("Creating IDX file........\n");//debug
    m_CDBIndex.DBCreate(sIndexFileName, sOptions, sOtherInfo);
}

//==========================================================================
// ���� : TCIndexedDBF::FindIndex
// ��; : ���ݹؼ�����ϣ�ͨ�������ļ���������ڵļ�¼��
// ԭ�� : long TCIndexedDBF::FindIndex(char *KeyData);
// ���� : �ؼ������
// ���� : ��¼�ţ��Ҳ�������0��
// ˵�� :
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
// ���� : TCIndexedDBF::Flush
// ��; : ��һЩ�仯���д��Ӳ��
// ԭ�� : void Flush();
// ���� : ��
// ���� : ��
// ˵�� : �ú���һ����һ���׶β�������Ժ���ã��Ա�֤���ݵ������ԡ�
//==========================================================================
void TCIndexedDBF::Flush()
{
    ASSERT(m_bFileOpened == true);

    TCFoxDBF::Flush();
    m_CDBIndex.Flush();
}

//==========================================================================
// ���� : TCIndexedDBF::Locate
// ��; : ���ݰ󶨱�����ͨ�������ļ���������ڵļ�¼��
// ԭ�� : long TCIndexedDBF::Locate()��
// ���� : ��
// ���� : ��¼�ţ��Ҳ�������0��
// ˵�� :
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
// ���� : TCIndexedDBF::Reindex
// ��; : �ؽ�����
// ԭ�� : void Reindex();
// ���� : ��
// ���� : ��
// ˵�� : ��������������ؽ�
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
// ���� : TCIndexedDBF::ZAP
// ��; : ���DBF�ļ��������ļ�
// ԭ�� : void ZAP();
// ���� : ��
// ���� : ��
// ˵�� : 
//==========================================================================
void TCIndexedDBF::ZAP()
{
    TCFoxDBF::ZAP();
    m_CDBIndex.DBZAP();
}


//==========================================================================
// ���� : TCIndexedDBF::DropDBFFile
// ��; : ɾ��DBF�ļ��������ļ�
// ԭ�� : static void DropDBFFile(TCString sFileName);
// ���� : DBF�ļ���
// ���� : ��
// ˵�� : DBF�ļ��������ļ����붼����
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


