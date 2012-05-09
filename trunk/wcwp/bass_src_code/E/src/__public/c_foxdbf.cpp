//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_foxdbf.h"

//---------------------------------------------------------------------------

//==========================================================================
// ���� : ���캯��
// ��; : ��ʼ������
//==========================================================================
TCFoxDBF::TCFoxDBF()
{
    m_bFileOpened = false;
    m_bHasAppended = false;

    m_ffpFoxFields = NULL;
    m_fbpFoxBinds = NULL;
    m_pRecordBuffer = NULL;

    m_bOpenWithReadOnly = false;
}

//==========================================================================
// ���� : TCFoxDBF��������
// ��; : ����ʱ���ر��ļ�(����ļ��Ѵ�)
//==========================================================================
TCFoxDBF::~TCFoxDBF()
{   CloseDBF();
}

//==========================================================================
// ���� : TCFoxDBF::Append
// ��; : ����һ����¼
// ԭ�� : void Append();
// ���� : ��
// ���� : ��
// ˵�� : Ϊ�����ٶȣ��ڽ���Append����ʱ����ˢ���ļ�ͷ����¼�ļ�β������
//        �ڹر��ļ�ʱ���м�¼�����Խ���Append������Ҫ��ʱ�ر��ļ�����
//        ��Flush()������
//==========================================================================
void TCFoxDBF::Append()
{
    memset(m_pRecordBuffer, ' ', RecordLength());

    PutDBFValue();

    m_fDBFFile.Seek(m_nRecordAddress + RecCount() * RecordLength(), SEEK_SET);
    m_fDBFFile.Write(m_pRecordBuffer, RecordLength());

    m_fhFoxHead.recnum ++;
    m_nCurrentRecordSeq = m_fhFoxHead.recnum - 1;

    m_bHasAppended = true;
}

//==========================================================================
// ���� : TCFoxDBF::AppendRecordByStringList
// ��; : ����StringList�е���������һ�����ݿ��¼
// ԭ�� : void AppendRecordByStringList(TCStringList & slRecordValue);
// ���� : �洢��¼ֵ��StringList
// ���� : ��
// ˵�� :
//==========================================================================
void TCFoxDBF::AppendRecordByStringList(TCStringList & slRecordValue)
{
    FillRecordBufferByStringList(slRecordValue);

    m_fDBFFile.Seek(m_nRecordAddress + RecCount() * RecordLength(), SEEK_SET);
    m_fDBFFile.Write(m_pRecordBuffer, RecordLength());

    m_fhFoxHead.recnum ++;
    m_nCurrentRecordSeq = m_fhFoxHead.recnum - 1;

    m_bHasAppended = true;
}

//==========================================================================
// ���� : TCFoxDBF::AttachFile
// ��; : ��DBF�ļ�������ȡ�ļ���Ϣ
// ԭ�� : void AttachFile(TCString sDBFFileName);
// ���� : �ļ���
// ���� : ��
// ˵�� : Ҫ����DBF�ļ�ǰ�������ȵ��øú�����
//==========================================================================
void TCFoxDBF::AttachFile(TCString sDBFFileName)
{
    //========= 0-. ��������֤����֤�Ƿ��޸�һ�� =====
    if (FileExists(GetModifyControlFileName(sDBFFileName)))
        throw TCException("TCFoxDBF::AttachFile() Error - "
                " The file may be modifying, Check file Integrity.  \n"
                " FileName:" + sDBFFileName);

    m_bModifyingCheck = false;

    //---------------------------------------------------

    m_sDBFFileName = sDBFFileName;

    DWORD       nReaded;
    TSFOXFIELD  ffTmp;

    // ======= 0. ��ʼ������ ========
    CloseDBF();

    m_bHasAppended = false;
    m_bFileOpened = false;

    m_nCurrentRecordSeq = -1;

	// ======== 1. ��DBF�ļ� ============
    if (!FileExists(sDBFFileName))
        throw TCException("TCFoxDBF::AttachFile() File doesn\'t exist - "
                "FileName : " + sDBFFileName);

    if (m_bOpenWithReadOnly)
        m_fDBFFile.Open(sDBFFileName, omRead | omShared);
    else
        m_fDBFFile.Open(sDBFFileName, omRead | omWrite | omExclusive_Waiting);

    m_bFileOpened = true;

    // =========== 2. ��ȡDBF�ļ�ͷ ===============
    // ======== DBF�ļ�ͷ�а�����RecCount��RecordLength��Ϣ ========
    nReaded = m_fDBFFile.Read(&m_fhFoxHead, sizeof(TSFOXHEAD));
    if (nReaded != sizeof(TSFOXHEAD))
        throw TCException("File Header Error FileName - " + m_sDBFFileName);

    if (m_fhFoxHead.sign != 3)
        throw TCException("Header Sign Error, illegal DBF File - "
                + m_sDBFFileName);

    RegulateHeaderByteOrder(m_fhFoxHead);

    //=========== 3. ������ȡDBF�ļ����ֶ����� ===========
    m_nFieldAmount = 0;
    while (true)
	{
        nReaded = m_fDBFFile.Read(&ffTmp, sizeof(TSFOXFIELD));
        if (nReaded != sizeof(TSFOXFIELD) && ffTmp.fldname[0] != 0x0D)
            throw TCException("DBF Field Description Error - "
                    + m_sDBFFileName);

        if (ffTmp.fldname[0] == 0x0D)
            break;

        m_nFieldAmount ++;
	}

    m_nRecordAddress = sizeof(TSFOXHEAD)
            + m_nFieldAmount*sizeof(TSFOXFIELD) + 1;

    // =========== 4. ���ֶη�����Ӧ���ڴ�ռ� ============
	m_ffpFoxFields = new TSFOXFIELD[m_nFieldAmount];    // �����ֶδ洢�ռ�
	if (m_ffpFoxFields == NULL)
        throw TCException("Memory Alloc Error");

	m_fbpFoxBinds = new TSFOXBIND[m_nFieldAmount];      // �����ֶΰ󶨴洢�ռ�
	if (m_fbpFoxBinds == NULL)
        throw TCException("Memory Alloc Error");

	memset(m_fbpFoxBinds, 0, m_nFieldAmount*sizeof(TSFOXBIND));

    m_pRecordBuffer = new char[RecordLength()];      // ���䵥����¼�洢�ռ�

    // ========= 5. ��ȡ�ֶνṹ ============
    m_fDBFFile.Seek(sizeof(TSFOXHEAD), SEEK_SET);

    nReaded = m_fDBFFile.Read(m_ffpFoxFields,
            sizeof(TSFOXFIELD) * m_nFieldAmount);
	if (nReaded != sizeof(TSFOXFIELD) * m_nFieldAmount)
        throw TCException("Read FoxField Structure Error - "
                + m_sDBFFileName);

    //========== 6. У�������ԣ��Ƚ��ļ���С�Ƿ����¼����� =====
    long nShouldFileSize, nActualFileSize;
    nShouldFileSize = m_nRecordAddress + RecCount() * RecordLength();
    nActualFileSize = m_fDBFFile.FileSize();

    if (nShouldFileSize != nActualFileSize
            && nShouldFileSize + 1 != nActualFileSize)
        throw TCException("DBF File Integrity Error - FileName: "
                + sDBFFileName + "\n"
                + " Actual Size : " + IntToStr(nActualFileSize)
                + "    Need Size : " + IntToStr(nShouldFileSize) + "\n");
}

//==========================================================================
// ���� : TCFoxDBF::AttachFileR
// ��; : ��ֻ������ʽ���ļ�
// ԭ�� : void AttachFileR(TCString sDBFFileName);
// ���� : �ļ���
// ���� : ��
// ˵�� : �����ж��������Ҫͬʱ����һ���ļ��ĳ���
// ��ʷ : 2001.11.29 OLDIX, ���Ӹú���
//==========================================================================
void TCFoxDBF::AttachFileR(TCString sDBFFileName)
{
    m_bOpenWithReadOnly = true;
    AttachFile(sDBFFileName);
    m_bOpenWithReadOnly = false;
}

//==========================================================================
// ���� : TCFoxDBF::AttachFileW
// ��; : д���DBF�ļ���Ҫ��У��������
// ԭ�� : void AttachFileW(TCString sDBFFileName);
// ���� : �ļ���
// ���� : ��
// ˵�� : 1. �øú����򿪣���ʵ��ģʽδ�䣬ֻ����д������
//        2. ����һ���ļ���sDBFFileName + "_mod_ify_"����������У��
//==========================================================================
void TCFoxDBF::AttachFileW(TCString sDBFFileName)
{
    AttachFile(sDBFFileName);

    m_bModifyingCheck = true;
    CreateBlankFile(GetModifyControlFileName(sDBFFileName));
}

//==========================================================================
// ���� : TCFoxDBF::CloseDBF
// ��; : ���йر��ļ����ͷ��ڴ�Ȳ���
// ԭ�� : void CloseDBF();
// ���� : ��
// ���� : ��
// ˵�� : ����ļ��ѱ����Ӽ�¼������д�ļ�ͷ����β��д������ַ���
// ��ʷ : 2001.12.25 ��Close֮ǰ����Flush()���
//==========================================================================
void TCFoxDBF::CloseDBF()
{
    if (!m_bFileOpened)
        return;

    if (m_bModifyingCheck)
    {
        TCString sModifyControlFileName;
        sModifyControlFileName = GetModifyControlFileName(GetDBFFileName());

        if (!FileExists(sModifyControlFileName))
            throw TCException("DBF Integrity Error : "
                    " modify control file missed - "
                    + GetDBFFileName() + " * " + sModifyControlFileName);

        DeleteFileE(sModifyControlFileName);

        m_bModifyingCheck = false;
    }

    Flush();    // ��ʷ: 2001.12.25����˾�

    m_bFileOpened = false;

    //========== ��Ϊ����Flush(), ע�͵�������� =======
    // if (m_bHasAppended)
    // {
    //     m_bHasAppended = false;
    //
    //     char c1A = 0x1A;
    //     m_fDBFFile.Seek(m_nRecordAddress + RecCount() * RecordLength(),
    //             SEEK_SET);
    //     m_fDBFFile.Write(&c1A, 1);
    //
    //     WriteFoxHeader();
    // }
    //==================== end of ע�͵� =======================

    if (m_ffpFoxFields)
        delete[] m_ffpFoxFields;

    if (m_fbpFoxBinds)
        delete[] m_fbpFoxBinds;

    if (m_pRecordBuffer)
        delete[] m_pRecordBuffer;

    m_ffpFoxFields = NULL;
    m_fbpFoxBinds = NULL;
    m_pRecordBuffer = NULL;

    m_fDBFFile.Close();
}

//==========================================================================
// ���� : TCFoxDBF::CreateFoxDBFBySelf
// ��; : ����һ��DBF�ļ�����ṹͬ��ǰ�򿪵��ļ��ṹ
// ԭ�� : void TCFoxDBF::CreateFoxDBFBySelf(TCString sDestDBF);
// ���� : Ŀ���ļ���
// ���� : ��
// ˵�� : 
//==========================================================================
void TCFoxDBF::CreateFoxDBFBySelf(TCString sDestDBF)
{
    // ====== 1. ��Ŀ���ļ� =========
    TCFileStream fDestDBF;
    fDestDBF.Open(sDestDBF, omWrite);

    // ======= 2. д���ļ�ͷ ========
    TSFOXHEAD fxhFoxHead;
    fxhFoxHead = m_fhFoxHead;
    fxhFoxHead.recnum = 0;

    RegulateHeaderByteOrder(fxhFoxHead);
    fDestDBF.Write(&fxhFoxHead, sizeof(TSFOXHEAD));
    RegulateHeaderByteOrder(fxhFoxHead);

    //========= 3. д���ֶ��������� ==========
    fDestDBF.Write(m_ffpFoxFields, sizeof(TSFOXFIELD) * FieldAmount());

    //======== 4. �ر��ļ� ==========
    fDestDBF.Write("\x0D", 1);
    fDestDBF.Close();
}

//==========================================================================
// ���� : TCFoxDBF::DBBindToPointer
// ��; : ��һ���ֶε�һ��ָ����
// ԭ�� : void TCFoxDBF::DBBindToPointer(long nFieldID,
//          TEFoxBindType fbFoxBindType, void *pBindPointer)
// ���� : �ֶ����, �����ͣ��󶨵�ַ
// ���� : ��
// ˵�� : �ú������ֶΰ󶨺��ĺ����������ǹ��ú��������Ǳ�һ�鹫��DBBind
//        �������á����������ַ��������˰����ͺ�ָ����ã�ʹ���ø��Ӽ�
//        ��
//==========================================================================
void TCFoxDBF::DBBindToPointer(long nFieldID, TEFoxBindType fbFoxBindType,
            void *pBindPointer)
{
    long nStartPos;
    long i;

    if (nFieldID >= m_nFieldAmount || nFieldID < 0)
        throw TCException("Invalid nFieldID - "+IntToStr(nFieldID)
                + " * " + m_sDBFFileName);

    m_fbpFoxBinds[nFieldID].ptype = fbFoxBindType;
    m_fbpFoxBinds[nFieldID].bp = pBindPointer;

    nStartPos = 1;

    for (i = 0; i < nFieldID; i++)
        nStartPos += FieldLength(i);

    m_fbpFoxBinds[nFieldID].start = nStartPos;
    m_fbpFoxBinds[nFieldID].length = FieldLength(nFieldID);

    GetDBFRecValue();
}

//==========================================================================
// ���� : TCFoxDBF::Delete
// ��; : ɾ����ǰ��¼
// ԭ�� : void Delete();
// ���� : ��
// ���� : ��
// ˵�� : ֻ�Լ�¼���ϱ�ǣ����Ҫ����ɾ����Ҫ����Pack()������
//==========================================================================
void TCFoxDBF::Delete()
{
    if (m_nCurrentRecordSeq < 0 || m_nCurrentRecordSeq >= RecCount())
        return;

    m_fDBFFile.Seek(m_nRecordAddress + m_nCurrentRecordSeq * RecordLength());

    m_fDBFFile.Write("\x2A", 1);
}

//==========================================================================
// ���� : TCFoxDBF::FetchDBFStructDefine
// ��; : ȡ��DBF�ṹ�Ķ���
// ԭ�� : void FetchDBFStructDefine(TCDBFStructDefine & dsStructDefine);
// ���� : DBF�ṹ�����������
// ���� : ��
// ˵�� : 
//==========================================================================
void TCFoxDBF::FetchDBFStructDefine(TCDBFStructDefine & dsStructDefine)
{
    dsStructDefine.Clear();

    long i;
    for (i = 0; i < FieldAmount(); i++)
        dsStructDefine.AddField(FieldName(i), FieldType(i), FieldLength(i),
                FieldPoint(i));
}

//==========================================================================
// ���� : TCFoxDBF::FieldName
// ��; : �õ��ֶ���
// ԭ�� : TCString FieldName(long nFieldSeq);
// ���� : �ֶ����
// ���� : �ֶ���
// ˵�� : �ֶ���Ŵ�0��ʼ
//==========================================================================
TCString TCFoxDBF::FieldName(long nFieldSeq)
{
    TCString sFieldName;

    if (m_ffpFoxFields[nFieldSeq].fldname
            [sizeof(m_ffpFoxFields[nFieldSeq].fldname) - 1] == '\0')
        sFieldName = TCString(m_ffpFoxFields[nFieldSeq].fldname);
    else
        sFieldName = TCString(m_ffpFoxFields[nFieldSeq].fldname,
                sizeof(m_ffpFoxFields[nFieldSeq].fldname));

	sFieldName = RightTrim(sFieldName);

    return sFieldName;
}

//==========================================================================
// ���� : TCFoxDBF::FillRecordBufferByStringList
// ��; : ��StringList��������䵽RecordBuffer��
// ԭ�� : void FillRecordBufferByStringList(TCStringList & slRecordValue);
// ���� : �洢��¼ֵ��StringList
// ���� :
// ˵�� : StringList��String����������DBF�ֶ������
// ��ʷ : 2001.9.30 ����ǰ�����������������'-'��Ϊ����������
//==========================================================================
void TCFoxDBF::FillRecordBufferByStringList(TCStringList & slRecordValue)
{
    //====== 1. ��֤String�Ĵ�С���ֶ����Ƿ���� =======
    if (slRecordValue.GetCount() != FieldAmount())
        throw TCException("FillRecordBufferByStringList() Error - \n"
                "StringList Size do not Match Field Amount : \nSize(SL)"
                + IntToStr(slRecordValue.GetCount())+ "  Size(FD)"
                + IntToStr(FieldAmount()));

    //======= 2. ��RecordBuffer�е�ֵ��� =========
    memset(m_pRecordBuffer, ' ', RecordLength());

    //======= 3. ѭ������ÿһ���ֶ� =======
    long i;

    long nStartPos = 1;     // ÿ���ֶε���ʼλ��

    for (i = 0; i < FieldAmount(); i++)
    {
        memset(m_szTmpBuffer, ' ', sizeof(m_szTmpBuffer));

        switch (FieldType(i))
        {
            case 'C':
            case 'D':
                FillPadr(m_szTmpBuffer, slRecordValue[i], FieldLength(i));

                memcpy(m_pRecordBuffer + nStartPos, m_szTmpBuffer,
                        FieldLength(i));
                break;
            case 'M':
                continue;
            case 'L':
                if (Left(slRecordValue[i], 1) == TCString("T") )
                    m_pRecordBuffer[nStartPos] = 'T';
                else
                    m_pRecordBuffer[nStartPos] = ' ';

                break;
            case 'N':
            {
                double lfDoubleValue;

                lfDoubleValue = StrToFloat(slRecordValue[i]);

                char szFormatString[256];
                sprintf(szFormatString, "%%%d.%dlf", FieldLength(i),
                        FieldPoint(i));

                sprintf(m_szTmpBuffer, szFormatString, lfDoubleValue);

				if ((long)strlen(m_szTmpBuffer) > FieldLength(i))
                {
                    TCString sOverflowCause;
                    sOverflowCause = "Field Name : " + FieldName(i);
                    sOverflowCause += "Length Needed : "
                            + IntToStr(FieldLength(i)) + "\n";
                    sOverflowCause += "The Data Input is : "
                            + TCString(m_szTmpBuffer);
                    sOverflowCause += "The Data Length is : "
                            + IntToStr(strlen(m_szTmpBuffer));
                    throw TCException("TCFoxDBF::FillRecordBuffer"
                            "ByStringList():The number length is overflow.  "
                            + sOverflowCause);
                }
                    // memset(m_szTmpBuffer, '-', FieldLength(i)); ע20010930

                memcpy(m_pRecordBuffer + nStartPos, m_szTmpBuffer,
                        FieldLength(i));

				break;
            }   // end of case 'N'
            default:
                continue;
        }   // end of switch (FieldType(i))
        nStartPos += FieldLength(i);
    }   // end of for (i = 0; i < FieldAmount(); i++)
}

//==========================================================================
// ���� : TCFoxDBF::FieldSeq
// ��; : �����ֶ����õ��ֶ����
// ԭ�� : long FieldSeq(TCString sFieldName);
// ���� : �ֶ���
// ���� : �ֶ����
// ˵�� : �ֶ���Ŵ�0��ʼ
//==========================================================================
long TCFoxDBF::FieldSeq(TCString sFieldName)
{
    long i;

    for (i = 0; i < FieldAmount(); i++)
        if (FieldName(i) == UpperCase(sFieldName))
            return i;

    throw TCException("Bad Field Name of this DBF File - " + sFieldName
            + " * " + m_sDBFFileName);
}

//==========================================================================
// ���� : TCFoxDBF::Flush
// ��; : ��һЩ�仯���д��Ӳ��
// ԭ�� : void Flush();
// ���� : ��
// ���� : ��
// ˵�� : �ú���һ����һ���׶β�������Ժ���ã��Ա�֤���ݵ������ԡ�
//==========================================================================
void TCFoxDBF::Flush()
{
    ASSERT(m_bFileOpened);

    if (m_bHasAppended)
    {
        m_bHasAppended = false;

        char c1A = 0x1A;
        m_fDBFFile.Seek(m_nRecordAddress + RecCount() * RecordLength(),
                SEEK_SET);
        m_fDBFFile.Write(&c1A, 1);

        WriteFoxHeader();
    }

    m_fDBFFile.Flush();
}

//==========================================================================
// ���� : TCFoxDBF::GetCurrentDataByFieldSeq
// ��; : �õ���ǰ��¼��ָ���ֶ���ŵ�ֵ(���ַ�����ʾ)
// ԭ�� : TCString GetCurrentDataByFieldSeq(long nFieldSeq);
// ���� : �ֶ����
// ���� : �ֶ�ֵ
// ˵�� : 1. �ú���һ�����ڱ�����������
//        2. �õ����ַ���������ȥ�ո���������ִ���Ҫ��ȥ�գ���Ҫ��
//           ��������ʽ���á�
//==========================================================================
TCString TCFoxDBF::GetCurrentDataByFieldSeq(long nFieldSeq)
{
    ASSERT(nFieldSeq >= 0 && nFieldSeq < FieldAmount());

    long i, nStartPos;
    TCString sRet;

    nStartPos = 1;

    for (i = 0; i < nFieldSeq; i++)
        nStartPos += FieldLength(i);

    sRet = TCString(m_pRecordBuffer + nStartPos, FieldLength(nFieldSeq));
    sRet = RightTrim(sRet);

    return sRet;
}

//==========================================================================
// ���� : TCFoxDBF::GetDBData
// ��; : �õ�ָ����¼��ָ���ֶ�����ֵ(���ַ�����ʾ)
// ԭ�� : TCString GetDBData(long nRecordSeq, TCString sFieldName);
// ���� : ��¼�ţ��ֶ���
// ���� : �ֶ�ֵ
// ˵�� : ��¼�Ŵ�1��ʼ����
//==========================================================================
TCString TCFoxDBF::GetDBData(long nRecordSeq, TCString sFieldName)
{
    ASSERT(nRecordSeq <= RecCount());

    m_nCurrentRecordSeq = nRecordSeq - 1;

    ReadRecordToBuffer();

    return GetCurrentDataByFieldSeq(FieldSeq(sFieldName));
}

//==========================================================================
// ���� : TCFoxDBF::GetDBFRecValue
// ��; : ��DBF�еļ�¼ֵ�����󶨱���
// ԭ�� : void GetDBFRecValue();
// ���� : ��
// ���� : ��
// ˵�� : �����ǰ��¼����Ч��¼�������и�ֵ������
//==========================================================================
void TCFoxDBF::GetDBFRecValue()
{
    //===== 1. ��ȡ��¼���� ========
    ReadRecordToBuffer();
    if (m_bEOF || m_bBOF)
        return;

    //====== 2. �жϼ�¼�Ƿ�ɾ��״̬ ======
    if (m_pRecordBuffer[0] == 0x2A)
        m_bIsCurrentDelete = true;
    else
        m_bIsCurrentDelete = false;

    //======== 3. ѭ����ȡ��¼ֵ =========
    long i;
    char *P;

    for (i = 0; i < FieldAmount(); i++)
    {
        if (m_fbpFoxBinds[i].bp == NULL)
            continue;

        P = m_pRecordBuffer + m_fbpFoxBinds[i].start;
        memcpy(m_szTmpBuffer, P, m_fbpFoxBinds[i].length);
        m_szTmpBuffer[m_fbpFoxBinds[i].length] = '\0';

        switch (m_fbpFoxBinds[i].ptype)
        {
        case FBChar:
            *(char *)m_fbpFoxBinds[i].bp = m_szTmpBuffer[0];
            break;
        case FBString:
            *(TCString *)m_fbpFoxBinds[i].bp
                    = RightTrim(TCString(m_szTmpBuffer));
            break;
        case FBInteger:
            if (m_szTmpBuffer[0] == 'T')
                *(int *)m_fbpFoxBinds[i].bp = true;
            else
                *(int *)m_fbpFoxBinds[i].bp = atoi(m_szTmpBuffer);
            break;
        case FBLong:
            if (m_szTmpBuffer[0] == 'T')
                *(long *)m_fbpFoxBinds[i].bp = true;
            else
                *(long *)m_fbpFoxBinds[i].bp = atol(m_szTmpBuffer);
            break;
        case FBUnsignedLong:
            if (m_szTmpBuffer[0] == 'T')
                *(unsigned long *)m_fbpFoxBinds[i].bp = true;
            else
                *(unsigned long *)m_fbpFoxBinds[i].bp = atol(m_szTmpBuffer);
            break;
        case FBFloat:
            *(float *)m_fbpFoxBinds[i].bp = atof(m_szTmpBuffer);
            break;
        case FBDouble:
            *(double *)m_fbpFoxBinds[i].bp = atof(m_szTmpBuffer);
            break;
        default:
            throw TCException("FBPFOXBINDS\'S PTYPE ERROR - "
                    + m_sDBFFileName);
        } // end of switch
    } // end of for
}

//==========================================================================
// ���� : TCFoxDBF::GetModifyControlFileName
// ��; : �õ��޸Ŀ����ļ�
// ԭ�� : static TCString GetModifyControlFileName(TCString sFileName);
// ���� : �����ļ���
// ���� : �޸Ŀ����ļ���
// ˵�� : ���ļ��������޸������Լ��
//==========================================================================
TCString TCFoxDBF::GetModifyControlFileName(TCString sFileName)
{
    return sFileName + "_mod_ify_";
}

//==========================================================================
// ���� : TCFoxDBF::GetRecCountOfDBFFile
// ��; : �õ�һ��DBF�ļ��ļ�¼��
// ԭ�� : static long GetRecCountOfDBFFile(TCString sFileName);
// ���� : DBF�ļ���
// ���� : ���ļ��ļ�¼��
// ˵�� :
// ��ʷ : 2001.11.27 ���Ӹú���
//==========================================================================
long TCFoxDBF::GetRecCountOfDBFFile(TCString sFileName)
{
    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(sFileName);
    return fdFoxDBF.RecCount();
}

//==========================================================================
// ���� : TCFoxDBF::Go
// ��; : �Ƶ�ָ���ļ�¼
// ԭ�� : void Go(long nRecNum)
// ���� : ��¼����
// ���� : ��
// ˵�� : ��¼������1��ʼ����
//==========================================================================
void TCFoxDBF::Go(long nRecNum)
{   m_nCurrentRecordSeq = nRecNum - 1;
    GetDBFRecValue();
}

//==========================================================================
// ���� : TCFoxDBF::Insert
// ��; : ���ݰ󶨵����ݲ����¼
// ԭ�� : void Insert();
// ���� : ��
// ���� : ��
// ˵�� : ���ڲ����¼���ļ��������ࣨ�����֮����ļ�����Ҫȫ�����ƣ���
//        �ʱ�����ֻ�Ƕ��ṩһ��ѡ��һ�㲻�Ƽ�ʹ�á�
//==========================================================================
void TCFoxDBF::Insert()
{
    if (m_nCurrentRecordSeq < 0 || m_nCurrentRecordSeq >= RecCount())
        throw TCException("TCFoxDBF::Insert() Error - \n"
                " CurrentRecord : " + IntToStr(m_nCurrentRecordSeq)
                + "\n TotalRecord : " + IntToStr(RecCount()));

    InsertPrepare();
    
    // �����м�¼��д�������
    m_fDBFFile.Seek(m_nRecordAddress + m_nCurrentRecordSeq * RecordLength());
    memset(m_pRecordBuffer, ' ', RecordLength());
    m_fDBFFile.Write(m_pRecordBuffer, RecordLength());

    Update();
    m_fhFoxHead.recnum ++;

    WriteFoxHeader();
}

//==========================================================================
// ���� : TCFoxDBF::InsertPrepare
// ��; : ������������ļ����ݺ��ƣ�Ϊ����������׼��
// ԭ�� : void InsertPrepare()
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCFoxDBF::InsertPrepare()
{
    char *RecordBuffer1, *RecordBuffer2;
    long i;

    RecordBuffer1 = new char [RecordLength()];
    RecordBuffer2 = new char [RecordLength()];

    m_fDBFFile.Seek(m_nRecordAddress + m_nCurrentRecordSeq * RecordLength());
    m_fDBFFile.Read(RecordBuffer1, RecordLength());

    for (i = m_nCurrentRecordSeq + 1; i <= RecCount(); i++)
    {
        // ��ȡ���ݵ�Buffer2
        if (i != RecCount())
        {
            m_fDBFFile.Seek(m_nRecordAddress + i * RecordLength());
            m_fDBFFile.Read(RecordBuffer2, RecordLength());
        }

        // ����һ����¼������(����Buffer1֮��)д��˼�¼
        m_fDBFFile.Seek(m_nRecordAddress + i * RecordLength());
        m_fDBFFile.Write(RecordBuffer1, RecordLength());

        // ��Buffer2�е����ݸ��Ƶ�Buffer1֮��
        memcpy(RecordBuffer1, RecordBuffer2, RecordLength());
    }

    m_fDBFFile.Write("\x1A", 1);

    delete [] RecordBuffer1;
    delete [] RecordBuffer2;
}

//==========================================================================
// ���� : TCFoxDBF::InsertRecordByStringList
// ��; : ����StringList�е����ݲ���һ�����ݿ��¼
// ԭ�� : void InsertRecordByStringList(TCStringList & slRecordValue);
// ���� : �洢��¼ֵ��StringList
// ���� : ��
// ˵�� :
//==========================================================================
void TCFoxDBF::InsertRecordByStringList(TCStringList & slRecordValue)
{
    if (m_nCurrentRecordSeq < 0 || m_nCurrentRecordSeq >= RecCount())
        throw TCException("TCFoxDBF::InsertRecordByStringList() Error - \n"
                " CurrentRecord : " + IntToStr(m_nCurrentRecordSeq)
                + "\n TotalRecord : " + IntToStr(RecCount())
                + "\n DBFFileName : " + GetDBFFileName());

    InsertPrepare();

    m_fDBFFile.Seek(m_nRecordAddress + m_nCurrentRecordSeq * RecordLength());

    FillRecordBufferByStringList(slRecordValue);

    m_fDBFFile.Write(m_pRecordBuffer, RecordLength());

    m_fhFoxHead.recnum ++;

    WriteFoxHeader();
}

//==========================================================================
// ���� : TCFoxDBF::Pack
// ��; : ����������ɾ��һ����¼
// ԭ�� : void Pack();
// ���� : ��
// ���� : ��
// ˵�� : ����ɾ����¼Delete()ʱ��ֻ����һ��ɾ����Ƕ���������ɾ����
//        ��¼����ִ�б�����ʱ����������������ɾ�����������ܹ���ʡ��
//        �䣬���翼�ǵ��ٶȵ�Ӱ�죬���Ƽ�Ƶ��ʹ�á�
//==========================================================================
void TCFoxDBF::Pack()
{
    char *RecordBuffer;
    long nReadRow, nWriteRow;

    RecordBuffer = new char [RecordLength()];

    nWriteRow = 0;

    //====== 1. ѭ����ȡÿһ����¼���粻��ɾ���ļ�¼����д�� ======
    for (nReadRow = 0; nReadRow < RecCount(); nReadRow++)
    {
        m_fDBFFile.Seek(m_nRecordAddress + nReadRow * RecordLength());
        m_fDBFFile.Read(RecordBuffer, RecordLength());

        if (RecordBuffer[0] == '\x2A')
            continue;

        m_fDBFFile.Seek(m_nRecordAddress + nWriteRow * RecordLength());
        m_fDBFFile.Write(RecordBuffer, RecordLength());
        nWriteRow ++;
    }

    //======= 2. �ض��ļ���д���ļ�β��ǣ������ļ�ͷ ======
    m_fDBFFile.Truncate(m_nRecordAddress + nWriteRow * RecordLength());

    m_fDBFFile.Seek(m_nRecordAddress + nWriteRow * RecordLength());
    m_fDBFFile.Write("\x1A", 1);

    m_fhFoxHead.recnum = nWriteRow;
    WriteFoxHeader();

    delete [] RecordBuffer;
}

//==========================================================================
// ���� : TCFoxDBF::PutDBFValue
// ��; : ���󶨵ı�����������γ�һ����¼���ַ�����
// ԭ�� : void PutDBFValue()
// ���� : ��
// ���� : ��
// ˵�� : ֻ��¼�󶨵ı�����δ�󶨱����л������е�ֵδ��ա����ԣ����Ҫ��
//        ����ջ������������ڵ��øú���ǰ��ʽ����memset����ջ�������
//        C, D -- FBString, FBChar
//        M    -- ��
//        L    -- FBChar, FBString, FBInteger, FBLong
//        N    -- FBString, FBInteger, FBLong, FBFloat, FBDouble
// ��ʷ : 2001.9.30 ����ǰ�����������������'-'��Ϊ����������
//        2001.10.2 PutDBFValue����һ���������Խ��Locateʱ����Locate��ô��
//                  ������
//==========================================================================
void TCFoxDBF::PutDBFValue(long nPutFieldCount)
{
    long i;
    TEFoxBindType fbBindType;
    long nCopyLength;

    if (nPutFieldCount == 0)
        nPutFieldCount = FieldAmount();

    for (i = 0; i < nPutFieldCount; i++)
    {
        if (m_fbpFoxBinds[i].bp == NULL)
            continue;

        fbBindType = m_fbpFoxBinds[i].ptype;

        memset(m_szTmpBuffer, ' ', sizeof(m_szTmpBuffer));

        switch (FieldType(i))
        {
            case 'C':
            case 'D':
                if (fbBindType != FBString && fbBindType != FBChar)
                    throw TCException("ptype error HFoxDbf.PutValue() C D - "
                            + m_sDBFFileName);

                nCopyLength = m_fbpFoxBinds[i].length;

                if (fbBindType == FBChar)
                    m_szTmpBuffer[0] = *(char *)m_fbpFoxBinds[i].bp;
                else
                    FillPadr(m_szTmpBuffer, *(TCString *)m_fbpFoxBinds[i].bp,
                            nCopyLength);

                memcpy(m_pRecordBuffer + m_fbpFoxBinds[i].start,
                        m_szTmpBuffer, nCopyLength);
                break;
            case 'M':
                continue;
            case 'L':
            {
                bool bBoolValue = false;

                if (fbBindType != FBChar && fbBindType != FBString
                        && fbBindType != FBInteger && fbBindType != FBLong
                        && fbBindType != FBUnsignedLong)
                    throw TCException("ptype error in HFoxDbf.PutValue() L - "
                            + m_sDBFFileName);

                switch (fbBindType)
                {
                    case FBChar:
                        if (*(char *)m_fbpFoxBinds[i].bp == 'T')
                            bBoolValue = true;
                        break;
                    case FBString:
                        if (Left(*(TCString *)m_fbpFoxBinds[i].bp, 1)
                                == TCString("T") )
                            bBoolValue = true;
                        break;
                    case FBInteger:
                        if (*(int *)m_fbpFoxBinds[i].bp != 0)
                            bBoolValue = true;
                        break;
                    case FBLong:
                        if (*(long *)m_fbpFoxBinds[i].bp != 0)
                            bBoolValue = true;
                        break;
                    case FBUnsignedLong:
                        if (*(unsigned long *)m_fbpFoxBinds[i].bp != 0)
                            bBoolValue = true;
                        break;
                }

                if (bBoolValue)
                    m_pRecordBuffer[m_fbpFoxBinds[i].start] = 'T';
                else
                    m_pRecordBuffer[m_fbpFoxBinds[i].start] = ' ';
                break;
            }   // end of case 'L'
            case 'N':
            {
                double lfDoubleValue;

                switch (fbBindType)
                {
                    case FBString:
                        lfDoubleValue
                                = StrToFloat(*(TCString *)m_fbpFoxBinds[i].bp);
                        break;
                    case FBInteger:
                        lfDoubleValue = *(int *)m_fbpFoxBinds[i].bp;
                        break;
                    case FBLong:
                        lfDoubleValue = *(long *)m_fbpFoxBinds[i].bp;
                        break;
                    case FBUnsignedLong:
                        lfDoubleValue = *(unsigned long *)m_fbpFoxBinds[i].bp;
                        break;
                    case FBFloat:
                        lfDoubleValue = *(float *)m_fbpFoxBinds[i].bp;
                        break;
                    case FBDouble:
                        lfDoubleValue = *(double *)m_fbpFoxBinds[i].bp;
                        break;
                    default:
                        throw TCException("ptype error in "
                                "HFoxDbf.PutValue() N - " + m_sDBFFileName);
                }   // end of switch (fbBindType)

                char szFormatString[256];
                sprintf(szFormatString, "%%%d.%dlf", FieldLength(i),
						m_ffpFoxFields[i].fpoint);

                sprintf(m_szTmpBuffer, szFormatString, lfDoubleValue);

				if ((long)strlen(m_szTmpBuffer) > FieldLength(i))
                {
                    TCString sOverflowCause;
                    sOverflowCause = "Field Name : " + FieldName(i) + "\n";
                    sOverflowCause += "Length Needed : "
                            + IntToStr(FieldLength(i)) + "\n";
                    sOverflowCause += "The Data Input is : "
                            + TCString(m_szTmpBuffer) + "\n";
                    sOverflowCause += "The Data Length is : "
                            + IntToStr(strlen(m_szTmpBuffer)) + "\n";
                    throw TCException("TCFoxDBF::PutDBFValue() : "
                            "Number overflow.\n" + sOverflowCause);
                }
                    // memset(m_szTmpBuffer, '-', FieldLength(i));

                memcpy(m_pRecordBuffer + m_fbpFoxBinds[i].start,
                        m_szTmpBuffer, FieldLength(i));

				break;
            }   // end of case 'N'
            default:
                continue;
        }   // end of switch (FieldType(i))
    }   // end of for (i = 0; i < FieldAmount(); i++)
}

//==========================================================================
// ���� : TCFoxDBF::ReadRecordIntoStringList
// ��; : ��ȡһ����¼�����ݵ�StringList��
// ԭ�� : void ReadRecordIntoStringList(TCStringList & slRecordValue,
//              long nRecordSeq = -1);
// ���� : Ҫ���ֵ��StringList����, ��¼��
// ���� : ��
// ˵�� :
//==========================================================================
void TCFoxDBF::ReadRecordIntoStringList(TCStringList & slRecordValue,
        long nRecordSeq)
{
    //======= 1. ���ָ���˼�¼�ţ���λ���ü�¼�ţ�����ȡ��ǰ��¼ֵ ========
    if (nRecordSeq != -1)
    {
        ASSERT(nRecordSeq <= RecCount());
        m_nCurrentRecordSeq = nRecordSeq - 1;
        ReadRecordToBuffer();
    }

    //====== 2. ���StringList�е����� ========
    slRecordValue.Clear();

    //==== 3. ���StringList������ =======
    long i;

    for (i = 0; i < FieldAmount(); i++)
        slRecordValue.Add(GetCurrentDataByFieldSeq(i));
}

//==========================================================================
// ���� : TCFoxDBF::ReadRecordToBuffer
// ��; : ��ȡһ����¼�����ݵ�������
// ԭ�� : void TCFoxDBF::ReadRecordToBuffer();
// ���� : ��
// ���� : ��
// ˵�� : ��¼�����ݴ洢��m_pRecordBuffer��
//==========================================================================
void TCFoxDBF::ReadRecordToBuffer()
{
    //======== 1. �жϵ�ǰ��¼�Ƿ񳬳���Χ���������趨�ļ�ͷ��β��־
    m_bBOF = false;
    m_bEOF = false;

    if (m_nCurrentRecordSeq >= RecCount())
        m_bEOF = true;

    if (m_nCurrentRecordSeq < 0)
        m_bBOF = true;

    if (m_bEOF || m_bBOF)
        return;

    //========== 2. ��ȡһ����¼����¼�������� =========
    long nReaded;

    m_fDBFFile.Seek(m_nRecordAddress + m_nCurrentRecordSeq * RecordLength(),
            SEEK_SET);
    nReaded = m_fDBFFile.Read(m_pRecordBuffer, RecordLength());
    if (nReaded != RecordLength())
        throw TCException("RecordRead Error. DBF File May be Corruptted. - "
                + m_sDBFFileName);
}

//==========================================================================
// ���� : TCFoxDBF::RegulateHeaderByteOrder
// ��; : ����DBF�ļ�ͷ�ṹ���ֽ���
// ԭ�� : void RegulateHeaderByteOrder();
// ���� : ��
// ���� : ��
// ˵�� : ����DBFͷ������洢�����ǰ�Intel��洢�ļ�¼������¼���ȣ���ʼ��
//        ַ����Ϣ�������ڶ����д��ͷ�ṹʱҪ���ñ����������ֽ��������
//==========================================================================
void TCFoxDBF::RegulateHeaderByteOrder(TSFOXHEAD &fxhFoxHead)
{
    TCSystem::RegulateMachineOrder(&fxhFoxHead.recnum,
            sizeof(fxhFoxHead.recnum), boIntel);
    TCSystem::RegulateMachineOrder(&fxhFoxHead.reclen,
            sizeof(fxhFoxHead.reclen), boIntel);
    TCSystem::RegulateMachineOrder(&fxhFoxHead.recaddr,
            sizeof(fxhFoxHead.recaddr), boIntel);
}

//==========================================================================
// ���� : TCFoxDBF::Skip
// ��; : �Ե�ǰ��¼Ϊ��׼��ǰ���ƶ���¼
// ԭ�� : void Skip(long nRecordOffset = 1);
// ���� : ƫ������ȱʡΪ1
// ���� : ��
// ˵�� : 
//==========================================================================
void TCFoxDBF::Skip(long nRecordOffset)
{
    m_nCurrentRecordSeq += nRecordOffset;

    GetDBFRecValue();
}

//==========================================================================
// ���� : TCFoxDBF::SortDBFFile
// ��; : ����һ��DBF�ļ�
// ԭ�� : static void SortDBFFile(TCString sDBFFileName, TCString sFieldNames);
// ���� : sSortedFileName - Ҫ�����DBF�ļ���
//        sFieldNames     - �ֶ���������ж���ֶΣ�����","�ָ�
// ���� : ��
// ˵�� : ��DBF�ļ������������ļ�������Դ�ļ���
//==========================================================================
void TCFoxDBF::SortDBFFile(TCString sDBFFileName, TCString sFieldNames)
{
    TCString sTempDBFFileName;

    sTempDBFFileName = sDBFFileName + "_sort_ing_";

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(sDBFFileName);

    fdFoxDBF.SortToFile(sTempDBFFileName, sFieldNames);

    fdFoxDBF.CloseDBF();

    if (!DeleteFile(sDBFFileName))
        throw TCException("TCFoxDBF::SortDBFFile() Error : "
                " Delete File Error - " + sDBFFileName);

    if (!RenameFile(sTempDBFFileName, sDBFFileName))
        throw TCException("TCFoxDBF::RenameFile() Error : "
                " Rename File Error - " + sTempDBFFileName
                + " to " + sDBFFileName);
}

//==========================================================================
// ���� : TCFoxDBF::SortToFile
// ��; : ���ֶ�����
// ԭ�� : void SortToFile(TCString sSortedFileName, TCString sFieldNames)
// ���� : sSortedFileName - ������DBF�ļ���
//        sFieldNames     - �ֶ���������ж���ֶΣ�����","�ָ�
// ���� : ��
// ˵�� :
//==========================================================================
void TCFoxDBF::SortToFile(TCString sSortedFileName, TCString sFieldNames)
{
    ASSERT(sSortedFileName != GetDBFFileName());
    
    //========= 1. ����Ҫ������ֶ� ======
    TCStringList slFieldNameList;
    slFieldNameList.CommaText(sFieldNames);

    //======= 2. ������ֶε�FieldSeq =========
    const long FIELD_SEQ_MAX_COUNT = 16;
    ASSERT(slFieldNameList.GetCount() <= FIELD_SEQ_MAX_COUNT);
    long FieldSeqList[FIELD_SEQ_MAX_COUNT];
    long i;

    for (i = 0; i < slFieldNameList.GetCount(); i++)
        FieldSeqList[i] = FieldSeq(slFieldNameList[i]);

    //======== 3. ���������StringList���ϸ��ֶε�ֵ =====
#ifdef __TEST__
    printf("Preparing StringList...\n");
#endif

    long nFieldSeq;
    TCStringList slWantSortList;
    TCString sWantSortString;
    long nRecordNumber;

    for (nRecordNumber = 1; nRecordNumber <= RecCount(); nRecordNumber++)
    {
        sWantSortString = "";

        Go(nRecordNumber);

        for (i = 0; i < slFieldNameList.GetCount(); i++)
        {
            nFieldSeq = FieldSeqList[i];
            sWantSortString += Padr(GetCurrentDataByFieldSeq(nFieldSeq),
                    FieldLength(nFieldSeq));
        }

        //======= 4. �ַ�������ϼ�¼�� ======
        sWantSortString += Padl(IntToStr(nRecordNumber), 10);

        slWantSortList.Add(sWantSortString);
    }

    //======== 5. ��StringList�������� ======
#ifdef __TEST__
    printf("Sorting...\n");
#endif
    slWantSortList.Sort();

    //========= 6. ����Ŀ���ļ� ======
    CreateFoxDBFBySelf(sSortedFileName);

    TCStringList slRecordValue;
    TCFoxDBF fdFoxDest;

    fdFoxDest.AttachFile(sSortedFileName);

    //======= 7. ������ȡ��¼����д��Ŀ���ļ� ======
#ifdef __TEST__
    printf("Writing Sorted File...\n");
#endif
    for (i = 0; i < slWantSortList.GetCount(); i++)
    {
        nRecordNumber = StrToInt(Right(slWantSortList[i], 10));
        Go(nRecordNumber);
        ReadRecordIntoStringList(slRecordValue);
        fdFoxDest.AppendRecordByStringList(slRecordValue);
    }

    fdFoxDest.CloseDBF();
}

//==========================================================================
// ���� : TCFoxDBF::Update
// ��; : ���¼�¼
// ԭ�� : void Update();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCFoxDBF::Update()
{
    if (m_nCurrentRecordSeq < 0 || m_nCurrentRecordSeq >= RecCount())
        throw TCException("TCFoxDBF::InsertRecordByStringList() Error - \n"
                " CurrentRecord : " + IntToStr(m_nCurrentRecordSeq)
                + "\n TotalRecord : " + IntToStr(RecCount()));

    m_fDBFFile.Seek(m_nRecordAddress + m_nCurrentRecordSeq * RecordLength());
    m_fDBFFile.Read(m_pRecordBuffer, RecordLength());

    m_pRecordBuffer[0] = ' ';
    PutDBFValue();

    m_fDBFFile.Seek(m_nRecordAddress + m_nCurrentRecordSeq * RecordLength());
    m_fDBFFile.Write(m_pRecordBuffer, RecordLength());
}

//==========================================================================
// ���� : TCFoxDBF::UpdateRecordByStringList
// ��; : ����StringList�е����ݸ���DBF�еļ�¼
// ԭ�� : void UpdateRecordByStringList(TCStringList & slRecordValue);
// ���� : �洢��¼ֵ��StringList
// ���� : ��
// ˵�� :
//==========================================================================
void TCFoxDBF::UpdateRecordByStringList(TCStringList & slRecordValue)
{
    if (m_nCurrentRecordSeq < 0 || m_nCurrentRecordSeq >= RecCount())
        throw TCException("TCFoxDBF::InsertRecordByStringList() Error - \n"
                " CurrentRecord : " + IntToStr(m_nCurrentRecordSeq)
                + "\n TotalRecord : " + IntToStr(RecCount()));

    FillRecordBufferByStringList(slRecordValue);
    m_pRecordBuffer[0] = ' ';

    m_fDBFFile.Seek(m_nRecordAddress + m_nCurrentRecordSeq * RecordLength());
    m_fDBFFile.Write(m_pRecordBuffer, RecordLength());
}

//==========================================================================
// ���� : TCFoxDBF::WriteFoxHeader
// ��; : д��DBF���ļ�ͷ
// ԭ�� : void WriteFoxHeader();
// ���� : ��
// ���� : ��
// ˵�� : �����ֽ������⣬д���ļ�ͷǰ��Ҫ���ļ�ͷ�е�������ݵ����ֽ�
//        �򡣸ú�����д���ļ�ͷʱ���á�
//==========================================================================
void TCFoxDBF::WriteFoxHeader()
{
    m_fDBFFile.Seek(0, SEEK_SET);

    RegulateHeaderByteOrder(m_fhFoxHead);
    m_fDBFFile.Write(&m_fhFoxHead, sizeof(TSFOXHEAD));
    RegulateHeaderByteOrder(m_fhFoxHead);
}

//==========================================================================
// ���� : TCFoxDBF::ZAP
// ��; : ���DBF�ļ�
// ԭ�� : void ZAP();
// ���� : ��
// ���� : ��
// ˵�� : 
//==========================================================================
void TCFoxDBF::ZAP()
{
    m_fDBFFile.Truncate(m_nRecordAddress);

    m_nCurrentRecordSeq = 0;

    m_fhFoxHead.recnum = 0;

    WriteFoxHeader();
}

//==========================================================================
// ���� :
// ��; :
// ԭ�� : void ImportFromTextFile(TCString sTxtFileName, char cCommaChar = ',',
//                  bool bCheckFieldAmount = true);
// ���� :
// ���� :
// ˵�� :
//==========================================================================
void TCFoxDBF::ImportFromTextFile(TCString sTxtFileName, char cCommaChar,
        bool bCheckFieldAmount)
{
    TCFileStream fTxtFile;
    TCString sLine;
    TCStringList slFields;
    fTxtFile.Open(sTxtFileName, omText | omRead);
    while (true)
    {
        if (fTxtFile.GetString(sLine) == NULL)
            break;
        if (AllTrim(sLine) == TCString("") )
            continue;
        slFields.CommaText(sLine, cCommaChar, false);

        if (!bCheckFieldAmount)
        {
            if (slFields.GetCount() > FieldAmount())
            {
                for (long i = slFields.GetCount()-1; i > FieldAmount()-1;
                        i --)
                    slFields.Delete(i);
            }
            else if (slFields.GetCount() < FieldAmount())
            {
                for (long i = slFields.GetCount()+1; i <= FieldAmount(); i++)
                    slFields.Add("");
            }
        }

        AppendRecordByStringList(slFields);
    }
    fTxtFile.Close();
}

void TCFoxDBF::ExportToTextFile(TCString sTxtFileName, char cCommaChar)
{
    TCFileStream fTxtFile;
    TCString sLine;
    TCStringList slFields;
    fTxtFile.Open(sTxtFileName, omText | omWrite);

    long i, j;
    for (i = 1; i <= RecCount(); i++)
    {
        ReadRecordIntoStringList(slFields, i);
        sLine = "";
        for (j = 0; j < slFields.GetCount(); j++)
        {
            if (j == 0)
                sLine += AllTrim(slFields[j]);
            else
                sLine += cCommaChar + AllTrim(slFields[j]);
        }
        fTxtFile.WriteLn(sLine);
    }
    fTxtFile.Close();
}

//==========================================================================
// ���� : TCDBFCreate::TCDBFCreate
// ��; : ���캯��
// ԭ�� : TCDBFCreate(TCString sDBFName, long nFieldCount);
// ���� : DBF�ļ������ֶ���
// ���� : ��
// ˵�� : �����ֶζ���Ŀռ��Ա�֮���AddField��������д�롣
//==========================================================================
TCDBFCreate::TCDBFCreate(TCString sDBFName, long nFieldCount)
{
    ASSERT(nFieldCount > 0);
    m_nFieldCount = nFieldCount;
    m_fdFieldsDef = new TSFoxFieldDef[nFieldCount];
    m_nFieldCurrentSeq = -1;
    m_sDBFName = sDBFName;
}

//==========================================================================
// ���� : TCDBFCreate::~TCDBFCreate
// ��; : ��������
// ԭ�� : ~TCDBFCreate();
// ���� : ��
// ���� : ��
// ˵�� : �ͷ��ڹ��캯��������ֶζ��岿�ֵ��ڴ�
//==========================================================================
TCDBFCreate::~TCDBFCreate()
{
    delete [] m_fdFieldsDef;
}

//==========================================================================
// ���� : TCDBFCreate::AddField
// ��; : ����һ���ֶζ���
// ԭ�� : void TCDBFCreate::AddField(TCString sFieldName, char cFieldType,
//          BYTE nFieldLen = 0, BYTE nFieldPoint = 0)
// ���� : �ֶ������ֶ����ͣ��ֶγ��ȣ�С�����ĳ���
// ���� : ��
// ˵�� : ֧�������ֶ����� C D N L
//        �����ָ���ֶγ��ȣ���ȱʡ�����:
//        C - 1, D - 8, N - 9, L - 1
//==========================================================================
void TCDBFCreate::AddField(TCString sFieldName, char cFieldType,
        long nFieldLen, long nFieldPoint)
{
    m_nFieldCurrentSeq ++;

    if (m_nFieldCurrentSeq >= m_nFieldCount)
        throw TCException("TCDBFCreate::AddField Error.  FieldSeq Exceeded. - "
                + m_sDBFName);

    ASSERT(cFieldType == 'C' || cFieldType == 'D' || cFieldType == 'N'
            || cFieldType == 'L');

    ASSERT(cFieldType == 'N' || nFieldPoint == 0);

    if (nFieldLen == 0)
        switch (cFieldType)
        {
        case 'D':
            nFieldLen = 8;
            break;
        case 'C':
            nFieldLen = 1;
            break;
        case 'N':
            nFieldLen = 9;
            break;
        case 'L':
            nFieldLen = 1;
            break;
        }

    ASSERT(nFieldPoint + 1 <= nFieldLen);
    ASSERT(nFieldLen > 0);
    ASSERT(cFieldType != 'L' || nFieldLen == 1);
    ASSERT(cFieldType != 'D' || nFieldLen == 8);

    m_fdFieldsDef[m_nFieldCurrentSeq].fldname = sFieldName;
    m_fdFieldsDef[m_nFieldCurrentSeq].ftype = cFieldType;
    m_fdFieldsDef[m_nFieldCurrentSeq].flen = (unsigned char)nFieldLen;
    m_fdFieldsDef[m_nFieldCurrentSeq].fpoint = (char)nFieldPoint;
}

//==========================================================================
// ���� : TCDBFCreate::CreateDBF
// ��; : ����������ļ������ֶζ��崴��DBF�ļ�
// ԭ�� : void CreateDBF();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCDBFCreate::CreateDBF()
{
    ASSERT(m_nFieldCurrentSeq == m_nFieldCount - 1);

    //====== 1. ���FOX�ļ�ͷ ========
    TSFOXHEAD fxhFoxHead;
    long nYear, nMonth, nDay;
    long i;

    fxhFoxHead.sign = 3;

    nYear = TCTime::Year(TCTime::Now());
    nMonth = TCTime::Month(TCTime::Now());
    nDay = TCTime::Day(TCTime::Now());
    fxhFoxHead.year = BYTE(nYear % 100);
    fxhFoxHead.month = BYTE(nMonth);
    fxhFoxHead.day = BYTE(nDay);

    fxhFoxHead.recnum = 0;

    fxhFoxHead.recaddr = sizeof(TSFOXHEAD) + 1;
    fxhFoxHead.reclen = 1;

    for (i = 0; i < m_nFieldCount; i++)
    {
        fxhFoxHead.recaddr += WORD(sizeof(TSFOXFIELD));
        fxhFoxHead.reclen += WORD(m_fdFieldsDef[i].flen);
    }

    memset(fxhFoxHead.sp, '\0', sizeof(fxhFoxHead.sp));

    //======= 2. ���ļ�����д���ļ�ͷ ==========
    TCFileStream fDestDBF;

    //yxw add ���ļ�ʱӦ����
    fDestDBF.Open(m_sDBFName, omWrite |omExclusive_Waiting);
    TCFoxDBF::RegulateHeaderByteOrder(fxhFoxHead);
    fDestDBF.Write(&fxhFoxHead, sizeof(TSFOXHEAD));      
    TCFoxDBF::RegulateHeaderByteOrder(fxhFoxHead);

    //======= 3. ׼���ֶζ��岿�� ============
    TSFOXFIELD *ffpFoxFields;
    long nStartOffset;
    TCString sFieldName;

    ffpFoxFields = new TSFOXFIELD[m_nFieldCount];
    memset(ffpFoxFields, '\0', sizeof(TSFOXFIELD)*m_nFieldCount);

    nStartOffset = 1;

    for (i = 0; i < m_nFieldCount; i++)
    {
        // �ֶ���
        FillPadr(ffpFoxFields[i].fldname, UpperCase(m_fdFieldsDef[i].fldname),
                10, '\0');

        // �ֶ�����
        ffpFoxFields[i].ftype = m_fdFieldsDef[i].ftype;
        // �ֶγ���
        ffpFoxFields[i].flen = m_fdFieldsDef[i].flen;
        // С����
        ffpFoxFields[i].fpoint = m_fdFieldsDef[i].fpoint;
        // ƫ����
        memcpy(ffpFoxFields[i].nouse, &nStartOffset, 4);     
        TCSystem::RegulateMachineOrder(ffpFoxFields[i].nouse,
                sizeof(ffpFoxFields[i].nouse), boIntel);

        nStartOffset += m_fdFieldsDef[i].flen;
    }

    // ========= 4. д���ֶζ��� =============
    fDestDBF.Write(ffpFoxFields, sizeof(TSFOXFIELD)*m_nFieldCount);

    delete [] ffpFoxFields;

    // ========= 5. д��β��ǣ��ر��ļ� ========
    char c0D;
    c0D = 0x0D;
    fDestDBF.Write(&c0D, 1);
    fDestDBF.Flush();
    fDestDBF.Close();

    //========= 6. ������޸�����У���ļ���ɾ��֮ ======
    TCString sModifyControlFileName;
    sModifyControlFileName = TCFoxDBF::GetModifyControlFileName(m_sDBFName);

    if (FileExists(sModifyControlFileName))
        DeleteFileE(sModifyControlFileName);
}

//==========================================================================
// ���� : TCDBFStructDefine::TCDBFStructDefine
// ��; : DBF�ṹ�๹�캯��
// ԭ�� : TCDBFStructDefine
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
TCDBFStructDefine::TCDBFStructDefine()
{
    m_nFieldCount = 0;
}

//==========================================================================
// ���� : TCDBFStructDefine::AddField
// ��; : ����һ���ֶζ���
// ԭ�� : void TCDBFCreate::AddField(TCString sFieldName, char cFieldType,
//          BYTE nFieldLen = 0, BYTE nFieldPoint = 0)
// ���� : �ֶ������ֶ����ͣ��ֶγ��ȣ�С�����ĳ���
// ���� : ��
// ˵�� :
//==========================================================================
void TCDBFStructDefine::AddField(TCString sFieldName, char cFieldType,
        long nFieldLen, long nFieldPoint)
{
    m_nFieldCount ++;

    ASSERT(m_nFieldCount <= MAX_FOX_FIELD_COUNT);

    SetFieldDefine(m_nFieldCount - 1, sFieldName, cFieldType,
            nFieldLen, nFieldPoint);
}

//==========================================================================
// ���� : TCDBFStructDefine::Clear
// ��; : ����ֶζ���
// ԭ�� : void TCDBFCreate::Clear
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCDBFStructDefine::Clear()
{
    m_nFieldCount = 0;
}

//==========================================================================
// ���� : TCDBFStructDefine::CreateDBF
// ��; : ���ݶ���Ľṹ����DBF�ļ�
// ԭ�� : void CreateDBF(TCString sFileName);
// ���� : DBF�ļ���
// ���� : ��
// ˵�� :
//==========================================================================
void TCDBFStructDefine::CreateDBF(TCString sFileName)
{
    long i;

    TCDBFCreate dcCreate(sFileName, FieldCount());

    for (i = 0; i < FieldCount(); i++)
        dcCreate.AddField(m_fdFieldsDef[i].fldname, m_fdFieldsDef[i].ftype,
                m_fdFieldsDef[i].flen, m_fdFieldsDef[i].fpoint);

    dcCreate.CreateDBF();
}

//==========================================================================
// ���� : TCDBFStructDefine::FieldCount
// ��; : �õ��ֶ�����
// ԭ�� : long FieldCount();
// ���� : ��
// ���� : �ֶ�����
// ˵�� :
//==========================================================================
long TCDBFStructDefine::FieldCount()
{
    return m_nFieldCount;
}

//==========================================================================
// ���� : TCDBFStructDefine::FieldName
// ��; : �����ֶ���ŵõ��ֶ���
// ԭ�� : long FieldName(long nFieldSeq);
// ���� : �ֶ����
// ���� : �ֶ���
// ˵�� : �ֶ���Ŵ�0��ʼ
//==========================================================================
TCString TCDBFStructDefine::FieldName(long nFieldSeq)
{
    ASSERT(nFieldSeq < m_nFieldCount);

    return UpperCase(m_fdFieldsDef[nFieldSeq].fldname);
}

//==========================================================================
// ���� : TCDBFStructDefine::FieldSeq
// ��; : �����ֶ����õ��ֶ����
// ԭ�� : long FieldSeq(TCString sFieldName);
// ���� : �ֶ���
// ���� : �ֶ����
// ˵�� : �ֶ���Ŵ�0��ʼ
//==========================================================================
long TCDBFStructDefine::FieldSeq(TCString sFieldName)
{
    long i;

    for (i = 0; i < m_nFieldCount; i++)
        if (UpperCase(m_fdFieldsDef[i].fldname) == UpperCase(sFieldName))
            return i;

    throw TCException("TCDBFStructDefine::FieldSeq() Error : "
            " Cannot Search out Field - " + sFieldName);
}

//==========================================================================
// ���� : TCDBFStructDefine::InsertField
// ��; : ����һ���ֶζ���
// ԭ�� : void InsertField(long nFieldSeq, TCString sFieldName,
//              char cFieldType, BYTE nFieldLen = 0, BYTE nFieldPoint = 0)
// ���� : �ֶ����ֶ������ֶ����ͣ��ֶγ��ȣ�С�����ĳ���
// ���� : ��
// ˵�� :
//==========================================================================
void TCDBFStructDefine::InsertField(long nFieldSeq, TCString sFieldName,
        char cFieldType, BYTE nFieldLen, BYTE nFieldPoint)
{
    ASSERT(nFieldSeq >= 0);
    ASSERT(nFieldSeq < m_nFieldCount);
    ASSERT(m_nFieldCount < MAX_FOX_FIELD_COUNT);

    long i;

    for (i = m_nFieldCount - 1; i >= nFieldSeq; i--)
    {
        m_fdFieldsDef[i + 1].fldname = m_fdFieldsDef[i].fldname;
        m_fdFieldsDef[i + 1].ftype   = m_fdFieldsDef[i].ftype;
        m_fdFieldsDef[i + 1].flen    = m_fdFieldsDef[i].flen;
        m_fdFieldsDef[i + 1].fpoint  = m_fdFieldsDef[i].fpoint;
    }

    SetFieldDefine(nFieldSeq, sFieldName, cFieldType, nFieldLen, nFieldPoint);

    m_nFieldCount ++;
}

//==========================================================================
// ���� : TCDBFStructDefine::SetFieldDefine
// ��; : ����һ���ֶζ���
// ԭ�� : void SetFieldDefine(long nFieldSeq, TCString sFieldName,
//              char cFieldType, BYTE nFieldLen, BYTE nFieldPoint);
// ���� : �ֶ������ֶ����ͣ��ֶγ��ȣ�С�����ĳ���
// ���� : ��
// ˵�� : ֧�������ֶ����� C D N L
//        �����ָ���ֶγ��ȣ���ȱʡ�����:
//        C - 1, D - 8, N - 9, L - 1
//==========================================================================
void TCDBFStructDefine::SetFieldDefine(long nFieldSeq, TCString sFieldName,
        char cFieldType, long nFieldLen, long nFieldPoint)
{
    ASSERT(nFieldSeq < MAX_FOX_FIELD_COUNT);

    ASSERT(cFieldType == 'C' || cFieldType == 'D' || cFieldType == 'N'
            || cFieldType == 'L');

    ASSERT(cFieldType == 'N' || nFieldPoint == 0);

    if (nFieldLen == 0)
        switch (cFieldType)
        {
        case 'D':
            nFieldLen = 8;
            break;
        case 'C':
            nFieldLen = 1;
            break;
        case 'N':
            nFieldLen = 9;
            break;
        case 'L':
            nFieldLen = 1;
            break;
        }

    ASSERT(nFieldPoint + 1 <= nFieldLen);
    ASSERT(nFieldLen > 0);
    ASSERT(cFieldType != 'L' || nFieldLen == 1);
    ASSERT(cFieldType != 'D' || nFieldLen == 8);

    m_fdFieldsDef[nFieldSeq].fldname = sFieldName;
    m_fdFieldsDef[nFieldSeq].ftype = cFieldType;
    m_fdFieldsDef[nFieldSeq].flen = (unsigned char)nFieldLen;
    m_fdFieldsDef[nFieldSeq].fpoint = (char)nFieldPoint;
}

#ifdef __TEST__

void DisplayTestDBFPrompt()
{
    printf("\n\n==== Test DBF ====\n\n");
    printf("0. Create DBF            ");
    printf("1. Append                \n");
    printf("2. Delete                ");
    printf("3. Insert                \n");
    printf("4. ZAP                   ");
    printf("5. Pack                  \n");
    printf("6. RecCount              ");
    printf("7. Browse                \n");
    printf("8. CreateDBFBySelf       ");
    printf("9. GetDataByFieldSeq     \n");
    printf("A. GetDBData             ");
    printf("B. Update                \n");
    printf("C. Read Into StringList  ");
    printf("D. Append By StringList  \n");
    printf("E. Insert By StringList  ");
    printf("F. Update By StringList  \n");
    printf("G. Alter Table           ");
    printf("H. Sort To Other Table   \n");
    printf("I. Sort Table Self       ");
    printf("J. Export To Text File   \n");
    printf("K. Import From Text File \n");
    printf("Q. Quit\n\n");
}

const TCString TEST_DBF = "C:\\TEMP\\PERSON1.DBF";

void TestDBF0CreateDBF()
{
    TCDBFCreate dcCreate(TEST_DBF, 5);
    dcCreate.AddField("name", 'C', 10);
    dcCreate.AddField("age", 'N', 7);
    dcCreate.AddField("career", 'C', 12);
    dcCreate.AddField("birthday", 'D');
    dcCreate.AddField("married", 'L');
    dcCreate.CreateDBF();
}

void TestDBF1Append()
{
    long i;

    TCString sName, sCareer, sBirthday;
    long nAge, nMarried;

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);

    fdFoxDBF.DBBind("name", sName);
    fdFoxDBF.DBBind("age", nAge);
    fdFoxDBF.DBBind("career", sCareer);
    fdFoxDBF.DBBind("birthday", sBirthday);
    fdFoxDBF.DBBind("married", nMarried);

    for (i = 1; i <= 1000; i++)
    {
        sName = "PS"+IntToStr(i);
        nAge = i % 20 + 5;
        sCareer = "ComputerS";
        sBirthday = TCTime::RelativeDate(TCTime::Today(), (-1)*nAge*365);

        if (nAge < 20)
            nMarried = 0;
        else
            nMarried = nAge % 3;

        fdFoxDBF.Append();
    }

    fdFoxDBF.CloseDBF();
}

void TestDBF2Delete()
{
    long nRecCount = 0;
    long nDeleteCount = 0;

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);
    fdFoxDBF.GoTop();

    while (!fdFoxDBF.Eof())
    {
        if (!fdFoxDBF.Deleted())
        {
            nRecCount ++;

            if ((nRecCount % 4) == 0)
            {
                nDeleteCount ++;
                fdFoxDBF.Delete();
            }
        }
        
        fdFoxDBF.Skip();
    }

    fdFoxDBF.CloseDBF();

    printf("%d Records Deleted", nDeleteCount);
}

void TestDBF3Insert()
{
    long i;

    TCString sName, sCareer, sBirthday;
    long nAge, nMarried;

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);

    fdFoxDBF.DBBind("name", sName);
    fdFoxDBF.DBBind("age", nAge);
    fdFoxDBF.DBBind("career", sCareer);
    fdFoxDBF.DBBind("birthday", sBirthday);
    fdFoxDBF.DBBind("married", nMarried);

    for (i = 1; i <= 10; i++)
    {
        fdFoxDBF.Go(i * 10);
        sName = "====IN====";
        nAge = i * 100;
        sCareer = "INSERTED";
        sBirthday = TCTime::Today();

        nMarried = i % 3;

        fdFoxDBF.Insert();
    }

    fdFoxDBF.CloseDBF();
}

void TestDBF4ZAP()
{
    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);
    fdFoxDBF.ZAP();
    printf("ZAPped.\n");
    fdFoxDBF.CloseDBF();
}

void TestDBF5Pack()
{
    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);
    fdFoxDBF.Pack();
    printf("Packed.\n");
    fdFoxDBF.CloseDBF();
}

void TestDBF6RecCount()
{
    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);
    printf("Record Count : %d\n", fdFoxDBF.RecCount());
    fdFoxDBF.CloseDBF();
}

void TestDBF8CreateDBFBySelf()
{
    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);
    fdFoxDBF.CreateFoxDBFBySelf("C:\\TEMP\\PERSON_CDS.DBF");
    fdFoxDBF.CloseDBF();
}

void TestDBF9GetDataByFieldSeq()
{
    long i;
    TCString sData;

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);
    fdFoxDBF.Go(20);

    for (i = 0; i < fdFoxDBF.FieldAmount(); i++)
    {
        sData = fdFoxDBF.GetCurrentDataByFieldSeq(i);
        printf("%d : %s\n", i, (char *)sData);
    }

    fdFoxDBF.CloseDBF();
}

void TestDBFAGetDBData()
{
    long i;
    TCString sData;

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);

    for (i = 0; i < fdFoxDBF.FieldAmount(); i++)
    {
        sData = fdFoxDBF.GetDBData(20, fdFoxDBF.FieldName(i));
        printf("%d : %s\n", i, (char *)sData);
    }

    fdFoxDBF.CloseDBF();
}

void TestDBFBUpdate()
{
    long nRecCount = 0;
    long nUpdateCount = 0;
    TCString sName;
    long nAge;

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);

    fdFoxDBF.DBBind("name", sName);
    fdFoxDBF.DBBind("age", nAge);

    fdFoxDBF.GoTop();

    while (!fdFoxDBF.Eof())
    {
        nRecCount ++;

        if ((nRecCount % 3) == 0)
        {
            nUpdateCount ++;
            sName = "====<<<<>>>>====";
            nAge = 100;
            fdFoxDBF.Update();
        }

        fdFoxDBF.Skip();
    }

    fdFoxDBF.CloseDBF();

    printf("%d Records Updated", nUpdateCount);
}

void TestDBFCReadIntoStringList()
{
    long i, j;
    TCStringList slRecordValue;

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);

    for (i = 1; i <= fdFoxDBF.RecCount(); i++)
    {
        printf("======= %d =======\n", i);
        fdFoxDBF.Go(i);
        fdFoxDBF.ReadRecordIntoStringList(slRecordValue);

        for (j = 0; j < slRecordValue.GetCount(); j++)
        {   printf("%d: %s\n", j, (char *)slRecordValue[j]);
        }
    }

    fdFoxDBF.CloseDBF();

    fflush(stdin);
    getchar();
}

void TestDBFDAppendByStringList()
{
    long i;
    TCStringList slRecordValue;

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);

    for (i = 1; i <= 10; i++)
    {
        slRecordValue.Clear();
        slRecordValue.Add("NAME");
        slRecordValue.Add("88.8");
        slRecordValue.Add("ENGINEER1234567890");
        slRecordValue.Add("20010121");
        slRecordValue.Add("T");
        fdFoxDBF.AppendRecordByStringList(slRecordValue);
    }

    fdFoxDBF.CloseDBF();
}

void TestDBFEInsertByStringList()
{
    long i;
    TCStringList slRecordValue;

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);

    for (i = 1; i <= 10; i++)
    {
        slRecordValue.Clear();
        slRecordValue.Add("NAME");
        slRecordValue.Add("88.8");
        slRecordValue.Add("ENGINEER1234567890");
        slRecordValue.Add("20010121");
        slRecordValue.Add("T");
        fdFoxDBF.Go(i * 10);
        fdFoxDBF.InsertRecordByStringList(slRecordValue);
    }

    fdFoxDBF.CloseDBF();
}

void TestDBFFUpdateByStringList()
{
    long i, j;
    TCStringList slRecordValue;

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);

    for (i = 1; i <= fdFoxDBF.RecCount(); i++)
    {
        fdFoxDBF.Go(i);
        fdFoxDBF.ReadRecordIntoStringList(slRecordValue);

        for (j = 0; j < slRecordValue.GetCount(); j++)
            slRecordValue[j] = slRecordValue[j] + "0";

        fdFoxDBF.UpdateRecordByStringList(slRecordValue);
    }

    fdFoxDBF.CloseDBF();
}

void TestDBFGAlterTable()
{
    TCFoxDBF fdFoxDBF, fdFoxDBF2;
    fdFoxDBF.AttachFile(TEST_DBF);

    TCDBFStructDefine dsDefine;
    fdFoxDBF.FetchDBFStructDefine(dsDefine);

    dsDefine.InsertField(1, "myf", 'C', 20);
    dsDefine.CreateDBF("c:\\temp\\person2.dbf");

    fdFoxDBF2.AttachFile("c:\\temp\\person2.dbf");

    long i;
    TCStringList slRecordValue;
    for (i = 1; i <= fdFoxDBF.RecCount(); i++)
    {
        fdFoxDBF.ReadRecordIntoStringList(slRecordValue, i);
        slRecordValue.Insert(1, "ABCDEFGHIJ" + IntToStr(i));
        fdFoxDBF2.AppendRecordByStringList(slRecordValue);
    }

    fdFoxDBF.CloseDBF();
    fdFoxDBF2.CloseDBF();
}

void TestDBFHSortTable()
{
    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);

    TCString sDestFileName;
    sDestFileName = CutFileExt(TEST_DBF) + "_SRT" + ExtractFileExt(TEST_DBF);
    fdFoxDBF.SortToFile(sDestFileName, "age,name");

    fdFoxDBF.CloseDBF();

    printf("   ==== Press Any Key ====\n");
    fflush(stdin);
    getchar();
}

void TestDBFISortTableSelf()
{
    TCFoxDBF::SortDBFFile(TEST_DBF, "age,name");

    printf("   ==== Press Any Key ====\n");
    fflush(stdin);
    getchar();
}

void TestDBFJExportToTextFile()
{
    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);
    fdFoxDBF.ExportToTextFile("c:\\temp\\person1_export.txt");
    printf("person1_export.txt exported.\n");
    fdFoxDBF.CloseDBF();
}

void TestDBFKImportFromTextFile()
{
    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(TEST_DBF);
    fdFoxDBF.ImportFromTextFile("c:\\temp\\person1_export.txt", ',', false);
    printf("person1_export.txt imported.\n");
    fdFoxDBF.CloseDBF();
}

void TestDBFMainFunc()
{
    int cChar;

    DisplayTestDBFPrompt();
    while (true)
    {
        cChar = getchar();

        switch (cChar)
        {
            case 'Q':
            case 'q':
            case 0x1B:
                return;

            case '0':
                TestDBF0CreateDBF();
                break;

            case '1':
                TestDBF1Append();
                break;

            case '2':
                TestDBF2Delete();
                break;

            case '3':
                TestDBF3Insert();
                break;

            case '4':
                TestDBF4ZAP();
                break;

            case '5':
                TestDBF5Pack();
                break;
                
            case '6':
                TestDBF6RecCount();
                break;

            case '8':
                TestDBF8CreateDBFBySelf();
                break;

            case '9':
                TestDBF9GetDataByFieldSeq();
                break;

            case 'a':
            case 'A':
                TestDBFAGetDBData();
                break;

            case 'b':
            case 'B':
                TestDBFBUpdate();
                break;

            case 'c':
            case 'C':
                TestDBFCReadIntoStringList();
                break;

            case 'd':
            case 'D':
                TestDBFDAppendByStringList();
                break;

            case 'e':
            case 'E':
                TestDBFEInsertByStringList();
                break;

            case 'f':
            case 'F':
                TestDBFFUpdateByStringList();
                break;

            case 'g':
            case 'G':
                TestDBFGAlterTable();
                break;

            case 'h':
            case 'H':
                TestDBFHSortTable();
                break;

            case 'i':
            case 'I':
                TestDBFISortTableSelf();
                break;

            case 'j':
            case 'J':
                TestDBFJExportToTextFile();
                break;

            case 'k':
            case 'K':
                TestDBFKImportFromTextFile();
                break;

            default:
                continue;
        }
        DisplayTestDBFPrompt();
    }
}

#endif

