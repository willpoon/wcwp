//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_foxdbf.h"

//---------------------------------------------------------------------------

//==========================================================================
// 函数 : 构造函数
// 用途 : 初始化变量
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
// 函数 : TCFoxDBF析构函数
// 用途 : 析构时，关闭文件(如果文件已打开)
//==========================================================================
TCFoxDBF::~TCFoxDBF()
{   CloseDBF();
}

//==========================================================================
// 函数 : TCFoxDBF::Append
// 用途 : 增加一条记录
// 原型 : void Append();
// 参数 : 无
// 返回 : 无
// 说明 : 为增加速度，在进行Append操作时，不刷新文件头及记录文件尾。而是
//        在关闭文件时进行记录。所以进行Append操作后，要及时关闭文件或运
//        行Flush()函数。
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
// 函数 : TCFoxDBF::AppendRecordByStringList
// 用途 : 根据StringList中的数据增加一条数据库记录
// 原型 : void AppendRecordByStringList(TCStringList & slRecordValue);
// 参数 : 存储记录值的StringList
// 返回 : 无
// 说明 :
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
// 函数 : TCFoxDBF::AttachFile
// 用途 : 打开DBF文件，并获取文件信息
// 原型 : void AttachFile(TCString sDBFFileName);
// 参数 : 文件名
// 返回 : 无
// 说明 : 要操作DBF文件前，必须先调用该函数。
//==========================================================================
void TCFoxDBF::AttachFile(TCString sDBFFileName)
{
    //========= 0-. 完整性验证，验证是否修改一半 =====
    if (FileExists(GetModifyControlFileName(sDBFFileName)))
        throw TCException("TCFoxDBF::AttachFile() Error - "
                " The file may be modifying, Check file Integrity.  \n"
                " FileName:" + sDBFFileName);

    m_bModifyingCheck = false;

    //---------------------------------------------------

    m_sDBFFileName = sDBFFileName;

    DWORD       nReaded;
    TSFOXFIELD  ffTmp;

    // ======= 0. 初始化变量 ========
    CloseDBF();

    m_bHasAppended = false;
    m_bFileOpened = false;

    m_nCurrentRecordSeq = -1;

	// ======== 1. 打开DBF文件 ============
    if (!FileExists(sDBFFileName))
        throw TCException("TCFoxDBF::AttachFile() File doesn\'t exist - "
                "FileName : " + sDBFFileName);

    if (m_bOpenWithReadOnly)
        m_fDBFFile.Open(sDBFFileName, omRead | omShared);
    else
        m_fDBFFile.Open(sDBFFileName, omRead | omWrite | omExclusive_Waiting);

    m_bFileOpened = true;

    // =========== 2. 读取DBF文件头 ===============
    // ======== DBF文件头中包含有RecCount、RecordLength信息 ========
    nReaded = m_fDBFFile.Read(&m_fhFoxHead, sizeof(TSFOXHEAD));
    if (nReaded != sizeof(TSFOXHEAD))
        throw TCException("File Header Error FileName - " + m_sDBFFileName);

    if (m_fhFoxHead.sign != 3)
        throw TCException("Header Sign Error, illegal DBF File - "
                + m_sDBFFileName);

    RegulateHeaderByteOrder(m_fhFoxHead);

    //=========== 3. 逐条读取DBF文件的字段描述 ===========
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

    // =========== 4. 给字段分配相应的内存空间 ============
	m_ffpFoxFields = new TSFOXFIELD[m_nFieldAmount];    // 分配字段存储空间
	if (m_ffpFoxFields == NULL)
        throw TCException("Memory Alloc Error");

	m_fbpFoxBinds = new TSFOXBIND[m_nFieldAmount];      // 分配字段绑定存储空间
	if (m_fbpFoxBinds == NULL)
        throw TCException("Memory Alloc Error");

	memset(m_fbpFoxBinds, 0, m_nFieldAmount*sizeof(TSFOXBIND));

    m_pRecordBuffer = new char[RecordLength()];      // 分配单条记录存储空间

    // ========= 5. 读取字段结构 ============
    m_fDBFFile.Seek(sizeof(TSFOXHEAD), SEEK_SET);

    nReaded = m_fDBFFile.Read(m_ffpFoxFields,
            sizeof(TSFOXFIELD) * m_nFieldAmount);
	if (nReaded != sizeof(TSFOXFIELD) * m_nFieldAmount)
        throw TCException("Read FoxField Structure Error - "
                + m_sDBFFileName);

    //========== 6. 校验完整性，比较文件大小是否与记录数相符 =====
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
// 函数 : TCFoxDBF::AttachFileR
// 用途 : 以只读共享方式打开文件
// 原型 : void AttachFileR(TCString sDBFFileName);
// 参数 : 文件名
// 返回 : 无
// 说明 : 用于有多个进程需要同时访问一个文件的场合
// 历史 : 2001.11.29 OLDIX, 增加该函数
//==========================================================================
void TCFoxDBF::AttachFileR(TCString sDBFFileName)
{
    m_bOpenWithReadOnly = true;
    AttachFile(sDBFFileName);
    m_bOpenWithReadOnly = false;
}

//==========================================================================
// 函数 : TCFoxDBF::AttachFileW
// 用途 : 写入打开DBF文件，要求校验完整性
// 原型 : void AttachFileW(TCString sDBFFileName);
// 参数 : 文件名
// 返回 : 无
// 说明 : 1. 用该函数打开，其实打开模式未变，只是有写入需求
//        2. 产生一个文件名sDBFFileName + "_mod_ify_"用以完整性校验
//==========================================================================
void TCFoxDBF::AttachFileW(TCString sDBFFileName)
{
    AttachFile(sDBFFileName);

    m_bModifyingCheck = true;
    CreateBlankFile(GetModifyControlFileName(sDBFFileName));
}

//==========================================================================
// 函数 : TCFoxDBF::CloseDBF
// 用途 : 进行关闭文件，释放内存等操作
// 原型 : void CloseDBF();
// 参数 : 无
// 返回 : 无
// 说明 : 如果文件已被增加记录，则重写文件头并在尾部写入结束字符。
// 历史 : 2001.12.25 在Close之前加入Flush()语句
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

    Flush();    // 历史: 2001.12.25加入此句

    m_bFileOpened = false;

    //========== 因为加入Flush(), 注释掉以下语句 =======
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
    //==================== end of 注释掉 =======================

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
// 函数 : TCFoxDBF::CreateFoxDBFBySelf
// 用途 : 生成一个DBF文件，其结构同当前打开的文件结构
// 原型 : void TCFoxDBF::CreateFoxDBFBySelf(TCString sDestDBF);
// 参数 : 目标文件名
// 返回 : 无
// 说明 : 
//==========================================================================
void TCFoxDBF::CreateFoxDBFBySelf(TCString sDestDBF)
{
    // ====== 1. 打开目标文件 =========
    TCFileStream fDestDBF;
    fDestDBF.Open(sDestDBF, omWrite);

    // ======= 2. 写入文件头 ========
    TSFOXHEAD fxhFoxHead;
    fxhFoxHead = m_fhFoxHead;
    fxhFoxHead.recnum = 0;

    RegulateHeaderByteOrder(fxhFoxHead);
    fDestDBF.Write(&fxhFoxHead, sizeof(TSFOXHEAD));
    RegulateHeaderByteOrder(fxhFoxHead);

    //========= 3. 写入字段描述部分 ==========
    fDestDBF.Write(m_ffpFoxFields, sizeof(TSFOXFIELD) * FieldAmount());

    //======== 4. 关闭文件 ==========
    fDestDBF.Write("\x0D", 1);
    fDestDBF.Close();
}

//==========================================================================
// 函数 : TCFoxDBF::DBBindToPointer
// 用途 : 绑定一个字段到一个指针上
// 原型 : void TCFoxDBF::DBBindToPointer(long nFieldID,
//          TEFoxBindType fbFoxBindType, void *pBindPointer)
// 参数 : 字段序号, 绑定类型，绑定地址
// 返回 : 无
// 说明 : 该函数是字段绑定核心函数，但不是公用函数，而是被一组公用DBBind
//        函数调用。我们用这种方法屏蔽了绑定类型和指针调用，使调用更加简
//        单
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
// 函数 : TCFoxDBF::Delete
// 用途 : 删除当前记录
// 原型 : void Delete();
// 参数 : 无
// 返回 : 无
// 说明 : 只对记录做上标记，如果要真正删除还要调用Pack()函数。
//==========================================================================
void TCFoxDBF::Delete()
{
    if (m_nCurrentRecordSeq < 0 || m_nCurrentRecordSeq >= RecCount())
        return;

    m_fDBFFile.Seek(m_nRecordAddress + m_nCurrentRecordSeq * RecordLength());

    m_fDBFFile.Write("\x2A", 1);
}

//==========================================================================
// 函数 : TCFoxDBF::FetchDBFStructDefine
// 用途 : 取得DBF结构的定义
// 原型 : void FetchDBFStructDefine(TCDBFStructDefine & dsStructDefine);
// 参数 : DBF结构定义类的引用
// 返回 : 无
// 说明 : 
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
// 函数 : TCFoxDBF::FieldName
// 用途 : 得到字段名
// 原型 : TCString FieldName(long nFieldSeq);
// 参数 : 字段序号
// 返回 : 字段名
// 说明 : 字段序号从0开始
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
// 函数 : TCFoxDBF::FillRecordBufferByStringList
// 用途 : 将StringList的内容填充到RecordBuffer中
// 原型 : void FillRecordBufferByStringList(TCStringList & slRecordValue);
// 参数 : 存储记录值的StringList
// 返回 :
// 说明 : StringList的String数量必须与DBF字段数相符
// 历史 : 2001.9.30 将以前如果数字溢出，则填充'-'改为抛例外的情况
//==========================================================================
void TCFoxDBF::FillRecordBufferByStringList(TCStringList & slRecordValue)
{
    //====== 1. 验证String的大小与字段数是否相符 =======
    if (slRecordValue.GetCount() != FieldAmount())
        throw TCException("FillRecordBufferByStringList() Error - \n"
                "StringList Size do not Match Field Amount : \nSize(SL)"
                + IntToStr(slRecordValue.GetCount())+ "  Size(FD)"
                + IntToStr(FieldAmount()));

    //======= 2. 将RecordBuffer中的值清空 =========
    memset(m_pRecordBuffer, ' ', RecordLength());

    //======= 3. 循环设置每一个字段 =======
    long i;

    long nStartPos = 1;     // 每个字段的起始位置

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
                    // memset(m_szTmpBuffer, '-', FieldLength(i)); 注20010930

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
// 函数 : TCFoxDBF::FieldSeq
// 用途 : 根据字段名得到字段序号
// 原型 : long FieldSeq(TCString sFieldName);
// 参数 : 字段名
// 返回 : 字段序号
// 说明 : 字段序号从0开始
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
// 函数 : TCFoxDBF::Flush
// 用途 : 将一些变化情况写入硬盘
// 原型 : void Flush();
// 参数 : 无
// 返回 : 无
// 说明 : 该函数一般在一个阶段操作完成以后调用，以保证数据的完整性。
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
// 函数 : TCFoxDBF::GetCurrentDataByFieldSeq
// 用途 : 得到当前记录的指定字段序号的值(用字符串表示)
// 原型 : TCString GetCurrentDataByFieldSeq(long nFieldSeq);
// 参数 : 字段序号
// 返回 : 字段值
// 说明 : 1. 该函数一般用于遍历所有数据
//        2. 得到的字符串已做右去空格处理。如果数字串需要左去空，则要在
//           函数外显式调用。
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
// 函数 : TCFoxDBF::GetDBData
// 用途 : 得到指定记录的指定字段名的值(用字符串表示)
// 原型 : TCString GetDBData(long nRecordSeq, TCString sFieldName);
// 参数 : 记录号，字段名
// 返回 : 字段值
// 说明 : 记录号从1开始计数
//==========================================================================
TCString TCFoxDBF::GetDBData(long nRecordSeq, TCString sFieldName)
{
    ASSERT(nRecordSeq <= RecCount());

    m_nCurrentRecordSeq = nRecordSeq - 1;

    ReadRecordToBuffer();

    return GetCurrentDataByFieldSeq(FieldSeq(sFieldName));
}

//==========================================================================
// 函数 : TCFoxDBF::GetDBFRecValue
// 用途 : 将DBF中的记录值赋给绑定变量
// 原型 : void GetDBFRecValue();
// 参数 : 无
// 返回 : 无
// 说明 : 如果当前记录非有效记录，不进行赋值操作。
//==========================================================================
void TCFoxDBF::GetDBFRecValue()
{
    //===== 1. 读取记录内容 ========
    ReadRecordToBuffer();
    if (m_bEOF || m_bBOF)
        return;

    //====== 2. 判断记录是否删除状态 ======
    if (m_pRecordBuffer[0] == 0x2A)
        m_bIsCurrentDelete = true;
    else
        m_bIsCurrentDelete = false;

    //======== 3. 循环读取记录值 =========
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
// 函数 : TCFoxDBF::GetModifyControlFileName
// 用途 : 得到修改控制文件
// 原型 : static TCString GetModifyControlFileName(TCString sFileName);
// 参数 : 操作文件名
// 返回 : 修改控制文件名
// 说明 : 该文件用以做修改完整性检查
//==========================================================================
TCString TCFoxDBF::GetModifyControlFileName(TCString sFileName)
{
    return sFileName + "_mod_ify_";
}

//==========================================================================
// 函数 : TCFoxDBF::GetRecCountOfDBFFile
// 用途 : 得到一个DBF文件的记录数
// 原型 : static long GetRecCountOfDBFFile(TCString sFileName);
// 参数 : DBF文件名
// 返回 : 该文件的记录数
// 说明 :
// 历史 : 2001.11.27 增加该函数
//==========================================================================
long TCFoxDBF::GetRecCountOfDBFFile(TCString sFileName)
{
    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(sFileName);
    return fdFoxDBF.RecCount();
}

//==========================================================================
// 函数 : TCFoxDBF::Go
// 用途 : 移到指定的记录
// 原型 : void Go(long nRecNum)
// 参数 : 记录条数
// 返回 : 无
// 说明 : 记录条数从1开始计数
//==========================================================================
void TCFoxDBF::Go(long nRecNum)
{   m_nCurrentRecordSeq = nRecNum - 1;
    GetDBFRecValue();
}

//==========================================================================
// 函数 : TCFoxDBF::Insert
// 用途 : 根据绑定的内容插入记录
// 原型 : void Insert();
// 参数 : 无
// 返回 : 无
// 说明 : 由于插入记录的文件操作过多（插入点之后的文件内容要全部后移），
//        故本函数只是多提供一种选择，一般不推荐使用。
//==========================================================================
void TCFoxDBF::Insert()
{
    if (m_nCurrentRecordSeq < 0 || m_nCurrentRecordSeq >= RecCount())
        throw TCException("TCFoxDBF::Insert() Error - \n"
                " CurrentRecord : " + IntToStr(m_nCurrentRecordSeq)
                + "\n TotalRecord : " + IntToStr(RecCount()));

    InsertPrepare();
    
    // 在现有记录处写入空数据
    m_fDBFFile.Seek(m_nRecordAddress + m_nCurrentRecordSeq * RecordLength());
    memset(m_pRecordBuffer, ' ', RecordLength());
    m_fDBFFile.Write(m_pRecordBuffer, RecordLength());

    Update();
    m_fhFoxHead.recnum ++;

    WriteFoxHeader();
}

//==========================================================================
// 函数 : TCFoxDBF::InsertPrepare
// 用途 : 将插入点后面的文件内容后移，为插入数据做准备
// 原型 : void InsertPrepare()
// 参数 : 无
// 返回 : 无
// 说明 :
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
        // 读取数据到Buffer2
        if (i != RecCount())
        {
            m_fDBFFile.Seek(m_nRecordAddress + i * RecordLength());
            m_fDBFFile.Read(RecordBuffer2, RecordLength());
        }

        // 将上一条记录的数据(存于Buffer1之中)写入此记录
        m_fDBFFile.Seek(m_nRecordAddress + i * RecordLength());
        m_fDBFFile.Write(RecordBuffer1, RecordLength());

        // 将Buffer2中的内容复制到Buffer1之中
        memcpy(RecordBuffer1, RecordBuffer2, RecordLength());
    }

    m_fDBFFile.Write("\x1A", 1);

    delete [] RecordBuffer1;
    delete [] RecordBuffer2;
}

//==========================================================================
// 函数 : TCFoxDBF::InsertRecordByStringList
// 用途 : 根据StringList中的数据插入一条数据库记录
// 原型 : void InsertRecordByStringList(TCStringList & slRecordValue);
// 参数 : 存储记录值的StringList
// 返回 : 无
// 说明 :
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
// 函数 : TCFoxDBF::Pack
// 用途 : 真正地物理删除一条记录
// 原型 : void Pack();
// 参数 : 无
// 返回 : 无
// 说明 : 在做删除记录Delete()时，只是做一下删除标记而并不真正删除该
//        记录。当执行本函数时即进行真正地物理删除。本操作能够节省空
//        间，但如考虑到速度的影响，不推荐频繁使用。
//==========================================================================
void TCFoxDBF::Pack()
{
    char *RecordBuffer;
    long nReadRow, nWriteRow;

    RecordBuffer = new char [RecordLength()];

    nWriteRow = 0;

    //====== 1. 循环读取每一条记录，如不是删除的记录，则写入 ======
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

    //======= 2. 截短文件，写入文件尾标记，更新文件头 ======
    m_fDBFFile.Truncate(m_nRecordAddress + nWriteRow * RecordLength());

    m_fDBFFile.Seek(m_nRecordAddress + nWriteRow * RecordLength());
    m_fDBFFile.Write("\x1A", 1);

    m_fhFoxHead.recnum = nWriteRow;
    WriteFoxHeader();

    delete [] RecordBuffer;
}

//==========================================================================
// 函数 : TCFoxDBF::PutDBFValue
// 用途 : 将绑定的变量内容组合形成一条记录的字符内容
// 原型 : void PutDBFValue()
// 参数 : 无
// 返回 : 无
// 说明 : 只记录绑定的变量。未绑定变量中缓冲区中的值未清空。所以，如果要事
//        先清空缓冲区，必须在调用该函数前显式调用memset以清空缓冲区。
//        C, D -- FBString, FBChar
//        M    -- 无
//        L    -- FBChar, FBString, FBInteger, FBLong
//        N    -- FBString, FBInteger, FBLong, FBFloat, FBDouble
// 历史 : 2001.9.30 将以前如果数字溢出，则填充'-'改为抛例外的情况
//        2001.10.2 PutDBFValue增加一个参数，以解决Locate时无需Locate那么多
//                  的问题
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
// 函数 : TCFoxDBF::ReadRecordIntoStringList
// 用途 : 读取一条记录的内容到StringList中
// 原型 : void ReadRecordIntoStringList(TCStringList & slRecordValue,
//              long nRecordSeq = -1);
// 参数 : 要填充值的StringList引用, 记录号
// 返回 : 无
// 说明 :
//==========================================================================
void TCFoxDBF::ReadRecordIntoStringList(TCStringList & slRecordValue,
        long nRecordSeq)
{
    //======= 1. 如果指定了记录号，则定位到该记录号，否则取当前记录值 ========
    if (nRecordSeq != -1)
    {
        ASSERT(nRecordSeq <= RecCount());
        m_nCurrentRecordSeq = nRecordSeq - 1;
        ReadRecordToBuffer();
    }

    //====== 2. 清除StringList中的内容 ========
    slRecordValue.Clear();

    //==== 3. 填充StringList的内容 =======
    long i;

    for (i = 0; i < FieldAmount(); i++)
        slRecordValue.Add(GetCurrentDataByFieldSeq(i));
}

//==========================================================================
// 函数 : TCFoxDBF::ReadRecordToBuffer
// 用途 : 读取一条记录的内容到缓冲区
// 原型 : void TCFoxDBF::ReadRecordToBuffer();
// 参数 : 无
// 返回 : 无
// 说明 : 记录的内容存储在m_pRecordBuffer中
//==========================================================================
void TCFoxDBF::ReadRecordToBuffer()
{
    //======== 1. 判断当前记录是否超出范围，如是则设定文件头／尾标志
    m_bBOF = false;
    m_bEOF = false;

    if (m_nCurrentRecordSeq >= RecCount())
        m_bEOF = true;

    if (m_nCurrentRecordSeq < 0)
        m_bBOF = true;

    if (m_bEOF || m_bBOF)
        return;

    //========== 2. 读取一条记录到记录缓冲区中 =========
    long nReaded;

    m_fDBFFile.Seek(m_nRecordAddress + m_nCurrentRecordSeq * RecordLength(),
            SEEK_SET);
    nReaded = m_fDBFFile.Read(m_pRecordBuffer, RecordLength());
    if (nReaded != RecordLength())
        throw TCException("RecordRead Error. DBF File May be Corruptted. - "
                + m_sDBFFileName);
}

//==========================================================================
// 函数 : TCFoxDBF::RegulateHeaderByteOrder
// 用途 : 调整DBF文件头结构的字节序
// 原型 : void RegulateHeaderByteOrder();
// 参数 : 无
// 返回 : 无
// 说明 : 由于DBF头的三项存储内容是按Intel序存储的记录数，记录长度，起始地
//        址等信息，所以在读入和写出头结构时要调用本函数进行字节序规整。
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
// 函数 : TCFoxDBF::Skip
// 用途 : 以当前记录为基准，前后移动记录
// 原型 : void Skip(long nRecordOffset = 1);
// 参数 : 偏移量，缺省为1
// 返回 : 无
// 说明 : 
//==========================================================================
void TCFoxDBF::Skip(long nRecordOffset)
{
    m_nCurrentRecordSeq += nRecordOffset;

    GetDBFRecValue();
}

//==========================================================================
// 函数 : TCFoxDBF::SortDBFFile
// 用途 : 排序一个DBF文件
// 原型 : static void SortDBFFile(TCString sDBFFileName, TCString sFieldNames);
// 参数 : sSortedFileName - 要排序的DBF文件名
//        sFieldNames     - 字段名，如果有多个字段，则以","分隔
// 返回 : 无
// 说明 : 将DBF文件排序，排序后的文件名就是源文件名
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
// 函数 : TCFoxDBF::SortToFile
// 用途 : 按字段排序
// 原型 : void SortToFile(TCString sSortedFileName, TCString sFieldNames)
// 参数 : sSortedFileName - 排序后的DBF文件名
//        sFieldNames     - 字段名，如果有多个字段，则以","分隔
// 返回 : 无
// 说明 :
//==========================================================================
void TCFoxDBF::SortToFile(TCString sSortedFileName, TCString sFieldNames)
{
    ASSERT(sSortedFileName != GetDBFFileName());
    
    //========= 1. 读出要排序的字段 ======
    TCStringList slFieldNameList;
    slFieldNameList.CommaText(sFieldNames);

    //======= 2. 求出各字段的FieldSeq =========
    const long FIELD_SEQ_MAX_COUNT = 16;
    ASSERT(slFieldNameList.GetCount() <= FIELD_SEQ_MAX_COUNT);
    long FieldSeqList[FIELD_SEQ_MAX_COUNT];
    long i;

    for (i = 0; i < slFieldNameList.GetCount(); i++)
        FieldSeqList[i] = FieldSeq(slFieldNameList[i]);

    //======== 3. 给待排序的StringList赋上各字段的值 =====
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

        //======= 4. 字符串后加上记录数 ======
        sWantSortString += Padl(IntToStr(nRecordNumber), 10);

        slWantSortList.Add(sWantSortString);
    }

    //======== 5. 将StringList进行排序 ======
#ifdef __TEST__
    printf("Sorting...\n");
#endif
    slWantSortList.Sort();

    //========= 6. 创建目标文件 ======
    CreateFoxDBFBySelf(sSortedFileName);

    TCStringList slRecordValue;
    TCFoxDBF fdFoxDest;

    fdFoxDest.AttachFile(sSortedFileName);

    //======= 7. 逐条读取记录，并写入目标文件 ======
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
// 函数 : TCFoxDBF::Update
// 用途 : 更新记录
// 原型 : void Update();
// 参数 : 无
// 返回 : 无
// 说明 :
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
// 函数 : TCFoxDBF::UpdateRecordByStringList
// 用途 : 根据StringList中的数据更新DBF中的记录
// 原型 : void UpdateRecordByStringList(TCStringList & slRecordValue);
// 参数 : 存储记录值的StringList
// 返回 : 无
// 说明 :
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
// 函数 : TCFoxDBF::WriteFoxHeader
// 用途 : 写入DBF的文件头
// 原型 : void WriteFoxHeader();
// 参数 : 无
// 返回 : 无
// 说明 : 由于字节序问题，写入文件头前后要对文件头中的相关内容调整字节
//        序。该函数在写入文件头时调用。
//==========================================================================
void TCFoxDBF::WriteFoxHeader()
{
    m_fDBFFile.Seek(0, SEEK_SET);

    RegulateHeaderByteOrder(m_fhFoxHead);
    m_fDBFFile.Write(&m_fhFoxHead, sizeof(TSFOXHEAD));
    RegulateHeaderByteOrder(m_fhFoxHead);
}

//==========================================================================
// 函数 : TCFoxDBF::ZAP
// 用途 : 清空DBF文件
// 原型 : void ZAP();
// 参数 : 无
// 返回 : 无
// 说明 : 
//==========================================================================
void TCFoxDBF::ZAP()
{
    m_fDBFFile.Truncate(m_nRecordAddress);

    m_nCurrentRecordSeq = 0;

    m_fhFoxHead.recnum = 0;

    WriteFoxHeader();
}

//==========================================================================
// 函数 :
// 用途 :
// 原型 : void ImportFromTextFile(TCString sTxtFileName, char cCommaChar = ',',
//                  bool bCheckFieldAmount = true);
// 参数 :
// 返回 :
// 说明 :
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
// 函数 : TCDBFCreate::TCDBFCreate
// 用途 : 构造函数
// 原型 : TCDBFCreate(TCString sDBFName, long nFieldCount);
// 参数 : DBF文件名，字段数
// 返回 : 无
// 说明 : 分配字段定义的空间以备之后的AddField函数进行写入。
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
// 函数 : TCDBFCreate::~TCDBFCreate
// 用途 : 析构函数
// 原型 : ~TCDBFCreate();
// 参数 : 无
// 返回 : 无
// 说明 : 释放在构造函数申请的字段定义部分的内存
//==========================================================================
TCDBFCreate::~TCDBFCreate()
{
    delete [] m_fdFieldsDef;
}

//==========================================================================
// 函数 : TCDBFCreate::AddField
// 用途 : 加入一个字段定义
// 原型 : void TCDBFCreate::AddField(TCString sFieldName, char cFieldType,
//          BYTE nFieldLen = 0, BYTE nFieldPoint = 0)
// 参数 : 字段名，字段类型，字段长度，小数点后的长度
// 返回 : 无
// 说明 : 支持四种字段类型 C D N L
//        如果不指定字段长度，则缺省情况下:
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
// 函数 : TCDBFCreate::CreateDBF
// 用途 : 根据填入的文件名及字段定义创建DBF文件
// 原型 : void CreateDBF();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCDBFCreate::CreateDBF()
{
    ASSERT(m_nFieldCurrentSeq == m_nFieldCount - 1);

    //====== 1. 填充FOX文件头 ========
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

    //======= 2. 打开文件，并写入文件头 ==========
    TCFileStream fDestDBF;

    //yxw add 打开文件时应加锁
    fDestDBF.Open(m_sDBFName, omWrite |omExclusive_Waiting);
    TCFoxDBF::RegulateHeaderByteOrder(fxhFoxHead);
    fDestDBF.Write(&fxhFoxHead, sizeof(TSFOXHEAD));      
    TCFoxDBF::RegulateHeaderByteOrder(fxhFoxHead);

    //======= 3. 准备字段定义部分 ============
    TSFOXFIELD *ffpFoxFields;
    long nStartOffset;
    TCString sFieldName;

    ffpFoxFields = new TSFOXFIELD[m_nFieldCount];
    memset(ffpFoxFields, '\0', sizeof(TSFOXFIELD)*m_nFieldCount);

    nStartOffset = 1;

    for (i = 0; i < m_nFieldCount; i++)
    {
        // 字段名
        FillPadr(ffpFoxFields[i].fldname, UpperCase(m_fdFieldsDef[i].fldname),
                10, '\0');

        // 字段类型
        ffpFoxFields[i].ftype = m_fdFieldsDef[i].ftype;
        // 字段长度
        ffpFoxFields[i].flen = m_fdFieldsDef[i].flen;
        // 小数点
        ffpFoxFields[i].fpoint = m_fdFieldsDef[i].fpoint;
        // 偏移量
        memcpy(ffpFoxFields[i].nouse, &nStartOffset, 4);     
        TCSystem::RegulateMachineOrder(ffpFoxFields[i].nouse,
                sizeof(ffpFoxFields[i].nouse), boIntel);

        nStartOffset += m_fdFieldsDef[i].flen;
    }

    // ========= 4. 写入字段定义 =============
    fDestDBF.Write(ffpFoxFields, sizeof(TSFOXFIELD)*m_nFieldCount);

    delete [] ffpFoxFields;

    // ========= 5. 写入尾标记，关闭文件 ========
    char c0D;
    c0D = 0x0D;
    fDestDBF.Write(&c0D, 1);
    fDestDBF.Flush();
    fDestDBF.Close();

    //========= 6. 如果有修改完整校验文件，删除之 ======
    TCString sModifyControlFileName;
    sModifyControlFileName = TCFoxDBF::GetModifyControlFileName(m_sDBFName);

    if (FileExists(sModifyControlFileName))
        DeleteFileE(sModifyControlFileName);
}

//==========================================================================
// 函数 : TCDBFStructDefine::TCDBFStructDefine
// 用途 : DBF结构类构造函数
// 原型 : TCDBFStructDefine
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
TCDBFStructDefine::TCDBFStructDefine()
{
    m_nFieldCount = 0;
}

//==========================================================================
// 函数 : TCDBFStructDefine::AddField
// 用途 : 加入一个字段定义
// 原型 : void TCDBFCreate::AddField(TCString sFieldName, char cFieldType,
//          BYTE nFieldLen = 0, BYTE nFieldPoint = 0)
// 参数 : 字段名，字段类型，字段长度，小数点后的长度
// 返回 : 无
// 说明 :
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
// 函数 : TCDBFStructDefine::Clear
// 用途 : 清除字段定义
// 原型 : void TCDBFCreate::Clear
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCDBFStructDefine::Clear()
{
    m_nFieldCount = 0;
}

//==========================================================================
// 函数 : TCDBFStructDefine::CreateDBF
// 用途 : 根据定义的结构创建DBF文件
// 原型 : void CreateDBF(TCString sFileName);
// 参数 : DBF文件名
// 返回 : 无
// 说明 :
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
// 函数 : TCDBFStructDefine::FieldCount
// 用途 : 得到字段总数
// 原型 : long FieldCount();
// 参数 : 无
// 返回 : 字段总数
// 说明 :
//==========================================================================
long TCDBFStructDefine::FieldCount()
{
    return m_nFieldCount;
}

//==========================================================================
// 函数 : TCDBFStructDefine::FieldName
// 用途 : 根据字段序号得到字段名
// 原型 : long FieldName(long nFieldSeq);
// 参数 : 字段序号
// 返回 : 字段名
// 说明 : 字段序号从0开始
//==========================================================================
TCString TCDBFStructDefine::FieldName(long nFieldSeq)
{
    ASSERT(nFieldSeq < m_nFieldCount);

    return UpperCase(m_fdFieldsDef[nFieldSeq].fldname);
}

//==========================================================================
// 函数 : TCDBFStructDefine::FieldSeq
// 用途 : 根据字段名得到字段序号
// 原型 : long FieldSeq(TCString sFieldName);
// 参数 : 字段名
// 返回 : 字段序号
// 说明 : 字段序号从0开始
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
// 函数 : TCDBFStructDefine::InsertField
// 用途 : 插入一个字段定义
// 原型 : void InsertField(long nFieldSeq, TCString sFieldName,
//              char cFieldType, BYTE nFieldLen = 0, BYTE nFieldPoint = 0)
// 参数 : 字段序，字段名，字段类型，字段长度，小数点后的长度
// 返回 : 无
// 说明 :
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
// 函数 : TCDBFStructDefine::SetFieldDefine
// 用途 : 设置一个字段定义
// 原型 : void SetFieldDefine(long nFieldSeq, TCString sFieldName,
//              char cFieldType, BYTE nFieldLen, BYTE nFieldPoint);
// 参数 : 字段名，字段类型，字段长度，小数点后的长度
// 返回 : 无
// 说明 : 支持四种字段类型 C D N L
//        如果不指定字段长度，则缺省情况下:
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

