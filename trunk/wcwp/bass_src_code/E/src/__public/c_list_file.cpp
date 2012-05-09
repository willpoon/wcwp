//---------------------------------------------------------------------------
#pragma hdrstop

#include "cmpublic.h"
#include "c_list_file.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)

const long RECORD_LENGTH = 36;

long GetFirstRecordNumber(TCFoxDBF &fdFoxDBF)
{
    long nRecCount;

    nRecCount = fdFoxDBF.RecCount();
    if (nRecCount == 0)
        return 0;

    fdFoxDBF.Go(nRecCount);
    if (fdFoxDBF.Deleted())
        return 0;

    fdFoxDBF.Go(1);
    if (!fdFoxDBF.Deleted())
        return 1;

    long nMin, nMax, nMid;
    nMin = 1;
    nMax = nRecCount;

    while (true)
    {
        if (nMax - nMin <= 1)
            return nMax;

        nMid = (nMax + nMin) / 2;

        fdFoxDBF.Go(nMid);
        if (fdFoxDBF.Deleted())
            nMin = nMid;
        else
            nMax = nMid;
    }
}

void TCListFile::CreateNewListFile()
{
    TCDBFCreate dcCreate(m_sFileName, 1);
    dcCreate.AddField("FILE_NAME", 'C', RECORD_LENGTH);
    dcCreate.CreateDBF();
}

//==========================================================================
// 函数 : SetFileName
// 用途 : 设置List文件名
// 原型 : void TCListFile::SetFileName(const TCString sFileName)
// 参数 : List文件名
// 返回 : 无
// 说明 :
//==========================================================================
void TCListFile::SetFileName(const TCString sFileName)
{
  m_sFileName = sFileName;
}

//==========================================================================
// 函数 : AppendRecord
// 用途 : 增加一条记录
// 原型 : void TCListFile::AppendRecord(const TCString Record)
// 参数 : 加入的记录
// 返回 : 无
// 说明 :
//==========================================================================
void TCListFile::AppendRecord(const TCString sRecord)
{
    ASSERT(sRecord != TCString("") ) ;

    if (sRecord == GetLastRecord())
        return;

    if (!FileExists(m_sFileName))
        CreateNewListFile();

    TCString sAppendFileName;
    sAppendFileName = sRecord;

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(m_sFileName);
    fdFoxDBF.DBBind("file_name", sAppendFileName);
    fdFoxDBF.Append();
    fdFoxDBF.CloseDBF();
}

void TCListFile::AppendRecord(TCStringList &  sRecordList)
{
    if (!FileExists(m_sFileName))
        CreateNewListFile();

    TCString sAppendFileName;
    long i;

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(m_sFileName);
    fdFoxDBF.DBBind("file_name", sAppendFileName);

    for (i = 0; i < sRecordList.GetCount(); i ++)
    {
        sAppendFileName = sRecordList[i];
        fdFoxDBF.Append();
    }

    fdFoxDBF.CloseDBF();
}

//==========================================================================
// 函数 : GetFirstRecord
// 用途 : 取出第一条记录
// 原型 : TCString TCListFile::GetFirstRecord()
// 参数 : 无
// 返回 : 第一条记录
// 说明 :
//==========================================================================
TCString TCListFile::GetFirstRecord()
{
    TCString sRecord;
    TCFoxDBF fdFoxDBF;
    long nRecordNumber;

    if (!FileExists(m_sFileName))
        return "";

    fdFoxDBF.AttachFile(m_sFileName);
    nRecordNumber = GetFirstRecordNumber(fdFoxDBF);

    if (nRecordNumber == 0)
    {   fdFoxDBF.CloseDBF();
        return "";
    }

    fdFoxDBF.DBBind("file_name", sRecord);
    fdFoxDBF.Go(nRecordNumber);

    fdFoxDBF.CloseDBF();

    //==== 加入一些准确性验证 ========
    //========== 1. 如果文件名中有空格，则抛出例外 =======
    if (Pos(sRecord, " ") != 0)
      throw TCException("LIST FILE FORMAT ERROR. BLANK IN FILE NAME");

    //======== 2. 如果取回空串，则截之 =======
    if (sRecord == TCString("") )
          CutFirstRecord();

    return sRecord;
}

//==========================================================================
// 函数 : GetLastRecord
// 用途 : 取出最后一条记录
// 原型 : TCString TCListFile::GetLastRecord()
// 参数 : 无
// 返回 : 最后一条记录
// 说明 :
//==========================================================================
TCString TCListFile::GetLastRecord()
{
    TCString sRecord;
    TCFoxDBF fdFoxDBF;

    if (!FileExists(m_sFileName))
        return "";

    fdFoxDBF.AttachFile(m_sFileName);
    if (fdFoxDBF.RecCount() == 0)
    {   fdFoxDBF.CloseDBF();
        return "";
    }

    fdFoxDBF.DBBind("file_name", sRecord);
    fdFoxDBF.GoBottom();

    fdFoxDBF.CloseDBF();

    return sRecord;
}

//==========================================================================
// 函数 : CutFirstRecord
// 用途 : 删除第一条记录
// 原型 : void TCListFile::CutFirstRecord()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCListFile::CutFirstRecord()
{
    TCFoxDBF fdFoxDBF;
    long nRecordNumber;

    fdFoxDBF.AttachFile(m_sFileName);
    nRecordNumber = GetFirstRecordNumber(fdFoxDBF);

    if (nRecordNumber == 0)
        throw TCException("TCListFile::CutFirstRecord() Error"
                "  The List File Already Empty");

    if (fdFoxDBF.RecCount() >= 256 && nRecordNumber > fdFoxDBF.RecCount() / 3)
    {
        fdFoxDBF.Pack();

        fdFoxDBF.Go(1);
    }
    else
        fdFoxDBF.Go(nRecordNumber);
        
    fdFoxDBF.Delete();

    fdFoxDBF.CloseDBF();
}

//==========================================================================
// 函数 : GetRecordCount
// 用途 : 获取List文件中的记录数
// 原型 : long TCListFile::GetRecordCount()
// 参数 : 无
// 返回 : 记录数
// 说明 :
//==========================================================================
long TCListFile::GetRecordCount()
{
    TCFoxDBF fdFoxDBF;
    long nRecordNumber;

    if (!FileExists(m_sFileName))
        return 0;

    fdFoxDBF.AttachFile(m_sFileName);
    nRecordNumber = GetFirstRecordNumber(fdFoxDBF);

    if (nRecordNumber == 0)
        return 0;

    return fdFoxDBF.RecCount() - nRecordNumber + 1;
}

//==========================================================================
// 函数 : TCListFile::FillStringList
// 用途 : 取出LIST文件中的所有有效记录填入StringList中
// 原型 : void FillStringList(TCStringList & slFileNameList);
// 参数 : 文件名StringList的引用
// 返回 : 无
// 说明 : 取出的文件名不含路径
//==========================================================================
void TCListFile::FillStringList(TCStringList & slFileNameList)
{
    //===== 1. 清空目标StringList ======
    slFileNameList.Clear();

    TCString sRecord;
    TCFoxDBF fdFoxDBF;
    long nRecordNumber;

    //=== 2. 文件不存在则直接返回 ======
    if (!FileExists(m_sFileName))
        return;

    //===== 3. 打开文件 ========
    fdFoxDBF.AttachFile(m_sFileName);

    //==== 4. 如果没有有效记录，则返回 ======
    nRecordNumber = GetFirstRecordNumber(fdFoxDBF);

    if (nRecordNumber == 0)
    {   fdFoxDBF.CloseDBF();
        return;
    }

    fdFoxDBF.DBBind("file_name", sRecord);

    //===== 5. 循环读取所有有效记录, 并加入StringList中 ======
    long i;

    for (i = nRecordNumber; i <= fdFoxDBF.RecCount(); i ++)
    {
        fdFoxDBF.Go(i);
        slFileNameList.Add(sRecord);
    }

    fdFoxDBF.CloseDBF();
}

//==========================================================================
// 函数 : TCListFile::AppendFileToList
// 用途 : 将一个文件加入到LIST文件之中
// 原型 : static void AppendFileToList(TCString sFullPathFileName);
// 参数 : 文件全名
// 返回 : 无
// 说明 : 加入的LIST文件与文件参数在同一个路径上，取名"LIST"
//==========================================================================
void TCListFile::AppendFileToList(TCString sFullPathFileName,TCString sFileListName)
{
    TCListFile ListFile;
    TCString sListFileName;

    //sListFileName = ExtractFilePath(sFullPathFileName) + "LIST";
    
    sListFileName = ExtractFilePath(sFullPathFileName) + sFileListName;

    ListFile.SetFileName(sListFileName);

    ListFile.AppendRecord(ExtractFileName(sFullPathFileName));
}

//==========================================================================
// 函数 : TCListFile::FetchFileThroughList
// 用途 : 得到一个目录中的LIST文件的第一条记录
// 原型 : static TCString FetchFileThroughList(TCString sDirName,TCString sFileListName="LIST");
// 参数 : 目录名
// 返回 : 文件全名
// 说明 : 取得的文件在目录之中。LIST文件取名"LIST"
//==========================================================================
TCString TCListFile::FetchFileThroughList(TCString sDirName,TCString sFileListName)
{
    TCListFile ListFile;
    TCString sListFileName;

    TCString sFileName;

    //sListFileName = MergePathAndFile(sDirName, "LIST");
    sListFileName = MergePathAndFile(sDirName, sFileListName);

    ListFile.SetFileName(sListFileName);

    sFileName = ListFile.GetFirstRecord();

    if (sFileName == TCString("") )
        return "";
    else
        return MergePathAndFile(sDirName, sFileName);
}

//==========================================================================
// 函数 : TCListFile::CutFirstFileOfList
// 用途 : 将指定文件从LIST文件中删除
// 原型 : static void TCListFile::CutFileFromList(TCString sFileName,TCString sFileListName="LIST");
// 参数 : 文件名
// 返回 : 无
// 说明 : 指定文件必须是LIST文件中的第一个文件，否则抛出例外
//==========================================================================
void TCListFile::CutFileFromList(TCString sFileName,TCString sFileListName)
{
    TCString sDirName;
    sDirName = ExtractFileDir(sFileName);

    TCListFile ListFile;
    TCString sListFileName;

    //sListFileName = MergePathAndFile(sDirName, "LIST");
    
    sListFileName = MergePathAndFile(sDirName, sFileListName);

    ListFile.SetFileName(sListFileName);

    TCString sFirstFileName;
    sFirstFileName = ListFile.GetFirstRecord();

    if (sFirstFileName != ExtractFileName(sFileName))
        throw TCException("TCListFile::CutFileFromList Error : "
                "File is not the first file of LIST - " + sFileName);

    ListFile.CutFirstRecord();
}

//==========================================================================
// 函数 : TCListFile::CutFirstFileOfList
// 用途 : 删除一个目录中的LIST文件的第一条记录
// 原型 : static void CutFirstFileOfList(TCString sDirName,TCString sFileListName);
// 参数 : 目录名
// 返回 : 无
// 说明 : LIST文件取名"LIST"
//==========================================================================
void TCListFile::CutFirstFileOfList(TCString sDirName,TCString sFileListName)
{
    TCListFile ListFile;
    TCString sListFileName;

    //sListFileName = MergePathAndFile(sDirName, "LIST");
    
    sListFileName = MergePathAndFile(sDirName, sFileListName);

    ListFile.SetFileName(sListFileName);

    ListFile.CutFirstRecord();
}

//==========================================================================
// 函数 : TCListFile::CutFirstFileOfList
// 用途 : 取出一个目录中的LIST文件中的所有有效记录填入StringList中
// 原型 : static void FillStringListThroughList(TCStringList & slFileNameList,
//              TCString sDirName);
// 参数 : 文件名StringList, 目录名
// 返回 : 无
// 说明 : 取出的文件名不含路径
//==========================================================================
void TCListFile::FillStringListThroughList(TCStringList & slFileNameList,
            TCString sDirName)
{
    TCListFile ListFile;
    TCString sListFileName;

    sListFileName = MergePathAndFile(sDirName, "LIST");

    ListFile.SetFileName(sListFileName);

    ListFile.FillStringList(slFileNameList);
}

#ifdef __TEST__

void DisplayTestListPrompt()
{
    printf("\n\n==== Test LIST ====\n\n");
    printf("0. Append List\n");
    printf("1. Get And Cut\n");
    printf("Q. Quit\n\n");
}

const TCString TEST_LIST_FILE = "C:\\TEMP\\LIST.DBF";

void TestList0AppendList()
{
    long i;
    TCListFile listfile;
    listfile.SetFileName(TEST_LIST_FILE);

    for (i = 1; i <= 1000; i++)
    {
        printf("%d\n", i);
        listfile.AppendRecord("FILE_" + IntToStr(i));
    }
    printf("==== rec left : %d\n", listfile.GetRecordCount());
}

void TestList1GetAndCut()
{
    long nGetCount;
    TCString sFileName;
    TCListFile listfile;
    listfile.SetFileName(TEST_LIST_FILE);

    nGetCount = 0;
    while (true)
    {
        sFileName = listfile.GetFirstRecord();
        if (sFileName == "")
            break;

        listfile.CutFirstRecord();

        nGetCount ++;
        printf("%d : %s\n", nGetCount, (char *)sFileName);
        if (nGetCount >= 400)
            break;
    }
    printf("==== rec left : %d\n", listfile.GetRecordCount());
}

void TestListMainFunc()
{
    int cChar;

    DisplayTestListPrompt();
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
                TestList0AppendList();
                break;

            case '1':
                TestList1GetAndCut();
                break;

            default:
                continue;
        }
        DisplayTestListPrompt();
    }
}

#endif


