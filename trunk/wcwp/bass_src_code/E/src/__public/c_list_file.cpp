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
// ���� : SetFileName
// ��; : ����List�ļ���
// ԭ�� : void TCListFile::SetFileName(const TCString sFileName)
// ���� : List�ļ���
// ���� : ��
// ˵�� :
//==========================================================================
void TCListFile::SetFileName(const TCString sFileName)
{
  m_sFileName = sFileName;
}

//==========================================================================
// ���� : AppendRecord
// ��; : ����һ����¼
// ԭ�� : void TCListFile::AppendRecord(const TCString Record)
// ���� : ����ļ�¼
// ���� : ��
// ˵�� :
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
// ���� : GetFirstRecord
// ��; : ȡ����һ����¼
// ԭ�� : TCString TCListFile::GetFirstRecord()
// ���� : ��
// ���� : ��һ����¼
// ˵�� :
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

    //==== ����һЩ׼ȷ����֤ ========
    //========== 1. ����ļ������пո����׳����� =======
    if (Pos(sRecord, " ") != 0)
      throw TCException("LIST FILE FORMAT ERROR. BLANK IN FILE NAME");

    //======== 2. ���ȡ�ؿմ������֮ =======
    if (sRecord == TCString("") )
          CutFirstRecord();

    return sRecord;
}

//==========================================================================
// ���� : GetLastRecord
// ��; : ȡ�����һ����¼
// ԭ�� : TCString TCListFile::GetLastRecord()
// ���� : ��
// ���� : ���һ����¼
// ˵�� :
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
// ���� : CutFirstRecord
// ��; : ɾ����һ����¼
// ԭ�� : void TCListFile::CutFirstRecord()
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : GetRecordCount
// ��; : ��ȡList�ļ��еļ�¼��
// ԭ�� : long TCListFile::GetRecordCount()
// ���� : ��
// ���� : ��¼��
// ˵�� :
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
// ���� : TCListFile::FillStringList
// ��; : ȡ��LIST�ļ��е�������Ч��¼����StringList��
// ԭ�� : void FillStringList(TCStringList & slFileNameList);
// ���� : �ļ���StringList������
// ���� : ��
// ˵�� : ȡ�����ļ�������·��
//==========================================================================
void TCListFile::FillStringList(TCStringList & slFileNameList)
{
    //===== 1. ���Ŀ��StringList ======
    slFileNameList.Clear();

    TCString sRecord;
    TCFoxDBF fdFoxDBF;
    long nRecordNumber;

    //=== 2. �ļ���������ֱ�ӷ��� ======
    if (!FileExists(m_sFileName))
        return;

    //===== 3. ���ļ� ========
    fdFoxDBF.AttachFile(m_sFileName);

    //==== 4. ���û����Ч��¼���򷵻� ======
    nRecordNumber = GetFirstRecordNumber(fdFoxDBF);

    if (nRecordNumber == 0)
    {   fdFoxDBF.CloseDBF();
        return;
    }

    fdFoxDBF.DBBind("file_name", sRecord);

    //===== 5. ѭ����ȡ������Ч��¼, ������StringList�� ======
    long i;

    for (i = nRecordNumber; i <= fdFoxDBF.RecCount(); i ++)
    {
        fdFoxDBF.Go(i);
        slFileNameList.Add(sRecord);
    }

    fdFoxDBF.CloseDBF();
}

//==========================================================================
// ���� : TCListFile::AppendFileToList
// ��; : ��һ���ļ����뵽LIST�ļ�֮��
// ԭ�� : static void AppendFileToList(TCString sFullPathFileName);
// ���� : �ļ�ȫ��
// ���� : ��
// ˵�� : �����LIST�ļ����ļ�������ͬһ��·���ϣ�ȡ��"LIST"
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
// ���� : TCListFile::FetchFileThroughList
// ��; : �õ�һ��Ŀ¼�е�LIST�ļ��ĵ�һ����¼
// ԭ�� : static TCString FetchFileThroughList(TCString sDirName,TCString sFileListName="LIST");
// ���� : Ŀ¼��
// ���� : �ļ�ȫ��
// ˵�� : ȡ�õ��ļ���Ŀ¼֮�С�LIST�ļ�ȡ��"LIST"
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
// ���� : TCListFile::CutFirstFileOfList
// ��; : ��ָ���ļ���LIST�ļ���ɾ��
// ԭ�� : static void TCListFile::CutFileFromList(TCString sFileName,TCString sFileListName="LIST");
// ���� : �ļ���
// ���� : ��
// ˵�� : ָ���ļ�������LIST�ļ��еĵ�һ���ļ��������׳�����
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
// ���� : TCListFile::CutFirstFileOfList
// ��; : ɾ��һ��Ŀ¼�е�LIST�ļ��ĵ�һ����¼
// ԭ�� : static void CutFirstFileOfList(TCString sDirName,TCString sFileListName);
// ���� : Ŀ¼��
// ���� : ��
// ˵�� : LIST�ļ�ȡ��"LIST"
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
// ���� : TCListFile::CutFirstFileOfList
// ��; : ȡ��һ��Ŀ¼�е�LIST�ļ��е�������Ч��¼����StringList��
// ԭ�� : static void FillStringListThroughList(TCStringList & slFileNameList,
//              TCString sDirName);
// ���� : �ļ���StringList, Ŀ¼��
// ���� : ��
// ˵�� : ȡ�����ļ�������·��
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


