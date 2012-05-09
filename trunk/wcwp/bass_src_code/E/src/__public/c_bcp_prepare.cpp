//---------------------------------------------------------------------------

#include "cmpublic.h"

#pragma hdrstop

#include "c_bcp_prepare.h"
#include "c_mzcompress.h"
//---------------------------------------------------------------------------

//==========================================================================
// ���� : TCBCPPrepare::AddToXBcpList
// ��; : �����LIST����һ������
// ԭ�� : static void AddToXBcpList(TCString sFileName, TCString sTableName,
//                  bool bSameFieldSeq = true, bool bSameFieldName = true);
// ���� : sFileName      - �ļ���(�����Ǿ���·����Ҳ���������·��)������
//                         ԭ���������LIST��
//        sTableName     - ����
//        bSameFieldSeq  - �Ƿ�����ͬ���ֶ�˳��
//        bSameFieldName - �Ƿ�ӵ����ͬ���ֶ�����
// ���� : ��
// ˵�� :
//==========================================================================
void TCBCPPrepare::AddToXBcpList(TCString sFileName, TCString sTableName,
        bool bSameFieldSeq, bool bSameFieldName)
{
    //===== 1. ����ļ������ڣ��򴴽��ļ� =======
    TCString sBCPDir, sBCPListFileName;
    sBCPDir = ExtractFilePath(sFileName);
    if( sBCPDir == "" )
        sBCPDir = GetBCPDir();
    else
        sFileName = ExtractFileName( sFileName );
    sBCPListFileName = MergePathAndFile(sBCPDir, "XBCPLIST");

    if (!FileExists(sBCPListFileName))
    {
        TCDBFCreate dcCreate(sBCPListFileName, 9);
        dcCreate.AddField("file_name",  'C', 128);
        dcCreate.AddField("format",     'C', 4);
        dcCreate.AddField("dbname",     'C', 8);
        dcCreate.AddField("table_name", 'C', 30);
        dcCreate.AddField("cmd_hint",   'C', 64);
        dcCreate.AddField("cmd_time",   'C', 14);
        dcCreate.AddField("delete",     'C', 1);
        dcCreate.AddField("same_fseq",  'C', 1);
        dcCreate.AddField("same_fname", 'C', 1);
        dcCreate.CreateDBF();
    }

    //======= 2. ����һ����¼ =======
    TCString sFormat, sDBName, sCmdHint, sCmdTime;
    char cDelete, cSameFSeq, cSameFName;

    sFormat = "DBF";
    sDBName = "MI";
    sCmdHint = "";
    sCmdTime = TCTime::Now();
    cDelete  = 'Y';

    if (bSameFieldSeq)
        cSameFSeq = 'Y';
    else
        cSameFSeq = 'N';

    if (bSameFieldName)
        cSameFName = 'Y';
    else
        cSameFName = 'N';

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(sBCPListFileName);

    fdFoxDBF.DBBind("file_name",  sFileName);
    fdFoxDBF.DBBind("format",     sFormat);
    fdFoxDBF.DBBind("dbname",     sDBName);
    fdFoxDBF.DBBind("table_name", sTableName);
    fdFoxDBF.DBBind("cmd_hint",   sCmdHint);
    fdFoxDBF.DBBind("cmd_time",   sCmdTime);
    fdFoxDBF.DBBind("delete",     cDelete);
    fdFoxDBF.DBBind("same_fseq",  cSameFSeq);
    fdFoxDBF.DBBind("same_fname", cSameFName);

    fdFoxDBF.Append();
}

//==========================================================================
// ���� : TCBCPPrepare::GetArchiveDir
// ��; : �õ�����ļ����ݸ�Ŀ¼
// ԭ�� : static TCString GetArchiveDir();
// ���� : ��
// ���� : ���ݸ�Ŀ¼
// ˵�� :
//==========================================================================
TCString TCBCPPrepare::GetArchiveDir()
{
    static TCString s_sArchiveDir;

    if (s_sArchiveDir == "")
    {
        s_sArchiveDir = ProfileAppString("bcp_prepare", "archive_rst",
                "directory", "");
        ASSERT(s_sArchiveDir != "");

        ForceDirectories(s_sArchiveDir);
    }

    return s_sArchiveDir;
}

//==========================================================================
// ���� : TCBCPPrepare::GetBCPDir
// ��; : �õ�����ļ����Ŀ¼
// ԭ�� : static TCString GetBCPDir();
// ���� : ��
// ���� : ����ļ����Ŀ¼
// ˵�� :
//==========================================================================
TCString TCBCPPrepare::GetBCPDir()
{
    static TCString s_sBCPDir;

    if (s_sBCPDir == "")
    {
        s_sBCPDir = ProfileAppString("bcp_prepare", "bcp", "directory", "");
        ASSERT(s_sBCPDir != "");

        ForceDirectories(s_sBCPDir);
    }

    return s_sBCPDir;
}

long TCBCPPrepare::GetBcpSyncDelay()
{
    static long s_nBcpSyncDelay = -1;

    if (s_nBcpSyncDelay == -1)
    {
        s_nBcpSyncDelay = ProfileAppInt("bcp_prepare", "general",
                "bcp_sync_delay", 9999);
        if (s_nBcpSyncDelay == 9999)
            throw TCException("BcpPrepare.ini BcpSyncDelay Not Set");

        if (s_nBcpSyncDelay < 15 || s_nBcpSyncDelay > 60 * 24)
            throw TCException("BcpPrepare.ini BcpSyncDelay Invalid Value");
    }

    return s_nBcpSyncDelay;
}

//==========================================================================
// ���� : TCBCPPrepare::GetWorkingDir
// ��; : �õ�����Ŀ¼
// ԭ�� : static TCString GetWorkingDir();
// ���� : ��
// ���� : ����Ŀ¼
// ˵�� :
//==========================================================================
TCString TCBCPPrepare::GetWorkingDir()
{
    static TCString s_sWorkingDir;

    if (s_sWorkingDir == "")
    {
        s_sWorkingDir = ProfileAppString("bcp_prepare", "working_dir",
                "directory", "");
        ASSERT(s_sWorkingDir != "");

        ForceDirectories(s_sWorkingDir);
    }

    return s_sWorkingDir;
}

//==========================================================================
// ���� : TCBCPPrepare::SubmitWorkingFileToBcp
// ��; : �ύһ�������ļ����
// ԭ�� : void SubmitWorkingFileToBcp(TCString sFileName, TCString sTableName,
//              TCString sDealDate, bool bSameFieldSeq = true,
//              bool bSameFieldName = true);
// ���� : �ļ���(������·��)���������������ڣ��Ƿ���ͬ�ֶ�˳���Ƿ���ͬ�ֶ���
// ���� : ��
// ˵�� : 1. �������������ύ�ļ��ı���
//        2. Ŀ���ļ���Ϊ : �ļ���_YYYYMMDD(��������).ԭ�ļ���չ��
//        3. �����ļ���Ϊ : <���ݸ�Ŀ¼>/<����>/<����>_YYYYMMDD.dbf
//==========================================================================
void TCBCPPrepare::SubmitWorkingFileToBcp(TCString sFileName,
        TCString sTableName, TCString sDealDate, bool bSameFieldSeq,
        bool bSameFieldName)
{
    ASSERT(TCTime::IsValidDatetime(sDealDate));

    //===== 1. �õ�Դ�ļ��� ======
    TCString sSourceFileName;

    sSourceFileName = MergePathAndFile(GetWorkingDir(), sFileName);
    if (!FileExists(sSourceFileName))
        return;

    //==== 2. �õ�Ŀ���ļ��� ======
    TCString sDestFileName;

    sDestFileName = ExtractFileName(sSourceFileName);
    sDestFileName = CutFileExt(sDestFileName) + "_" + sDealDate
            + ExtractFileExt(sDestFileName);
    sDestFileName = MergePathAndFile(GetBCPDir(), sDestFileName);

    //====== 3. �õ������ļ��� ========
    TCString sArchiveDirectory, sArchiveFileName;
    sArchiveDirectory = MergePathAndFile(GetArchiveDir(), sTableName);

    if (!ForceDirectories(sArchiveDirectory))
        throw TCException("TCBCPPrepare::SubmitWorkingFileToBcp() Error : "
                "Make Dir - " + sArchiveDirectory);

    sArchiveFileName = MergePathAndFile(sArchiveDirectory,
            ExtractFileName(sDestFileName));

    if (FileExists(sArchiveFileName + ".MZ"))
        throw TCException("TCBCPPrepare::SubmitWorkingFileToBcp() Error : "
                "File " + sArchiveFileName + ".MZ already in xarchive");

    //===== 4. �Ƶ�Ŀ��Ŀ¼ ======
    TCString sIDXFileName;

    sIDXFileName = ChangeFileExt(sSourceFileName, ".idx");
    if (FileExists(sIDXFileName))
        if (!DeleteFile(sIDXFileName))
            throw TCException("TCBCPPrepare::SubmitWorkingFileToBcp() : "
                    "Delete File : " + sIDXFileName);

    if (!RenameFile(sSourceFileName, sDestFileName, true))
        throw TCException("TCBCPPrepare::SubmitWorkingFileToBcp() : "
                    "Rename File : " + sSourceFileName + " * "
                    + sDestFileName);

    //====== 5. �����ļ� ========
    if (!CopyFile(sDestFileName, sArchiveFileName))
            throw TCException("TCBCPPrepare::SubmitWorkingFileToBcp() Error : "
                    "CopyFile Error - " + sDestFileName + " to "
                    + sArchiveFileName);
    TCMZCompress::CompressMZFile(sArchiveFileName);

    //======== 6. �ύ��� ======
    AddToXBcpList(ExtractFileName(sDestFileName), sTableName,
            bSameFieldSeq, bSameFieldName);
}

void TCBCPPrepare::SyncWorkingFileToBcp(TCString sFileName,
        TCString sTableName, bool bSameFieldSeq, bool bSameFieldName)
{
    static TCStringList slTableTimeSection;

    long nCurrentTimeSection;

    nCurrentTimeSection = TCTime::GetTimeSection(TCTime::Now(),
            GetBcpSyncDelay(), 6);

    TCString sLastSection;
    sLastSection = slTableTimeSection.GetValue(sTableName);

    if (sLastSection == "")
    {
        slTableTimeSection.SetValue(sTableName, IntToStr(nCurrentTimeSection));
        return;
    }

    if (StrToInt(sLastSection) == nCurrentTimeSection)
        return;

    slTableTimeSection.SetValue(sTableName, IntToStr(nCurrentTimeSection));

    SubmitWorkingFileToBcp(sFileName, sTableName, TCTime::Now(),
            bSameFieldSeq, bSameFieldName);
}


