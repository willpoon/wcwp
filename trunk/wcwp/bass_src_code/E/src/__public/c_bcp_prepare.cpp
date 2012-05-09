//---------------------------------------------------------------------------

#include "cmpublic.h"

#pragma hdrstop

#include "c_bcp_prepare.h"
#include "c_mzcompress.h"
//---------------------------------------------------------------------------

//==========================================================================
// 函数 : TCBCPPrepare::AddToXBcpList
// 用途 : 在入库LIST加入一项请求
// 原型 : static void AddToXBcpList(TCString sFileName, TCString sTableName,
//                  bool bSameFieldSeq = true, bool bSameFieldName = true);
// 参数 : sFileName      - 文件名(可以是绝对路径，也可以是相对路径)，将按
//                         原样加入入库LIST中
//        sTableName     - 表名
//        bSameFieldSeq  - 是否是相同的字段顺序
//        bSameFieldName - 是否拥有相同的字段名称
// 返回 : 无
// 说明 :
//==========================================================================
void TCBCPPrepare::AddToXBcpList(TCString sFileName, TCString sTableName,
        bool bSameFieldSeq, bool bSameFieldName)
{
    //===== 1. 如果文件不存在，则创建文件 =======
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

    //======= 2. 增加一条记录 =======
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
// 函数 : TCBCPPrepare::GetArchiveDir
// 用途 : 得到入库文件备份根目录
// 原型 : static TCString GetArchiveDir();
// 参数 : 无
// 返回 : 备份根目录
// 说明 :
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
// 函数 : TCBCPPrepare::GetBCPDir
// 用途 : 得到入库文件存放目录
// 原型 : static TCString GetBCPDir();
// 参数 : 无
// 返回 : 入库文件存放目录
// 说明 :
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
// 函数 : TCBCPPrepare::GetWorkingDir
// 用途 : 得到工作目录
// 原型 : static TCString GetWorkingDir();
// 参数 : 无
// 返回 : 工作目录
// 说明 :
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
// 函数 : TCBCPPrepare::SubmitWorkingFileToBcp
// 用途 : 提交一个工作文件入库
// 原型 : void SubmitWorkingFileToBcp(TCString sFileName, TCString sTableName,
//              TCString sDealDate, bool bSameFieldSeq = true,
//              bool bSameFieldName = true);
// 参数 : 文件名(不包括路径)，表名，处理日期，是否相同字段顺序，是否相同字段名
// 返回 : 无
// 说明 : 1. 本函数还包括提交文件的备份
//        2. 目标文件名为 : 文件名_YYYYMMDD(处理日期).原文件扩展名
//        3. 备份文件名为 : <备份根目录>/<表名>/<表名>_YYYYMMDD.dbf
//==========================================================================
void TCBCPPrepare::SubmitWorkingFileToBcp(TCString sFileName,
        TCString sTableName, TCString sDealDate, bool bSameFieldSeq,
        bool bSameFieldName)
{
    ASSERT(TCTime::IsValidDatetime(sDealDate));

    //===== 1. 得到源文件名 ======
    TCString sSourceFileName;

    sSourceFileName = MergePathAndFile(GetWorkingDir(), sFileName);
    if (!FileExists(sSourceFileName))
        return;

    //==== 2. 得到目标文件名 ======
    TCString sDestFileName;

    sDestFileName = ExtractFileName(sSourceFileName);
    sDestFileName = CutFileExt(sDestFileName) + "_" + sDealDate
            + ExtractFileExt(sDestFileName);
    sDestFileName = MergePathAndFile(GetBCPDir(), sDestFileName);

    //====== 3. 得到备份文件名 ========
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

    //===== 4. 移到目标目录 ======
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

    //====== 5. 备份文件 ========
    if (!CopyFile(sDestFileName, sArchiveFileName))
            throw TCException("TCBCPPrepare::SubmitWorkingFileToBcp() Error : "
                    "CopyFile Error - " + sDestFileName + " to "
                    + sArchiveFileName);
    TCMZCompress::CompressMZFile(sArchiveFileName);

    //======== 6. 提交入库 ======
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


