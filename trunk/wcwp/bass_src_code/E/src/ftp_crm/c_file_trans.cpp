//---------------------------------------------------------------------------
#pragma hdrstop
//---------------------------------------------------------------------------
//#pragma package(smart_init)
#include "cmpublic.h"
#include "c_file_trans.h"
#include "c_exception.h"

//==========================================================================
// 函数 : TCStdBill::AddListInfo
// 用途 : 将原始话单目录中的文件加入到LIST中
// 原型 : void AddListInfo();
// 参数 : 无
// 返回 : 无
// 说明 : 如果LIST中已无任何记录时，调用该函数
//==========================================================================
void TCFileTrans::AddListInfo()
{
    //======== 1. 验证LIST文件中的记录一定为空 =========
    TCString sRawFilePath;
    TCListFile lfListFile;

    sRawFilePath = GetSrcDir();
    lfListFile.SetFileName(MergePathAndFile(sRawFilePath, "LIST"));

    ASSERT(lfListFile.GetRecordCount() == 0);

    //==== 2. 填充LIST文件的内容 ==========
    TCStringList slRawFileList;
    GetDirFileList(slRawFileList, sRawFilePath, GetFileFilter());
    if (slRawFileList.GetCount() == 0)
        return;

    Application.SetNextDelay(0);
    slRawFileList.Sort();
    lfListFile.AppendRecord(slRawFileList);
}

//==========================================================================
// 函数 : TCFileTrans::Connect
// 用途 : 建立与server的连接
// 原型 : void Connect();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCFileTrans::Connect()
{
    printf("Connect start \n");
    m_tcFtp.m_sHost     = GetHostName();
    m_tcFtp.m_sUserID   = GetUserName();
    m_tcFtp.m_sPassword = GetPassword();
    m_tcFtp.Mode(GetTransMode());
    m_tcFtp.Open();
    printf("Connect end \n");
}

//==========================================================================
// 函数 : TCFileTrans::DoGetFile
// 用途 : 从SERVER上取文件的主函数
// 原型 : void DoGetFile();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCFileTrans::DoGetFile()
{
    long nBegin, nEnd;
    static bool s_bFirstTime=1;
    static long nWait = 0;
    TCString sSrcDir, sFileFilter;
    TSFtpFileInfo tsFileInfo;

    long nSrcDirCount;
    long nDstDirCount;
    long nPostfix;
    TCString sFftpName;

    nDstDirCount = GetDestDirList().GetCount();
    nSrcDirCount = GetSrcDirList().GetCount();

    bool bHaveFile = false;
    bool bUpdate;

    for(long i=0; i<nSrcDirCount; i++)
    {
        sSrcDir         = GetSrcDirList()[i];
        sFileFilter     = GetFileFilter();
        bUpdate = false;
        if(s_bFirstTime)
        {
            s_bFirstTime = false;
            s_nLastGetFileTime = time(0);
        }

        if(IsAccordFileDate())
        {   GetLastFileDate();
            sFileFilter = " -rt " + sFileFilter;
        }

	if(IsDealSubdirectory())
	{
	    GetFtpFileNameAndSubDir();
	    try
            {
                Connect();

                sFftpName = GetSubDirFileName(sSrcDir, m_sSubDir[i], m_sFileNamePostfix[i]);

                if(sFftpName != TCString("") )
                {
                    GetOneFile(sFftpName);
                    
                    bHaveFile = true;
                    bUpdate = true;
                    Application.ProcessMessages();
                }
                nEnd = time(0);
                if((nEnd - s_nLastGetFileTime) > 120*60)
                {
                    nPostfix = StrToInt(m_sFileNamePostfix[i]);
                    if(nPostfix == 99999999)
                        m_sFileNamePostfix[i] = "00000000";
                    else
                        m_sFileNamePostfix[i] = IntToStrn(nPostfix + 1,8);
                    bUpdate = true;
                    s_nLastGetFileTime = time(0);
                    nWait ++;
                }

                //======1. 若超过一个小时没采集到文件则报警=====
                if(nWait >= 1)
                {
                    TCAppErrorLog::AddLog(aeWarning, "ftp", "采集", GetHostName() +
                        "有" + IntToStr(nWait) + "个文件没采集到");
                    nWait = 0;
                }
                if(bUpdate)
                {
                    UpdateFtpFileNameAndSubDir(i,m_sSubDir[i], m_sFileNamePostfix[i]);
                }
            }
            catch(TCFTPException &e)
            {
                static long s_nLastErrorCode=0;
                s_nLastErrorCode = e.GetFTPErrorCode();
                TCString sErrMsg = GetHostName() + ":" +e.GetErrMessage();
                TCAppErrorLog::AddLog(aeError, "ftp", s_nLastErrorCode, sErrMsg);
            }
	}
	else
	{
            try
            {
                Connect();
                m_tcFtp.ChangeDir(sSrcDir);
                m_tcFtp.List(sFileFilter);

                for(long i=0; i<m_tcFtp.GetFileCount(); i++)
                {
                    tsFileInfo = m_tcFtp.GetFileInfo(i);
                    if(tsFileInfo.m_bIsDirectory)
                        continue;

                    GetOneFile(tsFileInfo.m_sFileName);
                    bHaveFile = true;
                    Application.ProcessMessages();
                }

                if(IsAlertNoFile())
                {
                    nEnd = time(0);
                    //======1. 若超过一个小时没采集到文件则报警=====
                    if((nEnd - s_nLastGetFileTime) > 60*60)
                        TCAppErrorLog::AddLog(aeWarning, "ftp", "采集", GetHostName() +
                            "已经" + IntToStr((nEnd-s_nLastGetFileTime)/60) + "分钟没采集到文件");
                }
            }
            catch(TCFTPException &e)
            {
                static long s_nLastErrorCode=0;
                s_nLastErrorCode = e.GetFTPErrorCode();
                TCString sErrMsg = GetHostName() + ":" +e.GetErrMessage();
                TCAppErrorLog::AddLog(aeError, "ftp", s_nLastErrorCode, sErrMsg);
            }
        }
    }

    if(bHaveFile)
        Application.SetNextDelay(0);
}

//==========================================================================
// 函数 : TCFileTrans::DoPutFile
// 用途 : 传送文件的主函数
// 原型 : void DoPutFile();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCFileTrans::DoPutFile()
{

    TCString sSrcFileName;
    TCString sDestFileName;
    long nSrcDirCount, nDestDirCount;
    TCString sSrcDir, sDestDir;

    //======== 1. 获取源和目的目录列表 =========
    nSrcDirCount = GetSrcDirList().GetCount();
    printf("nSrcDirCount is %d\n",nSrcDirCount);
    nDestDirCount = GetDestDirList().GetCount();
    printf("nDestDirCount is %d\n",nDestDirCount);
    ASSERT(nSrcDirCount == nDestDirCount);

    try
    {
        Connect();
        for (long i=0; i<nSrcDirCount; i++)
        {
            //====== 2. 得到源和目标文件名 =========
            sSrcDir = GetSrcDirList()[i];
            printf("sSrcDir is %s\n",(char *)sSrcDir);
            sDestDir = GetDestDirList()[i];
            printf("sDestDir is %s\n",(char *)sDestDir);
            sSrcFileName = TCListFile::FetchFileThroughList(sSrcDir);
            printf("sSrcFileName is %s\n",(char *)sSrcFileName);
            if (sSrcFileName == TCString("") )
                continue;
                            
            sDestFileName = MergePathAndFile(sDestDir,
                ExtractFileName(sSrcFileName));
            printf("sDestFileName is %s\n",(char *)sDestFileName);
            //====== 3. 上传一个文件 =======
            m_tcFtp.ChangeDir(GetTmpDir());
            printf("ChangeDir is ok \n");
            UploadOneFile(sSrcFileName, sDestFileName);
            printf("put ok!!\n");
            if(IsNeedDeleteSource())
                DeleteFileE(sSrcFileName);
            if(IsNeedSrcBackUp())
            {
                TCString sBackFileName;
                TCString sPureFileName =ExtractFileName(sSrcFileName);

    	        TCStringList &slSrcBackUpList =GetSrcBackUpList();
    	        for(i =0;i <slSrcBackUpList.GetCount();i++)
                {
    		        sBackFileName =MergePathAndFile(slSrcBackUpList[i],sPureFileName);
    	    	    RenameFileE(sSrcFileName,sBackFileName);
                    TCListFile::AppendFileToList(sBackFileName);

                }
    	    }
            TCListFile::CutFileFromList(sSrcFileName);
            
            Application.SetNextDelay(0);
        }
    }
    catch(TCFTPException &e)
    {
        static long s_nLastErrorCode=0;
        s_nLastErrorCode = e.GetFTPErrorCode();
        TCString sErrMsg = GetHostName() + ":" + e.GetErrMessage();
        TCAppErrorLog::AddLog(aeError, "ftp", s_nLastErrorCode, sErrMsg);
    }
}

//==========================================================================
// 函数 : TCFileTrans::GetOneFile
// 用途 : 取一个文件
// 原型 : void GetOneFile();
// 参数 : 源文件名
// 返回 : 无
// 说明 :
//==========================================================================
void TCFileTrans::GetOneFile(TCString sSrcFile)
{
    TCString sFileDate;
    int i=0;

    if( IsAccordFileDate())
    {
        if(GetCollectType() == TCString("smc") )
            sFileDate = Mid(sSrcFile, 4, 14);
        else
            sFileDate = Mid(sSrcFile, 5, 8) + Mid(sSrcFile, 14, 6);
        if(!TCTime::IsValidDatetime(sFileDate))
        {   TCAppErrorLog::AddLog(aeWarning, "ftp", GetCollectType(),
                GetHostName() + ":无效的文件名" + sSrcFile);
            return;
        }
        if(sFileDate <= m_sFileDate)
            return;
    }

    TCString sLogBeginTime = TCTime::Now();
    TCStringList & sDestDirList = GetDestDirList();
    for ( i = 0 ; i < sDestDirList.GetCount() ; i++ )
	    ForceDirectoriesE(sDestDirList[i]);
    TCString sTmpFile = MergePathAndFile(sDestDirList[0], "."+sSrcFile);

    m_tcFtp.Download(sSrcFile, sTmpFile);
    if(IsAccordFileDate())
    {   m_sFileDate = sFileDate;
        WriteFileDate();
    }

    if(IsNeedUncompress()&&Right(sTmpFile,2) == TCString(".Z") )
    {
        TCString sCommand = "uncompress " + sTmpFile;
        if(system( sCommand) < 0)
            throw TCException("TCFileTrans::GetOneFile() Error:"
                "Can not uncompress file "+sTmpFile);
        sTmpFile = Left(sTmpFile, Length(sTmpFile) - 2);
    }

    TCString sDestFile;
     for (i = 1; i < sDestDirList.GetCount() ; i++ )
    {
	sDestFile = MergePathAndFile(sDestDirList[i], Mid(ExtractFileName(sTmpFile),2));
	CopyFileE(sTmpFile,sDestFile);
	if(IsNeedAddList())
        	TCListFile::AppendFileToList(sDestFile);
    }

    sDestFile = MergePathAndFile(sDestDirList[0], Mid(ExtractFileName(sTmpFile),2));

    RenameFileE(sTmpFile, sDestFile);
    if ( IsNeedAddList() ) TCListFile::AppendFileToList(sDestFile);

    s_nLastGetFileTime = time(0);
    WriteFileTranLog(sSrcFile, FileSize(sDestFile),sLogBeginTime, TCTime::Now());

    if(IsNeedSrcBackUp()){
    	TCString sBackFileName;
    	TCString sPureFileName =ExtractFileName(sSrcFile);

    	TCStringList &slSrcBackUpList =GetSrcBackUpList();
    	for(i =0;i <slSrcBackUpList.GetCount();i++)
        {
    		sBackFileName =MergePathAndFile(slSrcBackUpList[i],sPureFileName);
    		m_tcFtp.Rename(sSrcFile,sBackFileName);
        }

    }
    else if ( IsNeedDeleteSource() ) m_tcFtp.DeleteFile(sSrcFile);
    //end if
}

//==========================================================================
// 函数 : TCFileTrans::GetCollectType
// 用途 : 获取采集类型
// 原型 : TCString GetCollectType();
// 参数 : 无
// 返回 : 采集类型
// 说明 :
//==========================================================================
TCString TCFileTrans::GetCollectType()
{
    static TCString s_sCollectType;
    if(s_sCollectType == TCString("") )
    {
        s_sCollectType = ProfileAppString(Application.GetAppName(), "collect", "type", "");

        if(s_sCollectType == TCString("") )
            throw TCException("TCFileTrans::GetTransMode() Error:"
                "[collect] type not set.");

        if(s_sCollectType != TCString("smc") && s_sCollectType != TCString("ismg") )
            throw TCException("TCFileTrans::GetTransMode() Error:"
                "[collect] type is invalid. (smc or ismg), " + s_sCollectType);
    }
    return s_sCollectType;
}

//==========================================================================
// 函数 : TCFileTrans::GetSourceDir
// 用途 : 读取文件在服务器上存放路径
// 原型 : TCString GetSourceDir();
// 参数 : 无
// 返回 : 路径名
// 说明 :
//==========================================================================
TCString TCFileTrans::GetSrcDir()
{
    static TCString s_sSourceDir;
    if( s_sSourceDir == TCString("") )
        s_sSourceDir = TAppPath::GetConfigSourceDirectory();

    return s_sSourceDir;
}

//==========================================================================
// 函数 : TCFileTrans::GetSourceDirList
// 用途 : 读取文件在服务器上存放路径列表
// 原型 : TCString GetSourceDirList();
// 参数 : 无
// 返回 : 路径列表
// 说明 :
//==========================================================================
TCStringList TCFileTrans::GetSrcDirList()
{
    static TCStringList s_sSourceDirList;
    if( s_sSourceDirList.GetCount() == 0 )
    {
        TCString sListName = ProfileAppString(Application.GetAppName(),
            "SRC_DIR_LIST","list_name","");
        ASSERT(sListName != TCString("") );

        TCIniFile ifConfig;
        ifConfig.Load(TAppFile::ApplicationConfig());
        ifConfig.ReadSection(sListName, s_sSourceDirList);
    }
    return s_sSourceDirList;
}

//==========================================================================
// 函数 : TCFileTrans::GetDestDirList
// 用途 : 读取文件在服务器上存放路径列表
// 原型 : TCString GetDestDirList();
// 参数 : 无
// 返回 : 路径列表
// 说明 :
//==========================================================================
TCStringList TCFileTrans::GetDestDirList()
{
    static TCStringList s_sDestDirList;
    if( s_sDestDirList.GetCount() == 0 )
    {
        TCString sListName = ProfileAppString(Application.GetAppName(),
            "DEST_DIR_LIST","list_name","");
        ASSERT(sListName != TCString("") );

        TCIniFile ifConfig;
        ifConfig.Load(TAppFile::ApplicationConfig());
        ifConfig.ReadSection(sListName, s_sDestDirList);
    }
    return s_sDestDirList;
}

//==========================================================================
// 函数 : TCFileTrans::GetSrcBackUpList
// 用途 : 读取文件在服务器上备份路径列表
// 原型 : TCString GetSrcBackUpList();
// 参数 : 无
// 返回 : 路径列表
// 说明 :
//==========================================================================
TCStringList TCFileTrans::GetSrcBackUpList()
{
    static TCStringList s_sDestDirList;
    if( s_sDestDirList.GetCount() == 0 )
    {
        TCString sListName = ProfileAppString(Application.GetAppName(),
            "SRC_BACKUP_DIR_LIST","list_name","");
        ASSERT(sListName != TCString("") );

        TCIniFile ifConfig;
        ifConfig.Load(TAppFile::ApplicationConfig());
        ifConfig.ReadSection(sListName, s_sDestDirList);
    }
    return s_sDestDirList;
}
//==========================================================================
// 函数 : TCFileTrans::GetInterval
// 用途 : 获取每次取文件的时间间隔
// 原型 : TCString GetInterval();
// 参数 : 无
// 返回 : 时间间隔以秒为单位();
// 说明 :
//==========================================================================
long TCFileTrans::GetInterval()
{
    static long s_nInterval=0;
    if(s_nInterval == 0)
    {
        s_nInterval = ProfileAppInt(Application.GetAppName(), "ftp", "interval", 0);

        if(s_nInterval == 0)
            throw TCException("TCFileTrans::GetInterval() Error:"
                "[ftp] interval not set.(1min-15min)");

        if(s_nInterval < 1 || s_nInterval > 15)
            throw TCException("TCFileTrans::GetInterval() Error:"
                "[ftp] interval must between 1 and 15.");
    }
    return s_nInterval;
}

//==========================================================================
// 函数 : TCFileTrans::GetHostName
// 用途 : 获取主机名或IP
// 原型 : TCString GetHostName();
// 参数 : 无
// 返回 : 主机名;
// 说明 :
//==========================================================================
TCString TCFileTrans::GetHostName()
{
    static TCString s_sHostName;
    if(s_sHostName == TCString("") )
    {
        s_sHostName = ProfileAppString(Application.GetAppName(), "ftp", "host", "");

        if(s_sHostName == TCString("") )
            throw TCException("TCFileTrans::GetHostName() Error:"
                "[ftp] host not set.");
    }
    return s_sHostName;
}

//==========================================================================
// 函数 : TCFileTrans::GetUserName
// 用途 : 获取用户名
// 原型 : TCString GetUserName();
// 参数 : 无
// 返回 : 用户名;
// 说明 :
//==========================================================================
TCString TCFileTrans::GetUserName()
{
    static TCString s_sUserName;
    if(s_sUserName == TCString("") )
    {
        s_sUserName = ProfileAppString(Application.GetAppName(), "ftp", "user", "");

        if(s_sUserName == TCString("") )
            throw TCException("TCFileTrans::GetUserName() Error:"
                "[ftp] user not set.");
    }
    return s_sUserName;
}

//==========================================================================
// 函数 : TCFileTrans::GetPassword
// 用途 : 获取口令
// 原型 : TCString GetPassword();
// 参数 : 无
// 返回 : 口令;
// 说明 :
//==========================================================================
TCString TCFileTrans::GetPassword()
{
    static TCString s_sPassword;
    if(s_sPassword == TCString("") )
    {
        s_sPassword = ProfileAppString(Application.GetAppName(), "ftp", "pass", "");

        if(s_sPassword == TCString("") )
            throw TCException("TCFileTrans::GetPassword() Error:"
                "[ftp] pass not set.");
    }
    return s_sPassword;
}


//==========================================================================
// 函数 : TCFileTrans::GetFileFilter
// 用途 : 文件名过滤字符串
// 原型 : TCString GetFileFilter();
// 参数 : 无
// 返回 : 文件名过滤字符串;
// 说明 :
//==========================================================================
TCString TCFileTrans::GetFileFilter()
{
    static TCString s_sFileFilter;
    static bool s_bFirstTime = true;
    if(s_bFirstTime)
    {
        s_bFirstTime = false;
        s_sFileFilter = ProfileAppString(Application.GetAppName(), "ftp", "filt", "");
    }
    return s_sFileFilter;
}

//==========================================================================
// 函数 : TCFileTrans::GetLastFileDate
// 用途 : 取得最后一个采件文件的时间戳
// 原型 : TCString GetLastFileDate();
// 参数 : 无
// 返回 : 时间戳;
// 说明 :
//==========================================================================
void TCFileTrans::GetLastFileDate()
{
    TCString sRunningInfoFile;
    TCIniFile iniFile;

    sRunningInfoFile = TAppPath::AppRunningInfo() + ".ftp_lastdate.ini";

    if (!FileExists(sRunningInfoFile))
    {   m_sFileDate = "";
    }
    else
    {
        iniFile.Load(sRunningInfoFile);
        m_sFileDate = iniFile.ReadString("general", "last_deal_date", "");
    }
}

//==========================================================================
// 函数 : TCFileTrans::GetTransMode
// 用途 : 获取传输模式
// 原型 : TCString GetTransMode();
// 参数 : 无
// 返回 : 获取传输模式
// 说明 :
//==========================================================================
TCString TCFileTrans::GetTransMode()
{
    static TCString s_sTransMode;
    if(s_sTransMode == TCString("") )
    {
        s_sTransMode = ProfileAppString(Application.GetAppName(), "ftp", "mode", "");

        if(s_sTransMode == TCString("") )
            throw TCException("TCFileTrans::GetTransMode() Error:"
                "[ftp] mode not set.");

        if(s_sTransMode != TCString("A") && s_sTransMode != TCString("B") && s_sTransMode != TCString("I") )
            throw TCException("TCFileTrans::GetTransMode() Error:"
                "[ftp] mode is invalid. (A, B, I), " + s_sTransMode);
    }
    return s_sTransMode;
}

//==========================================================================
// 函数 : TCFileTrans::GetTmpDir
// 用途 : 获取临时目录
// 原型 : TCString GetTmpDir();
// 参数 : 无
// 返回 : 获取临时目录
// 说明 :
//==========================================================================
TCString TCFileTrans::GetTmpDir()
{
    static TCString s_sTmpDir;
    if(s_sTmpDir == TCString("") )
    {
        s_sTmpDir = ProfileAppString(Application.GetAppName(), "TMP", "directory", "");

        if(s_sTmpDir == TCString("") )
            throw TCException("TCFileTrans::GetTmpDir() Error:"
                "[TMP] directory not set.");

    }
    return s_sTmpDir;
}

//==========================================================================
// 函数 : TCFileTrans::IsAccordFileDate
// 用途 : 是否依照文件名中的日期决定是否取该文件
// 原型 : bool IsAccordFileDate();
// 参数 : 无
// 返回 : 是否按文件名中的日期
// 说明 : 短信中心和短信网关的采集需要按文件名中的时间
//        判断该文件是否已被采集
//==========================================================================
bool TCFileTrans::IsAccordFileDate()
{
    static bool s_bFirstTime = 1;
    static bool s_bIsAccordFileDate;

    if (s_bFirstTime)
    {
        s_bFirstTime = false;
        s_bIsAccordFileDate = ProfileAppBool(Application.GetAppName(),
                        "ftp", "accord_file_date", false);
    }

    return s_bIsAccordFileDate;
}

//==========================================================================
// 函数 : TCFileTrans::IsNeedAddList
// 用途 : 是否需要将文件名加入到LIST
// 原型 : bool IsNeedAddList();
// 参数 : 无
// 返回 : 是否需加LIST
// 说明 :
//==========================================================================
bool  TCFileTrans::IsNeedAddList()
{
    static bool s_bFirstTime = 1;
    static bool s_bIsNeedAddList;

    if (s_bFirstTime)
    {
        s_bFirstTime = false;
        s_bIsNeedAddList = ProfileAppBool(Application.GetAppName(),
                        "ftp", "need_add_list", false);
    }

    return s_bIsNeedAddList;
}

//==========================================================================
// 函数 : TCFileTrans::IsNeedDeleteSource
// 用途 : 是否需要删除源文件
// 原型 : bool IsNeedDeleteSource();
// 参数 : 无
// 返回 : 是否需删除源文件
// 说明 :
//==========================================================================
bool  TCFileTrans::IsNeedDeleteSource()
{
    static bool s_bFirstTime = 1;
    static bool s_bIsNeedDelSrc;

    if (s_bFirstTime)
    {
        s_bFirstTime = false;
        s_bIsNeedDelSrc = ProfileAppBool(Application.GetAppName(),
                        "ftp", "need_del_src", false);
    }

    return s_bIsNeedDelSrc;
}

//==========================================================================
// 函数 : TCFileTrans::IsNeedUncompress
// 用途 : 是否需要解压
// 原型 : bool IsNeedUncompress();
// 参数 : 无
// 返回 : 是否需要解压
// 说明 :
//==========================================================================
bool  TCFileTrans::IsNeedUncompress()
{
    static bool s_bFirstTime = 1;
    static bool s_bIsNeedUncompress;

    if (s_bFirstTime)
    {
        s_bFirstTime = false;
        s_bIsNeedUncompress = ProfileAppBool(Application.GetAppName(),
                        "ftp", "need_uncompress", false);
    }

    return s_bIsNeedUncompress;
}

//==========================================================================
// 函数 : TCFileTrans::IsNeedSrcBackUp
// 用途 : 是否需要备份原文件
// 原型 : bool IsNeedSrcBackUp();
// 参数 : 无
// 返回 : 是否需要解压
// 说明 :
//==========================================================================
bool  TCFileTrans::IsNeedSrcBackUp()
{
    static bool s_bFirstTime = 1;
    static bool s_bIsNeedBackUp;

    if (s_bFirstTime)
    {
        s_bFirstTime = false;
        s_bIsNeedBackUp = ProfileAppBool(Application.GetAppName(),
                        "ftp", "need_backup_src", false);
    }

    return s_bIsNeedBackUp;
}

//==========================================================================
// 函数 : TCFileTrans::UploadOneFile
// 用途 : 上传一个文件
// 原型 : TCString UploadOneFile();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCFileTrans::UploadOneFile(TCString sSrcFile, TCString sDestFile)
{
    TCString sLogBeginTime = TCTime::Now();
    printf("sSrcFile is %s\n",(char *)sSrcFile);
    TCString sTmpFile = MergePathAndFile(GetTmpDir(), ExtractFileName(sSrcFile));
    printf("sTmpFile is %s\n",(char *)sTmpFile);
    m_tcFtp.Upload(sSrcFile, "");
    printf("upload is ok!!!\n");
    m_tcFtp.Rename(sTmpFile, sDestFile);

    WriteFileTranLog(ExtractFileName(sSrcFile), FileSize(sSrcFile),
            sLogBeginTime, TCTime::Now());
}

//==========================================================================
// 函数 : TCFileTrans::WriteFileDate
// 用途 : 记录采集文件的时间戳
// 原型 : TCString WriteFileDate();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCFileTrans::WriteFileDate()
{

    //====== 1. 得到上一个处理日期 =======
    TCString sRunningInfoFile;
    TCIniFile iniFile;

    sRunningInfoFile = TAppPath::AppRunningInfo() + ".ftp_lastdate.ini";

    if (!FileExists(sRunningInfoFile))
    {   iniFile.CreateNew(sRunningInfoFile);
    }
    else
        iniFile.Load(sRunningInfoFile);
    iniFile.WriteString("general", "last_deal_date", m_sFileDate);
}


//==========================================================================
// 函数 : TCFileTrans::WriteFileTranLog
// 用途 : 记录传输日志　
// 原型 : TCString WriteFileTranLog();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCFileTrans::WriteFileTranLog(TCString sFileName, long nFileSize,
        TCString dtStartTime, TCString dtStopTime)
{
    TCString sLogFileName;
    TCString sDailyLogFile;
    //======= 1. 如果日志文件不存在, 则创建之 ==============
    sDailyLogFile = TCAppLog::GetDailyLogFileNameOfApplication();
    sLogFileName = CutFileExt(sDailyLogFile) + "_" + Application.GetProcessFlag()
            + ExtractFileExt(sDailyLogFile);
    printf("sLogFileName is %s\n",(char *)sLogFileName);
    if (!FileExists(sLogFileName))
    {
        TCDBFCreate dcCreate(sLogFileName, 5);
        dcCreate.AddField("file_name",  'C', 24);
        dcCreate.AddField("file_size",  'N', 10);
        dcCreate.AddField("start_time", 'C', 14);
        dcCreate.AddField("stop_time",  'C', 14);
        dcCreate.AddField("time_cost",  'N', 6);

        dcCreate.CreateDBF();
    }

    //====== 2. 写日志文件 ========
    TCString sPureFileName = ExtractFileName(sFileName);
    long nTimeCost;
    nTimeCost = TCTime::SecondsAfter(dtStartTime, dtStopTime);

    TCFoxDBF dbfLog;
    dbfLog.AttachFile(sLogFileName);

    dbfLog.DBBind("file_name",  sFileName);
    dbfLog.DBBind("file_size",  nFileSize);
    dbfLog.DBBind("start_time", dtStartTime);
    dbfLog.DBBind("stop_time",  dtStopTime);
    dbfLog.DBBind("time_cost",  nTimeCost);

    dbfLog.Append();
    dbfLog.CloseDBF();
}

//==========================================================================
// 函数 : DealSubdirectory
// 用途 : 是否处理子目录　
// 原型 : bool DealSubdirectory();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
bool TCFileTrans::IsDealSubdirectory()
{
    static bool s_bFirst = true;
    static bool s_bDealSubdirectory;

    if (s_bFirst)
    {
        s_bFirst = false;
        s_bDealSubdirectory = ProfileAppBool(Application.GetAppName(),
                        "ftp", "deal_subdirectoty", false);
    }

    return s_bDealSubdirectory;
}

//==========================================================================
// 函数 : IsAlertNoFile
// 用途 : 是否在一个小时没有采集到文件报警　
// 原型 : bool IsAlertNoFile();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
bool TCFileTrans::IsAlertNoFile()
{
    static bool s_bFirst = true;
    static bool s_bAlertNoFile;

    if (s_bFirst)
    {
        s_bFirst = false;
        s_bAlertNoFile = ProfileAppBool(Application.GetAppName(),
                        "ftp", "alert_nofile", false);
    }

    return s_bAlertNoFile;
}

//==========================================================================
// 函数 : GetFtpFileNameAndSubDir
// 用途 : 得到要采集的　
// 原型 : bool GetFtpFileNameAndSubDir();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCFileTrans::GetFtpFileNameAndSubDir()
{
    long i;
    TCString DestFile;
    TCString sTmpStr;

    TCStringList &sDestDirList = GetDestDirList();


    	for(i=0; i<sDestDirList.GetCount(); i++)
    	{
    	    DestFile = MergePathAndFile(sDestDirList[i], ".ftpcj_file");

    	    if(!FileExists(DestFile))
    	    {
    	        throw TCException("采集配置" + DestFile + ""
    	            "文件不存在");
    	    }
    	    else
    	    {
    	        m_sSubDir[i] = ProfileString(DestFile, "sub_dir", "dir_name","");
    	        m_sFileNamePostfix[i] = ProfileString(DestFile, "file_name", "postfix","");
    	    }
    	}
}
//==========================================================================
// 函数 : UpdateFtpFileNameAndSubDir
// 用途 : 得到要采集的　
// 原型 : bool UpdateFtpFileNameAndSubDir();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCFileTrans::UpdateFtpFileNameAndSubDir(long i, TCString sSubDir, TCString sFileNamePostfix)
{
    TCFileStream fpFtpcjFile;
    TCString sTmpStr;
    TCString DestFile;

    DestFile = MergePathAndFile(GetDestDirList()[i], ".ftpcj_file");
    //DestFile = MergePathAndFile(GetDestDirList()[i], "ftpcj_file");
    fpFtpcjFile.Open(DestFile,omWrite+omText);

    sTmpStr = "[sub_dir]\ndir_name = " + sSubDir + "\n";
    fpFtpcjFile.Write(sTmpStr,Length(sTmpStr));
    sTmpStr = "[file_name]\npostfix = " + sFileNamePostfix+ "\n";
    fpFtpcjFile.Write(sTmpStr,Length(sTmpStr));

    fpFtpcjFile.Close();

}

//==========================================================================
// 函数 : GetFtpFileName
// 用途 : 是否进行采集　
// 原型 : long IsGetFile();
// 参数 : 服务端的文件列表，本地纪录的要采集的文件
// 返回 : 要采集文件的索引
// 说明 :
//==========================================================================
long TCFileTrans::GetFtpFileName(TCStringList tsFtpFileList, TCString sFileNamePostfix)
{
    long i;
    long nPostfixlen;
    long nPrefixlen;
    long nFileNamePostfix;
    long nFtpFile;
    TCString sPrefix;

    tsFtpFileList.Sort();

    i = tsFtpFileList.GetCount() -1 ;
    if(i <= 0)
    	return -1;

    nPostfixlen = Length(sFileNamePostfix);
    sPrefix = GetFileFilter();
    nPrefixlen = Length(sPrefix);

    nFtpFile = StrToInt(Mid(tsFtpFileList[i], nPrefixlen-1+8+1, nPostfixlen));
    nFileNamePostfix = StrToInt(sFileNamePostfix);

    while(!CheckFtpFileName(tsFtpFileList[i],nPostfixlen))
    {
        i--;
        if(i < 0)
            break;
    }
    while(nFtpFile > nFileNamePostfix)
    {
        i--;
        if(i < 0)
            break;
        while(!CheckFtpFileName(tsFtpFileList[i],nPostfixlen))
        {
            i--;
            if(i < 0)
            break;
        }
        nFtpFile = StrToInt(Mid(tsFtpFileList[i], nPrefixlen-1+8+1, nPostfixlen));
    }
    if(i >= 0 )
    {
#ifdef _DEBUG
TCString a = Mid(tsFtpFileList[i], nPrefixlen-1+8+1, nPostfixlen);
#endif
        if(Mid(tsFtpFileList[i], nPrefixlen-1+8+1, nPostfixlen) == sFileNamePostfix)
            return i;
    }
    
    return -1;
}

//==========================================================================
// 函数 : GetFtpSubDir
// 用途 : 是否进行采集　
// 原型 : long GetFtpSubDir();
// 参数 : 服务端的子目录列表，本地纪录的要采集的子目录
// 返回 : 要采集子目录的索引
// 说明 :
//==========================================================================
long TCFileTrans::GetFtpSubDir(TCStringList tsSubDir, TCString tsSubDirName)
{
    long i;
        
    tsSubDir.Sort();
    
    i = tsSubDir.GetCount() - 1;
    if(i <= 0)
    	return -1;
#ifdef _DEBUG
TCString a =  tsSubDir[0];
TCString b =  tsSubDir[1];
#endif
    while(Length(tsSubDir[i]) != 8 || !IsNumber(tsSubDir[i]) ||
       StrToInt(tsSubDir[i]) > StrToInt(tsSubDirName) )
    {
        i--;
        if(i < 0)
            break;
    }
    if(tsSubDir[i] == tsSubDirName)
        return i;
    
    return -1;
}


//==========================================================================
// 函数 : CheckFtpFileName
// 用途 : 得到采集的文件名　
// 原型 : long CheckFtpFileName(TCString sFileName,long nPostfixLength);
// 参数 : 要采集的文件，文件的后缀的长度
// 返回 : 是否为正确文件
// 说明 :
//==========================================================================
bool TCFileTrans::CheckFtpFileName(TCString sFileName,long nPostfixLength)
{
    TCString sPrefix;
    TCString sFilter;
    long nPrefixlen;
    
    sFilter = GetFileFilter();
    nPrefixlen = Length(sPrefix);
    sPrefix = Left(sFilter, nPrefixlen-1);

    if(Length(sFileName) < nPrefixlen-1+8+nPostfixLength)
    	return false;
    if(!IsNumber(Mid(sFileName, nPrefixlen-1+8+1, nPostfixLength)))
        return false;
   	
    return true;
}

//==========================================================================
// 函数 : GetSubDirFileName
// 用途 : 是否进行采集　
// 原型 : long GetSubDirFileName();
// 参数 : 要采集的文件，文件的后缀
// 返回 : 是否为正确文件
// 说明 :
//==========================================================================
TCString TCFileTrans::GetSubDirFileName(TCString sSrcDir, TCString& sSubDir, TCString& sPostfix)
{
    TCStringList tsFtpFileList;
    TCStringList tsFtpDirList;
    TCString sFileFilter;
    TCString sFtpSrcDir;
    TSFtpFileInfo tsFileInfo;
    
    long nSubDirIdx;
    long nFileNameIdx;
    long nSubDirCount;	
    long nPostfix;
    long i;
       
    sFileFilter = GetFileFilter();   
       
    m_tcFtp.ChangeDir(sSrcDir);
    m_tcFtp.List("");
    tsFtpDirList.Clear();
    for(i=0; i<m_tcFtp.GetFileCount(); i++)
    {
    	tsFileInfo = m_tcFtp.GetFileInfo(i);
        if(!tsFileInfo.m_bIsDirectory)
            continue;
        if(tsFileInfo.m_sFileName == TCString("") )
            continue;
        if(tsFileInfo.m_sFileName == TCString(".") )
            continue;
        if(tsFileInfo.m_sFileName == TCString("..") )
            continue;
        tsFtpDirList.Add(tsFileInfo.m_sFileName);
    }
    nSubDirIdx = GetFtpSubDir(tsFtpDirList,sSubDir);
    if(nSubDirIdx < 0)
    	return "";

    nSubDirCount = tsFtpDirList.GetCount();
    
    if(nSubDirIdx < nSubDirCount-1)
    {
    	if(IsFtpAll(sSrcDir,tsFtpDirList[nSubDirIdx],tsFtpDirList[nSubDirIdx+1],sPostfix))
    	{
    	    nSubDirIdx++;
	    sSubDir = tsFtpDirList[nSubDirIdx];
    	}
    }

    //sFtpSrcDir = MergePathAndFile(sSrcDir, sSubDir);
    sFtpSrcDir = sSrcDir + "/" + sSubDir;
    m_tcFtp.ChangeDir(sFtpSrcDir);
    m_tcFtp.List(sFileFilter);

    tsFtpFileList.Clear();
    for(i=0; i<m_tcFtp.GetFileCount(); i++)
    {
    	tsFileInfo = m_tcFtp.GetFileInfo(i);
        if(tsFileInfo.m_bIsDirectory)
            continue;
        tsFtpFileList.Add(tsFileInfo.m_sFileName);
    }

    nFileNameIdx = GetFtpFileName(tsFtpFileList,sPostfix);
    if(nFileNameIdx >= 0 )
    {
        nPostfix = StrToInt(sPostfix);
        if(nPostfix == 99999999)
        	sPostfix = "00000000";
        else
        	sPostfix = IntToStrn(nPostfix + 1,8);
        return tsFtpFileList[nFileNameIdx];
    }
    return "";
}

//==========================================================================
// 函数 : IsFtpAll
// 用途 : 是否进行采集　
// 原型 : long IsFtpAll();
// 参数 : 要采集的文件，文件的后缀
// 返回 : 是否为正确文件
// 说明 :
//==========================================================================
bool TCFileTrans::IsFtpAll(TCString sSrcDir, TCString sSubDir,TCString sNextDir,TCString sPostfix)
{
    long i;
    long nLen;
    long nNextLen;
    long nPrefixlen;
    long nPostfixlen;
    TCString sTmpSrcDir;
    TCString sFileFilter;
    TCStringList tsFtpFileList;
    TCStringList tsFtpFileListNext;
    TSFtpFileInfo tsFileInfo;
        
    sFileFilter = GetFileFilter();   
    
    //sTmpSrcDir = MergePathAndFile(sSrcDir, sSubDir);
    sTmpSrcDir = sSrcDir + "/" + sSubDir;
    m_tcFtp.ChangeDir(sTmpSrcDir);
    m_tcFtp.List(sFileFilter);
    
    tsFtpFileList.Clear();
    for(i=0; i<m_tcFtp.GetFileCount(); i++)
    {
    	tsFileInfo = m_tcFtp.GetFileInfo(i);
        if(tsFileInfo.m_bIsDirectory)
            continue;
        tsFtpFileList.Add(tsFileInfo.m_sFileName);
    }
    tsFtpFileList.Sort();
    nLen = tsFtpFileList.GetCount();
    if(nLen <= 0)
    	return false;
    
    //sTmpSrcDir = MergePathAndFile(sSrcDir, sNextDir);
    sTmpSrcDir = sSrcDir + "/" + sNextDir;
    m_tcFtp.ChangeDir(sTmpSrcDir);
    m_tcFtp.List(sFileFilter);
    tsFtpFileListNext.Clear();
    for(i=0; i<m_tcFtp.GetFileCount(); i++)
    {
    	tsFileInfo = m_tcFtp.GetFileInfo(i);
        if(tsFileInfo.m_bIsDirectory)
            continue;
        tsFtpFileListNext.Add(tsFileInfo.m_sFileName);
    }
    tsFtpFileListNext.Sort();
    nNextLen = tsFtpFileListNext.GetCount();
    if(nNextLen <= 0)
    	return false;
    
    nPostfixlen = Length(sPostfix);
    nPrefixlen = Length(sFileFilter);
    
    if(StrToInt(Mid(tsFtpFileList[nLen-1], nPrefixlen+8+1, nPostfixlen)) < StrToInt(sPostfix) &&
       StrToInt(Mid(tsFtpFileListNext[0], nPrefixlen+8+1, nPostfixlen)) >= StrToInt(sPostfix)) 
         return true;
    return false;
}
//==========================================================================
// 函数 : IntToStrn
// 用途 : 转化定长字符串
// 原型 : long IntToStrn();
// 参数 : 要采集的文件，文件的后缀
// 返回 : 是否为正确文件
// 说明 :
//==========================================================================
TCString TCFileTrans::IntToStrn(long nSrc, long nLen)
{
    long i;
    long nTmp;
    char cDest[20];
    char cRet[20];
    TCString sRet;

    for(i=0; i<nLen; i++)
    {
        nTmp = nSrc%10;
        nSrc = nSrc/10;
        cDest[i] = nTmp + '0';
    }
    for(i=0; i<nLen; i++)
    {
        cRet[i] = cDest[nLen-1-i];
    }
    cRet[nLen] = 0;
    sRet = cRet;
    return sRet;
}

