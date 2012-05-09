//---------------------------------------------------------------------------
#pragma hdrstop
//---------------------------------------------------------------------------
//#pragma package(smart_init)
#include "cmpublic.h"
#include "c_file_trans.h"
#include "c_exception.h"

//==========================================================================
// ���� : TCStdBill::AddListInfo
// ��; : ��ԭʼ����Ŀ¼�е��ļ����뵽LIST��
// ԭ�� : void AddListInfo();
// ���� : ��
// ���� : ��
// ˵�� : ���LIST�������κμ�¼ʱ�����øú���
//==========================================================================
void TCFileTrans::AddListInfo()
{
    //======== 1. ��֤LIST�ļ��еļ�¼һ��Ϊ�� =========
    TCString sRawFilePath;
    TCListFile lfListFile;

    sRawFilePath = GetSrcDir();
    lfListFile.SetFileName(MergePathAndFile(sRawFilePath, "LIST"));

    ASSERT(lfListFile.GetRecordCount() == 0);

    //==== 2. ���LIST�ļ������� ==========
    TCStringList slRawFileList;
    GetDirFileList(slRawFileList, sRawFilePath, GetFileFilter());
    if (slRawFileList.GetCount() == 0)
        return;

    Application.SetNextDelay(0);
    slRawFileList.Sort();
    lfListFile.AppendRecord(slRawFileList);
}

//==========================================================================
// ���� : TCFileTrans::Connect
// ��; : ������server������
// ԭ�� : void Connect();
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : TCFileTrans::DoGetFile
// ��; : ��SERVER��ȡ�ļ���������
// ԭ�� : void DoGetFile();
// ���� : ��
// ���� : ��
// ˵�� :
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

                //======1. ������һ��Сʱû�ɼ����ļ��򱨾�=====
                if(nWait >= 1)
                {
                    TCAppErrorLog::AddLog(aeWarning, "ftp", "�ɼ�", GetHostName() +
                        "��" + IntToStr(nWait) + "���ļ�û�ɼ���");
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
                    //======1. ������һ��Сʱû�ɼ����ļ��򱨾�=====
                    if((nEnd - s_nLastGetFileTime) > 60*60)
                        TCAppErrorLog::AddLog(aeWarning, "ftp", "�ɼ�", GetHostName() +
                            "�Ѿ�" + IntToStr((nEnd-s_nLastGetFileTime)/60) + "����û�ɼ����ļ�");
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
// ���� : TCFileTrans::DoPutFile
// ��; : �����ļ���������
// ԭ�� : void DoPutFile();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCFileTrans::DoPutFile()
{

    TCString sSrcFileName;
    TCString sDestFileName;
    long nSrcDirCount, nDestDirCount;
    TCString sSrcDir, sDestDir;

    //======== 1. ��ȡԴ��Ŀ��Ŀ¼�б� =========
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
            //====== 2. �õ�Դ��Ŀ���ļ��� =========
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
            //====== 3. �ϴ�һ���ļ� =======
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
// ���� : TCFileTrans::GetOneFile
// ��; : ȡһ���ļ�
// ԭ�� : void GetOneFile();
// ���� : Դ�ļ���
// ���� : ��
// ˵�� :
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
                GetHostName() + ":��Ч���ļ���" + sSrcFile);
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
// ���� : TCFileTrans::GetCollectType
// ��; : ��ȡ�ɼ�����
// ԭ�� : TCString GetCollectType();
// ���� : ��
// ���� : �ɼ�����
// ˵�� :
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
// ���� : TCFileTrans::GetSourceDir
// ��; : ��ȡ�ļ��ڷ������ϴ��·��
// ԭ�� : TCString GetSourceDir();
// ���� : ��
// ���� : ·����
// ˵�� :
//==========================================================================
TCString TCFileTrans::GetSrcDir()
{
    static TCString s_sSourceDir;
    if( s_sSourceDir == TCString("") )
        s_sSourceDir = TAppPath::GetConfigSourceDirectory();

    return s_sSourceDir;
}

//==========================================================================
// ���� : TCFileTrans::GetSourceDirList
// ��; : ��ȡ�ļ��ڷ������ϴ��·���б�
// ԭ�� : TCString GetSourceDirList();
// ���� : ��
// ���� : ·���б�
// ˵�� :
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
// ���� : TCFileTrans::GetDestDirList
// ��; : ��ȡ�ļ��ڷ������ϴ��·���б�
// ԭ�� : TCString GetDestDirList();
// ���� : ��
// ���� : ·���б�
// ˵�� :
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
// ���� : TCFileTrans::GetSrcBackUpList
// ��; : ��ȡ�ļ��ڷ������ϱ���·���б�
// ԭ�� : TCString GetSrcBackUpList();
// ���� : ��
// ���� : ·���б�
// ˵�� :
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
// ���� : TCFileTrans::GetInterval
// ��; : ��ȡÿ��ȡ�ļ���ʱ����
// ԭ�� : TCString GetInterval();
// ���� : ��
// ���� : ʱ��������Ϊ��λ();
// ˵�� :
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
// ���� : TCFileTrans::GetHostName
// ��; : ��ȡ��������IP
// ԭ�� : TCString GetHostName();
// ���� : ��
// ���� : ������;
// ˵�� :
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
// ���� : TCFileTrans::GetUserName
// ��; : ��ȡ�û���
// ԭ�� : TCString GetUserName();
// ���� : ��
// ���� : �û���;
// ˵�� :
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
// ���� : TCFileTrans::GetPassword
// ��; : ��ȡ����
// ԭ�� : TCString GetPassword();
// ���� : ��
// ���� : ����;
// ˵�� :
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
// ���� : TCFileTrans::GetFileFilter
// ��; : �ļ��������ַ���
// ԭ�� : TCString GetFileFilter();
// ���� : ��
// ���� : �ļ��������ַ���;
// ˵�� :
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
// ���� : TCFileTrans::GetLastFileDate
// ��; : ȡ�����һ���ɼ��ļ���ʱ���
// ԭ�� : TCString GetLastFileDate();
// ���� : ��
// ���� : ʱ���;
// ˵�� :
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
// ���� : TCFileTrans::GetTransMode
// ��; : ��ȡ����ģʽ
// ԭ�� : TCString GetTransMode();
// ���� : ��
// ���� : ��ȡ����ģʽ
// ˵�� :
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
// ���� : TCFileTrans::GetTmpDir
// ��; : ��ȡ��ʱĿ¼
// ԭ�� : TCString GetTmpDir();
// ���� : ��
// ���� : ��ȡ��ʱĿ¼
// ˵�� :
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
// ���� : TCFileTrans::IsAccordFileDate
// ��; : �Ƿ������ļ����е����ھ����Ƿ�ȡ���ļ�
// ԭ�� : bool IsAccordFileDate();
// ���� : ��
// ���� : �Ƿ��ļ����е�����
// ˵�� : �������ĺͶ������صĲɼ���Ҫ���ļ����е�ʱ��
//        �жϸ��ļ��Ƿ��ѱ��ɼ�
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
// ���� : TCFileTrans::IsNeedAddList
// ��; : �Ƿ���Ҫ���ļ������뵽LIST
// ԭ�� : bool IsNeedAddList();
// ���� : ��
// ���� : �Ƿ����LIST
// ˵�� :
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
// ���� : TCFileTrans::IsNeedDeleteSource
// ��; : �Ƿ���Ҫɾ��Դ�ļ�
// ԭ�� : bool IsNeedDeleteSource();
// ���� : ��
// ���� : �Ƿ���ɾ��Դ�ļ�
// ˵�� :
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
// ���� : TCFileTrans::IsNeedUncompress
// ��; : �Ƿ���Ҫ��ѹ
// ԭ�� : bool IsNeedUncompress();
// ���� : ��
// ���� : �Ƿ���Ҫ��ѹ
// ˵�� :
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
// ���� : TCFileTrans::IsNeedSrcBackUp
// ��; : �Ƿ���Ҫ����ԭ�ļ�
// ԭ�� : bool IsNeedSrcBackUp();
// ���� : ��
// ���� : �Ƿ���Ҫ��ѹ
// ˵�� :
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
// ���� : TCFileTrans::UploadOneFile
// ��; : �ϴ�һ���ļ�
// ԭ�� : TCString UploadOneFile();
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : TCFileTrans::WriteFileDate
// ��; : ��¼�ɼ��ļ���ʱ���
// ԭ�� : TCString WriteFileDate();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCFileTrans::WriteFileDate()
{

    //====== 1. �õ���һ���������� =======
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
// ���� : TCFileTrans::WriteFileTranLog
// ��; : ��¼������־��
// ԭ�� : TCString WriteFileTranLog();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCFileTrans::WriteFileTranLog(TCString sFileName, long nFileSize,
        TCString dtStartTime, TCString dtStopTime)
{
    TCString sLogFileName;
    TCString sDailyLogFile;
    //======= 1. �����־�ļ�������, �򴴽�֮ ==============
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

    //====== 2. д��־�ļ� ========
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
// ���� : DealSubdirectory
// ��; : �Ƿ�����Ŀ¼��
// ԭ�� : bool DealSubdirectory();
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : IsAlertNoFile
// ��; : �Ƿ���һ��Сʱû�вɼ����ļ�������
// ԭ�� : bool IsAlertNoFile();
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : GetFtpFileNameAndSubDir
// ��; : �õ�Ҫ�ɼ��ġ�
// ԭ�� : bool GetFtpFileNameAndSubDir();
// ���� : ��
// ���� : ��
// ˵�� :
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
    	        throw TCException("�ɼ�����" + DestFile + ""
    	            "�ļ�������");
    	    }
    	    else
    	    {
    	        m_sSubDir[i] = ProfileString(DestFile, "sub_dir", "dir_name","");
    	        m_sFileNamePostfix[i] = ProfileString(DestFile, "file_name", "postfix","");
    	    }
    	}
}
//==========================================================================
// ���� : UpdateFtpFileNameAndSubDir
// ��; : �õ�Ҫ�ɼ��ġ�
// ԭ�� : bool UpdateFtpFileNameAndSubDir();
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : GetFtpFileName
// ��; : �Ƿ���вɼ���
// ԭ�� : long IsGetFile();
// ���� : ����˵��ļ��б����ؼ�¼��Ҫ�ɼ����ļ�
// ���� : Ҫ�ɼ��ļ�������
// ˵�� :
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
// ���� : GetFtpSubDir
// ��; : �Ƿ���вɼ���
// ԭ�� : long GetFtpSubDir();
// ���� : ����˵���Ŀ¼�б����ؼ�¼��Ҫ�ɼ�����Ŀ¼
// ���� : Ҫ�ɼ���Ŀ¼������
// ˵�� :
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
// ���� : CheckFtpFileName
// ��; : �õ��ɼ����ļ�����
// ԭ�� : long CheckFtpFileName(TCString sFileName,long nPostfixLength);
// ���� : Ҫ�ɼ����ļ����ļ��ĺ�׺�ĳ���
// ���� : �Ƿ�Ϊ��ȷ�ļ�
// ˵�� :
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
// ���� : GetSubDirFileName
// ��; : �Ƿ���вɼ���
// ԭ�� : long GetSubDirFileName();
// ���� : Ҫ�ɼ����ļ����ļ��ĺ�׺
// ���� : �Ƿ�Ϊ��ȷ�ļ�
// ˵�� :
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
// ���� : IsFtpAll
// ��; : �Ƿ���вɼ���
// ԭ�� : long IsFtpAll();
// ���� : Ҫ�ɼ����ļ����ļ��ĺ�׺
// ���� : �Ƿ�Ϊ��ȷ�ļ�
// ˵�� :
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
// ���� : IntToStrn
// ��; : ת�������ַ���
// ԭ�� : long IntToStrn();
// ���� : Ҫ�ɼ����ļ����ļ��ĺ�׺
// ���� : �Ƿ�Ϊ��ȷ�ļ�
// ˵�� :
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

