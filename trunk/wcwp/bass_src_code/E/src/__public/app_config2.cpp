//---------------------------------------------------------------------------

#pragma hdrstop

#include "cmpublic.h"

#include "app_config.h"
//---------------------------------------------------------------------------
static TCString STA__sRunBinPath = "__UNKW__";

const long SUPER_CONFIG_PATH_MAX_COUNT = 8;                 // �������Ŀ¼��
long     STA__nAPLSuperConfigPathsCount = -1;               // ����Ŀ¼������
TCString STA__sAPLSuperConfigPaths[SUPER_CONFIG_PATH_MAX_COUNT];  // ����Ŀ¼

static bool STA__bPathLinkExistsRunFirstTime = true;

static TCString STA__sAppTempRoot = "__UNKW__";
static TCString STA__sLastAppTempReturn = "__UNKW__";

static bool STA__bAppLogRunFirstTime = true;
static bool STA__bAppRunningInfoRunFirstTime = true;

static TCString STA__sAppRoot = "__UNKW__";

static TCString STA__sLastAppName = "__UNKW__";
static TCString STA__sLastAppConfigFileName = "__UNKW__";

static TCString STA__sGeneralConfigFileName = "__UNKW__";

//==========================================================================
// ���� : TAppPathLinks::GetRunBinPath
// ��; : �õ���ִ���ļ���Ŀ¼
// ԭ�� : static TCString GetRunBinPath();
// ���� : ��
// ���� : ��ִ���ļ���Ŀ¼, ���û�з���""
// ˵�� : ����λ��Ϊ path_links.ini => run_bin_path => directory
//        ��ִ���ļ�һ����� <MI_ROOT_DIR>/binĿ¼�£�����ô�û�У���ɴ�
//        path_links.ini���ҵ�������
//==========================================================================
TCString TAppPathLinks::GetRunBinPath()
{
//    static TCString STA__sRunBinPath = "__UNKW__";

    if (STA__sRunBinPath == "__UNKW__" )
    {
        if (!PathLinkExists())
            STA__sRunBinPath = "";
        else
            STA__sRunBinPath = ProfileAppString("path_links", "run_bin_path",
                    "directory", "");

        if (STA__sRunBinPath != "")
            STA__sRunBinPath = IncludeTrailingSlash(STA__sRunBinPath);
    }

    return STA__sRunBinPath;
}


//==========================================================================
// ���� : TAppPathLinks::GetSuperConfigPath
// ��; : �õ���������Ŀ¼
// ԭ�� : static TCString TAppPathLinks::GetSuperConfigPath(long nPathSeq);
// ���� : Ŀ¼���(1 ~ 8)
// ���� : ��������Ŀ¼�����û�з���""
// ˵�� : ����λ��Ϊ path_links.ini => super_config_paths => directory<n>
//                                1 <= n <= SUPER_CONFIG_PATH_MAX_COUNT
//==========================================================================
TCString TAppPathLinks::GetSuperConfigPath(long nPathSeq)
{
    ASSERT(nPathSeq >= 1);

    if (nPathSeq > GetSuperConfigPathsCount())
        return "";
    else
        return STA__sAPLSuperConfigPaths[nPathSeq - 1];
}

//==========================================================================
// ���� : TAppPathLinks::GetSuperConfigPathsCount
// ��; : �õ���������Ŀ¼������
// ԭ�� : static long GetSuperConfigPathsCount();
// ���� : ��
// ���� : ��������Ŀ¼������
// ˵�� : �ú���ȡ�����е�����Ŀ¼����̬������
//==========================================================================
long TAppPathLinks::GetSuperConfigPathsCount()
{
    if (STA__nAPLSuperConfigPathsCount != -1 )
        return STA__nAPLSuperConfigPathsCount;

    STA__nAPLSuperConfigPathsCount = 0;

    long i;

    for (i = 1; i <= SUPER_CONFIG_PATH_MAX_COUNT; i++)
    {
        STA__sAPLSuperConfigPaths[i - 1] = ProfileAppString("path_links",
                "super_config_paths", "directory" + IntToStr(i), "");
        if (STA__sAPLSuperConfigPaths[i - 1] != "")
        {
            STA__nAPLSuperConfigPathsCount = i;
            STA__sAPLSuperConfigPaths[i - 1]
                    = IncludeTrailingSlash(STA__sAPLSuperConfigPaths[i - 1]);
        }
        else
            break;
    }

    return STA__nAPLSuperConfigPathsCount;
}

//==========================================================================
// ���� : TAppPathLinks::PathLinkExists
// ��; : �ж�����"path_links"����
// ԭ�� : static bool PathLinkExists();
// ���� : ��
// ���� : ����"path_links"����
// ˵�� :
//==========================================================================
bool TAppPathLinks::PathLinkExists()
{
//    static bool STA__bPathLinkExistsRunFirstTime = true;
    static bool STA__bPathLinkExists = false;

    if (STA__bPathLinkExistsRunFirstTime )
    {
        STA__bPathLinkExistsRunFirstTime = false;
        STA__bPathLinkExists = false;
        if (FileExists(MergePathAndFile(TAppPath::AppConfig(),
                "path_links.ini")))
            STA__bPathLinkExists = true;
    }

    return STA__bPathLinkExists;
}

//==========================================================================
// ���� : TAppPath::AppConfig
// ��; : �õ������ļ���ŵ�Ŀ¼
// ԭ�� : static TCString AppConfig();
// ���� : ��
// ���� : �����ļ���ŵ�Ŀ¼
// ˵�� : һ����Ӧ�ø�Ŀ¼�·���"config"Ŀ¼��Ϊ�����ļ���ŵ�Ŀ¼
//==========================================================================
TCString TAppPath::AppConfig()
{
#ifdef __WIN32__
    return AppRoot() + "config\\";
#else
    return AppRoot() + "config/" ;
#endif
}

//==========================================================================
// ���� : TAppPath::AppTemp
// ��; : �õ���ʱ�ļ���ŵ�Ŀ¼
// ԭ�� : static TCString AppTemp(TCString sSubDirName = "");
// ���� : sSubDirName -- ��Ŀ¼��, ȱʡʱ����Ŀ¼��ΪӦ����
// ���� : ��ʱ�ļ���ŵ�Ŀ¼ = ��ʱĿ¼ / ��Ŀ¼��
// ˵�� : ��ʱĿ¼ͨ����������MI_TEMP_DIR�õ�
// ��ʷ : 2001.11.14 ����������̬������ʹ��������һ��Ч�ʣ��Ҳ������Ƿ���
//                   ���������������ڻ��������ױ�ĳ��ϡ�
//        2001.11.29 �������������ļ���ȡ�Ĵ���
//==========================================================================
TCString TAppPath::AppTemp(TCString sSubDirName)
{
//    static TCString STA__sAppTempRoot = "__UNKW__";
//    static TCString STA__sLastAppTempReturn = "__UNKW__";

    //======= 1. ���η���ʱ��ͨ���������������ʱĿ¼�ĸ�Ŀ¼ =====
    if (STA__sAppTempRoot == "__UNKW__" )
    {
        STA__sAppTempRoot = GetAppConfigParm("��ʱ�ļ�Ŀ¼");

        if (STA__sAppTempRoot == "")
        {
#ifdef __WIN32__
            STA__sAppTempRoot = MergePathAndFile(AppRoot(), "mid_files");
#else
            char *pStr ;
            pStr = getenv("MI_TEMP_DIR");
            if ( pStr == NULL )
                ExitWithSystemFailLog("getenv(\"MI_TEMP_DIR\") fail!\n", "AppTemp");

            STA__sAppTempRoot = pStr;
            STA__sAppTempRoot = IncludeTrailingSlash(STA__sAppTempRoot);
#endif
        }

        ASSERT(STA__sAppTempRoot != "");
    }

    //====== 2. ���ص�Ŀ¼Ϊ��ʱĿ¼�ĸ�Ŀ¼�ټ���ָ������Ŀ¼ ========
    TCString sRet;

    if (sSubDirName == "")
        sSubDirName = Application.GetAppName();

    sRet = IncludeTrailingSlash(MergePathAndFile(STA__sAppTempRoot,
            sSubDirName));

    //===== 3. ���û�н���Ŀ¼���򴴽�֮ =========
    if (sRet != STA__sLastAppTempReturn)
    {
        STA__sLastAppTempReturn = sRet;
        ForceDirectories(sRet);
    }

    return sRet;
}

//==========================================================================
// ���� : TAppPath::AppLog
// ��; : �õ���־�ļ���ŵ�Ŀ¼
// ԭ�� : static TCString AppLog();
// ���� : ��
// ���� : ��־�ļ���ŵ�Ŀ¼
// ˵�� : һ����Ӧ�ø�Ŀ¼�·���"log"Ŀ¼��Ϊ�����ļ���ŵ�Ŀ¼
// ��ʷ : 2001.11.14 ���Ӿ�̬����
//==========================================================================
TCString TAppPath::AppLog()
{
//    static bool STA__bAppLogRunFirstTime = true;

    TCString sRet;
#ifdef __WIN32__
    sRet = AppRoot() + "log\\";
#else
    sRet = AppRoot() + "log/";
#endif
    if (STA__bAppLogRunFirstTime)
    {
        STA__bAppLogRunFirstTime = false;
        ForceDirectories(sRet);
    }

    return sRet;
}

//==========================================================================
// ���� : TAppPath::AppRunningInfo
// ��; : �õ��ļ�������Ϣ��ŵ�Ŀ¼, ��Ŀ¼��������ͣ���ƣ�ʱ�����
// ԭ�� : static TCString AppRunningInfo();
// ���� : ��
// ���� : �ļ�������Ϣ��ŵ�Ŀ¼
// ˵�� : һ����Ӧ������Ŀ¼�·���"running_info"Ŀ¼��Ϊ������ϢĿ¼
// ��ʷ : 2001.11.14 ���Ӿ�̬����
//==========================================================================
TCString TAppPath::AppRunningInfo()
{
//    static bool STA__bAppRunningInfoRunFirstTime = true;

    TCString sRet;
#ifdef __WIN32__
    sRet = AppConfig() + "running_info\\";
#else
    sRet = AppConfig() + "running_info/";
#endif

    if (STA__bAppRunningInfoRunFirstTime)
    {
        STA__bAppRunningInfoRunFirstTime = false;
        ForceDirectories(sRet);
    }

    return sRet;
}

//==========================================================================
// ���� : TAppPath::AppRoot
// ��; : �õ�Ӧ�����еĸ�Ŀ¼
// ԭ�� : static TCString AppRoot();
// ���� : ��
// ���� : Ӧ�����еĸ�Ŀ¼
// ˵�� : ��ʱĿ¼ͨ����������MI_ROOT_DIR�õ�
// ��ʷ : 2001.11.14 ���Ӿ�̬����
//==========================================================================
TCString TAppPath::AppRoot()
{
//    static TCString STA__sAppRoot = "__UNKW__";

    if (STA__sAppRoot == "__UNKW__")
    {
#ifdef __WIN32__
        STA__sAppRoot = "\\data2\\roaming_gw\\";
#else
        char *pStr ;
        pStr = getenv("MI_ROOT_DIR");
        if ( pStr == NULL )
            ExitWithSystemFailLog("getenv(\"MI_ROOT_DIR\") fail!\n", "AppRoot");

        STA__sAppRoot = pStr;
        STA__sAppRoot = IncludeTrailingSlash(STA__sAppRoot);
#endif
    }

    return STA__sAppRoot;
}

//==========================================================================
// ���� : TAppPath::GetConfigDestDirectory
// ��; : �õ����õ�Ŀ��Ŀ¼
// ԭ�� : static TCString GetConfigDestDirectory();
// ���� : ��
// ���� : Ŀ��Ŀ¼
// ˵�� : ������Ŀ¼�µĵ�ǰ���г������������ļ��У�ȡdest -> directory��
//==========================================================================
TCString TAppPath::GetConfigDestDirectory()
{
    static TCString s_sDestDir;

    if (s_sDestDir == "")
    {
        s_sDestDir = ProfileAppString(Application.GetAppName(), "dest",
                "directory", "");

        if (s_sDestDir == "")
            throw TCException("AppPath - DEST DIRECTORY NOT SET");

        ForceDirectories(s_sDestDir);
    }

    return s_sDestDir;
}

//==========================================================================
// ���� : TAppPath::GetConfigSourceDirectory
// ��; : �õ����õ�ԴĿ¼
// ԭ�� : static TCString GetConfigSourceDirectory();
// ���� : ��
// ���� : ԴĿ¼
// ˵�� : ������Ŀ¼�µĵ�ǰ���г������������ļ��У�ȡsource -> directory��
//==========================================================================
TCString TAppPath::GetConfigSourceDirectory()
{
    static TCString s_sSourceDir;

    if (s_sSourceDir == "")
    {
        s_sSourceDir = ProfileAppString(Application.GetAppName(), "source",
                "directory", "");

        if (s_sSourceDir == "")
            throw TCException("AppPath - SOURCE DIRECTORY NOT SET");

        ForceDirectories(s_sSourceDir);
    }

    return s_sSourceDir;
}

//==========================================================================
// ���� : TAppFile::ApplicationConfig
// ��; : �õ�Ӧ�õ������ļ���
// ԭ�� : static TCString ApplicationConfig(TCString sAppName = "");
// ���� : Ӧ�����ƣ�ȱʡʱȡ��Ӧ������
// ���� : Ӧ�õ������ļ���
// ˵�� : Ӧ�õ������ļ�����ȡ��������Ŀ¼�µ��ļ���������ļ������ڣ�������
//        path_links.ini�е�������ӡ�
// ��ʷ : 2001.11.15 ���뾲̬�����������ϴ���ʷƥ��, ���������׳�
//==========================================================================
TCString TAppFile::ApplicationConfig(TCString sAppName)
{
//    static TCString STA__sLastAppName = "__UNKW__";
//    static TCString STA__sLastAppConfigFileName = "__UNKW__";

    if (sAppName == "")
        sAppName = Application.GetAppName();

    //======== 1. ������ϴεĲ�����ͬ����ֱ��ȡ���ϴεĽ�� =====
    if (sAppName == STA__sLastAppName)
        return STA__sLastAppConfigFileName;

    //======= 2. �õ����������ļ� ========
    TCString sConfigFileName;
    sConfigFileName = TAppPath::AppConfig() + sAppName + ".ini";
    if (FileExists(sConfigFileName))
    {
        STA__sLastAppName = sAppName;
        STA__sLastAppConfigFileName = sConfigFileName;
        return sConfigFileName;
    }

    //======= 3. �������������ļ� ========
    long i;
    TCString sSuperConfigFileName;
    for (i = 1; i <= TAppPathLinks::GetSuperConfigPathsCount(); i++)
    {
        sSuperConfigFileName = TAppPathLinks::GetSuperConfigPath(i)
                + sAppName + ".ini";
        if (FileExists(sSuperConfigFileName))
        {
            STA__sLastAppName = sAppName;
            STA__sLastAppConfigFileName = sSuperConfigFileName;
            return sSuperConfigFileName;
        }
    }

    //======= 4. ��������ļ������ڣ����׳����� =========
    throw TCException("TAppFile::ApplicationConfig() : "
                "Config File Not Found - " + sAppName + ".ini");
}

//==========================================================================
// ���� : TAppFile::GatherGeneralConfig
// ��; : �õ�ͨ�������ļ���
// ԭ�� : static TCString GatherGeneralConfig();
// ���� : ��
// ���� : ͨ�������ļ���
// ˵�� :
//==========================================================================
TCString TAppFile::GatherGeneralConfig()
{
//    static TCString STA__sGeneralConfigFileName = "__UNKW__";

    if (STA__sGeneralConfigFileName = "__UNKW__")
        STA__sGeneralConfigFileName = ApplicationConfig("mig_config");

    return STA__sGeneralConfigFileName;
}

//==========================================================================
// ���� : TAppConfig::IsInLocalMode
// ��; : �õ��Ƿ񱾵�����ģʽ
// ԭ�� : static bool IsInLocalMode();
// ���� : ��
// ���� : �Ƿ񱾵�����ģʽ
// ˵�� : 1. ��ȡmig_config.ini => general => IN_LOCAL_MODE�����ж�
//        2. ���Ǳ�������ģʽ���������ݿ⣬ֱ�Ӵӱ���DBF�ļ���ȡ����
//==========================================================================
bool TAppConfig::IsInLocalMode()
{
    static bool s_bFirstTime = true;
    static bool s_bIsInLocalMode;

    if (s_bFirstTime)
    {
        s_bFirstTime = false;
        s_bIsInLocalMode = ProfileBool(TAppFile::GatherGeneralConfig(),
                        "GENERAL", "IN_LOCAL_MODE", false);
    }

    return s_bIsInLocalMode;
}

//==========================================================================
// ���� : TAppConfig::IsNationalCenter
// ��; : �ж��Ƿ���ȫ����������
// ԭ�� : static bool IsNationalCenter();
// ���� : ��
// ���� : �Ƿ�ȫ������
// ˵�� : ��ȡmig_config.ini => general => SCP_PROV_CODE�����ж�
//==========================================================================
bool TAppConfig::IsNationalCenter()
{
    static bool s_bFirstTime = true;
    static bool bIsNationalCenter;

    if (s_bFirstTime)
    {
        TCString sSCPProvCode;
        s_bFirstTime = false;

        sSCPProvCode = ProfileString(TAppFile::GatherGeneralConfig(),
                        "GENERAL", "SCP_PROV_CODE", "");

        bIsNationalCenter = false;

        if (Length(sSCPProvCode) != 3)
            bIsNationalCenter = true;

        if (sSCPProvCode == "000")
            bIsNationalCenter = true;
    }

    return bIsNationalCenter;
}

//==========================================================================
// ���� : TAppConfig::VerifyProvinceCode
// ��; : ��֤ʡ�������ȷ��
// ԭ�� : static void VerifyProvinceCode(TCString sProvinceCode);
// ���� : ʡ����
// ���� : ��
// ˵�� : ���������ȷ��ʡ�������׳�����
//==========================================================================
void TAppConfig::VerifyProvinceCode(TCString sProvinceCode)
{
    const long MAX_PROVINCE_COUNT = 31;

    static long s_ProvinceList[MAX_PROVINCE_COUNT]
            = { 100/*����*/, 200/*�㶫*/, 210/*�Ϻ�*/, 220/*���*/,
                230/*����*/, 240/*����*/, 250/*����*/, 270/*����*/,
                280/*�Ĵ�*/, 290/*����*/, 311/*�ӱ�*/, 351/*ɽ��*/,
                371/*����*/, 431/*����*/, 451/*������*/, 471/*���ɹ�*/,
                531/*ɽ��*/, 551/*����*/, 571/*�㽭*/, 591/*����*/,
                731/*��ɳ*/, 771/*����*/, 791/*����*/, 851/*����*/,
                871/*����*/, 891/*����*/, 898/*����*/, 931/*����*/,
                951/*����*/, 971/*�ຣ*/, 991/*�½�*/
            };

    long i;
    long nProvinceNumber;

    nProvinceNumber = StrToInt(sProvinceCode);

    for (i = 0; i < MAX_PROVINCE_COUNT; i++)
        if (nProvinceNumber == s_ProvinceList[i])
            return;

    throw TCException("TAppConfig::VerifyProvinceCode() : "
            + sProvinceCode + " May Be Incorrect Province Code.");
}

//==========================================================================
// ���� : GetAppConfigBool
// ��; : �õ��������в����Ĳ���ֵ��ʾ
// ԭ�� : bool GetAppConfigBool(TCString sParmName);
// ���� : �������崮
// ���� : ���в����Ĳ���ֵ��ʾ
// ˵�� : Ϊ�ɶ���Ҫ�󣬲������崮Ҫ���Ժ��ֶ���
//==========================================================================
bool GetAppConfigBool(TCString sParmName)
{
    if (sParmName == "���ݿ������־")
    {
        static bool s_bFirstTime = true;
        static bool s_bWriteDBLog;

        if (s_bFirstTime)
        {
            s_bFirstTime = false;
            s_bWriteDBLog = ProfileBool(TAppFile::GatherGeneralConfig(),
                    "Database", "WRITE_DB_LOG", true);
        }

        return s_bWriteDBLog;
    }

    throw TCException("GetAppConfigBool() : Parameter cannot matched.  "
            "ParmName : " + sParmName);
}

//==========================================================================
// ���� : GetAppConfigParm
// ��; : �õ��������в���
// ԭ�� : TCString GetAppConfigParm(TCString sParmName);
// ���� : �������崮
// ���� : ����ֵ��
// ˵�� : Ϊ�ɶ���Ҫ�󣬲������崮Ҫ���Ժ��ֶ���
//==========================================================================
TCString GetAppConfigParm(TCString sParmName)
{
    if (sParmName == "δ���λ���ͨ����")
        return "600";

    if (sParmName == "���λ���ͨ����")
        return "800";

    if (sParmName == "�����ļ�Ŀ¼")
        return TAppPath::AppConfig();

    if (sParmName == "��ʱ�ļ�Ŀ¼")
    {
        static TCString STA__TempDir = "__UNKW__";
        if (STA__TempDir == "__UNKW__")
            STA__TempDir = ProfileString(TAppFile::GatherGeneralConfig(),
                    "temp_directory", "directory", "");

        return STA__TempDir;
    }

    if (sParmName == "�ɼ���ͨ������")
        return TAppFile::GatherGeneralConfig();

    if (sParmName == "�ɼ�������Ŀ¼")
    {
        static TCString sWorkDataDir;
        if (sWorkDataDir == "")
            sWorkDataDir = ProfileString(TAppFile::GatherGeneralConfig(),
                    "DISK_MONITOR", "WORK_DATA_DIR", "/data");

        return sWorkDataDir;
    }

    if (sParmName == "�ɼ���ֹͣ���пռ�ٷֱ�")
    {
        static TCString s_sLeastFreePercent;

        if (s_sLeastFreePercent == "")
            s_sLeastFreePercent = ProfileString(TAppFile::GatherGeneralConfig(),
                    "DISK_MONITOR", "STOP_FREE_PERCENT", "8");

        return s_sLeastFreePercent;
    }

    if (sParmName == "�ɼ�����ͣ���пռ�ٷֱ�")
    {
        static TCString s_sPauseFreePercent;

        if (s_sPauseFreePercent == "")
            s_sPauseFreePercent = ProfileString(TAppFile::GatherGeneralConfig(),
                    "DISK_MONITOR", "PAUSE_FREE_PERCENT", "16");

        return s_sPauseFreePercent;
    }

    if (sParmName == "���ݿ��������")
        return ProfileString(TAppFile::GatherGeneralConfig(),
                "DATABASE", "ServerName", "");

    if (sParmName == "���ݿ��¼�û�")
        return ProfileString(TAppFile::GatherGeneralConfig(),
                "DATABASE", "LogID", "");

    if (sParmName == "���ݿ��¼����")
        return ProfileString(TAppFile::GatherGeneralConfig(),
                "DATABASE", "LogPassword", "");

    if (sParmName == "���ݿ����ݿ���")
        return ProfileString(TAppFile::GatherGeneralConfig(),
                "DATABASE", "Database", "");

    throw TCException("GetAppConfigParm() : Parameter cannot matched.  "
            "ParmName : " + sParmName);
}

//==========================================================================
// ���� : ProfileAppString
// ��; : �õ�Ӧ�����õ��ַ���ֵ
// ԭ�� : TCString ProfileAppString(TCString sAppName, TCString sSection,
//              TCString sIdent, TCString sDefault);
// ���� : Ӧ���������ö��������ùؼ��֣�����ȱʡֵ
// ���� : ���õ��ַ���ֵ
// ˵�� :
//==========================================================================
TCString ProfileAppString(TCString sAppName, TCString sSection, TCString sIdent,
        TCString sDefault)
{
    TCString sFlag;
    sFlag = Application.GetProcessFlag();
    if( sFlag != "" )
    {
       sFlag = sSection+"*{"+sFlag+"}*";
       sFlag = ProfileString(TAppFile::ApplicationConfig(sAppName),
                            sFlag, sIdent, sDefault) ;
       if( sFlag != sDefault )
           return sFlag;
    }
    return ProfileString(TAppFile::ApplicationConfig(sAppName),
             sSection, sIdent, sDefault);
}

//==========================================================================
// ���� : ProfileAppBool
// ��; : �õ�Ӧ�����õĲ���ֵ
// ԭ�� : bool ProfileAppBool(TCString sAppName, TCString sSection,
//              TCString sIdent, bool bDefault, bool bThrowException = false);
// ���� : Ӧ���������ö��������ùؼ��֣�����ȱʡֵ, ���û��ƥ���Ƿ��׳�����
// ���� : ���õĲ���ֵ
// ˵�� : ���ڲ���ֵֻ��������ȡֵ���������Ϳ���ȡһ����ʾ����û�����õ�ֵ��
//        "-1", �ַ����Ϳ���ȡһ����ʾ����û�����õĴ�����������һ���׳�����
//        �Ĳ�����������û������ʱֱ���׳����⡣
//==========================================================================
bool ProfileAppBool(TCString sAppName, TCString sSection, TCString sIdent,
        bool bDefault, bool bThrowException)
{
    bool bRet;
    TCString sFlag;
    sFlag = Application.GetProcessFlag();
    if( sFlag != "" )
    {
       sFlag = sSection+"*{"+sFlag+"}*";
       bRet = ProfileBool(TAppFile::ApplicationConfig(sAppName),
                            sFlag, sIdent, bDefault) ;
       if( bRet != bDefault )
           return bRet;
    }
    return ProfileBool(TAppFile::ApplicationConfig(sAppName),
            sSection, sIdent, bDefault, bThrowException);
}

//==========================================================================
// ���� : ProfileAppInt
// ��; : �õ�Ӧ�����õ�����ֵ
// ԭ�� : long ProfileAppInt(TCString sAppName, TCString sSection,
//                  TCString sIdent, long nDefault);
// ���� : Ӧ���������ö��������ùؼ��֣�����ȱʡֵ
// ���� : ���õ�����ֵ
// ˵�� :
//==========================================================================
long ProfileAppInt(TCString sAppName, TCString sSection, TCString sIdent,
        long nDefault)
{
    long nRet;
    TCString sFlag;
    sFlag = Application.GetProcessFlag();
    if( sFlag != "" )
    {
       sFlag = sSection+"*{"+sFlag+"}*";
       nRet = ProfileInt(TAppFile::ApplicationConfig(sAppName),
                            sFlag, sIdent, nDefault) ;
       if( nRet != nDefault )
           return nRet;
    }
    return ProfileInt(TAppFile::ApplicationConfig(sAppName),
            sSection, sIdent, nDefault);
}

//==========================================================================
// ���� : ProfileAppSession
// ��; : �õ�Ӧ�����õĶ��еĹؼ����б�
// ԭ�� : void ProfileAppSession(TCString sAppName, TCString sSection,
//                  TCStringList& pStrings);
// ���� : Ӧ���������ö������ؼ����б������
// ���� : ��
// ˵�� :
//==========================================================================
void ProfileAppSession(TCString sAppName, TCString sSection,
        TCStringList& pStrings)
{
    TCString sFlag;
    sFlag = Application.GetProcessFlag();
    if( sFlag != "" )
    {
       sFlag = sSection+"*{"+sFlag+"}*";
       ProfileSession(TAppFile::ApplicationConfig(sAppName),
                      sFlag,pStrings);
       if( pStrings.GetCount()>0 )
           return ;
    }
    ProfileSession(TAppFile::ApplicationConfig(sAppName),
                sSection,pStrings);
}

