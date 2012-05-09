//---------------------------------------------------------------------------

#pragma hdrstop

#include "cmpublic.h"

#include "app_config.h"
//---------------------------------------------------------------------------
static TCString STA__sRunBinPath = "__UNKW__";

const long SUPER_CONFIG_PATH_MAX_COUNT = 8;                 // 最多配置目录数
long     STA__nAPLSuperConfigPathsCount = -1;               // 配置目录的数量
TCString STA__sAPLSuperConfigPaths[SUPER_CONFIG_PATH_MAX_COUNT];  // 配置目录

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
// 函数 : TAppPathLinks::GetRunBinPath
// 用途 : 得到可执行文件的目录
// 原型 : static TCString GetRunBinPath();
// 参数 : 无
// 返回 : 可执行文件的目录, 如果没有返回""
// 说明 : 放置位置为 path_links.ini => run_bin_path => directory
//        可执行文件一般放在 <MI_ROOT_DIR>/bin目录下，如果该处没有，则可从
//        path_links.ini中找到其所在
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
// 函数 : TAppPathLinks::GetSuperConfigPath
// 用途 : 得到其它配置目录
// 原型 : static TCString TAppPathLinks::GetSuperConfigPath(long nPathSeq);
// 参数 : 目录序号(1 ~ 8)
// 返回 : 其它配置目录，如果没有返回""
// 说明 : 放置位置为 path_links.ini => super_config_paths => directory<n>
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
// 函数 : TAppPathLinks::GetSuperConfigPathsCount
// 用途 : 得到其它配置目录的数量
// 原型 : static long GetSuperConfigPathsCount();
// 参数 : 无
// 返回 : 其它配置目录的数量
// 说明 : 该函数取出所有的配置目录到静态数组中
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
// 函数 : TAppPathLinks::PathLinkExists
// 用途 : 判断有无"path_links"设置
// 原型 : static bool PathLinkExists();
// 参数 : 无
// 返回 : 有无"path_links"设置
// 说明 :
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
// 函数 : TAppPath::AppConfig
// 用途 : 得到配置文件存放的目录
// 原型 : static TCString AppConfig();
// 参数 : 无
// 返回 : 配置文件存放的目录
// 说明 : 一般是应用根目录下放置"config"目录做为配置文件存放的目录
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
// 函数 : TAppPath::AppTemp
// 用途 : 得到临时文件存放的目录
// 原型 : static TCString AppTemp(TCString sSubDirName = "");
// 参数 : sSubDirName -- 子目录名, 缺省时，子目录名为应用名
// 返回 : 临时文件存放的目录 = 临时目录 / 子目录名
// 说明 : 临时目录通过环境变量MI_TEMP_DIR得到
// 历史 : 2001.11.14 增加两个静态变量，使处理增加一点效率，且不用总是访问
//                   环境变量，可用于环境变量易变的场合。
//        2001.11.29 增加先在配置文件读取的处理
//==========================================================================
TCString TAppPath::AppTemp(TCString sSubDirName)
{
//    static TCString STA__sAppTempRoot = "__UNKW__";
//    static TCString STA__sLastAppTempReturn = "__UNKW__";

    //======= 1. 初次访问时，通过环境变量获得临时目录的根目录 =====
    if (STA__sAppTempRoot == "__UNKW__" )
    {
        STA__sAppTempRoot = GetAppConfigParm("临时文件目录");

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

    //====== 2. 返回的目录为临时目录的根目录再加上指定的子目录 ========
    TCString sRet;

    if (sSubDirName == "")
        sSubDirName = Application.GetAppName();

    sRet = IncludeTrailingSlash(MergePathAndFile(STA__sAppTempRoot,
            sSubDirName));

    //===== 3. 如果没有建过目录，则创建之 =========
    if (sRet != STA__sLastAppTempReturn)
    {
        STA__sLastAppTempReturn = sRet;
        ForceDirectories(sRet);
    }

    return sRet;
}

//==========================================================================
// 函数 : TAppPath::AppLog
// 用途 : 得到日志文件存放的目录
// 原型 : static TCString AppLog();
// 参数 : 无
// 返回 : 日志文件存放的目录
// 说明 : 一般是应用根目录下放置"log"目录做为配置文件存放的目录
// 历史 : 2001.11.14 增加静态变量
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
// 函数 : TAppPath::AppRunningInfo
// 用途 : 得到文件运行信息存放的目录, 该目录可用于启停控制，时间监测等
// 原型 : static TCString AppRunningInfo();
// 参数 : 无
// 返回 : 文件运行信息存放的目录
// 说明 : 一般是应用配置目录下放置"running_info"目录做为运行信息目录
// 历史 : 2001.11.14 增加静态变量
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
// 函数 : TAppPath::AppRoot
// 用途 : 得到应用运行的根目录
// 原型 : static TCString AppRoot();
// 参数 : 无
// 返回 : 应用运行的根目录
// 说明 : 临时目录通过环境变量MI_ROOT_DIR得到
// 历史 : 2001.11.14 增加静态变量
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
// 函数 : TAppPath::GetConfigDestDirectory
// 用途 : 得到配置的目标目录
// 原型 : static TCString GetConfigDestDirectory();
// 参数 : 无
// 返回 : 目标目录
// 说明 : 在配置目录下的当前运行程序名的配置文件中，取dest -> directory。
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
// 函数 : TAppPath::GetConfigSourceDirectory
// 用途 : 得到配置的源目录
// 原型 : static TCString GetConfigSourceDirectory();
// 参数 : 无
// 返回 : 源目录
// 说明 : 在配置目录下的当前运行程序名的配置文件中，取source -> directory。
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
// 函数 : TAppFile::ApplicationConfig
// 用途 : 得到应用的配置文件名
// 原型 : static TCString ApplicationConfig(TCString sAppName = "");
// 参数 : 应用名称，缺省时取本应用名称
// 返回 : 应用的配置文件名
// 说明 : 应用的配置文件首先取基本配置目录下的文件，但如果文件不存在，则搜索
//        path_links.ini中的相关连接。
// 历史 : 2001.11.15 加入静态变量，加入上次历史匹配, 加入例外抛出
//==========================================================================
TCString TAppFile::ApplicationConfig(TCString sAppName)
{
//    static TCString STA__sLastAppName = "__UNKW__";
//    static TCString STA__sLastAppConfigFileName = "__UNKW__";

    if (sAppName == "")
        sAppName = Application.GetAppName();

    //======== 1. 如果与上次的参数相同，则直接取出上次的结果 =====
    if (sAppName == STA__sLastAppName)
        return STA__sLastAppConfigFileName;

    //======= 2. 得到基本配置文件 ========
    TCString sConfigFileName;
    sConfigFileName = TAppPath::AppConfig() + sAppName + ".ini";
    if (FileExists(sConfigFileName))
    {
        STA__sLastAppName = sAppName;
        STA__sLastAppConfigFileName = sConfigFileName;
        return sConfigFileName;
    }

    //======= 3. 搜索其它配置文件 ========
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

    //======= 4. 如果配置文件不存在，则抛出例外 =========
    throw TCException("TAppFile::ApplicationConfig() : "
                "Config File Not Found - " + sAppName + ".ini");
}

//==========================================================================
// 函数 : TAppFile::GatherGeneralConfig
// 用途 : 得到通用配置文件名
// 原型 : static TCString GatherGeneralConfig();
// 参数 : 无
// 返回 : 通用配置文件名
// 说明 :
//==========================================================================
TCString TAppFile::GatherGeneralConfig()
{
//    static TCString STA__sGeneralConfigFileName = "__UNKW__";

    if (STA__sGeneralConfigFileName = "__UNKW__")
        STA__sGeneralConfigFileName = ApplicationConfig("mig_config");

    return STA__sGeneralConfigFileName;
}

//==========================================================================
// 函数 : TAppConfig::IsInLocalMode
// 用途 : 得到是否本地运行模式
// 原型 : static bool IsInLocalMode();
// 参数 : 无
// 返回 : 是否本地运行模式
// 说明 : 1. 读取mig_config.ini => general => IN_LOCAL_MODE项来判断
//        2. 如是本地运行模式，则不连数据库，直接从本地DBF文件中取数据
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
// 函数 : TAppConfig::IsNationalCenter
// 用途 : 判断是否在全国中心运行
// 原型 : static bool IsNationalCenter();
// 参数 : 无
// 返回 : 是否全国中心
// 说明 : 读取mig_config.ini => general => SCP_PROV_CODE项来判断
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
// 函数 : TAppConfig::VerifyProvinceCode
// 用途 : 验证省代码的正确性
// 原型 : static void VerifyProvinceCode(TCString sProvinceCode);
// 参数 : 省代码
// 返回 : 无
// 说明 : 如果不是正确的省代码则抛出例外
//==========================================================================
void TAppConfig::VerifyProvinceCode(TCString sProvinceCode)
{
    const long MAX_PROVINCE_COUNT = 31;

    static long s_ProvinceList[MAX_PROVINCE_COUNT]
            = { 100/*北京*/, 200/*广东*/, 210/*上海*/, 220/*天津*/,
                230/*重庆*/, 240/*辽宁*/, 250/*江苏*/, 270/*湖北*/,
                280/*四川*/, 290/*陕西*/, 311/*河北*/, 351/*山西*/,
                371/*湖南*/, 431/*吉林*/, 451/*黑龙江*/, 471/*内蒙古*/,
                531/*山东*/, 551/*安徽*/, 571/*浙江*/, 591/*福建*/,
                731/*长沙*/, 771/*广西*/, 791/*江西*/, 851/*贵州*/,
                871/*云南*/, 891/*西藏*/, 898/*海南*/, 931/*甘肃*/,
                951/*宁夏*/, 971/*青海*/, 991/*新疆*/
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
// 函数 : GetAppConfigBool
// 用途 : 得到程序运行参数的布尔值表示
// 原型 : bool GetAppConfigBool(TCString sParmName);
// 参数 : 参数定义串
// 返回 : 运行参数的布尔值表示
// 说明 : 为可读性要求，参数定义串要求以汉字定义
//==========================================================================
bool GetAppConfigBool(TCString sParmName)
{
    if (sParmName == "数据库操作日志")
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
// 函数 : GetAppConfigParm
// 用途 : 得到程序运行参数
// 原型 : TCString GetAppConfigParm(TCString sParmName);
// 参数 : 参数定义串
// 返回 : 参数值串
// 说明 : 为可读性要求，参数定义串要求以汉字定义
//==========================================================================
TCString GetAppConfigParm(TCString sParmName)
{
    if (sParmName == "未漫游基本通话费")
        return "600";

    if (sParmName == "漫游基本通话费")
        return "800";

    if (sParmName == "配置文件目录")
        return TAppPath::AppConfig();

    if (sParmName == "临时文件目录")
    {
        static TCString STA__TempDir = "__UNKW__";
        if (STA__TempDir == "__UNKW__")
            STA__TempDir = ProfileString(TAppFile::GatherGeneralConfig(),
                    "temp_directory", "directory", "");

        return STA__TempDir;
    }

    if (sParmName == "采集机通用配置")
        return TAppFile::GatherGeneralConfig();

    if (sParmName == "采集机数据目录")
    {
        static TCString sWorkDataDir;
        if (sWorkDataDir == "")
            sWorkDataDir = ProfileString(TAppFile::GatherGeneralConfig(),
                    "DISK_MONITOR", "WORK_DATA_DIR", "/data");

        return sWorkDataDir;
    }

    if (sParmName == "采集机停止运行空间百分比")
    {
        static TCString s_sLeastFreePercent;

        if (s_sLeastFreePercent == "")
            s_sLeastFreePercent = ProfileString(TAppFile::GatherGeneralConfig(),
                    "DISK_MONITOR", "STOP_FREE_PERCENT", "8");

        return s_sLeastFreePercent;
    }

    if (sParmName == "采集机暂停运行空间百分比")
    {
        static TCString s_sPauseFreePercent;

        if (s_sPauseFreePercent == "")
            s_sPauseFreePercent = ProfileString(TAppFile::GatherGeneralConfig(),
                    "DISK_MONITOR", "PAUSE_FREE_PERCENT", "16");

        return s_sPauseFreePercent;
    }

    if (sParmName == "数据库服务名称")
        return ProfileString(TAppFile::GatherGeneralConfig(),
                "DATABASE", "ServerName", "");

    if (sParmName == "数据库登录用户")
        return ProfileString(TAppFile::GatherGeneralConfig(),
                "DATABASE", "LogID", "");

    if (sParmName == "数据库登录口令")
        return ProfileString(TAppFile::GatherGeneralConfig(),
                "DATABASE", "LogPassword", "");

    if (sParmName == "数据库数据库名")
        return ProfileString(TAppFile::GatherGeneralConfig(),
                "DATABASE", "Database", "");

    throw TCException("GetAppConfigParm() : Parameter cannot matched.  "
            "ParmName : " + sParmName);
}

//==========================================================================
// 函数 : ProfileAppString
// 用途 : 得到应用配置的字符串值
// 原型 : TCString ProfileAppString(TCString sAppName, TCString sSection,
//              TCString sIdent, TCString sDefault);
// 参数 : 应用名，配置段名，配置关键字，配置缺省值
// 返回 : 配置的字符串值
// 说明 :
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
// 函数 : ProfileAppBool
// 用途 : 得到应用配置的布尔值
// 原型 : bool ProfileAppBool(TCString sAppName, TCString sSection,
//              TCString sIdent, bool bDefault, bool bThrowException = false);
// 参数 : 应用名，配置段名，配置关键字，配置缺省值, 如果没有匹配是否抛出例外
// 返回 : 配置的布尔值
// 说明 : 由于布尔值只能有两种取值，不像整型可以取一个表示根本没有设置的值如
//        "-1", 字符串型可以取一个表示根本没有设置的串，所以增加一个抛出例外
//        的参数，可以在没有设置时直接抛出例外。
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
// 函数 : ProfileAppInt
// 用途 : 得到应用配置的整型值
// 原型 : long ProfileAppInt(TCString sAppName, TCString sSection,
//                  TCString sIdent, long nDefault);
// 参数 : 应用名，配置段名，配置关键字，配置缺省值
// 返回 : 配置的整型值
// 说明 :
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
// 函数 : ProfileAppSession
// 用途 : 得到应用配置的段中的关键字列表
// 原型 : void ProfileAppSession(TCString sAppName, TCString sSection,
//                  TCStringList& pStrings);
// 参数 : 应用名，配置段名，关键字列表的引用
// 返回 : 无
// 说明 :
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

