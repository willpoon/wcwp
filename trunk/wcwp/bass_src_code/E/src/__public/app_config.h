//---------------------------------------------------------------------------

#ifndef app_configH
#define app_configH

//---------------------------------------------------------------------------
// 类   : TAppPathLinks
// 用途 : 该函数用于多组程序共享同一组可执行程序或配置文件的场合
// 说明 : 在config目录下建立path_links.ini文件，存储一些其它目录的指向，示例
//        如下:
//
//    [run_bin_path]                 ; 可执行程序存放的路径
//    directory=/bs_boss_apr/bin/
//
//    [super_config_paths]           ; 其它配置文件存放的路径，最多允许有8个
//    directory1=/bs_boss_apr/config/
//    directory2=... ...
//
// 历史 : 2001.11.13 创建类，进行path_links的处理，以处理复杂运行环境的需求,
//                   同时保持原有的接口尽量不变
//---------------------------------------------------------------------------
class TAppPathLinks
{
public:
    static bool PathLinkExists();

    static TCString GetRunBinPath();
    static TCString GetSuperConfigPath(long nPathSeq);
    static long     GetSuperConfigPathsCount();
};

//---------------------------------------------------------------------------
// 类   : TAppPath
// 用途 : 封装一组通用静态函数，用于得到路径名信息
//---------------------------------------------------------------------------
class TAppPath
{
public:
    static TCString AppRoot();                          // 应用根目录
    static TCString AppTemp(TCString sSubDirName = ""); // 临时文件目录
    static TCString AppLog();                           // 日志目录
    static TCString AppConfig();                        // 配置文件目录
    static TCString AppRunningInfo();                   // 运行信息目录

    static TCString GetConfigDestDirectory();           // 配置的目标目录
    static TCString GetConfigSourceDirectory();         // 配置的源目录
};

//---------------------------------------------------------------------------
// 类   : TAppFile
// 用途 : 封装一组通用静态函数，用于得到文件名信息
//---------------------------------------------------------------------------
class TAppFile
{
public:
    static TCString GatherGeneralConfig();                      // 通用配置文件
    static TCString ApplicationConfig(TCString sAppName = "");  // 应用配置文件
};

//---------------------------------------------------------------------------
// 类   : TAppConfig
// 用途 : 封装一组通用静态函数，用于得到一些通用配置信息
//---------------------------------------------------------------------------
class TAppConfig
{
public:
    static bool IsInLocalMode();                // 是否本地模式
    static bool IsNationalCenter();             // 是否全国中心

    static void VerifyProvinceCode(TCString sProvinceCode);     // 检验省代码
};

bool     GetAppConfigBool(TCString sParmName);      // 程序运行参数(bool)
TCString GetAppConfigParm(TCString sParmName) ;     // 程序运行参数(string)

TCString ProfileAppString(TCString sAppName, TCString sSection,
        TCString sIdent, TCString sDefault);            // 应用配置(string)
bool ProfileAppBool(TCString sAppName, TCString sSection, TCString sIdent,
        bool bDefault, bool bThrowException = false);   // 应用配置(bool)
long ProfileAppInt(TCString sAppName, TCString sSection, TCString sIdent,
        long nDefault);                                 // 应用配置(long)
void ProfileAppSession(TCString sAppName, TCString sSection,
        TCStringList& pStrings);                        // 应用配置(KeyList)

//---------------------------------------------------------------------------
#endif

