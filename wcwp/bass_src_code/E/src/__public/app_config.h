//---------------------------------------------------------------------------

#ifndef app_configH
#define app_configH

//---------------------------------------------------------------------------
// ��   : TAppPathLinks
// ��; : �ú������ڶ��������ͬһ���ִ�г���������ļ��ĳ���
// ˵�� : ��configĿ¼�½���path_links.ini�ļ����洢һЩ����Ŀ¼��ָ��ʾ��
//        ����:
//
//    [run_bin_path]                 ; ��ִ�г����ŵ�·��
//    directory=/bs_boss_apr/bin/
//
//    [super_config_paths]           ; ���������ļ���ŵ�·�������������8��
//    directory1=/bs_boss_apr/config/
//    directory2=... ...
//
// ��ʷ : 2001.11.13 �����࣬����path_links�Ĵ����Դ��������л���������,
//                   ͬʱ����ԭ�еĽӿھ�������
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
// ��   : TAppPath
// ��; : ��װһ��ͨ�þ�̬���������ڵõ�·������Ϣ
//---------------------------------------------------------------------------
class TAppPath
{
public:
    static TCString AppRoot();                          // Ӧ�ø�Ŀ¼
    static TCString AppTemp(TCString sSubDirName = ""); // ��ʱ�ļ�Ŀ¼
    static TCString AppLog();                           // ��־Ŀ¼
    static TCString AppConfig();                        // �����ļ�Ŀ¼
    static TCString AppRunningInfo();                   // ������ϢĿ¼

    static TCString GetConfigDestDirectory();           // ���õ�Ŀ��Ŀ¼
    static TCString GetConfigSourceDirectory();         // ���õ�ԴĿ¼
};

//---------------------------------------------------------------------------
// ��   : TAppFile
// ��; : ��װһ��ͨ�þ�̬���������ڵõ��ļ�����Ϣ
//---------------------------------------------------------------------------
class TAppFile
{
public:
    static TCString GatherGeneralConfig();                      // ͨ�������ļ�
    static TCString ApplicationConfig(TCString sAppName = "");  // Ӧ�������ļ�
};

//---------------------------------------------------------------------------
// ��   : TAppConfig
// ��; : ��װһ��ͨ�þ�̬���������ڵõ�һЩͨ��������Ϣ
//---------------------------------------------------------------------------
class TAppConfig
{
public:
    static bool IsInLocalMode();                // �Ƿ񱾵�ģʽ
    static bool IsNationalCenter();             // �Ƿ�ȫ������

    static void VerifyProvinceCode(TCString sProvinceCode);     // ����ʡ����
};

bool     GetAppConfigBool(TCString sParmName);      // �������в���(bool)
TCString GetAppConfigParm(TCString sParmName) ;     // �������в���(string)

TCString ProfileAppString(TCString sAppName, TCString sSection,
        TCString sIdent, TCString sDefault);            // Ӧ������(string)
bool ProfileAppBool(TCString sAppName, TCString sSection, TCString sIdent,
        bool bDefault, bool bThrowException = false);   // Ӧ������(bool)
long ProfileAppInt(TCString sAppName, TCString sSection, TCString sIdent,
        long nDefault);                                 // Ӧ������(long)
void ProfileAppSession(TCString sAppName, TCString sSection,
        TCStringList& pStrings);                        // Ӧ������(KeyList)

//---------------------------------------------------------------------------
#endif

