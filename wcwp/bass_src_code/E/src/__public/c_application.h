//---------------------------------------------------------------------------

#ifndef c_applicationH
#define c_applicationH
//---------------------------------------------------------------------------

#include "cmpublic.h"

/*==========================================================================
��   : TCApplication
�������ڷ�װһ��Ӧ�ó���
Ŀǰ����������ڴ���
    . �������
    . ѭ��ʱ��
    . ���Ӳ�̿ռ�ﵽһ�����޶ȣ���ͣ��ֹͣ���г���
����������һ��ȫ�ֱ���: Application

���÷�ʽ����:

void MainFunc();

main(int argc, char* argv[])
{
    // ... ... �������е�������г�ʼ������
    Application.MutiProcess(3); ���������������ͬʱ����
                    ͨ��������ָ������ʽ�� AppName -MutiP1

    Application.Initialize(sAppName, argc, argv);         // ���г�ʼ��

    Application.SetRunningHandle(MainFunc);     // �������к���

    // �������м�������������ʱ������������ģʽ��Ϊѭ������ģʽ������
    // ��ѭ��MainFunc�����ڵ������Ժ�ȴ�����ָ���ĺ����������û�и���䣬
    // ����ֻ�����MainFunc()һ�Ρ�
    Application.SetRunningDelay(1000);

    Application.Run();
}

void MainFunc()
{

    // ���������Application.SetNextDelay(50), ���������һ���ӳ�ʱ���ӳٱ�
            // ��ָ����ʱ�䡣֮��ȱʡ�ӳ�ʱ��ָ����С����ֵ���һ�����Ѵ�
            // �������ݵ���Ҫ���촦���µ�����ʱʹ�á�
    // ���������Application.RequestTerminate()����������ֹ����
}

�����ṩ
        MutiProcess(long nMaxNumber);
        Application.ParamCount���Ӳ�������
        Application.ParamStr(n)���ز�����

==========================================================================*/

enum TERunningStatus{rsRunning, rsStop, rsDeath};

const long MAX_EXIT_FUNC_COUNT = 8;

void TerminateApplication(int nSignal);

class TCApplication
{
enum TERunningMode {rmRunningOnce, rmRunningRecursively};

enum TEStartStopLogType { ltStart, ltStop };

private:
    long m_argc;
    char **m_argv;
    TERunningMode m_RunningMode;
    void (*m_RunningFunc)();

    void (*m_ExitFunc[MAX_EXIT_FUNC_COUNT])();
    long m_nExitFuncCount;

    long m_nRunningDelay;
    long m_nNextDelay;

    bool m_bNeedTerminate;

    bool m_bAnotherInstance;
    bool m_bCheckTime;

    long m_nMaxPNumber;
    TCString m_sProcessFlag;      //����̱�־
    TCString m_sAppName;          //��ǰ��Ӧ�ó�������

    time_t m_tmCreate;
    time_t m_tmCheck;
    time_t m_tmTrack;

    long m_nDiskFreePercentPause;
    long m_nDiskFreePercentStop;

    enum TEDiskLimitType { dlPauseDiskLimit, dlStopDiskLimit };
    bool HasReachDiskLimit(TEDiskLimitType dlDiskLimitType);

    void DoApplicationQuitProcess(long nQuitValue = 0);

    bool CheckStopRequest(TCString sAppName = "");
    void CreateRunningFile();

    void CheckTimeReverseAdjusted();
    void CheckTimeReverseAdjustedOfRecord();

    void NoteAppStartStopLog(TEStartStopLogType ltType);

    void NoteCurrentTime();

public:
    TCApplication();
    ~TCApplication();

    void CreateStopRequestFile(TCString sAppName);

    TCString GetAppName();
    TCString GetProcessFlag(){return m_sProcessFlag;};

    void DisableCheckTime();    //%%%ADD
    TERunningStatus GetAppRunningStatus(TCString sAppName = "");

    void Initialize(TCString sAppName, int argc, char* argv[]);

    void InstallExitHandle(void (*func)());

    void  MutiProcess(long nMaxNumber);  //���õ���������

    long ParamCount();
    TCString ParamStr(long nIndex);

    void ProcessMessages();

    void RequestTerminate();

    void Run();

    void SetDiskFreePercentPause(long nPercent)
    {   m_nDiskFreePercentPause = nPercent;
    };

    void SetDiskFreePercentStop(long nPercent)
    {   m_nDiskFreePercentStop = nPercent;
    };

    void SetNextDelay(long nNextDelay = 0);
    void SetRunningDelay(long nRunningDelay = 1000);
    void SetRunningHandle(void (*func)());

friend void ExitWithSystemFailLog(char *szFailedString, char *szFailType);
};

extern TCApplication Application;

#endif
