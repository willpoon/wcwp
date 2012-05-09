//---------------------------------------------------------------------------

#ifndef c_applicationH
#define c_applicationH
//---------------------------------------------------------------------------

#include "cmpublic.h"

/*==========================================================================
类   : TCApplication
该类用于封装一个应用程序。
目前该类可以用于处理
    . 例外机制
    . 循环时延
    . 如果硬盘空间达到一定的限度，暂停或停止运行程序
该类已声明一个全局变量: Application

调用方式举例:

void MainFunc();

main(int argc, char* argv[])
{
    // ... ... 对命令行等情况进行初始化处理
    Application.MutiProcess(3); 最多允许三个进程同时运行
                    通过命令行指定，格式： AppName -MutiP1

    Application.Initialize(sAppName, argc, argv);         // 进行初始化

    Application.SetRunningHandle(MainFunc);     // 设置运行函数

    // 设置运行间隔。如果设置了时间间隔，则运行模式改为循环运行模式，程序
    // 会循环MainFunc，并在调用完以后等待这里指定的毫秒数。如果没有该语句，
    // 程序只会调用MainFunc()一次。
    Application.SetRunningDelay(1000);

    Application.Run();
}

void MainFunc()
{

    // 如果调用了Application.SetNextDelay(50), 则程序在下一次延迟时会延迟本
            // 次指定的时间。之后按缺省延迟时间恢复运行。这种调用一般在已处
            // 理了数据但又要尽快处理新的数据时使用。
    // 如果调用了Application.RequestTerminate()，则程序会终止运行
}

另外提供
        MutiProcess(long nMaxNumber);
        Application.ParamCount返加参数个数
        Application.ParamStr(n)返回参数串

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
    TCString m_sProcessFlag;      //多进程标志
    TCString m_sAppName;          //当前的应用程序名称

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

    void  MutiProcess(long nMaxNumber);  //设置单程序多进程

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
