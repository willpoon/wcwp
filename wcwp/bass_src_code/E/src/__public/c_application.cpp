//---------------------------------------------------------------------------
#ifndef __WIN32__
#include <signal.h>
#endif

#include "cmpublic.h"

#pragma hdrstop

#include "c_application.h"

//---------------------------------------------------------------------------
#pragma package(smart_init)

TCApplication Application;

#pragma argsused
void TerminateApplication(int nSignal)
{
    Application.RequestTerminate();
}

//==========================================================================
// 函数 : TCApplication::TCApplication
// 用途 : TCApplication构造函数
// 原型 : TCApplication();
// 参数 : 无
// 说明 : 初始化各变量
//==========================================================================
TCApplication::TCApplication()
{
    m_argc = 0;
    m_argv = NULL;
    m_RunningMode = TCApplication::rmRunningOnce;

    m_RunningFunc = NULL;

    m_nMaxPNumber = 1;
    m_nRunningDelay = 1000;
    m_nNextDelay = -1;

    m_bNeedTerminate = false;

    m_bAnotherInstance = false;
    m_bCheckTime = true;

    m_nDiskFreePercentPause = -1;
    m_nDiskFreePercentStop = -1;

    m_nExitFuncCount = 0;
    m_tmCreate = 0;
    m_tmCheck = time(NULL);
    m_tmTrack = 0;
}

//==========================================================================
// 函数 : TCApplication::~TCApplication()
// 用途 : TCApplication析构函数
// 说明 :
//==========================================================================
TCApplication::~TCApplication()
{
}

void TCApplication::DoApplicationQuitProcess(long nQuitValue)
{
    try
    {
        if (m_bAnotherInstance)
            exit(nQuitValue);

        if (m_RunningMode == rmRunningRecursively)
            NoteAppStartStopLog(ltStop);

        TCString sRuningInfoFileName;
        TCString sStopInfoFileName;

        sRuningInfoFileName = TAppPath::AppRunningInfo() + "running_" + m_sAppName;
        sStopInfoFileName = TAppPath::AppRunningInfo() + "stop_" + m_sAppName;

        //删除 RunningInfo 文件信息
        if (FileExists(sRuningInfoFileName))
            DeleteFile(sRuningInfoFileName);

        //删除 StopInfo 文件信息
        if (FileExists(sStopInfoFileName))
            DeleteFile(sStopInfoFileName);

        // 调用安装的退出句柄
        long i;

        for (i = 0; i < m_nExitFuncCount; i++)
            m_ExitFunc[i]();
    }
    catch (...)
    {
        exit(nQuitValue);
    }
    exit(nQuitValue);
}

//==========================================================================
// 函数 : TCApplication::HasReachDiskLimit
// 用途 : 判断硬盘的剩余空间已到了需要暂停或停止程序运行的程度
// 原型 : bool HasReachDiskLimit
//          (TCApplication::TEDiskLimitType dlDiskLimitType);
// 参数 : 判断类型(暂停还是停止)
// 返回 : 是否需要暂停或停止运行
// 说明 : 1. 为增加速度，该函数每隔1分钟进行一次真正的判断，其它情况下返
//           回上一次判断结果。
//        2. 该函数需要读取配置信息。配置信息项为
//           FILE_NAME : mig_config.ini
//           SECTION   : DISK_MONITOR
//           IDENTS    : WORK_DATA_DIR (如不配置，则取 - /data)
//                       PAUSE_FREE_PERCENT (如不配置，则取 - 16)
//                       STOP_FREE_PERCENT (如不配置，则取 - 8)
//==========================================================================
bool TCApplication::HasReachDiskLimit
        (TCApplication::TEDiskLimitType dlDiskLimitType)
{
    static long s_nLastTimeSection = -1;
    static bool s_bReachPauseLimit = false;
    static bool s_bReachStopLimit = false;

    long nCurrentTimeSection;

    // 每隔1分钟判断硬盘空间是否已满
    nCurrentTimeSection = TCTime::GetTimeSection(TCTime::Now(), 1);
    if (nCurrentTimeSection != s_nLastTimeSection)
    {
        s_nLastTimeSection = nCurrentTimeSection;

        TCString sWorkDir;
        long nStopPercent, nPausePercent;
        long nCurrentDiskFreePercent;

        sWorkDir = GetAppConfigParm("采集机数据目录");

        if (m_nDiskFreePercentStop == -1)
            nStopPercent = StrToInt
                    (GetAppConfigParm("采集机停止运行空间百分比"));
        else
            nStopPercent = m_nDiskFreePercentStop;

        if (m_nDiskFreePercentPause == -1)
            nPausePercent = StrToInt
                    (GetAppConfigParm("采集机暂停运行空间百分比"));
        else
            nPausePercent = m_nDiskFreePercentPause;

        nCurrentDiskFreePercent = TCSystem::DiskFreePercent(sWorkDir);

        if (nCurrentDiskFreePercent <= nStopPercent)
        {
            if (s_bReachStopLimit == false)
                TCAppErrorLog::AddLog(aeError, "磁盘容量限制", "程序退出",
                        "当前空闲百分比: " + IntToStr(nCurrentDiskFreePercent)
                        + "%   应达到的空闲百分比: "
                        + IntToStr(nStopPercent) + "%");
            s_bReachPauseLimit = true;
            s_bReachStopLimit = true;
            return true;
        }
        else
            s_bReachStopLimit = false;

        if (nCurrentDiskFreePercent <= nPausePercent)
        {
            if (s_bReachPauseLimit == false)
                TCAppErrorLog::AddLog(aeWarning, "磁盘容量限制", "程序暂停",
                        "当前空闲百分比: " + IntToStr(nCurrentDiskFreePercent)
                        + "%   应达到的空闲百分比: "
                        + IntToStr(nPausePercent) + "%");
            s_bReachPauseLimit = true;
        }
        else
            s_bReachPauseLimit = false;
    }

    if (dlDiskLimitType == dlPauseDiskLimit)
        return s_bReachPauseLimit;
    else
        return s_bReachStopLimit;
}

//==========================================================================
// 函数 : TCApplication::CheckStopRequest
// 用途 : 判断该应用程序是否要被控制程序中止
// 原型 : bool TCApplication::CheckStopRequest(TCString sAppName = "");
// 参数 : 应用程序名称
// 返回 : true 应该终止, false 不应该终止
// 说明 :
//==========================================================================
bool TCApplication::CheckStopRequest(TCString sAppName)
{
    if (sAppName == TCString("") )
        sAppName = m_sAppName;

    TCString sStopInfoFileName;

    sStopInfoFileName = TAppPath::AppRunningInfo() + "stop_" + sAppName;
    if (FileExists(sStopInfoFileName))
      return true;
    else
      return false;
}

//==========================================================================
// 函数 : TCApplication::CreateRunningFile
// 用途 : 创建正在运行标示文件
// 原型 : void CreateRunningFile()
// 参数 : 应用程序名称
// 返回 : 无
// 说明 : 无
//==========================================================================
void TCApplication::CreateRunningFile()
{
  TCString sRuningInfoFileName;

  sRuningInfoFileName = TAppPath::AppRunningInfo() + "running_" + m_sAppName;

  CreateBlankFile(sRuningInfoFileName);

}

//==========================================================================
// 函数 : TCApplication::CreateStopRequestFile
// 用途 : 创建停止请求文件
// 原型 : void CreateStopRequestFile(TCString sAppName);
// 参数 : 应用程序名称
// 返回 : 无
// 说明 : 该函数一般由控制程序调用。以停止应用程序的运行。
//==========================================================================
void TCApplication::CreateStopRequestFile(TCString sAppName)
{
    TCString sStopRequestFileName;
    sStopRequestFileName = TAppPath::AppRunningInfo() + "stop_" + sAppName;

    CreateBlankFile(sStopRequestFileName);
}

//==========================================================================
// 函数 : TCApplication::MutiProcess
// 用途 : 设置单程序多进程允许最多进程数
// 原型 : void  TCApplication::MutiProcess(long nMaxNumber)
// 参数 : 最大进程数
// 返回 : 无
// 说明 : 通过命令行方式实现单程序多进程，各进程读取的配置有区别
//        命令格式： AppName -MutiP1
//==========================================================================
void  TCApplication::MutiProcess(long nMaxNumber)
{
//   ASSERT((nMaxNumber > 0)&&(nMaxNumber <= 9));
   m_nMaxPNumber = nMaxNumber;
   
}
//==========================================================================
// 函数 : TCApplication::GetAppName
// 用途 : 得到当前应用程序名
// 原型 : TCString GetAppName();
// 参数 : 无
// 返回 : 当前应用程序名
// 说明 : 该名称是在Initialize()中传入的
//==========================================================================
TCString TCApplication::GetAppName()
{
	
    if( m_nMaxPNumber>1 )
    {
         return Left(m_sAppName,LastDelimiter("-P",m_sAppName) -2);
         // Left(m_sAppName,Length(m_sAppName)-3);
    }        
    else return  m_sAppName;
}

//==========================================================================
// 函数 : TCApplication::GetAppRunningStatus
// 用途 : 获取应用程序的运行状态
// 原型 : TERunningStatus GetAppRunningStatus(TCString sAppName = "");
// 参数 : 应用程序名称
// 返回 : 该应用程序的状态
// 说明 :
//==========================================================================
TERunningStatus TCApplication::GetAppRunningStatus(TCString sAppName)
{
    const long MAX_DELAY_SECONDS = 240;  // 4分钟如果未心跳，则认为已死

    if (sAppName == TCString("") )
        sAppName = m_sAppName;

    TCString sRuningInfoFileName;

    struct stat statbuf;                   
    long DelaySeconds;
    TERunningStatus Result;

    sRuningInfoFileName = TAppPath::AppRunningInfo()
            + "running_" + sAppName;

    if (FileExists(sRuningInfoFileName))
    {
      stat(sRuningInfoFileName, &statbuf);

      DelaySeconds = TCTime::SecondsAfter
            (TCTime::GetDatetimeStringByTimeT(statbuf.st_ctime),
            TCTime::Now());

      if (DelaySeconds < MAX_DELAY_SECONDS)
      {
        Result = rsRunning;
      }
      else
      {
        Result = rsDeath;
//        DeleteFile(sRuningInfoFileName);      // 注释掉，可以保留现场
      }
    }
    else
    {
      Result = rsStop;
    }

    return Result;
}

//==========================================================================
// 函数 : TCApplication::Initialize
// 用途 : main()中首先调用，传进命令行参数
// 原型 : void Initialize(int argc, char* argv[]);
// 参数 : 参数数量，参数
// 返回 : 无
// 说明 :
//==========================================================================
void TCApplication::Initialize(TCString sAppName, int argc, char* argv[])
{
    if( m_nMaxPNumber>1 )
    {
       if( argc <= 1 )
            m_sProcessFlag = "-MutiP1";
       else m_sProcessFlag = argv[1];
       if( (Length(m_sProcessFlag) < 7) || (m_sProcessFlag < TCString("-MutiP1") ) )
            ExitWithSystemFailLog("命令行参数错误","TCApplication::Initialize");
       if( StrToInt(Mid(m_sProcessFlag,7)) > m_nMaxPNumber )
            ExitWithSystemFailLog("命令行参数中指定的进程序号过大",
                                  "TCApplication::Initialize");
       m_sProcessFlag = Mid(m_sProcessFlag,6);
       m_sAppName = sAppName + "_" + m_sProcessFlag;
       
    }
    else m_sAppName = sAppName;

    m_argc = argc;
    m_argv = argv;

    if (GetAppRunningStatus() == rsRunning)
    {
        printf("Another Instance of Application \"" + sAppName
                + "\" is Running.  Terminate the program.   Thank you...");

        m_bAnotherInstance = true;

        DoApplicationQuitProcess(1);
    }
}

//==========================================================================
// 函数 : TCApplication::InstallExitHandle
// 用途 : 安装程序退出句柄
// 原型 : void InstallExitHandle(void (*func)());
// 参数 : 函数指针
// 返回 : 无
// 说明 : 该函数可以调用多次，在TCApplication析构时会运行安装的函数。
//==========================================================================
void TCApplication::InstallExitHandle(void (*func)())
{
    m_nExitFuncCount ++;

    ASSERT(m_nExitFuncCount <= MAX_EXIT_FUNC_COUNT);

    m_ExitFunc[m_nExitFuncCount - 1] = func;

}

//==========================================================================
// 函数 : TCApplication::CheckTimeReverseAdjusted
// 用途 : 判断时间是否被逆调过
// 原型 : bool CheckTimeReverseAdjusted();
// 参数 : 无
// 返回 : 是否被逆调过
// 说明 :
//==========================================================================
void TCApplication::CheckTimeReverseAdjusted()
{
    time_t tmNow;
    tmNow = time(NULL);

    if( m_tmCheck > (tmNow+60) )
    {   throw TCException("Time Reversed.  " + TCTime::GetDatetimeStringByTimeT(m_tmCheck) +
              " >> " + TCTime::GetDatetimeStringByTimeT(tmNow));
    }
    else if( (m_tmCheck + SECONDS_OF_DAY) < tmNow )
    {   throw TCException("Time Advance Adjust.  " + TCTime::GetDatetimeStringByTimeT(m_tmCheck) +
              " >> " + TCTime::GetDatetimeStringByTimeT(tmNow));
    }
    else
    { m_tmCheck = tmNow;
      if( (m_tmTrack + 120) < tmNow )
      {
          NoteCurrentTime();
          m_tmTrack = tmNow;
      }
    }
}

//==========================================================================
// 函数 : TCApplication::NoteCurrentTime
// 用途 : 在文件中记录下当前时间
// 原型 : void NoteCurrentTime();
// 参数 : 无
// 返回 : 无
// 说明 : 该函数用于判断是否时间出现逆调的现象
//==========================================================================
void TCApplication::NoteCurrentTime()
{
    TCString sCurrentTimeFileName;
    TCIniFile mIniFile;

    sCurrentTimeFileName = TAppPath::AppRunningInfo() + ".security_track_time";
    if( !FileExists(sCurrentTimeFileName) )
        mIniFile.CreateNew(sCurrentTimeFileName);
    else
        mIniFile.Load(sCurrentTimeFileName);
    mIniFile.WriteString("security","_track_time_",
                          TCTime::GetDatetimeStringByTimeT(m_tmCheck));
}
void TCApplication::DisableCheckTime()
{
  m_bCheckTime = false;
}

void TCApplication::CheckTimeReverseAdjustedOfRecord()
{
    TCString sCurrentTimeFileName;
    TCString dtTrackDateTime;

    sCurrentTimeFileName = TAppPath::AppRunningInfo() + ".security_track_time";
    if( !FileExists(sCurrentTimeFileName) )
        return;

    dtTrackDateTime = ProfileString(sCurrentTimeFileName,"security",
                                     "_track_time_","");
    if( dtTrackDateTime == TCString("") ) return;

    long nSecondsEllapsed;
    nSecondsEllapsed = TCTime::SecondsAfter(dtTrackDateTime, TCTime::Now());
    if (nSecondsEllapsed < -60)
        throw TCException("Time Reversed.  " + dtTrackDateTime + " >> "
                + TCTime::Now());

    if (nSecondsEllapsed > SECONDS_OF_DAY * 7)
        throw TCException("Time Advance Adjust.  " + dtTrackDateTime + " >> "
                + TCTime::Now());
}

//==========================================================================
// 函数 : TCApplication::NoteAppStartStopLog
// 用途 : 记录应用启停日志
// 原型 : void NoteAppStartStopLog(TEStartStopLogType ltType);
// 参数 : 启动还是停止
// 返回 : 无
// 说明 :
//==========================================================================
void TCApplication::NoteAppStartStopLog(TEStartStopLogType ltType)
{
    TCString sLogFileName;

    sLogFileName = TAppPath::AppLog() + "run_exit.mlg";

    if (!FileExists(sLogFileName))
    {   TCDBFCreate dcCreate(sLogFileName, 3);
        dcCreate.AddField("program", 'C', 16);
        dcCreate.AddField("time", 'C', 14);
        dcCreate.AddField("action", 'C', 5);
        dcCreate.CreateDBF();
    }

    TCString sProgram, dtTime, sAction;
    sProgram = GetAppName();
    dtTime = TCTime::Now();
    if (ltType == ltStart)
        sAction = "START";
    else
        sAction = "STOP";

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(sLogFileName);
    fdFoxDBF.DBBind("program", sProgram);
    fdFoxDBF.DBBind("time", dtTime);
    fdFoxDBF.DBBind("action", sAction);

    fdFoxDBF.Append();
}

//==========================================================================
// 函数 : TCApplication::ParamCount
// 用途 : 得到命令行参数的数量
// 原型 : long ParamCount()
// 参数 : 无
// 返回 : 参数数量
// 说明 :
//==========================================================================
long TCApplication::ParamCount()
{
    return m_argc;
}

//==========================================================================
// 函数 : TCApplication::ParamStr
// 用途 : 得到命令行参数的数量
// 原型 : TCString ParamStr(long nIndex);
// 参数 : 参数序号
// 返回 : 参数串
// 说明 : 第0个参数放应用程序名，第1个参数开始是真正的参数
//==========================================================================
TCString TCApplication::ParamStr(long nIndex)
{
    if (nIndex >= ParamCount())
        throw TCException("PARAMSTR GET PARM ERROR");

    return m_argv[nIndex];
}

//==========================================================================
// 函数 : TCApplication::ProcessMessages
// 用途 : 在需要大段时间运行的过程中调用，以处理一些心跳、刷新类的处理
// 原型 : void ProcessMessages()
// 参数 : 无
// 返回 : 无
// 说明 : 1. 本函数的命名源自VCL, 区别是，本函数并不处理任何消息。
//        2. 原则上，在一个可能耗时较长(会超过一分钟)，且中间不能中断的函数
//           处理中调用Application.ProcessMessages()
//==========================================================================
void TCApplication::ProcessMessages()
{
   if( (m_tmCreate + 8) < time(NULL) || (m_tmCreate > time(NULL)) )
   {   CreateRunningFile();
       m_tmCreate = time(NULL);
   }

}

//==========================================================================
// 函数 : TCApplication::RequestTerminate
// 用途 : 需要程序退出
// 原型 : void RequestTerminate()
// 参数 : 无
// 返回 : 无
// 说明 : 在主运行函数中可以进行调用，以通知应用程序退出
//==========================================================================
void TCApplication::RequestTerminate()
{
    m_bNeedTerminate = true;
}

//==========================================================================
// 函数 : TCApplication::Run
// 用途 : 主运行程序
// 原型 : void Run()
// 参数 : 无
// 返回 : 无
// 说明 : 在该函数中进行例外处理及时延
//==========================================================================
void TCApplication::Run()
{
    try
    {
        if (m_RunningMode == rmRunningOnce)
        {
            m_RunningFunc();
        }

        if (m_RunningMode == rmRunningRecursively)
        {
#ifndef __WIN32__
            signal(SIGINT, SIG_IGN);                    // 中断键被按下
            signal(SIGQUIT, SIG_IGN);                   // 停止键被按下
            signal(SIGTERM, TerminateApplication);      // 软件Kill
#endif

            NoteAppStartStopLog(ltStart);

            if( m_bCheckTime )
               CheckTimeReverseAdjustedOfRecord();

            while (true)
            {
                long nShouldDelay;

                if( m_bCheckTime )
                {
                    CheckTimeReverseAdjusted();
                }
                ProcessMessages();
                if (!HasReachDiskLimit(dlPauseDiskLimit))
                {
                    m_RunningFunc();
                }

                if (CheckStopRequest())
                {
                    break;
                }
				
		if (m_bNeedTerminate)
		{
                    break;
                }

                if (HasReachDiskLimit(dlStopDiskLimit))
                {
                    break;
                }

                if (m_nNextDelay == -1)
                {
                    nShouldDelay = m_nRunningDelay;
                }
                else
                {   nShouldDelay = m_nNextDelay;
                    m_nNextDelay = -1;
                }
                TCSystem::DelayMicroSeconds(nShouldDelay);
            }
        }
    }
    catch (TCException e)
    {
        printf("%s\n", (char *)e.GetMessage());

        TCFileStream fs;
        fs.Open(TAppPath::AppLog()+"sys_fail.log", omAppend|omText);

        fs.WriteLn(GetAppName() + "  " + TCTime::Now() + "   EXCEPTION");
        fs.WriteLn(e.GetMessage());
        fs.WriteLn("");

        DoApplicationQuitProcess(-1);
    }

    DoApplicationQuitProcess();
}

//==========================================================================
// 函数 : TCApplication::SetNextDelay
// 用途 : 设置程序下一个时延
// 原型 : void SetNextDelay(long nNextDelay);
// 参数 : 下一个时延
// 返回 : 无
// 说明 : 在主运行函数中可以调用之。调用以后程序在下一次延迟时会延迟本
//        次指定的时间。之后按缺省延迟时间恢复运行。这种调用一般在已处
//        理了数据但又要尽快处理新的数据时使用。
//==========================================================================
void TCApplication::SetNextDelay(long nNextDelay)
{
    m_nNextDelay = nNextDelay;
}

//==========================================================================
// 函数 : TCApplication::SetRunningDelay
// 用途 : 设置程序缺省时延
// 原型 : void SetRunningDelay(long nRunningDelay);
// 参数 : 缺省时延
// 返回 : 无
// 说明 : 一般在main()中调用。如果设置了时间间隔，则运行模式改为循环
//        运行模式，程序会循环运行MainFunc，并在调用完以后等待这里指
//        定的毫秒数。如果没有该语句，程序只会调用MainFunc()一次。
//==========================================================================
void TCApplication::SetRunningDelay(long nRunningDelay)
{
    m_RunningMode = rmRunningRecursively;

    m_nRunningDelay = nRunningDelay;

}

//==========================================================================
// 函数 : TCApplication::SetRunningHandle
// 用途 : 设置主运行函数
// 原型 : void SetRunningHandle(void (*func)());
// 参数 : 无
// 返回 : 无
// 说明 : 该函数一定要在main()中调用。设置的主运行函数应为可单独运行的
//        自维护函数。在主运行函数退出时应释放其申请的一切内存。
//==========================================================================
void TCApplication::SetRunningHandle(void (*func)())
{
    m_RunningFunc = func;

}


