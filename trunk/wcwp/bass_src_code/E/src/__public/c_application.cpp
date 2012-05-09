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
// ���� : TCApplication::TCApplication
// ��; : TCApplication���캯��
// ԭ�� : TCApplication();
// ���� : ��
// ˵�� : ��ʼ��������
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
// ���� : TCApplication::~TCApplication()
// ��; : TCApplication��������
// ˵�� :
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

        //ɾ�� RunningInfo �ļ���Ϣ
        if (FileExists(sRuningInfoFileName))
            DeleteFile(sRuningInfoFileName);

        //ɾ�� StopInfo �ļ���Ϣ
        if (FileExists(sStopInfoFileName))
            DeleteFile(sStopInfoFileName);

        // ���ð�װ���˳����
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
// ���� : TCApplication::HasReachDiskLimit
// ��; : �ж�Ӳ�̵�ʣ��ռ��ѵ�����Ҫ��ͣ��ֹͣ�������еĳ̶�
// ԭ�� : bool HasReachDiskLimit
//          (TCApplication::TEDiskLimitType dlDiskLimitType);
// ���� : �ж�����(��ͣ����ֹͣ)
// ���� : �Ƿ���Ҫ��ͣ��ֹͣ����
// ˵�� : 1. Ϊ�����ٶȣ��ú���ÿ��1���ӽ���һ���������жϣ���������·�
//           ����һ���жϽ����
//        2. �ú�����Ҫ��ȡ������Ϣ��������Ϣ��Ϊ
//           FILE_NAME : mig_config.ini
//           SECTION   : DISK_MONITOR
//           IDENTS    : WORK_DATA_DIR (�粻���ã���ȡ - /data)
//                       PAUSE_FREE_PERCENT (�粻���ã���ȡ - 16)
//                       STOP_FREE_PERCENT (�粻���ã���ȡ - 8)
//==========================================================================
bool TCApplication::HasReachDiskLimit
        (TCApplication::TEDiskLimitType dlDiskLimitType)
{
    static long s_nLastTimeSection = -1;
    static bool s_bReachPauseLimit = false;
    static bool s_bReachStopLimit = false;

    long nCurrentTimeSection;

    // ÿ��1�����ж�Ӳ�̿ռ��Ƿ�����
    nCurrentTimeSection = TCTime::GetTimeSection(TCTime::Now(), 1);
    if (nCurrentTimeSection != s_nLastTimeSection)
    {
        s_nLastTimeSection = nCurrentTimeSection;

        TCString sWorkDir;
        long nStopPercent, nPausePercent;
        long nCurrentDiskFreePercent;

        sWorkDir = GetAppConfigParm("�ɼ�������Ŀ¼");

        if (m_nDiskFreePercentStop == -1)
            nStopPercent = StrToInt
                    (GetAppConfigParm("�ɼ���ֹͣ���пռ�ٷֱ�"));
        else
            nStopPercent = m_nDiskFreePercentStop;

        if (m_nDiskFreePercentPause == -1)
            nPausePercent = StrToInt
                    (GetAppConfigParm("�ɼ�����ͣ���пռ�ٷֱ�"));
        else
            nPausePercent = m_nDiskFreePercentPause;

        nCurrentDiskFreePercent = TCSystem::DiskFreePercent(sWorkDir);

        if (nCurrentDiskFreePercent <= nStopPercent)
        {
            if (s_bReachStopLimit == false)
                TCAppErrorLog::AddLog(aeError, "������������", "�����˳�",
                        "��ǰ���аٷֱ�: " + IntToStr(nCurrentDiskFreePercent)
                        + "%   Ӧ�ﵽ�Ŀ��аٷֱ�: "
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
                TCAppErrorLog::AddLog(aeWarning, "������������", "������ͣ",
                        "��ǰ���аٷֱ�: " + IntToStr(nCurrentDiskFreePercent)
                        + "%   Ӧ�ﵽ�Ŀ��аٷֱ�: "
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
// ���� : TCApplication::CheckStopRequest
// ��; : �жϸ�Ӧ�ó����Ƿ�Ҫ�����Ƴ�����ֹ
// ԭ�� : bool TCApplication::CheckStopRequest(TCString sAppName = "");
// ���� : Ӧ�ó�������
// ���� : true Ӧ����ֹ, false ��Ӧ����ֹ
// ˵�� :
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
// ���� : TCApplication::CreateRunningFile
// ��; : �����������б�ʾ�ļ�
// ԭ�� : void CreateRunningFile()
// ���� : Ӧ�ó�������
// ���� : ��
// ˵�� : ��
//==========================================================================
void TCApplication::CreateRunningFile()
{
  TCString sRuningInfoFileName;

  sRuningInfoFileName = TAppPath::AppRunningInfo() + "running_" + m_sAppName;

  CreateBlankFile(sRuningInfoFileName);

}

//==========================================================================
// ���� : TCApplication::CreateStopRequestFile
// ��; : ����ֹͣ�����ļ�
// ԭ�� : void CreateStopRequestFile(TCString sAppName);
// ���� : Ӧ�ó�������
// ���� : ��
// ˵�� : �ú���һ���ɿ��Ƴ�����á���ֹͣӦ�ó�������С�
//==========================================================================
void TCApplication::CreateStopRequestFile(TCString sAppName)
{
    TCString sStopRequestFileName;
    sStopRequestFileName = TAppPath::AppRunningInfo() + "stop_" + sAppName;

    CreateBlankFile(sStopRequestFileName);
}

//==========================================================================
// ���� : TCApplication::MutiProcess
// ��; : ���õ���������������������
// ԭ�� : void  TCApplication::MutiProcess(long nMaxNumber)
// ���� : ��������
// ���� : ��
// ˵�� : ͨ�������з�ʽʵ�ֵ��������̣������̶�ȡ������������
//        �����ʽ�� AppName -MutiP1
//==========================================================================
void  TCApplication::MutiProcess(long nMaxNumber)
{
//   ASSERT((nMaxNumber > 0)&&(nMaxNumber <= 9));
   m_nMaxPNumber = nMaxNumber;
   
}
//==========================================================================
// ���� : TCApplication::GetAppName
// ��; : �õ���ǰӦ�ó�����
// ԭ�� : TCString GetAppName();
// ���� : ��
// ���� : ��ǰӦ�ó�����
// ˵�� : ����������Initialize()�д����
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
// ���� : TCApplication::GetAppRunningStatus
// ��; : ��ȡӦ�ó��������״̬
// ԭ�� : TERunningStatus GetAppRunningStatus(TCString sAppName = "");
// ���� : Ӧ�ó�������
// ���� : ��Ӧ�ó����״̬
// ˵�� :
//==========================================================================
TERunningStatus TCApplication::GetAppRunningStatus(TCString sAppName)
{
    const long MAX_DELAY_SECONDS = 240;  // 4�������δ����������Ϊ����

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
//        DeleteFile(sRuningInfoFileName);      // ע�͵������Ա����ֳ�
      }
    }
    else
    {
      Result = rsStop;
    }

    return Result;
}

//==========================================================================
// ���� : TCApplication::Initialize
// ��; : main()�����ȵ��ã����������в���
// ԭ�� : void Initialize(int argc, char* argv[]);
// ���� : ��������������
// ���� : ��
// ˵�� :
//==========================================================================
void TCApplication::Initialize(TCString sAppName, int argc, char* argv[])
{
    if( m_nMaxPNumber>1 )
    {
       if( argc <= 1 )
            m_sProcessFlag = "-MutiP1";
       else m_sProcessFlag = argv[1];
       if( (Length(m_sProcessFlag) < 7) || (m_sProcessFlag < TCString("-MutiP1") ) )
            ExitWithSystemFailLog("�����в�������","TCApplication::Initialize");
       if( StrToInt(Mid(m_sProcessFlag,7)) > m_nMaxPNumber )
            ExitWithSystemFailLog("�����в�����ָ���Ľ�����Ź���",
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
// ���� : TCApplication::InstallExitHandle
// ��; : ��װ�����˳����
// ԭ�� : void InstallExitHandle(void (*func)());
// ���� : ����ָ��
// ���� : ��
// ˵�� : �ú������Ե��ö�Σ���TCApplication����ʱ�����а�װ�ĺ�����
//==========================================================================
void TCApplication::InstallExitHandle(void (*func)())
{
    m_nExitFuncCount ++;

    ASSERT(m_nExitFuncCount <= MAX_EXIT_FUNC_COUNT);

    m_ExitFunc[m_nExitFuncCount - 1] = func;

}

//==========================================================================
// ���� : TCApplication::CheckTimeReverseAdjusted
// ��; : �ж�ʱ���Ƿ������
// ԭ�� : bool CheckTimeReverseAdjusted();
// ���� : ��
// ���� : �Ƿ������
// ˵�� :
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
// ���� : TCApplication::NoteCurrentTime
// ��; : ���ļ��м�¼�µ�ǰʱ��
// ԭ�� : void NoteCurrentTime();
// ���� : ��
// ���� : ��
// ˵�� : �ú��������ж��Ƿ�ʱ��������������
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
// ���� : TCApplication::NoteAppStartStopLog
// ��; : ��¼Ӧ����ͣ��־
// ԭ�� : void NoteAppStartStopLog(TEStartStopLogType ltType);
// ���� : ��������ֹͣ
// ���� : ��
// ˵�� :
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
// ���� : TCApplication::ParamCount
// ��; : �õ������в���������
// ԭ�� : long ParamCount()
// ���� : ��
// ���� : ��������
// ˵�� :
//==========================================================================
long TCApplication::ParamCount()
{
    return m_argc;
}

//==========================================================================
// ���� : TCApplication::ParamStr
// ��; : �õ������в���������
// ԭ�� : TCString ParamStr(long nIndex);
// ���� : �������
// ���� : ������
// ˵�� : ��0��������Ӧ�ó���������1��������ʼ�������Ĳ���
//==========================================================================
TCString TCApplication::ParamStr(long nIndex)
{
    if (nIndex >= ParamCount())
        throw TCException("PARAMSTR GET PARM ERROR");

    return m_argv[nIndex];
}

//==========================================================================
// ���� : TCApplication::ProcessMessages
// ��; : ����Ҫ���ʱ�����еĹ����е��ã��Դ���һЩ������ˢ����Ĵ���
// ԭ�� : void ProcessMessages()
// ���� : ��
// ���� : ��
// ˵�� : 1. ������������Դ��VCL, �����ǣ����������������κ���Ϣ��
//        2. ԭ���ϣ���һ�����ܺ�ʱ�ϳ�(�ᳬ��һ����)�����м䲻���жϵĺ���
//           �����е���Application.ProcessMessages()
//==========================================================================
void TCApplication::ProcessMessages()
{
   if( (m_tmCreate + 8) < time(NULL) || (m_tmCreate > time(NULL)) )
   {   CreateRunningFile();
       m_tmCreate = time(NULL);
   }

}

//==========================================================================
// ���� : TCApplication::RequestTerminate
// ��; : ��Ҫ�����˳�
// ԭ�� : void RequestTerminate()
// ���� : ��
// ���� : ��
// ˵�� : �������к����п��Խ��е��ã���֪ͨӦ�ó����˳�
//==========================================================================
void TCApplication::RequestTerminate()
{
    m_bNeedTerminate = true;
}

//==========================================================================
// ���� : TCApplication::Run
// ��; : �����г���
// ԭ�� : void Run()
// ���� : ��
// ���� : ��
// ˵�� : �ڸú����н������⴦��ʱ��
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
            signal(SIGINT, SIG_IGN);                    // �жϼ�������
            signal(SIGQUIT, SIG_IGN);                   // ֹͣ��������
            signal(SIGTERM, TerminateApplication);      // ���Kill
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
// ���� : TCApplication::SetNextDelay
// ��; : ���ó�����һ��ʱ��
// ԭ�� : void SetNextDelay(long nNextDelay);
// ���� : ��һ��ʱ��
// ���� : ��
// ˵�� : �������к����п��Ե���֮�������Ժ��������һ���ӳ�ʱ���ӳٱ�
//        ��ָ����ʱ�䡣֮��ȱʡ�ӳ�ʱ��ָ����С����ֵ���һ�����Ѵ�
//        �������ݵ���Ҫ���촦���µ�����ʱʹ�á�
//==========================================================================
void TCApplication::SetNextDelay(long nNextDelay)
{
    m_nNextDelay = nNextDelay;
}

//==========================================================================
// ���� : TCApplication::SetRunningDelay
// ��; : ���ó���ȱʡʱ��
// ԭ�� : void SetRunningDelay(long nRunningDelay);
// ���� : ȱʡʱ��
// ���� : ��
// ˵�� : һ����main()�е��á����������ʱ������������ģʽ��Ϊѭ��
//        ����ģʽ�������ѭ������MainFunc�����ڵ������Ժ�ȴ�����ָ
//        ���ĺ����������û�и���䣬����ֻ�����MainFunc()һ�Ρ�
//==========================================================================
void TCApplication::SetRunningDelay(long nRunningDelay)
{
    m_RunningMode = rmRunningRecursively;

    m_nRunningDelay = nRunningDelay;

}

//==========================================================================
// ���� : TCApplication::SetRunningHandle
// ��; : ���������к���
// ԭ�� : void SetRunningHandle(void (*func)());
// ���� : ��
// ���� : ��
// ˵�� : �ú���һ��Ҫ��main()�е��á����õ������к���ӦΪ�ɵ������е�
//        ��ά���������������к����˳�ʱӦ�ͷ��������һ���ڴ档
//==========================================================================
void TCApplication::SetRunningHandle(void (*func)())
{
    m_RunningFunc = func;

}


