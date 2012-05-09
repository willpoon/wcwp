//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_thread_pool_run.h"
#include "c_handletask.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)
TCThreadPoolRun *STA_pThreadPoolRun = NULL;

//==========================================================================
// 函数 : TCThreadPoolRun::ReadIniFile()
// 用途 : 初始化文件生成初始化参数
// 原型 : bool ReadIniFile();
// 参数 : 无
// 返回 : 无
// 说明 : 该函数从参数文件中读取运行参数,如参数不合法,抛出例外
//==========================================================================
bool TCThreadPoolRun::ReadIniFile()
{
    TCString sWorkingTag;
    TCString sListName;
    TCStringList slWorkingList;
    long nInterfaceCount;

    //连接数据库
    //***==== Get database config ===***
    sWorkingTag = "Database";
    m_tsTaskDBCon.sDBServer = ProfileAppString(Application.GetAppName(),
        sWorkingTag, "ServerName", "");
    ASSERT(m_tsTaskDBCon.sDBServer != TCString("") ) ;

    m_tsTaskDBCon.sDBName = ProfileAppString(Application.GetAppName(),
        sWorkingTag, "Database", "");
    ASSERT(m_tsTaskDBCon.sDBName != TCString("") );

    m_tsTaskDBCon.sDBUser = ProfileAppString(Application.GetAppName(),
        sWorkingTag, "LogID", "");
    ASSERT(m_tsTaskDBCon.sDBUser != TCString("") );

    m_tsTaskDBCon.sDBPass = ProfileAppString(Application.GetAppName(),
        sWorkingTag, "LogPassword", "");
    ASSERT(m_tsTaskDBCon.sDBPass != TCString("") );

    //***==== Get application config ===***
    sWorkingTag = "Application";
    mnMaxThread = ProfileAppInt(Application.GetAppName(),
        sWorkingTag, "MaxProcess", 14);
    if (mnMaxThread>100)
    {
        mnMaxThread = 14;
    }

    //获取是否记录脚本中每个SQL的运行时间
    mbRecordRunTime = ProfileAppBool(Application.GetAppName(),
                    sWorkingTag, "RecordRunTime", false);
                    
    //获取是否针对单个脚本中SQL执行发生错误时发送短信给有关人员
    mbSendErrorMsg = ProfileAppBool(Application.GetAppName(),
                    sWorkingTag, "SendErrorMsg", false);                    

    //***==== Get Source config ===***
    sWorkingTag = "Source";
    m_sWorkingDir = ProfileAppString(Application.GetAppName(), sWorkingTag,
        "directory", "");
    ASSERT(m_sWorkingDir != TCString("") );
    ForceDirectoriesE(m_sWorkingDir);
    m_sSendDir = ProfileAppString(Application.GetAppName(), sWorkingTag,
        "send_dir", "");
    ASSERT(m_sSendDir != TCString("") );
    ForceDirectoriesE(m_sSendDir);

    sListName = ProfileAppString(Application.GetAppName(), sWorkingTag,
        "list_name", "");

    ProfileAppSession(Application.GetAppName(), sListName, slWorkingList);
    nInterfaceCount = slWorkingList.GetCount();
    for (long i=0; i<nInterfaceCount; i++)
    {
        sWorkingTag = slWorkingList[i];
        TSInterfaceConfig& tsConfig = m_tsInterfaceConfig[i];
        TCString sInterfaceType;
        sInterfaceType = ProfileAppString(Application.GetAppName(),
            sWorkingTag, "interface_type", "");
        
        //modify by lgk at 20060726
        //取消对接口模块的判断，这样限制了配置文件的灵活性
        //if (sInterfaceType != "ZK" && sInterfaceType != "JF" &&
        //    sInterfaceType != "WJJS" && sInterfaceType != "CSC")
        //    throw TCException("TCThreadPoolRun::ReadIniFile() interface_type is invalid");
        //end of modify

        tsConfig.sInterfaceType = sInterfaceType;
        //=====Read database server name from ini file
        tsConfig.tsDBCon.sDBServer = ProfileAppString(Application.GetAppName(),
            sWorkingTag, "server", "");
        ASSERT(tsConfig.tsDBCon.sDBServer != TCString("") );

        //=====Read database name from ini file
        tsConfig.tsDBCon.sDBName = ProfileAppString(Application.GetAppName(),
            sWorkingTag, "dbname", "");
        ASSERT(tsConfig.tsDBCon.sDBName != TCString("") );

        //=====Read database user from ini file
        tsConfig.tsDBCon.sDBUser = ProfileAppString(Application.GetAppName(),
            sWorkingTag, "dbuser", "");
        ASSERT(tsConfig.tsDBCon.sDBUser != TCString("") );

        //=====Read database password from ini file
        tsConfig.tsDBCon.sDBPass = ProfileAppString(Application.GetAppName(),
            sWorkingTag, "dbpass", "");
        ASSERT(tsConfig.tsDBCon.sDBPass != TCString("") );
    }

    return true;
}

//==========================================================================
// 函数 : TCThreadPoolRun::Initialize
// 用途 : 成员变量初始化
// 原型 : TCThreadPoolRun::Initialize()
// 参数 :
// 返回 :
// 说明 :
//==========================================================================
void TCThreadPoolRun::Initialize()
{
    //读取配置信息
    ReadIniFile();

    //数据库连接
    DatabaseMain.SetServerName(m_tsTaskDBCon.sDBServer);
    DatabaseMain.SetDBName(m_tsTaskDBCon.sDBName);
    DatabaseMain.SetUserName(m_tsTaskDBCon.sDBUser);
    DatabaseMain.SetPassword(m_tsTaskDBCon.sDBPass);
    if (!DatabaseMain.Connect())
        return;
    ASSERT(DatabaseMain.IsConnected());
    m_pQuery = new TCQuery(DatabaseMain);    
}

//==========================================================================
// 函数 : TCThreadPoolRun::TCThreadPoolRun
// 用途 : 构造函数
// 原型 : TCThreadPoolRun()
// 参数 :
// 返回 :
// 说明 :
//==========================================================================
TCThreadPoolRun::TCThreadPoolRun()
{
//
    Initialize();    
}

//==========================================================================
// 函数 : TCThreadPoolRun::~TCThreadPoolRun()
// 用途 : 析构函数
// 原型 : ~TCThreadPoolRun()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
TCThreadPoolRun::~TCThreadPoolRun()
{
    //断开数据库连接
    m_pQuery->Close();
    delete m_pQuery;
    m_pQuery=NULL;
    
    if (DatabaseMain.IsConnected())
        DatabaseMain.Disconnect();
}

//==========================================================================
// 函数 : TCThreadPoolRun::GetNewAThread
// 用途 :
// 原型 : TCThreadPoolRun * GetNewAThread()
// 参数 :
// 返回 : TCThreadRun指针
// 说明 :
//==========================================================================
TCThreadRun * TCThreadPoolRun::GetNewAThread()
{
    TCThreadRun *pThreadRun;
    try
    {
        pThreadRun = new TCThreadRun();

    }
    catch(...)
    {
        throw TCException("TCThreadPoolRun::GetNewAThread() Error:"
                          " can not create a new thread");
    }
    return pThreadRun;
}

//==========================================================================
// 函数 : TCThreadRun::GetNewAThread
// 用途 :
// 原型 : TCThreadRun * GetNewAThread()
// 参数 :
// 返回 : TCThreadRun指针
// 说明 :
//==========================================================================
TCThreadRun * TCThreadPoolRun::GetNewAThread(const TSDBConnectConfig tsWorkDBCon,
const TSDBConnectConfig tsTaskDBCon,const bool bRecordRunTime,const bool bSendErrorMsg)
{
    TCThreadRun *pThreadRun;
    try
    {
        pThreadRun = new TCThreadRun(tsWorkDBCon,tsTaskDBCon,bRecordRunTime,bSendErrorMsg);

    }
    catch(...)
    {
    	DestroyAThread(pThreadRun);
        throw TCException("TCThreadPoolRun::GetNewAThread() Error:"
                          " can not create a new thread");
    }
    return pThreadRun;
}

//==========================================================================
// 函数 : TCThreadPoolRun::DestroyAThread
// 用途 : 清除服务线程
// 原型 : void DestroyAThread(TCThreadRun *pThreadRun)
// 参数 : pServerThread -- 服务线程指针
// 返回 : 无
// 说明 :
//==========================================================================
void TCThreadPoolRun::DestroyAThread(TCThreadRun *pThreadRun)
{
	if (pThreadRun != NULL)
	{
	    delete (TCThreadRun *)pThreadRun;
	    pThreadRun = NULL;
    }
}

//==========================================================================
// 函数 : GetThreadPoolRun()
// 用途 : 得到一个StatRun的引用
// 原型 : TCThreadPoolRun & GetThreadPoolRun()
// 参数 : 无
// 返回 : TCThreadPoolRun的引用
// 说明 : 本函数是TCThreadPoolRun类的对外接口，TCThreadPoolRun不再
//        创建别的实例。
//==========================================================================
TCThreadPoolRun & GetThreadPoolRun()
{
    if (STA_pThreadPoolRun == NULL)
    {
        try
        {
            STA_pThreadPoolRun = new TCThreadPoolRun;
        }
        catch(...)
        {
            throw TCException("unable create TCThreadPoolRun instant! can not new");
        }
        Application.InstallExitHandle(ReleaseThreadPoolRun);
    }
    return *STA_pThreadPoolRun;
}

//==========================================================================
// 函数 : ReleaseThreadPoolRun
// 用途 : 释放由GetThreadPoolRun()调用申请的内存
// 原型 : void ReleaseThreadPoolRun()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void ReleaseThreadPoolRun()
{
    delete (TCThreadPoolRun *)STA_pThreadPoolRun;
    STA_pThreadPoolRun = NULL;
}


//==========================================================================
// 函数 : TCThreadPoolRun::Run
// 用途 : 主运行函数，创建线程池启动服务
// 原型 : void Run()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCThreadPoolRun::Run()
{
    TCThreadRun *pThreadRun;
    long nScriptId;
    TCString sTaskId,sCycleId,sModule,sScriptName;
    TCString sDBServer,sDBName,sDBUser,sDBPass;
    TSDBConnectConfig tsDBCon;
    ASSERT(DatabaseMain.IsConnected());
    ASSERT(m_pQuery != NULL);
    
    static bool STA_bIsFirstTime = true;
    if (STA_bIsFirstTime)
    {
        //刚启动程序时，把非完成状态的任务更新为未完成状态
        RecoverTaskStatus(m_pQuery);
        STA_bIsFirstTime = false;
    }

    //清除已完成的线程
    ClearFinishedThread();

    //m_csListLock.Enter();
    //判断是否生成新的任务
    IsFrequencyChanged(m_pQuery);
    //m_csListLock.Leave();

    while (true)
    {
        if (m_lstThreadList.GetCount()>=mnMaxThread) break;

        //m_csListLock.Enter();
        bool bHaveTask=GetTask(m_pQuery,sTaskId,nScriptId,sCycleId,sModule,sScriptName);
        //m_csListLock.Leave();

        if (bHaveTask)
        {
            UpdateTaskStatus(m_pQuery,nScriptId,sCycleId,TASK_STATUS_MARKED);
            for (long i=0; i<MAX_INTERFACE_TYPE_COUNT; i++)
            {
                if (sModule==m_tsInterfaceConfig[i].sInterfaceType)
                {
                    tsDBCon=m_tsInterfaceConfig[i].tsDBCon;
                    break;
                }
            }
            pThreadRun = GetNewAThread(tsDBCon,m_tsTaskDBCon,mbRecordRunTime,mbSendErrorMsg);
            pThreadRun->SetWorkingDir(m_sWorkingDir);
            pThreadRun->SetSendDir(m_sSendDir);
            pThreadRun->SetTaskId(sTaskId);
            pThreadRun->SetScriptId(nScriptId);
            pThreadRun->SetCycleId(sCycleId);
            pThreadRun->SetScriptName(sScriptName);

            pThreadRun->Start(false);
            AddThread(pThreadRun);
            pThreadRun->SleepMilliSeconds(1000);
        }
        else
        {
            break;
        }
    }

    return;
}
