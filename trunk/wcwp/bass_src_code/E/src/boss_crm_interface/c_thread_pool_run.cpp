//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_thread_pool_run.h"
#include "c_handletask.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)
TCThreadPoolRun *STA_pThreadPoolRun = NULL;

//==========================================================================
// ���� : TCThreadPoolRun::ReadIniFile()
// ��; : ��ʼ���ļ����ɳ�ʼ������
// ԭ�� : bool ReadIniFile();
// ���� : ��
// ���� : ��
// ˵�� : �ú����Ӳ����ļ��ж�ȡ���в���,��������Ϸ�,�׳�����
//==========================================================================
bool TCThreadPoolRun::ReadIniFile()
{
    TCString sWorkingTag;
    TCString sListName;
    TCStringList slWorkingList;
    long nInterfaceCount;

    //�������ݿ�
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

    //��ȡ�Ƿ��¼�ű���ÿ��SQL������ʱ��
    mbRecordRunTime = ProfileAppBool(Application.GetAppName(),
                    sWorkingTag, "RecordRunTime", false);
                    
    //��ȡ�Ƿ���Ե����ű���SQLִ�з�������ʱ���Ͷ��Ÿ��й���Ա
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
        //ȡ���Խӿ�ģ����жϣ����������������ļ��������
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
// ���� : TCThreadPoolRun::Initialize
// ��; : ��Ա������ʼ��
// ԭ�� : TCThreadPoolRun::Initialize()
// ���� :
// ���� :
// ˵�� :
//==========================================================================
void TCThreadPoolRun::Initialize()
{
    //��ȡ������Ϣ
    ReadIniFile();

    //���ݿ�����
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
// ���� : TCThreadPoolRun::TCThreadPoolRun
// ��; : ���캯��
// ԭ�� : TCThreadPoolRun()
// ���� :
// ���� :
// ˵�� :
//==========================================================================
TCThreadPoolRun::TCThreadPoolRun()
{
//
    Initialize();    
}

//==========================================================================
// ���� : TCThreadPoolRun::~TCThreadPoolRun()
// ��; : ��������
// ԭ�� : ~TCThreadPoolRun()
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
TCThreadPoolRun::~TCThreadPoolRun()
{
    //�Ͽ����ݿ�����
    m_pQuery->Close();
    delete m_pQuery;
    m_pQuery=NULL;
    
    if (DatabaseMain.IsConnected())
        DatabaseMain.Disconnect();
}

//==========================================================================
// ���� : TCThreadPoolRun::GetNewAThread
// ��; :
// ԭ�� : TCThreadPoolRun * GetNewAThread()
// ���� :
// ���� : TCThreadRunָ��
// ˵�� :
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
// ���� : TCThreadRun::GetNewAThread
// ��; :
// ԭ�� : TCThreadRun * GetNewAThread()
// ���� :
// ���� : TCThreadRunָ��
// ˵�� :
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
// ���� : TCThreadPoolRun::DestroyAThread
// ��; : ��������߳�
// ԭ�� : void DestroyAThread(TCThreadRun *pThreadRun)
// ���� : pServerThread -- �����߳�ָ��
// ���� : ��
// ˵�� :
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
// ���� : GetThreadPoolRun()
// ��; : �õ�һ��StatRun������
// ԭ�� : TCThreadPoolRun & GetThreadPoolRun()
// ���� : ��
// ���� : TCThreadPoolRun������
// ˵�� : ��������TCThreadPoolRun��Ķ���ӿڣ�TCThreadPoolRun����
//        �������ʵ����
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
// ���� : ReleaseThreadPoolRun
// ��; : �ͷ���GetThreadPoolRun()����������ڴ�
// ԭ�� : void ReleaseThreadPoolRun()
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void ReleaseThreadPoolRun()
{
    delete (TCThreadPoolRun *)STA_pThreadPoolRun;
    STA_pThreadPoolRun = NULL;
}


//==========================================================================
// ���� : TCThreadPoolRun::Run
// ��; : �����к����������̳߳���������
// ԭ�� : void Run()
// ���� : ��
// ���� : ��
// ˵�� :
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
        //����������ʱ���ѷ����״̬���������Ϊδ���״̬
        RecoverTaskStatus(m_pQuery);
        STA_bIsFirstTime = false;
    }

    //�������ɵ��߳�
    ClearFinishedThread();

    //m_csListLock.Enter();
    //�ж��Ƿ������µ�����
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
