#include "cmpublic_db.h"

#ifdef  __ORACLECLASS
#include <oci.h>
#endif

#include "c_database.h"

TCDatabase DatabaseMain;

//==========================================================================
// ���� : TCDatabase::TCDatabase
// ��; : ���캯��
// ԭ�� : TCDatabase()
// ���� : ��
// ���� : ��
// ˵�� : ��ʼ�����˽�б���
//==========================================================================
TCDatabase::TCDatabase()
{
	m_sServerName	  = "" ;
	m_sUserName	      = "" ;
	m_sPassword	      = "" ;
    m_sDatabaseName   = "" ;
    m_sErrorString    = "" ;
    m_DBProc		  = NULL ;

	m_bAutoCommit	  = true ;
    m_bConnected      = false ;
    m_bOCIInited      = false ;
    m_nErrorCode      = 0 ;
    m_nOCIStatus      = 0 ;
    m_bThrowException = true ;
}

//==========================================================================
// ���� : TCDatabase::~TCDatabase
// ��; : ��������
// ԭ�� : ~TCDatabase()
// ���� : ��
// ���� : ��
// ˵�� : ����ʱ�Ͽ����ݿ�����
//==========================================================================
TCDatabase::~TCDatabase()
{
  Disconnect() ;
}

//==========================================================================
// ���� : TCDatabase::CheckError
// ��; : �����OCI���÷��ص�ֵ
// ԭ�� : void CheckError(sword status);
// ���� : OCI���õķ���ֵ
// ���� : ��
// ˵�� : 1. �ú����˶����ݿ������룬�������׳���������
//        2. Ŀǰ���Ե��÷���ֵΪOCI_ERROR��OCI_INVALID_HANDLE��������׳�
//           ���⣨��Ҫ�������Ƿ��׳���������ã��������Ժ�Ҫ����
//==========================================================================
void TCDatabase::CheckError(sword status)
{
    TCString  errbuf = "";
    text 	  errtmp[512];
    sb4 	  errcode = 0;

    switch(status)
    {
        case OCI_SUCCESS:
            break;

        case OCI_SUCCESS_WITH_INFO:
            errbuf = "Error - OCI_SUCCESS_WITH_INFO" ;
            break;

        case OCI_NEED_DATA:
            errbuf = "Error - OCI_NEED_DATA" ;
            break;

        case OCI_NO_DATA:
            errbuf = "Error - OCI_NODATA" ;
            break;

        case OCI_ERROR:
            if (m_DBProc )
            {
                OCIErrorGet((dvoid *)m_DBProc->phError, (ub4)1, (text *) NULL,
                        &errcode, errtmp, (ub4)sizeof(errtmp), OCI_HTYPE_ERROR);

                errbuf = "Error - " + TCString((char *)errtmp) ;

                if (m_bThrowException)
                      throw TCDBException(dbeCallStatement, errbuf, errcode);
            }
            else
                throw TCDBException(dbeNotActiveDB,
                        "OCI_Error - Not Active DB");
            break;

        case OCI_INVALID_HANDLE:
            errbuf = "Error - OCI_INVALID_HANDLE" ;

            if (m_bThrowException)
                throw TCDBException(dbeInvalidHandle, errbuf);

            break;

        case OCI_STILL_EXECUTING:
            errbuf = "Error - OCI_STILL_EXECUTE" ;
            break;

        case OCI_CONTINUE:
            errbuf = "Error - OCI_CONTINUE" ;
            break;

        default:
            break;
    }

     m_nErrorCode    = errcode ;
     m_sErrorString  = errbuf ;
     m_nOCIStatus    = status ;
}

//==========================================================================
// ���� : TCDatabase::Commit
// ��; : �ύ���͵����ݿ������
// ԭ�� : bool TCDatabase::Commit(bool fFreeHandle = false);
// ���� : �Ƿ�Ҫ�ͷ�SQL���
// ���� : �Ƿ�ɹ��ύ
// ˵�� :
//==========================================================================
bool TCDatabase::Commit(bool fFreeHandle)
{
     sword status ;

     status = OCITransCommit(m_DBProc->phService,m_DBProc->phError,(ub4)0);

     if (status != 0)
     {
        CheckError(status) ;
        return false ;
     }

    if (m_DBProc->phSQL != NULL && fFreeHandle)
    {
        OCIHandleFree((dvoid *)m_DBProc->phSQL, OCI_HTYPE_STMT);
        m_DBProc->phSQL = NULL;
    }

    return true ;
}

//==========================================================================
// ���� : TCDatabase::Connect
// ��; : �������ݿ�
// ԭ�� : bool Connect();
// ���� : �û���
// ���� : ���ݿ������Ƿ�ɹ�
// ˵�� : ������Ӳ��ɹ�����m_bThrowExceptionΪtrue(ȱʡ)�����׳�����
//==========================================================================
bool TCDatabase::Connect()
{
    long 	i;
    sword   status;
    OCIDBPROCESS * retProc = NULL;

    //====== 0. д������־ ========
    TCString sLogDes;
    sLogDes = "ServerName=" + m_sServerName + "  UserName=" + m_sUserName
            + "  Password=" + m_sPassword + "  DatabaseName=" + m_sDatabaseName;
    #ifdef _DEBUG
    WriteDBLogFile("CONN", sLogDes);
    #endif

    //======== 1. ��ʼ��OCI�������� ======
    if(!m_bOCIInited)
    {
        for (i = 0; i < OCIMAX_PROCNUM; i++)
        {
            m_stOCIProcesses[i].InUse		= 0;
            m_stOCIProcesses[i].phEnviron	= NULL;
            m_stOCIProcesses[i].phError		= NULL;
            m_stOCIProcesses[i].phServer	= NULL;
            m_stOCIProcesses[i].phService	= NULL;
            m_stOCIProcesses[i].phSession	= NULL;
            m_stOCIProcesses[i].phSQL	= NULL;
        }

        (void)OCIInitialize((ub4)OCI_DEFAULT, (dvoid *)0,
                          (dvoid * (*)(dvoid *, size_t)) 0,
                          (dvoid * (*)(dvoid *, dvoid *, size_t))0,
                          (void (*)(dvoid *, dvoid *)) 0 );

        m_bOCIInited = true;
    }

    //====== 2. ����һ��ע����� ======
    for(i = 0; i < OCIMAX_PROCNUM; i++)
    {
        if(!m_stOCIProcesses[i].InUse)
        {
            m_stOCIProcesses[i].InUse = 1;
            retProc = &(m_stOCIProcesses[i]);
            break;
        }
    }

    //===== 3. �����������, ������� =======
    if(!retProc)
        throw TCDBException(dbeAllocateDBProcEntry,
                "TCDatabase::Connect : Cannot allocate OCIDBPROCESS entry.") ;

    m_DBProc = retProc ;

    //====== 4. ��ʼ���ṹ�еĸ����������������ݿ� =======
    OCIEnvInit((OCIEnv **)&retProc->phEnviron, OCI_DEFAULT,
               (size_t) 0, (dvoid **) 0 );

    OCIHandleAlloc((dvoid *)retProc->phEnviron,
                   (dvoid **)&retProc->phError,
                    OCI_HTYPE_ERROR, (size_t)0, (dvoid **)0);

    if ((status = OCIHandleAlloc((dvoid *)retProc->phEnviron,
                   (dvoid **)&retProc->phServer,
                   OCI_HTYPE_SERVER, (size_t) 0, (dvoid **)0)) != OCI_SUCCESS)
        goto ErrExit;

    if ((status = OCIHandleAlloc((dvoid *)retProc->phEnviron,
                   (dvoid **)&retProc->phService,
                     OCI_HTYPE_SVCCTX, (size_t)0, (dvoid **)0)) != OCI_SUCCESS)
        goto ErrExit;

    if ((status = OCIServerAttach(retProc->phServer, retProc->phError,
            (text *)((char *)m_sServerName), Length(m_sServerName),0))
            != OCI_SUCCESS)
        goto ErrExit;

    if ((status = OCIAttrSet((dvoid *)retProc->phService, OCI_HTYPE_SVCCTX,
                (dvoid *)retProc->phServer, (ub4)0, OCI_ATTR_SERVER,
                (OCIError *)retProc->phError)) != OCI_SUCCESS)
        goto ErrExit;

    if ((status = OCIHandleAlloc((dvoid *)retProc->phEnviron,
            (dvoid **)&retProc->phSession, (ub4)OCI_HTYPE_SESSION,
            (size_t)0,(dvoid **)0)) != OCI_SUCCESS)
        goto ErrExit;

    if((status = OCIAttrSet((dvoid *)retProc->phSession,(ub4) OCI_HTYPE_SESSION,
            (dvoid *)((char *)m_sUserName),(ub4) Length(m_sUserName),
            (ub4)OCI_ATTR_USERNAME, retProc->phError)) != OCI_SUCCESS)
        goto ErrExit;

    if((status = OCIAttrSet((dvoid *)retProc->phSession,(ub4) OCI_HTYPE_SESSION,
            (dvoid *)((char *)m_sPassword), (ub4)Length(m_sPassword),
            (ub4)OCI_ATTR_PASSWORD, retProc->phError)) != OCI_SUCCESS)
        goto ErrExit;

    if ((status = OCISessionBegin(retProc->phService,retProc->phError,
            retProc->phSession, OCI_CRED_RDBMS,(ub4) OCI_DEFAULT))
            != OCI_SUCCESS)
        goto ErrExit;

    if((status = OCIAttrSet((dvoid *)retProc->phService,(ub4)OCI_HTYPE_SVCCTX,
                     (dvoid *)retProc->phSession,(ub4) 0,
                     (ub4)OCI_ATTR_SESSION,retProc->phError)) != OCI_SUCCESS)
        goto ErrExit;

    //====== 5. �ɹ����ӷ����� ========
    m_bConnected = true ;

    return true;

    //======= 6. �������ݿⲻ�ɹ� =======
ErrExit:
    CheckError(status);
    m_DBProc = NULL ;
    retProc->InUse = 0;

    return false ;
}

//==========================================================================
// ���� : TCDatabase::ConnectDBViaConfig
// ��; : ͨ�����õ����ã��������ݿ�
// ԭ�� : void TCDatabase::ConnectDBViaConfig();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCDatabase::ConnectDBViaConfig()
{
    if (!IsConnected())
    {
        SetServerName(GetAppConfigParm("���ݿ��������"));
        SetUserName(GetAppConfigParm("���ݿ��¼�û�"));
        SetPassword(GetAppConfigParm("���ݿ��¼����"));
        SetDBName(GetAppConfigParm("���ݿ����ݿ���"));

        Connect();
    }
}

//==========================================================================
// ���� : TCDatabase::ConnectDBSalesConfig
// ��; : ͨ�����õ����ã��������ݿ�
// ԭ�� : void TCDatabase::ConnectDBSalesConfig();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCDatabase::ConnectDBSalesConfig()
{
    if (!IsConnected())
    {
        SetServerName(GetAppConfigParm("SalesServerName"));
        SetUserName(GetAppConfigParm("SalesLogID"));
        SetPassword(GetAppConfigParm("SalesPasswd"));
        SetDBName(GetAppConfigParm("SalesDB"));

        Connect();
    }
}

//==========================================================================
// ���� : TCDatabase::ConnectDBMainCustConfig
// ��; : ͨ�����õ����ã��������ݿ�
// ԭ�� : void TCDatabase::ConnectDBMainCustConfig();
// ���� : ��
// ���� : ��
// ˵�� : add by zhoulm in 20030102 
//==========================================================================
void TCDatabase::ConnectDBMainCustConfig()
{
    if (!IsConnected())
    {
        SetServerName(GetAppConfigParm("MainCustServerName"));
        SetUserName(GetAppConfigParm("MainCustLogID"));
        SetPassword(GetAppConfigParm("MainCustPasswd"));
        SetDBName(GetAppConfigParm("MainCustDB"));

        Connect();
    }
}

//==========================================================================
// ���� : TCDatabase::ConnectDBAccountConfig
// ��; : ͨ�����õ����ã��������ݿ�
// ԭ�� : void TCDatabase::ConnectDBAccountConfig();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCDatabase::ConnectDBAccountConfig()
{
    if (!IsConnected())
    {
        SetServerName(GetAppConfigParm("AccountServerName"));
        SetUserName(GetAppConfigParm("AccountLogID"));
        SetPassword(GetAppConfigParm("AccountPasswd"));
        SetDBName(GetAppConfigParm("AccountDB"));

        Connect();
    }
}

//==========================================================================
// ���� : TCDatabase::ConnectDBParaConfig
// ��; : ͨ�����õ����ã��������ݿ�
// ԭ�� : void TCDatabase::ConnectDBParaConfig();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCDatabase::ConnectDBParaConfig()
{
    if (!IsConnected())
    {
        SetServerName(GetAppConfigParm("ParaServerName"));
        SetUserName(GetAppConfigParm("ParaLogID"));
        SetPassword(GetAppConfigParm("ParaPasswd"));
        SetDBName(GetAppConfigParm("ParaDB"));

        Connect();
    }
}


//==========================================================================
// ���� : TCDatabase::Disconnect
// ��; : �Ͽ������ݿ�����ӣ��ͷŷ���ľ��
// ԭ�� : void TCDatabase::Disconnect();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCDatabase::Disconnect()
{
    long i;

    if (m_DBProc == NULL)
        return;

    for (i = 0; i < OCIMAX_PROCNUM; i++)
    {
        if (&(m_stOCIProcesses[i]) == m_DBProc)
            break;
    }

    if (i == OCIMAX_PROCNUM)
        return;

    //modified by liqing on 2002.12.08
//    WriteDBLogFile("DISCONN", "");

    if (m_DBProc->phSQL != NULL)
    {
        OCIHandleFree((dvoid *)m_DBProc->phSQL,OCI_HTYPE_STMT);
        m_DBProc->phSQL = NULL;
    }

    (void) OCISessionEnd(m_DBProc->phService, m_DBProc->phError,
            m_DBProc->phSession, (ub4) OCI_DEFAULT );

    // ����Disconnect��socket��û�ж� 2002.01.31��������һ�����
    if (m_DBProc->phServer)
        (void) OCIServerDetach(m_DBProc->phServer,
                m_DBProc->phError, (ub4) OCI_DEFAULT);

    if (m_DBProc->phError)
        (void)OCIHandleFree(m_DBProc->phError,OCI_HTYPE_ERROR) ;
    if (m_DBProc->phSession)
        (void)OCIHandleFree(m_DBProc->phSession,OCI_HTYPE_SESSION) ;
    if (m_DBProc->phService)
        (void)OCIHandleFree(m_DBProc->phService,OCI_HTYPE_SVCCTX) ;

    if (m_DBProc->phEnviron)
        (void)OCIHandleFree(m_DBProc->phEnviron, OCI_HTYPE_ENV);

    m_stOCIProcesses[i].InUse = 0;
    m_stOCIProcesses[i].phEnviron = NULL;

    m_bConnected = false ;

    m_DBProc = NULL ;
}

//==========================================================================
// ���� : TCDatabase::IsConnected
// ��; : �õ����ݿ������״̬
// ԭ�� : bool IsConnected();
// ���� : ��
// ���� : �Ƿ�����
// ˵�� :
//==========================================================================
bool TCDatabase::IsConnected()
{
  return m_bConnected ;
}

//==========================================================================
// ���� : TCDatabase::GetCommitMode
// ��; : �õ��Զ��ύģʽ
// ԭ�� : bool GetCommitMode();
// ���� : ��
// ���� : �Ƿ��Զ��ύ
// ˵�� :
//==========================================================================
bool TCDatabase::GetCommitMode()
{
  return m_bAutoCommit ;
}

//==========================================================================
// ���� : TCDatabase::GetDBProc
// ��; : �õ����ݿ����ӽṹָ��
// ԭ�� : OCIDBPROCESS * GetDBProc();
// ���� : ��
// ���� : ���ݿ����ӽṹָ��
// ˵�� :
//==========================================================================
OCIDBPROCESS * TCDatabase::GetDBProc()
{
  return m_DBProc ;
}

//==========================================================================
// ���� : TCDatabase::GetServerName
// ��; : �õ���������
// ԭ�� : TCString TCDatabase::GetServerName();
// ���� : ��
// ���� : ��������
// ˵�� :
//==========================================================================
TCString TCDatabase::GetServerName()
{
  return m_sServerName ;
}

//==========================================================================
// ���� : TCDatabase::GetUserName
// ��; : �õ��û���
// ԭ�� : TCString TCDatabase::GetUserName();
// ���� : ��
// ���� : �û���
// ˵�� :
//==========================================================================
TCString TCDatabase::GetUserName(void)
{
    return m_sUserName ;
}

//==========================================================================
// ���� : TCDatabase::Rollback
// ��; : �ع����͵����ݿ������
// ԭ�� : bool TCDatabase::Rollback(bool fFreeHandle = false);
// ���� : �Ƿ�Ҫ�ͷ�SQL���
// ���� : �Ƿ�ɹ��ع�
// ˵�� :
//==========================================================================
bool TCDatabase::Rollback(bool fFreeHandle)
{
    sword status ;

    status = OCITransRollback(m_DBProc->phService,m_DBProc->phError,
            (ub4)OCI_DEFAULT);

    if (status != 0)
    {
        CheckError(status) ;
        return false ;
    }
    if (m_DBProc->phSQL != NULL && fFreeHandle)
    {
        OCIHandleFree((dvoid *)m_DBProc->phSQL,OCI_HTYPE_STMT);
        m_DBProc->phSQL = NULL;
    }
    return true ;
}

//==========================================================================
// ���� : TCDatabase::SetCommitMode
// ��; : �����Զ��ύģʽ
// ԭ�� : void SetCommitMode(bool bMode);
// ���� : �Ƿ��Զ��ύ
// ���� : ��
// ˵�� :
//==========================================================================
void TCDatabase::SetCommitMode(bool bMode)
{
    m_bAutoCommit = bMode ;
}

//==========================================================================
// ���� : TCDatabase::SetPassword
// ��; : �����û���¼����
// ԭ�� : void SetUserName(TCString sPassword);
// ���� : �û���¼����
// ���� : ��
// ˵�� :
//==========================================================================
void TCDatabase::SetPassword(TCString sPassword)
{
    m_sPassword = sPassword ;
}

//==========================================================================
// ���� : TCDatabase::SetServerName
// ��; : ���÷�������
// ԭ�� : void SetServerName(TCString sServerName);
// ���� : ��������
// ���� : ��
// ˵�� :
//==========================================================================
void TCDatabase::SetServerName(TCString sServerName)
{
  m_sServerName = sServerName ;
}

//==========================================================================
// ���� : TCDatabase::SetUserName
// ��; : �����û���
// ԭ�� : void SetUserName(TCString sUserName);
// ���� : �û���
// ���� : ��
// ˵�� :
//==========================================================================
void TCDatabase::SetUserName(TCString sUserName)
{
  m_sUserName = sUserName ;
}

//==========================================================================
// ���� : TCDatabase::SetUserName
// ��; : д���ݿ������־
// ԭ�� : static void WriteDBLogFile(TCString sAction, TCString sSQL);
// ���� : sAction -- ��������
//           SELECT  : ��ѯ���
//           EXEC    : ִ�����
//           CONN    : �������ݿ�
//           DISCONN : �������ݿ�
//        sSQL    -- SQL���, ������������Ϣ
// ���� : ��
// ˵�� :
//==========================================================================
void TCDatabase::WriteDBLogFile(TCString sAction, TCString sSQL)
{
    if (!GetAppConfigBool("���ݿ������־"))
        return;

    //======= 1. �õ����ݿ���־�ļ��� ======
    TCString sLogFileName;

    sLogFileName = MergePathAndFile(TAppPath::AppLog(), "db_log.mlg");

    //======= 2. ����ļ�������, �򴴽�֮ =====
    if (!FileExists(sLogFileName))
    {   TCDBFCreate dcCreate(sLogFileName, 4);
        dcCreate.AddField("program",    'C', 16);
        dcCreate.AddField("time",       'C', 14);
        dcCreate.AddField("action",     'C', 8);
        dcCreate.AddField("sql",        'C', 255);
        dcCreate.CreateDBF();
    }

    //====== 3. ������־�� =========
    TCString sProgram, dtTime;
    sProgram = Application.GetAppName();
    dtTime = TCTime::Now();

    TCFoxDBF fdFoxDBF;
    fdFoxDBF.AttachFile(sLogFileName);
    fdFoxDBF.DBBind("program", sProgram);
    fdFoxDBF.DBBind("time",    dtTime);
    fdFoxDBF.DBBind("action",  sAction);
    fdFoxDBF.DBBind("sql",     sSQL);

    fdFoxDBF.Append();

    //====== 4. ����ļ�������ض�1/3 =====
    if (fdFoxDBF.RecCount() >= 10000)
    {
        long i;
        for (i = 1; i <= fdFoxDBF.RecCount() / 3; i++)
        {
            fdFoxDBF.Go(i);
            fdFoxDBF.Delete();
        }

        fdFoxDBF.Pack();
    }

    fdFoxDBF.CloseDBF();
}

//==========================================================================
// ���� : TCDatabase::TCDBException
// ��; : ���ݿ�����Ĺ��캯��
// ԭ�� : TCDBException(TEDBErrorCode dbeErrorStatus,
//              TCString sMsg, sb4 nDBErrorCode = 0)
// ���� : ����״̬(���ݿ���÷���ֵ), ������Ϣ����, �������
// ���� : ��
// ˵�� :
//==========================================================================
TCDBException::TCDBException(TEDBErrorCode dbeErrorStatus,
        TCString sMsg, sb4 nDBErrorCode) : TCException(sMsg)
{
    m_nDBErrorCode = nDBErrorCode;
    m_dbeErrorStatus = dbeErrorStatus;

    if (nDBErrorCode != 0)
        m_sMessage += "    DBErrorCode - " + IntToStr(nDBErrorCode);
}







