#include "cmpublic_db.h"

#ifdef  __ORACLECLASS
#include <oci.h>
#endif

#include "c_database.h"

TCDatabase DatabaseMain;

//==========================================================================
// 函数 : TCDatabase::TCDatabase
// 用途 : 构造函数
// 原型 : TCDatabase()
// 参数 : 无
// 返回 : 无
// 说明 : 初始化类的私有变量
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
// 函数 : TCDatabase::~TCDatabase
// 用途 : 析构函数
// 原型 : ~TCDatabase()
// 参数 : 无
// 返回 : 无
// 说明 : 析构时断开数据库连接
//==========================================================================
TCDatabase::~TCDatabase()
{
  Disconnect() ;
}

//==========================================================================
// 函数 : TCDatabase::CheckError
// 用途 : 检查由OCI调用返回的值
// 原型 : void CheckError(sword status);
// 参数 : OCI调用的返回值
// 返回 : 无
// 说明 : 1. 该函数核对数据库错误代码，并可能抛出错误例外
//        2. 目前仅对调用返回值为OCI_ERROR和OCI_INVALID_HANDLE两种情况抛出
//           例外（还要依赖于是否抛出例外的设置），可能以后还要调整
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
// 函数 : TCDatabase::Commit
// 用途 : 提交发送到数据库的事务
// 原型 : bool TCDatabase::Commit(bool fFreeHandle = false);
// 参数 : 是否要释放SQL句柄
// 返回 : 是否成功提交
// 说明 :
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
// 函数 : TCDatabase::Connect
// 用途 : 连接数据库
// 原型 : bool Connect();
// 参数 : 用户名
// 返回 : 数据库连接是否成功
// 说明 : 如果连接不成功，且m_bThrowException为true(缺省)，则抛出例外
//==========================================================================
bool TCDatabase::Connect()
{
    long 	i;
    sword   status;
    OCIDBPROCESS * retProc = NULL;

    //====== 0. 写操作日志 ========
    TCString sLogDes;
    sLogDes = "ServerName=" + m_sServerName + "  UserName=" + m_sUserName
            + "  Password=" + m_sPassword + "  DatabaseName=" + m_sDatabaseName;
    #ifdef _DEBUG
    WriteDBLogFile("CONN", sLogDes);
    #endif

    //======== 1. 初始化OCI操作环境 ======
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

    //====== 2. 分配一个注册入口 ======
    for(i = 0; i < OCIMAX_PROCNUM; i++)
    {
        if(!m_stOCIProcesses[i].InUse)
        {
            m_stOCIProcesses[i].InUse = 1;
            retProc = &(m_stOCIProcesses[i]);
            break;
        }
    }

    //===== 3. 如果数组已满, 报告错误 =======
    if(!retProc)
        throw TCDBException(dbeAllocateDBProcEntry,
                "TCDatabase::Connect : Cannot allocate OCIDBPROCESS entry.") ;

    m_DBProc = retProc ;

    //====== 4. 初始化结构中的各变量，并连接数据库 =======
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

    //====== 5. 成功连接服务器 ========
    m_bConnected = true ;

    return true;

    //======= 6. 连接数据库不成功 =======
ErrExit:
    CheckError(status);
    m_DBProc = NULL ;
    retProc->InUse = 0;

    return false ;
}

//==========================================================================
// 函数 : TCDatabase::ConnectDBViaConfig
// 用途 : 通过配置的设置，连接数据库
// 原型 : void TCDatabase::ConnectDBViaConfig();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCDatabase::ConnectDBViaConfig()
{
    if (!IsConnected())
    {
        SetServerName(GetAppConfigParm("数据库服务名称"));
        SetUserName(GetAppConfigParm("数据库登录用户"));
        SetPassword(GetAppConfigParm("数据库登录口令"));
        SetDBName(GetAppConfigParm("数据库数据库名"));

        Connect();
    }
}

//==========================================================================
// 函数 : TCDatabase::ConnectDBSalesConfig
// 用途 : 通过配置的设置，连接数据库
// 原型 : void TCDatabase::ConnectDBSalesConfig();
// 参数 : 无
// 返回 : 无
// 说明 :
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
// 函数 : TCDatabase::ConnectDBMainCustConfig
// 用途 : 通过配置的设置，连接数据库
// 原型 : void TCDatabase::ConnectDBMainCustConfig();
// 参数 : 无
// 返回 : 无
// 说明 : add by zhoulm in 20030102 
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
// 函数 : TCDatabase::ConnectDBAccountConfig
// 用途 : 通过配置的设置，连接数据库
// 原型 : void TCDatabase::ConnectDBAccountConfig();
// 参数 : 无
// 返回 : 无
// 说明 :
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
// 函数 : TCDatabase::ConnectDBParaConfig
// 用途 : 通过配置的设置，连接数据库
// 原型 : void TCDatabase::ConnectDBParaConfig();
// 参数 : 无
// 返回 : 无
// 说明 :
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
// 函数 : TCDatabase::Disconnect
// 用途 : 断开与数据库的连接，释放分配的句柄
// 原型 : void TCDatabase::Disconnect();
// 参数 : 无
// 返回 : 无
// 说明 :
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

    // 调用Disconnect，socket仍没有断 2002.01.31加上下面一条语句
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
// 函数 : TCDatabase::IsConnected
// 用途 : 得到数据库的连接状态
// 原型 : bool IsConnected();
// 参数 : 无
// 返回 : 是否连接
// 说明 :
//==========================================================================
bool TCDatabase::IsConnected()
{
  return m_bConnected ;
}

//==========================================================================
// 函数 : TCDatabase::GetCommitMode
// 用途 : 得到自动提交模式
// 原型 : bool GetCommitMode();
// 参数 : 无
// 返回 : 是否自动提交
// 说明 :
//==========================================================================
bool TCDatabase::GetCommitMode()
{
  return m_bAutoCommit ;
}

//==========================================================================
// 函数 : TCDatabase::GetDBProc
// 用途 : 得到数据库连接结构指针
// 原型 : OCIDBPROCESS * GetDBProc();
// 参数 : 无
// 返回 : 数据库连接结构指针
// 说明 :
//==========================================================================
OCIDBPROCESS * TCDatabase::GetDBProc()
{
  return m_DBProc ;
}

//==========================================================================
// 函数 : TCDatabase::GetServerName
// 用途 : 得到服务器名
// 原型 : TCString TCDatabase::GetServerName();
// 参数 : 无
// 返回 : 服务器名
// 说明 :
//==========================================================================
TCString TCDatabase::GetServerName()
{
  return m_sServerName ;
}

//==========================================================================
// 函数 : TCDatabase::GetUserName
// 用途 : 得到用户名
// 原型 : TCString TCDatabase::GetUserName();
// 参数 : 无
// 返回 : 用户名
// 说明 :
//==========================================================================
TCString TCDatabase::GetUserName(void)
{
    return m_sUserName ;
}

//==========================================================================
// 函数 : TCDatabase::Rollback
// 用途 : 回滚发送到数据库的事务
// 原型 : bool TCDatabase::Rollback(bool fFreeHandle = false);
// 参数 : 是否要释放SQL句柄
// 返回 : 是否成功回滚
// 说明 :
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
// 函数 : TCDatabase::SetCommitMode
// 用途 : 设置自动提交模式
// 原型 : void SetCommitMode(bool bMode);
// 参数 : 是否自动提交
// 返回 : 无
// 说明 :
//==========================================================================
void TCDatabase::SetCommitMode(bool bMode)
{
    m_bAutoCommit = bMode ;
}

//==========================================================================
// 函数 : TCDatabase::SetPassword
// 用途 : 设置用户登录密码
// 原型 : void SetUserName(TCString sPassword);
// 参数 : 用户登录密码
// 返回 : 无
// 说明 :
//==========================================================================
void TCDatabase::SetPassword(TCString sPassword)
{
    m_sPassword = sPassword ;
}

//==========================================================================
// 函数 : TCDatabase::SetServerName
// 用途 : 设置服务器名
// 原型 : void SetServerName(TCString sServerName);
// 参数 : 服务器名
// 返回 : 无
// 说明 :
//==========================================================================
void TCDatabase::SetServerName(TCString sServerName)
{
  m_sServerName = sServerName ;
}

//==========================================================================
// 函数 : TCDatabase::SetUserName
// 用途 : 设置用户名
// 原型 : void SetUserName(TCString sUserName);
// 参数 : 用户名
// 返回 : 无
// 说明 :
//==========================================================================
void TCDatabase::SetUserName(TCString sUserName)
{
  m_sUserName = sUserName ;
}

//==========================================================================
// 函数 : TCDatabase::SetUserName
// 用途 : 写数据库操作日志
// 原型 : static void WriteDBLogFile(TCString sAction, TCString sSQL);
// 参数 : sAction -- 动作类型
//           SELECT  : 查询语句
//           EXEC    : 执行语句
//           CONN    : 连接数据库
//           DISCONN : 断连数据库
//        sSQL    -- SQL语句, 或其它描述信息
// 返回 : 无
// 说明 :
//==========================================================================
void TCDatabase::WriteDBLogFile(TCString sAction, TCString sSQL)
{
    if (!GetAppConfigBool("数据库操作日志"))
        return;

    //======= 1. 得到数据库日志文件名 ======
    TCString sLogFileName;

    sLogFileName = MergePathAndFile(TAppPath::AppLog(), "db_log.mlg");

    //======= 2. 如果文件不存在, 则创建之 =====
    if (!FileExists(sLogFileName))
    {   TCDBFCreate dcCreate(sLogFileName, 4);
        dcCreate.AddField("program",    'C', 16);
        dcCreate.AddField("time",       'C', 14);
        dcCreate.AddField("action",     'C', 8);
        dcCreate.AddField("sql",        'C', 255);
        dcCreate.CreateDBF();
    }

    //====== 3. 增加日志项 =========
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

    //====== 4. 如果文件过大，则截短1/3 =====
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
// 函数 : TCDatabase::TCDBException
// 用途 : 数据库例外的构造函数
// 原型 : TCDBException(TEDBErrorCode dbeErrorStatus,
//              TCString sMsg, sb4 nDBErrorCode = 0)
// 参数 : 错误状态(数据库调用返回值), 错误信息描述, 错误代码
// 返回 : 无
// 说明 :
//==========================================================================
TCDBException::TCDBException(TEDBErrorCode dbeErrorStatus,
        TCString sMsg, sb4 nDBErrorCode) : TCException(sMsg)
{
    m_nDBErrorCode = nDBErrorCode;
    m_dbeErrorStatus = dbeErrorStatus;

    if (nDBErrorCode != 0)
        m_sMessage += "    DBErrorCode - " + IntToStr(nDBErrorCode);
}







