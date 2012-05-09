#ifndef c_databaseH
#define c_databaseH

#include "cmpublic.h"
#include "cmpublic_db.h"

#ifdef  __ORACLECLASS
#include <oci.h>

const long OCIMAX_PROCNUM = 10;     //OCI最大数据库进程数

const long OCIMAX_COLUMNNUM = 100;  // OCI最大列数

//--------------------------------------------------------------------------
// 结构 : OCIDBPROCESS
// 说明 : 封装OCI访问数据库的一组环境参数
//--------------------------------------------------------------------------
struct OCIDBPROCESS
{
  OCIEnv     *phEnviron;      // 环境句柄指针   
  OCIError   *phError;        // 错误句柄指针   
  OCIServer  *phServer;       // 服务器句柄指针 
  OCISvcCtx  *phService;      // 服务句柄指针   
  OCISession *phSession;      // 会话句柄指针   
  OCIStmt    *phSQL;          // SQL句柄
  OCIDefine  *phDefine[OCIMAX_COLUMNNUM];   // 变量定义句柄     
  OCIBind    *BindByPos[OCIMAX_COLUMNNUM] ; // 绑定输出变量数组 
  OCIInd     NullFlag[OCIMAX_COLUMNNUM];    // NULL标志         
  int        ExecFlag;                      // SQL语句是否执行  
  int        InUse;                         // 是否在用标志     
} ;

#endif

//------------------------------------------------------------------------------
// 类   : TCDatabase
// 说明 : 封装了对数据库的连接, 断开和其他访问参数的设置
//        该类使用ORACEL OCI80接口访问ORACEL数据库，所以在连接工程文件时要
//        包含ORACLE 库文件OCI80.LIB
//------------------------------------------------------------------------------
class TCDatabase
{
  private:
    TCString        m_sServerName ;            //数据库服务器名
    TCString        m_sUserName ;              //登录用户名
    TCString        m_sPassword ;              //登录口令
    TCString        m_sDatabaseName ;          //数据库名
    OCIDBPROCESS    *m_DBProc ;                 //进程指针
    bool            m_bAutoCommit ;            //事务是否自动提交

    bool            m_bConnected ;             //是否已建立连接

    OCIDBPROCESS    m_stOCIProcesses[OCIMAX_PROCNUM] ;    //数据库进程数组
    int             m_bOCIInited ;                       //OCI库是否初始化?
    int             m_nErrorCode ;                        //数据库错误代码
    TCString        m_sErrorString ;                      //数据库错误文本
    int             m_nOCIStatus ;                        //OCI调用返回状态值
    bool            m_bThrowException ;                   //发生错误是否抛出例外

  public:
    TCDatabase() ;                              //构造函数
    ~TCDatabase() ;                             //析构函数

    void            CheckError(sword) ;         //发生错误时,抛出错误Exception

    void            ConnectDBViaConfig();       // 通过配置信息，连接数据库
    void    	ConnectDBSalesConfig();      // 通过配置信息，连接营业数据库
    void    	ConnectDBAccountConfig();      // 通过配置信息，连接帐务数据库
    void    	ConnectDBParaConfig();      // 通过配置信息，连接帐务数据库

    // add by zhoulm in 20030102
    void    	ConnectDBMainCustConfig();      // 通过配置信息，连接重要客户数据库

    void            SetServerName(TCString) ;   //设置服务器名
    TCString        GetServerName() ;           //得到服务器名

    void     SetDBName(TCString sDBName)        //设置数据库名(SYBASE)
    {   m_sDatabaseName = sDBName ;
    }
    TCString        GetDBName()                 // 得到数据库名
    {   return m_sDatabaseName; }

    void            SetUserName(TCString) ;     //设置登录用户名
    TCString        GetUserName() ;             //得到登录用户名

    void            SetPassword(TCString) ;     //设置登录口令
    TCString        GetPassword()               //得到登录口令
    { return m_sPassword; }

    OCIDBPROCESS *  GetDBProc() ;               //得到数据库进程指针

    void            SetCommitMode(bool) ;       //设置提交模式
    bool            GetCommitMode() ;           //得到提交模式

    bool            IsConnected() ;             //是否已建立连接?
    bool            Connect() ;                 //连接数据库服务器
    void            Disconnect() ;              //断开连接

    bool            Commit(bool fFreeHandle = false);   //提交已发送事务
    bool            Rollback(bool fFreeHandle = false); //回滚已发送事务

    int      GetErrorCode()             //得到数据库错误代码
    {   return m_nErrorCode ;
    }
    int      GetDBStatus()             //得到OCI调用返回值
    {   return m_nOCIStatus ;
    }
    TCString GetErrorString()           //得到错误文本
    {   return m_sErrorString ;
    }
    void SetExceptionMode(bool f)       // 设置发生数据库错误时是否抛出例外
    {   m_bThrowException = f ;
    }
    bool GetExceptionMode()             // 得到发生数据库错误时是否抛出例外
    {   return m_bThrowException ;
    }

    static void WriteDBLogFile(TCString sAction, TCString sSQL);
} ;

enum TEDBErrorCode
{   dbeAllocateDBProcEntry,     // DBConnect时已没有数组空间
    dbeCallStatement,           // 调用时返回OCI_ERROR
    dbeInvalidHandle,           // 调用时返回OCI_INVALID_HANDLE
    dbeNotActiveDB              // 试图在不存在或未连接的DB上操作
};

class TCDBException : public TCException
{
private:
    TEDBErrorCode m_dbeErrorStatus;
    sb4           m_nDBErrorCode;
public:
    TCDBException(TEDBErrorCode dbeErrorStatus, TCString sMsg,
            sb4 nDBErrorCode = 0);
};

#endif

