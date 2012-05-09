#ifndef c_databaseH
#define c_databaseH

#include "cmpublic.h"
#include "cmpublic_db.h"

#ifdef  __ORACLECLASS
#include <oci.h>

const long OCIMAX_PROCNUM = 10;     //OCI������ݿ������

const long OCIMAX_COLUMNNUM = 100;  // OCI�������

//--------------------------------------------------------------------------
// �ṹ : OCIDBPROCESS
// ˵�� : ��װOCI�������ݿ��һ�黷������
//--------------------------------------------------------------------------
struct OCIDBPROCESS
{
  OCIEnv     *phEnviron;      // �������ָ��   
  OCIError   *phError;        // ������ָ��   
  OCIServer  *phServer;       // ���������ָ�� 
  OCISvcCtx  *phService;      // ������ָ��   
  OCISession *phSession;      // �Ự���ָ��   
  OCIStmt    *phSQL;          // SQL���
  OCIDefine  *phDefine[OCIMAX_COLUMNNUM];   // ����������     
  OCIBind    *BindByPos[OCIMAX_COLUMNNUM] ; // ������������� 
  OCIInd     NullFlag[OCIMAX_COLUMNNUM];    // NULL��־         
  int        ExecFlag;                      // SQL����Ƿ�ִ��  
  int        InUse;                         // �Ƿ����ñ�־     
} ;

#endif

//------------------------------------------------------------------------------
// ��   : TCDatabase
// ˵�� : ��װ�˶����ݿ������, �Ͽ����������ʲ���������
//        ����ʹ��ORACEL OCI80�ӿڷ���ORACEL���ݿ⣬���������ӹ����ļ�ʱҪ
//        ����ORACLE ���ļ�OCI80.LIB
//------------------------------------------------------------------------------
class TCDatabase
{
  private:
    TCString        m_sServerName ;            //���ݿ��������
    TCString        m_sUserName ;              //��¼�û���
    TCString        m_sPassword ;              //��¼����
    TCString        m_sDatabaseName ;          //���ݿ���
    OCIDBPROCESS    *m_DBProc ;                 //����ָ��
    bool            m_bAutoCommit ;            //�����Ƿ��Զ��ύ

    bool            m_bConnected ;             //�Ƿ��ѽ�������

    OCIDBPROCESS    m_stOCIProcesses[OCIMAX_PROCNUM] ;    //���ݿ��������
    int             m_bOCIInited ;                       //OCI���Ƿ��ʼ��?
    int             m_nErrorCode ;                        //���ݿ�������
    TCString        m_sErrorString ;                      //���ݿ�����ı�
    int             m_nOCIStatus ;                        //OCI���÷���״ֵ̬
    bool            m_bThrowException ;                   //���������Ƿ��׳�����

  public:
    TCDatabase() ;                              //���캯��
    ~TCDatabase() ;                             //��������

    void            CheckError(sword) ;         //��������ʱ,�׳�����Exception

    void            ConnectDBViaConfig();       // ͨ��������Ϣ���������ݿ�
    void    	ConnectDBSalesConfig();      // ͨ��������Ϣ������Ӫҵ���ݿ�
    void    	ConnectDBAccountConfig();      // ͨ��������Ϣ�������������ݿ�
    void    	ConnectDBParaConfig();      // ͨ��������Ϣ�������������ݿ�

    // add by zhoulm in 20030102
    void    	ConnectDBMainCustConfig();      // ͨ��������Ϣ��������Ҫ�ͻ����ݿ�

    void            SetServerName(TCString) ;   //���÷�������
    TCString        GetServerName() ;           //�õ���������

    void     SetDBName(TCString sDBName)        //�������ݿ���(SYBASE)
    {   m_sDatabaseName = sDBName ;
    }
    TCString        GetDBName()                 // �õ����ݿ���
    {   return m_sDatabaseName; }

    void            SetUserName(TCString) ;     //���õ�¼�û���
    TCString        GetUserName() ;             //�õ���¼�û���

    void            SetPassword(TCString) ;     //���õ�¼����
    TCString        GetPassword()               //�õ���¼����
    { return m_sPassword; }

    OCIDBPROCESS *  GetDBProc() ;               //�õ����ݿ����ָ��

    void            SetCommitMode(bool) ;       //�����ύģʽ
    bool            GetCommitMode() ;           //�õ��ύģʽ

    bool            IsConnected() ;             //�Ƿ��ѽ�������?
    bool            Connect() ;                 //�������ݿ������
    void            Disconnect() ;              //�Ͽ�����

    bool            Commit(bool fFreeHandle = false);   //�ύ�ѷ�������
    bool            Rollback(bool fFreeHandle = false); //�ع��ѷ�������

    int      GetErrorCode()             //�õ����ݿ�������
    {   return m_nErrorCode ;
    }
    int      GetDBStatus()             //�õ�OCI���÷���ֵ
    {   return m_nOCIStatus ;
    }
    TCString GetErrorString()           //�õ������ı�
    {   return m_sErrorString ;
    }
    void SetExceptionMode(bool f)       // ���÷������ݿ����ʱ�Ƿ��׳�����
    {   m_bThrowException = f ;
    }
    bool GetExceptionMode()             // �õ��������ݿ����ʱ�Ƿ��׳�����
    {   return m_bThrowException ;
    }

    static void WriteDBLogFile(TCString sAction, TCString sSQL);
} ;

enum TEDBErrorCode
{   dbeAllocateDBProcEntry,     // DBConnectʱ��û������ռ�
    dbeCallStatement,           // ����ʱ����OCI_ERROR
    dbeInvalidHandle,           // ����ʱ����OCI_INVALID_HANDLE
    dbeNotActiveDB              // ��ͼ�ڲ����ڻ�δ���ӵ�DB�ϲ���
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

