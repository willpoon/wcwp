//---------------------------------------------------------------------------

#ifndef c_thread_runH
#define c_thread_runH
//---------------------------------------------------------------------------
#include "c_thread.h"
#include "cmpublic_db.h"

const TCString ENDOFFILE = "END OF FILE ";

//---------------------------------------------------------------------------
// �ṹ ��: TSDBConnectConfig
// ��; : ���ݿ�������Ϣ
//---------------------------------------------------------------------------
struct TSDBConnectConfig
{
    TCString sDBServer;
    TCString sDBName;
    TCString sDBUser;
    TCString sDBPass;
};

//---------------------------------------------------------------------------
// �� ��: TCThreadStat
// ��; : �����߳�
//---------------------------------------------------------------------------
class TCThreadRun : public TCThread
{
private:
    //�������ݿ�
    TCDatabase m_WorkDataBase;
    TCQuery *m_pWorkQuery;
    TCDatabase m_TaskDataBase;
    TCQuery *m_pTaskQuery;
    
    //�߼����еı���
    bool m_bBuildFileFlag;   //true���ɽӿ��ļ���falseִֻ��SQL�������ɽӿ��ļ�
    TCString m_sTaskId;     //�ӿ�ID��Ҳ������ID
    long m_nScriptId;       //�ű����
    TCString m_sScriptName; //�ű�����    
    long m_nStep;           //�ű�˳���
    long m_nType;           //�ű����ͣ�0��ͨ�ű���1���ű�������״̬��2��ű�
    TCString m_sCycleId;    //ִ������
    TCStringList m_slSQLList; //�ű�SQL����б�
    long m_nSheetCount;     //�������
    long m_nErrorCode;		//������
    TCString m_sErrorMsg;	//������Ϣ
    
    //�����ļ����ö�Ӧ�ı���
    TCString m_sWorkingFileName; //�ӿ��ļ���
    TCString m_sWorkingDir; //���ɽӿ��ļ���Ŀ¼
    TCString m_sSendDir;    //FTP�Ĺ���Ŀ¼
    bool m_bRecordRunTime;   //��¼SQL����ʱ��    
    bool m_bSendErrorMsg;   //��������ӿڷ��������Ƿ��������ʹ�����Ϣ
    
    //˽�й��̺ͺ���
	void ExecSQLScript(long &nErrorCode,TCString &sErrorMsg);   //ִ��SQL�б�
    
    //�ӿ������������жϼ����������Զ�����
    void AlertMessage();    //�������жϼ����͸澯��Ϣ
    bool GeneralErrorHandle(const long nError); //�ӿڶ�Ӧ��ͨ��������
    void BuildEmptyFileToCrm(); //���ɿյĽӿ��ļ�
	bool SendMessage(const TCString sMsg); //����澯���ű��Ӷ�ʵ���Զ����Ͷ���    
protected:
    bool LinkSubFile(const TCString sInterfaceCode);
    bool SubmitFileToCrm(const TCString sInterfaceCode);
    void AddInterfaceLog(TCString sTaskId, TCString sFileName,TCString sCycleId, long nFileSize, long nSheetCount);
    void AddInterfaceEndFile(TCString sFileName,long nSheetCount);
    TCString GetResultFileName(const TCString sInterfaceCode,long nFileSerial);
public:
    TCThreadRun();
    TCThreadRun(const TSDBConnectConfig tsWorkDBCon,
        const TSDBConnectConfig tsTaskDBCon,const bool bRecordRunTime,const bool bSendErrorMsg);
    ~TCThreadRun();

    void SetScriptId(const long& nScriptId){m_nScriptId = nScriptId;};
    void SetTaskId(const TCString& sTaskId){m_sTaskId = sTaskId;};
    void SetCycleId(const TCString& sCycleId);
    void SetScriptName(const TCString& sScriptName){m_sScriptName = sScriptName;};    
    
    void SetWorkingDir(const TCString& sWorkingDir);
    void SetSendDir(const TCString& sSendDir);
    void SetSQLList(TCStringList& slSQLList);
    bool GetFinished();

    void Execute() ;
    bool ClientExecute() ;
    bool RequestStop();
};
#endif
