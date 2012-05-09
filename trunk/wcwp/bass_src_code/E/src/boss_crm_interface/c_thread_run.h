//---------------------------------------------------------------------------

#ifndef c_thread_runH
#define c_thread_runH
//---------------------------------------------------------------------------
#include "c_thread.h"
#include "cmpublic_db.h"

const TCString ENDOFFILE = "END OF FILE ";

//---------------------------------------------------------------------------
// 结构 　: TSDBConnectConfig
// 用途 : 数据库连接信息
//---------------------------------------------------------------------------
struct TSDBConnectConfig
{
    TCString sDBServer;
    TCString sDBName;
    TCString sDBUser;
    TCString sDBPass;
};

//---------------------------------------------------------------------------
// 类 　: TCThreadStat
// 用途 : 服务线程
//---------------------------------------------------------------------------
class TCThreadRun : public TCThread
{
private:
    //连接数据库
    TCDatabase m_WorkDataBase;
    TCQuery *m_pWorkQuery;
    TCDatabase m_TaskDataBase;
    TCQuery *m_pTaskQuery;
    
    //逻辑特有的变量
    bool m_bBuildFileFlag;   //true生成接口文件，false只执行SQL、不生成接口文件
    TCString m_sTaskId;     //接口ID，也叫任务ID
    long m_nScriptId;       //脚本编号
    TCString m_sScriptName; //脚本名称    
    long m_nStep;           //脚本顺序号
    long m_nType;           //脚本类型：0普通脚本、1父脚本可运行状态、2虚脚本
    TCString m_sCycleId;    //执行周期
    TCStringList m_slSQLList; //脚本SQL语句列表
    long m_nSheetCount;     //结果条数
    long m_nErrorCode;		//错误码
    TCString m_sErrorMsg;	//错误信息
    
    //配置文件设置对应的变量
    TCString m_sWorkingFileName; //接口文件名
    TCString m_sWorkingDir; //生成接口文件的目录
    TCString m_sSendDir;    //FTP的工作目录
    bool m_bRecordRunTime;   //记录SQL运行时间    
    bool m_bSendErrorMsg;   //如果单个接口发生错误，是否立即发送错误信息
    
    //私有过程和函数
	void ExecSQLScript(long &nErrorCode,TCString &sErrorMsg);   //执行SQL列表
    
    //接口条数合理性判断及常见错误自动处理
    void AlertMessage();    //合理性判断及发送告警信息
    bool GeneralErrorHandle(const long nError); //接口对应的通常错误处理
    void BuildEmptyFileToCrm(); //生成空的接口文件
	bool SendMessage(const TCString sMsg); //插入告警短信表，从而实现自动发送短信    
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
