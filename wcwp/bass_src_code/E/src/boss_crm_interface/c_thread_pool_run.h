//---------------------------------------------------------------------------

#ifndef c_thread_pool_runH
#define c_thread_pool_runH
//---------------------------------------------------------------------------
#include "c_thread_pool.h"

const long MAX_INTERFACE_TYPE_COUNT = 10;
struct TSInterfaceConfig
{
    TCString sInterfaceType;
    TSDBConnectConfig tsDBCon;
};

class TCThreadPoolRun : public TCThreadPool
{
private:
    TSDBConnectConfig m_tsTaskDBCon;//数据库连接信息
    TCQuery *m_pQuery;

    TCString m_sWorkingDir;
    TCString m_sSendDir;
    TSInterfaceConfig  m_tsInterfaceConfig[MAX_INTERFACE_TYPE_COUNT];

    bool ReadIniFile();
    virtual void Initialize();
public:
    TCThreadPoolRun();
    ~TCThreadPoolRun();

    void Run();
    TCThreadRun * GetNewAThread();
    TCThreadRun * GetNewAThread(const TSDBConnectConfig tsWorkDBCon,
        const TSDBConnectConfig tsTaskDBCon,const bool bRecordRunTime,const bool bSendErrorMsg);
    void DestroyAThread(TCThreadRun *pThreadRun);
};
TCThreadPoolRun & GetThreadPoolRun();
void ReleaseThreadPoolRun();
#endif
