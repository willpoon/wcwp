//---------------------------------------------------------------------------

#ifndef c_handletaskH
#define c_handletaskH
//---------------------------------------------------------------------------
#include "cmpublic_db.h"

//分别是未完成、标识、正在运行、成功完成、失败
enum TASK_STATUS{TASK_STATUS_UNCOMPLETE,TASK_STATUS_MARKED,
TASK_STATUS_WAITING,TASK_STATUS_SUCCESS,TASK_STATUS_FAIL};
//---------------------------------------------------------------------------
struct TSRunParam
{
TCString m_sReplaceKind;   //如-r或者-c
TCString m_sReplaced;      //被替换的
TCString m_sReplaceStyle;  //如MON或者NUM或者DAY
TCString m_sStart;         //如891或者200501
TCString m_sEnd;           //如200508，当是-c有效
long m_nAddFactor;      //辅助因子，如-1或者1
};

TCString To_String(enum TASK_STATUS eTaskStatus);
bool RecoverTaskStatus(TCQuery *pQuery);
bool UpdateTaskStatus(TCQuery *pQuery,long nScriptId,TCString sCycleId,enum TASK_STATUS eTaskStatus);
bool UpdateTaskSheet(TCQuery *pQuery,long nScriptId,TCString sCycleId,long nSheetCnt);
bool UpdateTaskSPID(TCQuery *pQuery,long nScriptId,TCString sCycleId,long nSPID);
bool GetTask(TCQuery *pQuery,TCString &sTaskId,long &nScriptId,TCString &sCycleId,TCString &sModule,TCString &sScriptName);
bool GetTaskSql(TCQuery *pQuery,long nScriptId,TCString sCycleId,long &nStep,long &nType,bool &bBuildFileFlag,
    TCStringList &sScriptSQLList,TCList &lstSingleRunParam,TCList &lstMultiRunParam,TCString &sErrorMsg);
bool GetHandleErrorSql(TCQuery *pQuery,long nTaskId,long nErrorNum,long nHandleType,
	TCStringList &sScriptSQLList,TCList &lstSingleRunParam,TCList &lstMultiRunParam,TCString &sErrorMsg);
void RecordTaskMsg(TCQuery *pQuery,long nScriptId,TCString sCycleId,TCString sMsg);
bool IsFrequencyChanged(TCQuery *pQuery);
bool ApartAndBuildSqlList(TCString sSQLScript,TCStringList &slSQLScriptList,TCString &sErrorMsg);
#endif
