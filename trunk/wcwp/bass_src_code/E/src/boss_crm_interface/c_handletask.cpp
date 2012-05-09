//---------------------------------------------------------------------------
#pragma hdrstop

#include "cmpublic.h"
#include "c_handletask.h"
#include "c_base_func.h"
//---------------------------------------------------------------------------

//==========================================================================
// 函数 : To_String
// 用途 : 把枚举类型转换为字符串
// 原型 : To_String()
// 参数 : eTaskStatus任务状态标识
// 返回 : 字符串
// 说明 :
//==========================================================================
TCString To_String(enum TASK_STATUS eTaskStatus)
{
    switch(eTaskStatus)
    {
        case TASK_STATUS_UNCOMPLETE:
            return "A";
        case TASK_STATUS_MARKED:
            return "M";
        case TASK_STATUS_WAITING:
            return "W";
        case TASK_STATUS_SUCCESS:
            return "C";
        case TASK_STATUS_FAIL:
            return "F";
        default:
            return "";
    }
}

//==========================================================================
// 函数 : :RecoverTaskStatus
// 用途 : 恢复任务状态
// 原型 : RecoverTaskStatus()
// 参数 :
// 返回 : TRUE更新成功，FALSE更新失败
// 说明 :
//==========================================================================
bool RecoverTaskStatus(TCQuery *pQuery)
{
    ASSERT(pQuery != NULL);
    TCString sSQL;

    //删除虚节点TYPE=2
    sSQL="delete mb_if_jyfx_task_running where type=2";

    if (!pQuery->ExecSQL(sSQL))
        return false;

    sSQL="update mb_if_jyfx_task_running"
         " set type=0,error_msg=null,stime=null,sheet_cnt=0,"
         " status="+QuotedStr(To_String(TASK_STATUS_UNCOMPLETE))+
         " where status="+QuotedStr(To_String(TASK_STATUS_MARKED))+
         " or status="+QuotedStr(To_String(TASK_STATUS_WAITING));

    if (!pQuery->ExecSQL(sSQL))
        return false;

    return true;
}


//==========================================================================
// 函数 : :UpdateTaskStatus
// 用途 : 更新任务状态
// 原型 : UpdateTaskStatus()
// 参数 : nScriptId脚本编号，eTaskStatus状态标识
// 返回 : TRUE更新成功，FALSE更新失败
// 说明 :
//==========================================================================
bool UpdateTaskStatus(TCQuery *pQuery,long nScriptId,TCString sCycleId,enum TASK_STATUS eTaskStatus)
{
    ASSERT(pQuery != NULL);
    TCString sSQL;
    long nType,nFlag,nSheetCnt,nParentScriptId;

    switch(eTaskStatus)
    {
        case TASK_STATUS_UNCOMPLETE:
        case TASK_STATUS_MARKED:
        case TASK_STATUS_WAITING:
            sSQL="update mb_if_jyfx_task_running"
                 " set status="+QuotedStr(To_String(eTaskStatus))+
                 " ,stime=decode(type,1,nvl(stime,sysdate),sysdate),etime=null,error_msg=null"
                 " where script_id="+IntToStr(nScriptId)+
                 " and cycle_id="+QuotedStr(sCycleId);
            break;
        case TASK_STATUS_SUCCESS:
        case TASK_STATUS_FAIL:
            sSQL="update mb_if_jyfx_task_running"
                 " set status="+QuotedStr(To_String(eTaskStatus))+
                 " ,etime=sysdate"
                 " where script_id="+IntToStr(nScriptId)+
                 " and cycle_id="+QuotedStr(sCycleId);
            break;
        default:
            throw TCException("TCThreadRun::UpdateTaskStatus() Status is invalid!");
    }

    if (!pQuery->ExecSQL(sSQL))
        return false;

    if (eTaskStatus!=TASK_STATUS_SUCCESS)
        return true;

    //处理虚节点
    sSQL="select parent_script_id,type,flag,sheet_cnt from mb_if_jyfx_task_running where script_id="
        +IntToStr(nScriptId)+" and cycle_id="+QuotedStr(sCycleId);
    pQuery->SetSQL(sSQL);
    if (!pQuery->OpenE())
        return false;

    while (pQuery->FetchE() == 0)
    {
        nParentScriptId=StrToInt(pQuery->GetFieldValue("parent_script_id"));
        nType=StrToInt(pQuery->GetFieldValue("type"));
        nFlag=StrToInt(pQuery->GetFieldValue("flag"));
        nSheetCnt=StrToInt(pQuery->GetFieldValue("sheet_cnt"));
    }

    sSQL="insert into mb_if_jyfx_task_log"
         " select * from mb_if_jyfx_task_running"
         " where script_id="+IntToStr(nScriptId)+
         " and cycle_id="+QuotedStr(sCycleId);
    if (!pQuery->ExecSQL(sSQL))
        return false;

    sSQL="delete mb_if_jyfx_task_running"
         " where script_id="+IntToStr(nScriptId)+
         " and cycle_id="+QuotedStr(sCycleId);
    if (!pQuery->ExecSQL(sSQL))
        return false;

    if (nType==2)
    {
        //若虚任务是生成子接口文件的，则更新父任务的条数
        if (nFlag==0)
        {
            UpdateTaskSheet(pQuery,nParentScriptId,sCycleId,nSheetCnt);
        }
        sSQL="update mb_if_jyfx_task_running"
             " set type=1,status="+QuotedStr(To_String(TASK_STATUS_UNCOMPLETE))+
             " where type=0 "
             " and script_id="+IntToStr(nParentScriptId)+
             " and cycle_id="+QuotedStr(sCycleId)+
             " and not exists (select * from mb_if_jyfx_task_running"
             " where parent_script_id="+IntToStr(nParentScriptId)+
             " and cycle_id="+QuotedStr(sCycleId)+
             " and status<>"+QuotedStr(To_String(TASK_STATUS_SUCCESS))+
             ")";
        if (!pQuery->ExecSQL(sSQL))
        {
            return false;
        }
    }

    return true;
}

//==========================================================================
// 函数 : :UpdateTaskSheet
// 用途 : 更新任务生成的接口文件行数
// 原型 : UpdateTaskSheet()
// 参数 : nScriptId脚本编号，nSheetCnt行数
// 返回 : TRUE更新成功，FALSE更新失败
// 说明 :
//==========================================================================
bool UpdateTaskSheet(TCQuery *pQuery,long nScriptId,TCString sCycleId,long nSheetCnt)
{
    ASSERT(pQuery != NULL);
    TCString sSQL;

    sSQL="update mb_if_jyfx_task_running"
         " set sheet_cnt=sheet_cnt+"+IntToStr(nSheetCnt)+
         " where script_id="+IntToStr(nScriptId)+
         " and cycle_id="+QuotedStr(sCycleId);

    if (!pQuery->ExecSQL(sSQL))
        return false;

    return true;
}

//==========================================================================
// 函数 : :UpdateTaskSPID
// 用途 : 更新任务生成的线程ID
// 原型 : UpdateTaskSPID()
// 参数 : nScriptId脚本编号，nSPID线程ID
// 返回 : TRUE更新成功，FALSE更新失败
// 说明 :
//==========================================================================
bool UpdateTaskSPID(TCQuery *pQuery,long nScriptId,TCString sCycleId,long nSPID)
{
    ASSERT(pQuery != NULL);
    TCString sSQL;

    sSQL="update mb_if_jyfx_task_running"
         " set SPID="+IntToStr(nSPID)+
         " where script_id="+IntToStr(nScriptId)+
         " and cycle_id="+QuotedStr(sCycleId);

    if (!pQuery->ExecSQL(sSQL))
        return false;

    return true;
}


//==========================================================================
// 函数 : GetTask
// 用途 : 是否有任务
// 原型 : GetTask()
// 参数 : 变参nScriptId脚本标识，sCycleId周期，sModule归属模块
// 返回 : TRUE有，FALSE无
// 说明 :
//==========================================================================
bool GetTask(TCQuery *pQuery,TCString &sTaskId,long &nScriptId,TCString &sCycleId,TCString &sModule,TCString &sScriptName)
{
    ASSERT(pQuery != NULL);
    TCString sSQL,sOneSQL;
    long nCnt=0;

    nScriptId=0;
    //获取接口代码、所属模块、周期等信息
    sSQL="select task_id,script_id,cycle_id,module,script_name from ("
         " select a.task_id,a.script_id,a.cycle_id,a.module,a.script_name from mb_if_jyfx_task_running a"
         " where a.plan_time<=sysdate and a.status="+QuotedStr(To_String(TASK_STATUS_UNCOMPLETE))+
         " and (a.type in (1,2) or a.type=0 "
         " and (a.script_id not in (select script_id from mb_if_jyfx_script_depend)"
         " or a.script_id in (select script_id from mb_if_jyfx_script_depend)"
         " and not exists(select * from mb_if_jyfx_task_running b,mb_if_jyfx_script_depend c"
         " where a.script_id=c.script_id and c.depend_script_id=b.script_id"
         " and a.cycle_id=b.cycle_id"
         " and b.status<>"+QuotedStr(To_String(TASK_STATUS_SUCCESS))+
         ")))"
         " order by a.priority,a.script_id,a.cycle_id"
         ")"
         " where rownum=1";

    pQuery->SetSQL(sSQL);
    if (!pQuery->OpenE())
        return false;

    while (pQuery->FetchE() == 0)
    {
        nCnt++;
        sTaskId=pQuery->GetFieldValue("task_id");
        nScriptId=StrToInt(pQuery->GetFieldValue("script_id"));
        sCycleId=pQuery->GetFieldValue("cycle_id");
        sModule=pQuery->GetFieldValue("module");
        sScriptName=pQuery->GetFieldValue("script_name");
    }

    if (nCnt!=1) return false;
    return true;
}

//==========================================================================
// 函数 : TCThreadRun::ApartAndBuildSqlList
// 用途 : 拆分并生成SQL脚本列表
// 原型 : bool ApartAndBuildSqlList()
// 参数 : TCString sSQLScript
// 返回 : true成功，false失败
// 说明 : 在sSQL中每个子SQL必须以/*BEGIN*/或者/*BEGIN,IGNORE_ERROR*/开始，以/*END*/结束
//其中/*BEGIN,IGNORE_ERROR*/代表这个子SQL忽略SQL错误，比如先删除中间表，再建立中间表，
//那么删除中间表这个子SQL就可以忽略SQL错误
//1.如果子SQL是子过程，那么过程结尾的end后面必须有分号
//2.如果子SQL是单个SQL语句，那么SQL结尾不能有分号
//==========================================================================
bool ApartAndBuildSqlList(TCString sSQLScript,TCStringList &slSQLScriptList,
TCString &sErrorMsg)
{
    TCString sSQL,sSubSQL;
    TCString sBeginFlag,sBeginFlagIgnoreError,sEndFlag;
    long nPos,nPos1;
    long nSqlSeq;

    slSQLScriptList.Clear();
    sSQL=AllTrim(sSQLScript);
    sBeginFlag="/*BEGIN*/";
    sBeginFlagIgnoreError="/*BEGIN,IGNORE_ERROR*/";
    sEndFlag="/*END*/";
    nSqlSeq=0;

    while (true)
    {
        nSqlSeq++;
        nPos=Pos(sSQL,sBeginFlag);
        nPos1=Pos(sSQL,sBeginFlagIgnoreError);
        if ((nPos==0) && (nPos1==0)) break;

        if ((nPos1==0) || (nPos>0) && (nPos<nPos1))
        {
            sSQL=AllTrim(Mid(sSQL,nPos + Length(sBeginFlag)));
        }
        else
        {
            sSQL=AllTrim(Mid(sSQL,nPos1 + Length(sBeginFlagIgnoreError)));
        }

        nPos=Pos(sSQL,sEndFlag);
        if (nPos>0)
        {
            sSubSQL=AllTrim(Mid(sSQL,1,nPos - 1));
            sSQL=AllTrim(Mid(sSQL,nPos + Length(sEndFlag)));
        }
        else
        {
            sErrorMsg="序号"+IntToStr(nSqlSeq)+"错误信息:子SQL缺少''"+sEndFlag+"''\n";
            return false;
        }

        slSQLScriptList.Add(sSubSQL);
    }//end of while

    return true;
}

//==========================================================================
// 函数 : GetTaskSql
// 用途 : 获取任务的SQL列表
// 原型 : GetTaskSql()
// 参数 : 变参nScriptId脚本标识，sScriptSQLList接口SQL列表
// 返回 : TRUE成功，FALSE失败
// 说明 :
//==========================================================================
bool GetTaskSql(TCQuery *pQuery,long nScriptId,TCString sCycleId,long &nStep,long &nType,bool &bBuildFileFlag,
TCStringList &sScriptSQLList,TCList &lstSingleRunParam,TCList &lstMultiRunParam,TCString &sErrorMsg)
{
    TCString sSQL,sRunParam,sRunParamPart;
    TCString sScriptSQL,sRunSql1,sRunSql2,sRunSql3;
    TSRunParam *pstRunParam;
    long nPos,nFlag;

    sScriptSQL="";
    nStep=0;
    bBuildFileFlag=false;
    lstSingleRunParam.Clear();
    lstMultiRunParam.Clear();

    //获取对应的SQL列表
    sSQL="select step,type,flag,run_sql1,run_sql2,run_sql3,run_param from mb_if_jyfx_task_running"
         " where script_id="+IntToStr(nScriptId)+
         " and cycle_id="+QuotedStr(sCycleId);

    pQuery->SetSQL(sSQL);
    if (!pQuery->OpenE())
    {
        sErrorMsg="错误信息:"+pQuery->GetErrorMsg();
        return false;
    }

    while (pQuery->FetchE() == 0)
    {
        nStep=StrToInt(pQuery->GetFieldValue("step"));
        nType=StrToInt(pQuery->GetFieldValue("type"));
        nFlag=StrToInt(pQuery->GetFieldValue("flag"));
        sRunSql1=AllTrim(pQuery->GetFieldValue("run_sql1"));
        sRunSql2=AllTrim(pQuery->GetFieldValue("run_sql2"));
        sRunSql3=AllTrim(pQuery->GetFieldValue("run_sql3"));
        sRunParam=AllTrim(pQuery->GetFieldValue("run_param"));

        if (nFlag==0)
        {
            bBuildFileFlag=true;
        }
    }
    //合并SQL
    sScriptSQL=sRunSql1+sRunSql2+sRunSql3;
    
    //分析SQL
    if (!ApartAndBuildSqlList(sScriptSQL,sScriptSQLList,sErrorMsg))
        return false;

    //分析替换因子
    while (Length(sRunParam)>0)
    {
        pstRunParam=new TSRunParam;
        nPos=Pos(sRunParam,"\n");
        if (nPos==0)
        {
            sRunParamPart=AllTrim(sRunParam);
            sRunParam="";
        }
        else
        {
            sRunParamPart=AllTrim(Mid(sRunParam,1,nPos - 1));
            sRunParam=AllTrim(Mid(sRunParam,nPos + Length("\n")));
        }

        if (Pos(sRunParamPart,"-r")>0)
        {
            nPos=Pos(sRunParamPart,"-r");
            pstRunParam->m_sReplaceKind="r";
            sRunParamPart=AllTrim(Mid(sRunParamPart,nPos + 2));
            nPos=Pos(sRunParamPart,"=");
            pstRunParam->m_sReplaced=AllTrim(Mid(sRunParamPart,1,nPos - 1));
            pstRunParam->m_sStart=AllTrim(Mid(sRunParamPart,nPos + 1));
            pstRunParam->m_sReplaceStyle="";
            pstRunParam->m_sEnd="";
            pstRunParam->m_nAddFactor=0;

            lstSingleRunParam.Add(pstRunParam);
        }
        else if (Pos(sRunParamPart,"-c")>0)
        {
            nPos=Pos(sRunParamPart,"-c");
            pstRunParam->m_sReplaceKind="c";
            sRunParamPart=AllTrim(Mid(sRunParamPart,nPos + 2));
            nPos=Pos(sRunParamPart,"=");
            pstRunParam->m_sReplaced=AllTrim(Mid(sRunParamPart,1,nPos - 1));
            sRunParamPart=AllTrim(Mid(sRunParamPart,nPos + 1));

            if (Pos(sRunParamPart,"MON")>0)
            {
                nPos=Pos(sRunParamPart,"MON");
                pstRunParam->m_sReplaceStyle="MON";
            }
            else if (Pos(sRunParamPart,"NUM")>0)
            {
                nPos=Pos(sRunParamPart,"NUM");
                pstRunParam->m_sReplaceStyle="NUM";
            }
            else if (Pos(sRunParamPart,"DAY")>0)
            {
                nPos=Pos(sRunParamPart,"DAY");
                pstRunParam->m_sReplaceStyle="DAY";
            }
            else
            {
                sErrorMsg="错误信息:run_param is invalid!\n";
                return false;
            }

            sRunParamPart=AllTrim(Mid(sRunParamPart,nPos + 3));
            //去掉前后[]
            sRunParamPart=Mid(sRunParamPart,2,Length(sRunParamPart) - 2);

            nPos=Pos(sRunParamPart,"-");
            if (nPos==0) nPos=Pos(sRunParamPart,"+");
            if (nPos==0)
            {
                sErrorMsg="错误信息:run_param is invalid!\n";
                return false;
            }

            pstRunParam->m_sStart=AllTrim(Mid(sRunParamPart,1,nPos - 1));
            pstRunParam->m_sEnd=AllTrim(Mid(sRunParamPart,nPos + 1));
            //增加判断，以免后面生成虚任务时程序挂死
            if (!IsNumber(pstRunParam->m_sStart) || !IsNumber(pstRunParam->m_sEnd))
            {
                sErrorMsg="错误信息:循环起始值不是合法数字，run_param is invalid!\n";
                return false;
            }
            if (StrToInt(pstRunParam->m_sStart)<=StrToInt(pstRunParam->m_sEnd))
                pstRunParam->m_nAddFactor=1;
            else
                pstRunParam->m_nAddFactor=-1;

            lstMultiRunParam.Add(pstRunParam);
        }
    }

    return true;
}


//==========================================================================
// 函数 : GetHandleErrorSql
// 用途 : 获取处理错误的SQL列表
// 原型 : GetHandleErrorSql()
// 参数 : 变参nTaskId任务标识，sScriptSQLList接口SQL列表
// 返回 : TRUE成功，FALSE失败
// 说明 :
//==========================================================================
bool GetHandleErrorSql(TCQuery *pQuery,long nTaskId,long nErrorNum,long nHandleType,
TCStringList &sScriptSQLList,TCList &lstSingleRunParam,TCList &lstMultiRunParam,TCString &sErrorMsg)
{
    TCString sSQL,sRunParam,sRunParamPart;
    TCString sScriptSQL;
    TSRunParam *pstRunParam;
    long nPos;

    sScriptSQL="";
    lstSingleRunParam.Clear();
    lstMultiRunParam.Clear();

    //获取对应的SQL列表
    sSQL="select run_sql1||run_sql2||run_sql3 sql,run_param from MB_IF_JYFX_GENERROR_HANDLE"
         " where task_id="+IntToStr(nTaskId)+
         " and error_num="+IntToStr(nErrorNum)+
         " and handle_type="+IntToStr(nHandleType);

    pQuery->SetSQL(sSQL);
    if (!pQuery->OpenE())
    {
        sErrorMsg="错误信息:"+pQuery->GetErrorMsg();
        return false;
    }

    while (pQuery->FetchE() == 0)
    {
        sScriptSQL=AllTrim(pQuery->GetFieldValue("sql"));
        sRunParam=AllTrim(pQuery->GetFieldValue("run_param"));
    }
    //分析SQL
    if (!ApartAndBuildSqlList(sScriptSQL,sScriptSQLList,sErrorMsg))
        return false;

    //分析替换因子
    while (Length(sRunParam)>0)
    {
        pstRunParam=new TSRunParam;
        nPos=Pos(sRunParam,"\n");
        if (nPos==0)
        {
            sRunParamPart=AllTrim(sRunParam);
            sRunParam="";
        }
        else
        {
            sRunParamPart=AllTrim(Mid(sRunParam,1,nPos - 1));
            sRunParam=AllTrim(Mid(sRunParam,nPos + Length("\n")));
        }

        if (Pos(sRunParamPart,"-r")>0)
        {
            nPos=Pos(sRunParamPart,"-r");
            pstRunParam->m_sReplaceKind="r";
            sRunParamPart=AllTrim(Mid(sRunParamPart,nPos + 2));
            nPos=Pos(sRunParamPart,"=");
            pstRunParam->m_sReplaced=AllTrim(Mid(sRunParamPart,1,nPos - 1));
            pstRunParam->m_sStart=AllTrim(Mid(sRunParamPart,nPos + 1));
            pstRunParam->m_sReplaceStyle="";
            pstRunParam->m_sEnd="";
            pstRunParam->m_nAddFactor=0;

            lstSingleRunParam.Add(pstRunParam);
        }
        else if (Pos(sRunParamPart,"-c")>0)
        {
            nPos=Pos(sRunParamPart,"-c");
            pstRunParam->m_sReplaceKind="c";
            sRunParamPart=AllTrim(Mid(sRunParamPart,nPos + 2));
            nPos=Pos(sRunParamPart,"=");
            pstRunParam->m_sReplaced=AllTrim(Mid(sRunParamPart,1,nPos - 1));
            sRunParamPart=AllTrim(Mid(sRunParamPart,nPos + 1));

            if (Pos(sRunParamPart,"MON")>0)
            {
                nPos=Pos(sRunParamPart,"MON");
                pstRunParam->m_sReplaceStyle="MON";
            }
            else if (Pos(sRunParamPart,"NUM")>0)
            {
                nPos=Pos(sRunParamPart,"NUM");
                pstRunParam->m_sReplaceStyle="NUM";
            }
            else if (Pos(sRunParamPart,"DAY")>0)
            {
                nPos=Pos(sRunParamPart,"DAY");
                pstRunParam->m_sReplaceStyle="DAY";
            }
            else
            {
                sErrorMsg="错误信息:run_param is invalid!\n";
                return false;
            }

            sRunParamPart=AllTrim(Mid(sRunParamPart,nPos + 3));
            //去掉前后[]
            sRunParamPart=Mid(sRunParamPart,2,Length(sRunParamPart) - 2);

            nPos=Pos(sRunParamPart,"-");
            if (nPos==0) nPos=Pos(sRunParamPart,"+");
            if (nPos==0)
            {
                sErrorMsg="错误信息:run_param is invalid!\n";
                return false;
            }

            pstRunParam->m_sStart=AllTrim(Mid(sRunParamPart,1,nPos - 1));
            pstRunParam->m_sEnd=AllTrim(Mid(sRunParamPart,nPos + 1));
            //增加判断，以免后面生成虚任务时程序挂死
            if (!IsNumber(pstRunParam->m_sStart) || !IsNumber(pstRunParam->m_sEnd))
            {
                sErrorMsg="错误信息:循环起始值不是合法数字，run_param is invalid!\n";
                return false;
            }
            if (StrToInt(pstRunParam->m_sStart)<=StrToInt(pstRunParam->m_sEnd))
                pstRunParam->m_nAddFactor=1;
            else
                pstRunParam->m_nAddFactor=-1;

            lstMultiRunParam.Add(pstRunParam);
        }
    }

    return true;
}

//==========================================================================
// 函数 : RecordTaskMsg
// 用途 : 记录错误信息或者每SQL运行时间
// 原型 : void RecordTaskMsg()
// 参数 : long nScript_id,TCString sCycleId,TCString sMsg
// 返回 :
// 说明 :
//==========================================================================
void RecordTaskMsg(TCQuery *pQuery,long nScriptId,TCString sCycleId,TCString sMsg)
{
    TCString sSQL;

    sSQL="update mb_if_jyfx_task_running set error_msg=error_msg||to_char(sysdate,'YYYYMMDD HH24:MI:SS')||"
    +QuotedStr(sMsg)+
    " where script_id="+IntToStr(nScriptId)+
    " and cycle_id="+QuotedStr(sCycleId);
    
    pQuery->ExecSQL(sSQL);
}


//==========================================================================
// 函数 : IsFrequencyChanged
// 用途 : 判断是否处理日期段切换
// 原型 : IsFrequencyChanged()
// 参数 : 原来日期
// 返回 : 无
// 说明 : 如果要日期切换则插入记录到表mb_if_jyfx_task_running
//==========================================================================
bool IsFrequencyChanged(TCQuery *pQuery)
{
    TCString sCurDate = TCTime::Today();
    TCString sSQL;
    TCString sNextYear,sNextMonth,sNextDay,sYear,sMonth,sDay;
    long m_nTableCount;

    sNextYear=IntToStr(TCTime::Year(sCurDate));
    sNextMonth=Left(sCurDate,6);
    sNextDay=sCurDate;
    sYear=IntToStr(StrToInt(sNextYear) - 1);
    sMonth=RelativeMonth(sNextMonth,-1);
    sDay=TCTime::RelativeDate(sNextDay,-1);

    //处理年报
    m_nTableCount=0;
    sSQL="select nvl(count(*),0) table_cnt from mb_if_jyfx_task_list "
         "where frequency='Y' and pre_cycle_id<"+QuotedStr(sYear);
    pQuery->SetSQL(sSQL);
    if (!pQuery->OpenE())
        return false;

    while (pQuery->FetchE() == 0)
    {
        m_nTableCount=StrToInt(pQuery->GetFieldValue("table_cnt"));
    }
    if (m_nTableCount>0)
    {     
        sSQL="delete from mb_if_jyfx_task_running"
        	 " where task_id||cycle_id"
             " in (select distinct task_id||"+QuotedStr(sYear)+
             " from mb_if_jyfx_task_list where frequency='Y' "
             " and pre_cycle_id<"+QuotedStr(sYear)+")";
        if (!pQuery->ExecSQL(sSQL))
            return false;

        sSQL="insert into mb_if_jyfx_task_running "
             " select a.task_id,"+QuotedStr(sYear)+" cycle_id,0 step,0 parent_script_id,b.script_id,"
             " a.task_name||decode(b.step,0,'','--步骤'||b.step) script_name,0 type,"
             " a.module,b.run_param,b.sql1 runsql1,b.sql2 run_sql2,b.sql3 run_sql3,"
             " to_date("+QuotedStr(sNextYear)+"||a.plan_time,'YYYYMMDDHH24MISS') ptime,"
             " null stime,null etime,'A' status,"
             " null ERROR_MSG,a.priority,b.flag,null spid,0 sheet_cnt"
             " from mb_if_jyfx_task_list a,mb_if_jyfx_task_sql b"
             " where a.task_id=b.task_id and a.frequency='Y' "
             " and a.pre_cycle_id<"+QuotedStr(sYear);
        if (!pQuery->ExecSQL(sSQL))
            return false;

        sSQL="update mb_if_jyfx_task_list set pre_cycle_id="+QuotedStr(sYear)+
             " where frequency='Y' "
             "and pre_cycle_id<"+QuotedStr(sYear);

        if (!pQuery->ExecSQL(sSQL))
            return false;
    }

    //处理月报
    m_nTableCount=0;
    sSQL="select nvl(count(*),0) table_cnt from mb_if_jyfx_task_list "
         "where frequency='M' and pre_cycle_id<"+QuotedStr(sMonth);

    pQuery->SetSQL(sSQL);
    if (!pQuery->OpenE())
        return false;

    while (pQuery->FetchE() == 0)
    {
        m_nTableCount=StrToInt(pQuery->GetFieldValue("table_cnt"));
    }
    if (m_nTableCount>0)
    {
        sSQL="delete from mb_if_jyfx_task_running "
        	 "where task_id||cycle_id"
             " in (select distinct task_id||"+QuotedStr(sMonth)+
             "from mb_if_jyfx_task_list where frequency='M' "
             "and pre_cycle_id<"+QuotedStr(sMonth)+")";

        if (!pQuery->ExecSQL(sSQL))
            return false;

        sSQL="insert into mb_if_jyfx_task_running "
             " select a.task_id,"+QuotedStr(sMonth)+" cycle_id,0 step,0 parent_script_id,b.script_id,"
             " a.task_name||decode(b.step,0,'','--步骤'||b.step) script_name,0 type,"
             " a.module,replace(replace(b.run_param,'MONTH',"+QuotedStr(sMonth)+"),'NEXTM',to_char(add_months(to_date("+QuotedStr(sMonth)+",'YYYYMM'),1),'YYYYMM')),"
             " b.sql1 runsql1,b.sql2 run_sql2,b.sql3 run_sql3,"
             " to_date("+QuotedStr(sNextMonth)+"||a.plan_time,'YYYYMMDDHH24MISS') ptime,"
             " null stime,null etime,'A' status,"
             " null ERROR_MSG,a.priority,b.flag,null spid,0 sheet_cnt"
             " from mb_if_jyfx_task_list a,mb_if_jyfx_task_sql b"
             " where a.task_id=b.task_id and a.frequency='M' "
             " and a.pre_cycle_id<"+QuotedStr(sMonth);
        if (!pQuery->ExecSQL(sSQL))
            return false;

        sSQL="update mb_if_jyfx_task_list set pre_cycle_id="+QuotedStr(sMonth)+
             " where frequency='M' "
             "and pre_cycle_id<"+QuotedStr(sMonth);

        if (!pQuery->ExecSQL(sSQL))
            return false;
    }

    //处理日报
    m_nTableCount=0;
    sSQL="select nvl(count(*),0) table_cnt from mb_if_jyfx_task_list "
         "where frequency='D' and pre_cycle_id<"+QuotedStr(sDay);

    pQuery->SetSQL(sSQL);
    if (!pQuery->OpenE())
        return false;

    while (pQuery->FetchE() == 0)
    {
        m_nTableCount=StrToInt(pQuery->GetFieldValue("table_cnt"));
    }
    if (m_nTableCount>0)
    {
        sSQL="delete from mb_if_jyfx_task_running "
        	 "where task_id||cycle_id"
             " in (select distinct task_id||"+QuotedStr(sDay)+
             "from mb_if_jyfx_task_list where frequency='D' "
             "and pre_cycle_id<"+QuotedStr(sDay)+")";

        if (!pQuery->ExecSQL(sSQL))
            return false;

        sSQL="insert into mb_if_jyfx_task_running "
             " select a.task_id,"+QuotedStr(sDay)+" cycle_id,0 step,0 parent_script_id,b.script_id,"
             " a.task_name||decode(b.step,0,'','--步骤'||b.step) script_name,0 type,"
             " a.module,replace(replace(b.run_param,'DAY',"+QuotedStr(sDay)+"),'MONTH',"+QuotedStr(Mid(sDay,1,6))+"),"
             " b.sql1 runsql1,b.sql2 run_sql2,b.sql3 run_sql3,"
             " to_date("+QuotedStr(sNextDay)+"||a.plan_time,'YYYYMMDDHH24MISS') ptime,"
             " null stime,null etime,'A' status,"
             " null ERROR_MSG,a.priority,b.flag,null spid,0 sheet_cnt"
             " from mb_if_jyfx_task_list a,mb_if_jyfx_task_sql b"
             " where a.task_id=b.task_id and a.frequency='D' "
             " and a.pre_cycle_id<"+QuotedStr(sDay);
        if (!pQuery->ExecSQL(sSQL))
            return false;

        sSQL="update mb_if_jyfx_task_list set pre_cycle_id="+QuotedStr(sDay)+
             " where frequency='D' "
             "and pre_cycle_id<"+QuotedStr(sDay);
        if (!pQuery->ExecSQL(sSQL))
            return false;
    }
    return true;
}
