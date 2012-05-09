//---------------------------------------------------------------------------

#pragma hdrstop
#include "c_thread_run.h"
#include "c_handletask.h"
#include "c_base_func.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)

TCCriticalSection STA_Lock;

//==========================================================================
// 函数 : TCThreadRun::TCThreadRun
// 用途 : 构造函数
// 原型 :
// 参数 :
// 返回 :
// 说明 :
//==========================================================================
TCThreadRun::TCThreadRun(const TSDBConnectConfig tsWorkDBCon,
const TSDBConnectConfig tsTaskDBCon,const bool bRecordRunTime,const bool bSendErrorMsg)
{
	TCString sSQL;

    m_bRecordRunTime=bRecordRunTime;
    m_bSendErrorMsg=bSendErrorMsg;

    //连接工作数据库
    m_WorkDataBase.SetServerName(tsWorkDBCon.sDBServer);
    m_WorkDataBase.SetDBName(tsWorkDBCon.sDBName);
    m_WorkDataBase.SetUserName(tsWorkDBCon.sDBUser);
    m_WorkDataBase.SetPassword(tsWorkDBCon.sDBPass);
    if (!m_WorkDataBase.Connect())
    {
        throw TCException("TCThreadRun::TCThreadRun() Error:connect work's db fail!");
    }
    m_WorkDataBase.SetExceptionMode(false);

    ASSERT(m_WorkDataBase.IsConnected());
    m_pWorkQuery = new TCQuery(m_WorkDataBase);
    
    //加优化参数
    //sSQL="alter session set optimizer_mode = 'rule'";
	//m_pWorkQuery->ExecSQL(sSQL);
    //sSQL="alter session set sort_area_size=500000000";
	//m_pWorkQuery->ExecSQL(sSQL);

    //连接任务数据库
    m_TaskDataBase.SetServerName(tsTaskDBCon.sDBServer);
    m_TaskDataBase.SetDBName(tsTaskDBCon.sDBName);
    m_TaskDataBase.SetUserName(tsTaskDBCon.sDBUser);
    m_TaskDataBase.SetPassword(tsTaskDBCon.sDBPass);
    if (!m_TaskDataBase.Connect())
    {
        throw TCException("TCThreadRun::TCThreadRun() Error:connect task's db fail!");
    }
    m_TaskDataBase.SetExceptionMode(false);

    ASSERT(m_TaskDataBase.IsConnected());
    m_pTaskQuery = new TCQuery(m_TaskDataBase);
}

//==========================================================================
// 函数 : TCThreadRun::TCThreadRun
// 用途 : 构造函数
// 原型 :
// 参数 :
// 返回 :
// 说明 :
//==========================================================================
TCThreadRun::TCThreadRun()
{
    //
}

//==========================================================================
// 函数 : TCThreadRun::~TCThreadRun()
// 用途 : 析构函数
// 原型 : ~TCThreadRun()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
TCThreadRun::~TCThreadRun()
{
    //断开数据库连接
    m_pWorkQuery->Close();
    delete m_pWorkQuery;
    m_pWorkQuery=NULL;

    if (m_WorkDataBase.IsConnected())
        m_WorkDataBase.Disconnect();

    m_pTaskQuery->Close();
    delete m_pTaskQuery;
    m_pTaskQuery=NULL;

    if (m_TaskDataBase.IsConnected())
        m_TaskDataBase.Disconnect();
}

//==========================================================================
// 函数 : TCThreadRun::GetFinished
// 用途 : 返回m_bFinished
// 原型 : void GetFinished()
// 参数 : 无
// 返回 : bool
// 说明 :
//==========================================================================
bool TCThreadRun::GetFinished()
{
    return m_bFinished;
}

//==========================================================================
// 函数 : TCThreadRun::SetWorkingDir
// 用途 : 设置接口文件工作目录
// 原型 : void SetWorkingDir(const TCString & sWorkingDir)
// 参数 : 接口文件工作目录
// 返回 : 无
// 说明 :
//==========================================================================
void TCThreadRun::SetWorkingDir(const TCString& sWorkingDir)
{
    m_sWorkingDir = sWorkingDir;
    ForceDirectoriesE(m_sWorkingDir);
    return;
}

//==========================================================================
// 函数 : TCThreadRun::SetSendDir
// 用途 : 设置接口文件目标目录
// 原型 : void SetSendDir(const TCString & sWorkingDir)
// 参数 : 接口文件目标目录
// 返回 : 无
// 说明 :
//==========================================================================
void TCThreadRun::SetSendDir(const TCString& sSendDir)
{
    m_sSendDir = sSendDir;
    ForceDirectoriesE(m_sSendDir);
    return;
}

//==========================================================================
// 函数 : TCThreadRun::SetCycleId
// 用途 : 设置处理周期
// 原型 : void SetCycleId(const TCString& sCycleId);
// 参数 : 处理周期
// 返回 : 无
// 说明 :
//==========================================================================
void TCThreadRun::SetCycleId(const TCString& sCycleId)
{
    m_sCycleId = sCycleId;
    return;
}

//==========================================================================
// 函数 : TCThreadRun::SetSQLList
// 用途 : 设置接口查询SQL语句列表
// 原型 : void SetSQLList(const TCStringList& slSQLList)
// 参数 : SQL列表
// 返回 : 无
// 说明 : 
//==========================================================================
void TCThreadRun::SetSQLList(TCStringList& slSQLList)
{
    m_slSQLList = slSQLList;
    return;
}

//==========================================================================
// 函数 : TCThreadRun::Execute
// 用途 : 服务线程的执行函数,接受客户端的连接
// 原型 : void Execute()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCThreadRun::Execute()
{
    if (IsTerminated())
        Terminate();
    if (RequestStop())
        Terminate();

    ClientExecute();
}

//==========================================================================
// 函数 : TCThreadRun::ExecSQLScript
// 用途 : 运行SQL脚本
// 原型 : void ExecSQLScript(long &nErrorCode,TCString &sErrorMsg)
// 参数 : 无
// 返回 : 0成功，非0（错误代码）失败
// 说明 ：m_nErrorCode错误码，0成功，非0（错误代码）失败
//        m_sErrorMsg错误信息
//==========================================================================
void TCThreadRun::ExecSQLScript(long &nErrorCode,TCString &sErrorMsg)
{
    TCString sSQL,sOneLine;
    TCString sStartTime,sEndTime;
    TCFileStream tcWorkingFile;
    long i,j,nSqlSeq,nFetchRtn;
    bool bIgnoreError;
    TCString sWorkingFile; //接口文件全路径

    nSqlSeq=0;
    bIgnoreError=false;
    m_nSheetCount=0;
    nErrorCode=0;
    sErrorMsg="";

    try
    {
        //若父任务不生成接口文件，则直接返回成功
        if (m_nType==1 && !m_bBuildFileFlag)
        {
            return;
        }

        //删除已有的接口文件
        if (m_bBuildFileFlag)
        {
            m_sWorkingFileName = GetResultFileName(m_sTaskId,m_nStep);
            sWorkingFile = MergePathAndFile(m_sWorkingDir, m_sWorkingFileName);
            if (FileExists(sWorkingFile))
                DeleteFileE(sWorkingFile);
        }

        //如果是父脚本，不用执行，只从记录中获取生成的接口条数
        if (m_nType==1)
        {
            m_sWorkingFileName = GetResultFileName(m_sTaskId,m_nStep);
            sSQL="select sheet_cnt from mb_if_jyfx_task_running"
                 " where script_id="+IntToStr(m_nScriptId)+
                 " and cycle_id="+QuotedStr(m_sCycleId);
            m_pTaskQuery->SetSQL(sSQL);
            if (m_pTaskQuery->OpenE() == false)
            {
                sErrorMsg="错误信息:"+m_TaskDataBase.GetErrorString();
                nErrorCode=m_TaskDataBase.GetErrorCode();
                RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
                
                return;
            }
            while (m_pTaskQuery->FetchE() == 0)
            {
                m_nSheetCount=StrToInt(m_pTaskQuery->GetFieldValue("sheet_cnt"));
            }

            //合并子脚本生成的子接口文件
            if (!LinkSubFile(m_sWorkingFileName))
            {
                sErrorMsg="错误信息:合并子接口文件失败！";
                //错误码为1，代表操作系统合并子接口文件失败
                nErrorCode=1;
                RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
                
                return;
            }
        }

        if (m_bBuildFileFlag)
        {
            tcWorkingFile.Open(sWorkingFile, omText|omAppend);
        }

        if (m_nType!=1)
        {
            //执行每个子SQL
            for (i=0;i<m_slSQLList.GetCount();i++)
            {
                nSqlSeq++;
                sSQL = m_slSQLList[i];

                if (m_bRecordRunTime)
                {
                    sStartTime=TCTime::Now();
                }

                if (m_bBuildFileFlag)
                {
                    m_pWorkQuery->SetSQL(sSQL);
                    if (m_pWorkQuery->OpenE() == false)
                    {
                        tcWorkingFile.Close();
                        DeleteFileE(sWorkingFile);
                        sErrorMsg="序号"+IntToStr(nSqlSeq)+"错误信息:"+m_WorkDataBase.GetErrorString();
                        nErrorCode=m_WorkDataBase.GetErrorCode();
                        
                        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
                        return;
                    }
                    while (nFetchRtn = m_pWorkQuery->FetchE() == 0)
                    {
                        sOneLine = "";
                        for (j=1; j<=m_pWorkQuery->GetColumnCount(); j++)
                        {
                            sOneLine = sOneLine + m_pWorkQuery->GetFieldValue(j) + "$";
                        }
                        tcWorkingFile.WriteLn(sOneLine);
                        m_nSheetCount ++;
                        if ((m_nSheetCount % 10000)==0)
                            Application.ProcessMessages();
                    }
                    //获取结果集中断
                    if (nFetchRtn == -1)
                    {
                        sErrorMsg="序号"+IntToStr(nSqlSeq)+"错误信息:"+m_WorkDataBase.GetErrorString();
				        nErrorCode=m_WorkDataBase.GetErrorCode();
				        
				        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
				        tcWorkingFile.Close();
				        DeleteFileE(sWorkingFile);
				        return;
                    }
                }
                else
                {
                    if (!m_pWorkQuery->ExecSQL(sSQL))
                    {
                        if (!bIgnoreError)
                        {
                            sErrorMsg="序号"+IntToStr(nSqlSeq)+"错误信息:"+m_WorkDataBase.GetErrorString();
                            nErrorCode=m_WorkDataBase.GetErrorCode();
                            RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
                            
                            return;
                        }
                    }
                }

                if (m_bRecordRunTime)
                {
                    sEndTime=TCTime::Now();
                    sErrorMsg="序号"+IntToStr(nSqlSeq)+"耗时(秒):"+IntToStr(TCTime::SecondsAfter(sStartTime,sEndTime))+"\n";
                    RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
                }
            }//end of for
        }//end of if

        //写接口文件结尾行
        if (m_bBuildFileFlag)
        {
            //li.you by del
            /* 	
            if (m_nStep==0)
            {
                sOneLine = ENDOFFILE + m_sWorkingFileName + " " + IntToStr(m_nSheetCount);
                tcWorkingFile.WriteLn(sOneLine);
            }
            */
            tcWorkingFile.Close();
        }
    } 
    catch(TCException &e)
    {
        sErrorMsg="序号"+IntToStr(nSqlSeq)+"错误信息:"+m_WorkDataBase.GetErrorString();
        nErrorCode=m_WorkDataBase.GetErrorCode();
        
        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
        tcWorkingFile.Close();
        DeleteFileE(sWorkingFile);
        return;
    }
    catch(...)
    {
        sErrorMsg="序号"+IntToStr(nSqlSeq)+"错误信息:"+m_WorkDataBase.GetErrorString();
        nErrorCode=m_WorkDataBase.GetErrorCode();
        
        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
        tcWorkingFile.Close();
        DeleteFileE(sWorkingFile);
        return;
    }

    return;
}


//==========================================================================
// 函数 : TCThreadRun::SendMessage
// 用途 : 根据配置文件中SendErrorMsg值的设置来决定是否发送信息
// 原型 : bool SendMessage(const long nError);
// 参数 : sMsg要发送的信息
// 返回 : true插入短信表成功false插入短信表失败
// 说明 :
//==========================================================================
bool TCThreadRun::SendMessage(const TCString sMsg)
{
	TCString sSQL;
	
    sSQL="insert into kt.sms_info@lk_yingzhang(ID,MSISDN,FLAG,MSG)"
		 " select kt.sms_id.nextval@lk_yingzhang,phone_id,'SEND',"+QuotedStr(sMsg)+
		 " from mb_if_jyfx_sms_phone";

    if (!m_pTaskQuery->ExecSQL(sSQL)) return false;	
	return true;
}

//==========================================================================
// 函数 : TCThreadRun::AlertMessage
// 用途 : 根据接口文件的记录数和日志表MB_IF_JYFX_TASK_LOG中上周期的记录数来判断是否告警
// 原型 : void AlertMessage(const TCString sFileName);
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCThreadRun::AlertMessage()
{
	TCString sSQL,sSQLMsg,sAlertExp,sPreCycleId;
	long nSheetCount,nResult;
	
	//0.获取上周期
	if (Length(m_sCycleId)==6)
	{
		sPreCycleId=GetNextValue("MON",m_sCycleId,-1);
	}
	else if (Length(m_sCycleId)==8)
	{
		sPreCycleId=GetNextValue("DAY",m_sCycleId,-1);
	}
	else
		return;
	
	//1.获取上周期的记录数
	nSheetCount=0;
    sSQL="select nvl(to_number(substr(max(to_char(etime,'YYYYMMDDHH24MISS')||to_char(sheet_cnt)),15)),-1) sheet_cnt"
    	 " from mb_if_jyfx_task_log"
         " where type<>2 and task_id="+QuotedStr(m_sTaskId)+
         " and cycle_id="+QuotedStr(sPreCycleId);
    m_pTaskQuery->SetSQL(sSQL);
    if (m_pTaskQuery->OpenE() == false)
    {
        sSQLMsg="错误信息:"+m_TaskDataBase.GetErrorString();
        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sSQLMsg);
        return;
    }
    while (m_pTaskQuery->FetchE() == 0)
    {
        nSheetCount=StrToInt(m_pTaskQuery->GetFieldValue("sheet_cnt"));
    }
    if (nSheetCount==-1) return;
    
    //2.获取接口的告警表达式
    sSQL="select alert_expression"
    	 " from mb_if_jyfx_task_list"
         " where task_id="+QuotedStr(m_sTaskId);
    m_pTaskQuery->SetSQL(sSQL);
    if (m_pTaskQuery->OpenE() == false)
    {
        sSQLMsg="错误信息:"+m_TaskDataBase.GetErrorString();
        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sSQLMsg);
        return;
    }
    while (m_pTaskQuery->FetchE() == 0)
    {
        sAlertExp=m_pTaskQuery->GetFieldValue("alert_expression");
    }
    if (Length(AllTrim(sAlertExp))==0) return;
    
    //3.替换表达式中的THIS和LAST
    sAlertExp=ReplaceAllSubStr(sAlertExp,"THIS",IntToStr(m_nSheetCount));
    sAlertExp=ReplaceAllSubStr(sAlertExp,"LAST",IntToStr(nSheetCount));    
    
    //4.计算表达式
    sSQL="select case when "+sAlertExp+" then 1 else 0 end result from dual";
    m_pTaskQuery->SetSQL(sSQL);
    if (m_pTaskQuery->OpenE() == false)
    {
        sSQLMsg="错误信息:"+m_TaskDataBase.GetErrorString();
        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sSQLMsg);
        return;
    }
    while (m_pTaskQuery->FetchE() == 0)
    {
        nResult=StrToInt(m_pTaskQuery->GetFieldValue("result"));
    }
    
    //5.插入告警信息
    if (nResult==0)
    {
	    sSQLMsg="经分接口中接口代码："+m_sTaskId+"，接口名称："+m_sScriptName+"，周期："+m_sCycleId+"，记录条数："+IntToStr(m_nSheetCount)
			+"，记录数不合理。请相关人员检查并处理！";
		SendMessage(sSQLMsg);
	}
	return;
}

//==========================================================================
// 函数 : TCThreadRun::GeneralErrorHandle
// 用途 : 根据错误代码及MB_IF_JYFX_GENERROR_HANDLE表中定义的错误处理手段来处理
// 原型 : bool GeneralErrorHandle(const long nError);
// 参数 : nError错误代码
// 返回 : true只针对生成空的接口文件false没有完全处理完的情况
// 说明 :
//==========================================================================
bool TCThreadRun::GeneralErrorHandle(const long nError)
{
    bool bReturn;
    TCString sSQL,sSQLMsg,sHandleTypeMsg;
    long nHandleType,nDelaySeconds;
    TCQuery *pHandleErrorTaskQuery;
    
    pHandleErrorTaskQuery = new TCQuery(m_TaskDataBase);
    bReturn=false;
    //1.获取相应该接口错误代码的通常处理手段编号
    nHandleType=0;
    sSQL="select nvl(handle_type,0) handle_type"
    	 " from MB_IF_JYFX_GENERROR_HANDLE"
         " where task_id="+QuotedStr(m_sTaskId)+
         " and error_num="+IntToStr(nError);
    pHandleErrorTaskQuery->SetSQL(sSQL);
    if (pHandleErrorTaskQuery->OpenE() == false)
    {
        sSQLMsg="错误信息:"+m_TaskDataBase.GetErrorString();
        RecordTaskMsg(pHandleErrorTaskQuery,m_nScriptId,m_sCycleId,sSQLMsg);
        
        pHandleErrorTaskQuery->Close();
	    delete pHandleErrorTaskQuery;
	    pHandleErrorTaskQuery=NULL;
        return bReturn;
    }
    sHandleTypeMsg="";
    while (pHandleErrorTaskQuery->FetchE() == 0)
    {
        nHandleType=StrToInt(pHandleErrorTaskQuery->GetFieldValue("handle_type"));
	    if (nHandleType==0) return bReturn;
    
	    //2.做相应的处理
	    switch (nHandleType)
	    {
	    	case 1: //送空接口
		    	BuildEmptyFileToCrm();
	 			sHandleTypeMsg=sHandleTypeMsg+"1、送空接口；";
	 			
		        bReturn=true;
		        break;
	    	case 2: //执行指定SQL
	    	
	    		sHandleTypeMsg=sHandleTypeMsg+"2、执行指定SQL；";
	    		break;
	    	case 3: //延时执行
			    sSQL="select nvl(delay_seconds,0) delay_seconds"
			    	 " from MB_IF_JYFX_GENERROR_HANDLE"
			         " where task_id="+QuotedStr(m_sTaskId)+
			         " and error_num="+IntToStr(nError)+
			         " and handle_type="+IntToStr(nHandleType);
			    m_pTaskQuery->SetSQL(sSQL);
			    if (m_pTaskQuery->OpenE() == false)
			    {
			        sSQLMsg="错误信息:"+m_TaskDataBase.GetErrorString();
			        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sSQLMsg);
			        return bReturn;
			    }
			    while (m_pTaskQuery->FetchE() == 0)
			    {
			        nDelaySeconds=StrToInt(m_pTaskQuery->GetFieldValue("delay_seconds"));
			    }
	        	
	        	sSQL="update MB_IF_JYFX_TASK_RUNNING set PLAN_TIME=sysdate+"+IntToStr(nDelaySeconds)+"/60/60/24"
	             " where script_id="+IntToStr(m_nScriptId)+
	             " and cycle_id="+QuotedStr(m_sCycleId);
		
		        if (!m_pTaskQuery->ExecSQL(sSQL)) return bReturn;
		        UpdateTaskStatus(m_pTaskQuery,m_nScriptId,m_sCycleId,TASK_STATUS_UNCOMPLETE);   
		        
		        sHandleTypeMsg=sHandleTypeMsg+"3、延时"+IntToStr(nDelaySeconds)+"秒后执行；";
	    		break;
	    	default:   	
	    		break;
	    }
	}
    pHandleErrorTaskQuery->Close();
    delete pHandleErrorTaskQuery;
    pHandleErrorTaskQuery=NULL;	

    //2.插入错误处理完成信息
    if (m_bSendErrorMsg)
    {
	    sSQLMsg="经分接口中接口代码："+m_sTaskId+"，接口名称："+m_sScriptName+"，周期："+m_sCycleId+"，错误处理信息："+sHandleTypeMsg
			+"。该短信系统自动发送，请相关人员检查并处理！";
		SendMessage(sSQLMsg);
	}
	
    return bReturn;
}

//==========================================================================
// 函数 : TCThreadRun::ClientExecute
// 用途 : 主要实现具体的业务逻辑，由客户端重载
// 原型 : bool ClientExecute()
// 参数 :
// 返回 : true统计完成、false统计失败
// 说明 : 应用程序需继承此类并重载此函数
//==========================================================================
bool TCThreadRun::ClientExecute()
{
    ASSERT(m_WorkDataBase.IsConnected());
    ASSERT(m_pWorkQuery != NULL);

    TCString sSQL,sRunParam,sRunParamPart,sErrorMsg;
    TCString sSubStr,sReplaceStr;
    TCString sStart,sEnd;
    bool bSame;
    TCStringList slOldRunParam,slNewRunParam; //记录替换参数
    TCList lstSingleRunParam,lstMultiRunParam;
    TSRunParam *pstRunParam;
    long i,j,nPos,nReturn;

    UpdateTaskStatus(m_pTaskQuery,m_nScriptId,m_sCycleId,TASK_STATUS_WAITING);
    UpdateTaskSPID(m_pTaskQuery,m_nScriptId,m_sCycleId,GetThreadID());

    //清空真正可执行的SQL语句列表
    if (!GetTaskSql(m_pTaskQuery,m_nScriptId,m_sCycleId,m_nStep,m_nType,m_bBuildFileFlag,m_slSQLList,lstSingleRunParam,lstMultiRunParam,sErrorMsg))
    {
        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
        UpdateTaskStatus(m_pTaskQuery,m_nScriptId,m_sCycleId,TASK_STATUS_FAIL);
        return false;
    }

    //运行参数可无，或者是-r和-c打头，如-r XXX=891 -c YYYYMM=MON[200508-200501]
    //允许有多个-c或者-r，但每个要单独一行
    //-r代表单次替代，-c代表是循环替代，规则有三个：MON月份，NUM数字，DAY日期
    //注释部分在c_handletask.cpp中的IsFrequencyChanged实现，如果保留字很多的话，只能在这实现
    for (i=0;i<lstSingleRunParam.GetCount();i++)
    {
        pstRunParam=(TSRunParam *)lstSingleRunParam[i];
        for (j=0;j<m_slSQLList.GetCount();j++)
        {
            m_slSQLList[j]=ReplaceAllSubStr(m_slSQLList[j],pstRunParam->m_sReplaced,pstRunParam->m_sStart);
        }
    }

    if ((lstMultiRunParam.GetCount()==0) || (m_nType==1))
    {
        //执行SQL，根据设置决定是否生成接口
        ExecSQLScript(m_nErrorCode,m_sErrorMsg);
        //错误码为0是没有错误，其他为有错误发生
        if (m_nErrorCode!=0)
        {
        	if (m_bSendErrorMsg)
        	{
			    sErrorMsg="经分接口中接口代码："+m_sTaskId+"，接口名称："+m_sScriptName+"，周期："+m_sCycleId+"，错误信息："+m_sErrorMsg
					+"。请相关人员检查并处理！";
				SendMessage(sErrorMsg);
			}
		        	
            UpdateTaskStatus(m_pTaskQuery,m_nScriptId,m_sCycleId,TASK_STATUS_FAIL);
            if (!GeneralErrorHandle(m_nErrorCode))
            	return false;
        }

        if (m_bBuildFileFlag)
        {
            if (m_nType!=1)
            {
            	UpdateTaskSheet(m_pTaskQuery,m_nScriptId,m_sCycleId,m_nSheetCount);
            }
            if (m_nType!=2)
            {
	            if(SubmitFileToCrm(m_sWorkingFileName))
	            {
	                //long nSize = FileSize(m_sWorkingFileName);
	                //AddInterfaceLog(m_sTaskId,m_sWorkingFileName,m_sCycleId,nSize,m_nSheetCount);
	                AddInterfaceEndFile(m_sWorkingFileName,m_nSheetCount);
	            }
	            
	            //根据记录数决定是否发告警信息
	            if (m_bSendErrorMsg)
	            {
	            	AlertMessage();
	            }
            }
        }

        //更新日志记录，使任务状态为已完成
        UpdateTaskStatus(m_pTaskQuery,m_nScriptId,m_sCycleId,TASK_STATUS_SUCCESS);
    }
    else
    {
        //插入虚节点到任务表中
        //1.生成SQL运行参数列表
        //1.1处理单次因子
        sRunParam="";
        for (i=0;i<lstSingleRunParam.GetCount();i++)
        {
            pstRunParam=(TSRunParam *)lstSingleRunParam[i];
            sRunParam=sRunParam+"-r "+pstRunParam->m_sReplaced+"="+pstRunParam->m_sStart+"\n";
        }

        slOldRunParam.Clear();
        slOldRunParam.Add(sRunParam);
        slNewRunParam.Clear();

        //1.2处理循环因子
        for (i=0;i<lstMultiRunParam.GetCount();i++)
        {
            pstRunParam=(TSRunParam *)lstMultiRunParam[i];
            sStart=pstRunParam->m_sStart;
            sEnd=pstRunParam->m_sEnd;
            bSame=false;
            while (true)
            {
                for (j=0;j<slOldRunParam.GetCount();j++)
                {
                    slNewRunParam.Add(slOldRunParam[j]+"-r "+pstRunParam->m_sReplaced+"="+sStart+"\n");
                }

                if (bSame) break;
                sStart=GetNextValue(pstRunParam->m_sReplaceStyle,sStart,pstRunParam->m_nAddFactor);
                if (sStart==sEnd) bSame=true;
            }//end of while

            slOldRunParam.Clear();
            slOldRunParam=slNewRunParam;
            slNewRunParam.Clear();
        }//end of for


        //2.插入虚节点，暂时设计虚节点的最大数为999
        m_WorkDataBase.SetCommitMode(false);
        for (i=0;i<slOldRunParam.GetCount();i++)
        {
            sSQL="insert into mb_if_jyfx_task_running"
                 " select task_id,cycle_id,"+IntToStr(i+1)+" step,script_id parent_script_id,script_id*1000+"
                 +IntToStr(i+1)+" script_id,"
                 " script_name,2 type,module,"+QuotedStr(slOldRunParam[i])+" run_param,"
                 " run_sql1,run_sql2,run_sql3,plan_time,"
                 " null stime,null etime,"
                 +QuotedStr(To_String(TASK_STATUS_UNCOMPLETE))+" status,null error_msg,"
                 " priority,flag,null spid,0 sheet_cnt"
                 " from mb_if_jyfx_task_running"
                 " where script_id="+IntToStr(m_nScriptId)+
                 " and cycle_id="+QuotedStr(m_sCycleId);

            if (!m_pTaskQuery->ExecSQL(sSQL)) return false;
        }//end of for

        m_TaskDataBase.Commit();
        m_TaskDataBase.SetCommitMode(true);
    }

    lstSingleRunParam.Clear();
    lstMultiRunParam.Clear();
    slOldRunParam.Clear();

    return true;
}


//==========================================================================
// 函数 : TCThreadRun::LinkSubFile
// 用途 : 合并子脚本生成的接口文件
// 原型 : bool LinkSubFile(const TCString sFileName);
// 参数 : 接口文件名
// 返回 : 
// 说明 :
//==========================================================================
bool TCThreadRun::LinkSubFile(const TCString sResultFileName)
{
    TCString sSrcFile,sDstFile,sSubFileName;
    TCString sCommandLine;

    sSubFileName="_"+Left(sResultFileName,Length(sResultFileName) - 10)+"*.AVL";
    sSrcFile = MergePathAndFile(m_sWorkingDir, sSubFileName);
    sDstFile = MergePathAndFile(m_sWorkingDir, sResultFileName);
    #ifdef __WIN32__

    #else
        sCommandLine = "cat "+sSrcFile+" > " + sDstFile;
        if (!TCSystem::RunCommand(sCommandLine))
            throw TCException("TCThreadRun::LinkSubFile cat file error!");
        sCommandLine = "rm "+sSrcFile;
        if (!TCSystem::RunCommand(sCommandLine))
            throw TCException("TCThreadRun::LinkSubFile rm file error!");
    #endif
    return true;
}

//==========================================================================
// 函数 : TCThreadRun::BuildEmptyFileToCrm
// 用途 : 生成空接口文本文件送经分
// 原型 : void BuildEmptyFileToCrm();
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCThreadRun::BuildEmptyFileToCrm()
{
	TCString sWorkingFile;
	TCFileStream tcWorkingFile;
	TCString sOneLine;
	
	m_nSheetCount=0;
        //删除已有的接口文件
        if (m_bBuildFileFlag)
        {
            m_sWorkingFileName = GetResultFileName(m_sTaskId,m_nStep);
            sWorkingFile = MergePathAndFile(m_sWorkingDir, m_sWorkingFileName);
            if (FileExists(sWorkingFile))
                DeleteFileE(sWorkingFile);
            
            tcWorkingFile.Open(sWorkingFile, omText|omAppend);
            if (m_nStep==0)
            {
            	//li.you by del 
            	/*
                sOneLine = ENDOFFILE + m_sWorkingFileName + " " + IntToStr(m_nSheetCount);
                tcWorkingFile.WriteLn(sOneLine);
                */
            }
            tcWorkingFile.Close();
        }
        return;
}
        
//==========================================================================
// 函数 : TCThreadRun::SubmitFileToCrm
// 用途 : 把接口文本文件压缩并提交到发送目录
// 原型 : bool SubmitFileToCrm(const TCString& sInterfaceCode);
// 参数 : 接口文件编号
// 返回 : 
// 说明 :
//==========================================================================
bool TCThreadRun::SubmitFileToCrm(const TCString sInterfaceCode)
{
    TCString sSrcFile, sDestFile;
    TCString sCommandLine;

    sSrcFile = MergePathAndFile(m_sWorkingDir, sInterfaceCode);
    sDestFile = MergePathAndFile(m_sSendDir, sInterfaceCode);
    if (!FileExists(sSrcFile))
        return false;
    RenameFileE(sSrcFile, sDestFile, true);

    #ifdef __WIN32__
        //STA_Lock.Enter();
        TCListFile::AppendFileToList(sDestFile);
        //STA_Lock.Leave();
    #else
        sCommandLine = "compress  -f " + sDestFile;
        if (!TCSystem::RunCommand(sCommandLine))
            throw TCException("TCThreadRun::SubmitFileToCrm compress file error!");
        //STA_Lock.Enter();
        TCListFile::AppendFileToList(sDestFile + ".Z");
        //STA_Lock.Leave();
    #endif
    return true;
}

//==========================================================================
// 函数 : TCThreadRun::AddInterfaceEndFile
// 用途 : 写每日接口文件结束文本
// 原型 : void AddInterfaceEndFile(TCString sFileName,long nSheetCount);
// 参数 : 文件名, 话单总数
// 返回 : 无
// 说明 :
//==========================================================================
void TCThreadRun::AddInterfaceEndFile(TCString sFileName,long nSheetCount)
{
    TCFileStream tcWorkingFile;
    TCString sEndSrcFileName;
    
    //li.you by add 
    long      nFileSize      = FileSize(    MergePathAndFile(m_sSendDir,sFileName+".Z")  );
    TCString sFileBuildTime  = FileGetTime( MergePathAndFile(m_sSendDir,sFileName+".Z")  );
    TCString sFileDataTime   = Mid(ExtractFileName(sFileName),7,8);
    
    if (!tcWorkingFile.HasOpened())
    {
        sEndSrcFileName = MergePathAndFile(m_sWorkingDir,CutFileExt(ExtractFileName(sFileName)) + ".CHK");
        if (!FileExists(sEndSrcFileName))
        {
            tcWorkingFile.Open(sEndSrcFileName, omText|omAppend);
            tcWorkingFile.WriteLn(ExtractFileName(sFileName+".Z")+ "$"+IntToStr(nFileSize)+"$"+IntToStr(nSheetCount)+"$"+sFileDataTime+"$"+sFileBuildTime);
        }
    }
    tcWorkingFile.Close();
    TCString sEndDestFileName;
    sEndDestFileName = MergePathAndFile(m_sSendDir,CutFileExt(ExtractFileName(sEndSrcFileName)) + ".CHK");
    RenameFileE(sEndSrcFileName,sEndDestFileName);
    //STA_Lock.Enter();
    TCListFile::AppendFileToList(sEndDestFileName);
    //STA_Lock.Leave();
}

//==========================================================================
// 函数 : TCThreadRun::GetResultFileName
// 用途 : 根据接口单元编号形成接口文件名
// 原型 : void GetResultFileName(TCString & sInterfaceCode);
// 参数 : 接口单元编号
// 返回 : 当前日期当前编号的接口文件名
// 说明 : 增加对虚任务生成的文件名处理
//==========================================================================
TCString TCThreadRun::GetResultFileName(const TCString sInterfaceCode,long nFileSerial)
{
	TCString sResultFileName;
	
    if (Left(sInterfaceCode,1) == TCString("M") )
        sResultFileName=sInterfaceCode + m_sCycleId+ "00" + Padl(IntToStr(nFileSerial),6,'0') + ".AVL";
    else
        sResultFileName=sInterfaceCode + m_sCycleId + Padl(IntToStr(nFileSerial),6,'0') + ".AVL";
	
	if (nFileSerial>0)
	{
		sResultFileName="_"+sResultFileName;    
    }
    return sResultFileName;
}

//==========================================================================
// 函数 : TCThreadRun::AddInterfaceLog
// 用途 : 写接口文件处理日志
// 原型 : void AddInterfaceLog(TCString sTaskId, TCString sFileName,
//        TCString sCycleId, long nFileSize, long nSheetCount);
// 参数 : 接口代码、文件名、处理周期, 文件大小, 话单总数,
// 返回 : 无
// 说明 :
//==========================================================================
void TCThreadRun::AddInterfaceLog(TCString sTaskId, TCString sFileName,TCString sCycleId, long nFileSize, long nSheetCount)
{
    TCString sLogFileName;
    sLogFileName = TCAppLog::GetDailyLogFileNameOfApplication();
    //==== 1. 如果日志文件不存在, 则创建之 ========
    if (!FileExists(sLogFileName))
    {
        TCDBFCreate dcCreate(sLogFileName, 6);
        dcCreate.AddField("task_id",  'C', 6);       // 接口文件类型
        dcCreate.AddField("file_name",  'C', 24);      // 接口文件名
        dcCreate.AddField("cycle_id",  'C', 8);       // 处理日期
        dcCreate.AddField("file_size",  'N', 10);      // 文件大小
        dcCreate.AddField("sheet_cnt",  'N', 9);       // 话单总数
        dcCreate.AddField("act_time",   'C', 14);      // 填写时间

        dcCreate.CreateDBF();
    }

    //===== 2. 将相关信息写入日志文件 =========
    TCFoxDBF dbfLog;
    dbfLog.AttachFile(sLogFileName);

    TCString sPureFileName = ExtractFileName(sFileName);
    TCString dtActTime = TCTime::Now();

    dbfLog.DBBind("task_id",  sTaskId);
    dbfLog.DBBind("file_name",  sPureFileName);
    dbfLog.DBBind("cycle_id",  sCycleId);
    dbfLog.DBBind("file_size",  nFileSize);
    dbfLog.DBBind("sheet_cnt",  nSheetCount);
    dbfLog.DBBind("act_time",   dtActTime);

    dbfLog.Append();
    dbfLog.CloseDBF();
}

//==========================================================================
// 函数 : TCThreadRun::RequestStop
// 用途 : 根据标志文件是否存在来判断用户是否请求退出程序
// 原型 : void RequestStop();
// 参数 : 无
// 返回 : true请求退出，false没有请求退出
// 说明 :
//==========================================================================
bool TCThreadRun::RequestStop()
{
    TCString sStopInfoFileName;
    //Chaned by Kevin since there is no parameters +"_"+Application.GetProcessFlag();
    sStopInfoFileName = TAppPath::AppRunningInfo() + "stop_"
                    + Application.GetAppName();
    if (FileExists(sStopInfoFileName))
      return true;
    else
      return false;
}

