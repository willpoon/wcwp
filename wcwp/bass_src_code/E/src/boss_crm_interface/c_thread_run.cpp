//---------------------------------------------------------------------------

#pragma hdrstop
#include "c_thread_run.h"
#include "c_handletask.h"
#include "c_base_func.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)

TCCriticalSection STA_Lock;

//==========================================================================
// ���� : TCThreadRun::TCThreadRun
// ��; : ���캯��
// ԭ�� :
// ���� :
// ���� :
// ˵�� :
//==========================================================================
TCThreadRun::TCThreadRun(const TSDBConnectConfig tsWorkDBCon,
const TSDBConnectConfig tsTaskDBCon,const bool bRecordRunTime,const bool bSendErrorMsg)
{
	TCString sSQL;

    m_bRecordRunTime=bRecordRunTime;
    m_bSendErrorMsg=bSendErrorMsg;

    //���ӹ������ݿ�
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
    
    //���Ż�����
    //sSQL="alter session set optimizer_mode = 'rule'";
	//m_pWorkQuery->ExecSQL(sSQL);
    //sSQL="alter session set sort_area_size=500000000";
	//m_pWorkQuery->ExecSQL(sSQL);

    //�����������ݿ�
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
// ���� : TCThreadRun::TCThreadRun
// ��; : ���캯��
// ԭ�� :
// ���� :
// ���� :
// ˵�� :
//==========================================================================
TCThreadRun::TCThreadRun()
{
    //
}

//==========================================================================
// ���� : TCThreadRun::~TCThreadRun()
// ��; : ��������
// ԭ�� : ~TCThreadRun()
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
TCThreadRun::~TCThreadRun()
{
    //�Ͽ����ݿ�����
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
// ���� : TCThreadRun::GetFinished
// ��; : ����m_bFinished
// ԭ�� : void GetFinished()
// ���� : ��
// ���� : bool
// ˵�� :
//==========================================================================
bool TCThreadRun::GetFinished()
{
    return m_bFinished;
}

//==========================================================================
// ���� : TCThreadRun::SetWorkingDir
// ��; : ���ýӿ��ļ�����Ŀ¼
// ԭ�� : void SetWorkingDir(const TCString & sWorkingDir)
// ���� : �ӿ��ļ�����Ŀ¼
// ���� : ��
// ˵�� :
//==========================================================================
void TCThreadRun::SetWorkingDir(const TCString& sWorkingDir)
{
    m_sWorkingDir = sWorkingDir;
    ForceDirectoriesE(m_sWorkingDir);
    return;
}

//==========================================================================
// ���� : TCThreadRun::SetSendDir
// ��; : ���ýӿ��ļ�Ŀ��Ŀ¼
// ԭ�� : void SetSendDir(const TCString & sWorkingDir)
// ���� : �ӿ��ļ�Ŀ��Ŀ¼
// ���� : ��
// ˵�� :
//==========================================================================
void TCThreadRun::SetSendDir(const TCString& sSendDir)
{
    m_sSendDir = sSendDir;
    ForceDirectoriesE(m_sSendDir);
    return;
}

//==========================================================================
// ���� : TCThreadRun::SetCycleId
// ��; : ���ô�������
// ԭ�� : void SetCycleId(const TCString& sCycleId);
// ���� : ��������
// ���� : ��
// ˵�� :
//==========================================================================
void TCThreadRun::SetCycleId(const TCString& sCycleId)
{
    m_sCycleId = sCycleId;
    return;
}

//==========================================================================
// ���� : TCThreadRun::SetSQLList
// ��; : ���ýӿڲ�ѯSQL����б�
// ԭ�� : void SetSQLList(const TCStringList& slSQLList)
// ���� : SQL�б�
// ���� : ��
// ˵�� : 
//==========================================================================
void TCThreadRun::SetSQLList(TCStringList& slSQLList)
{
    m_slSQLList = slSQLList;
    return;
}

//==========================================================================
// ���� : TCThreadRun::Execute
// ��; : �����̵߳�ִ�к���,���ܿͻ��˵�����
// ԭ�� : void Execute()
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : TCThreadRun::ExecSQLScript
// ��; : ����SQL�ű�
// ԭ�� : void ExecSQLScript(long &nErrorCode,TCString &sErrorMsg)
// ���� : ��
// ���� : 0�ɹ�����0��������룩ʧ��
// ˵�� ��m_nErrorCode�����룬0�ɹ�����0��������룩ʧ��
//        m_sErrorMsg������Ϣ
//==========================================================================
void TCThreadRun::ExecSQLScript(long &nErrorCode,TCString &sErrorMsg)
{
    TCString sSQL,sOneLine;
    TCString sStartTime,sEndTime;
    TCFileStream tcWorkingFile;
    long i,j,nSqlSeq,nFetchRtn;
    bool bIgnoreError;
    TCString sWorkingFile; //�ӿ��ļ�ȫ·��

    nSqlSeq=0;
    bIgnoreError=false;
    m_nSheetCount=0;
    nErrorCode=0;
    sErrorMsg="";

    try
    {
        //�����������ɽӿ��ļ�����ֱ�ӷ��سɹ�
        if (m_nType==1 && !m_bBuildFileFlag)
        {
            return;
        }

        //ɾ�����еĽӿ��ļ�
        if (m_bBuildFileFlag)
        {
            m_sWorkingFileName = GetResultFileName(m_sTaskId,m_nStep);
            sWorkingFile = MergePathAndFile(m_sWorkingDir, m_sWorkingFileName);
            if (FileExists(sWorkingFile))
                DeleteFileE(sWorkingFile);
        }

        //����Ǹ��ű�������ִ�У�ֻ�Ӽ�¼�л�ȡ���ɵĽӿ�����
        if (m_nType==1)
        {
            m_sWorkingFileName = GetResultFileName(m_sTaskId,m_nStep);
            sSQL="select sheet_cnt from mb_if_jyfx_task_running"
                 " where script_id="+IntToStr(m_nScriptId)+
                 " and cycle_id="+QuotedStr(m_sCycleId);
            m_pTaskQuery->SetSQL(sSQL);
            if (m_pTaskQuery->OpenE() == false)
            {
                sErrorMsg="������Ϣ:"+m_TaskDataBase.GetErrorString();
                nErrorCode=m_TaskDataBase.GetErrorCode();
                RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
                
                return;
            }
            while (m_pTaskQuery->FetchE() == 0)
            {
                m_nSheetCount=StrToInt(m_pTaskQuery->GetFieldValue("sheet_cnt"));
            }

            //�ϲ��ӽű����ɵ��ӽӿ��ļ�
            if (!LinkSubFile(m_sWorkingFileName))
            {
                sErrorMsg="������Ϣ:�ϲ��ӽӿ��ļ�ʧ�ܣ�";
                //������Ϊ1���������ϵͳ�ϲ��ӽӿ��ļ�ʧ��
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
            //ִ��ÿ����SQL
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
                        sErrorMsg="���"+IntToStr(nSqlSeq)+"������Ϣ:"+m_WorkDataBase.GetErrorString();
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
                    //��ȡ������ж�
                    if (nFetchRtn == -1)
                    {
                        sErrorMsg="���"+IntToStr(nSqlSeq)+"������Ϣ:"+m_WorkDataBase.GetErrorString();
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
                            sErrorMsg="���"+IntToStr(nSqlSeq)+"������Ϣ:"+m_WorkDataBase.GetErrorString();
                            nErrorCode=m_WorkDataBase.GetErrorCode();
                            RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
                            
                            return;
                        }
                    }
                }

                if (m_bRecordRunTime)
                {
                    sEndTime=TCTime::Now();
                    sErrorMsg="���"+IntToStr(nSqlSeq)+"��ʱ(��):"+IntToStr(TCTime::SecondsAfter(sStartTime,sEndTime))+"\n";
                    RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
                }
            }//end of for
        }//end of if

        //д�ӿ��ļ���β��
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
        sErrorMsg="���"+IntToStr(nSqlSeq)+"������Ϣ:"+m_WorkDataBase.GetErrorString();
        nErrorCode=m_WorkDataBase.GetErrorCode();
        
        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
        tcWorkingFile.Close();
        DeleteFileE(sWorkingFile);
        return;
    }
    catch(...)
    {
        sErrorMsg="���"+IntToStr(nSqlSeq)+"������Ϣ:"+m_WorkDataBase.GetErrorString();
        nErrorCode=m_WorkDataBase.GetErrorCode();
        
        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
        tcWorkingFile.Close();
        DeleteFileE(sWorkingFile);
        return;
    }

    return;
}


//==========================================================================
// ���� : TCThreadRun::SendMessage
// ��; : ���������ļ���SendErrorMsgֵ�������������Ƿ�����Ϣ
// ԭ�� : bool SendMessage(const long nError);
// ���� : sMsgҪ���͵���Ϣ
// ���� : true������ű�ɹ�false������ű�ʧ��
// ˵�� :
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
// ���� : TCThreadRun::AlertMessage
// ��; : ���ݽӿ��ļ��ļ�¼������־��MB_IF_JYFX_TASK_LOG�������ڵļ�¼�����ж��Ƿ�澯
// ԭ�� : void AlertMessage(const TCString sFileName);
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCThreadRun::AlertMessage()
{
	TCString sSQL,sSQLMsg,sAlertExp,sPreCycleId;
	long nSheetCount,nResult;
	
	//0.��ȡ������
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
	
	//1.��ȡ�����ڵļ�¼��
	nSheetCount=0;
    sSQL="select nvl(to_number(substr(max(to_char(etime,'YYYYMMDDHH24MISS')||to_char(sheet_cnt)),15)),-1) sheet_cnt"
    	 " from mb_if_jyfx_task_log"
         " where type<>2 and task_id="+QuotedStr(m_sTaskId)+
         " and cycle_id="+QuotedStr(sPreCycleId);
    m_pTaskQuery->SetSQL(sSQL);
    if (m_pTaskQuery->OpenE() == false)
    {
        sSQLMsg="������Ϣ:"+m_TaskDataBase.GetErrorString();
        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sSQLMsg);
        return;
    }
    while (m_pTaskQuery->FetchE() == 0)
    {
        nSheetCount=StrToInt(m_pTaskQuery->GetFieldValue("sheet_cnt"));
    }
    if (nSheetCount==-1) return;
    
    //2.��ȡ�ӿڵĸ澯���ʽ
    sSQL="select alert_expression"
    	 " from mb_if_jyfx_task_list"
         " where task_id="+QuotedStr(m_sTaskId);
    m_pTaskQuery->SetSQL(sSQL);
    if (m_pTaskQuery->OpenE() == false)
    {
        sSQLMsg="������Ϣ:"+m_TaskDataBase.GetErrorString();
        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sSQLMsg);
        return;
    }
    while (m_pTaskQuery->FetchE() == 0)
    {
        sAlertExp=m_pTaskQuery->GetFieldValue("alert_expression");
    }
    if (Length(AllTrim(sAlertExp))==0) return;
    
    //3.�滻���ʽ�е�THIS��LAST
    sAlertExp=ReplaceAllSubStr(sAlertExp,"THIS",IntToStr(m_nSheetCount));
    sAlertExp=ReplaceAllSubStr(sAlertExp,"LAST",IntToStr(nSheetCount));    
    
    //4.������ʽ
    sSQL="select case when "+sAlertExp+" then 1 else 0 end result from dual";
    m_pTaskQuery->SetSQL(sSQL);
    if (m_pTaskQuery->OpenE() == false)
    {
        sSQLMsg="������Ϣ:"+m_TaskDataBase.GetErrorString();
        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sSQLMsg);
        return;
    }
    while (m_pTaskQuery->FetchE() == 0)
    {
        nResult=StrToInt(m_pTaskQuery->GetFieldValue("result"));
    }
    
    //5.����澯��Ϣ
    if (nResult==0)
    {
	    sSQLMsg="���ֽӿ��нӿڴ��룺"+m_sTaskId+"���ӿ����ƣ�"+m_sScriptName+"�����ڣ�"+m_sCycleId+"����¼������"+IntToStr(m_nSheetCount)
			+"����¼���������������Ա��鲢����";
		SendMessage(sSQLMsg);
	}
	return;
}

//==========================================================================
// ���� : TCThreadRun::GeneralErrorHandle
// ��; : ���ݴ�����뼰MB_IF_JYFX_GENERROR_HANDLE���ж���Ĵ������ֶ�������
// ԭ�� : bool GeneralErrorHandle(const long nError);
// ���� : nError�������
// ���� : trueֻ������ɿյĽӿ��ļ�falseû����ȫ����������
// ˵�� :
//==========================================================================
bool TCThreadRun::GeneralErrorHandle(const long nError)
{
    bool bReturn;
    TCString sSQL,sSQLMsg,sHandleTypeMsg;
    long nHandleType,nDelaySeconds;
    TCQuery *pHandleErrorTaskQuery;
    
    pHandleErrorTaskQuery = new TCQuery(m_TaskDataBase);
    bReturn=false;
    //1.��ȡ��Ӧ�ýӿڴ�������ͨ�������ֶα��
    nHandleType=0;
    sSQL="select nvl(handle_type,0) handle_type"
    	 " from MB_IF_JYFX_GENERROR_HANDLE"
         " where task_id="+QuotedStr(m_sTaskId)+
         " and error_num="+IntToStr(nError);
    pHandleErrorTaskQuery->SetSQL(sSQL);
    if (pHandleErrorTaskQuery->OpenE() == false)
    {
        sSQLMsg="������Ϣ:"+m_TaskDataBase.GetErrorString();
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
    
	    //2.����Ӧ�Ĵ���
	    switch (nHandleType)
	    {
	    	case 1: //�Ϳսӿ�
		    	BuildEmptyFileToCrm();
	 			sHandleTypeMsg=sHandleTypeMsg+"1���Ϳսӿڣ�";
	 			
		        bReturn=true;
		        break;
	    	case 2: //ִ��ָ��SQL
	    	
	    		sHandleTypeMsg=sHandleTypeMsg+"2��ִ��ָ��SQL��";
	    		break;
	    	case 3: //��ʱִ��
			    sSQL="select nvl(delay_seconds,0) delay_seconds"
			    	 " from MB_IF_JYFX_GENERROR_HANDLE"
			         " where task_id="+QuotedStr(m_sTaskId)+
			         " and error_num="+IntToStr(nError)+
			         " and handle_type="+IntToStr(nHandleType);
			    m_pTaskQuery->SetSQL(sSQL);
			    if (m_pTaskQuery->OpenE() == false)
			    {
			        sSQLMsg="������Ϣ:"+m_TaskDataBase.GetErrorString();
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
		        
		        sHandleTypeMsg=sHandleTypeMsg+"3����ʱ"+IntToStr(nDelaySeconds)+"���ִ�У�";
	    		break;
	    	default:   	
	    		break;
	    }
	}
    pHandleErrorTaskQuery->Close();
    delete pHandleErrorTaskQuery;
    pHandleErrorTaskQuery=NULL;	

    //2.��������������Ϣ
    if (m_bSendErrorMsg)
    {
	    sSQLMsg="���ֽӿ��нӿڴ��룺"+m_sTaskId+"���ӿ����ƣ�"+m_sScriptName+"�����ڣ�"+m_sCycleId+"����������Ϣ��"+sHandleTypeMsg
			+"���ö���ϵͳ�Զ����ͣ��������Ա��鲢����";
		SendMessage(sSQLMsg);
	}
	
    return bReturn;
}

//==========================================================================
// ���� : TCThreadRun::ClientExecute
// ��; : ��Ҫʵ�־����ҵ���߼����ɿͻ�������
// ԭ�� : bool ClientExecute()
// ���� :
// ���� : trueͳ����ɡ�falseͳ��ʧ��
// ˵�� : Ӧ�ó�����̳д��ಢ���ش˺���
//==========================================================================
bool TCThreadRun::ClientExecute()
{
    ASSERT(m_WorkDataBase.IsConnected());
    ASSERT(m_pWorkQuery != NULL);

    TCString sSQL,sRunParam,sRunParamPart,sErrorMsg;
    TCString sSubStr,sReplaceStr;
    TCString sStart,sEnd;
    bool bSame;
    TCStringList slOldRunParam,slNewRunParam; //��¼�滻����
    TCList lstSingleRunParam,lstMultiRunParam;
    TSRunParam *pstRunParam;
    long i,j,nPos,nReturn;

    UpdateTaskStatus(m_pTaskQuery,m_nScriptId,m_sCycleId,TASK_STATUS_WAITING);
    UpdateTaskSPID(m_pTaskQuery,m_nScriptId,m_sCycleId,GetThreadID());

    //���������ִ�е�SQL����б�
    if (!GetTaskSql(m_pTaskQuery,m_nScriptId,m_sCycleId,m_nStep,m_nType,m_bBuildFileFlag,m_slSQLList,lstSingleRunParam,lstMultiRunParam,sErrorMsg))
    {
        RecordTaskMsg(m_pTaskQuery,m_nScriptId,m_sCycleId,sErrorMsg);
        UpdateTaskStatus(m_pTaskQuery,m_nScriptId,m_sCycleId,TASK_STATUS_FAIL);
        return false;
    }

    //���в������ޣ�������-r��-c��ͷ����-r XXX=891 -c YYYYMM=MON[200508-200501]
    //�����ж��-c����-r����ÿ��Ҫ����һ��
    //-r�����������-c������ѭ�������������������MON�·ݣ�NUM���֣�DAY����
    //ע�Ͳ�����c_handletask.cpp�е�IsFrequencyChangedʵ�֣���������ֺܶ�Ļ���ֻ������ʵ��
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
        //ִ��SQL���������þ����Ƿ����ɽӿ�
        ExecSQLScript(m_nErrorCode,m_sErrorMsg);
        //������Ϊ0��û�д�������Ϊ�д�����
        if (m_nErrorCode!=0)
        {
        	if (m_bSendErrorMsg)
        	{
			    sErrorMsg="���ֽӿ��нӿڴ��룺"+m_sTaskId+"���ӿ����ƣ�"+m_sScriptName+"�����ڣ�"+m_sCycleId+"��������Ϣ��"+m_sErrorMsg
					+"���������Ա��鲢����";
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
	            
	            //���ݼ�¼�������Ƿ񷢸澯��Ϣ
	            if (m_bSendErrorMsg)
	            {
	            	AlertMessage();
	            }
            }
        }

        //������־��¼��ʹ����״̬Ϊ�����
        UpdateTaskStatus(m_pTaskQuery,m_nScriptId,m_sCycleId,TASK_STATUS_SUCCESS);
    }
    else
    {
        //������ڵ㵽�������
        //1.����SQL���в����б�
        //1.1����������
        sRunParam="";
        for (i=0;i<lstSingleRunParam.GetCount();i++)
        {
            pstRunParam=(TSRunParam *)lstSingleRunParam[i];
            sRunParam=sRunParam+"-r "+pstRunParam->m_sReplaced+"="+pstRunParam->m_sStart+"\n";
        }

        slOldRunParam.Clear();
        slOldRunParam.Add(sRunParam);
        slNewRunParam.Clear();

        //1.2����ѭ������
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


        //2.������ڵ㣬��ʱ�����ڵ�������Ϊ999
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
// ���� : TCThreadRun::LinkSubFile
// ��; : �ϲ��ӽű����ɵĽӿ��ļ�
// ԭ�� : bool LinkSubFile(const TCString sFileName);
// ���� : �ӿ��ļ���
// ���� : 
// ˵�� :
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
// ���� : TCThreadRun::BuildEmptyFileToCrm
// ��; : ���ɿսӿ��ı��ļ��;���
// ԭ�� : void BuildEmptyFileToCrm();
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
void TCThreadRun::BuildEmptyFileToCrm()
{
	TCString sWorkingFile;
	TCFileStream tcWorkingFile;
	TCString sOneLine;
	
	m_nSheetCount=0;
        //ɾ�����еĽӿ��ļ�
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
// ���� : TCThreadRun::SubmitFileToCrm
// ��; : �ѽӿ��ı��ļ�ѹ�����ύ������Ŀ¼
// ԭ�� : bool SubmitFileToCrm(const TCString& sInterfaceCode);
// ���� : �ӿ��ļ����
// ���� : 
// ˵�� :
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
// ���� : TCThreadRun::AddInterfaceEndFile
// ��; : дÿ�սӿ��ļ������ı�
// ԭ�� : void AddInterfaceEndFile(TCString sFileName,long nSheetCount);
// ���� : �ļ���, ��������
// ���� : ��
// ˵�� :
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
// ���� : TCThreadRun::GetResultFileName
// ��; : ���ݽӿڵ�Ԫ����γɽӿ��ļ���
// ԭ�� : void GetResultFileName(TCString & sInterfaceCode);
// ���� : �ӿڵ�Ԫ���
// ���� : ��ǰ���ڵ�ǰ��ŵĽӿ��ļ���
// ˵�� : ���Ӷ����������ɵ��ļ�������
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
// ���� : TCThreadRun::AddInterfaceLog
// ��; : д�ӿ��ļ�������־
// ԭ�� : void AddInterfaceLog(TCString sTaskId, TCString sFileName,
//        TCString sCycleId, long nFileSize, long nSheetCount);
// ���� : �ӿڴ��롢�ļ�������������, �ļ���С, ��������,
// ���� : ��
// ˵�� :
//==========================================================================
void TCThreadRun::AddInterfaceLog(TCString sTaskId, TCString sFileName,TCString sCycleId, long nFileSize, long nSheetCount)
{
    TCString sLogFileName;
    sLogFileName = TCAppLog::GetDailyLogFileNameOfApplication();
    //==== 1. �����־�ļ�������, �򴴽�֮ ========
    if (!FileExists(sLogFileName))
    {
        TCDBFCreate dcCreate(sLogFileName, 6);
        dcCreate.AddField("task_id",  'C', 6);       // �ӿ��ļ�����
        dcCreate.AddField("file_name",  'C', 24);      // �ӿ��ļ���
        dcCreate.AddField("cycle_id",  'C', 8);       // ��������
        dcCreate.AddField("file_size",  'N', 10);      // �ļ���С
        dcCreate.AddField("sheet_cnt",  'N', 9);       // ��������
        dcCreate.AddField("act_time",   'C', 14);      // ��дʱ��

        dcCreate.CreateDBF();
    }

    //===== 2. �������Ϣд����־�ļ� =========
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
// ���� : TCThreadRun::RequestStop
// ��; : ���ݱ�־�ļ��Ƿ�������ж��û��Ƿ������˳�����
// ԭ�� : void RequestStop();
// ���� : ��
// ���� : true�����˳���falseû�������˳�
// ˵�� :
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

