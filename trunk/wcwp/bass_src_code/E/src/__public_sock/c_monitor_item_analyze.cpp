//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_monitor_item_analyze.h"

//---------------------------------------------------------------------------
#pragma package(smart_init)


void TCMonitorConfig::GetConnectAddress(TCString& sAddress,unsigned short& nPort)
{
   sAddress = ProfileAppString(Application.GetAppName(),"Connection","Address",
                               "132.32.22.104");
   nPort = ProfileAppInt(Application.GetAppName(),"Connection","ConnectPort",8781);
}

//------------2001-2-19 ADD-----------------------------------------------------
void TCMonitorConfig::GetNodeNames(TCStringList & slNode)
{
   int i;
   ProfileAppSession(Application.GetAppName(),"NodeInfo",slNode);
}
//------------------------------------------------------------------------------

TCString TCMonitorConfig::GetPassword()
{
   return ProfileAppString(Application.GetAppName(),"Connection","ConnectPassword","");
}

unsigned short TCMonitorConfig::GetListenPort()
{
   return ProfileAppInt(Application.GetAppName(),"Connection","ListenPort",8781);

}

//-----------20001-2-19 MODIFY--------------------------------------------------
void TCMonitorConfig::GetIncludeList(TCStringList& sItemList,TCString sNodeName)
{
   ProfileAppSession(Application.GetAppName(),"Include "+sNodeName,sItemList);
}
//------------------------------------------------------------------------------

void TCMonitorConfig::GetItemListByInclude(TCString sItemName,TCStringList& sSubItemList)
{
   ProfileAppSession(sItemName,"Item_List",sSubItemList);
}

void TCMonitorConfig::GetItemInfo(TCString sItemName,TCString sSubItemName,
                        TSMItemInfo * pItemInfo)
{
   TCString sFileName;
   TCIniFile CIniFile ;
   sFileName = TAppPath::AppConfig() + sItemName + ".ini";
   CIniFile.Load(sFileName) ;
   pItemInfo->sName = CIniFile.ReadString(sSubItemName,"Name","") ;
   pItemInfo->sTypeName = CIniFile.ReadString(sSubItemName,"TypeName","");
   IfNullStringTerminate(sSubItemName + " : TypeName",pItemInfo->sTypeName);
   pItemInfo->sSource = CIniFile.ReadString(sSubItemName,"Source","") ;
   IfNullStringTerminate(sSubItemName + " : Source",pItemInfo->sSource);
   pItemInfo->sTime_Section = CIniFile.ReadString(sSubItemName,"Time_Section",
                                                  IntToStr(GetDefaultWarnTime())) ;
   pItemInfo->sConfig = CIniFile.ReadString(sSubItemName,"Config","") ;
   pItemInfo->sCommand = CIniFile.ReadString(sSubItemName,"Command","") ;
   pItemInfo->Status = CIniFile.ReadString(sSubItemName,"Status","") ;
   pItemInfo->sFieldNames = CIniFile.ReadString(sSubItemName,"FieldNames","") ;
   pItemInfo->Attribute = "00" ; //初始设置
}

void TCMonitorConfig::IfNullStringTerminate(TCString szItemName,TCString szValue)
{
   if( szValue == TCString("") )
       throw TCException( szItemName + "= NULL, setup error!");
}

long TCMonitorConfig::GetDefaultWarnTime()
{
  return ProfileAppInt(Application.GetAppName(),"Default","Warn_Time_Inteval",300);
}

//------------2001-2-19 ADD-----------------------------------------------------
TCMonitorConfig::TCMonitorConfig()
{
   m_lMItemAnalyze.Clear();
}
TCMonitorConfig::~TCMonitorConfig()
{
   FreeAll();
}
void TCMonitorConfig::FreeAll()
{
   int i;
   TCMItemAnalyze * pMItemAnalyze;
   for( i = 0;i<m_lMItemAnalyze.GetCount();i++)
   {
      pMItemAnalyze = (TCMItemAnalyze *)m_lMItemAnalyze[i];
      delete pMItemAnalyze;
   }
   m_lMItemAnalyze.Clear();

}
long TCMonitorConfig::LoadAll()
{
   int i;
   TCStringList slNode;
   TCMItemAnalyze * pMItemAnalyze;
   GetNodeNames(slNode);
   for( i = 0;i<slNode.GetCount();i++)
   {
      pMItemAnalyze = new TCMItemAnalyze();
      pMItemAnalyze->Load(slNode[i]);
      m_lMItemAnalyze.Add(pMItemAnalyze);
   }
   return m_lMItemAnalyze.GetCount();
}

long TCMonitorConfig::GetCount()
{
  return m_lMItemAnalyze.GetCount();
}

TCMItemAnalyze * TCMonitorConfig::GetByIndex(long nIndex)
{
   if( (nIndex >= m_lMItemAnalyze.GetCount()) || (nIndex < 0) )
       throw TCException("Error TCMonitorConfig::GetByIndex(long nIndex)" );
   return  (TCMItemAnalyze * )m_lMItemAnalyze[nIndex];
}
//------------------------------------------------------------------------------

TCMItemAnalyze::TCMItemAnalyze()
{
    mtRefreshTime = 0;
    m_tmCreate = time(NULL);
    mtReference = 0;
    m_bActive = false;
    m_nNodeID = -1;
    pthread_mutex_init(&m_mut,NULL);
}

TCMItemAnalyze::~TCMItemAnalyze()
{
   Clear();
   pthread_mutex_destroy(&m_mut);
}

void TCMItemAnalyze::mut_lock()
{
  if( pthread_mutex_lock(&m_mut) != 0 )
      throw TCException("Error TCMItemAnalyze::mut_lock()" );
}

void TCMItemAnalyze::mut_unlock()
{
   if( pthread_mutex_unlock(&m_mut) != 0 )
      throw TCException("Error TCMItemAnalyze::mut_unlock()" );
}

//==========================================================================
// 函数 : Load
// 用途 : 从INI配置文件取出读出所有的项目
// 原型 : void TCMItemAnalyze::Load()
// 参数 :
// 返回 :
// 说明 :
//==========================================================================
void TCMItemAnalyze::Load(TCString sNodeName)
{
   long i,nItemNum;
   Clear();
   TSMItemInfo * pMItemInfo;
   TCStringList sItemList,sSubItemList;
   m_sNodeName = sNodeName;
   m_nNodeID = ProfileAppInt(Application.GetAppName(),"NodeInfo",sNodeName,0);
   TCMonitorConfig::GetIncludeList( sItemList,sNodeName );
   for( i = 0 ;i< sItemList.GetCount();i++ )
   {
      TCMonitorConfig::GetItemListByInclude(sItemList[i],sSubItemList);
      for( nItemNum = 0; nItemNum < sSubItemList.GetCount();nItemNum++)
      {
        pMItemInfo = new TSMItemInfo[1];
        try
        {
          TCMonitorConfig::GetItemInfo(sItemList[i],sSubItemList[nItemNum],pMItemInfo);
          pMItemInfo->report_time = 0;
          TCList::Add(pMItemInfo);
        }
        catch (TCException e)
        {
          Clear();
          delete[] pMItemInfo;
          throw e;
        }
      }
   }
   m_tmCreate = time(NULL);
}

//==========================================================================
// 函数 :
// 用途 : 将项目数据串用换行符相连以便发送
// 原型 :
// 参数 :
// 返回 :
// 说明 :
//==========================================================================
long TCMItemAnalyze::FillToBuffer(long& nCount,char *pBuf,long nMaxSize)
{
    long i,j,nFilledCount = 0;
    TCString sFillStr;
    TCStringList sItemList;
    TSMItemInfo * pMItemInfo;
    mut_lock();

    for( i = nCount; i<GetCount(); i++ )
    {
      pMItemInfo = (TSMItemInfo *)m_Pointer[i];
      sItemList.Add((char*)pMItemInfo->sName);              //1
      sItemList.Add((char*)pMItemInfo->sTypeName);          //2
      sItemList.Add((char*)pMItemInfo->sSource);            //3
      sItemList.Add((char*)pMItemInfo->sTime_Section);      //4
      sItemList.Add((char*)pMItemInfo->sConfig);            //5
      sItemList.Add((char*)pMItemInfo->sCommand);           //6
      sItemList.Add((char*)pMItemInfo->Status);             //7
      sItemList.Add((char*)pMItemInfo->Attribute);          //8
      sItemList.Add((char*)pMItemInfo->sInfoField);         //9
      sItemList.Add((char*)pMItemInfo->sInfoData);          //10
      if ( Length(sItemList.GetText()) >= nMaxSize )
      {
         ASSERT( i > nCount );
         long nIdx;
         for(j = 0;j<10;j++)
         {
           nIdx = sItemList.GetCount() - 1;
           if(nIdx <0) break;
           sItemList.Delete(nIdx);
         }
         break;
      }
      nFilledCount++;
   }

   mut_unlock();
   nCount = nCount + nFilledCount;
   if( nFilledCount == 0 )
       return 0;
   sFillStr = sItemList.GetText();
   memcpy(pBuf, (char *)sFillStr,Length(sFillStr)+1);

   return Length(sFillStr)+1;
}

//==========================================================================
// 函数 :
// 用途 :
// 原型 :
// 参数 :
// 返回 :
// 说明 :
//==========================================================================
TSMItemInfo * TCMItemAnalyze::GetItem(long nIndex)
{
   if( (nIndex < 0)||(nIndex >= GetCount()) )
        return NULL;
   else
        return (TSMItemInfo *)m_Pointer[nIndex];
}

//==========================================================================
// 函数 :
// 用途 : 将接收到的项目数据串放入队列
// 原型 :
// 参数 :
// 返回 :
// 说明 :
//==========================================================================
bool TCMItemAnalyze::AddToQueue(char *pBuf)
{
   long i = 0;
   TSMItemInfo * pMItemInfo;
   TCString sSrcStr;
   TCStringList sItemList;
   sSrcStr = pBuf;
   sItemList.SetText(sSrcStr);

   if (sItemList.GetCount()%10 !=0 )
       return false;

   mut_lock();

   while(i < sItemList.GetCount())
   {
      pMItemInfo = new TSMItemInfo[1];
      pMItemInfo->sName = sItemList[i];
      pMItemInfo->sTypeName = sItemList[i+1];
      pMItemInfo->sSource = sItemList[i+2];
      pMItemInfo->sTime_Section = sItemList[i+3];
      pMItemInfo->sConfig = sItemList[i+4];
      pMItemInfo->sCommand = sItemList[i+5];
      pMItemInfo->Status = sItemList[i+6];
      pMItemInfo->Attribute = sItemList[i+7];
      pMItemInfo->sInfoField = sItemList[i+8];
      pMItemInfo->sInfoData = sItemList[i+9];
      pMItemInfo->report_time = 0;
      i = i+10;
      TCList::Add(pMItemInfo);
   }
   sItemList.Clear();
   mut_unlock();

   return true;
}

//==========================================================================
// 函数 :
// 用途 : 填写一个项目的状态数据
// 原型 :
// 参数 :
// 返回 :
// 说明 :一般用于接收到更新项目的消息之后的动作！
//==========================================================================
bool TCMItemAnalyze::FillStatus(long nIndex,char *pBuf)
{
   TSMItemInfo * pMItemInfo;
   TCStringList slInfoStrings;
   slInfoStrings.SetText(pBuf);
   if( slInfoStrings.GetCount() != 3 )   //Statue,InfoField,InfoData
   {   printf("Error in refresh item,receive strings = %s",pBuf);
       return true;
   }
   bool bRet = false;

   mut_lock();
   if( (nIndex >= GetCount() ) || nIndex < 0 )
   {    mut_unlock();
        printf("Error in refresh item,Index = %d",nIndex);
        return true;
   }

   pMItemInfo = GetItem(nIndex);
   if( pMItemInfo->Status == slInfoStrings[0] )
       bRet = true;
   pMItemInfo->Status = slInfoStrings[0];
   pMItemInfo->sInfoField = slInfoStrings[1];
   pMItemInfo->sInfoData = slInfoStrings[2];
   pMItemInfo->report_time = time(NULL);
   mtRefreshTime = pMItemInfo->report_time ;
   slInfoStrings.Clear();
   mut_unlock();
   return bRet;
}
//==========================================================================
// 函数 :
// 用途 : 清除项目队列
// 原型 :
// 参数 :
// 返回 :
// 说明 :
//==========================================================================
void TCMItemAnalyze::Clear()
{
  long i;
  mut_lock();

  TSMItemInfo *pMItemInfo;
  for( i = 0 ;i < GetCount(); i++)
  {
    pMItemInfo = (TSMItemInfo *)(m_Pointer[i]) ;
    delete[] pMItemInfo;
  }
  TCList::Clear();

  mut_unlock();
}


long TCMItemAnalyze::GetWarnLevel(TCString sStatus)
{
  if( Length(sStatus) > 1 )
    return StrToInt(StrGetName(sStatus,','));
  return 0;
}

