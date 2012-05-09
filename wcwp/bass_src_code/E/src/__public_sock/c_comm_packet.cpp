//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_comm_packet.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)
TCCommPacket::TCCommPacket()
{
   m_pPacketHeader = (TSPacketHeader *)m_SBuffer;
   m_pEventInfo = (TSEventInfo *)(m_SBuffer+sizeof(TSPacketHeader));
   m_pSBuf = m_SBuffer + sizeof(TSPacketHeader) + sizeof(TSEventInfo);
   m_nDataLen = 0;
   m_nNodeID = 0;
   m_DestNodeID = 0;
}

TCCommPacket::~TCCommPacket()
{

}

long TCCommPacket::GetMaxFillSize()
{
  return (SOCK_MAX_MSG_SIZE - sizeof(TSPacketHeader) - sizeof(TSEventInfo) - 3);
}

bool TCCommPacket::CheckPacketHeader()
{
   unsigned long PacketType;
   long PacketSize;
   bool bRet = false;
   PacketType = CharArrayToInt(m_pPacketHeader->PacketType);
   PacketSize = CharArrayToInt(m_pPacketHeader->PacketSize);
   m_snPacketInfo.PacketType = PacketType;
   m_snPacketInfo.PacketSize = PacketSize;
   m_nDataLen = PacketSize - sizeof(TSEventInfo);
   if( (m_pPacketHeader->Signature[0] == char(0x0e))&&
       (m_pPacketHeader->Signature[1] == char(0xf0)) )
     switch(PacketType)
     {
       case DS_RegisterAsk:
           if ( PacketSize ==  (long)sizeof(TSEventInfo) )
               bRet = true;
            break;
       case DS_Register:
       case DS_Monitor:
       case DS_MonitorAsk:
       case DS_SysControl:
       case DS_SysControlAsk:
       case DS_NotifyEvent :
       case DS_NotifyEventAsk:
       case DS_ItemInfo:
       case DS_ItemInfoAsk:
            if( (PacketSize >= (long)sizeof(TSEventInfo))&&
                (PacketSize < (long)(SOCK_MAX_MSG_SIZE-sizeof(TSPacketHeader))) )
                bRet = true;
            break;
       case DS_Heartbeat:
            if( PacketSize == 0)
                bRet = true;
            break;
     default:
            bRet = false;
   }
   if (bRet == false)
       Terminate();
   return bRet;
}


void TCCommPacket::SendHeartbeat()
{
   if ( !Terminated )
   {
        FillPacketHeader(DS_Heartbeat,0,false);
        Write(m_pPacketHeader,sizeof(TSPacketHeader));
   }
}

void TCCommPacket::FillPacketHeader(long PacketType,long nDataSize,
                                          bool bIncludeEventPacketSize)
{
   m_nDataLen = nDataSize;
   m_snPacketInfo.PacketType = PacketType;
   m_pPacketHeader->Signature[0] = char(0x0e);
   m_pPacketHeader->Signature[1] = char(0xf0);
   IntToCharArray(PacketType,m_pPacketHeader->PacketType);
   if( bIncludeEventPacketSize )
      nDataSize = sizeof(TSEventInfo) + m_nDataLen ;
   m_snPacketInfo.PacketSize = nDataSize;
   IntToCharArray(nDataSize,m_pPacketHeader->PacketSize);
}

void TCCommPacket::FillEventInfo(long EventCode,long Order,
                                 long nSrcNodeID,long nDestNodeID)
{
  m_snPacketInfo.SrcNodeID = nSrcNodeID;
  IntToCharArray(nSrcNodeID,m_pEventInfo->SrcNodeID);
  m_snPacketInfo.DestNodeID = nDestNodeID;
  IntToCharArray(m_snPacketInfo.DestNodeID,m_pEventInfo->DestNodeID);
  m_snPacketInfo.EventCode = EventCode;
  IntToCharArray(EventCode,m_pEventInfo->EventCode);
  m_snPacketInfo.Order = Order;
  IntToCharArray(Order,m_pEventInfo->Order);
}

void TCCommPacket::FillRegisterInfo()    //
{


}

void TCCommPacket::RegisterToCenter()
{
  FillRegisterInfo();
  SendEvent();
  if ( Terminated )
      return ;
  if( !Read(m_pPacketHeader,sizeof(TSPacketHeader)) )
      return;
  if( !CheckPacketHeader() )
      return;
  if( m_snPacketInfo.PacketType != DS_RegisterAsk )
  {
     Terminate();
     return;
  }
  if( !Read(m_pEventInfo,m_snPacketInfo.PacketSize+1) )
     return;
  if ( !CheckTailFlag() )
     return;
  if( m_snPacketInfo.Order != 0 )
  {
     Terminate();
     return;
  }
  m_nNodeID = m_snPacketInfo.EventCode ;
}


void TCCommPacket::DoRegister()           //
{
  if( !Read(m_pPacketHeader,sizeof(TSPacketHeader)) )
     return;
  if( !CheckPacketHeader() )
     return;
  if( m_snPacketInfo.PacketType != DS_Register )
  {
     Terminate();
     return;
  }
  if( !Read(m_pEventInfo,m_snPacketInfo.PacketSize+1) )
     return;

  CheckTailFlag();
}

bool TCCommPacket::CheckTailFlag()
{
  m_nDataLen = m_snPacketInfo.PacketSize - sizeof(TSEventInfo);
  m_snPacketInfo.DestNodeID = CharArrayToInt(m_pEventInfo->DestNodeID);
  m_snPacketInfo.SrcNodeID = CharArrayToInt(m_pEventInfo->SrcNodeID);
  m_snPacketInfo.EventCode = CharArrayToInt(m_pEventInfo->EventCode);
  m_snPacketInfo.Order = CharArrayToInt(m_pEventInfo->Order);
  if (  m_pSBuf[m_nDataLen] != SOCK_TAIL_FLAG )
  {
      Terminate();
      return false;
  }
  return true;
}

void TCCommPacket::ReceiveEvent()
{
  if( !Read(m_pPacketHeader,sizeof(TSPacketHeader)) )
     return;
  if( !CheckPacketHeader() )
     return;
  if( m_snPacketInfo.PacketType == DS_Heartbeat )
     return;
  if( !Read(m_pEventInfo,m_snPacketInfo.PacketSize + 1) )
     return;
  CheckTailFlag();
}

void TCCommPacket::SendEvent()
{
  m_pSBuf[m_nDataLen] = SOCK_TAIL_FLAG;
  Write(m_pPacketHeader,sizeof(TSPacketHeader) +
                              sizeof(TSEventInfo) + m_nDataLen  + 1);
  m_nDataLen = 0;
}


