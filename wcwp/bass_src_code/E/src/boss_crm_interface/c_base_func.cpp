//---------------------------------------------------------------------------
#pragma hdrstop

#include "cmpublic.h"
#include "c_base_func.h"
//---------------------------------------------------------------------------

//==========================================================================
// ���� : RelativeMonth
// ��; : ���ݲ���ת���·ݺ��·����ֵ���ؽ���·�
// ԭ�� : RelativeMonth()
// ���� : sMonth����ת���·ݣ�lAdustValue�·����ֵ
// ���� : TCString����·�
// ˵�� :
//==========================================================================
TCString RelativeMonth(TCString sMonth,long lAdustValue)
{
    long lDate,lYear,lMonth,lYearAdd;

    lDate=StrToInt(sMonth);

    lYear=lDate/100;
    lMonth=lDate%100;
    
    lYearAdd = (lMonth+lAdustValue)<= 0 ? ((lMonth+lAdustValue-12)/12):((lMonth + lAdustValue - 1)/12);
    lMonth =   ((( lMonth + lAdustValue )%12) + 12 ) % 12;
    if (lMonth==0)
        lMonth=12;

    lDate = (lYear + lYearAdd)*100 + lMonth;
    return IntToStr(lDate);
}

//==========================================================================
// ���� : RelativeDay
// ��; : ���ݲ���ת�����ں��������ֵ���ؽ������
// ԭ�� : RelativeMonth()
// ���� : sDay����ת�����ڣ�lAdustValue�������ֵ
// ���� : TCString�������
// ˵�� :
//==========================================================================
TCString RelativeDay(TCString sDay,long lAdustValue)
{
  long nAdjustSeconds = lAdustValue * 24 * 60 * 60;    //�����ƫ�������
  TCString sTmpDate = Mid(TCTime::RelativeTime(sDay + "000000",nAdjustSeconds),1,8);    //��ʽYYYYMMDD

  return sTmpDate;
}

//==========================================================================
// ���� : GetNextValue
// ��; : ���ݲ�ͬ�����͵õ���ԭʼֵ�仯ָ�������ֵ��Ľ��ֵ
// ԭ�� : bool GetNextValue()
// ���� : TCString sStyle,TCString sValue,long lAdustValue
// ���� : ���ֵ
// ˵�� : sStyle��NUM,MON,DAY����������֮һ��sValueԭʼֵ��lAdustValue���ֵ
//==========================================================================
TCString GetNextValue(TCString sStyle,TCString sValue,long lAdustValue)
{
    if (sStyle==TCString("NUM") )
        return IntToStr(StrToInt(sValue)+lAdustValue);
    else if (sStyle==TCString("MON") )
        return RelativeMonth(sValue,lAdustValue);
    else if (sStyle==TCString("DAY") )
        return RelativeDay(sValue,lAdustValue);
    else
        throw TCException("TCThreadRun::GetNextValue() Style:"+sStyle+" is invalid!");

}