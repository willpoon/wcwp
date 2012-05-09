//---------------------------------------------------------------------------
#pragma hdrstop

#include "cmpublic.h"
#include "c_base_func.h"
//---------------------------------------------------------------------------

//==========================================================================
// 函数 : RelativeMonth
// 用途 : 根据参数转换月份和月份相对值返回结果月份
// 原型 : RelativeMonth()
// 参数 : sMonth参数转换月份，lAdustValue月份相对值
// 返回 : TCString结果月份
// 说明 :
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
// 函数 : RelativeDay
// 用途 : 根据参数转换日期和日期相对值返回结果日期
// 原型 : RelativeMonth()
// 参数 : sDay参数转换日期，lAdustValue日期相对值
// 返回 : TCString结果日期
// 说明 :
//==========================================================================
TCString RelativeDay(TCString sDay,long lAdustValue)
{
  long nAdjustSeconds = lAdustValue * 24 * 60 * 60;    //计算出偏差的秒数
  TCString sTmpDate = Mid(TCTime::RelativeTime(sDay + "000000",nAdjustSeconds),1,8);    //格式YYYYMMDD

  return sTmpDate;
}

//==========================================================================
// 函数 : GetNextValue
// 用途 : 根据不同的类型得到对原始值变化指定的相对值后的结果值
// 原型 : bool GetNextValue()
// 参数 : TCString sStyle,TCString sValue,long lAdustValue
// 返回 : 结果值
// 说明 : sStyle（NUM,MON,DAY三种类型中之一）sValue原始值，lAdustValue相对值
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