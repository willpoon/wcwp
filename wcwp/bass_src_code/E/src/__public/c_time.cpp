//---------------------------------------------------------------------------
#pragma hdrstop

#include <time.h>

#include "cmpublic.h"

#include "c_time.h"
//---------------------------------------------------------------------------

//==========================================================================
// 函数 : TCTime::GetDateStringByTimeT
// 用途 : 由time_t类型的时间，得到日期串
// 原型 : static TCString GetDateStringByTimeT(time_t t);
// 参数 : time_t类型的时间
// 返回 : 日期串
// 说明 :
//==========================================================================
TCString TCTime::GetDateStringByTimeT(time_t t)
{
    struct tm *tblock;
    char szDate[9];

    tblock = localtime(&t);

    sprintf(szDate, "%04d%02d%02d", 1900 + tblock->tm_year,
            tblock->tm_mon + 1, tblock->tm_mday);
    ASSERT(Length(szDate) == 8);

    return TCString(szDate);
}

//==========================================================================
// 函数 : TCTime::GetTimeStringByTimeT
// 用途 : 由time_t类型的时间，得到时间串
// 原型 : static TCString GetTimeStringByTimeT(time_t t);
// 参数 : time_t类型的时间
// 返回 : 时间串
// 说明 :
//==========================================================================
TCString TCTime::GetTimeStringByTimeT(time_t t)
{
    struct tm *tblock;
    char szTime[7];

    tblock = localtime(&t);

    sprintf(szTime, "%02d%02d%02d", tblock->tm_hour, tblock->tm_min,
            tblock->tm_sec);
    ASSERT(Length(szTime) == 6);

    return TCString(szTime);
}

//==========================================================================
// 函数 : TCTime::GetDatetimeStringByTimeT
// 用途 : 由time_t类型的时间，得到日期时间串
// 原型 : static TCString GetDatetimeStringByTimeT(time_t t);
// 参数 : time_t类型的时间
// 返回 : 日期时间串
// 说明 :
//==========================================================================
TCString TCTime::GetDatetimeStringByTimeT(time_t t)
{
    return GetDateStringByTimeT(t) + GetTimeStringByTimeT(t);
}

//==========================================================================
// 函数 : TCTime::GetTimeTByDatetimeString
// 用途 : 由日期时间串, 得到time_t类型的时间
// 原型 : static time_t GetTimeTByDatetimeString(TCString dtDatetime);
// 参数 : 日期时间串
// 返回 : time_t类型的时间
// 说明 :
//==========================================================================
time_t TCTime::GetTimeTByDatetimeString(TCString dtDatetime)
{
    struct tm time_check;
    time_t t;

    time_check.tm_year = Year(dtDatetime) - 1900;
    time_check.tm_mon  = Month(dtDatetime) - 1;
    time_check.tm_mday = Day(dtDatetime);
    time_check.tm_hour = Hour(dtDatetime);
    time_check.tm_min  = Minute(dtDatetime);
    time_check.tm_sec  = Second(dtDatetime);
    time_check.tm_isdst = -1;

    t = mktime(&time_check);

    return t;
}

//==========================================================================
// 函数 : TCTime::GetDatePart
// 用途 : 得到时间串的日期部分
// 原型 : static TCString GetDatePart(TCString dtDatetime);
// 参数 : 日期串或日期时间串
// 返回 : 日期串
// 说明 : 如果长度不符合要求，则返回空串；不做日期的有效性检验
//==========================================================================
TCString TCTime::GetDatePart ( TCString dtDatetime )
{
    long nLenDT;

    nLenDT = Length(dtDatetime);

    if ((nLenDT == 14) || (nLenDT == 8))
        return Left(dtDatetime, 8);
    else
        return "";
}

//==========================================================================
// 函数 : TCTime::GetTimePart
// 用途 : 得到时间串的时间部分
// 原型 : static TCString GetTimePart(TCString dtDatetime);
// 参数 : 时间串或日期时间串
// 返回 : 时间串
// 说明 : 如果长度不符合要求，则返回空串；不做时间的有效性检验
//==========================================================================
TCString TCTime::GetTimePart ( TCString dtDatetime )
{
    long nLenDT;

    nLenDT = Length(dtDatetime);

    if ((nLenDT == 14) || (nLenDT == 6))
        return Right(dtDatetime, 6);
    else
        return "";
}

//==========================================================================
// 函数 : TCTime::IsValidDatetime
// 用途 : 判断日期时间串是否合法
// 原型 : static bool IsValidDatetime(TCString dtDatetime);
// 参数 : 日期时间串
// 返回 : 是否合法
// 说明 : 8位日期串，14位日期时间串都为合法
//==========================================================================
bool TCTime::IsValidDatetime(const TCString & dtDatetime)
{
    long nLength;
    nLength = Length(dtDatetime);

    if (nLength == 8)
        return IsValidDate(dtDatetime);

    if (nLength == 14)
    {
        TCString sDate, sTime;
        sDate = Left(dtDatetime, 8);
        sTime = Right(dtDatetime, 6);

        return (IsValidDate(sDate) && IsValidTime(sTime));
    }

    return false;
}

//==========================================================================
// 函数 : TCTime::IsValidDatetime
// 用途 : 判断日期串是否合法
// 原型 : static bool IsValidDatetime(TCString dtDatetime);
// 参数 : 日期串
// 返回 : 是否合法
// 说明 : 该函数没有判断大小月、闰年等
//==========================================================================
bool TCTime::IsValidDate(const TCString & dDate)
{
    long nDate;
    nDate = StrToInt(dDate);

    long nYear, nMonth, nDay;
    nYear = nDate / 10000;
    nMonth = (nDate - nYear * 10000) / 100;
    nDay = nDate - nYear * 10000 - nMonth * 100;

    if (nYear < 1900 || nYear > 2500)
        return false;

    if (nMonth < 1 || nMonth > 12)
        return false;

    if (nDay < 1 || nDay > 31)
        return false;

    return true;
}

//==========================================================================
// 函数 : TCTime::IsValidDatetime
// 用途 : 判断时间串是否合法
// 原型 : static bool IsValidDatetime(TCString dtDatetime);
// 参数 : 时间串
// 返回 : 是否合法
// 说明 : 
//==========================================================================
bool TCTime::IsValidTime(const TCString & tTime)
{
    long nTime;
    nTime = StrToInt(tTime);

    long nHour, nMinute, nSecond;
    nHour = nTime / 10000;
    nMinute = (nTime - nHour * 10000) / 100;
    nSecond = nTime - nHour * 10000 - nMinute * 100;

    if (nHour < 0 || nHour > 23)
        return false;

    if (nMinute < 0 || nMinute > 59)
        return false;

    if (nSecond < 0 || nSecond > 59)
        return false;

    return true;
}

//==========================================================================
// 函数 : TCTime::Now
// 用途 : 得到当前时间的14位字符串表示
// 原型 : static TCString Now();
// 参数 : 无
// 返回 : 日期时间串
// 说明 :
//==========================================================================
TCString TCTime::Now()
{
   time_t timer;

   timer = time(NULL);

   return GetDateStringByTimeT(timer) + GetTimeStringByTimeT(timer);
}

//==========================================================================
// 函数 : TCTime::Today
// 用途 : 得到当前日期的8位字符串表示
// 原型 : static TCString Today();
// 参数 : 无
// 返回 : 日期串
// 说明 :
//==========================================================================
TCString TCTime::Today()
{
   time_t timer;

   timer = time(NULL);

   return GetDateStringByTimeT(timer);
}

//==========================================================================
// 函数 : TCTime::Year
// 用途 : 得到日期串的年份
// 原型 : static long Year(TCString dtDateTime = "");
// 参数 : 日期串
// 返回 : 年份，整型，YYYY
// 说明 : 如果不指定日期串，或日期串为空，则返回当前的年份
//==========================================================================
long TCTime::Year(TCString dtDatetime)
{
    long nYear;

    if (GetDatePart(dtDatetime) == TCString("") )
        dtDatetime = Today();

    nYear = StrToInt(Left(dtDatetime, 4));
    ASSERT(nYear >= 1900 && nYear <= 2500);

    return StrToInt(Left(dtDatetime, 4));
}

//==========================================================================
// 函数 : TCTime::Month
// 用途 : 得到日期串的月份
// 原型 : static long Month(TCString dtDateTime = "");
// 参数 : 日期串
// 返回 : 月份，整型(1-12)
// 说明 : 如果不指定日期串，或日期串为空，则返回当前的月份
//==========================================================================
long TCTime::Month(TCString dtDatetime)
{
    long nMonth;

    if (GetDatePart(dtDatetime) == TCString("") )
        dtDatetime = Today();

    nMonth = StrToInt(Mid(dtDatetime, 5, 2));
    ASSERT(nMonth >= 1 && nMonth <= 12);

    return nMonth;
}

//==========================================================================
// 函数 : TCTime::Day
// 用途 : 得到日期串的天数(Day of Month)
// 原型 : static long Day(TCString dtDateTime = "");
// 参数 : 日期串
// 返回 : 天数，整型，YYYY
// 说明 : 如果不指定日期串，或日期串为空，则返回当前的天数
//==========================================================================
long TCTime::Day(TCString dtDatetime)
{
    long nDay;

    if (GetDatePart(dtDatetime) == TCString("") )
        dtDatetime = Today();

    nDay = StrToInt(Mid(dtDatetime, 7, 2));
    ASSERT(nDay >= 1 && nDay <= 31);

    return nDay;
}

//==========================================================================
// 函数 : TCTime::Hour
// 用途 : 得到小时
// 原型 : static long Hour(TCString dtDateTime);
// 参数 : 日期串
// 返回 : 小时，整型，24小时制(0-23)
// 说明 : 小时数为24也允许
//==========================================================================
long TCTime::Hour(TCString dtDatetime)
{
    ASSERT(IsValidDatetime(dtDatetime));

    long nDTLen;
    long nHour;

    if (GetTimePart(dtDatetime) == TCString("") )
        return 0;

    nDTLen = Length(dtDatetime);

    nHour = StrToInt(Mid(dtDatetime, nDTLen - 5, 2));
    ASSERT(nHour >= 0 && nHour <= 24);

    return nHour;
}

//==========================================================================
// 函数 : TCTime::Minute
// 用途 : 得到分钟
// 原型 : static long Minute(TCString dtDateTime);
// 参数 : 日期串
// 返回 : 分钟，整型, 0-59
// 说明 :
//==========================================================================
long TCTime::Minute(TCString dtDatetime)
{
    ASSERT(IsValidDatetime(dtDatetime));

    long nDTLen;
    long nMinute;

    if (GetTimePart(dtDatetime) == TCString("") )
        return 0;

    nDTLen = Length(dtDatetime);

    nMinute = StrToInt(Mid(dtDatetime, nDTLen - 3, 2));
    ASSERT(nMinute >= 0 && nMinute <= 59);

    return nMinute;
}

//==========================================================================
// 函数 : TCTime::Second
// 用途 : 得到秒数
// 原型 : static long Second(TCString dtDateTime);
// 参数 : 日期串
// 返回 : 秒数，整型, 0-59
// 说明 :
//==========================================================================
long TCTime::Second(TCString dtDatetime)
{
    ASSERT(IsValidDatetime(dtDatetime));

    long nDTLen;
    long nSecond;

    if (GetTimePart(dtDatetime) == TCString("") )
        return 0;

    nDTLen = Length(dtDatetime);

    nSecond = StrToInt(Mid(dtDatetime, nDTLen - 1, 2));
    ASSERT(nSecond >= 0 && nSecond <= 59);

    return nSecond;
}

//==========================================================================
// 函数 : TCTime::EncodeDate
// 用途 : 通过年、月、日得到日期串
// 原型 : static TCString EncodeDate(long nYear, long nMonth, long nDay);
// 参数 : 年，月，日
// 返回 : 日期串
// 说明 :
//==========================================================================
TCString TCTime::EncodeDate(long nYear, long nMonth, long nDay)
{
    char szDate[9];

    sprintf(szDate, "%04d%02d%02d", nYear, nMonth, nDay);
    ASSERT(Length(szDate) == 8);

    return TCString(szDate);
}

//==========================================================================
// 函数 : TCTime::EncodeDate
// 用途 : 日期整型值，得到日期串
// 原型 : static TCString EncodeDate(long nDateNum);
// 参数 : 日期整型值（YYYYMMDD）
// 返回 : 日期串
// 说明 :
//==========================================================================
TCString TCTime::EncodeDate(long nDateNum)
{
    TCString sDate;
    sDate = IntToStr(nDateNum);

    ASSERT(Length(sDate) == 8);

    return sDate;
}

//==========================================================================
// 函数 : TCTime::EncodeTime
// 用途 : 通过时、分、秒得到时间串
// 原型 : static TCString EncodeTime(long nHour, long nMinute, long nSecond);
// 参数 : 时，分，秒
// 返回 : 时间串
// 说明 :
//==========================================================================
TCString TCTime::EncodeTime(long nHour, long nMinute, long nSecond)
{
    char szTime[7];

    sprintf(szTime, "%02d%02d%02d", nHour, nMinute, nSecond);
    ASSERT(Length(szTime) == 6);

    return TCString(szTime);
}

//==========================================================================
// 函数 : TCTime::EncodeTime
// 用途 : 时间整型值，得到时间串
// 原型 : static TCString EncodeTime(long nTimeNum);
// 参数 : 时间整型值(HHMMSS)
// 返回 : 时间串
// 说明 :
//==========================================================================
TCString TCTime::EncodeTime(long nTimeNum)
{
    TCString sTime;
    sTime = IntToStr(nTimeNum);
    sTime = Padl(sTime, 6, '0');

    ASSERT(Length(sTime) == 6);

    return sTime;
}

//==========================================================================
// 函数 : TCTime::RelativeDate
// 用途 : 得到指定天数之后的日期
// 原型 : static TCString RelativeDate(TCString dtDatetime, long nNumOfDays);
// 参数 : 日期，天数
// 返回 : 日期时间串
// 说明 : 返回的日期时间串长度与指定值相同
//==========================================================================
TCString TCTime::RelativeDate(TCString dtDatetime, long nNumOfDays)
{
    time_t t;

    t = GetTimeTByDatetimeString(dtDatetime);

    t = t + SECONDS_OF_DAY * nNumOfDays;

    return GetDateStringByTimeT(t) + GetTimePart(dtDatetime);
}

//==========================================================================
// 函数 : TCTime::RelativeTime
// 用途 : 得到指定秒数之后的日期时间
// 原型 : static TCString RelativeTime(TCString dtDatetime, long nNumOfSeconds);
// 参数 : 日期，秒数
// 返回 : 日期时间串
// 说明 : 输入与返回的时间必须是14位的
//==========================================================================
TCString TCTime::RelativeTime(TCString dtDatetime, long nNumOfSeconds)
{
    ASSERT(Length(dtDatetime) == 14);

    time_t t;

    t = GetTimeTByDatetimeString(dtDatetime);

    t = t + nNumOfSeconds;

    return GetDatetimeStringByTimeT(t);
}

//==========================================================================
// 函数 : TCTime::DaysAfter
// 用途 : 得到两个日期之间的日期差值
// 原型 : static long DaysAfter(TCString dtFrom, TCString dtTo);
// 参数 : 起始日期，终止日期
// 返回 : 天数
// 说明 : 只计算日期，只要是跨越日期的，即使真正时间差值很小，也会有一天之差
//==========================================================================
long TCTime::DaysAfter(TCString dtFrom, TCString dtTo)
{
    long nSeconds;

    nSeconds = difftime(GetTimeTByDatetimeString(GetDatePart(dtTo)),
            GetTimeTByDatetimeString(GetDatePart(dtFrom)));
    return nSeconds / SECONDS_OF_DAY;
}

//==========================================================================
// 函数 : TCTime::SecondsAfter
// 用途 : 得到两个日期之间的秒数差值
// 原型 : static long SecondsAfter(TCString dtFrom, TCString dtTo);
// 参数 : 起始日期时间，终止日期时间
// 返回 : 秒数
// 说明 :
//==========================================================================
long TCTime::SecondsAfter(TCString dtFrom, TCString dtTo)
{
    long nSeconds;

    nSeconds = (long)difftime(GetTimeTByDatetimeString(dtTo),
            GetTimeTByDatetimeString(dtFrom));
    return nSeconds;
}

//==========================================================================
// 函数 : TCTime::DayOfWeek
// 用途 : 得到星期几
// 原型 : static long DayOfWeek(TCString dtDatetime);
// 参数 : 日期串
// 返回 : 星期几(1-7)
// 说明 : 星期一：1  星期日：7
//==========================================================================
long TCTime::DayOfWeek(TCString dtDatetime)
{
    time_t t;
    long nDayOfWeek;
    struct tm *tblock;

    t = GetTimeTByDatetimeString(dtDatetime);

    tblock = localtime(&t);

    nDayOfWeek = tblock->tm_wday;

    if (nDayOfWeek == 0)
        return 7;
    else
        return nDayOfWeek;
}

//==========================================================================
// 函数 : TCTime::IsHoliday
// 用途 : 是否假日
// 原型 : bool IsHoliday(TCString dtDatetime);
// 参数 : 日期串
// 返回 : 是否假日
// 说明 : 1. 判断是否周末（星期六、星期日）
//        2. 判断是否固定的假日(1.1, 5.1, 5.2, 5.3, 10.1, 10.2, 10.3)
//Added on 2000.11.22    3. Y2K正月初一,初二,初三 2001年1月
//==========================================================================
bool TCTime::IsHoliday(TCString dtDatetime)	
{
    long nDayOfWeek;
    nDayOfWeek = DayOfWeek(dtDatetime);
    if (nDayOfWeek == 6 || nDayOfWeek == 7)
        return true;

    long nMonth, nDay;

    nMonth = Month(dtDatetime);
    nDay = Day(dtDatetime);

    switch (nMonth)
    {
        case 1:
            if (nDay == 1)
                return true;
            break;
        case 5:
            if (nDay >= 1 && nDay <= 3)
                return true;
            break;
        case 10:
            if (nDay >= 1 && nDay <= 3)
                return true;
            break;
    }

//%%% Y2K正月初一,初二,初三 2001年2月 1, 2,3
    TCString sDate;
    sDate = GetDatePart(dtDatetime);
    if (sDate == TCString("20020201") ||sDate == TCString("20020202")
                     ||sDate == TCString("20020203") )
        return true;
//%%%%
    return false;
}

//==========================================================================
// 函数 : TCTime::GetTimeSection
// 用途 : 得到指定时间的时间片
// 原型 : long GetTimeSection(TCString dtTimeValue, long nSectionLength
//              long nOffset = 0);
// 参数 : 时间，时间片长度(以分钟计), 时间片偏移量
// 返回 : 时间片数值
// 说明 : 1. 该函数用于生成每隔一定时间生成文件的场合 或 每隔一定时间进行一
//           定的处理;
//        2. 时间片的偏移量使操作不会过分集中在一个时刻进行。
//==========================================================================
long TCTime::GetTimeSection(TCString dtTimeValue, long nSectionLength,
        long nOffset)
{
    ASSERT(nOffset < nSectionLength);

    long nPassedMinutes, nSectionValue;
    nPassedMinutes = TCTime::Hour(dtTimeValue) * 60
            + TCTime::Minute(dtTimeValue);

    nSectionValue = nPassedMinutes - nOffset;
    if (nSectionValue < 0)
        nSectionValue = 24 * 60 + nPassedMinutes - nOffset;

    return  nSectionValue / nSectionLength;
}




