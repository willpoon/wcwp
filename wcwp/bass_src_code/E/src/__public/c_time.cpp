//---------------------------------------------------------------------------
#pragma hdrstop

#include <time.h>

#include "cmpublic.h"

#include "c_time.h"
//---------------------------------------------------------------------------

//==========================================================================
// ���� : TCTime::GetDateStringByTimeT
// ��; : ��time_t���͵�ʱ�䣬�õ����ڴ�
// ԭ�� : static TCString GetDateStringByTimeT(time_t t);
// ���� : time_t���͵�ʱ��
// ���� : ���ڴ�
// ˵�� :
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
// ���� : TCTime::GetTimeStringByTimeT
// ��; : ��time_t���͵�ʱ�䣬�õ�ʱ�䴮
// ԭ�� : static TCString GetTimeStringByTimeT(time_t t);
// ���� : time_t���͵�ʱ��
// ���� : ʱ�䴮
// ˵�� :
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
// ���� : TCTime::GetDatetimeStringByTimeT
// ��; : ��time_t���͵�ʱ�䣬�õ�����ʱ�䴮
// ԭ�� : static TCString GetDatetimeStringByTimeT(time_t t);
// ���� : time_t���͵�ʱ��
// ���� : ����ʱ�䴮
// ˵�� :
//==========================================================================
TCString TCTime::GetDatetimeStringByTimeT(time_t t)
{
    return GetDateStringByTimeT(t) + GetTimeStringByTimeT(t);
}

//==========================================================================
// ���� : TCTime::GetTimeTByDatetimeString
// ��; : ������ʱ�䴮, �õ�time_t���͵�ʱ��
// ԭ�� : static time_t GetTimeTByDatetimeString(TCString dtDatetime);
// ���� : ����ʱ�䴮
// ���� : time_t���͵�ʱ��
// ˵�� :
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
// ���� : TCTime::GetDatePart
// ��; : �õ�ʱ�䴮�����ڲ���
// ԭ�� : static TCString GetDatePart(TCString dtDatetime);
// ���� : ���ڴ�������ʱ�䴮
// ���� : ���ڴ�
// ˵�� : ������Ȳ�����Ҫ���򷵻ؿմ����������ڵ���Ч�Լ���
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
// ���� : TCTime::GetTimePart
// ��; : �õ�ʱ�䴮��ʱ�䲿��
// ԭ�� : static TCString GetTimePart(TCString dtDatetime);
// ���� : ʱ�䴮������ʱ�䴮
// ���� : ʱ�䴮
// ˵�� : ������Ȳ�����Ҫ���򷵻ؿմ�������ʱ�����Ч�Լ���
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
// ���� : TCTime::IsValidDatetime
// ��; : �ж�����ʱ�䴮�Ƿ�Ϸ�
// ԭ�� : static bool IsValidDatetime(TCString dtDatetime);
// ���� : ����ʱ�䴮
// ���� : �Ƿ�Ϸ�
// ˵�� : 8λ���ڴ���14λ����ʱ�䴮��Ϊ�Ϸ�
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
// ���� : TCTime::IsValidDatetime
// ��; : �ж����ڴ��Ƿ�Ϸ�
// ԭ�� : static bool IsValidDatetime(TCString dtDatetime);
// ���� : ���ڴ�
// ���� : �Ƿ�Ϸ�
// ˵�� : �ú���û���жϴ�С�¡������
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
// ���� : TCTime::IsValidDatetime
// ��; : �ж�ʱ�䴮�Ƿ�Ϸ�
// ԭ�� : static bool IsValidDatetime(TCString dtDatetime);
// ���� : ʱ�䴮
// ���� : �Ƿ�Ϸ�
// ˵�� : 
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
// ���� : TCTime::Now
// ��; : �õ���ǰʱ���14λ�ַ�����ʾ
// ԭ�� : static TCString Now();
// ���� : ��
// ���� : ����ʱ�䴮
// ˵�� :
//==========================================================================
TCString TCTime::Now()
{
   time_t timer;

   timer = time(NULL);

   return GetDateStringByTimeT(timer) + GetTimeStringByTimeT(timer);
}

//==========================================================================
// ���� : TCTime::Today
// ��; : �õ���ǰ���ڵ�8λ�ַ�����ʾ
// ԭ�� : static TCString Today();
// ���� : ��
// ���� : ���ڴ�
// ˵�� :
//==========================================================================
TCString TCTime::Today()
{
   time_t timer;

   timer = time(NULL);

   return GetDateStringByTimeT(timer);
}

//==========================================================================
// ���� : TCTime::Year
// ��; : �õ����ڴ������
// ԭ�� : static long Year(TCString dtDateTime = "");
// ���� : ���ڴ�
// ���� : ��ݣ����ͣ�YYYY
// ˵�� : �����ָ�����ڴ��������ڴ�Ϊ�գ��򷵻ص�ǰ�����
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
// ���� : TCTime::Month
// ��; : �õ����ڴ����·�
// ԭ�� : static long Month(TCString dtDateTime = "");
// ���� : ���ڴ�
// ���� : �·ݣ�����(1-12)
// ˵�� : �����ָ�����ڴ��������ڴ�Ϊ�գ��򷵻ص�ǰ���·�
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
// ���� : TCTime::Day
// ��; : �õ����ڴ�������(Day of Month)
// ԭ�� : static long Day(TCString dtDateTime = "");
// ���� : ���ڴ�
// ���� : ���������ͣ�YYYY
// ˵�� : �����ָ�����ڴ��������ڴ�Ϊ�գ��򷵻ص�ǰ������
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
// ���� : TCTime::Hour
// ��; : �õ�Сʱ
// ԭ�� : static long Hour(TCString dtDateTime);
// ���� : ���ڴ�
// ���� : Сʱ�����ͣ�24Сʱ��(0-23)
// ˵�� : Сʱ��Ϊ24Ҳ����
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
// ���� : TCTime::Minute
// ��; : �õ�����
// ԭ�� : static long Minute(TCString dtDateTime);
// ���� : ���ڴ�
// ���� : ���ӣ�����, 0-59
// ˵�� :
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
// ���� : TCTime::Second
// ��; : �õ�����
// ԭ�� : static long Second(TCString dtDateTime);
// ���� : ���ڴ�
// ���� : ����������, 0-59
// ˵�� :
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
// ���� : TCTime::EncodeDate
// ��; : ͨ���ꡢ�¡��յõ����ڴ�
// ԭ�� : static TCString EncodeDate(long nYear, long nMonth, long nDay);
// ���� : �꣬�£���
// ���� : ���ڴ�
// ˵�� :
//==========================================================================
TCString TCTime::EncodeDate(long nYear, long nMonth, long nDay)
{
    char szDate[9];

    sprintf(szDate, "%04d%02d%02d", nYear, nMonth, nDay);
    ASSERT(Length(szDate) == 8);

    return TCString(szDate);
}

//==========================================================================
// ���� : TCTime::EncodeDate
// ��; : ��������ֵ���õ����ڴ�
// ԭ�� : static TCString EncodeDate(long nDateNum);
// ���� : ��������ֵ��YYYYMMDD��
// ���� : ���ڴ�
// ˵�� :
//==========================================================================
TCString TCTime::EncodeDate(long nDateNum)
{
    TCString sDate;
    sDate = IntToStr(nDateNum);

    ASSERT(Length(sDate) == 8);

    return sDate;
}

//==========================================================================
// ���� : TCTime::EncodeTime
// ��; : ͨ��ʱ���֡���õ�ʱ�䴮
// ԭ�� : static TCString EncodeTime(long nHour, long nMinute, long nSecond);
// ���� : ʱ���֣���
// ���� : ʱ�䴮
// ˵�� :
//==========================================================================
TCString TCTime::EncodeTime(long nHour, long nMinute, long nSecond)
{
    char szTime[7];

    sprintf(szTime, "%02d%02d%02d", nHour, nMinute, nSecond);
    ASSERT(Length(szTime) == 6);

    return TCString(szTime);
}

//==========================================================================
// ���� : TCTime::EncodeTime
// ��; : ʱ������ֵ���õ�ʱ�䴮
// ԭ�� : static TCString EncodeTime(long nTimeNum);
// ���� : ʱ������ֵ(HHMMSS)
// ���� : ʱ�䴮
// ˵�� :
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
// ���� : TCTime::RelativeDate
// ��; : �õ�ָ������֮�������
// ԭ�� : static TCString RelativeDate(TCString dtDatetime, long nNumOfDays);
// ���� : ���ڣ�����
// ���� : ����ʱ�䴮
// ˵�� : ���ص�����ʱ�䴮������ָ��ֵ��ͬ
//==========================================================================
TCString TCTime::RelativeDate(TCString dtDatetime, long nNumOfDays)
{
    time_t t;

    t = GetTimeTByDatetimeString(dtDatetime);

    t = t + SECONDS_OF_DAY * nNumOfDays;

    return GetDateStringByTimeT(t) + GetTimePart(dtDatetime);
}

//==========================================================================
// ���� : TCTime::RelativeTime
// ��; : �õ�ָ������֮�������ʱ��
// ԭ�� : static TCString RelativeTime(TCString dtDatetime, long nNumOfSeconds);
// ���� : ���ڣ�����
// ���� : ����ʱ�䴮
// ˵�� : �����뷵�ص�ʱ�������14λ��
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
// ���� : TCTime::DaysAfter
// ��; : �õ���������֮������ڲ�ֵ
// ԭ�� : static long DaysAfter(TCString dtFrom, TCString dtTo);
// ���� : ��ʼ���ڣ���ֹ����
// ���� : ����
// ˵�� : ֻ�������ڣ�ֻҪ�ǿ�Խ���ڵģ���ʹ����ʱ���ֵ��С��Ҳ����һ��֮��
//==========================================================================
long TCTime::DaysAfter(TCString dtFrom, TCString dtTo)
{
    long nSeconds;

    nSeconds = difftime(GetTimeTByDatetimeString(GetDatePart(dtTo)),
            GetTimeTByDatetimeString(GetDatePart(dtFrom)));
    return nSeconds / SECONDS_OF_DAY;
}

//==========================================================================
// ���� : TCTime::SecondsAfter
// ��; : �õ���������֮���������ֵ
// ԭ�� : static long SecondsAfter(TCString dtFrom, TCString dtTo);
// ���� : ��ʼ����ʱ�䣬��ֹ����ʱ��
// ���� : ����
// ˵�� :
//==========================================================================
long TCTime::SecondsAfter(TCString dtFrom, TCString dtTo)
{
    long nSeconds;

    nSeconds = (long)difftime(GetTimeTByDatetimeString(dtTo),
            GetTimeTByDatetimeString(dtFrom));
    return nSeconds;
}

//==========================================================================
// ���� : TCTime::DayOfWeek
// ��; : �õ����ڼ�
// ԭ�� : static long DayOfWeek(TCString dtDatetime);
// ���� : ���ڴ�
// ���� : ���ڼ�(1-7)
// ˵�� : ����һ��1  �����գ�7
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
// ���� : TCTime::IsHoliday
// ��; : �Ƿ����
// ԭ�� : bool IsHoliday(TCString dtDatetime);
// ���� : ���ڴ�
// ���� : �Ƿ����
// ˵�� : 1. �ж��Ƿ���ĩ���������������գ�
//        2. �ж��Ƿ�̶��ļ���(1.1, 5.1, 5.2, 5.3, 10.1, 10.2, 10.3)
//Added on 2000.11.22    3. Y2K���³�һ,����,���� 2001��1��
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

//%%% Y2K���³�һ,����,���� 2001��2�� 1, 2,3
    TCString sDate;
    sDate = GetDatePart(dtDatetime);
    if (sDate == TCString("20020201") ||sDate == TCString("20020202")
                     ||sDate == TCString("20020203") )
        return true;
//%%%%
    return false;
}

//==========================================================================
// ���� : TCTime::GetTimeSection
// ��; : �õ�ָ��ʱ���ʱ��Ƭ
// ԭ�� : long GetTimeSection(TCString dtTimeValue, long nSectionLength
//              long nOffset = 0);
// ���� : ʱ�䣬ʱ��Ƭ����(�Է��Ӽ�), ʱ��Ƭƫ����
// ���� : ʱ��Ƭ��ֵ
// ˵�� : 1. �ú�����������ÿ��һ��ʱ�������ļ��ĳ��� �� ÿ��һ��ʱ�����һ
//           ���Ĵ���;
//        2. ʱ��Ƭ��ƫ����ʹ����������ּ�����һ��ʱ�̽��С�
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




