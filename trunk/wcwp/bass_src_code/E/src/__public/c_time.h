//---------------------------------------------------------------------------

#ifndef c_timeH
#define c_timeH
//---------------------------------------------------------------------------

#include <time.h>

const long SECONDS_OF_DAY = 60 * 60 * 24;

enum TTimeToStrType
{   tsTime, tsTime_Sec,
    tsEDate, tsCDate,
    tsEDatetime, tsCDatetime,
    tsEDatetime_Sec, tsCDatetime_Sec
};

class TCTime
{
public:
    static TCString GetDateStringByTimeT(time_t t);
    static TCString GetTimeStringByTimeT(time_t t);
    static TCString GetDatetimeStringByTimeT(time_t t);

    static time_t GetTimeTByDatetimeString(TCString dtDatetime);

    static TCString GetDatePart(TCString dtDatetime);
    static TCString GetTimePart(TCString dtDatetime);

    static bool IsValidDatetime(const TCString & dtDatetime);
    static bool IsValidDate(const TCString & dDate);
    static bool IsValidTime(const TCString & tTime);

    static TCString Now();
    static TCString Today();

    static long Year(TCString dtDateTime );
    static long Month(TCString dtDateTime);
    static long Day(TCString dtDateTime);
    static long Hour(TCString dtDateTime);
    static long Minute(TCString dtDateTime);
    static long Second(TCString dtDateTime);

    static TCString EncodeDate(long nYear, long nMonth, long nDay);
    static TCString EncodeDate(long nDateNum);
    static TCString EncodeTime(long nHour, long nMinute, long nSecond);
    static TCString EncodeTime(long nTimeNum);

    static TCString RelativeDate(TCString dtDatetime, long nNumOfDays);
    static TCString RelativeTime(TCString dtDatetime, long nNumOfSeconds);

    static long DaysAfter(TCString dtFrom, TCString dtTo);
    static long SecondsAfter(TCString dtFrom, TCString dtTo);

    static long DayOfWeek(TCString dtDatetime);

    static bool IsHoliday(TCString dtDatetime);
    static long GetTimeSection(TCString dtTimeValue, long nSectionLength,
            long nOffset = 0);

};

#endif
