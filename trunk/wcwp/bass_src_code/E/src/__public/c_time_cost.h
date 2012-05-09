//---------------------------------------------------------------------------

#ifndef c_time_costH
#define c_time_costH
//---------------------------------------------------------------------------
#ifdef __WIN32__
#include <windows.h>
#include <winbase.h>
#else
#include <sys/time.h>
#endif
//---------------------------------------------------------------------------
// 类 　: 计时类
// 用途 : 用于程序执行过程中的精确计时
//---------------------------------------------------------------------------
class TCTimeCost
{
private:
#ifdef __WIN32__
    LARGE_INTEGER m_nFreq;
    LARGE_INTEGER m_nStart;
    LARGE_INTEGER m_nEnd;
#else
    struct timeval m_tmStart;
    struct timeval m_tmEnd;
#endif

public:
    TCTimeCost();
    void Start();
    long Cost();
};
#endif
 