//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_time_cost.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)

//==========================================================================
// 函数 : TCTimeCost::TCTimeCost()
// 用途 : 构造函数
// 原型 : TCTimeCost()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
TCTimeCost::TCTimeCost()
{
#ifdef __WIN32__
    QueryPerformanceFrequency(&m_nFreq);
#endif
}

//==========================================================================
// 函数 : TCTimeCost::Start()
// 用途 : 开始计时
// 原型 : void Start()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCTimeCost::Start()
{
#ifdef __WIN32__
    QueryPerformanceCounter(&m_nStart);
#else
     gettimeofday(&m_tmStart, 0);
#endif
}

//==========================================================================
// 函数 : TCTimeCost::Cost()
// 用途 : 计算所耗时间
// 原型 : long Cost()
// 参数 : 无
// 返回 : 耗时（单位为微秒)
// 说明 :
//==========================================================================
long TCTimeCost::Cost()
{
    long nTimeCost;
#ifdef __WIN32__
    double fInterval;
    QueryPerformanceCounter(&m_nEnd);
    fInterval = ((double)m_nEnd.QuadPart - (double)m_nStart.QuadPart)
                / (double)m_nFreq.QuadPart;
    nTimeCost = (long)(fInterval * 1000000);
#else
    gettimeofday(&m_tmEnd, 0);
    nTimeCost = (m_tmEnd.tv_usec - m_tmStart.tv_usec) +
                (m_tmEnd.tv_sec - m_tmStart.tv_sec) * 1000000;
#endif
    return nTimeCost;
}
