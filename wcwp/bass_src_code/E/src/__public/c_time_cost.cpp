//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_time_cost.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)

//==========================================================================
// ���� : TCTimeCost::TCTimeCost()
// ��; : ���캯��
// ԭ�� : TCTimeCost()
// ���� : ��
// ���� : ��
// ˵�� :
//==========================================================================
TCTimeCost::TCTimeCost()
{
#ifdef __WIN32__
    QueryPerformanceFrequency(&m_nFreq);
#endif
}

//==========================================================================
// ���� : TCTimeCost::Start()
// ��; : ��ʼ��ʱ
// ԭ�� : void Start()
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : TCTimeCost::Cost()
// ��; : ��������ʱ��
// ԭ�� : long Cost()
// ���� : ��
// ���� : ��ʱ����λΪ΢��)
// ˵�� :
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
