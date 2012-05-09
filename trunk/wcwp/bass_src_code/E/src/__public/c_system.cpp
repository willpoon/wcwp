//---------------------------------------------------------------------------

#pragma hdrstop

#include "cmpublic.h"

#ifdef __WIN32__
#include <windows.h>
#else
#include <sys/types.h>
#include <sys/statvfs.h>
#include <errno.h>
#endif

#include "c_system.h"

//---------------------------------------------------------------------------

//==========================================================================
// ���� : TCSystem::DelayMicroSeconds
// ��; : ��ʱһ����ʱ��
// ԭ�� : static void DelayMicroSeconds(long nMicroSeconds);
// ���� : ��ʱ�ĺ�����
// ���� : ��
// ˵�� : �ڸú�����ʵ���У�win32����ֻ�ܾ�ȷ���롣
//==========================================================================
void TCSystem::DelayMicroSeconds(long nMicroSeconds)
{
#ifdef __WIN32__
    Sleep(nMicroSeconds);
//#pragma warn -8053
//    sleep((nMicroSeconds+500)/1000);
//#pragma warn +8053
#else
	struct timeval timeout;
	timeout.tv_sec=nMicroSeconds / 1000;
	timeout.tv_usec=(nMicroSeconds % 1000) * 1000;
	select(0,(fd_set*)0,(fd_set*)0,(fd_set*)0,&timeout);
	return;
#endif
}

//==========================================================================
// ���� : TCSystem::DiskFreeInMegabytes
// ��; : �õ�Ӳ�̿��пռ�
// ԭ�� : static long DiskFreeInMegabytes(TCString sFilePath);
// ���� : ·��
// ���� : ����Ϊ��λ��Ӳ�̿��пռ�
// ˵�� :
//==========================================================================
long TCSystem::DiskFreeInMegabytes(TCString sFilePath)
{
#ifdef __WIN32__
    struct dfree free;
    double fFreeBytes;

    getdfree(0, &free);
    if ( free.df_sclus == -1)
        throw TCException("getdfree() call error in "
                "TCSystem::GetFreeDiskInMegabytes()");

    fFreeBytes = (double) free.df_avail
          * (long) free.df_bsec
          * (long) free.df_sclus;

    return (long)(fFreeBytes / (1024 * 1024));
#else
    struct statvfs fsbuf;
    long    nFreeMegaBytes;
    double  nFreeBytes;  
    
    for (; ;)
    {
        if ( statvfs(sFilePath,&fsbuf) == -1 )
            throw TCException("statvfs() call error in "
                    "TCSystem::GetDiskFreeInMegabytes()");

        nFreeBytes = fsbuf.f_bavail;
        nFreeBytes *= fsbuf.f_frsize;
        nFreeMegaBytes =(long)((nFreeBytes)/(1024*1024));

        if ( nFreeMegaBytes > 0 ) break;
        TCSystem::DelayMicroSeconds(100);
    }

    return nFreeMegaBytes;
#endif
}

//==========================================================================
// ���� : TCSystem::DiskSizeInMegabytes
// ��; : �õ�Ӳ�̿ռ��������
// ԭ�� : static long DiskSizeInMegabytes(TCString sFilePath);
// ���� : ·��
// ���� : ����Ϊ��λ��Ӳ��������
// ˵�� :
//==========================================================================
long TCSystem::DiskSizeInMegabytes(TCString sFilePath)
{
#ifdef __WIN32__
    struct dfree free;
    double fFreeBytes;

    getdfree(0, &free);
    if ( free.df_sclus == -1)
        throw TCException("getdfree() call error in "
                "TCSystem::GetFreeDiskInMegabytes()");

    fFreeBytes = (double) free.df_total
          * (long) free.df_bsec
          * (long) free.df_sclus;

    return (long)(fFreeBytes / (1024 * 1024));
#else
    struct statvfs fsbuf;
    long    nTotalMegaBytes;
    double  nTotalBytes;

    for (; ;)
    {
        if ( statvfs(sFilePath,&fsbuf) == -1 )
            throw TCException("statvfs() call error in "
                    "TCSystem::GetDiskSizeInMegabytes()");

        nTotalBytes = fsbuf.f_blocks;
        nTotalBytes *= fsbuf.f_frsize;
        nTotalMegaBytes =(long)((nTotalBytes)/(1024*1024));

        if ( nTotalMegaBytes > 0 ) break;
        TCSystem::DelayMicroSeconds(100);
    }

    return nTotalMegaBytes;
#endif
}

//==========================================================================
// ���� : TCSystem::DiskFreePercent
// ��; : �õ����д��̵İٷֱ�
// ԭ�� : static long DiskFreePercent(TCString sFilePath);
// ���� : ·��
// ���� : �ٷֱ�(0-100)
// ˵�� : �ú��������ԶԴ��̲����ܼ��͵�Ӧ�ý��м�أ���ʣ��ռ����һ����
//        ֵʱ����ͣ���˳�Ӧ�á�
//==========================================================================
long TCSystem::DiskFreePercent(TCString sFilePaths)
{
    //==== 1. ��ʼ����С���аٷֱ� ========
    long nMinPercent;
    nMinPercent = 999;

    //===== 2. �õ�Ҫ���ӵ�Ӳ�̿ռ�LIST =======
    TCStringList slFilePathList;
    long i;

    slFilePathList.CommaText(sFilePaths);

    //======= 3. ѭ���õ���С��Ӳ�̿ռ���аٷֱ� ======
    long nPercent;
    for (i = 0; i < slFilePathList.GetCount(); i ++)
    {
        nPercent = (long)((100.0 * DiskFreeInMegabytes(slFilePathList[i]))
                / DiskSizeInMegabytes(slFilePathList[i]) + 0.5);
        if (nPercent < nMinPercent)
            nMinPercent = nPercent;
    }

    ASSERT(nMinPercent >= 0 && nMinPercent <= 101);

    return nMinPercent;
}

//==========================================================================
// ���� : TCSystem::ReadStringFromConsole
// ��; : �ӱ�׼������һ���ַ���
// ԭ�� : static TCString ReadStringFromConsole();
// ���� : ��
// ���� : ������ַ���
// ˵�� : ���ص��ַ����о�����ȥ�ո���
//==========================================================================
TCString TCSystem::ReadStringFromConsole()
{
    char szReadString[512];

    fflush(stdin);

    gets(szReadString);

    if (strlen(szReadString) >= 511)
        throw TCException("GetStringFromConsole() : Too Long String Inputted.");

    fflush(stdin);

    return AllTrim(TCString(szReadString));
}

//==========================================================================
// ���� : TCSystem::RegulateMachineOrder
// ��; : ���ݻ������ͣ����������ֽ���
// ԭ�� : void RegulateMachineOrder (void *pWhat, long nLen,
//              TEMachineByteOrder boOrder);
// ���� : ����ָ�룬���ݿ�ȣ�Ҫת�����ֽ���Ҫת�������ֽ���
// ���� : ��
// ˵�� : ת���ת���������ô˺��������ת����ָ��ָ���ֽ����뱾���ֽ���
//        ֮�����ת����
//==========================================================================
void TCSystem::RegulateMachineOrder (void *pWhat, long nLen,
        TEMachineByteOrder boOrder)
{
    if (boOrder == ThisMachineByteOrder())
        return;

    long i;

    char cTmp;
    for (i = 0; i < nLen / 2; i++)
    {
        cTmp = *((char *)pWhat + i);
        *((char *)pWhat + i) = *((char *)pWhat + nLen - i - 1);
        *((char *)pWhat + nLen - i - 1) = cTmp;
    };
}

//==========================================================================
// ���� : TCSystem::ThisMachineByteOrder
// ��; : �õ������������ֽ���
// ԭ�� : static TEMachineByteOrder ThisMachineByteOrder();
// ���� : ��
// ���� : �����ֽ���
// ˵�� :
//==========================================================================
TEMachineByteOrder TCSystem::ThisMachineByteOrder()
{
#ifdef __WIN32__
        return boIntel;
#else
        return boReverseIntel;
#endif
}

//==========================================================================
// ���� : TCSystem::GetLastError
// ��; : �õ����һ��������
// ԭ�� : static int TCSystem::GetLastError();
// ���� : ��
// ���� : ������
// ˵�� :
//==========================================================================
int TCSystem::GetLastError()
{
#ifdef __WIN32__
    return ::GetLastError();
#else
    return errno;
#endif
}

//==========================================================================
// ���� : TCSystem::SysErrorMessage
// ��; : �õ�������Ĵ���������
// ԭ�� : static TCString SysErrorMessage(int nErrorCode);
// ���� : ������
// ���� : ������Ĵ���������
// ˵�� :
//==========================================================================
TCString TCSystem::SysErrorMessage(int nErrorCode)
{
    TCString sResult;

#ifdef __WIN32__
    long nLen;
    char buffer[256];

    nLen = FormatMessage
            (FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_ARGUMENT_ARRAY,
            NULL, nErrorCode, 0, buffer, sizeof(buffer), NULL);

    char cChar;
    while (nLen > 0)
    {
        cChar = buffer[nLen - 1];
        if (!((cChar >= '\0' && cChar <= '\x20') || cChar == '.'))
            break;
        nLen --;
    }

    sResult = TCString(buffer, nLen);
#else
    sResult = strerror(nErrorCode);
#endif

    return sResult;
}

//==========================================================================
// ���� : TCSystem::RunCommand
// ��; : ����һ��������
// ԭ�� : bool RunCommand(TCString sCommand);
// ���� : ������
// ���� : �����Ƿ�ɹ�
// ˵�� :
//==========================================================================
bool TCSystem::RunCommand(TCString sCommand)
{
    ASSERT(sCommand != TCString("") ) ;
    
    if (system((char *)sCommand) == 0)
        return true;
    else
        return false;
}

void DisplayMemoryDump(char * pMemoryDump, long nSize)
{
    long i;
    
    for (i = 0; i < nSize; i++)
        printf("%02X ", *(pMemoryDump + i));
    printf("\n");
} 

//==========================================================================
// ���� : RandLong
// ��; : ����������������ص�������1��nRange֮����������
// ԭ�� : long RandLong(long nRange);
// ���� : ��������ֵ
// ���� : ���ص������
// ˵�� : �ڸú�����������������ʼ�����롣�����ڵ��øú���֮ǰû�б�Ҫ
//        ����randomize()��srand()���г�ʼ����
//==========================================================================
long RandLong(long nRange)
{
    if (nRange <= 0 || nRange >= (long)32767 * (long)32767)
        throw TCException("Randlong() : Value Exceed Range - "
                + IntToStr(nRange));

    if (nRange <= 32767)
        return Random(nRange) + 1;

    long nStep, nRet;

    nStep = nRange / 32767 + 1;

    while (true)
    {
        nRet = Random(nStep) * 32767 + Random(32767) + 1;

        if (nRet > 0 && nRet <=nRange)
            return nRet;
    }
}

int  Random(int num)
{
    ASSERT(num > 0 && num <= 32767);

    static bool s_bFirstTime = true;

    if (s_bFirstTime)
    {
        s_bFirstTime = false;

        time_t t;
        srand((unsigned)time(&t));
    }

    long nHighest;
    long nCount = 0;
    int  nRet;

    nHighest = 32767 - 32767 % num - 1;

    while (true)
    {
        nCount ++;

        if (nCount > 40000)
            throw TCException("Random() : Too many loops.  "
                    "Something must be wrong.");

        nRet = rand();
        if (nRet > nHighest)
            continue;

        return (nRet % num);
    }
}

TCString RandString(long nLength)
{
    ASSERT(nLength >= 0);

    TCString sStr;
    char c;
    long i;
    long nRand;

    for (i = 0; i < nLength; i++)
    {
        nRand = RandLong(62);

        if (nRand >= 1 && nRand <= 10)
            c = '0' + nRand - 1;
        else if (nRand >= 11 && nRand <= 11 + 26)
            c = 'a' + nRand - 11;
        else if (nRand >= 11 + 26 + 1 && nRand <= 11 + 26 + 26)
            c = 'A' + nRand - (11 + 26 + 1);
        else
            throw TCException("RandString() : Something Must be wrong.");

        sStr += c;
    }

    return sStr;
}



