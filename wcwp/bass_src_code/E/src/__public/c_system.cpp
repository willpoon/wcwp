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
// 函数 : TCSystem::DelayMicroSeconds
// 用途 : 延时一定的时长
// 原型 : static void DelayMicroSeconds(long nMicroSeconds);
// 参数 : 延时的毫秒数
// 返回 : 无
// 说明 : 在该函数的实现中，win32环境只能精确到秒。
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
// 函数 : TCSystem::DiskFreeInMegabytes
// 用途 : 得到硬盘空闲空间
// 原型 : static long DiskFreeInMegabytes(TCString sFilePath);
// 参数 : 路径
// 返回 : 以兆为单位的硬盘空闲空间
// 说明 :
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
// 函数 : TCSystem::DiskSizeInMegabytes
// 用途 : 得到硬盘空间的总容量
// 原型 : static long DiskSizeInMegabytes(TCString sFilePath);
// 参数 : 路径
// 返回 : 以兆为单位的硬盘总容量
// 说明 :
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
// 函数 : TCSystem::DiskFreePercent
// 用途 : 得到空闲磁盘的百分比
// 原型 : static long DiskFreePercent(TCString sFilePath);
// 参数 : 路径
// 返回 : 百分比(0-100)
// 说明 : 该函数可用以对磁盘操作密集型的应用进行监控，当剩余空间低于一定的
//        值时，暂停或退出应用。
//==========================================================================
long TCSystem::DiskFreePercent(TCString sFilePaths)
{
    //==== 1. 初始化最小空闲百分比 ========
    long nMinPercent;
    nMinPercent = 999;

    //===== 2. 得到要监视的硬盘空间LIST =======
    TCStringList slFilePathList;
    long i;

    slFilePathList.CommaText(sFilePaths);

    //======= 3. 循环得到较小的硬盘空间空闲百分比 ======
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
// 函数 : TCSystem::ReadStringFromConsole
// 用途 : 从标准输入获得一个字符串
// 原型 : static TCString ReadStringFromConsole();
// 参数 : 无
// 返回 : 输入的字符串
// 说明 : 返回的字符串中经过了去空格处理
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
// 函数 : TCSystem::RegulateMachineOrder
// 用途 : 根据机器类型，重整数字字节序
// 原型 : void RegulateMachineOrder (void *pWhat, long nLen,
//              TEMachineByteOrder boOrder);
// 参数 : 数字指针，数据宽度，要转换的字节序／要转换到的字节序
// 返回 : 无
// 说明 : 转入和转出都可以用此函数解决。转换是指在指定字节序与本机字节序
//        之间进行转换。
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
// 函数 : TCSystem::ThisMachineByteOrder
// 用途 : 得到本机的数字字节序
// 原型 : static TEMachineByteOrder ThisMachineByteOrder();
// 参数 : 无
// 返回 : 本机字节序
// 说明 :
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
// 函数 : TCSystem::GetLastError
// 用途 : 得到最后一个错误码
// 原型 : static int TCSystem::GetLastError();
// 参数 : 无
// 返回 : 错误码
// 说明 :
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
// 函数 : TCSystem::SysErrorMessage
// 用途 : 得到错误码的错误描述串
// 原型 : static TCString SysErrorMessage(int nErrorCode);
// 参数 : 错误码
// 返回 : 错误码的错误描述串
// 说明 :
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
// 函数 : TCSystem::RunCommand
// 用途 : 运行一个命令行
// 原型 : bool RunCommand(TCString sCommand);
// 参数 : 命令行
// 返回 : 运行是否成功
// 说明 :
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
// 函数 : RandLong
// 用途 : 随机数发生器。返回的数是在1和nRange之间的随机数。
// 原型 : long RandLong(long nRange);
// 参数 : 随机数最大值
// 返回 : 返回的随机数
// 说明 : 在该函数中有随机数随机初始化代码。所以在调用该函数之前没有必要
//        调用randomize()或srand()进行初始化。
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



