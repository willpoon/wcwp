//---------------------------------------------------------------------------

#ifndef c_systemH
#define c_systemH
//---------------------------------------------------------------------------

enum TEMachineByteOrder { boIntel, boReverseIntel };

class TCSystem
{
public:
    static void DelayMicroSeconds(long nMicroSeconds);

    static long DiskFreeInMegabytes(TCString sFilePath);
    static long DiskSizeInMegabytes(TCString sFilePath);
    static long DiskFreePercent(TCString sFilePaths);

    static TCString ReadStringFromConsole();

    static void RegulateMachineOrder (void *pWhat, long nLen,
            TEMachineByteOrder boOrder);
    static TEMachineByteOrder ThisMachineByteOrder();

    static TCString SysErrorMessage(int nErrorCode);
    static int GetLastError();

    static bool RunCommand(TCString sCommand);
};

void        DisplayMemoryDump(char * pMemoryDump, long nSize);
long        RandLong(long nRange);
int         Random(int num);
TCString    RandString(long nLength);

//注：由于不同系统字节序不同的问题，所有的数值都经由下面函数做转换
inline void IntToCharArray(long nSize,char *pSize);
inline void ShortToCharArray(short nSize,char *pSize);
inline long CharArrayToInt(char *pSize);
inline short CharArrayToShort(char *pSize);

inline void IntToCharArray(long nSize,char *pSize)
{
     unsigned char i,ch;
     for(i=sizeof(long);i>0;i--)
     {
        ch = nSize%256;
        pSize[i-1] = ch;
        nSize >>= 8;
     }
}

inline void ShortToCharArray(long nSize,char *pSize)
{
     unsigned char i,ch;
     for(i=sizeof(short);i>0;i--)
     {
        ch = nSize%256;
        pSize[i-1] = ch;
        nSize >>= 8;
     }
}

inline long CharArrayToInt(char *pSize)
{
    unsigned char i,ch;
    long nCount=0;
    for(i=0;i<sizeof(long);i++)
    {
       ch = pSize[i];
       nCount = nCount | ch;
       if( i<(sizeof(long)-1) )
          nCount<<=8;
    }
    return nCount;
}

inline short CharArrayToShort(char *pSize)
{
    unsigned char i,ch;
    short nCount=0;
    for(i=0;i<sizeof(short);i++)
    {
       ch = pSize[i];
       nCount = nCount | ch;
       if( i<(sizeof(short)-1) )
          nCount<<=8;
    }
    return nCount;
}

#endif
