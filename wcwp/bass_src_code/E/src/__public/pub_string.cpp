//---------------------------------------------------------------------------
#include "cmpublic.h"
#pragma hdrstop

#include "pub_string.h"

//---------------------------------------------------------------------------
//==========================================================================
// 函数 : PrintTestMsg
// 用途 : 打印调试用字符串
// 原型 : void PrintTestMsg(const TCString sMsg);
// 参数 : 要显示出来的字符串
// 返回 : 无
// 说明 : add by lgk at 2005.05.24调试用
//==========================================================================
void PrintTestMsg(const TCString sMsg)
{
	#ifdef __TEST__
    	printf("%s\r\n",(char*)sMsg);
    #endif
    return;
}

//==========================================================================
// 函数 : LeftTrim
// 用途 : 去掉字符串的左空格
// 原型 : TCString LeftTrim(const TCString cstrSource);
// 参数 : 源字符串
// 返回 : 去掉左空格的字符串
// 说明 :
//==========================================================================
TCString LeftTrim(const TCString cstrSource)
{
    TCString cstrTemp(cstrSource);
    cstrTemp.TrimLeft();
    return cstrTemp;
}

//==========================================================================
// 函数 : RightTrim
// 用途 : 去掉字符串的右空格
// 原型 : TCString RightTrim(const TCString cstrSource);
// 参数 : 源字符串
// 返回 : 去掉右空格的字符串
// 说明 :
//==========================================================================
TCString RightTrim(const TCString cstrSource)
{
    TCString cstrTemp(cstrSource);
    cstrTemp.TrimRight();
    return cstrTemp;
}

//==========================================================================
// 函数 : AllTrim
// 用途 : 去掉字符串的前后空格
// 原型 : TCString AllTrim(const TCString cstrSource);
// 参数 : 源字符串
// 返回 : 去掉前后空格的字符串
// 说明 :
//==========================================================================
TCString AllTrim(const TCString cstrSource)
{
    TCString cstrTemp;
    cstrTemp = LeftTrim(RightTrim(cstrSource));
    return cstrTemp;
}

//==========================================================================
// 函数 : Fill
// 用途 : 得到字符重复指定次数的字符串
// 原型 : TCString Fill(char cChar, long nCount)
// 参数 : 指定字符，重复次数
// 返回 : 填充以指定字符，指定长度的字符串
// 说明 :
//==========================================================================
TCString Fill(char cChar, long nCount)
{
/*
    // 本段代码性能不高，但保留于此以做其他测试
    TCString cstrTemp;
    long i;

    cstrTemp = "";
    for (i=1; i<=nCount; i++)
        cstrTemp += cChar;

    return cstrTemp;
*/
    TCString cstrTemp(cChar, nCount);
    return cstrTemp;
}

//==========================================================================
// 函数 : IntToStr
// 用途 : 得到整数数字（长整形）的字符串表示
// 原型 : TCString IntToStr(long nValue);
// 参数 : 数字(long型, 本系统约定所有的整形数都是long型)
// 返回 : 数字字符串
// 说明 : 返回以10为基的字符串，其他如16进制、8进制暂不考虑
//==========================================================================
TCString IntToStr(long nValue)
{
    char szValue[16];

    sprintf(szValue, "%ld", nValue);

    ASSERT(strlen(szValue) <= 15);
    return TCString(szValue);
}

//==========================================================================
// 函数 : StrToInt
// 用途 : 得到字符串表示的整型值
// 原型 : long StrToInt(TCString sValue);
// 参数 : 字符串
// 返回 : 长整数
// 说明 : 字符串以10为基，其他如16进制、8进制暂不考虑
//        调用atol。如果转换不成功，则返回0。
//==========================================================================
long StrToInt(const TCString sValue)
{
    return atol(sValue);
}

double StrToFloat(const TCString sValue)
{
    return atof(sValue);
}

//==========================================================================
// 函数 : Space
// 用途 : 得到指定长度的填以空格的字符串
// 原型 : TCString Space(long nCount)
// 参数 : 指定长度
// 返回 : 填充以空格的指定长度的字符串
// 说明 :
//==========================================================================
TCString Space(long nCount)
{
    return Fill(' ', nCount);
}

//==========================================================================
// 函数 : Length
// 用途 : 字符串的长度
// 原型 : long Length(const TCString cstrSource)
// 参数 : 字符串
// 返回 : 字符串长度
// 说明 :
//==========================================================================
long Length(const TCString cstrSource)
{
    return cstrSource.GetLength();
}

//==========================================================================
// 函数 : Mid
// 用途 : 截取字符串指定起始位置后指定长度的字符串
// 原型 : TCString Mid(const TCString cstrSource, long nFirst, long nCount);
// 参数 : 源字符串, 起始位置，长度
// 返回 : 截取后的字符串
// 说明 : 字符串的起始位置从1开始计数
//==========================================================================
TCString Mid(const TCString cstrSource, long nFirst, long nCount)
{
    return cstrSource.Mid(nFirst, nCount);
}

//==========================================================================
// 函数 : Mid
// 用途 : 截取字符串指定起始位置后的字符串
// 原型 : TCString Mid(const TCString cstrSource, long nFirst);
// 参数 : 源字符串, 起始位置
// 返回 : 截取后的字符串
// 说明 : 字符串的起始位置从1开始计数
//==========================================================================
TCString Mid(const TCString cstrSource, long nFirst)
{
    return cstrSource.Mid(nFirst);
}

//==========================================================================
// 函数 : Left
// 用途 : 返回字符左边的批定长度的字符串
// 原型 : TCString Left(const TCString cstrSource, long nCount);
// 参数 : 源字符串, 指定长度
// 返回 : 截取后的字符串
// 说明 : 如果源字符串的长度小于指定的长度，返回该字符串
//==========================================================================
TCString Left(const TCString cstrSource, long nCount)
{
    return cstrSource.Left(nCount);
}

//==========================================================================
// 函数 : Right
// 用途 : 返回字符右边的批定长度的字符串
// 原型 : TCString Right(const TCString cstrSource, long nCount);
// 参数 : 源字符串, 指定长度
// 返回 : 截取后的字符串
// 说明 : 如果源字符串的长度小于指定的长度，返回该字符串
//==========================================================================
TCString Right(const TCString cstrSource, long nCount)
{
    return cstrSource.Right(nCount);
}

//==========================================================================
// 函数 : Pos
// 用途 : 查找一个字符串在另一字符串中的位置
// 原型 : long Pos(const TCString s1, const TCString s2);
// 参数 : s1 -- 要查找s2的字符串
//        s2 -- 要在s1中查找的字符串 (查找s1是否包含s2)
// 返回 : 查找到的字符串位置，如果没有找到则返回0
// 说明 : 字符串的起始位置从1开始计数
//==========================================================================
long Pos(const TCString sString, const TCString sSubStr)
{
    long nFindPos;
    nFindPos = sString.Find(sSubStr);

    if (nFindPos == -1)
        return 0;
    else
        return nFindPos ;
}

TCString LowerCase(const TCString sValue)
{
    TCString sTmp;
    sTmp = sValue;
    sTmp.MakeLower();
    return sTmp;
}

TCString UpperCase(const TCString sValue)
{
    TCString sTmp;
    sTmp = sValue;
    sTmp.MakeUpper();
    return sTmp;
}

//==========================================================================
// 函数 : Padl
// 用途 : 字符串左边填以空格或指定字符（右对齐）的指定长度的字符串
// 原型 : TCString Padl(const TCString cstrSource, long nLen,
//          char cFillChar = ' ');
// 参数 : 源字符串, 指定长度, 填充字符
// 返回 : 填充后的字符串
// 说明 : 如果源字符串的长度大于指定的长度，则左截取该字符串
//==========================================================================
TCString Padl(const TCString cstrSource, long nLen, char cFillChar)
{
    TCString cstrTemp(cstrSource);
    TCString cstrSpace;
    if (Length(cstrTemp) > nLen)
        return Left(cstrTemp, nLen);

    cstrSpace = Fill(cFillChar, nLen - Length(cstrSource));

    return cstrSpace + cstrTemp;
};

void FillPadl(char *pDest, const TCString cstrSource, long nLen, char cFillChar)
{
    long nSourceLen;
    long nDelta;

    nSourceLen = Length(cstrSource);
    if (nLen == -1)
        nLen = nSourceLen;

    nDelta = nLen - nSourceLen;

    if (nDelta > 0)
    {
        memset(pDest, cFillChar, nDelta);
        memcpy(pDest + nDelta, (char *)cstrSource, nSourceLen);
    }
    else
        memcpy(pDest, (char *)cstrSource, nLen);
};

//==========================================================================
// 函数 : Padr
// 用途 : 字符串右边填以空格或指定字符（左对齐）的指定长度的字符串
// 原型 : TCString Padr(const TCString cstrSource, long nLen,
//          char cFillChar = ' ');
// 参数 : 源字符串, 指定长度, 填充字符
// 返回 : 填充后的字符串
// 说明 : 如果源字符串的长度大于指定的长度，则左截取该字符串
//==========================================================================
TCString Padr(const TCString cstrSource, long nLen, char cFillChar)
{
    TCString cstrTemp(cstrSource);
    TCString cstrSpace;
    if (Length(cstrTemp) > nLen)
        return Left(cstrTemp, nLen);

    cstrSpace = Fill(cFillChar, nLen - Length(cstrSource));

    return cstrTemp + cstrSpace;
};

void FillPadr(char *pDest, const TCString cstrSource, long nLen, char cFillChar)
{
    long nSourceLen;
    long nDelta;

    nSourceLen = Length(cstrSource);

    if (nLen == -1)
        nLen = nSourceLen;

    nDelta = nLen - nSourceLen;

    if (nDelta > 0)
    {
        memcpy(pDest, (char *)cstrSource, nSourceLen);
        memset(pDest + nSourceLen, cFillChar, nDelta);
    }
    else
        memcpy(pDest, (char *)cstrSource, nLen);
};

//==========================================================================
// 函数 : Padc
// 用途 : 字符串两边填以空格或指定字符（中对齐）的指定长度的字符串
// 原型 : TCString Padc(const TCString cstrSource, long nLen,
//          char cFillChar = ' ');
// 参数 : 源字符串, 指定长度, 填充字符
// 返回 : 填充后的字符串
// 说明 : 如果源字符串的长度大于指定的长度，则左截取该字符串
//==========================================================================
TCString Padc(const TCString cstrSource, long nLen, char cFillChar)
{
    TCString cstrTemp(cstrSource);
    TCString cstrSpaceLeft, cstrSpaceRight;
    long nSpaceLen;

    if (Length(cstrTemp) > nLen)
        return Left(cstrTemp, nLen);

    nSpaceLen = nLen - Length(cstrSource);

    cstrSpaceLeft = Fill(cFillChar, nSpaceLen / 2);
    cstrSpaceRight = Fill(cFillChar, nSpaceLen - nSpaceLen / 2);

    return cstrSpaceLeft + cstrTemp + cstrSpaceRight;
};

//==========================================================================
// 函数 : FillCharArray
// 用途 : 将字符串的内容复制到指定的字符串数组中，字符串的尾缀'\0'不复制
// 原型 : void FillCharArray(TCString cstrSource,
//        char *CharArray, long nLen = -1,
//        TEAlignMode eamAlignMode = amLeftAlign,
//        char cFillChar = ' ')
// 参数 : cstrSource   -- 源字符串
//        CharArray    -- 字符串数组，数组中应已分配足够长的空间
//        nLen         -- 复制长度，如指定复制长度，则填充字符串至指定长度
//        eamAlignMode -- 对齐模式，缺省时为左对齐（调用Padr）
//        cFillChar    -- 填充的字符
// 返回 : 无
// 说明 :
//==========================================================================
void FillCharArray(const TCString cstrSource, char *CharArray, long nLen,
        TEAlignMode eamAlignMode, char cFillChar)
{

    TCString cstrTemp;
    cstrTemp = cstrSource;
    if (nLen != -1)     // 如果不是缺省长度
    {
        switch (eamAlignMode)
        {
            case amLeftAlign:
                cstrTemp = Padr(cstrSource, nLen, cFillChar);
                break;
            case amRightAlign:
                cstrTemp = Padl(cstrSource, nLen, cFillChar);
                break;
            default:
                cstrTemp = Padc(cstrSource, nLen, cFillChar);
        }
    };

    memcpy(CharArray, cstrTemp, Length(cstrTemp));

}

//==========================================================================
// 函数 : AT
// 用途 : 在一个字符串指定位置开始查找另一个字符串
// 原型 : long AT(TCString S, TCString sSubStr, long nStart = 1);
// 参数 : 字符串，子字符串，开始位置
// 返回 : 查找到的字符串位置，如果没有找到则返回0
// 说明 :
//==========================================================================
long AT(TCString S, TCString sSubStr, long nStart)
{
    TCString sRightStr;
    long nPos;

    if (nStart < 1)
        nStart = 1;

    sRightStr = Mid(S, nStart);
    nPos = Pos(sRightStr, sSubStr);

    if (nPos == 0)
        return 0;
    else
        return nStart + nPos - 1;
}

//==========================================================================
// 函数 : Replace
// 用途 : 将一个字符串中指定位置，指定长度的子字符串置换成另一个字符串
// 原型 : TCString Replace(TCString sStr, long nStart,
//            long n, TCString sRepStr);
// 参数 : 字符串，开始位置，长度，置换字符串
// 返回 : 置换过的字符串
// 说明 :
//==========================================================================
TCString Replace(TCString sStr, long nStart, long n, TCString sRepStr)
{
    return Mid(sStr, 1, nStart - 1) + sRepStr + Mid(sStr, nStart + n);
}

//==========================================================================
// 函数 : ReplaceAllSubStr
// 用途 : 将一个字符串中的指定字符串全部变为另一字符串
// 原型 : TCString ReplaceAllSubStr(TCString sString, TCString sSubStr,
//            TCString sRepStr);
// 参数 : 字符串，查找字符串，替换字符串
// 返回 : 置换过的字符串
// 说明 :
//==========================================================================
TCString ReplaceAllSubStr(TCString sString, TCString sSubStr, TCString sRepStr)
{
    long nStartPos, nPos;

    if (sSubStr == TCString("") )
        return sString;

    nStartPos = 1;
    nPos = AT(sString, sSubStr, nStartPos);

    while (nPos != 0)
    {
        sString = Replace(sString, nPos, Length(sSubStr), sRepStr);
        nStartPos = nPos + Length(sRepStr);
        nPos = AT(sString, sSubStr, nStartPos);
    }

    return sString;
}

//==========================================================================
// 函数 : LastDelimiter
// 用途 : 得到一个字符串中最后一个分隔符，分隔符定义在指定字符串中
// 原型 : long  LastDelimiter(const TCString Delimiters, TCString S)
// 参数 : 分隔符串，待查找的字符串
// 返回 : 找到的分隔符位置，如果没有找到则返回0
// 说明 :
//==========================================================================
long  LastDelimiter(const TCString Delimiters, TCString S)
{
    long nRet;

    nRet = Length(S);

    while (nRet > 0)
    {
        if (S[nRet] != '\0' && Delimiters.Find(S[nRet]) != -1)
            return nRet;

        nRet --;
    }

    return 0;
}
//==========================================================================
// 函数 : FloatToStr
// 用途 : 把浮点数转换成串
// 原型 : TCString FloatToStr(double dValue,char nPrecision)
// 参数 : 浮点数，转换精度
// 返回 : 浮点数
// 说明 :
//==========================================================================
TCString FloatToStr(double dValue,char nPrecision)
{
  ASSERT( (nPrecision >= 0)&&(nPrecision < 20) );
  TCString S;
  char nCount[50];
  sprintf(nCount,"%1.*lf",nPrecision,dValue);
  S = nCount;
  return S;
}

//==========================================================================
// 函数 : IsNumber
// 用途 : 判断一个字符串是否数字串
// 原型 : bool IsNumber(TCString sString);
// 参数 : 字符串
// 返回 : 是否数字串
// 说明 :
//==========================================================================
bool IsNumber(TCString sString)
{
    if (Length(sString) < 1)
        return false;

    long i;
    char cChar;

    for (i = 1; i <= Length(sString); i++)
    {
        cChar = sString[i];

        if ((cChar < '0' || cChar > '9') && (cChar != ' ') && (cChar != '.'))
            return false;
    }

    return true;
}

//==========================================================================
// 函数 : GetSepStringBySeq
// 用途 : 得到一个字符串的指定序号指定分隔符的子串值
// 原型 : TCString GetSepStringBySeq(TCString sFullString,
//              TCString sSepChar, long nSeq);
// 参数 : 全字符串, 分隔符, 序数值(从1计数)
// 返回 : 子串值
// 说明 :
//==========================================================================
TCString GetSepStringBySeq(TCString sFullString, TCString sSepChar, long nSeq)
{
    TCString sTmpString;
    long nTmpPos;
    long nSeqCount;

    sTmpString = sFullString;
    nSeqCount = 1;

    while (true)
    {
        if (nSeq > nSeqCount)
        {
            nTmpPos = Pos(sTmpString, sSepChar);
            if (nTmpPos == 0)
                return "";

            nSeqCount ++;

            sTmpString = Mid(sTmpString, nTmpPos + Length(sSepChar));
        }
        else
        {
            nTmpPos = Pos(sTmpString, sSepChar);

            if (nTmpPos == 0)
                return sTmpString;

            return Mid(sTmpString, 1, nTmpPos - 1);
        }
    }
}

//==========================================================================
// 函数 : QuotedStr
// 用途 : 将一个字符串的两端加上单引号
// 原型 : TCString QuotedStr(TCString sString)
// 参数 : 字符串
// 返回 : 加上单引号的字符串
// 说明 :
//==========================================================================
TCString QuotedStr(TCString sString)
{
    return "'" + sString + "'";
}

//==========================================================================
// 函数 : StrGetName
// 用途 : 得到一个字符串的名称,'='前面的
// 原型 : TCString StrGetName(TCString sString)
// 参数 : 字符串
// 返回 : 字符串
// 说明 :
//==========================================================================
TCString StrGetName(TCString sString,char cCommaChar)  //%%%Add  20001211
{
   long nPos;
   nPos = sString.Find(cCommaChar);
   if (nPos == -1)
       return AllTrim(sString);
   else
       return AllTrim(Left(sString, nPos - 1));
}

//==========================================================================
// 函数 : StrGetName
// 用途 : 得到一个字符串的值,'='后面的
// 原型 : TCString StrGetValue(TCString sString)
// 参数 : 字符串
// 返回 : 字符串
// 说明 :
//==========================================================================
TCString StrGetValue(TCString sString,char cCommaChar)
{
   long nPos;
   nPos = sString.Find(cCommaChar);
   if (nPos == -1)
        return "";
   else
        return AllTrim(Mid(sString, nPos + 1));
}

//==========================================================================
// BCB码 : BCB码类似BCD码，不含'\0'，且按顺序存放
//         BCB码与BCD码之间的对应关系表示如下:
//
//             表示  BCD码  BCB码
//             ----  -----  -----
//             0     0      A
//             1~9   1~9    1~9
//             结束  F      0
//
//==========================================================================
// 函数 : BCBToStr
// 用途 : 将BCB码转换为数字字符串编码
// 原型 : TCString BCBToStr(const TCString & sBCBStr);
// 参数 : BCB串
// 返回 : 数字字符串
// 说明 :
//    原串: "\x13\x29\x38\x76"
//    结果: "13293876"
//==========================================================================
TCString BCBToStr(const TCString & sBCBStr)
{
    long nLen;

    long i;
    char c1, c2;
    TCString sResult;

    nLen = Length(sBCBStr);

    //====== 1. 循环处理每一个字符 ======
    for (i = 1; i <= nLen; i++)
    {
        c1 = sBCBStr[i];
        //====== 2. 处理高位 ======
        c2 = (c1 >> 4) & 0x0F;
        if (c2 == 0x00)
            break;
        if (c2 >= 0x01 && c2 <= 0x09)
            c2 = c2 + 0x30;
        else if (c2 == 0x0A)
            c2 = '0';
        else
            throw TCException("BCBToStr() Error : Invalid BCB String - " 
                    + sBCBStr);
        sResult += c2;

        //====== 3. 处理低位 ======
        c2 = c1 & 0x0F;
        if (c2 == 0x00)
            break;
        if (c2 >= 0x01 && c2 <= 0x09)
            c2 = c2 + 0x30;
        else if (c2 == 0x0A)
            c2 = '0';
        else
            throw TCException("BCBToStr() Error : Invalid BCB String - " 
                    + sBCBStr);
        sResult += c2;
    }

    return sResult;
}

//==========================================================================
// 函数 : StrToBCB
// 用途 : 将数字字符串转换为BCB码
// 原型 : TCString StrToBCB(const TCString & sNumStr);
// 参数 : 数字字符串
// 返回 : BCB编码字符串
// 说明 :
//    原串: "13293876"
//    结果: "\x13\x29\x38\x76"
//==========================================================================
TCString StrToBCB(const TCString & sNumStr)
{
    long nLen;

    long i;
    char c1, c2;
    TCString sResult;

    nLen = Length(sNumStr);

    //====== 1. 循环处理每一个字符 ======
    for (i = 1; i <= nLen; i++)
    {
        c1 = sNumStr[i];
        if (c1 < '0' || c1 > '9')
            throw TCException("StrToBCB() Error : NumStr Invalid - " + sNumStr);

        //====== 2. 处理前一个字符 ======
        if (i % 2 == 1)
        {
            if (c1 >= '1' && c1 <= '9')
                c2 = (c1 - 0x30) << 4;
            else
            {
                ASSERT(c1 == '0');
                c2 = 0xA0;
            }
        }
        else
        {
            if (c1 >= '1' && c1 <= '9')
                c2 = (c1 - 0x30) | c2;
            else
            {
                ASSERT(c1 == '0');
                c2 = 0x0A | c2;
            }

            sResult += c2;
        }
    }

    if (nLen % 2 == 1)
        sResult += c2;

    return sResult;
}

//==========================================================================
// 函数 : MakeReverse
// 用途 : 将字符串倒序排列
// 原型 : TCString MakeReverse(const TCString & sString);
// 参数 : 字符串
// 返回 : 倒序后的字符串
// 说明 :
//==========================================================================
TCString MakeReverse(const TCString & sString)
{
    TCString sResult;
    sResult = sString;
    sResult.MakeReverse();
    return sResult;
}

//add by chenyi 2001/8/26
unsigned long AscStrToInt(const TCString &sString)
{
    int i;
    unsigned long nResult = 0;

    ASSERT(sString.GetLength()<=4);

    for(i=1; i<=sString.GetLength(); i++)
        nResult = nResult * 256 + (unsigned char)sString[i];

    return nResult;
}

unsigned long DesStrToInt(const TCString &sString)
{
    int i;
    unsigned long nResult = 0;

    ASSERT(sString.GetLength()<=4);

    for(i=sString.GetLength(); i>=1; i--)
        nResult = nResult * 256 + (unsigned char)sString[i];

    return nResult;
}

//==========================================================================
// 函数 : BcdAscToStr
// 用途 : 将BCD码转换为数字字符串编码
// 原型 : TCString BcdAscToStr(TCString &sItem, bool bByteFlag, int nStop);
// 参数 : BCD串
// 返回 : 数字字符串
// 说明 :
//    原串: "\x13\x29\x38\x76"
//    结果: "13293876"
//==========================================================================
TCString AscBCDToStr(const TCString &sString, bool bByteFlag, int nStop)
{
    int i;
    unsigned char c1,c2;
    TCString sResult;

    for(i=1; i<=sString.GetLength(); i++)
    {
        if(bByteFlag)
            c1 = (unsigned char)sString[i] >> 4;
        else
            c1 = (unsigned char)sString[i] &0x0f;

        if ((int)c1 == nStop)
            break;
        if(c1 <= 9)
            c1 += '0';
        else
            c1 += 'A' - 10;
        sResult += c1;

        if(bByteFlag)
            c2 = (unsigned char)sString[i] & 0x0f;
        else
            c2 = (unsigned char)sString[i] >> 4;

        if((int)c2 == nStop)
            break;
        if(c2 <= 9)
            c2 += '0';
        else
            c2 += 'A' - 10;
        sResult += c2;
    }

    return sResult;
}

TCString DesBCDToStr(const TCString &sString, bool bByteFlag, int nStop)
{
    int i;
    unsigned char c1,c2;
    TCString sResult;

    for(i=sString.GetLength(); i>=1; i--)
    {
        if(bByteFlag)
            c1 = (unsigned char)sString[i] >> 4;
        else
            c1 = (unsigned char)sString[i] &0x0f;

        if ((int)c1 == nStop)
            break;
        if(c1 <= 9)
            c1 += '0';
        else
            c1 += 'A' - 10;
        sResult += c1;

        if(bByteFlag)
            c2 = (unsigned char)sString[i] & 0x0f;
        else
            c2 = (unsigned char)sString[i] >> 4;

        if((int)c1 == nStop)
            break;
        if(c2 <= 9)
            c2 += '0';
        else
            c2 += 'A' - 10;
        sResult += c2;
    }

    return sResult;
}

