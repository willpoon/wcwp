//---------------------------------------------------------------------------
#include "cmpublic.h"
#pragma hdrstop

#include "pub_string.h"

//---------------------------------------------------------------------------
//==========================================================================
// ���� : PrintTestMsg
// ��; : ��ӡ�������ַ���
// ԭ�� : void PrintTestMsg(const TCString sMsg);
// ���� : Ҫ��ʾ�������ַ���
// ���� : ��
// ˵�� : add by lgk at 2005.05.24������
//==========================================================================
void PrintTestMsg(const TCString sMsg)
{
	#ifdef __TEST__
    	printf("%s\r\n",(char*)sMsg);
    #endif
    return;
}

//==========================================================================
// ���� : LeftTrim
// ��; : ȥ���ַ�������ո�
// ԭ�� : TCString LeftTrim(const TCString cstrSource);
// ���� : Դ�ַ���
// ���� : ȥ����ո���ַ���
// ˵�� :
//==========================================================================
TCString LeftTrim(const TCString cstrSource)
{
    TCString cstrTemp(cstrSource);
    cstrTemp.TrimLeft();
    return cstrTemp;
}

//==========================================================================
// ���� : RightTrim
// ��; : ȥ���ַ������ҿո�
// ԭ�� : TCString RightTrim(const TCString cstrSource);
// ���� : Դ�ַ���
// ���� : ȥ���ҿո���ַ���
// ˵�� :
//==========================================================================
TCString RightTrim(const TCString cstrSource)
{
    TCString cstrTemp(cstrSource);
    cstrTemp.TrimRight();
    return cstrTemp;
}

//==========================================================================
// ���� : AllTrim
// ��; : ȥ���ַ�����ǰ��ո�
// ԭ�� : TCString AllTrim(const TCString cstrSource);
// ���� : Դ�ַ���
// ���� : ȥ��ǰ��ո���ַ���
// ˵�� :
//==========================================================================
TCString AllTrim(const TCString cstrSource)
{
    TCString cstrTemp;
    cstrTemp = LeftTrim(RightTrim(cstrSource));
    return cstrTemp;
}

//==========================================================================
// ���� : Fill
// ��; : �õ��ַ��ظ�ָ���������ַ���
// ԭ�� : TCString Fill(char cChar, long nCount)
// ���� : ָ���ַ����ظ�����
// ���� : �����ָ���ַ���ָ�����ȵ��ַ���
// ˵�� :
//==========================================================================
TCString Fill(char cChar, long nCount)
{
/*
    // ���δ������ܲ��ߣ��������ڴ�������������
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
// ���� : IntToStr
// ��; : �õ��������֣������Σ����ַ�����ʾ
// ԭ�� : TCString IntToStr(long nValue);
// ���� : ����(long��, ��ϵͳԼ�����е�����������long��)
// ���� : �����ַ���
// ˵�� : ������10Ϊ�����ַ�����������16���ơ�8�����ݲ�����
//==========================================================================
TCString IntToStr(long nValue)
{
    char szValue[16];

    sprintf(szValue, "%ld", nValue);

    ASSERT(strlen(szValue) <= 15);
    return TCString(szValue);
}

//==========================================================================
// ���� : StrToInt
// ��; : �õ��ַ�����ʾ������ֵ
// ԭ�� : long StrToInt(TCString sValue);
// ���� : �ַ���
// ���� : ������
// ˵�� : �ַ�����10Ϊ����������16���ơ�8�����ݲ�����
//        ����atol�����ת�����ɹ����򷵻�0��
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
// ���� : Space
// ��; : �õ�ָ�����ȵ����Կո���ַ���
// ԭ�� : TCString Space(long nCount)
// ���� : ָ������
// ���� : ����Կո��ָ�����ȵ��ַ���
// ˵�� :
//==========================================================================
TCString Space(long nCount)
{
    return Fill(' ', nCount);
}

//==========================================================================
// ���� : Length
// ��; : �ַ����ĳ���
// ԭ�� : long Length(const TCString cstrSource)
// ���� : �ַ���
// ���� : �ַ�������
// ˵�� :
//==========================================================================
long Length(const TCString cstrSource)
{
    return cstrSource.GetLength();
}

//==========================================================================
// ���� : Mid
// ��; : ��ȡ�ַ���ָ����ʼλ�ú�ָ�����ȵ��ַ���
// ԭ�� : TCString Mid(const TCString cstrSource, long nFirst, long nCount);
// ���� : Դ�ַ���, ��ʼλ�ã�����
// ���� : ��ȡ����ַ���
// ˵�� : �ַ�������ʼλ�ô�1��ʼ����
//==========================================================================
TCString Mid(const TCString cstrSource, long nFirst, long nCount)
{
    return cstrSource.Mid(nFirst, nCount);
}

//==========================================================================
// ���� : Mid
// ��; : ��ȡ�ַ���ָ����ʼλ�ú���ַ���
// ԭ�� : TCString Mid(const TCString cstrSource, long nFirst);
// ���� : Դ�ַ���, ��ʼλ��
// ���� : ��ȡ����ַ���
// ˵�� : �ַ�������ʼλ�ô�1��ʼ����
//==========================================================================
TCString Mid(const TCString cstrSource, long nFirst)
{
    return cstrSource.Mid(nFirst);
}

//==========================================================================
// ���� : Left
// ��; : �����ַ���ߵ��������ȵ��ַ���
// ԭ�� : TCString Left(const TCString cstrSource, long nCount);
// ���� : Դ�ַ���, ָ������
// ���� : ��ȡ����ַ���
// ˵�� : ���Դ�ַ����ĳ���С��ָ���ĳ��ȣ����ظ��ַ���
//==========================================================================
TCString Left(const TCString cstrSource, long nCount)
{
    return cstrSource.Left(nCount);
}

//==========================================================================
// ���� : Right
// ��; : �����ַ��ұߵ��������ȵ��ַ���
// ԭ�� : TCString Right(const TCString cstrSource, long nCount);
// ���� : Դ�ַ���, ָ������
// ���� : ��ȡ����ַ���
// ˵�� : ���Դ�ַ����ĳ���С��ָ���ĳ��ȣ����ظ��ַ���
//==========================================================================
TCString Right(const TCString cstrSource, long nCount)
{
    return cstrSource.Right(nCount);
}

//==========================================================================
// ���� : Pos
// ��; : ����һ���ַ�������һ�ַ����е�λ��
// ԭ�� : long Pos(const TCString s1, const TCString s2);
// ���� : s1 -- Ҫ����s2���ַ���
//        s2 -- Ҫ��s1�в��ҵ��ַ��� (����s1�Ƿ����s2)
// ���� : ���ҵ����ַ���λ�ã����û���ҵ��򷵻�0
// ˵�� : �ַ�������ʼλ�ô�1��ʼ����
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
// ���� : Padl
// ��; : �ַ���������Կո��ָ���ַ����Ҷ��룩��ָ�����ȵ��ַ���
// ԭ�� : TCString Padl(const TCString cstrSource, long nLen,
//          char cFillChar = ' ');
// ���� : Դ�ַ���, ָ������, ����ַ�
// ���� : ������ַ���
// ˵�� : ���Դ�ַ����ĳ��ȴ���ָ���ĳ��ȣ������ȡ���ַ���
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
// ���� : Padr
// ��; : �ַ����ұ����Կո��ָ���ַ�������룩��ָ�����ȵ��ַ���
// ԭ�� : TCString Padr(const TCString cstrSource, long nLen,
//          char cFillChar = ' ');
// ���� : Դ�ַ���, ָ������, ����ַ�
// ���� : ������ַ���
// ˵�� : ���Դ�ַ����ĳ��ȴ���ָ���ĳ��ȣ������ȡ���ַ���
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
// ���� : Padc
// ��; : �ַ����������Կո��ָ���ַ����ж��룩��ָ�����ȵ��ַ���
// ԭ�� : TCString Padc(const TCString cstrSource, long nLen,
//          char cFillChar = ' ');
// ���� : Դ�ַ���, ָ������, ����ַ�
// ���� : ������ַ���
// ˵�� : ���Դ�ַ����ĳ��ȴ���ָ���ĳ��ȣ������ȡ���ַ���
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
// ���� : FillCharArray
// ��; : ���ַ��������ݸ��Ƶ�ָ�����ַ��������У��ַ�����β׺'\0'������
// ԭ�� : void FillCharArray(TCString cstrSource,
//        char *CharArray, long nLen = -1,
//        TEAlignMode eamAlignMode = amLeftAlign,
//        char cFillChar = ' ')
// ���� : cstrSource   -- Դ�ַ���
//        CharArray    -- �ַ������飬������Ӧ�ѷ����㹻���Ŀռ�
//        nLen         -- ���Ƴ��ȣ���ָ�����Ƴ��ȣ�������ַ�����ָ������
//        eamAlignMode -- ����ģʽ��ȱʡʱΪ����루����Padr��
//        cFillChar    -- �����ַ�
// ���� : ��
// ˵�� :
//==========================================================================
void FillCharArray(const TCString cstrSource, char *CharArray, long nLen,
        TEAlignMode eamAlignMode, char cFillChar)
{

    TCString cstrTemp;
    cstrTemp = cstrSource;
    if (nLen != -1)     // �������ȱʡ����
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
// ���� : AT
// ��; : ��һ���ַ���ָ��λ�ÿ�ʼ������һ���ַ���
// ԭ�� : long AT(TCString S, TCString sSubStr, long nStart = 1);
// ���� : �ַ��������ַ�������ʼλ��
// ���� : ���ҵ����ַ���λ�ã����û���ҵ��򷵻�0
// ˵�� :
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
// ���� : Replace
// ��; : ��һ���ַ�����ָ��λ�ã�ָ�����ȵ����ַ����û�����һ���ַ���
// ԭ�� : TCString Replace(TCString sStr, long nStart,
//            long n, TCString sRepStr);
// ���� : �ַ�������ʼλ�ã����ȣ��û��ַ���
// ���� : �û������ַ���
// ˵�� :
//==========================================================================
TCString Replace(TCString sStr, long nStart, long n, TCString sRepStr)
{
    return Mid(sStr, 1, nStart - 1) + sRepStr + Mid(sStr, nStart + n);
}

//==========================================================================
// ���� : ReplaceAllSubStr
// ��; : ��һ���ַ����е�ָ���ַ���ȫ����Ϊ��һ�ַ���
// ԭ�� : TCString ReplaceAllSubStr(TCString sString, TCString sSubStr,
//            TCString sRepStr);
// ���� : �ַ����������ַ������滻�ַ���
// ���� : �û������ַ���
// ˵�� :
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
// ���� : LastDelimiter
// ��; : �õ�һ���ַ��������һ���ָ������ָ���������ָ���ַ�����
// ԭ�� : long  LastDelimiter(const TCString Delimiters, TCString S)
// ���� : �ָ������������ҵ��ַ���
// ���� : �ҵ��ķָ���λ�ã����û���ҵ��򷵻�0
// ˵�� :
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
// ���� : FloatToStr
// ��; : �Ѹ�����ת���ɴ�
// ԭ�� : TCString FloatToStr(double dValue,char nPrecision)
// ���� : ��������ת������
// ���� : ������
// ˵�� :
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
// ���� : IsNumber
// ��; : �ж�һ���ַ����Ƿ����ִ�
// ԭ�� : bool IsNumber(TCString sString);
// ���� : �ַ���
// ���� : �Ƿ����ִ�
// ˵�� :
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
// ���� : GetSepStringBySeq
// ��; : �õ�һ���ַ�����ָ�����ָ���ָ������Ӵ�ֵ
// ԭ�� : TCString GetSepStringBySeq(TCString sFullString,
//              TCString sSepChar, long nSeq);
// ���� : ȫ�ַ���, �ָ���, ����ֵ(��1����)
// ���� : �Ӵ�ֵ
// ˵�� :
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
// ���� : QuotedStr
// ��; : ��һ���ַ��������˼��ϵ�����
// ԭ�� : TCString QuotedStr(TCString sString)
// ���� : �ַ���
// ���� : ���ϵ����ŵ��ַ���
// ˵�� :
//==========================================================================
TCString QuotedStr(TCString sString)
{
    return "'" + sString + "'";
}

//==========================================================================
// ���� : StrGetName
// ��; : �õ�һ���ַ���������,'='ǰ���
// ԭ�� : TCString StrGetName(TCString sString)
// ���� : �ַ���
// ���� : �ַ���
// ˵�� :
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
// ���� : StrGetName
// ��; : �õ�һ���ַ�����ֵ,'='�����
// ԭ�� : TCString StrGetValue(TCString sString)
// ���� : �ַ���
// ���� : �ַ���
// ˵�� :
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
// BCB�� : BCB������BCD�룬����'\0'���Ұ�˳����
//         BCB����BCD��֮��Ķ�Ӧ��ϵ��ʾ����:
//
//             ��ʾ  BCD��  BCB��
//             ----  -----  -----
//             0     0      A
//             1~9   1~9    1~9
//             ����  F      0
//
//==========================================================================
// ���� : BCBToStr
// ��; : ��BCB��ת��Ϊ�����ַ�������
// ԭ�� : TCString BCBToStr(const TCString & sBCBStr);
// ���� : BCB��
// ���� : �����ַ���
// ˵�� :
//    ԭ��: "\x13\x29\x38\x76"
//    ���: "13293876"
//==========================================================================
TCString BCBToStr(const TCString & sBCBStr)
{
    long nLen;

    long i;
    char c1, c2;
    TCString sResult;

    nLen = Length(sBCBStr);

    //====== 1. ѭ������ÿһ���ַ� ======
    for (i = 1; i <= nLen; i++)
    {
        c1 = sBCBStr[i];
        //====== 2. �����λ ======
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

        //====== 3. �����λ ======
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
// ���� : StrToBCB
// ��; : �������ַ���ת��ΪBCB��
// ԭ�� : TCString StrToBCB(const TCString & sNumStr);
// ���� : �����ַ���
// ���� : BCB�����ַ���
// ˵�� :
//    ԭ��: "13293876"
//    ���: "\x13\x29\x38\x76"
//==========================================================================
TCString StrToBCB(const TCString & sNumStr)
{
    long nLen;

    long i;
    char c1, c2;
    TCString sResult;

    nLen = Length(sNumStr);

    //====== 1. ѭ������ÿһ���ַ� ======
    for (i = 1; i <= nLen; i++)
    {
        c1 = sNumStr[i];
        if (c1 < '0' || c1 > '9')
            throw TCException("StrToBCB() Error : NumStr Invalid - " + sNumStr);

        //====== 2. ����ǰһ���ַ� ======
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
// ���� : MakeReverse
// ��; : ���ַ�����������
// ԭ�� : TCString MakeReverse(const TCString & sString);
// ���� : �ַ���
// ���� : �������ַ���
// ˵�� :
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
// ���� : BcdAscToStr
// ��; : ��BCD��ת��Ϊ�����ַ�������
// ԭ�� : TCString BcdAscToStr(TCString &sItem, bool bByteFlag, int nStop);
// ���� : BCD��
// ���� : �����ַ���
// ˵�� :
//    ԭ��: "\x13\x29\x38\x76"
//    ���: "13293876"
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

