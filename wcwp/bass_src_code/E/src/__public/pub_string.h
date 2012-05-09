//---------------------------------------------------------------------------

#ifndef pub_stringH
#define pub_stringH
//---------------------------------------------------------------------------
void PrintTestMsg(const TCString sMsg);

TCString LeftTrim(const TCString cstrSource);
TCString RightTrim(const TCString cstrSource);
TCString AllTrim(const TCString cstrSource);

TCString Fill(char cChar, long nCount);
TCString Space(long nCount);
long Length(const TCString cstrSource);

TCString Mid(const TCString cstrSource, long nFirst, long nCount);
TCString Mid(const TCString cstrSource, long nFirst);
TCString Left(const TCString cstrSource, long nCount);
TCString Right(const TCString cstrSource, long nCount);

long Pos(const TCString sString, const TCString sSubStr);

TCString LowerCase(const TCString sValue);
TCString UpperCase(const TCString sValue);

TCString Padl(const TCString cstrSource, long nLen, char cFillChar = ' ');
TCString Padr(const TCString cstrSource, long nLen, char cFillChar = ' ');
TCString Padc(const TCString cstrSource, long nLen, char cFillChar = ' ');

void FillPadl(char *pDest, const TCString cstrSource, long nLen = -1,
        char cFillChar = ' ');
void FillPadr(char *pDest, const TCString cstrSource, long nLen = -1,
        char cFillChar = ' ');

TCString IntToStr(long nValue);
long StrToInt(const TCString sValue);
double StrToFloat(const TCString sValue);

enum TEAlignMode { amLeftAlign, amRightAlign, amCenterAlign };
void FillCharArray(const TCString cstrSource, char *CharArray,
        long nLen = -1, TEAlignMode eamAlignMode = amLeftAlign,
        char cFillChar = ' ');


long AT(TCString S, TCString sSubStr, long nStart = 1);
TCString Replace(TCString sStr, long nStart, long n, TCString sRepStr);
TCString ReplaceAllSubStr(TCString sString, TCString sSubStr, TCString sRepStr);

long  LastDelimiter(const TCString Delimiters, TCString S);
TCString FloatToStr(double dValue,char nPrecision = 0);

bool IsNumber(TCString sString);

TCString GetSepStringBySeq(TCString sFullString, TCString sSepChar, long nSeq);

TCString QuotedStr(TCString sString);

TCString StrGetName(TCString sString,char cCommaChar = '=');  //%%%Add  20001211
TCString StrGetValue(TCString sString,char cCommaChar = '='); //%%%Add

TCString BCBToStr(const TCString & sBCBStr);
TCString StrToBCB(const TCString & sNumStr);

TCString MakeReverse(const TCString & sString);

//add by chenyi 2001/8/26
unsigned long AscStrToInt(const TCString &sString);
unsigned long DesStrToInt(const TCString &sString);
TCString AscBCDToStr(const TCString &sString, bool bByteFlag, int nStop);
TCString DesBCDToStr(const TCString &sString, bool bByteFlag, int nStop);

#endif

