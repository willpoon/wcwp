//---------------------------------------------------------------------------

#ifndef c_mzcompressH
#define c_mzcompressH

//---------------------------------------------------------------------------
#include "cmpublic.h"

class TCMZCompress
{
private:
    static void (*s_m_pfunPutcHandle)(int c);

    static void _CompressMZFile(TCString sRawFile, TCString sCompressedFile);
    static void _ExpandMZFile(TCString sCompressedFile, TCString sExpandedFile);
public:
    static void CompressMZFile(TCString sRawFile, TCString sCompressedFile = "",
            bool bDeleteOriginalFile = true);
    static void ExpandMZFile(TCString sCompressedFile,
            TCString sExpandedFile = "", bool bDeleteOriginalFile = true);

    static void Putc(int c);

    static void SetPutcHandle(void (*pfunPutcHandle)(int c));
};

class TCCompressException : public TCException
{
public:
    TCCompressException(TCString sMsg);
};

#endif
