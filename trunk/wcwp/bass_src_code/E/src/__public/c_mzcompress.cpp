//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_mzcompress.h"
#include "c_mzcompress_bitio.h"
#include "c_mzcompress_lzw15v.h"
//---------------------------------------------------------------------------

void (*TCMZCompress::s_m_pfunPutcHandle)(int c) = NULL;

//==========================================================================
// 函数 : TCMZCompress::_CompressMZFile
// 用途 : 压缩文件
// 原型 : static void _CompressMZFile(TCString sRawFile,
//            TCString sCompressedFile);
// 参数 : 源文件名，压缩文件
// 返回 : 无
// 说明 : 压缩文件主要操作函数(私有)。被CompressMZFile()调用。
//==========================================================================
void TCMZCompress::_CompressMZFile(TCString sRawFile, TCString sCompressedFile)
{
	BIT_FILE *output;
	FILE *input;

	input = fopen(sRawFile, "rb");
	if(input == NULL)
		throw TCCompressException("Error opening " + sRawFile + " for input");
	output = OpenOutputBitFile(sCompressedFile);
	if(output == NULL)
    {   fclose(input);
		throw TCCompressException("Error opening " + sCompressedFile
                + " for output");
    }
	CompressFile(input, output);
	CloseOutputBitFile(output);
	fclose(input);
}

//==========================================================================
// 函数 : TCMZCompress::_ExpandMZFile
// 用途 : 解压文件
// 原型 : static void _ExpandMZFile(TCString sCompressedFile,
//              TCString sExpandedFile);
// 参数 : 压缩文件, 解压目标文件
// 返回 : 无
// 说明 : 解压文件主要操作函数(私有)。被ExpandMZFile()调用。
//==========================================================================
void TCMZCompress::_ExpandMZFile(TCString sCompressedFile,
        TCString sExpandedFile)
{
	BIT_FILE *input;
	FILE *output;
	
	input = OpenInputBitFile(sCompressedFile);
	if(input == NULL)
    {   CloseInputBitFile(input);
		throw TCCompressException("Error opening " + sCompressedFile
                + " for input");
    }
	output = fopen(sExpandedFile, "wb");
	if(output == NULL)
		throw TCCompressException("Error opening " + sExpandedFile
                + " for output");
	ExpandFile(input, output);
	CloseInputBitFile(input);
	fclose(output);
}

//==========================================================================
// 函数 : TCMZCompress::CompressMZFile
// 用途 : 压缩文件公有函数
// 原型 : static void CompressMZFile(TCString sRawFile,
//          TCString sCompressedFile = "", bool bDeleteOriginalFile = true);
// 参数 : 源文件，目标压缩文件，是否删除源文件
// 返回 : 无
// 说明 : 如果不指定目标文件，则目标文件为源文件名后加扩展名".MZ"
//        缺省情况下，删除源文件
//==========================================================================
void TCMZCompress::CompressMZFile(TCString sRawFile, TCString sCompressedFile,
            bool bDeleteOriginalFile)
{
    if (!FileExists(sRawFile))
        throw TCException("TCMZCompress::CompressMZFile() : File "
                + sRawFile + " Does not Exists.");

    TCString sTempFileName;

    if (sCompressedFile == TCString("") )
        sCompressedFile = sRawFile + ".MZ";

    sTempFileName = MergePathAndFile(ExtractFilePath(sCompressedFile),
            ExtractFileName(sRawFile) + "_Z" + Right(TCTime::Now(), 4));

    _CompressMZFile(sRawFile, sTempFileName);

    if (FileExists(sCompressedFile))
        DeleteFile(sCompressedFile);

    if (!RenameFile(sTempFileName, sCompressedFile))
        throw TCCompressException("CompressMZFile() rename error. "
                " From file : " + sTempFileName
                + " To file : " + sCompressedFile);

    if (bDeleteOriginalFile)
        if (!DeleteFile(sRawFile))
            throw TCCompressException("CompressMZFile() Delete File error. "
                    " FileName : " + sRawFile);
}

//==========================================================================
// 函数 : TCMZCompress::ExpandMZFile
// 用途 : 解压文件公有函数
// 原型 : static void ExpandMZFile(TCString sCompressedFile,
//            TCString sExpandedFile = "", bool bDeleteOriginalFile = true);
// 参数 : 源压缩文件，目标解压文件，是否删除源文件
// 返回 : 无
// 说明 : 如果不指定目标文件，则目标文件为源文件名剥去扩展名".MZ"，这种
//        情况下，如果源文件的扩展名不是".MZ"，则抛出例外。
//        缺省情况下，删除源文件
//==========================================================================
void TCMZCompress::ExpandMZFile(TCString sCompressedFile,
        TCString sExpandedFile, bool bDeleteOriginalFile)
{
    if (!FileExists(sCompressedFile))
        throw TCException("TCMZCompress::ExpandMZFile() : File "
                + sCompressedFile + " Does not Exists.");
    TCString sTempFileName;

    if (sExpandedFile == TCString("") )
    {
        if (Right(sCompressedFile, 3) != TCString(".MZ") )
            throw TCCompressException("ExpandMZFile() Filename != \"*.MZ\"");

        sExpandedFile = Left(sCompressedFile, Length(sCompressedFile) - 3);
    }

    sTempFileName = MergePathAndFile(ExtractFilePath(sExpandedFile),
            ExtractFileName(sCompressedFile) + "_Z" + Right(TCTime::Now(), 4));

    _ExpandMZFile(sCompressedFile, sTempFileName);

    if (FileExists(sExpandedFile))
        DeleteFile(sExpandedFile);

    if (!RenameFile(sTempFileName, sExpandedFile))
        throw TCCompressException("ExpandMZFile() rename error. "
                " From file : " + sTempFileName
                + " To file : " + sExpandedFile);

    if (bDeleteOriginalFile)
        if (!DeleteFile(sCompressedFile))
            throw TCCompressException("ExpandMZFile() Delete File error. "
                    " FileName : " + sCompressedFile);
}

//==========================================================================
// 函数 : TCMZCompress::Putc
// 用途 : 输出字符
// 原型 : void Putc(int c);
// 参数 : 字符
// 返回 : 无
// 说明 : 该函数在处理时被压缩时的处理函数调用。用于显示当前的进展情况。
//==========================================================================
void TCMZCompress::Putc(int c)
{
    if (s_m_pfunPutcHandle != NULL)
        s_m_pfunPutcHandle(c);
}

//==========================================================================
// 函数 : TCMZCompress::SetPutcHandle
// 用途 : 安装字符输出的句柄
// 原型 : void SetPutcHandle(void (*pfunPutcHandle)(int c));
// 参数 : 输出字符的函数指针
// 返回 : 无
// 说明 : 在命令行状态下可调用之来安装句柄，用于显示进展状况。
//==========================================================================
void TCMZCompress::SetPutcHandle(void (*pfunPutcHandle)(int c))
{
    s_m_pfunPutcHandle = pfunPutcHandle;
}

TCCompressException::TCCompressException(TCString sMsg) : TCException(sMsg)
{
} 



