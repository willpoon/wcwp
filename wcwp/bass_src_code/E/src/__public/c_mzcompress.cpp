//---------------------------------------------------------------------------

#pragma hdrstop

#include "c_mzcompress.h"
#include "c_mzcompress_bitio.h"
#include "c_mzcompress_lzw15v.h"
//---------------------------------------------------------------------------

void (*TCMZCompress::s_m_pfunPutcHandle)(int c) = NULL;

//==========================================================================
// ���� : TCMZCompress::_CompressMZFile
// ��; : ѹ���ļ�
// ԭ�� : static void _CompressMZFile(TCString sRawFile,
//            TCString sCompressedFile);
// ���� : Դ�ļ�����ѹ���ļ�
// ���� : ��
// ˵�� : ѹ���ļ���Ҫ��������(˽��)����CompressMZFile()���á�
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
// ���� : TCMZCompress::_ExpandMZFile
// ��; : ��ѹ�ļ�
// ԭ�� : static void _ExpandMZFile(TCString sCompressedFile,
//              TCString sExpandedFile);
// ���� : ѹ���ļ�, ��ѹĿ���ļ�
// ���� : ��
// ˵�� : ��ѹ�ļ���Ҫ��������(˽��)����ExpandMZFile()���á�
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
// ���� : TCMZCompress::CompressMZFile
// ��; : ѹ���ļ����к���
// ԭ�� : static void CompressMZFile(TCString sRawFile,
//          TCString sCompressedFile = "", bool bDeleteOriginalFile = true);
// ���� : Դ�ļ���Ŀ��ѹ���ļ����Ƿ�ɾ��Դ�ļ�
// ���� : ��
// ˵�� : �����ָ��Ŀ���ļ�����Ŀ���ļ�ΪԴ�ļ��������չ��".MZ"
//        ȱʡ����£�ɾ��Դ�ļ�
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
// ���� : TCMZCompress::ExpandMZFile
// ��; : ��ѹ�ļ����к���
// ԭ�� : static void ExpandMZFile(TCString sCompressedFile,
//            TCString sExpandedFile = "", bool bDeleteOriginalFile = true);
// ���� : Դѹ���ļ���Ŀ���ѹ�ļ����Ƿ�ɾ��Դ�ļ�
// ���� : ��
// ˵�� : �����ָ��Ŀ���ļ�����Ŀ���ļ�ΪԴ�ļ�����ȥ��չ��".MZ"������
//        ����£����Դ�ļ�����չ������".MZ"�����׳����⡣
//        ȱʡ����£�ɾ��Դ�ļ�
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
// ���� : TCMZCompress::Putc
// ��; : ����ַ�
// ԭ�� : void Putc(int c);
// ���� : �ַ�
// ���� : ��
// ˵�� : �ú����ڴ���ʱ��ѹ��ʱ�Ĵ��������á�������ʾ��ǰ�Ľ�չ�����
//==========================================================================
void TCMZCompress::Putc(int c)
{
    if (s_m_pfunPutcHandle != NULL)
        s_m_pfunPutcHandle(c);
}

//==========================================================================
// ���� : TCMZCompress::SetPutcHandle
// ��; : ��װ�ַ�����ľ��
// ԭ�� : void SetPutcHandle(void (*pfunPutcHandle)(int c));
// ���� : ����ַ��ĺ���ָ��
// ���� : ��
// ˵�� : ��������״̬�¿ɵ���֮����װ�����������ʾ��չ״����
//==========================================================================
void TCMZCompress::SetPutcHandle(void (*pfunPutcHandle)(int c))
{
    s_m_pfunPutcHandle = pfunPutcHandle;
}

TCCompressException::TCCompressException(TCString sMsg) : TCException(sMsg)
{
} 



