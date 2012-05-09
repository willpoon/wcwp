//---------------------------------------------------------------------------
#pragma hdrstop

#include "cmpublic.h"
#include "pub_dir_file.h"
#include "errno.h"


#ifdef __WIN32__

extern "C" {

struct dirent
{
    char        d_name[260];
};

typedef struct
{
    unsigned long _d_hdir;              /* directory handle */
    char         *_d_dirname;           /* directory name */
    unsigned      _d_magic;             /* magic cookie for verifying handle */
    unsigned      _d_nfiles;            /* no. of files remaining in buf */
    char          _d_buf[255];  /* buffer for a single file */
} DIR;


DIR            _FAR * _RTLENTRY _EXPFUNC opendir  (const char _FAR *__dirname);
struct dirent  _FAR * _RTLENTRY _EXPFUNC readdir  (DIR _FAR *__dir);
int                   _RTLENTRY _EXPFUNC closedir (DIR _FAR *__dir);
}
#else
#include <dirent.h>
#endif


#pragma argsused

//---------------------------------------------------------------------------

#ifdef __WIN32__

#define F_OK 0

#endif

//==========================================================================
// ���� : ChangeFileExt
// ��; : �ı�һ���ļ�����չ��
// ԭ�� : TCString ChangeFileExt(const TCString sFileName,
//              const TCString sExtension);
// ���� : �ļ�������չ��
// ���� : �ı����չ�����ļ���
// ˵�� : ��չ����".", ��".txt"
//==========================================================================
TCString ChangeFileExt(const TCString sFileName, const TCString sExtension)
{
    long i;

    i = LastDelimiter(".\\:/", sFileName);

    if (i == 0 || sFileName[i] != '.')
        i = MAX_PATH_LEN;

    return Mid(sFileName, 1, i - 1) + sExtension;
}

//==========================================================================
// ���� : CopyFile
// ��; : �����ļ�
// ԭ�� : bool CopyFile(const TCString sSrcName, const TCString sDestName);
// ���� : Դ�ļ�����Ŀ���ļ���
// ���� : �Ƿ�ɹ�
// ˵�� : ������TCFileStream�е���غ���
//==========================================================================
bool CopyFile(const TCString sSrcName, const TCString sDestName)
{
    if (!FileExists(sSrcName))
        return false;

    long nReadLen;

    TCFileStream fsSrc, fsDest;

    fsSrc.Open(RegulatePathFile(sSrcName), omRead | omShared);

    fsDest.Open(RegulatePathFile(sDestName), omWrite | omShared);

    while ((nReadLen = fsSrc.InBufferRead(BUFFER_BLOCK)) != 0)
    {
        fsDest.Write(fsSrc.GetBuffer(), nReadLen);
    };

    return true;
}

void CopyFileE(const TCString sSrcName, const TCString sDestName)
{
    if (!CopyFile(sSrcName, sDestName))
        throw TCFileException("COPYFILE", sSrcName, sDestName, "");
}

//==========================================================================
// ���� : CopyFileToDir
// ��; : �����ļ�
// ԭ�� : bool CopyFileToDir(const TCString sSrcFileName,
//                const TCString sDestDir);
// ���� : Դ�ļ�����Ŀ��Ŀ¼��
// ���� : �Ƿ�ɹ�
// ˵�� :
//==========================================================================
bool CopyFileToDir(const TCString sSrcFileName, const TCString sDestDir)
{
    TCString sDestFileName;

    sDestFileName = MergePathAndFile(sDestDir, ExtractFileName(sSrcFileName));

    return CopyFile(sSrcFileName, sDestFileName);
}

void CopyFileToDirE(const TCString sSrcFileName, const TCString sDestDir)
{
    if (!CopyFileToDir(sSrcFileName, sDestDir))
        throw TCFileException("COPYFILETODIR", sSrcFileName, sDestDir, "");
}

//==========================================================================
// ���� : CreateBlankFile
// ��; : ����һ�����ļ�(�ֽ���Ϊ0)
// ԭ�� : void CreateBlankFile(const TCString sFileName);
// ���� : �ļ���
// ���� : ��
// ˵�� :
//==========================================================================
void CreateBlankFile(const TCString sFileName)
{
    TCString sPath;

    sPath = ExtractFilePath(sFileName);

    if (!ForceDirectories(sPath))
        throw TCFileException("CREATEBLANKFILE", sFileName, "", "");

    TCFileStream fsA;

    fsA.Open(RegulatePathFile(sFileName), omWrite|omExclusive);

    fsA.Close();
}

//==========================================================================
// ���� : CreateDir
// ��; : ����Ŀ¼
// ԭ�� : bool CreateDir(const TCString sDirName);
// ���� : Ŀ¼��
// ���� : �����Ƿ�ɹ�
// ˵�� : �ú�������һ�ν������Ŀ¼, �������Ŀ¼�Ѿ�����Ҳ���������һ��
//        ����½�����ú���ForceDirectories
//==========================================================================
bool CreateDir(const TCString sDirName)
{
    TCString sNDirName;

    sNDirName = ExcludeTrailingSlash(sDirName);
    umask(002);//Added by HMY.2002-12-02
#ifdef __WIN32__
    if (mkdir((char *)RegulatePathFile(sNDirName)) == 0)
#else
    if (mkdir((char *)RegulatePathFile(sNDirName),0777) == 0)
#endif
    {
    	umask(022);//Added by HMY.2002-12-02
        return true;
    }
    else
    {
    	umask(022);//Added by HMY.2002-12-02
        return false;
    }
}

void CreateDirE(const TCString sDirName)
{
    if (!CreateDir(sDirName))
        throw TCFileException("CREATEDIR", sDirName, "", "");
}

//==========================================================================
// ���� : CutFileExt
// ��; : �ص�һ���ļ�����չ���������ļ������ಿ��
// ԭ�� : TCString CutFileExt(const TCString sFileName)
// ���� : �ļ���
// ���� : �ص���չ�����ļ���
// ˵�� : ����ļ�����·�������·��һ�𷵻�
//==========================================================================
TCString CutFileExt(const TCString sFileName)
{
    long i;
    TCString sRet;

    i = LastDelimiter(".\\:/", sFileName);

    if ( i > 0 && sFileName[i] == '.')
    {   sRet = Left(sFileName, i - 1);
        return sRet;
    }
    else
        return sFileName;
}

//==========================================================================
// ���� : DeleteFile
// ��; : ɾ���ļ�
// ԭ�� : bool DeleteFile(const TCString sFileName);
// ���� : Ҫɾ�����ļ���
// ���� : �Ƿ�ɹ�
// ˵�� : 
//==========================================================================
bool DeleteFile(const TCString sFileName)
{
    if (remove(RegulatePathFile(sFileName)) == 0)
        return true;
    else
        return false;
}

void DeleteFileE(const TCString sFileName)
{
    if (!DeleteFile(sFileName))
        throw TCFileException("DELETE", sFileName, "", "");
}

//==========================================================================
// ���� : DirectoryExists
// ��; : �ж�һ��Ŀ¼�Ƿ����
// ԭ�� : bool DirectoryExists(TCString sDirName);
// ���� : Ŀ¼��
// ���� : �Ƿ����
// ˵�� :
//==========================================================================
bool DirectoryExists(TCString sDirName)
{
    return (access(RegulatePathFile(ExcludeTrailingSlash(sDirName)),
            F_OK) == 0);
}

//--------------------------------------------------------------------------
// ö�� : TEFileMode
// ��; : �ļ���дģʽ
//--------------------------------------------------------------------------
enum TEFileMode
{
  fmXother=1,             //�����û���ִ��
  fmWother=2,             //�����û���д
  fmRother=4,             //�����û��ɶ�
  fmRWXother=7,           //�����û��ɶ�����д����ִ��
  fmXgroup=8,             //���û���ִ��
  fmWgroup=16,            //���û���д
  fmRgroup= 32,           //���û��ɶ�
  fmRWXgroup=56,          //���û��ɶ�����д����ִ��
  fmXuser=64,             //�ļ�������ִ��
  fmWuser=128,            //�ļ�������д
  fmRuser=256,            //�ļ������ɶ�
  fmRWXuser=448,          //�ļ������ɶ�����д����ִ��
  fmRall=292,             //�ļ����������û��������û����ɶ� ��00444
  fmWall=146,             //�ļ����������û��������û�����д ��00222
  fmRWall=438,            //�ļ����������û��������û����ɶ�����д����00666
  fmRWXall=511            //�ļ����������û��������û����ɶ�����д����ִ��
                          //��00777
};

//=====================================================================
// ���� : EnableReadFile
// ��; : ׷���ļ��ɶ���ʽ
// ԭ�� : bool EnableReadFile(TCString sFileName, char cAppend='A')
// ���� : �ļ�����׷��ģʽ
// ���� : �Ƿ�ɹ�
// ˵�� ������cAppend ׷��ģʽ�� U--user,G--group,O--Other,A--AllUser
//        �����ÿɶ�
//====================================================================
bool EnableReadFile(const TCString sFileName,const char cAppend)
{
    TCString sRegulateFile;

    sRegulateFile = RegulatePathFile(sFileName);

    if ( !FileExists(sRegulateFile))
        return false;

    struct stat stbuf;
    int nAppendMode;

    if (stat((char *) sRegulateFile, &stbuf) != 0)
        return false;

#ifdef __WIN32__
    switch (cAppend)
    {
        case 'U':
        case 'G':
        case 'O':
        case 'A':
            nAppendMode = stbuf.st_mode | fmRuser;
            break;
        default :
            return false;
    }
    return ( chmod((char *) sRegulateFile,nAppendMode)==0);
#else
    switch (cAppend)
    {
        case 'U':
            nAppendMode = stbuf.st_mode | fmRuser;
            break;
        case 'G':
            nAppendMode = stbuf.st_mode | fmRgroup;
            break;
        case 'O':
            nAppendMode = stbuf.st_mode | fmRother;
            break;
        case 'A':
            nAppendMode = stbuf.st_mode | fmRall;
            break;
        default :
            return false;
    }
    return ( chmod((char *) sRegulateFile,nAppendMode)==0);
#endif
}
//=====================================================================
// ���� : EnableReadFileE
// ��; : ׷���ļ��ɶ���ʽ
// ԭ�� : void EnableReadFileE(TCString sFileName, char cAppend='A')
// ���� : �ļ�����׷��ģʽ
// ���� : ��
// ˵�� ������cAppend ׷��ģʽ�� U--user,G--group,O--Other,A--AllUser
//        �����ÿɶ���������ɹ���������
//====================================================================
void EnableReadFileE(const TCString sFileName,const char cAppend)
{
    if (! EnableReadFile(sFileName,cAppend))
        throw TCException("EnableReadFile " + sFileName + ":" + cAppend);
}
//=====================================================================
// ���� : EnableWriteFileE
// ��; : ׷���ļ���д��ʽ
// ԭ�� : bool EnableWriteFile(TCString sFileName, char cAppend='A')
// ���� : �ļ�����׷��ģʽ
// ���� : �Ƿ�ɹ�
// ˵�� ������cAppend ׷��ģʽ�� U--User,G--Group,O--Other,A--AllUser
//        ��׷�����ÿ�д
//====================================================================
bool EnableWriteFile(const TCString sFileName,const char cAppend)
{
    TCString sRegulateFile;
    sRegulateFile = RegulatePathFile(sFileName);

    if ( !FileExists(sRegulateFile))
        return false;

    struct stat stbuf;
    int nAppendMode;

    if (stat((char *) sRegulateFile, &stbuf) != 0)
        return false;

#ifdef __WIN32__
    switch (cAppend)
    {
        case 'U':
        case 'G':
        case 'O':
        case 'A':
            nAppendMode = stbuf.st_mode | fmWuser;
            break;
        default :
            return false;
    }
    return ( chmod((char *) sRegulateFile,nAppendMode)==0);
#else
    switch (cAppend)
    {
        case 'U':
            nAppendMode = stbuf.st_mode | fmWuser;
            break;
        case 'G':
            nAppendMode = stbuf.st_mode | fmWgroup;
            break;
        case 'O':
            nAppendMode = stbuf.st_mode | fmWother;
            break;
        case 'A':
            nAppendMode = stbuf.st_mode | fmWall;
            break;
        default :
            return false;
    }
    return ( chmod((char *) sRegulateFile,nAppendMode)==0);
#endif
}

//=====================================================================
// ���� : EnableWriteFileE
// ��; : ׷���ļ���д��ʽ
// ԭ�� : void EnableWriteFile(TCString sFileName, char cAppend='A')
// ���� : �ļ�����׷��ģʽ
// ���� : ��
// ˵�� ������cAppend ׷��ģʽ�� U--User,G--Group,O--Other,A--AllUser
//        ��׷�����ÿ�д��������ɹ���������
//====================================================================
void EnableWriteFileE(const TCString sFileName,const char cAppend)
{
    if (! EnableWriteFile(sFileName,cAppend))
        throw TCException("EnableWriteFile " + sFileName + ":" + cAppend);
}

//==========================================================================
// ���� : ExcludeTrailingSlash
// ��; : ȥ���ַ���β����Ŀ¼�ָ���
// ԭ�� : TCString ExcludeTrailingSlash(const TCString S);
// ���� : �ַ���
// ���� : ȥ���ָ����Ժ���ַ���
// ˵�� :
//==========================================================================
TCString ExcludeTrailingSlash(const TCString S)
{
    TCString sRet;

    sRet = S;

    if (IsPathDelimiter(sRet, Length(sRet)))
        sRet = Left(sRet, Length(sRet) - 1);

    return sRet;
}

//==========================================================================
// ���� : IncludeTrailingSlash
// ��; : �����ַ���β����Ŀ¼�ָ���
// ԭ�� : TCString IncludeTrailingSlash(const TCString S);
// ���� : �ַ���
// ���� : ���Ϸָ����Ժ���ַ���
// ˵�� :
//==========================================================================
TCString IncludeTrailingSlash(const TCString S)
{
    TCString sRet;

    sRet = S;

    if (!IsPathDelimiter(sRet, Length(sRet)))
#ifdef __WIN32__
        sRet = sRet + "\\";
#else
        sRet = sRet + "/";
#endif

    return sRet;
}

//==========================================================================
// ���� : ExtractFileDir
// ��; : �õ�һ���ļ���Ŀ¼���֣��ò���Ŀ¼ȥ��Ŀ¼β׺,
//        �����ڽ���Ŀ¼�Ȳ�����
// ԭ�� : TCString ExtractFileDir(const TCString sFileName)
// ���� : �ļ����ַ���
// ���� : Ŀ¼��
// ˵�� :
//==========================================================================
TCString ExtractFileDir(const TCString sFileName)
{
    long i;

    i = LastDelimiter("\\:/", sFileName);

    if ( i > 1 && (sFileName[i] == '\\' || sFileName[i]== '/') &&
            (sFileName[i - 1] != ':' && sFileName[i - 1] != '/' &&
            sFileName[i - 1] != '\\'))
        i --;

    return Left(sFileName, i);
}

//==========================================================================
// ���� : ExtractFileExt
// ��; : �õ�һ���ļ�����չ������
// ԭ�� : TCString ExtractFileExt(const TCString sFileName)
// ���� : �ļ���
// ���� : ��չ��(�������չ������".txt")
// ˵�� :
//==========================================================================
TCString ExtractFileExt(const TCString sFileName)
{
    long i;
    TCString sRet;

    i = LastDelimiter(".\\:/", sFileName);

    if ( i > 0 && sFileName[i] == '.')
    {   sRet = Mid(sFileName, i);
        return sRet;
    }
    else
        return "";
}

//==========================================================================
// ���� : ExtractFileName
// ��; : �õ�һ���ļ�ȫ�����ļ�������
// ԭ�� : TCString ExtractFileName(const TCString sFileName)
// ���� : �ļ���
// ���� : �ļ�ȫ��
// ˵�� :
//==========================================================================
TCString ExtractFileName(const TCString sFileName)
{
    long i;

    i = LastDelimiter("\\:/", sFileName);

    return Mid(sFileName, i + 1);
}

//==========================================================================
// ���� : ExtractFilePath
// ��; : �õ�һ���ļ�����Ŀ¼����
// ԭ�� : TCString ExtractFilePath(const TCString sFileName)
// ���� : �ļ���
// ���� : Ŀ¼�ַ���
// ˵�� :
//==========================================================================
TCString ExtractFilePath(const TCString sFileName)
{
    long i;

    i = LastDelimiter("\\:/", sFileName);

    return Left(sFileName, i);
}

//==========================================================================
// ���� : FileExists
// ��; : �ж�һ���ļ��Ƿ����
// ԭ�� : bool FileExists(TCString sFileName)
// ���� : �ļ���
// ���� : �Ƿ����
// ˵�� :
//==========================================================================
bool FileExists(TCString sFileName)
{
    return (access(RegulatePathFile(sFileName), F_OK) == 0);
}

//==========================================================================
// ���� : FileGetTime
// ��; : �õ�һ���ļ�������䶯����
// ԭ�� : TCString FileGetTime(TCString sFileName);
// ���� : �ļ���
// ���� : ����䶯����
// ˵�� :
//==========================================================================
TCString FileGetTime(TCString sFileName)
{
   struct stat statbuf;

   stat(sFileName, &statbuf);

   return TCTime::GetDatetimeStringByTimeT(statbuf.st_mtime);
}

//==========================================================================
// ���� : FileSize
// ��; : �õ�һ���ļ��Ĵ�С
// ԭ�� : long FileSize(TCString sFileName);
// ���� : �ļ���
// ���� : �ļ���С
// ˵�� :
//==========================================================================
long FileSize(TCString sFileName)
{
   struct stat statbuf;

   stat(sFileName, &statbuf);

   return statbuf.st_size;
}

//==========================================================================
// ���� : ForceDirectories
// ��; : ���Ŀ¼�����ڣ�����Ŀ¼·����������Ŀ¼
// ԭ�� : bool ForceDirectories(const TCString sDirName);
// ���� : ·����
// ���� : �Ƿ�ɹ�
// ˵�� :
//==========================================================================
bool ForceDirectories(const TCString sDirName)
{
    if (!IsAbsolutePathName(sDirName))
        return false;

    bool bRet;
    TCString sMkDir;

    bRet = true;

    ASSERT(Length(sDirName) != 0);

    sMkDir = ExcludeTrailingSlash(sDirName);

    if (Length(sMkDir) < 2 || DirectoryExists(sMkDir)
            || ExtractFilePath(sMkDir) == sMkDir)
        return bRet;

    return ForceDirectories(ExtractFilePath(sMkDir)) && CreateDir(sMkDir);
}

void ForceDirectoriesE(const TCString sDirName)
{
    if (!ForceDirectories(sDirName))
        throw TCFileException("FORCEDIRECTORIES", sDirName, "", "");
}

//==========================================================================
// ���� : IsAbsolutePathName
// ��; : �ж�һ��·�����Ƿ��Ǿ���·����
// ԭ�� : bool IsAbsolutePathName(const TCString S);
// ���� : ·����
// ���� : �Ƿ����·��
// ˵�� :
//==========================================================================
bool IsAbsolutePathName(const TCString S)
{
    if (IsPathDelimiter(S, 1))
        return true;

    if (Mid(AllTrim(S), 2, 1) == TCString(":") )
        return true;

    return false;
}

//==========================================================================
// ���� : IsPathDelimiter
// ��; : һ���ַ����е�ָ��λ���ַ��Ƿ���Ŀ¼�ָ���
// ԭ�� : bool IsPathDelimiter(const TCString S, long Index);
// ���� : �ַ�����ָ��λ��
// ���� : �Ƿ�Ŀ¼�ָ���
// ˵�� :
//==========================================================================
bool IsPathDelimiter(const TCString S, long Index)
{
    return (Index > 0) && (Index <= Length(S))
            && (S[Index] == '\\' || S[Index] == '/');
}

//==========================================================================
// ���� : MergePathAndFile
// ��; : �ϲ�Ŀ¼�����ļ����������ļ�ȫ��
// ԭ�� : TCString MergePathAndFile(TCString sPath, TCString sFile)
// ���� : Ŀ¼�����ļ���
// ���� : �ļ�ȫ��
// ˵�� :
//==========================================================================
TCString MergePathAndFile(TCString sPath, TCString sFile)
{
    TCString sPathFile;

    // ���·�����ļ�û��ָ������ֱ�Ӽ���
    if (sPath == TCString("") || sFile == TCString("") )
        return sPath + sFile;

    sPathFile = ExcludeTrailingSlash(sPath);

#ifdef __WIN32__
    sPathFile = sPathFile + "\\";
#else
    sPathFile = sPathFile + "/";
#endif

    sPathFile = sPathFile + sFile;

    return sPathFile;
}

//==========================================================================
// ���� : MoveFile
// ��; : �ƶ��ļ�
// ԭ�� : bool MoveFile(TCString sFileName, TCString sNewPath,
//              bool bWithOverWrite = false);
// ���� : �ļ�����Ҫ�ƶ�����·�����Ƿ������Ǿ��ļ�
// ���� : �Ƿ�ɹ�
// ˵�� :
//==========================================================================
bool MoveFile(TCString sFileName, TCString sNewPath, bool bWithOverWrite)
{
    ASSERT(Length(sNewPath) != 0);

    TCString sNewName;

    sNewName = MergePathAndFile(sNewPath, ExtractFileName(sFileName));

    if (RenameFile(RegulatePathFile(sFileName), RegulatePathFile(sNewName)))
        return true;

    if (bWithOverWrite)
    {
        if (!CopyFile(sFileName, sNewName))
            return false;

        DeleteFile(sFileName);

        return true;
    }
    else
        return false;
}

void MoveFileE(TCString sFileName, TCString sNewPath, bool bWithOverWrite)
{
    TCString sMessage;
    if (bWithOverWrite)
        sMessage = "Overwrite Exists File While Moving File";
    else
        sMessage = "Do not Overwrite Exists File While Moving File";

    if (!MoveFile(sFileName, sNewPath, bWithOverWrite))
        throw TCFileException("MOVEFILE", sFileName, sNewPath, sMessage);
}

//==========================================================================
// ���� : RegulatePathFile
// ��; : ���ļ���ת���ɷ���ϵͳҪ����ļ���
// ԭ�� : TCString RegulatePathFile(TCString sPathFileName);
// ���� : �ļ���
// ���� : ת�������ļ���
// ˵�� :
//==========================================================================
TCString RegulatePathFile(TCString sPathFileName)
{
    long i;

    for (i = 1; i <= Length(sPathFileName); i++)
    {
#ifdef __WIN32__
        if (sPathFileName[i] == '/')
            sPathFileName.SetAt(i, '\\');
#else
        if (sPathFileName[i] == '\\')
            sPathFileName.SetAt(i, '/');
#endif
    }

    return sPathFileName;
}

//==========================================================================
// ���� : RenameFile
// ��; : �ı��ļ���
// ԭ�� : bool RenameFile(TCString sOldName, TCString sNewName,
//              bool bWithOverWrite = false);
// ���� : ���ļ��������ļ���
// ���� : �Ƿ�ɹ�
// ˵�� :
//==========================================================================
bool RenameFile(TCString sOldName, TCString sNewName, bool bWithOverWrite)
{
    ASSERT(sOldName != sNewName);

    if (bWithOverWrite)
        if (FileExists(sNewName))
            DeleteFile(sNewName);

    if (rename(RegulatePathFile(sOldName), RegulatePathFile(sNewName)) == 0)
    {
        return true;
    }
    else
    {
    	perror("error");
    	printf("The Error no [%s] ->[%s]:%d \n",(char*)sOldName,(char*)sNewName,errno);
        if (bWithOverWrite)
        {
            if (!CopyFile(sOldName, sNewName))
                return false;

            DeleteFile(sOldName);

            return true;
        }
        else
            return false;
    }
}

void RenameFileE(TCString sOldName, TCString sNewName, bool bWithOverWrite)
{
    TCString sMessage;
    if (bWithOverWrite)
        sMessage = "Overwrite Exists File While Moving File";
    else
        sMessage = "Do not Overwrite Exists File While Moving File";

    if (!RenameFile(sOldName, sNewName, bWithOverWrite))
        throw TCFileException("RENAMEFILE", sOldName, sNewName, sMessage);
}

//==========================================================================
// ���� : RemoveDir
// ��; : ɾ��Ŀ¼
// ԭ�� : bool RemoveDir(TCString sDirectoryName);
// ���� : ָ����Ŀ¼��
// ���� : �Ƿ�ɹ�
// ˵�� : ��Ŀ¼�����ǿ�Ŀ¼(�����κ��ļ�)
//==========================================================================
bool RemoveDir(TCString sDirectoryName)
{
    TCString sRDirName;

    sRDirName = RegulatePathFile(ExcludeTrailingSlash(sDirectoryName));

#ifdef __WIN32__
    if (rmdir(sRDirName) == 0)
        return true;
    else
        return false;
#else
    if (remove(sRDirName) == 0)
        return true;
    else
        return false;
#endif
}

void RemoveDirE(TCString sDirectoryName)
{
    if (!RemoveDir(sDirectoryName))
    {
        throw TCFileException("REMOVEDIR", sDirectoryName, "", "");
    }
}

//==========================================================================
// ���� : GetDirFileList
// ��; : ��ȡһ��Ŀ¼�����ָ���ļ����͵������ļ���֧��ͨ���
// ԭ�� : void GetDirFileList(TCStringList& slFileName, TCString sPath ,
//                     TCString sFilterStr,bool IsIncSubDir = false,
//                     bool IsIncPath = false);
// ���� : �ַ����б�·�����ļ��� ���Ƿ������Ŀ¼���Ƿ���Ҫ��·�����ļ���
// ���� : ��
// ˵�� : ע�⣺���������еݹ���ã����Դ������� StringList ��ҪԤ�����
//==========================================================================
void GetDirFileList(TCStringList& slFileName, TCString sPath ,
                     TCString sFilterStr,bool IsIncSubDir,
                     bool IsIncPath )
{
  DIR *pDir;
  struct dirent * pDirent;
  int i;
  struct stat sBuf;

  TCStringList slDirList;
/*
#ifdef _DEBUG
  printf("Path = %s ; Filter = %s\n",(char *)sPath,(char *)sFilterStr);
#endif
*/
  sFilterStr.TrimRight();
  sFilterStr.TrimLeft();

  sPath = RegulatePathFile(sPath);


  pDir = opendir(sPath);
  if (pDir != NULL)
  {
    while ((pDirent = readdir(pDir)) != NULL)
    {
      if ((strcmp(pDirent->d_name,".")== 0) || (strcmp(pDirent->d_name,"..")== 0))
      {
        continue;
      }

      //��ȡ�ļ����ԣ�ʶ���Ƿ�ΪĿ¼
      if (stat(MergePathAndFile(sPath,TCString(pDirent->d_name)),&sBuf) == -1)
      {
        continue;
      }

//      if ((S_ISDIR(sBuf.st_mode)) && (sBuf.st_size == 0))
      //�ж��Ƿ�ΪĿ¼
      if (S_ISDIR(sBuf.st_mode))
      {
        if (IsIncSubDir)
        {
          slDirList.Add(TCString(pDirent->d_name));
        }
        else
        {
          //�����Ŀ¼������
          continue;
        }
      }
       if (FilterStr(pDirent->d_name,sFilterStr))
       {
            if (IsIncPath)
              slFileName.Add(MergePathAndFile(sPath,TCString(pDirent->d_name)));
            else
              slFileName.Add(TCString(pDirent->d_name));
       }
    }
    closedir(pDir);

    //���������Ŀ¼�͵ݹ�������ļ�����
    if (IsIncSubDir)
    {
      for (i=0; i < slDirList.GetCount(); i ++)
      {
        GetDirFileList(slFileName, MergePathAndFile(sPath,
                       slDirList[i]) ,sFilterStr,IsIncSubDir,IsIncPath);

      }
    }

  }
}

//==========================================================================
// ���� : GetDirDirList
// ��; : ��ȡһ��Ŀ¼����ķ���ָ��ƥ�䴮�������ļ�
// ԭ�� : void GetDirDirList(TCStringList& slDirNames, TCString sPath,
//                     TCString sFilterStr, bool IsIncPath = false)
// ���� : �ַ����б�·����ƥ�䴮���Ƿ���Ҫ��·����Ŀ¼��
// ���� : ��
// ˵�� : �ú���������ַ����б������ٽ������䡣
//==========================================================================
void GetDirDirList(TCStringList& slDirNames, TCString sPath,
                     TCString sFilterStr, bool IsIncPath )
{
    //===== 1. ���Ŀ¼LIST ======
    slDirNames.Clear();

    //===== 2. ��·�� =====
    DIR *pDir;
    sPath = RegulatePathFile(sPath);

    pDir = opendir(sPath);
    if (pDir == NULL)
        return;

    //==== 3. ѭ����ȡĿ¼���� ========
    struct dirent * pDirent;

    struct stat statBuf;

    while ((pDirent = readdir(pDir)) != NULL)
    {
        //===== 4. �����"."��"..", ���������ļ� =====
        if ((strcmp(pDirent->d_name,".")== 0)
                || (strcmp(pDirent->d_name,"..")== 0))
            continue;

        //==== 5. ��ȡ�ļ����� =======
        if (stat(MergePathAndFile(sPath,TCString(pDirent->d_name)),
                &statBuf) == -1)
        {
            continue;
        }

        //====== 6. �ж��Ƿ�ΪĿ¼�ַ��Ϲ��˴�, �����List�� ====
        if (S_ISDIR(statBuf.st_mode))
            if (FilterStr(pDirent->d_name, sFilterStr))
                if (IsIncPath)
                    slDirNames.Add(MergePathAndFile(sPath,
                            TCString(pDirent->d_name)));
                else
                    slDirNames.Add(TCString(pDirent->d_name));
    }   // end of while ((pDirent = readdir(pDir)) != NULL)

    //======== 7. �ر�·�� =====
    closedir(pDir);
}

#ifdef __TEST__

void TestGetDirDirListFunc()
{
    char sPath[256], sFilter[256];
    char cIncPath;

    printf("Input the Path to Get Dir : ");
    scanf("%s", sPath);

    printf("Input the Filter : ");
    scanf("%s", sFilter);

    fflush(stdin);

    printf("Want Full Path Name (N)? ");
    scanf("%c", &cIncPath);

    fflush(stdin);

    TCStringList slDirNames;
    bool bIncPath;

    if (cIncPath == 'Y' || cIncPath == 'y')
        bIncPath = true;
    else
        bIncPath = false;

    GetDirDirList(slDirNames, sPath, sFilter, bIncPath);

    long i;
    for (i = 0; i < slDirNames.GetCount(); i++)
        printf("%s\n", (char *)slDirNames[i]);

    getchar();
}

#endif

bool FilterStr(const TCString sFileName,const TCString sFilter)
{
//   ASSERT( sFileName != "" ) ;
    int nFilterLen,nFileNameLen ;
    int nPos;
    int nPosFilter,nPosFileName;

    char Token, NextToken;

    if ( (sFilter == TCString("") ) && (sFileName == TCString("") ))
        return true;

    nFilterLen = sFilter.GetLength() ;
    nFileNameLen = sFileName.GetLength() ;

    Token = sFilter[1];

    if (nFilterLen > 1)
        NextToken = sFilter[2];
    else
        NextToken = '\0';

    switch (Token)
    {
        case '*' :
            switch (NextToken)
            {
                case '*' :
                    //������������� ��*���ͽ�ȥһ����*��
                    nPosFilter = 2;
                    nPosFileName = 1;
                    break;
                case '?' :
                    //������ȷ���� *?*? ����������
                    nPosFilter = 3;
                    nPosFileName = 2;
                    break;
                case '\0' :
                    //������һ��Ϊ ��*���򷵻���
                    return true;
                default :
                    //���Ϊ��ͨ�ַ���������.���Ͳ�����Ӧ�� �ַ���
                    if ((nPos = sFileName.Find(sFilter[2])) != -1)
                    {
                        nPosFilter = 3;
                        nPosFileName = nPos + 1;
                    }
                    else
                    {
                        return false ;
                    }
                    break;
            }
            break;

        case '?' :

            if ((nFilterLen == 1) && (nFileNameLen == 1))
            {
                //����������������һ������ʱ��ͷ��سɹ�
                return true;
            }
            else
            {
                //����ָ������
                nPosFilter = 2;
                nPosFileName = 2;
            }
            break;

        default :

            if (sFileName == sFilter)
            {
                //����ַ�����Ⱦͷ�����
                return true;
            }
            else
            {
              if (sFileName[1] != sFilter[1])
              {
                  //�����ͨ�ַ���ƥ��ͷ���ʧ��
                  return false;
              }
              else
              {
                  //ָ������
                  nPosFilter = 2;
                  nPosFileName = 2;
              }
            }
            break;

    }
    if ((nPosFileName <= nFileNameLen) && (nPosFilter <= nFilterLen))
    {
        //��û�г����ַ�����Ч���Ⱦ͵ݹ����
        return FilterStr(sFileName.Mid(nPosFileName,nFileNameLen - nPosFileName + 1),
            sFilter.Mid(nPosFilter,nFilterLen - nPosFilter + 1)) ;
    }
    else
    {
        //�������ַ�����Ч���Ⱦͷ���ʧ��
        return false;
    }
}

//==========================================================================
// ���� : DelTree
// ��; : ����Ŀ¼ɾ��Ŀ¼
// ԭ�� : void DelTree(const TCString & sDirectoryName, bool bReserveDirectory = false);
// ���� : Ŀ¼�����Ƿ���Ŀ¼
// ���� : ��
// ˵�� :
// ��ʷ : 2001.11.19 ����bReserveDirectory����
//==========================================================================
void DelTree(const TCString & sDirectoryName, bool bReserveDirectory)
{
    TCStringList slDirList, slFileList;
    long i;

    GetDirDirList(slDirList, sDirectoryName, "*", true);
    for (i = 0; i < slDirList.GetCount(); i++)
    {
        DelTree(slDirList[i], bReserveDirectory);
        // RemoveDirE(slDirList[i]);    // ע�͵�2001.9.29 BUG OLDIX
    }

    GetDirFileList(slFileList, sDirectoryName, "*", false, true);
    for (i = 0; i < slFileList.GetCount(); i++)
        DeleteFileE(slFileList[i]);

    if (!bReserveDirectory)
        RemoveDirE(sDirectoryName);
}

//==========================================================================
// ��ʱ�ļ���������(TMPFile...)��ʹ��˵��:
//     TMPFile����������ʱ�ļ������㴦����ʱ�ļ������������������⡣
//     ����������Ϊ: TMPFileGetByFileName��TMPFileGetByDirName��TMPFileCommit
//
// �����м��ֵ���˳��:
// (1) sTmpFile = TMPFileGetByFileName(sFile);
//     <Process sTmpFile>
//     TMPFileCommit(sTmpFile);
// (2) sTmpFile = TMPFileGetByFileName(sFile);
//     <Process sTmpFile>
//     TMPFileCommit(sFile);     ------- ������������˵�����ύʱ�ȿ����ύ
//                                       ��ʱ�ļ����ֿ����ύ����ʱ�ļ�
// (3) sTmpFile = TMPFileGetByDirName(sDir);
//             ------ Ҳ����ͨ��������ʽ�õ���ʱ�ļ�����ʱ�ļ�Ҳ��������
//                    ��Ŀ¼����Ҫ���ܹ���������(��ͬһ���ļ�ϵͳ)��
//     <Process sTmpFile>
//     TMPFileCommit(sTmpFile, sActuallyFileName);
//
// ע�� : TMPFileCommit()���Ŀ���ļ����ڣ�����ø��Ƿ�ʽ������Ҫ�ж�Ŀ����
//        ���Ĵ������������Ҫ�����д���롣
//==========================================================================

//==========================================================================
// ���� : TMPFileGetByFileName
// ��; : ����һ���ļ���Ӧ����ʱ�ļ���
// ԭ�� : TCString TMPFileGetByFileName(TCString sPathFileName);
// ���� : �ļ���
// ���� : ��Ӧ����ʱ�ļ���
// ˵�� :
//==========================================================================
TCString TMPFileGetByFileName(TCString sPathFileName)
{
    TCString sDirName;
    TCString sFileName;

    sDirName = ExtractFilePath(sPathFileName);
    sFileName = ExtractFileName(sPathFileName);

    sFileName = "__" + sFileName + "!!!";

    return MergePathAndFile(sDirName, sFileName);
}

//==========================================================================
// ���� : TMPFileGetByDirName
// ��; : ����Ŀ¼�µ�һ����ʱ�ļ���
// ԭ�� : TCString TMPFileGetByDirName(TCString sDirName);
// ���� : Ŀ¼��
// ���� : ��ʱ�ļ���
// ˵�� :
//==========================================================================
TCString TMPFileGetByDirName(TCString sDirName)
{
    TCString sFileName;

    sFileName = "__" + TCTime::Now() + "." + IntToStr(RandLong(9999)) + "!!";

    return MergePathAndFile(sDirName, sFileName);
}

//==========================================================================
// ���� : TMPFileCommit
// ��; : �ύ��ʱ�ļ�
// ԭ�� : TCString TMPFileCommit(TCString sTmpFileName,
//                  TCString sNormalFileName = "");
// ���� : ��ʱ�ļ�������ͨ�ļ���
// ���� : ��
// ˵�� :
//==========================================================================
TCString TMPFileCommit(TCString sTmpFileName, TCString sNormalFileName)
{
    if (sNormalFileName != TCString("") )
    {
        if (FileExists(sNormalFileName))
            DeleteFileE(sNormalFileName);
        RenameFileE(sTmpFileName, sNormalFileName);
        return sNormalFileName;
    }

    TCString sSrcFileName, sDestFileName;
    TCString sFilePureName, sFilePurePath;
    sFilePureName = ExtractFileName(sTmpFileName);
    sFilePurePath = ExtractFilePath(sTmpFileName);

    if (Left(sFilePureName, 2) == TCString("__") && Right(sFilePureName, 3) == TCString("!!!") )
    {
        sSrcFileName = sTmpFileName;
        sDestFileName = MergePathAndFile(sFilePurePath,
                Mid(sFilePureName, 3, Length(sFilePureName) - 5));
    }
    else
    {
        sSrcFileName = MergePathAndFile(sFilePurePath,
                "__" + sFilePureName + "!!!");
        sDestFileName = sTmpFileName;
    }
    if (!FileExists(sSrcFileName))
        throw TCFileException("TMPFileCommit", sTmpFileName, sNormalFileName,
                "Temp file does not exist. Temp file name = " + sSrcFileName);
    if (FileExists(sDestFileName))
        DeleteFileE(sDestFileName);
    RenameFileE(sSrcFileName, sDestFileName);

    return sDestFileName;
}

//==========================================================================
// ���� : TCFileException::TCFileException
// ��; : �ļ���������Ĺ��캯��
// ԭ�� : TCFileException(TCString sOperation, TCString sSrcName = "",
//              TCString sDestName = "", TCString sMsg = "");
// ���� : ����, Դ����Ŀ��������Ϣ��
// ���� : ��
// ˵�� :
//==========================================================================
TCFileException::TCFileException(TCString sOperation, TCString sSrcName,
        TCString sDestName, TCString sMsg) : TCException(sMsg)
{
    m_sOperation = UpperCase(sOperation);
    m_sSrcName = sSrcName;
    m_sDestName = sDestName;

    if (sMsg == TCString("") )
        sMsg = TCSystem::SysErrorMessage(TCSystem::GetLastError());

    m_sMessage = "File & Directory Operation Exception : \n"
            "Operation   : " + sOperation + "\n"
            "Source Name : " + sSrcName + "\n"
            "Dest Name   : " + sDestName + "\n"
            "Message     : " + sMsg;
}





