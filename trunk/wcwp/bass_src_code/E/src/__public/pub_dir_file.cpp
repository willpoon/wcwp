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
// 函数 : ChangeFileExt
// 用途 : 改变一个文件的扩展名
// 原型 : TCString ChangeFileExt(const TCString sFileName,
//              const TCString sExtension);
// 参数 : 文件名，扩展名
// 返回 : 改变过扩展名的文件名
// 说明 : 扩展名带".", 如".txt"
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
// 函数 : CopyFile
// 用途 : 拷贝文件
// 原型 : bool CopyFile(const TCString sSrcName, const TCString sDestName);
// 参数 : 源文件名，目标文件名
// 返回 : 是否成功
// 说明 : 调用了TCFileStream中的相关函数
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
// 函数 : CopyFileToDir
// 用途 : 拷贝文件
// 原型 : bool CopyFileToDir(const TCString sSrcFileName,
//                const TCString sDestDir);
// 参数 : 源文件名，目标目录名
// 返回 : 是否成功
// 说明 :
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
// 函数 : CreateBlankFile
// 用途 : 产生一个空文件(字节数为0)
// 原型 : void CreateBlankFile(const TCString sFileName);
// 参数 : 文件名
// 返回 : 无
// 说明 :
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
// 函数 : CreateDir
// 用途 : 建立目录
// 原型 : bool CreateDir(const TCString sDirName);
// 参数 : 目录名
// 返回 : 建立是否成功
// 说明 : 该函数不能一次建立多层目录, 而且如果目录已经存在也会出错。所以一般
//        情况下建议调用函数ForceDirectories
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
// 函数 : CutFileExt
// 用途 : 截掉一个文件的扩展名，返回文件的其余部分
// 原型 : TCString CutFileExt(const TCString sFileName)
// 参数 : 文件名
// 返回 : 截掉扩展名的文件名
// 说明 : 如果文件名带路径，则带路径一起返回
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
// 函数 : DeleteFile
// 用途 : 删除文件
// 原型 : bool DeleteFile(const TCString sFileName);
// 参数 : 要删除的文件名
// 返回 : 是否成功
// 说明 : 
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
// 函数 : DirectoryExists
// 用途 : 判断一个目录是否存在
// 原型 : bool DirectoryExists(TCString sDirName);
// 参数 : 目录名
// 返回 : 是否存在
// 说明 :
//==========================================================================
bool DirectoryExists(TCString sDirName)
{
    return (access(RegulatePathFile(ExcludeTrailingSlash(sDirName)),
            F_OK) == 0);
}

//--------------------------------------------------------------------------
// 枚举 : TEFileMode
// 用途 : 文件读写模式
//--------------------------------------------------------------------------
enum TEFileMode
{
  fmXother=1,             //其他用户可执行
  fmWother=2,             //其他用户可写
  fmRother=4,             //其他用户可读
  fmRWXother=7,           //其他用户可读、可写、可执行
  fmXgroup=8,             //组用户可执行
  fmWgroup=16,            //组用户可写
  fmRgroup= 32,           //组用户可读
  fmRWXgroup=56,          //组用户可读、可写、可执行
  fmXuser=64,             //文件属主可执行
  fmWuser=128,            //文件属主可写
  fmRuser=256,            //文件属主可读
  fmRWXuser=448,          //文件属主可读、可写、可执行
  fmRall=292,             //文件属主、组用户、其他用户都可读 即00444
  fmWall=146,             //文件属主、组用户、其他用户都可写 即00222
  fmRWall=438,            //文件属主、组用户、其他用户都可读、可写，即00666
  fmRWXall=511            //文件属主、组用户、其他用户都可读、可写、可执行
                          //即00777
};

//=====================================================================
// 函数 : EnableReadFile
// 用途 : 追加文件可读方式
// 原型 : bool EnableReadFile(TCString sFileName, char cAppend='A')
// 参数 : 文件名，追加模式
// 返回 : 是否成功
// 说明 ：根据cAppend 追加模式是 U--user,G--group,O--Other,A--AllUser
//        来设置可读
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
// 函数 : EnableReadFileE
// 用途 : 追加文件可读方式
// 原型 : void EnableReadFileE(TCString sFileName, char cAppend='A')
// 参数 : 文件名，追加模式
// 返回 : 无
// 说明 ：根据cAppend 追加模式是 U--user,G--group,O--Other,A--AllUser
//        来设置可读，如果不成功，抛例外
//====================================================================
void EnableReadFileE(const TCString sFileName,const char cAppend)
{
    if (! EnableReadFile(sFileName,cAppend))
        throw TCException("EnableReadFile " + sFileName + ":" + cAppend);
}
//=====================================================================
// 函数 : EnableWriteFileE
// 用途 : 追加文件可写方式
// 原型 : bool EnableWriteFile(TCString sFileName, char cAppend='A')
// 参数 : 文件名，追加模式
// 返回 : 是否成功
// 说明 ：根据cAppend 追加模式是 U--User,G--Group,O--Other,A--AllUser
//        来追加设置可写
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
// 函数 : EnableWriteFileE
// 用途 : 追加文件可写方式
// 原型 : void EnableWriteFile(TCString sFileName, char cAppend='A')
// 参数 : 文件名，追加模式
// 返回 : 无
// 说明 ：根据cAppend 追加模式是 U--User,G--Group,O--Other,A--AllUser
//        来追加设置可写，如果不成功，抛例外
//====================================================================
void EnableWriteFileE(const TCString sFileName,const char cAppend)
{
    if (! EnableWriteFile(sFileName,cAppend))
        throw TCException("EnableWriteFile " + sFileName + ":" + cAppend);
}

//==========================================================================
// 函数 : ExcludeTrailingSlash
// 用途 : 去掉字符串尾部的目录分隔符
// 原型 : TCString ExcludeTrailingSlash(const TCString S);
// 参数 : 字符串
// 返回 : 去除分隔符以后的字符串
// 说明 :
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
// 函数 : IncludeTrailingSlash
// 用途 : 加上字符串尾部的目录分隔符
// 原型 : TCString IncludeTrailingSlash(const TCString S);
// 参数 : 字符串
// 返回 : 加上分隔符以后的字符串
// 说明 :
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
// 函数 : ExtractFileDir
// 用途 : 得到一个文件的目录部分（该部分目录去除目录尾缀,
//        可用于建立目录等操作）
// 原型 : TCString ExtractFileDir(const TCString sFileName)
// 参数 : 文件名字符串
// 返回 : 目录名
// 说明 :
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
// 函数 : ExtractFileExt
// 用途 : 得到一个文件的扩展名部分
// 原型 : TCString ExtractFileExt(const TCString sFileName)
// 参数 : 文件名
// 返回 : 扩展名(带点的扩展名，例".txt")
// 说明 :
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
// 函数 : ExtractFileName
// 用途 : 得到一个文件全名的文件名部分
// 原型 : TCString ExtractFileName(const TCString sFileName)
// 参数 : 文件名
// 返回 : 文件全名
// 说明 :
//==========================================================================
TCString ExtractFileName(const TCString sFileName)
{
    long i;

    i = LastDelimiter("\\:/", sFileName);

    return Mid(sFileName, i + 1);
}

//==========================================================================
// 函数 : ExtractFilePath
// 用途 : 得到一个文件名的目录部分
// 原型 : TCString ExtractFilePath(const TCString sFileName)
// 参数 : 文件名
// 返回 : 目录字符串
// 说明 :
//==========================================================================
TCString ExtractFilePath(const TCString sFileName)
{
    long i;

    i = LastDelimiter("\\:/", sFileName);

    return Left(sFileName, i);
}

//==========================================================================
// 函数 : FileExists
// 用途 : 判断一个文件是否存在
// 原型 : bool FileExists(TCString sFileName)
// 参数 : 文件名
// 返回 : 是否存在
// 说明 :
//==========================================================================
bool FileExists(TCString sFileName)
{
    return (access(RegulatePathFile(sFileName), F_OK) == 0);
}

//==========================================================================
// 函数 : FileGetTime
// 用途 : 得到一个文件的最近变动日期
// 原型 : TCString FileGetTime(TCString sFileName);
// 参数 : 文件名
// 返回 : 最近变动日期
// 说明 :
//==========================================================================
TCString FileGetTime(TCString sFileName)
{
   struct stat statbuf;

   stat(sFileName, &statbuf);

   return TCTime::GetDatetimeStringByTimeT(statbuf.st_mtime);
}

//==========================================================================
// 函数 : FileSize
// 用途 : 得到一个文件的大小
// 原型 : long FileSize(TCString sFileName);
// 参数 : 文件名
// 返回 : 文件大小
// 说明 :
//==========================================================================
long FileSize(TCString sFileName)
{
   struct stat statbuf;

   stat(sFileName, &statbuf);

   return statbuf.st_size;
}

//==========================================================================
// 函数 : ForceDirectories
// 用途 : 如果目录不存在，沿着目录路径建立所有目录
// 原型 : bool ForceDirectories(const TCString sDirName);
// 参数 : 路径名
// 返回 : 是否成功
// 说明 :
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
// 函数 : IsAbsolutePathName
// 用途 : 判断一个路径名是否是绝对路径名
// 原型 : bool IsAbsolutePathName(const TCString S);
// 参数 : 路径名
// 返回 : 是否绝对路径
// 说明 :
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
// 函数 : IsPathDelimiter
// 用途 : 一个字符串中的指定位置字符是否是目录分隔符
// 原型 : bool IsPathDelimiter(const TCString S, long Index);
// 参数 : 字符串，指定位置
// 返回 : 是否目录分隔符
// 说明 :
//==========================================================================
bool IsPathDelimiter(const TCString S, long Index)
{
    return (Index > 0) && (Index <= Length(S))
            && (S[Index] == '\\' || S[Index] == '/');
}

//==========================================================================
// 函数 : MergePathAndFile
// 用途 : 合并目录名及文件名，生成文件全名
// 原型 : TCString MergePathAndFile(TCString sPath, TCString sFile)
// 参数 : 目录名，文件名
// 返回 : 文件全名
// 说明 :
//==========================================================================
TCString MergePathAndFile(TCString sPath, TCString sFile)
{
    TCString sPathFile;

    // 如果路径或文件没有指定，则直接加起
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
// 函数 : MoveFile
// 用途 : 移动文件
// 原型 : bool MoveFile(TCString sFileName, TCString sNewPath,
//              bool bWithOverWrite = false);
// 参数 : 文件名，要移动到的路径，是否允许覆盖旧文件
// 返回 : 是否成功
// 说明 :
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
// 函数 : RegulatePathFile
// 用途 : 将文件名转换成符合系统要求的文件名
// 原型 : TCString RegulatePathFile(TCString sPathFileName);
// 参数 : 文件名
// 返回 : 转换过的文件名
// 说明 :
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
// 函数 : RenameFile
// 用途 : 改变文件名
// 原型 : bool RenameFile(TCString sOldName, TCString sNewName,
//              bool bWithOverWrite = false);
// 参数 : 旧文件名，新文件名
// 返回 : 是否成功
// 说明 :
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
// 函数 : RemoveDir
// 用途 : 删除目录
// 原型 : bool RemoveDir(TCString sDirectoryName);
// 参数 : 指定的目录名
// 返回 : 是否成功
// 说明 : 本目录必须是空目录(不含任何文件)
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
// 函数 : GetDirFileList
// 用途 : 获取一个目录下面的指定文件类型的所有文件，支持通配符
// 原型 : void GetDirFileList(TCStringList& slFileName, TCString sPath ,
//                     TCString sFilterStr,bool IsIncSubDir = false,
//                     bool IsIncPath = false);
// 参数 : 字符串列表，路径，文件名 ，是否包含子目录，是否需要带路径的文件名
// 返回 : 无
// 说明 : 注意：由于里面有递归调用，所以传进来的 StringList 需要预先清空
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

      //获取文件属性，识别是否为目录
      if (stat(MergePathAndFile(sPath,TCString(pDirent->d_name)),&sBuf) == -1)
      {
        continue;
      }

//      if ((S_ISDIR(sBuf.st_mode)) && (sBuf.st_size == 0))
      //判断是否为目录
      if (S_ISDIR(sBuf.st_mode))
      {
        if (IsIncSubDir)
        {
          slDirList.Add(TCString(pDirent->d_name));
        }
        else
        {
          //如果是目录就跳过
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

    //如果包含子目录就递归调用列文件函数
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
// 函数 : GetDirDirList
// 用途 : 获取一个目录下面的符合指定匹配串的所有文件
// 原型 : void GetDirDirList(TCStringList& slDirNames, TCString sPath,
//                     TCString sFilterStr, bool IsIncPath = false)
// 参数 : 字符串列表，路径，匹配串，是否需要带路径的目录名
// 返回 : 无
// 说明 : 该函数会清空字符串列表，无需再将清空语句。
//==========================================================================
void GetDirDirList(TCStringList& slDirNames, TCString sPath,
                     TCString sFilterStr, bool IsIncPath )
{
    //===== 1. 清空目录LIST ======
    slDirNames.Clear();

    //===== 2. 打开路径 =====
    DIR *pDir;
    sPath = RegulatePathFile(sPath);

    pDir = opendir(sPath);
    if (pDir == NULL)
        return;

    //==== 3. 循环读取目录内容 ========
    struct dirent * pDirent;

    struct stat statBuf;

    while ((pDirent = readdir(pDir)) != NULL)
    {
        //===== 4. 如果是"."或"..", 则跳过该文件 =====
        if ((strcmp(pDirent->d_name,".")== 0)
                || (strcmp(pDirent->d_name,"..")== 0))
            continue;

        //==== 5. 获取文件属性 =======
        if (stat(MergePathAndFile(sPath,TCString(pDirent->d_name)),
                &statBuf) == -1)
        {
            continue;
        }

        //====== 6. 判断是否为目录又符合过滤串, 则加入List中 ====
        if (S_ISDIR(statBuf.st_mode))
            if (FilterStr(pDirent->d_name, sFilterStr))
                if (IsIncPath)
                    slDirNames.Add(MergePathAndFile(sPath,
                            TCString(pDirent->d_name)));
                else
                    slDirNames.Add(TCString(pDirent->d_name));
    }   // end of while ((pDirent = readdir(pDir)) != NULL)

    //======== 7. 关闭路径 =====
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
                    //如果两个连续的 “*”就截去一个“*”
                    nPosFilter = 2;
                    nPosFileName = 1;
                    break;
                case '?' :
                    //不能正确出来 *?*? 这样的序列
                    nPosFilter = 3;
                    nPosFileName = 2;
                    break;
                case '\0' :
                    //如果最后一个为 “*”则返回真
                    return true;
                default :
                    //如果为普通字符（包括“.”就查找相应的 字符）
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
                //如果两个串都是最后一个串的时候就返回成功
                return true;
            }
            else
            {
                //否则指针下移
                nPosFilter = 2;
                nPosFileName = 2;
            }
            break;

        default :

            if (sFileName == sFilter)
            {
                //如果字符串相等就返回真
                return true;
            }
            else
            {
              if (sFileName[1] != sFilter[1])
              {
                  //如果普通字符不匹配就返回失败
                  return false;
              }
              else
              {
                  //指针下移
                  nPosFilter = 2;
                  nPosFileName = 2;
              }
            }
            break;

    }
    if ((nPosFileName <= nFileNameLen) && (nPosFilter <= nFilterLen))
    {
        //当没有超过字符串有效长度就递归调用
        return FilterStr(sFileName.Mid(nPosFileName,nFileNameLen - nPosFileName + 1),
            sFilter.Mid(nPosFilter,nFilterLen - nPosFilter + 1)) ;
    }
    else
    {
        //当超过字符串有效长度就返回失败
        return false;
    }
}

//==========================================================================
// 函数 : DelTree
// 用途 : 带子目录删除目录
// 原型 : void DelTree(const TCString & sDirectoryName, bool bReserveDirectory = false);
// 参数 : 目录名，是否保留目录
// 返回 : 无
// 说明 :
// 历史 : 2001.11.19 加入bReserveDirectory参数
//==========================================================================
void DelTree(const TCString & sDirectoryName, bool bReserveDirectory)
{
    TCStringList slDirList, slFileList;
    long i;

    GetDirDirList(slDirList, sDirectoryName, "*", true);
    for (i = 0; i < slDirList.GetCount(); i++)
    {
        DelTree(slDirList[i], bReserveDirectory);
        // RemoveDirE(slDirList[i]);    // 注释掉2001.9.29 BUG OLDIX
    }

    GetDirFileList(slFileList, sDirectoryName, "*", false, true);
    for (i = 0; i < slFileList.GetCount(); i++)
        DeleteFileE(slFileList[i]);

    if (!bReserveDirectory)
        RemoveDirE(sDirectoryName);
}

//==========================================================================
// 临时文件几个函数(TMPFile...)的使用说明:
//     TMPFile函数处理临时文件，方便处理临时文件的命名、改名等问题。
//     这三个函数为: TMPFileGetByFileName、TMPFileGetByDirName、TMPFileCommit
//
// 可以有几种调用顺序:
// (1) sTmpFile = TMPFileGetByFileName(sFile);
//     <Process sTmpFile>
//     TMPFileCommit(sTmpFile);
// (2) sTmpFile = TMPFileGetByFileName(sFile);
//     <Process sTmpFile>
//     TMPFileCommit(sFile);     ------- 以上两个例子说明，提交时既可以提交
//                                       临时文件，又可以提交非临时文件
// (3) sTmpFile = TMPFileGetByDirName(sDir);
//             ------ 也可以通过其它方式得到临时文件，临时文件也可以在其
//                    它目录，但要求能够正常改名(在同一个文件系统)。
//     <Process sTmpFile>
//     TMPFileCommit(sTmpFile, sActuallyFileName);
//
// 注意 : TMPFileCommit()如果目标文件存在，则采用覆盖方式。如需要判断目标文
//        件的存在情况，则需要额外编写代码。
//==========================================================================

//==========================================================================
// 函数 : TMPFileGetByFileName
// 用途 : 给出一个文件对应的临时文件名
// 原型 : TCString TMPFileGetByFileName(TCString sPathFileName);
// 参数 : 文件名
// 返回 : 对应的临时文件名
// 说明 :
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
// 函数 : TMPFileGetByDirName
// 用途 : 给出目录下的一个临时文件名
// 原型 : TCString TMPFileGetByDirName(TCString sDirName);
// 参数 : 目录名
// 返回 : 临时文件名
// 说明 :
//==========================================================================
TCString TMPFileGetByDirName(TCString sDirName)
{
    TCString sFileName;

    sFileName = "__" + TCTime::Now() + "." + IntToStr(RandLong(9999)) + "!!";

    return MergePathAndFile(sDirName, sFileName);
}

//==========================================================================
// 函数 : TMPFileCommit
// 用途 : 提交临时文件
// 原型 : TCString TMPFileCommit(TCString sTmpFileName,
//                  TCString sNormalFileName = "");
// 参数 : 临时文件名，普通文件名
// 返回 : 无
// 说明 :
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
// 函数 : TCFileException::TCFileException
// 用途 : 文件操作例外的构造函数
// 原型 : TCFileException(TCString sOperation, TCString sSrcName = "",
//              TCString sDestName = "", TCString sMsg = "");
// 参数 : 操作, 源名，目标名，信息串
// 返回 : 无
// 说明 :
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





