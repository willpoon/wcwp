//---------------------------------------------------------------------------

#ifndef pub_dir_fileH
#define pub_dir_fileH
//---------------------------------------------------------------------------

TCString ChangeFileExt(const TCString sFileName, const TCString sExtension);

bool CopyFile(const TCString sSrcName, const TCString sDestName);
void CopyFileE(const TCString sSrcName, const TCString sDestName);
bool CopyFileToDir(const TCString sSrcFileName, const TCString sDestDir);
void CopyFileToDirE(const TCString sSrcFileName, const TCString sDestDir);

void CreateBlankFile(const TCString sFileName);

bool CreateDir(const TCString sDirName);
void CreateDirE(const TCString sDirName);

bool DeleteFile(const TCString sFileName);   
void DeleteFileE(const TCString sFileName);

bool DirectoryExists(TCString sDirName);

bool EnableReadFile(const TCString sFileName,const char cAppend='A');
void EnableReadFileE(const TCString sFileName,const char cAppend='A');
bool EnableWriteFile(const TCString sFileName,const char cAppend='A');
void EnableWriteFileE(const TCString sFileName,const char cAppend='A');

TCString ExcludeTrailingSlash(const TCString S);
TCString IncludeTrailingSlash(const TCString S);

TCString ExtractFileDir(const TCString sFileName);
TCString ExtractFileExt(const TCString sFileName);
TCString ExtractFileName(const TCString sFileName);
TCString ExtractFilePath(const TCString sFileName);
TCString CutFileExt(const TCString sFileName) ;


bool FileExists(TCString sFileName);
TCString FileGetTime(TCString sFileName);
long FileSize(TCString sFileName);

bool ForceDirectories(const TCString sDirName);
void ForceDirectoriesE(const TCString sDirName);

bool IsAbsolutePathName(const TCString S);
bool IsPathDelimiter(const TCString S, long Index);

TCString MergePathAndFile(TCString sPath, TCString sFile);

bool MoveFile(TCString sFileName, TCString sNewPath,
        bool bWithOverWrite = false);
void MoveFileE(TCString sFileName, TCString sNewPath,
        bool bWithOverWrite = false);

bool RenameFile(TCString sOldName, TCString sNewName,
        bool bWithOverWrite = false);
void RenameFileE(TCString sOldName, TCString sNewName,
        bool bWithOverWrite = false);

bool RemoveDir(TCString sDirecotryName);
void RemoveDirE(TCString sDirecotryName);

TCString RegulatePathFile(TCString sPathFileName);

void GetDirFileList(TCStringList& slFileName, TCString sPath ,
                     TCString sFilterStr,bool IsIncSubDir = false,
                     bool IsIncPath = false);
void GetDirDirList(TCStringList& slDirNames, TCString sPath,
                     TCString sFilterStr, bool IsIncPath = false);
bool FilterStr(const TCString sFileName,const TCString sFilter);

void DelTree(const TCString & sDirectoryName, bool bReserveDirectory = false);

TCString TMPFileGetByFileName(TCString sPathFileName);
TCString TMPFileGetByDirName(TCString sDirName);
TCString TMPFileCommit(TCString sTmpFileName, TCString sNormalFileName = "");

class TCFileException : public TCException
{
private:
    TCString m_sOperation;
    TCString m_sSrcName;
    TCString m_sDestName;
public:
    TCFileException(TCString sOperation, TCString sSrcName = "",
            TCString sDestName = "", TCString sMsg = "");
};

#ifdef __TEST__
void TestGetDirDirListFunc();
#endif

#endif

