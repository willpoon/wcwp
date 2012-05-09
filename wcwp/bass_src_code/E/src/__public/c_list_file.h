#ifndef c_list_fileH
#define c_list_fileH
#include "cmpublic.h"

//---------------------------------------------------------------------------

class TCListFile
{
  private :
    TCString m_sFileName ;      // ÎÄ¼þÃû
    void CreateNewListFile();
  public :
    void SetFileName(const TCString sFileName) ;
    void AppendRecord(const TCString sRecord) ;
    TCString GetFirstRecord() ;
    TCString GetLastRecord();
    long GetRecordCount();
    void CutFirstRecord();
    void AppendRecord(TCStringList & sRecordList) ;

    void FillStringList(TCStringList & slFileNameList);

  public:
    static void AppendFileToList(TCString sFullPathFileName,TCString sFileListName="LIST");
    static TCString FetchFileThroughList(TCString sDirName,TCString sFileListName="LIST");	//modi for SERV BUFF
    static void CutFileFromList(TCString sFileName,TCString sFileListName="LIST");
    static void CutFirstFileOfList(TCString sDirName,TCString sFileListName="LIST");		//modi for SERV BUFF

    static void FillStringListThroughList(TCStringList & slFileNameList,
            TCString sDirName);
};

#ifdef __TEST__
void TestListMainFunc();
#endif

#endif

