//---------------------------------------------------------------------------

#ifndef c_indexed_dbfH
#define c_indexed_dbfH

#include "cmpublic.h"

#include "c_db_index.h"

//---------------------------------------------------------------------------

//==========================================================================
// 类   : TCIndexedDBF
// 用途 : 带有索引的数据文件
// 说明 : 1. 索引文件结构采用的是自定义BTree格式。
//        2. 索引文件的关键字采用的是字段的前n个字段，不能任意指定字段。
//        3. 目前仅支持插入(Append), 查找(Locate), 重建索引(Reindex)等操作。
//==========================================================================
class TCIndexedDBF : public TCFoxDBF
{
private:
    TCDBIndex m_CDBIndex;

    long m_nKeyCount;       // 关键字段数
    long m_nKeyFieldsSize;  // 关键字段总长
    long m_nIndexDataSize;  // 索引项总长

    char *m_pIndexData;     // 存放一个索引项的全部数据

    void CreateIndexFile(TCString sIndexFileName, bool bUnique);

public:
    ~TCIndexedDBF();

    TEDBAddResult Append();

    void AttachFile(TCString sFileName, long nKeyCount, bool bUnique = true);
    void AttachFileR(TCString sFileName, long nKeyCount, bool bUnique = true);

    void CloseDBF();

    long FindIndex(char *KeyData);

    void Flush();

    long Locate();

    void Reindex();
    void ZAP();

    static void DropDBFFile(TCString sFileName);

    long LocateFirst();
    long LocateNext();
};

#ifdef __TEST__
void TestIndexedDBFMainFunc();
#endif

#endif

