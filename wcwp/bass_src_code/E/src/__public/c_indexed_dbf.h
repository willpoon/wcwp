//---------------------------------------------------------------------------

#ifndef c_indexed_dbfH
#define c_indexed_dbfH

#include "cmpublic.h"

#include "c_db_index.h"

//---------------------------------------------------------------------------

//==========================================================================
// ��   : TCIndexedDBF
// ��; : ���������������ļ�
// ˵�� : 1. �����ļ��ṹ���õ����Զ���BTree��ʽ��
//        2. �����ļ��Ĺؼ��ֲ��õ����ֶε�ǰn���ֶΣ���������ָ���ֶΡ�
//        3. Ŀǰ��֧�ֲ���(Append), ����(Locate), �ؽ�����(Reindex)�Ȳ�����
//==========================================================================
class TCIndexedDBF : public TCFoxDBF
{
private:
    TCDBIndex m_CDBIndex;

    long m_nKeyCount;       // �ؼ��ֶ���
    long m_nKeyFieldsSize;  // �ؼ��ֶ��ܳ�
    long m_nIndexDataSize;  // �������ܳ�

    char *m_pIndexData;     // ���һ���������ȫ������

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

