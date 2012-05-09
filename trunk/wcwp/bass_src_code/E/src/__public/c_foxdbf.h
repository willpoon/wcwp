//---------------------------------------------------------------------------

#ifndef c_foxdbfH
#define c_foxdbfH
//---------------------------------------------------------------------------

#include "cmpublic.h"

class TCDBFStructDefine;

//==========================================================================
// ��   : TCFoxDBF
// ��; : �ṩͨ�ô洢�ṹ����ͼ�ﵽ�����Ч��Ч����
// ˵�� : �������ΪC++��(HFoxDBF)�����ΪDelphi�ࡣ�˴θ�дĿ����
//        ����TCString�����͡�Ϊʹ�����ʹ�ø����㲢����ʹ���е�һ
//        Щ�����ĵط���
//==========================================================================

//==========================================================================
// �ṹ : TSFOXHEAD
// ��; : FOX�ļ�ͷ���洢�ļ�ͷ��Ϣ��
// ˵�� : recnum, recaddr, reclen�����ֶ���ʵ���������ڻ���������˳��
//        ����Ƿ�Intel�����ڶ���ʱҪ���з�ת������д��ʱ�ٷ�ת������
//==========================================================================
struct TSFOXHEAD
{	BYTE sign;      // ��־
	BYTE year;      // ��
	BYTE month;     // ��
	BYTE day;       // ��
	DWORD recnum;   // ��¼��
	WORD recaddr;   // ��¼��ַ
	WORD reclen;    // ��¼����
	BYTE sp[20];    // ����
};

//==========================================================================
// �ṹ : TSFOXFIELD
// ��; : �洢�ֶ���Ϣ
// ˵�� :
//==========================================================================
struct TSFOXFIELD
{	char fldname[10];            // �ֶ���
	char sp1;                    // ����
	char ftype;                  // ����
	char nouse[4];               // ����
	unsigned char flen;          // ����
	char fpoint;                 // С������λ��
	char sp2[14];                // ����
};

//==========================================================================
// ö�� : TEFoxBindType
// ��; : �ֶΰ�����
// ˵�� :
//==========================================================================
enum TEFoxBindType
{	FBChar,         // �ַ���(char)
    FBString,       // �ַ�����(TCString)
    FBInteger,      // ���Ͱ�(Integer)
    FBLong,         // �����Ͱ�(Long)
    FBUnsignedLong, // �޷��ų����Ͱ�(Unsigned Long)
    FBFloat,        // �����(Float)
    FBDouble        // ˫���Ȱ�(Double)
};

//==========================================================================
// �ṹ : TSFoxBind
// ��; : �ֶΰ���Ϣ
// ˵�� : �洢�ֶΰ���Ϣ�����ݰ���Ϣ�������ݵĴ�ȡ������
//==========================================================================
struct TSFOXBIND
{	void           * bp;        // ��ָ��
	TEFoxBindType   ptype;      // ������
	long            start;      // �ֶ���ʼλ��
	long            length;     // �ֶγ���
};

class TCFoxDBF
{
protected:
    bool m_bOpenWithReadOnly;               // ֻ����ʽ(2001.11.29 ADDED) 

protected:
    TSFOXFIELD *m_ffpFoxFields;     // DBF�ֶ���Ϣ
    TSFOXBIND *m_fbpFoxBinds;       // �ֶΰ���Ϣ
    char *m_pRecordBuffer;          // �洢һ����¼�Ļ�����

    bool m_bHasAppended;            // �Ƿ��¼�������ӹ�
    bool m_bFileOpened;             // �ļ��Ƿ��ѱ��򿪹�

    TCFileStream m_fDBFFile;        // �ļ�ָ�����

    TSFOXHEAD m_fhFoxHead;          // ���ݿ��ļ��ļ�ͷ

    long m_nFieldAmount;            // �ֶ�����
    long m_nRecordAddress;          // ��¼��ʼ��ַ

    long m_nCurrentRecordSeq;       // ��ǰ��¼

    bool m_bBOF;                    // �����ļ�ͷ
    bool m_bEOF;                    // �����ļ�β

    bool m_bIsCurrentDelete;        // ��ǰ��¼�Ƿ��ѱ�ɾ��

    bool m_bModifyingCheck;         // �ļ���AttachFileW��

    TCString m_sDBFFileName;        // ���ݿ��ļ���

    char m_szTmpBuffer[255];        // �����ݴ��ȡ��д��ʱ���ֶ�����

    void DBBindToPointer(long nFieldID, TEFoxBindType fbFoxBindType,
            void *pBindPointer);

    void FillRecordBufferByStringList(TCStringList & slRecordValue);

    void GetDBFRecValue();

    void InsertPrepare();       // Insert��ʵ��¼ǰ�ƶ���¼��Ϣ

    void PutDBFValue(long nPutFieldCount = 0);

    void ReadRecordToBuffer();

    void WriteFoxHeader();

public:
    TCFoxDBF();
    ~TCFoxDBF();

    void Append();
    void AppendRecordByStringList(TCStringList & slStringList);

    void AttachFile(TCString sDBFFileName);
    void AttachFileR(TCString sDBFFileName);
    void AttachFileW(TCString sDBFFileName);

    bool Bof()              // ���ļ�ͷ����
    { return m_bBOF; }

    void CloseDBF();

    void CreateFoxDBFBySelf(TCString sDestDBF);

    // ========= BIND VALUES ACCORDING FIELD_ID =======
    void DBBind(long nFieldID, char & cCharValue)
    { DBBindToPointer(nFieldID, FBChar, &cCharValue); }

    void DBBind(long nFieldID, TCString & sStringValue)
    { DBBindToPointer(nFieldID, FBString, &sStringValue); }

    void DBBind(long nFieldID, int & nIntValue)
    { DBBindToPointer(nFieldID, FBInteger, &nIntValue); }

    void DBBind(long nFieldID, long & nLongValue)
    { DBBindToPointer(nFieldID, FBLong, &nLongValue); }

    void DBBind(long nFieldID, unsigned long & nUnsignedLongValue)
    { DBBindToPointer(nFieldID, FBUnsignedLong, &nUnsignedLongValue); }

    void DBBind(long nFieldID, float & fFloatValue)
    { DBBindToPointer(nFieldID, FBFloat, &fFloatValue); }

    void DBBind(long nFieldID, double & fDoubleValue)
    { DBBindToPointer(nFieldID, FBDouble, &fDoubleValue); }

    // ========= BIND VALUES ACCORDING FIELD_NAME =======
    void DBBind(char *sFieldName, char & cCharValue)
    { DBBindToPointer(FieldSeq(sFieldName), FBChar, &cCharValue); }

    void DBBind(char *sFieldName, TCString & sStringValue)
    { DBBindToPointer(FieldSeq(sFieldName), FBString, &sStringValue); }

    void DBBind(char *sFieldName, int & nIntValue)
    { DBBindToPointer(FieldSeq(sFieldName), FBInteger, &nIntValue); }

    void DBBind(char *sFieldName, long & nLongValue)
    { DBBindToPointer(FieldSeq(sFieldName), FBLong, &nLongValue); }

    void DBBind(char *sFieldName, unsigned long & nUnsignedLongValue)
    { DBBindToPointer(FieldSeq(sFieldName), FBUnsignedLong,
            &nUnsignedLongValue); }

    void DBBind(char *sFieldName, float & fFloatValue)
    { DBBindToPointer(FieldSeq(sFieldName), FBFloat, &fFloatValue); }

    void DBBind(char *sFieldName, double & fDoubleValue)
    { DBBindToPointer(FieldSeq(sFieldName), FBDouble, &fDoubleValue); }
    //================== BIND PROCEDURES END =================

    void Delete();

    bool Deleted()
    { return m_bIsCurrentDelete; }

    bool Eof()                              // ���ļ�β����
    { return m_bEOF; }

    void FetchDBFStructDefine(TCDBFStructDefine & dsStructDefine);

    long FieldAmount()                      // �õ��ֶ�����
    { return m_nFieldAmount; }

    long FieldLength(long nFieldSeq)        // �õ��ֶγ���
    { return m_ffpFoxFields[nFieldSeq].flen; }

    TCString FieldName(long nFieldSeq);

    long FieldPoint(long nFieldSeq)         // �õ��ֶ�С������λ��
    {  return m_ffpFoxFields[nFieldSeq].fpoint; }

    long FieldSeq(TCString sFieldName);

    char FieldType(long nFieldSeq)          // �õ��ֶ�����
    { return m_ffpFoxFields[nFieldSeq].ftype; }

    bool FileOpened()
    { return m_bFileOpened; }

    void Flush();

    TCString GetCurrentDataByFieldSeq(long nFieldSeq);
    TCString GetDBData(long nRecordSeq, TCString sFieldName);

    TCString GetDBFFileName()
    { return m_sDBFFileName; }

    static TCString GetModifyControlFileName(TCString sFileName); 

    void ReadRecordIntoStringList(TCStringList & slRecordValue,
            long nRecordSeq = -1);

    void Go(long nRecNum);

    void GoBottom()                         // �Ƶ����һ����¼
    { Go(RecCount()); }

    void GoTop()                            // �Ƶ���һ����¼
    { Go(1); }

    void Insert();
    void InsertRecordByStringList(TCStringList & slRecordValue);

    void Pack();

    long RecCount()                         // �õ���¼��
    { return m_fhFoxHead.recnum; }
    static long GetRecCountOfDBFFile(TCString sFileName);

    long RecNo()
    { return m_nCurrentRecordSeq + 1; }

    long RecordLength()     // �õ���¼�ܳ���
    { return m_fhFoxHead.reclen; }

    static void RegulateHeaderByteOrder(TSFOXHEAD &fxhFoxHead);

    void Skip(long nRecordOffset = 1);

    void SortToFile(TCString sSortedFileName, TCString sFieldNames);
    static void SortDBFFile(TCString sDBFFileName, TCString sFieldNames);

    void Update();
    void UpdateRecordByStringList(TCStringList & slRecordValue);

    void ZAP();

    void ImportFromTextFile(TCString sTxtFileName, char cCommaChar = ',',
            bool bCheckFieldAmount = true);
    void ExportToTextFile(TCString sTxtFileName, char cCommaChar = ',');
};

//==========================================================================
// �ṹ : TSFoxFieldDef
// ��; : �洢�ֶζ���
// ˵�� : �����½����ݿ�ʱ�����ݿⶨ�����顣
//==========================================================================
struct TSFoxFieldDef
{
    TCString    fldname;
    char        ftype;
    BYTE        flen;
    BYTE        fpoint;
};

class TCDBFCreate
{
private:
    TSFoxFieldDef *m_fdFieldsDef;
    long m_nFieldCount;
    long m_nFieldCurrentSeq;
    TCString m_sDBFName;

public:
    TCDBFCreate(TCString sDBFName, long nFieldCount);
    ~TCDBFCreate();
    void AddField(TCString sFieldName, char cFieldType, long nFieldLen = 0,
            long nFieldPoint = 0);
    void CreateDBF();
};

//==========================================================================
// ��   : TCDBFStructDefine
// ��; : DBF�ṹ����
// ˵�� : �洢DBF�ṹ�����ݸýṹ��������������ȣ���󻹿�����alter table
//        �Ȳ���
//==========================================================================
const long MAX_FOX_FIELD_COUNT = 256;

class TCDBFStructDefine
{
private:
    TSFoxFieldDef m_fdFieldsDef[MAX_FOX_FIELD_COUNT];
    long m_nFieldCount;

    void SetFieldDefine(long nFieldSeq, TCString sFieldName, char cFieldType,
            long nFieldLen, long nFieldPoint);

public:
    TCDBFStructDefine();
    void AddField(TCString sFieldName, char cFieldType, long nFieldLen = 0,
            long nFieldPoint = 0);
    void InsertField(long nFieldSeq, TCString sFieldName, char cFieldType,
            BYTE nFieldLen = 0, BYTE nFieldPoint = 0);

    void Clear();

    long FieldSeq(TCString sFieldName);
    TCString FieldName(long nFieldSeq);

    long FieldCount();
    void CreateDBF(TCString sFileName);
};

#ifdef __TEST__
void TestDBFMainFunc();
#endif

#endif

