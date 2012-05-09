//---------------------------------------------------------------------------

#ifndef c_foxdbfH
#define c_foxdbfH
//---------------------------------------------------------------------------

#include "cmpublic.h"

class TCDBFStructDefine;

//==========================================================================
// 类   : TCFoxDBF
// 用途 : 提供通用存储结构。力图达到方便高效的效果。
// 说明 : 该类最初为C++类(HFoxDBF)，后改为Delphi类。此次改写目的是
//        加入TCString等类型。为使该类的使用更方便并避免使用中的一
//        些繁琐的地方。
//==========================================================================

//==========================================================================
// 结构 : TSFOXHEAD
// 用途 : FOX文件头。存储文件头信息。
// 说明 : recnum, recaddr, reclen三个字段在实现上依赖于机器的数字顺序。
//        如果是反Intel序，则在读入时要进行反转。而在写入时再反转回来。
//==========================================================================
struct TSFOXHEAD
{	BYTE sign;      // 标志
	BYTE year;      // 年
	BYTE month;     // 月
	BYTE day;       // 日
	DWORD recnum;   // 记录数
	WORD recaddr;   // 记录地址
	WORD reclen;    // 记录长度
	BYTE sp[20];    // 保留
};

//==========================================================================
// 结构 : TSFOXFIELD
// 用途 : 存储字段信息
// 说明 :
//==========================================================================
struct TSFOXFIELD
{	char fldname[10];            // 字段名
	char sp1;                    // 保留
	char ftype;                  // 类型
	char nouse[4];               // 保留
	unsigned char flen;          // 长茺
	char fpoint;                 // 小数点后的位数
	char sp2[14];                // 保留
};

//==========================================================================
// 枚举 : TEFoxBindType
// 用途 : 字段绑定类型
// 说明 :
//==========================================================================
enum TEFoxBindType
{	FBChar,         // 字符绑定(char)
    FBString,       // 字符串绑定(TCString)
    FBInteger,      // 整型绑定(Integer)
    FBLong,         // 长整型绑定(Long)
    FBUnsignedLong, // 无符号长整型绑定(Unsigned Long)
    FBFloat,        // 浮点绑定(Float)
    FBDouble        // 双精度绑定(Double)
};

//==========================================================================
// 结构 : TSFoxBind
// 用途 : 字段绑定信息
// 说明 : 存储字段绑定信息，根据绑定信息进行数据的存取操作。
//==========================================================================
struct TSFOXBIND
{	void           * bp;        // 绑定指针
	TEFoxBindType   ptype;      // 绑定类型
	long            start;      // 字段起始位置
	long            length;     // 字段长度
};

class TCFoxDBF
{
protected:
    bool m_bOpenWithReadOnly;               // 只读方式(2001.11.29 ADDED) 

protected:
    TSFOXFIELD *m_ffpFoxFields;     // DBF字段信息
    TSFOXBIND *m_fbpFoxBinds;       // 字段绑定信息
    char *m_pRecordBuffer;          // 存储一条记录的缓冲区

    bool m_bHasAppended;            // 是否记录条数增加过
    bool m_bFileOpened;             // 文件是否已被打开过

    TCFileStream m_fDBFFile;        // 文件指针操作

    TSFOXHEAD m_fhFoxHead;          // 数据库文件文件头

    long m_nFieldAmount;            // 字段总数
    long m_nRecordAddress;          // 记录开始地址

    long m_nCurrentRecordSeq;       // 当前记录

    bool m_bBOF;                    // 到达文件头
    bool m_bEOF;                    // 到达文件尾

    bool m_bIsCurrentDelete;        // 当前记录是否已被删除

    bool m_bModifyingCheck;         // 文件以AttachFileW打开

    TCString m_sDBFFileName;        // 数据库文件名

    char m_szTmpBuffer[255];        // 用于暂存读取或写入时的字段内容

    void DBBindToPointer(long nFieldID, TEFoxBindType fbFoxBindType,
            void *pBindPointer);

    void FillRecordBufferByStringList(TCStringList & slRecordValue);

    void GetDBFRecValue();

    void InsertPrepare();       // Insert真实记录前移动记录信息

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

    bool Bof()              // 到文件头了吗？
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

    bool Eof()                              // 到文件尾了吗？
    { return m_bEOF; }

    void FetchDBFStructDefine(TCDBFStructDefine & dsStructDefine);

    long FieldAmount()                      // 得到字段总数
    { return m_nFieldAmount; }

    long FieldLength(long nFieldSeq)        // 得到字段长度
    { return m_ffpFoxFields[nFieldSeq].flen; }

    TCString FieldName(long nFieldSeq);

    long FieldPoint(long nFieldSeq)         // 得到字段小数点后的位长
    {  return m_ffpFoxFields[nFieldSeq].fpoint; }

    long FieldSeq(TCString sFieldName);

    char FieldType(long nFieldSeq)          // 得到字段类型
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

    void GoBottom()                         // 移到最后一条记录
    { Go(RecCount()); }

    void GoTop()                            // 移到第一条记录
    { Go(1); }

    void Insert();
    void InsertRecordByStringList(TCStringList & slRecordValue);

    void Pack();

    long RecCount()                         // 得到记录数
    { return m_fhFoxHead.recnum; }
    static long GetRecCountOfDBFFile(TCString sFileName);

    long RecNo()
    { return m_nCurrentRecordSeq + 1; }

    long RecordLength()     // 得到记录总长度
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
// 结构 : TSFoxFieldDef
// 用途 : 存储字段定义
// 说明 : 用于新建数据库时的数据库定义数组。
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
// 类   : TCDBFStructDefine
// 用途 : DBF结构定义
// 说明 : 存储DBF结构，根据该结构可以做建表操作等，今后还可扩充alter table
//        等操作
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

