#include "cmpublic_db.h"

#ifdef  __ORACLECLASS
#include <oci.h>
#endif

//==========================================================================
// 函数 : TCQuery::TCQuery()
// 用途 : 构造函数
// 原型 : TCQuery()
// 参数 : 无
// 返回 : 无
// 说明 : 如果是缺省构造函数，则其关联的数据库为缺省数据库
//==========================================================================
TCQuery::TCQuery()
{
    m_Database = & DatabaseMain;
    _Init();
}

//==========================================================================
// 函数 : TCQuery::TCQuery
// 用途 : 构造函数
// 原型 : TCQuery(TCDatabase & db);
// 参数 : 关联的数据库
// 返回 : 无
// 说明 :
//==========================================================================
TCQuery::TCQuery(TCDatabase & db)
{
	m_Database = & db ;
    _Init();
}

//==========================================================================
// 函数 : TCQuery::~TCQuery
// 用途 : 析构函数
// 原型 : ~TCQuery();
// 参数 : 无
// 返回 : 无
// 说明 : 析构时关闭结果集
//==========================================================================
TCQuery::~TCQuery()
{
    Close() ;
}

//==========================================================================
// 函数 : TCQuery::_Init
// 用途 : 初始化成员变量
// 原型 : void _Init();
// 参数 : 无
// 返回 : 无
// 说明 : 该函数仅被构造函数调用
//==========================================================================
void TCQuery::_Init()
{
	m_DataSet       = NULL ;
	m_sSQL	        = "" ;
	m_nRecordCount  = 0 ;
    m_nNumCols      = 0 ;
    m_nCurrentRow   = 0 ;
}

//==========================================================================
// 函数 : TCQuery::BindArray
// 用途 : 绑定数组结构
// 原型 : bool BindArray(int nBindPos,int nArraySize);
// 参数 : nBindPos   -- 数组对应的列位置
//        nArraySize -- 数组长度
// 返回 : 是否绑定成功
// 说明 :
//==========================================================================
bool TCQuery::BindArray(int nBindPos,int nArraySize)
{
    sword status ;

    if (nBindPos < 1) return false ;

    OCIDBPROCESS * DBProc = m_Database->GetDBProc() ;

    status = OCIBindArrayOfStruct(DBProc->BindByPos[nBindPos - 1],
            DBProc->phError, (ub4)nArraySize, (ub4)0, (ub4)0, (ub4)0);
            
    if (status != OCI_SUCCESS)
    {
        m_Database->CheckError(status);
        return false ;
    }

    return true ;
}

//==========================================================================
// 函数 : TCQuery::BindParamByName
// 用途 : 按列名绑定输入变量
// 原型 : bool BindParamByName(void *pBuffer,int nBufferlen,
//              TCString sColName,int nBindType);
// 参数 : pBuffer    -- 变量地址
//        nBufferlen -- 变量长度
//        sColName   -- 列名
//        nBindType  -- 列数据类型
// 返回 : 是否绑定成功
// 说明 :
//==========================================================================
bool TCQuery::BindParamByName(void *pBuffer,int nBufferlen,
            TCString sColName,int nBindType)
{
    sword status ;

    OCIDBPROCESS * DBProc = m_Database->GetDBProc() ;
    OCIBind * bndp ;

    status = OCIBindByName(DBProc->phSQL, &bndp, DBProc->phError,
            (text *)((char *)sColName), -1, (dvoid *)pBuffer,
            (sb4)nBufferlen, (ub2)nBindType, (dvoid *)0,
            (ub2 *)0, (ub2)0, (ub4 )0, (ub4 *)0, (ub4)OCI_DEFAULT);
    if (status != OCI_SUCCESS)
    {
        m_Database->CheckError(status);
        return false ;
    }

    return true ;
}

//==========================================================================
// 函数 : TCQuery::BindParamByPos
// 用途 : 按列位置绑定输入变量
// 原型 : bool BindParamByPos(void *pBuffer,int nBufferlen,
//              int nBindPos,int nBindType);
// 参数 : pBuffer    -- 变量地址
//        nBufferlen -- 变量长度
//        nBindPos   -- 列位置
//        nBindType  -- 列数据类型
// 返回 : 是否绑定成功
// 说明 : 
//==========================================================================
bool TCQuery::BindParamByPos(void *pBuffer,int nBufferLen,
            int nBindPos,int nBindType)
{
    sword status ;
    if (nBindPos < 1) return false ;

    OCIDBPROCESS * DBProc = m_Database->GetDBProc() ;

    status = OCIBindByPos(DBProc->phSQL,&DBProc->BindByPos[nBindPos - 1],
            DBProc->phError,(ub4)nBindPos, (dvoid *)pBuffer,
            (sb4)nBufferLen, (ub2)nBindType, (dvoid *)0,
            (ub2 *)0, (ub2) 0, (ub4)0, (ub4 *)0, (ub4)OCI_DEFAULT);

    if (status != OCI_SUCCESS)
    {
        m_Database->CheckError(status);
        return false ;
    }

    return true ;
}

//==========================================================================
// 函数 : TCQuery::CheckDBActive
// 用途 : 检查数据库是否已激活
// 原型 : bool CheckDBActive();
// 参数 : 无
// 返回 : 是否已激活
// 说明 : 如果未激活，则根据相关联的数据库设置，决定是否抛出例外
//==========================================================================
bool TCQuery::CheckDBActive()
{
	ASSERT(m_Database != NULL);

	if (m_Database->IsConnected())
        return true;
    else
    {
        if (NeedThrowException())
            throw TCDBException(dbeNotActiveDB,
                    "TCQuery::CheckDBActive DB Not Active");
        else
            return false;
    }
}

//==========================================================================
// 函数 : TCQuery::Close
// 用途 : 关闭本次查询
// 原型 : void Close()
// 参数 : 无
// 返回 : 无
// 说明 :
//==========================================================================
void TCQuery::Close()
{
	if (m_DataSet != NULL)
	{
		delete  m_DataSet ;
        m_DataSet =  NULL ;
	}

  	m_sSQL	= "" ;
	m_nRecordCount = 0 ;
    m_nNumCols = 0 ;
}

//==========================================================================
// 函数 : TCQuery::ExecSQL
// 用途 : 执行SQL语句
// 原型 : bool ExecSQL();
// 参数 : 无
// 返回 : 是否执行成功
// 说明 :
//==========================================================================
bool TCQuery::ExecSQL()
{
    return ExecSQL(0, 1, true) ;
}

//==========================================================================
// 函数 : TCQuery::ExecSQL
// 用途 : 执行SQL语句
// 原型 : bool ExecSQL(TCString sSQL);
// 参数 : 无
// 返回 : 是否执行成功
// 说明 :
//==========================================================================
bool TCQuery::ExecSQL(TCString sSQL)
{
    TCString sOneStatement;
    long i;

    for (i = 1; i <= 400; i++)
    {
        sOneStatement = GetSepStringBySeq(sSQL, "$$", i);
        if (Length(sOneStatement) < 10)
        {
            if (i == 1)
                throw TCException("TCQuery::ExecSQL() : "
                        "Something must be wrong - " + sSQL);
            else
                return true;
        }

        SetSQL(sOneStatement);
        Prepare();
        if (!ExecSQL())
            return false;
    }

    throw TCException("TCQuery::ExecSQL() : Something must be wrong - "
            + sSQL);
}

//==========================================================================
// 函数 : TCQuery::ExecSQL
// 用途 : 执行多次数据更新的SQL语句
// 原型 : bool ExecSQL(int nOffRow,int nRows,bool bRollback);
// 参数 : nOffRow   -- 执行开始行号
//        nRows     -- 执行结束行号
//        bRollback -- 发生错误时是否自动回滚
// 返回 : 是否执行成功
// 说明 :
//==========================================================================
bool TCQuery::ExecSQL(int nOffRow,int nRows,bool bRollback)
{
    //====== 1. 检查执行情况，赋值数据库调用结构 =======
    OCIDBPROCESS * DBProc;
    if (!CheckDBActive())
        return false ;

    Close() ;

    DBProc = m_Database->GetDBProc() ;

    //===== 2. 执行语句 ========
    sword nStatus ;
    nStatus = OCIStmtExecute(DBProc->phService, DBProc->phSQL,
                  DBProc->phError,(ub4)nRows,(ub4)nOffRow,
                 (CONST OCISnapshot *)NULL,(OCISnapshot *)NULL,
                 (ub4)OCI_DEFAULT);

    //======= 3. 错误处理 =====
    if(nStatus != OCI_SUCCESS)
    {
        m_Database->CheckError(nStatus);
        if (bRollback)
              m_Database->Rollback(false) ;
        return false ;
    }

    //======= 4. 确定提交该事务 =====
    if (m_Database->GetCommitMode())
       return (m_Database->Commit(false)) ;

    return true ;
}

//==========================================================================
// 函数 : TCQuery::First
// 用途 : 将当前位置移动结果集的第一行
// 原型 : bool First();
// 参数 : 无
// 返回 : 是否成功
// 说明 : 如果结果集为空，则返回false
//==========================================================================
bool TCQuery::First()
{
	if (m_nRecordCount == 0) return false ;

	m_nCurrentRow = 1 ;
	return true ;
}

//==========================================================================
// 函数 : TCQuery::GetFieldValue
// 用途 : 按列位置取结果集中当前记录的字段值
// 原型 : TCString GetFieldValue(int nColIndex);
// 参数 : 列位置（从1开始）
// 返回 : 字段值
// 说明 : 1. 如果无记录，则返回空串
//        2. 返回的值为字符串，如要取整型值，可用StrToInt进行转换
//==========================================================================
TCString TCQuery::GetFieldValue(int nColIndex)
{
   if (m_nRecordCount <= 0) return ("") ;
   if (nColIndex <= 0 || nColIndex > m_nNumCols)
   {
        throw TCException("TCQuery::GetFieldValue : the column index exceeds");
   }

   return m_DataSet->GetCell(nColIndex - 1, m_nCurrentRow - 1);
}

//==========================================================================
// 函数 : TCQuery::GetFieldValue
// 用途 : 按列位置取结果集中指定记录的字段值
// 原型 : TCString GetFieldValue(int nRowIndex,int nColIndex);
// 参数 : 记录号（从1计数），列位置（从1计数）
// 返回 : 字段值
// 说明 :
//==========================================================================
TCString TCQuery::GetFieldValue(int nRowIndex,int nColIndex)
{
   if (nRowIndex <= 0 || nRowIndex > m_nRecordCount)
    return ("") ;

   m_nCurrentRow = nRowIndex ;

   return (GetFieldValue(nColIndex)) ;
}

//==========================================================================
// 函数 : TCQuery::GetFieldValue
// 用途 : 按列名取结果集中当前记录的字段值
// 原型 : TCString GetFieldValue(TCString sColName);
// 参数 : 列名
// 返回 : 字段值
// 说明 :
//==========================================================================
TCString TCQuery::GetFieldValue(TCString sColName)
{
    int      i;
    bool     bFound = false;
    TCString s;

    if (m_nRecordCount <= 0)
        return "";

    for (i = 0 ; i < m_nNumCols ; i ++)
    {
        s = m_DataSet->GetColName(i) ;

        if (UpperCase(s) == UpperCase(sColName))
        {
            bFound = true ;
            break ;
        }
    }

    if (!bFound)
        throw TCException("TCQuery::GetFieldValue : The column name : "
                + sColName + " doesn\'t not exists!") ;

    return (m_DataSet->GetCell(i,m_nCurrentRow - 1)) ;
}

//==========================================================================
// 函数 : TCQuery::GetFieldValue
// 用途 : 按列名取结果集中指定记录的字段值
// 原型 : TCString GetFieldValue(int nRowIndex,TCString sColName);
// 参数 : 记录号(从1计数)，列名
// 返回 : 字段值
// 说明 :
//==========================================================================
TCString TCQuery::GetFieldValue(int nRowIndex,TCString sColName)
{
    if (nRowIndex <= 0 || nRowIndex > m_nRecordCount)
      return "";

     m_nCurrentRow = nRowIndex;

     return GetFieldValue(sColName);
}

//==========================================================================
// 函数 : TCQuery::GetRecordCount
// 用途 : 返回上一次SELECT操作返回的行数
// 原型 : long GetRecordCount()；
// 参数 : 无
// 返回 : 返回的行数
// 说明 :
//==========================================================================
long TCQuery::GetRecordCount()
{
	return m_nRecordCount ;
}

//==========================================================================
// 函数 : TCQuery::GetRowsAffected
// 用途 : 返回上一次操作影响的行数
// 原型 : long GetRowsAffected();
// 参数 : 无
// 返回 : 影响的行数
// 说明 :
//==========================================================================
long TCQuery::GetRowsAffected()
{
    if (!CheckDBActive())
        return 0;

    long nSuccessRows = 0 ;

    OCIDBPROCESS * DBProc = m_Database->GetDBProc() ;

    OCIAttrGet((dvoid *)DBProc->phSQL,OCI_HTYPE_STMT,
                  (dvoid *)&nSuccessRows,(ub4 *)0,
                  (ub4)OCI_ATTR_ROW_COUNT,DBProc->phError) ;
    return nSuccessRows ;
}

//==========================================================================
// 函数 : TCQuery::IsActive
// 用途 : 判断相关联的数据库当前是否已激活
// 原型 : bool IsActive();
// 参数 : 无
// 返回 : 是否已激活
// 说明 :
//==========================================================================
bool TCQuery::IsActive()
{
	ASSERT(m_Database != NULL );

	return (m_Database->IsConnected()) ;
}

//==========================================================================
// 函数 : TCQuery::Next
// 用途 : 将当前位置移动结果集的下一行
// 原型 : bool Next();
// 参数 : 无
// 返回 : 是否成功
// 说明 : 如当前位置已达到最后一行，则返回false
//==========================================================================
bool TCQuery::Next()
{
	if (m_nRecordCount == 0) return false ;
	if (m_nCurrentRow >= m_nRecordCount) return false ;
    m_nCurrentRow ++ ;

	return true ;
}

//==========================================================================
// 函数 : TCQuery::Open
// 用途 : 准备SELECT语句, 并把结果填入结果集
// 原型 : bool Open();
// 参数 : 无
// 返回 : 是否成功执行
// 说明 :
//==========================================================================
bool TCQuery::Open()
{
    //====== 1. 检查执行情况，赋值数据库调用结构 =======
    OCIDBPROCESS * DBProc;

    if (!CheckDBActive()) return false ;

    if (!Prepare()) return false ;

    Close() ;

    DBProc = m_Database->GetDBProc() ;

    sword         nStatus;

    //===== 2. 执行SQL语句 ========
    nStatus = OCIStmtExecute(DBProc->phService, DBProc->phSQL,
             DBProc->phError, (ub4) 0,(ub4)0,
             (CONST OCISnapshot *)NULL,(OCISnapshot *)NULL,
             OCI_DEFAULT);

    if((nStatus != OCI_SUCCESS) && (nStatus != OCI_NO_DATA))
    {
        m_Database->CheckError(nStatus);
        return false;
    }

    //===== 3. 得到列数 ===========
    long nNumCols ;
    DBProc->ExecFlag = 1;

    OCIAttrGet(DBProc->phSQL, OCI_HTYPE_STMT, &nNumCols,
                        0, OCI_ATTR_PARAM_COUNT, DBProc->phError) ;

    //==== 4. 给bufffer和m_DataSet分配足够的空间 =======
    char ** buffer ;

    buffer      = new char * [nNumCols] ;
    ASSERT(buffer != NULL);
    m_DataSet   = new TCStringGrid(nNumCols,0) ;
    ASSERT(m_DataSet != NULL);

    if (buffer == NULL || m_DataSet == NULL)
        throw TCException("TCQuery::Open() Could not allocate memory") ;

    //====== 5. 逐列取得列属性，并绑定列值 =======
    long       i, j;
    ub2        nFDLen, nFDType ;
    ub4        nColNameSize ;
    OCIParam * opOCIParm ;
    text *     szGettedColName ;
    char *     szColName ;

    for (i = 1 ; i <= nNumCols ; i ++)
    {
        //====== 5.1 取得列属性 ======
        nStatus = OCIParamGet(DBProc->phSQL, OCI_HTYPE_STMT, DBProc->phError,
                (void **)&opOCIParm,(ub4) i);

        if ( nStatus != OCI_SUCCESS )
        {
            m_Database->CheckError(nStatus) ;

            for (j = 0 ; j < i ; j++)
                delete[] buffer[j] ;
            delete[] buffer ;

            Close() ;
            return false ;
        }

        //====== 5.2 取得列宽度 =========
        m_Database->CheckError(OCIAttrGet((dvoid*) opOCIParm,
                (ub4) OCI_DTYPE_PARAM, (dvoid*) &nFDLen,(ub4 *) 0,
                (ub4) OCI_ATTR_DATA_SIZE, (OCIError *) DBProc->phError));

        //====== 5.3 取得列类型 ========
        m_Database->CheckError(OCIAttrGet((dvoid*)opOCIParm,
                (ub4)OCI_DTYPE_PARAM, (dvoid*)&nFDType, (ub4 *)0,
                (ub4)OCI_ATTR_DATA_TYPE, (OCIError *) DBProc->phError )) ;

        //====== 5.4 取得列名及列名长度 =======
        m_Database->CheckError(OCIAttrGet((dvoid *)opOCIParm,
                (ub4)OCI_DTYPE_PARAM, (dvoid*)&szGettedColName,
                (ub4 *)&nColNameSize, (ub4)OCI_ATTR_NAME,
                (OCIError *)DBProc->phError)) ;

        //===== 5.5 给m_DataSet赋予取得的列名信息 =====
        szColName = new char[nColNameSize + 1] ;
        ASSERT(szColName != NULL);

        memset(szColName,0x00,nColNameSize + 1) ;
        memcpy(szColName,szGettedColName,nColNameSize) ;

        m_DataSet->SetColName(i - 1,TCString((char *)szColName)) ;
        delete szColName ;

        //======= 5.6 绑定列到buffer =====
        buffer[i - 1] = new char [nFDLen + 1] ;
        ASSERT(buffer[i - 1] != NULL);
        memset(buffer[i - 1],0x00,nFDLen + 1) ;

        nStatus = OCIDefineByPos(DBProc->phSQL, &DBProc->phDefine[i],
                DBProc->phError, (ub4)i, (dvoid *)buffer[i - 1],
                (sb4)nFDLen + 1, SQLT_STR, (dvoid *)&DBProc->NullFlag[i],
                (ub2 *)0, (ub2 *)0, OCI_DEFAULT);

        if(nStatus != OCI_SUCCESS)
        {
            m_Database->CheckError(nStatus);

            for (j = 0 ; j < i ; j++)
                    delete[] buffer[j] ;
            delete[] buffer ;

            Close() ;
            return false ;
        }
    }

    //========= 6. 检取结果集 ========
    long nRow = 0 ;

    while(true)
    {
        //===== 6.1 取出一行结果 =======
        nStatus = OCIStmtFetch(DBProc->phSQL, DBProc->phError,
             (ub4)1,(ub4) OCI_FETCH_NEXT,(ub4) OCI_DEFAULT);

        //===== 6.2 如果成功取出，则增加结果集 =====
        if(nStatus == OCI_SUCCESS || nStatus == OCI_SUCCESS_WITH_INFO)
        {
            m_DataSet->AddRows(1);
            
            for (i = 0; i < nNumCols ; i++)
                m_DataSet->SetCell(i, nRow, buffer[i]) ;

        }

        //======= 6.3 如果已取无数据，则退出循环 ======
        else if(nStatus == OCI_NO_DATA)
            break ;

        //===== 6.4 如返回值出错，则进行出错处理 ======
        else
        {
            m_Database->CheckError(nStatus);

            for (i = 0 ; i < nNumCols ; i++)
               delete[] buffer[i] ;
            delete[] buffer ;

            Close() ;
            return false ;
        }

        //====== 6.5 取完一行，清空一行的结果 =======
//modi by chang at 20030520 sizeof(m_sBindValue[i])为指针的长度4，直接置'\0'
        for (i = 0 ; i < nNumCols ; i++)
            buffer[i][0] = '\0';
        //  memset(buffer[i],0x00,sizeof(buffer[i])) ;

        nRow ++ ;
    }

    //===== 7. 释放缓冲区 =======
    for (i = 0 ; i < nNumCols ; i++)
        delete[] buffer[i] ;
    delete[] buffer ;

    //==== 8. 赋值列数、行数 =====
    m_nNumCols = nNumCols ;
    m_nRecordCount = nRow ;

    //======= 9. 移动第一行 =====
   if (m_nRecordCount > 0 )
        m_nCurrentRow = 1 ;

    First() ;

    return true ;
}

//==========================================================================
// 函数 : TCQuery::Prepare
// 用途 : 准备执行SQL语句
// 原型 : bool Prepare();
// 参数 : 无
// 返回 : 是否成功准备
// 说明 :
//==========================================================================
bool TCQuery::Prepare()
{
    //===== 1. 检查数据库是否已连接 =======
	if (!CheckDBActive())
        return false ;

    //===== 2. 释放以前的SQL内存 ========
	int status;

    OCIDBPROCESS * DBProc = m_Database->GetDBProc() ;
    DBProc->ExecFlag = 0;

    if (DBProc->phSQL != NULL)
    {
        OCIHandleFree((dvoid *)DBProc->phSQL,OCI_HTYPE_STMT);
        DBProc->phSQL = NULL;
    }

    //====== 3. 分配SQL内存并准备SQL语句 =======
    if((status = OCIHandleAlloc((dvoid *)DBProc->phEnviron,
              (dvoid **)&DBProc->phSQL,
              OCI_HTYPE_STMT,(size_t) 0, (dvoid **)0)) == OCI_SUCCESS)
    {
        status = OCIStmtPrepare(DBProc->phSQL,DBProc->phError,
               (text*)((char *)m_sSQL), (ub4)Length(m_sSQL),
               (ub4) OCI_NTV_SYNTAX,(ub4) OCI_DEFAULT);
    }

    //===== 4. 检查返回结果 =======
    if(status != OCI_SUCCESS)
    {
        m_Database->CheckError(status);
        return false;
    }

    return true;
}

//==========================================================================
// 函数 : TCQuery::SetSQL
// 用途 : 设置要执行的SQL语句
// 原型 : bool SetSQL(TCString sSQL);
// 参数 : SQL语句
// 返回 : 是否成功设置
// 说明 :
//==========================================================================
bool TCQuery::SetSQL(TCString sSQL)
{
    if (!CheckDBActive())
        return false;

    #ifdef _DEBUG
    if (Pos(UpperCase(sSQL), "SELECT ") != 0)
        TCDatabase::WriteDBLogFile("SELECT", sSQL);
    else
        TCDatabase::WriteDBLogFile("EXEC", sSQL);
    #endif

	m_sSQL = sSQL ;

    return true;
}


//==========================================================================
// 函数 : TCQuery::OpenE
// 用途 : 准备SELECT语句
// 原型 : bool OpenE();
// 参数 : 无
// 返回 : 是否成功执行
// 说明 :
//==========================================================================
bool TCQuery::OpenE()
{
    //====== 1. 检查执行情况，赋值数据库调用结构 =======
    OCIDBPROCESS * DBProc;

    if (!CheckDBActive()) return false ;
    if (!Prepare()) return false ;
    Close() ;

    DBProc = m_Database->GetDBProc() ;
    sword         nStatus;

    //===== 2. 执行SQL语句 ========
    nStatus = OCIStmtExecute(DBProc->phService, DBProc->phSQL,
             DBProc->phError, (ub4) 0,(ub4)0,
             (CONST OCISnapshot *)NULL,(OCISnapshot *)NULL,
             OCI_DEFAULT);

    if((nStatus != OCI_SUCCESS) && (nStatus != OCI_NO_DATA))
    {
        m_Database->CheckError(nStatus);
        return false;
    }

    //===== 3. 得到列数 ===========
    long nNumCols ;
    DBProc->ExecFlag = 1;
    OCIAttrGet(DBProc->phSQL, OCI_HTYPE_STMT, &nNumCols,
                        0, OCI_ATTR_PARAM_COUNT, DBProc->phError) ;

    //==== 4. 赋值列数、行数 =====
    m_nNumCols = nNumCols ;

    int        i,j;
    ub2        nFDLen, nFDType ;
    ub4        nColNameSize ;
    OCIParam * opOCIParm ;
    text *     szGettedColName ;
    char *     szColName ;

    m_sBindValue      = new char * [m_nNumCols] ;
    ASSERT (m_sBindValue != NULL);
    m_DataSet   = new TCStringGrid(m_nNumCols,0) ;
    ASSERT(m_DataSet != NULL);

    if ( m_sBindValue == NULL || m_DataSet == NULL )
        throw TCException("TCQuery::Open() Could not allocate memory") ;

   //====== 1. 逐列取得列属性，并绑定列值 =======
    for (i = 1 ; i <= m_nNumCols ; i ++)
    {
        //====== 1.1 取得列属性 ======
        nStatus = OCIParamGet(DBProc->phSQL, OCI_HTYPE_STMT, DBProc->phError,
                (void **)&opOCIParm,(ub4) i);

        if ( nStatus != OCI_SUCCESS )
        {
            m_Database->CheckError(nStatus) ;
            for ( j = 0 ; j < i ; j++)
                    delete[] m_sBindValue[j] ;
            delete[] m_sBindValue ;

            Close() ;
            return false ;
        }

        //====== 1.2 取得列宽度 =========
        m_Database->CheckError(OCIAttrGet((dvoid*) opOCIParm,
                (ub4) OCI_DTYPE_PARAM, (dvoid*) &nFDLen,(ub4 *) 0,
                (ub4) OCI_ATTR_DATA_SIZE, (OCIError *) DBProc->phError));

        //====== 1.3 取得列类型 ========
        m_Database->CheckError(OCIAttrGet((dvoid*)opOCIParm,
                (ub4)OCI_DTYPE_PARAM, (dvoid*)&nFDType, (ub4 *)0,
                (ub4)OCI_ATTR_DATA_TYPE, (OCIError *) DBProc->phError )) ;

        //====== 1.4 取得列名及列名长度 =======
        m_Database->CheckError(OCIAttrGet((dvoid *)opOCIParm,
                (ub4)OCI_DTYPE_PARAM, (dvoid*)&szGettedColName,
                (ub4 *)&nColNameSize, (ub4)OCI_ATTR_NAME,
                (OCIError *)DBProc->phError)) ;

        //===== 1.5 给m_DataSet赋予取得的列名信息 =====
        szColName = new char[nColNameSize + 1] ;
        ASSERT(szColName != NULL);

        memset(szColName,0x00,nColNameSize + 1) ;
        memcpy(szColName,szGettedColName,nColNameSize) ;

        m_DataSet->SetColName(i - 1,TCString((char *)szColName)) ;
        //modified by liqing on 2002.12.08
//        delete szColName ;
        delete [] szColName ;

        //======= 1.6 绑定列到buffer =====
        m_sBindValue[i - 1] = new char [nFDLen + 1] ;
        ASSERT(m_sBindValue[i - 1] != NULL);
        memset(m_sBindValue[i - 1],0x00,nFDLen + 1) ;

        nStatus = OCIDefineByPos(DBProc->phSQL, &DBProc->phDefine[i],
                DBProc->phError, (ub4)i, (dvoid *)m_sBindValue[i - 1],
                (sb4)nFDLen + 1, SQLT_STR, (dvoid *)&DBProc->NullFlag[i],
                (ub2 *)0, (ub2 *)0, OCI_DEFAULT);

        if( nStatus != OCI_SUCCESS)
        {
            m_Database->CheckError(nStatus);

            for ( j = 0 ; j < i ; j++)
                    delete[] m_sBindValue[j] ;
            delete[] m_sBindValue ;

            Close() ;
            return false ;
        }
    }

    //==========增加部分===========
    m_nCurrNum = 0;
    m_nCurrentRow = 0;
    m_bFetchFlag    = false;
    m_bEndFlag      = false;
    for ( i = 0 ; i <= 50000; i++ ) m_DataSet->AddRows(1);

    return true ;
}

//==========================================================================
// 函数 : TCQuery::FetchE
// 用途 :  取结果
// 原型 : bool FetchE();
// 参数 : 无
// 返回 : 是否成功执行
// 说明 :
//==========================================================================
int TCQuery::FetchE()
{
    sword   nStatus;
    long    i, j;
    OCIDBPROCESS *DBProc = m_Database->GetDBProc();

    if ( m_nCurrentRow == 50000 )
    {
        m_nCurrNum = 0;
        m_nCurrentRow = 0;
    }

    long nRow = 0;
    //========= 1. 检取结果集 ========
    while( m_nCurrNum < 50000 && !m_bEndFlag )
    {
        //===== 2.1 取出一行结果 =======
        nStatus = OCIStmtFetch(DBProc->phSQL, DBProc->phError,
             (ub4)1,(ub4) OCI_FETCH_NEXT,(ub4) OCI_DEFAULT);

        //===== 2.2 如果成功取出，则增加结果集 =====
        if(nStatus == OCI_SUCCESS || nStatus == OCI_SUCCESS_WITH_INFO)
        {
            for (i = 0; i < m_nNumCols ; i++)
                m_DataSet->SetCell(i,nRow, m_sBindValue[i]) ;
        }
        //======= 2.3如果已取无数据，则退出循环 ======
        else if( nStatus == OCI_NO_DATA)
        {
            m_bEndFlag = true;
            break ;
        }
        //===== 2.4 如返回值出错，则进行出错处理 ======
        else
        {
            m_Database->CheckError(nStatus);

            for (i = 0 ; i < m_nNumCols ; i++) delete[] m_sBindValue[i] ;
            delete[] m_sBindValue ;

            Close() ;
            return -1 ;
        }
        //====== 2.5 取完一行，清空一行的结果 =======
//modi by chang at 20030520 sizeof(m_sBindValue[i])为指针的长度4，直接置'\0'

        for (i = 0 ; i < m_nNumCols ; i++)
            m_sBindValue[i][0] = '\0';
            //memset(m_sBindValue[i],0x00,sizeof(m_sBindValue[i])) ;

        nRow++;
        m_nCurrNum++;
    }

    //===== 3. 释放缓冲区 =======
    m_nRecordCount += nRow;
//deleted by liqing on 2002.12.13
//    if ( m_nRecordCount == 0 )  return 1;

    if ( nStatus == OCI_NO_DATA )
    {
        for (i = 0 ; i < m_nNumCols ; i++ ) delete[] m_sBindValue[i];

        delete[] m_sBindValue;
        m_bEndFlag = true;
    }

    //modified by liqing on 2002.12.13修正程序柝构时不断柝构TCString
    if ( m_nRecordCount == 0 )  return 1;

    if ( m_nCurrentRow == 0 )
        m_nCurrentRow = 1;
    else
        m_nCurrentRow++;

    if ( m_bEndFlag  && m_nCurrentRow == m_nCurrNum + 1 )
        return 1;
    else
        return 0;
}




