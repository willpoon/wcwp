#include "cmpublic_db.h"

#ifdef  __ORACLECLASS
#include <oci.h>
#endif

//==========================================================================
// ���� : TCQuery::TCQuery()
// ��; : ���캯��
// ԭ�� : TCQuery()
// ���� : ��
// ���� : ��
// ˵�� : �����ȱʡ���캯����������������ݿ�Ϊȱʡ���ݿ�
//==========================================================================
TCQuery::TCQuery()
{
    m_Database = & DatabaseMain;
    _Init();
}

//==========================================================================
// ���� : TCQuery::TCQuery
// ��; : ���캯��
// ԭ�� : TCQuery(TCDatabase & db);
// ���� : ���������ݿ�
// ���� : ��
// ˵�� :
//==========================================================================
TCQuery::TCQuery(TCDatabase & db)
{
	m_Database = & db ;
    _Init();
}

//==========================================================================
// ���� : TCQuery::~TCQuery
// ��; : ��������
// ԭ�� : ~TCQuery();
// ���� : ��
// ���� : ��
// ˵�� : ����ʱ�رս����
//==========================================================================
TCQuery::~TCQuery()
{
    Close() ;
}

//==========================================================================
// ���� : TCQuery::_Init
// ��; : ��ʼ����Ա����
// ԭ�� : void _Init();
// ���� : ��
// ���� : ��
// ˵�� : �ú����������캯������
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
// ���� : TCQuery::BindArray
// ��; : ������ṹ
// ԭ�� : bool BindArray(int nBindPos,int nArraySize);
// ���� : nBindPos   -- �����Ӧ����λ��
//        nArraySize -- ���鳤��
// ���� : �Ƿ�󶨳ɹ�
// ˵�� :
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
// ���� : TCQuery::BindParamByName
// ��; : ���������������
// ԭ�� : bool BindParamByName(void *pBuffer,int nBufferlen,
//              TCString sColName,int nBindType);
// ���� : pBuffer    -- ������ַ
//        nBufferlen -- ��������
//        sColName   -- ����
//        nBindType  -- ����������
// ���� : �Ƿ�󶨳ɹ�
// ˵�� :
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
// ���� : TCQuery::BindParamByPos
// ��; : ����λ�ð��������
// ԭ�� : bool BindParamByPos(void *pBuffer,int nBufferlen,
//              int nBindPos,int nBindType);
// ���� : pBuffer    -- ������ַ
//        nBufferlen -- ��������
//        nBindPos   -- ��λ��
//        nBindType  -- ����������
// ���� : �Ƿ�󶨳ɹ�
// ˵�� : 
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
// ���� : TCQuery::CheckDBActive
// ��; : ������ݿ��Ƿ��Ѽ���
// ԭ�� : bool CheckDBActive();
// ���� : ��
// ���� : �Ƿ��Ѽ���
// ˵�� : ���δ������������������ݿ����ã������Ƿ��׳�����
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
// ���� : TCQuery::Close
// ��; : �رձ��β�ѯ
// ԭ�� : void Close()
// ���� : ��
// ���� : ��
// ˵�� :
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
// ���� : TCQuery::ExecSQL
// ��; : ִ��SQL���
// ԭ�� : bool ExecSQL();
// ���� : ��
// ���� : �Ƿ�ִ�гɹ�
// ˵�� :
//==========================================================================
bool TCQuery::ExecSQL()
{
    return ExecSQL(0, 1, true) ;
}

//==========================================================================
// ���� : TCQuery::ExecSQL
// ��; : ִ��SQL���
// ԭ�� : bool ExecSQL(TCString sSQL);
// ���� : ��
// ���� : �Ƿ�ִ�гɹ�
// ˵�� :
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
// ���� : TCQuery::ExecSQL
// ��; : ִ�ж�����ݸ��µ�SQL���
// ԭ�� : bool ExecSQL(int nOffRow,int nRows,bool bRollback);
// ���� : nOffRow   -- ִ�п�ʼ�к�
//        nRows     -- ִ�н����к�
//        bRollback -- ��������ʱ�Ƿ��Զ��ع�
// ���� : �Ƿ�ִ�гɹ�
// ˵�� :
//==========================================================================
bool TCQuery::ExecSQL(int nOffRow,int nRows,bool bRollback)
{
    //====== 1. ���ִ���������ֵ���ݿ���ýṹ =======
    OCIDBPROCESS * DBProc;
    if (!CheckDBActive())
        return false ;

    Close() ;

    DBProc = m_Database->GetDBProc() ;

    //===== 2. ִ����� ========
    sword nStatus ;
    nStatus = OCIStmtExecute(DBProc->phService, DBProc->phSQL,
                  DBProc->phError,(ub4)nRows,(ub4)nOffRow,
                 (CONST OCISnapshot *)NULL,(OCISnapshot *)NULL,
                 (ub4)OCI_DEFAULT);

    //======= 3. ������ =====
    if(nStatus != OCI_SUCCESS)
    {
        m_Database->CheckError(nStatus);
        if (bRollback)
              m_Database->Rollback(false) ;
        return false ;
    }

    //======= 4. ȷ���ύ������ =====
    if (m_Database->GetCommitMode())
       return (m_Database->Commit(false)) ;

    return true ;
}

//==========================================================================
// ���� : TCQuery::First
// ��; : ����ǰλ���ƶ�������ĵ�һ��
// ԭ�� : bool First();
// ���� : ��
// ���� : �Ƿ�ɹ�
// ˵�� : ��������Ϊ�գ��򷵻�false
//==========================================================================
bool TCQuery::First()
{
	if (m_nRecordCount == 0) return false ;

	m_nCurrentRow = 1 ;
	return true ;
}

//==========================================================================
// ���� : TCQuery::GetFieldValue
// ��; : ����λ��ȡ������е�ǰ��¼���ֶ�ֵ
// ԭ�� : TCString GetFieldValue(int nColIndex);
// ���� : ��λ�ã���1��ʼ��
// ���� : �ֶ�ֵ
// ˵�� : 1. ����޼�¼���򷵻ؿմ�
//        2. ���ص�ֵΪ�ַ�������Ҫȡ����ֵ������StrToInt����ת��
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
// ���� : TCQuery::GetFieldValue
// ��; : ����λ��ȡ�������ָ����¼���ֶ�ֵ
// ԭ�� : TCString GetFieldValue(int nRowIndex,int nColIndex);
// ���� : ��¼�ţ���1����������λ�ã���1������
// ���� : �ֶ�ֵ
// ˵�� :
//==========================================================================
TCString TCQuery::GetFieldValue(int nRowIndex,int nColIndex)
{
   if (nRowIndex <= 0 || nRowIndex > m_nRecordCount)
    return ("") ;

   m_nCurrentRow = nRowIndex ;

   return (GetFieldValue(nColIndex)) ;
}

//==========================================================================
// ���� : TCQuery::GetFieldValue
// ��; : ������ȡ������е�ǰ��¼���ֶ�ֵ
// ԭ�� : TCString GetFieldValue(TCString sColName);
// ���� : ����
// ���� : �ֶ�ֵ
// ˵�� :
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
// ���� : TCQuery::GetFieldValue
// ��; : ������ȡ�������ָ����¼���ֶ�ֵ
// ԭ�� : TCString GetFieldValue(int nRowIndex,TCString sColName);
// ���� : ��¼��(��1����)������
// ���� : �ֶ�ֵ
// ˵�� :
//==========================================================================
TCString TCQuery::GetFieldValue(int nRowIndex,TCString sColName)
{
    if (nRowIndex <= 0 || nRowIndex > m_nRecordCount)
      return "";

     m_nCurrentRow = nRowIndex;

     return GetFieldValue(sColName);
}

//==========================================================================
// ���� : TCQuery::GetRecordCount
// ��; : ������һ��SELECT�������ص�����
// ԭ�� : long GetRecordCount()��
// ���� : ��
// ���� : ���ص�����
// ˵�� :
//==========================================================================
long TCQuery::GetRecordCount()
{
	return m_nRecordCount ;
}

//==========================================================================
// ���� : TCQuery::GetRowsAffected
// ��; : ������һ�β���Ӱ�������
// ԭ�� : long GetRowsAffected();
// ���� : ��
// ���� : Ӱ�������
// ˵�� :
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
// ���� : TCQuery::IsActive
// ��; : �ж�����������ݿ⵱ǰ�Ƿ��Ѽ���
// ԭ�� : bool IsActive();
// ���� : ��
// ���� : �Ƿ��Ѽ���
// ˵�� :
//==========================================================================
bool TCQuery::IsActive()
{
	ASSERT(m_Database != NULL );

	return (m_Database->IsConnected()) ;
}

//==========================================================================
// ���� : TCQuery::Next
// ��; : ����ǰλ���ƶ����������һ��
// ԭ�� : bool Next();
// ���� : ��
// ���� : �Ƿ�ɹ�
// ˵�� : �統ǰλ���Ѵﵽ���һ�У��򷵻�false
//==========================================================================
bool TCQuery::Next()
{
	if (m_nRecordCount == 0) return false ;
	if (m_nCurrentRow >= m_nRecordCount) return false ;
    m_nCurrentRow ++ ;

	return true ;
}

//==========================================================================
// ���� : TCQuery::Open
// ��; : ׼��SELECT���, ���ѽ����������
// ԭ�� : bool Open();
// ���� : ��
// ���� : �Ƿ�ɹ�ִ��
// ˵�� :
//==========================================================================
bool TCQuery::Open()
{
    //====== 1. ���ִ���������ֵ���ݿ���ýṹ =======
    OCIDBPROCESS * DBProc;

    if (!CheckDBActive()) return false ;

    if (!Prepare()) return false ;

    Close() ;

    DBProc = m_Database->GetDBProc() ;

    sword         nStatus;

    //===== 2. ִ��SQL��� ========
    nStatus = OCIStmtExecute(DBProc->phService, DBProc->phSQL,
             DBProc->phError, (ub4) 0,(ub4)0,
             (CONST OCISnapshot *)NULL,(OCISnapshot *)NULL,
             OCI_DEFAULT);

    if((nStatus != OCI_SUCCESS) && (nStatus != OCI_NO_DATA))
    {
        m_Database->CheckError(nStatus);
        return false;
    }

    //===== 3. �õ����� ===========
    long nNumCols ;
    DBProc->ExecFlag = 1;

    OCIAttrGet(DBProc->phSQL, OCI_HTYPE_STMT, &nNumCols,
                        0, OCI_ATTR_PARAM_COUNT, DBProc->phError) ;

    //==== 4. ��bufffer��m_DataSet�����㹻�Ŀռ� =======
    char ** buffer ;

    buffer      = new char * [nNumCols] ;
    ASSERT(buffer != NULL);
    m_DataSet   = new TCStringGrid(nNumCols,0) ;
    ASSERT(m_DataSet != NULL);

    if (buffer == NULL || m_DataSet == NULL)
        throw TCException("TCQuery::Open() Could not allocate memory") ;

    //====== 5. ����ȡ�������ԣ�������ֵ =======
    long       i, j;
    ub2        nFDLen, nFDType ;
    ub4        nColNameSize ;
    OCIParam * opOCIParm ;
    text *     szGettedColName ;
    char *     szColName ;

    for (i = 1 ; i <= nNumCols ; i ++)
    {
        //====== 5.1 ȡ�������� ======
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

        //====== 5.2 ȡ���п�� =========
        m_Database->CheckError(OCIAttrGet((dvoid*) opOCIParm,
                (ub4) OCI_DTYPE_PARAM, (dvoid*) &nFDLen,(ub4 *) 0,
                (ub4) OCI_ATTR_DATA_SIZE, (OCIError *) DBProc->phError));

        //====== 5.3 ȡ�������� ========
        m_Database->CheckError(OCIAttrGet((dvoid*)opOCIParm,
                (ub4)OCI_DTYPE_PARAM, (dvoid*)&nFDType, (ub4 *)0,
                (ub4)OCI_ATTR_DATA_TYPE, (OCIError *) DBProc->phError )) ;

        //====== 5.4 ȡ���������������� =======
        m_Database->CheckError(OCIAttrGet((dvoid *)opOCIParm,
                (ub4)OCI_DTYPE_PARAM, (dvoid*)&szGettedColName,
                (ub4 *)&nColNameSize, (ub4)OCI_ATTR_NAME,
                (OCIError *)DBProc->phError)) ;

        //===== 5.5 ��m_DataSet����ȡ�õ�������Ϣ =====
        szColName = new char[nColNameSize + 1] ;
        ASSERT(szColName != NULL);

        memset(szColName,0x00,nColNameSize + 1) ;
        memcpy(szColName,szGettedColName,nColNameSize) ;

        m_DataSet->SetColName(i - 1,TCString((char *)szColName)) ;
        delete szColName ;

        //======= 5.6 ���е�buffer =====
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

    //========= 6. ��ȡ����� ========
    long nRow = 0 ;

    while(true)
    {
        //===== 6.1 ȡ��һ�н�� =======
        nStatus = OCIStmtFetch(DBProc->phSQL, DBProc->phError,
             (ub4)1,(ub4) OCI_FETCH_NEXT,(ub4) OCI_DEFAULT);

        //===== 6.2 ����ɹ�ȡ���������ӽ���� =====
        if(nStatus == OCI_SUCCESS || nStatus == OCI_SUCCESS_WITH_INFO)
        {
            m_DataSet->AddRows(1);
            
            for (i = 0; i < nNumCols ; i++)
                m_DataSet->SetCell(i, nRow, buffer[i]) ;

        }

        //======= 6.3 �����ȡ�����ݣ����˳�ѭ�� ======
        else if(nStatus == OCI_NO_DATA)
            break ;

        //===== 6.4 �緵��ֵ��������г����� ======
        else
        {
            m_Database->CheckError(nStatus);

            for (i = 0 ; i < nNumCols ; i++)
               delete[] buffer[i] ;
            delete[] buffer ;

            Close() ;
            return false ;
        }

        //====== 6.5 ȡ��һ�У����һ�еĽ�� =======
//modi by chang at 20030520 sizeof(m_sBindValue[i])Ϊָ��ĳ���4��ֱ����'\0'
        for (i = 0 ; i < nNumCols ; i++)
            buffer[i][0] = '\0';
        //  memset(buffer[i],0x00,sizeof(buffer[i])) ;

        nRow ++ ;
    }

    //===== 7. �ͷŻ����� =======
    for (i = 0 ; i < nNumCols ; i++)
        delete[] buffer[i] ;
    delete[] buffer ;

    //==== 8. ��ֵ���������� =====
    m_nNumCols = nNumCols ;
    m_nRecordCount = nRow ;

    //======= 9. �ƶ���һ�� =====
   if (m_nRecordCount > 0 )
        m_nCurrentRow = 1 ;

    First() ;

    return true ;
}

//==========================================================================
// ���� : TCQuery::Prepare
// ��; : ׼��ִ��SQL���
// ԭ�� : bool Prepare();
// ���� : ��
// ���� : �Ƿ�ɹ�׼��
// ˵�� :
//==========================================================================
bool TCQuery::Prepare()
{
    //===== 1. ������ݿ��Ƿ������� =======
	if (!CheckDBActive())
        return false ;

    //===== 2. �ͷ���ǰ��SQL�ڴ� ========
	int status;

    OCIDBPROCESS * DBProc = m_Database->GetDBProc() ;
    DBProc->ExecFlag = 0;

    if (DBProc->phSQL != NULL)
    {
        OCIHandleFree((dvoid *)DBProc->phSQL,OCI_HTYPE_STMT);
        DBProc->phSQL = NULL;
    }

    //====== 3. ����SQL�ڴ沢׼��SQL��� =======
    if((status = OCIHandleAlloc((dvoid *)DBProc->phEnviron,
              (dvoid **)&DBProc->phSQL,
              OCI_HTYPE_STMT,(size_t) 0, (dvoid **)0)) == OCI_SUCCESS)
    {
        status = OCIStmtPrepare(DBProc->phSQL,DBProc->phError,
               (text*)((char *)m_sSQL), (ub4)Length(m_sSQL),
               (ub4) OCI_NTV_SYNTAX,(ub4) OCI_DEFAULT);
    }

    //===== 4. ��鷵�ؽ�� =======
    if(status != OCI_SUCCESS)
    {
        m_Database->CheckError(status);
        return false;
    }

    return true;
}

//==========================================================================
// ���� : TCQuery::SetSQL
// ��; : ����Ҫִ�е�SQL���
// ԭ�� : bool SetSQL(TCString sSQL);
// ���� : SQL���
// ���� : �Ƿ�ɹ�����
// ˵�� :
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
// ���� : TCQuery::OpenE
// ��; : ׼��SELECT���
// ԭ�� : bool OpenE();
// ���� : ��
// ���� : �Ƿ�ɹ�ִ��
// ˵�� :
//==========================================================================
bool TCQuery::OpenE()
{
    //====== 1. ���ִ���������ֵ���ݿ���ýṹ =======
    OCIDBPROCESS * DBProc;

    if (!CheckDBActive()) return false ;
    if (!Prepare()) return false ;
    Close() ;

    DBProc = m_Database->GetDBProc() ;
    sword         nStatus;

    //===== 2. ִ��SQL��� ========
    nStatus = OCIStmtExecute(DBProc->phService, DBProc->phSQL,
             DBProc->phError, (ub4) 0,(ub4)0,
             (CONST OCISnapshot *)NULL,(OCISnapshot *)NULL,
             OCI_DEFAULT);

    if((nStatus != OCI_SUCCESS) && (nStatus != OCI_NO_DATA))
    {
        m_Database->CheckError(nStatus);
        return false;
    }

    //===== 3. �õ����� ===========
    long nNumCols ;
    DBProc->ExecFlag = 1;
    OCIAttrGet(DBProc->phSQL, OCI_HTYPE_STMT, &nNumCols,
                        0, OCI_ATTR_PARAM_COUNT, DBProc->phError) ;

    //==== 4. ��ֵ���������� =====
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

   //====== 1. ����ȡ�������ԣ�������ֵ =======
    for (i = 1 ; i <= m_nNumCols ; i ++)
    {
        //====== 1.1 ȡ�������� ======
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

        //====== 1.2 ȡ���п�� =========
        m_Database->CheckError(OCIAttrGet((dvoid*) opOCIParm,
                (ub4) OCI_DTYPE_PARAM, (dvoid*) &nFDLen,(ub4 *) 0,
                (ub4) OCI_ATTR_DATA_SIZE, (OCIError *) DBProc->phError));

        //====== 1.3 ȡ�������� ========
        m_Database->CheckError(OCIAttrGet((dvoid*)opOCIParm,
                (ub4)OCI_DTYPE_PARAM, (dvoid*)&nFDType, (ub4 *)0,
                (ub4)OCI_ATTR_DATA_TYPE, (OCIError *) DBProc->phError )) ;

        //====== 1.4 ȡ���������������� =======
        m_Database->CheckError(OCIAttrGet((dvoid *)opOCIParm,
                (ub4)OCI_DTYPE_PARAM, (dvoid*)&szGettedColName,
                (ub4 *)&nColNameSize, (ub4)OCI_ATTR_NAME,
                (OCIError *)DBProc->phError)) ;

        //===== 1.5 ��m_DataSet����ȡ�õ�������Ϣ =====
        szColName = new char[nColNameSize + 1] ;
        ASSERT(szColName != NULL);

        memset(szColName,0x00,nColNameSize + 1) ;
        memcpy(szColName,szGettedColName,nColNameSize) ;

        m_DataSet->SetColName(i - 1,TCString((char *)szColName)) ;
        //modified by liqing on 2002.12.08
//        delete szColName ;
        delete [] szColName ;

        //======= 1.6 ���е�buffer =====
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

    //==========���Ӳ���===========
    m_nCurrNum = 0;
    m_nCurrentRow = 0;
    m_bFetchFlag    = false;
    m_bEndFlag      = false;
    for ( i = 0 ; i <= 50000; i++ ) m_DataSet->AddRows(1);

    return true ;
}

//==========================================================================
// ���� : TCQuery::FetchE
// ��; :  ȡ���
// ԭ�� : bool FetchE();
// ���� : ��
// ���� : �Ƿ�ɹ�ִ��
// ˵�� :
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
    //========= 1. ��ȡ����� ========
    while( m_nCurrNum < 50000 && !m_bEndFlag )
    {
        //===== 2.1 ȡ��һ�н�� =======
        nStatus = OCIStmtFetch(DBProc->phSQL, DBProc->phError,
             (ub4)1,(ub4) OCI_FETCH_NEXT,(ub4) OCI_DEFAULT);

        //===== 2.2 ����ɹ�ȡ���������ӽ���� =====
        if(nStatus == OCI_SUCCESS || nStatus == OCI_SUCCESS_WITH_INFO)
        {
            for (i = 0; i < m_nNumCols ; i++)
                m_DataSet->SetCell(i,nRow, m_sBindValue[i]) ;
        }
        //======= 2.3�����ȡ�����ݣ����˳�ѭ�� ======
        else if( nStatus == OCI_NO_DATA)
        {
            m_bEndFlag = true;
            break ;
        }
        //===== 2.4 �緵��ֵ��������г����� ======
        else
        {
            m_Database->CheckError(nStatus);

            for (i = 0 ; i < m_nNumCols ; i++) delete[] m_sBindValue[i] ;
            delete[] m_sBindValue ;

            Close() ;
            return -1 ;
        }
        //====== 2.5 ȡ��һ�У����һ�еĽ�� =======
//modi by chang at 20030520 sizeof(m_sBindValue[i])Ϊָ��ĳ���4��ֱ����'\0'

        for (i = 0 ; i < m_nNumCols ; i++)
            m_sBindValue[i][0] = '\0';
            //memset(m_sBindValue[i],0x00,sizeof(m_sBindValue[i])) ;

        nRow++;
        m_nCurrNum++;
    }

    //===== 3. �ͷŻ����� =======
    m_nRecordCount += nRow;
//deleted by liqing on 2002.12.13
//    if ( m_nRecordCount == 0 )  return 1;

    if ( nStatus == OCI_NO_DATA )
    {
        for (i = 0 ; i < m_nNumCols ; i++ ) delete[] m_sBindValue[i];

        delete[] m_sBindValue;
        m_bEndFlag = true;
    }

    //modified by liqing on 2002.12.13���������ع�ʱ�����ع�TCString
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




