#include "cmpublic.h"
#include "cmpublic_db.h"

TCTable::TCTable()
{
	DBColumns = NULL ;
	ColumnCount = 0 ;
}

void TCTable::Close()
{
	if (DBColumns)
		delete[] DBColumns ;
	DBColumns = NULL ;
}

TCTable::~TCTable()
{
	Close() ;
}

bool TCTable::Open(TCString sTableName)
{   return Open(DatabaseMain, sTableName);
}

bool TCTable::Open(TCDatabase & db,TCString sTableName)
{
    sword         retval;
    OCIParam      *parmp, *collst;
    OCIParam      *colhd;
    OCIDescribe   *dschp;
    ub4           parmcnt, colnamesize;
    ub2           numcols;
    ub4           objid = 0;
    ub2           i, colsize, coltype;
    text          *colname;


	if (!db.IsConnected()) return false ;

	OCIDBPROCESS * DBProc = db.GetDBProc() ;
	if (DBProc == NULL) return false ;

	db.CheckError (OCIHandleAlloc((dvoid *)DBProc->phEnviron,
                                            (dvoid **) &dschp,
			                                (ub4) OCI_HTYPE_DESCRIBE,
			                                (size_t) 0, (dvoid **) 0));

	 if ((retval = OCIDescribeAny(DBProc->phService,DBProc->phError,
                                (dvoid *)((char *)sTableName),
                                 (ub4)Length(sTableName),
                                 OCI_OTYPE_NAME, (ub1)1,
                                 OCI_PTYPE_TABLE, dschp)) != OCI_SUCCESS)
  {
    if (retval != OCI_NO_DATA)
    {
       /* OCI_ERROR */
      db.CheckError(retval);
    }
    return false ;
  }

 	db.CheckError (OCIAttrGet((dvoid *)dschp,
                             (ub4)OCI_HTYPE_DESCRIBE,
			     (dvoid *)&parmp,
			     (ub4 *)0, (ub4)OCI_ATTR_PARAM,
			     (OCIError *)DBProc->phError));

        /* Get the attributes of the table */
    db.CheckError ( OCIAttrGet((dvoid*) parmp,
                             (ub4) OCI_DTYPE_PARAM,
			     (dvoid*) &objid, (ub4 *) 0,
			     (ub4) OCI_ATTR_OBJID,
			     (OCIError *)DBProc->phError));
       
	     /* column list of the table */
    db.CheckError(OCIAttrGet((dvoid*) parmp,
		     (ub4) OCI_DTYPE_PARAM, 
			 (dvoid*) &collst, (ub4 *) 0, 
			 (ub4) OCI_ATTR_LIST_COLUMNS, (OCIError *)DBProc->phError));
         
	     /* number of columns */
    db.CheckError ( OCIAttrGet((dvoid*) parmp,
		     (ub4) OCI_DTYPE_PARAM,
			 (dvoid*) &numcols, (ub4 *) 0,
			 (ub4) OCI_ATTR_NUM_COLS, (OCIError *)DBProc->phError));	                  
	
	try
	{
		DBColumns = new TCDBColumn [numcols] ;
        ASSERT(DBColumns != NULL);
	}
	catch(...)
	{
		throw TCException("could not allocate memory!") ;
	}
	
	for (i = 1; i <= numcols; i++)
	{
        /* get parameter for column i */
       retval = OCIParamGet(collst, OCI_DTYPE_PARAM, DBProc->phError,
                     (dvoid **)&colhd, i);

       if (retval != OCI_SUCCESS)
	   {
		   db.CheckError(retval);
		   return false ;
	   }

        /* get parameter for column i */
       retval = OCIAttrGet((dvoid *)colhd, (ub4)OCI_DTYPE_PARAM,
                       (dvoid*)&colname,
			           (ub4 *)&colnamesize,
                       (ub4)OCI_ATTR_NAME, (OCIError *)DBProc->phError);
	   if ( retval != OCI_SUCCESS)
	   {
			db.CheckError(retval);
			return false ;
	   }

	   DBColumns[i - 1].ColumnName = TCString((char *)colname, colnamesize) ;

       retval = OCIAttrGet(colhd, OCI_DTYPE_PARAM,
		      (void *)&coltype, 0,
			  OCI_ATTR_DATA_TYPE, DBProc->phError);
	   if ( retval != OCI_SUCCESS )
	   {
			db.CheckError(retval);
			return false;
	   }
	   //DBColumns[i - 1].ColumnDataType = GetDataType(coltype) ;
       DBColumns[i - 1].ColumnDataType = coltype ;

       ub2 nTempColumnSize;
       retval = OCIAttrGet(colhd, OCI_DTYPE_PARAM,
   	         (dvoid *)&nTempColumnSize, 0,
			 OCI_ATTR_DATA_SIZE, DBProc->phError);

       DBColumns[i - 1].ColumnSize = nTempColumnSize;
	   if (retval != OCI_SUCCESS )
	   {
			db.CheckError(retval);
			return false ;
	   }
     }

     /*  free Paramter handle */
  (void) OCIHandleFree((dvoid *) colhd,(ub4) OCI_DTYPE_PARAM);
  (void) OCIHandleFree((dvoid *) collst,(ub4) OCI_DTYPE_PARAM);
  (void) OCIHandleFree((dvoid *) parmp,(ub4) OCI_DTYPE_PARAM);

      /* free the describe handle */
  OCIHandleFree((dvoid *) dschp, (ub4) OCI_HTYPE_DESCRIBE);

  TableName 	= sTableName ;
  ColumnCount 	= numcols ;
  Database 		= &db ;

  return true;
}

TCString& TCTable::GetColumnName(int nCol)
{
	TCString sCol ="";
   	//if (DBColumns == NULL) return (TCString &)((TCString)"") ;
   	//if (nCol > ColumnCount || nCol < 1) return (TCString &)((TCString)"");
	if (DBColumns == NULL) return (TCString &)(sCol) ;
	if (nCol > ColumnCount || nCol < 1) return (TCString &)(sCol);
	return (DBColumns[nCol - 1].ColumnName) ;
}

int TCTable::GetColumnDataType(int nCol)
{
	if (DBColumns == NULL)
        throw TCException("GetColumnDataType() Error");

	if (nCol > ColumnCount || nCol < 1)
        throw TCException("GetColumnDataType() Error");

	return (DBColumns[nCol - 1].ColumnDataType) ;
}

TEDBDataType TCTable::GetColumnDBDataType(int nCol)
{
    return ConvertToDBDataType(GetColumnDataType(nCol));
}

int TCTable::GetColumnSize(int nCol)
{
	if (DBColumns == NULL) return NULL ;

	if (nCol > ColumnCount || nCol < 1) return NULL ;

	return (DBColumns[nCol - 1].ColumnSize) ;
}

int TCTable::GetColumnCount()
{
	return ColumnCount ;
}


TEDBDataType TCTable::ConvertToDBDataType(ub2 dt)
{
  switch(dt)
  {
      case (SQLT_STR):
        return (dtString);

      case (SQLT_INT):
      case (SQLT_NUM):
      case (SQLT_VCS):
        return (dtInteger);

      case (SQLT_FLT):
        return (dtFloat) ;

      case (SQLT_AFC):
      case (SQLT_CHR):
        return (dtString);

      default:
        return(dtString) ;
  }
}

bool TCTable::TableExists(const TCString & sTableName)
{
    const long OCI_OBJECT_NOT_EXISTS = 4043;

    DatabaseMain.SetExceptionMode(false);

    TCTable tblTable;

    if (!tblTable.Open(sTableName))
    {
        if (DatabaseMain.GetErrorCode() == OCI_OBJECT_NOT_EXISTS)
        {   tblTable.Close();
            DatabaseMain.SetExceptionMode(true);
            return false;
        }
        else
            throw TCException("TCTable::TableExists() Error : "
                    "Open Table - " + sTableName);
    }

    tblTable.Close();
    DatabaseMain.SetExceptionMode(true);
    return true;
}

//add by liqing on 2002.11.20
bool TCTable::TableExists(TCDatabase &tcDatabaseMain,const TCString & sTableName)
{

    const long OCI_OBJECT_NOT_EXISTS = 4043;

    tcDatabaseMain.SetExceptionMode(false);

    TCTable tblTable;

    if (!tblTable.Open(tcDatabaseMain,sTableName))
    {
        if (tcDatabaseMain.GetErrorCode() == OCI_OBJECT_NOT_EXISTS)
        {
            tblTable.Close();
            tcDatabaseMain.SetExceptionMode(true);
            return false;
        }
        else
            throw TCException("TCTable::TableExists() Error : "
                    "Open Table - " + sTableName);
    }

    tblTable.Close();
    tcDatabaseMain.SetExceptionMode(true);
    return true;
}
