#ifndef c_tableH
#define c_tableH

#include "cmpublic.h"   
#include "cmpublic_db.h"

enum  TEDBDataType { dtString = 1,dtInteger = 2,dtFloat = 4} ;

class TCDBColumn
{
public:
    TCString ColumnName ;
    int		 ColumnDataType ;
    int		 ColumnSize ;
} ;

class TCTable
{
private:
    TCString 		TableName ;
    TCDBColumn * 	DBColumns ;
    int				ColumnCount ;
    TCDatabase *	Database ;
    TEDBDataType    ConvertToDBDataType(ub2 dt);

public:
    TCTable() ;
    ~TCTable() ;

    bool 		Open(TCDatabase &,TCString);
    bool        Open(TCString sTableName);

    void 		Close() ;
    int			GetColumnCount() ;
    TCString &	GetColumnName(int) ;

    int 	     GetColumnDataType(int) ;
    TEDBDataType GetColumnDBDataType(int);

    int 		GetColumnSize(int) ;
    TCString    GetTableName()
    {   return TableName ;
    }

    static bool TableExists(const TCString & sTableName); 
    static bool TableExists(TCDatabase &tcDatabaseMain,const TCString & sTableName);
} ;

#endif

