#ifndef c_queryH
#define c_queryH

#include "cmpublic.h"
#include "cmpublic_db.h"

#ifdef  __ORACLECLASS
#include <oci.h>
#endif

#include "c_string_grid.h"

//--------------------------------------------------------------------------
// 类   : TCQuery
// 说明 : 封装SQL查询
//--------------------------------------------------------------------------
class TCQuery
{
	private:
		TCStringGrid *  m_DataSet ;         // 用来保存SELECT返回结果集
		TCDatabase *    m_Database ;        // 关联的数据库对象
		TCString	    m_sSQL ;            // 设定的发送的SQL语句

		long		    m_nRecordCount ;    // SELECT返回的结果集行数
		long		    m_nCurrentRow ;     // 当前记录号
        long            m_nNumCols ;        // SELECT的列数

        void            _Init();            // 由构造函数调用，初始化成员变量
        bool            CheckDBActive();    // 检查数据库是否激活, 并可能抛例外

        // 因增加类ftechE增加变量
        bool            m_bFetchFlag;       // 取数据的标志
        long            m_nCurrNum;        // 每次Fetch容器的条数
        char            **m_sBindValue;     // 绑定的值
        bool            m_bEndFlag;
        

        bool   NeedThrowException()         // 是否要抛例外
        {   return m_Database->GetExceptionMode();
        }

	public:
		TCQuery() ;                         //构造函数
		TCQuery(TCDatabase &) ;             //构造函数
		~TCQuery() ;                        //柝构函数

		bool	IsActive() ;                //TCQuery类是否已连接数据库
		bool	Open() ;                    //执行SELECT操作
		void	Close() ;                   //关闭本次结果集

        bool    OpenE();                    // 执行SELECT操作,不取结果集
        int     FetchE();                   // 取一条结果

		bool	SetSQL(TCString) ;          //设置要执行的SQL语句
		bool	Prepare() ;                 //准备执行SQL语句
		bool	ExecSQL() ;                 //执行一次数据更新语句
        bool    ExecSQL(int nOffRow, int nRows,
                bool bRollback) ;           //执行N次数据更新语句
        bool    ExecSQL(TCString sSQL);     //执行SQL语句

		bool	First() ;                   //当前记录移到第一行
		bool	Next() ;                    //当前记录移到下一行

		long	GetRecordCount() ;          //返回SELECT结果行数
		long	GetRowsAffected() ;         //返回上一次执行的事务影响的行数
		long    GetColumnCount()
        {   return m_nNumCols; }

		TCString GetFieldValue(int) ;       //返回结果集字段值
        TCString GetFieldValue(TCString) ;  //返回结果集字段值
       	TCString GetFieldValue(int,int) ;      //返回结果集指定行字段值
        TCString GetFieldValue(int,TCString) ; //返回结果集指定行字段值

        bool    BindParamByPos(void *pBuffer,int nBufferLen,
                int nBindPos,int nBindType) ;               //按位置绑定参数
        bool    BindParamByName(void *,int,TCString,int) ;  //按参数名绑定参数
        bool    BindArray(int nBindPos,int nArraySize) ;                        //绑定结构数组
        //add by lgk
        TCString GetErrorMsg()         // 是否要抛例外
        {   return m_Database->GetErrorString();
        }
} ;

#endif


