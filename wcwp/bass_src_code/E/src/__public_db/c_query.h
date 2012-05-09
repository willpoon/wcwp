#ifndef c_queryH
#define c_queryH

#include "cmpublic.h"
#include "cmpublic_db.h"

#ifdef  __ORACLECLASS
#include <oci.h>
#endif

#include "c_string_grid.h"

//--------------------------------------------------------------------------
// ��   : TCQuery
// ˵�� : ��װSQL��ѯ
//--------------------------------------------------------------------------
class TCQuery
{
	private:
		TCStringGrid *  m_DataSet ;         // ��������SELECT���ؽ����
		TCDatabase *    m_Database ;        // ���������ݿ����
		TCString	    m_sSQL ;            // �趨�ķ��͵�SQL���

		long		    m_nRecordCount ;    // SELECT���صĽ��������
		long		    m_nCurrentRow ;     // ��ǰ��¼��
        long            m_nNumCols ;        // SELECT������

        void            _Init();            // �ɹ��캯�����ã���ʼ����Ա����
        bool            CheckDBActive();    // ������ݿ��Ƿ񼤻�, ������������

        // ��������ftechE���ӱ���
        bool            m_bFetchFlag;       // ȡ���ݵı�־
        long            m_nCurrNum;        // ÿ��Fetch����������
        char            **m_sBindValue;     // �󶨵�ֵ
        bool            m_bEndFlag;
        

        bool   NeedThrowException()         // �Ƿ�Ҫ������
        {   return m_Database->GetExceptionMode();
        }

	public:
		TCQuery() ;                         //���캯��
		TCQuery(TCDatabase &) ;             //���캯��
		~TCQuery() ;                        //�ع�����

		bool	IsActive() ;                //TCQuery���Ƿ����������ݿ�
		bool	Open() ;                    //ִ��SELECT����
		void	Close() ;                   //�رձ��ν����

        bool    OpenE();                    // ִ��SELECT����,��ȡ�����
        int     FetchE();                   // ȡһ�����

		bool	SetSQL(TCString) ;          //����Ҫִ�е�SQL���
		bool	Prepare() ;                 //׼��ִ��SQL���
		bool	ExecSQL() ;                 //ִ��һ�����ݸ������
        bool    ExecSQL(int nOffRow, int nRows,
                bool bRollback) ;           //ִ��N�����ݸ������
        bool    ExecSQL(TCString sSQL);     //ִ��SQL���

		bool	First() ;                   //��ǰ��¼�Ƶ���һ��
		bool	Next() ;                    //��ǰ��¼�Ƶ���һ��

		long	GetRecordCount() ;          //����SELECT�������
		long	GetRowsAffected() ;         //������һ��ִ�е�����Ӱ�������
		long    GetColumnCount()
        {   return m_nNumCols; }

		TCString GetFieldValue(int) ;       //���ؽ�����ֶ�ֵ
        TCString GetFieldValue(TCString) ;  //���ؽ�����ֶ�ֵ
       	TCString GetFieldValue(int,int) ;      //���ؽ����ָ�����ֶ�ֵ
        TCString GetFieldValue(int,TCString) ; //���ؽ����ָ�����ֶ�ֵ

        bool    BindParamByPos(void *pBuffer,int nBufferLen,
                int nBindPos,int nBindType) ;               //��λ�ð󶨲���
        bool    BindParamByName(void *,int,TCString,int) ;  //���������󶨲���
        bool    BindArray(int nBindPos,int nArraySize) ;                        //�󶨽ṹ����
        //add by lgk
        TCString GetErrorMsg()         // �Ƿ�Ҫ������
        {   return m_Database->GetErrorString();
        }
} ;

#endif


