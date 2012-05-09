#ifndef c_string_gridH
#define c_string_gridH

enum stSortType { stAscend = 0,stDescend = 1 } ;

class TCGridRow
{
	friend class TCStringGrid ;

	protected:
		TCGridRow*  m_PrevRow ;
		TCGridRow*  m_NextRow ;
		TCString*   m_Data ;
		int         m_Cols ;

		TCGridRow(int) ;
		~TCGridRow() ;

		void        SetCol(int nCols,TCString sCol) ;
		TCString    GetCol(int nCol) ;
} ;

class TCStringGrid
{
	private:
		TCGridRow * m_FirstRow ;
		TCGridRow * m_LastRow ;
		TCGridRow ** m_RowIndex ;

		int         m_ColCount ;
		int         m_RowCount ;
		TCString *  m_ColsName ;
		int         m_ExtendRow ;

		TCGridRow*  GotoRow(int nRow) ;
		void        CreateRowIndex() ;
		void        Release() ;
	public:
		TCStringGrid(int,int) ;
		~TCStringGrid() ;

		void        AddRows(int) ;
		int         AddRow() ;
		void        DeleteRow(int nRow) ;
		int         InsertRow(int nRow) ;

		int         GetColCount(void) ;
		int         GetRowCount(void) ;
		void        SetColName(int,TCString) ;
		TCString    GetColName(int) ;
		void        SetCell(int,int,TCString) ;
		void        SetLastCell(int nCol,TCString sCell) ;
		TCString    GetCell(int,int) ;
		void        Clear() ;
		void        Sort(int,stSortType) ;
		void        Sort(TCString,stSortType) ;
		void        QuickSort(int,long,long,stSortType) ;

        int         Find(int nCol,TCString sStr) ;

} ;

#endif

