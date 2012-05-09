#include "cmpublic.h"
#include "c_string_grid.h"

#define INIT_ROWS 500

#define MAX(a,b)    a > b ? a : b
///////////////////////////////////////////////////////////////////////////////
TCGridRow::TCGridRow(int nCols)
{
	ASSERT(nCols > 0) ;

	m_Data      = NULL ;
	m_PrevRow   = NULL ;
	m_NextRow   = NULL ;
	m_Cols      = nCols ;

	try
	{
		m_Data  = new TCString [nCols] ;
        ASSERT(m_Data != NULL);
	}
	catch(...)
	{
		throw TCException("Could not allocate memory") ;
	}
}
///////////////////////////////////////////////////////////////////////////////
TCGridRow::~TCGridRow()
{
  delete[] m_Data ;
}
///////////////////////////////////////////////////////////////////////////////
void TCGridRow::SetCol(int nCols,TCString sCol)
{
	ASSERT(m_Data != NULL) ;
	ASSERT(nCols < m_Cols && nCols >= 0) ;
	m_Data[nCols] = sCol ;
}

///////////////////////////////////////////////////////////////////////////////
TCString TCGridRow::GetCol(int nCols)
{
	ASSERT(m_Data != NULL) ;
	ASSERT(nCols < m_Cols && nCols >= 0) ;
	return m_Data[nCols] ;
}
///////////////////////////////////////////////////////////////////////////////
TCStringGrid::TCStringGrid(int nCols,int nRows)
{
	ASSERT(nCols > 0 && nRows >= 0) ;

	m_FirstRow  = NULL ;
	m_LastRow   = NULL ;

	m_ColCount  = nCols ;
	m_RowCount  = 0 ;

	m_ExtendRow = INIT_ROWS ;
	m_RowIndex  = new TCGridRow * [m_ExtendRow] ;
    ASSERT(m_RowIndex != NULL);

	try
	{
		m_ColsName = new TCString[nCols] ;
        ASSERT(m_ColsName != NULL);
	}
	catch(...)
	{
		throw TCException("Cauld not allocate memory") ;
	}

	if (nRows > 0)
		AddRows(nRows) ;
}
///////////////////////////////////////////////////////////////////////////////
TCStringGrid::~TCStringGrid()
{
   Release() ;
}
///////////////////////////////////////////////////////////////////////////////
void TCStringGrid::Release()
{
	 int nRows = m_RowCount ;

	TCGridRow *pRow ;
	for ( int i = nRows - 1 ; i >= 0 ; i --)
	{
		pRow = m_LastRow ;
		if (m_LastRow->m_PrevRow)
		{
			m_LastRow->m_PrevRow->m_NextRow = NULL ;
			m_LastRow = m_LastRow->m_PrevRow ;
		}

		delete pRow ;
	  //DeleteRow(i) ;

	}
	delete [] m_ColsName ;

	if (m_RowIndex != NULL)
		delete[] m_RowIndex ;

}
///////////////////////////////////////////////////////////////////////////////
void TCStringGrid::DeleteRow(int nRow)
{
	ASSERT (nRow < m_RowCount && nRow >= 0)  ;

	TCGridRow *pRow,*pNextRow,*pPrevRow ;
	pRow = GotoRow(nRow) ;
	pNextRow = pRow->m_NextRow ;
	pPrevRow = pRow->m_PrevRow ;

	delete pRow ;

	// delete the first row
	if (pPrevRow == NULL && pNextRow != NULL)
	{
		m_FirstRow = pNextRow ;
		pNextRow->m_PrevRow = NULL ;

		if (m_RowCount > 1)
		{
		  memmove(&m_RowIndex[nRow],&m_RowIndex[nRow + 1],
				(m_RowCount - nRow - 1) * sizeof(TCGridRow *)) ;
		}
		else
		{
		  delete [] m_RowIndex ;
		  m_ExtendRow = INIT_ROWS ;
		  m_RowIndex  = new TCGridRow * [m_ExtendRow] ;
          ASSERT(m_RowIndex != NULL);
		}
	}  // delete the last row
	else if (pNextRow == NULL && pPrevRow != NULL)
	{
		m_LastRow = pPrevRow ;
		pPrevRow->m_NextRow = NULL ;
		 if (m_RowCount <= 1)
		{
		  delete [] m_RowIndex ;
		  m_ExtendRow = INIT_ROWS ;
		  m_RowIndex  = new TCGridRow * [m_ExtendRow] ;
          ASSERT(m_RowIndex != NULL);
		}
	}   // delete the middle row
	else if (pNextRow != NULL && pPrevRow != NULL)
	{
	  pPrevRow->m_NextRow = pNextRow ;
	  pNextRow->m_PrevRow = pPrevRow ;

	  memmove(&m_RowIndex[nRow],&m_RowIndex[nRow + 1],
		(m_RowCount - nRow - 1) * sizeof(TCGridRow *)) ;

	}

	m_RowCount -- ;

}
///////////////////////////////////////////////////////////////////////////////
void TCStringGrid::Sort(int nCol,stSortType st)
{
  if (m_RowCount > 1)
	QuickSort(nCol,0,m_RowCount - 1,st) ;
}
///////////////////////////////////////////////////////////////////////////////
void TCStringGrid::Sort(TCString sColName,stSortType st)
{
  bool bFound = false ;
  int i ;
  if (sColName == TCString("") ) return ;

  for (i = 0 ; i < m_ColCount ; i ++)
  {
	 if (UpperCase(m_ColsName[i]) == UpperCase(sColName))
	 {
	   bFound = true ;
	   break ;
	 }
  }
  if (!bFound)
	throw TCException("The column name:" + sColName + " doesn't exsits!") ;

  Sort(i,st) ;
}

///////////////////////////////////////////////////////////////////////////////
void TCStringGrid::QuickSort(int nCol,long nL ,long nR,stSortType st)
{
 long i,j,p ;
 TCString S ;
 TCGridRow * pRow1,*pRow2 ;
 do
 {
   i = nL ;
   j = nR ;
   p = (nL + nR) >> 1 ;
   do
   {
	 if (st == stAscend)
	 {
		while ( m_RowIndex[i]->GetCol(nCol) < m_RowIndex[p]->GetCol(nCol)) i++;
		while ( m_RowIndex[j]->GetCol(nCol) > m_RowIndex[p]->GetCol(nCol) ) j--;
	 }
	 else
	 {
	   while ( m_RowIndex[i]->GetCol(nCol) > m_RowIndex[p]->GetCol(nCol)) i++;
	   while ( m_RowIndex[j]->GetCol(nCol) < m_RowIndex[p]->GetCol(nCol) ) j--;
	 }
	  if ( i <= j )
	  {
	// swap the two rows
	   pRow1 = m_RowIndex[i] ;
	   pRow2 = m_RowIndex[j] ;
	   S = pRow1->GetCol(nCol) ;
	   pRow1->SetCol(nCol,pRow2->GetCol(nCol)) ;
	   pRow2->SetCol(nCol,S) ;

		if ( p == i )
		   p = j ;
		else if (p == j)
				p = i;
		i++ ;
		j-- ;
	  }
   } while(i <= j) ;
   if (nL < j )
	  QuickSort(nCol,nL, j,st);
   nL = i ;
 } while(i < nR) ;

}

///////////////////////////////////////////////////////////////////////////////
void TCStringGrid::Clear()
{
 	Release () ;
 	TCStringGrid(m_ColCount,0) ;
}
///////////////////////////////////////////////////////////////////////////////
int TCStringGrid::InsertRow(int nRow)
{
	ASSERT (nRow < m_RowCount && nRow >= 0)  ;

	if (m_RowCount == 0)
	{
		return AddRow() ;
	}

	TCGridRow *pRow,*pPrevRow,*pNextRow,*pNewRow ;
	 try
	{
		pNewRow = new TCGridRow(m_ColCount) ;
        ASSERT(pNewRow != NULL);
	}
	catch(...)
	{
		throw TCException("Cauld not allocate memory") ;
	}

	pRow = GotoRow(nRow) ;
	pPrevRow = pRow->m_PrevRow ;
	pNextRow = pRow->m_NextRow ;

	// insert at the first row
	if (pPrevRow == NULL && pNextRow != NULL)
	{
		m_FirstRow->m_PrevRow = pNewRow ;
		pNewRow->m_NextRow = m_FirstRow ;
		m_FirstRow = pNewRow ;

	}  // insert at the last row
	else if (pNextRow == NULL && pPrevRow != NULL)
	{
		m_LastRow->m_PrevRow = pNewRow ;
		pNewRow->m_NextRow = m_LastRow ;
		m_LastRow = pNewRow ;
	}   // insert at the middle row
	else if (pNextRow != NULL && pPrevRow != NULL)
	{
	  pPrevRow->m_NextRow = pNewRow ;
	  pNewRow->m_PrevRow = pPrevRow ;

	  pRow->m_PrevRow = pNewRow ;
	  pNewRow->m_NextRow = pRow ;

	}
	CreateRowIndex() ;
	memmove(&m_RowIndex[nRow + 1],&m_RowIndex[nRow],
			(m_RowCount - nRow) * sizeof(TCGridRow *)) ;
	m_RowIndex[nRow] = pNewRow ;

	m_RowCount ++ ;

    return nRow ;
}
///////////////////////////////////////////////////////////////////////////////
void TCStringGrid::AddRows(int nRows)
{
	for (int i = 0 ; i < nRows ; i ++)
		AddRow() ;
}
///////////////////////////////////////////////////////////////////////////////
int TCStringGrid::AddRow()
{
	TCGridRow * pRow ;
	try
	{
		pRow = new TCGridRow(m_ColCount) ;
        ASSERT(pRow != NULL);
	}
	catch(...)
	{
		throw TCException("Cauld not allocate memory") ;
	}

	if (m_FirstRow != NULL)
	{
	   pRow->m_PrevRow = m_LastRow ;
	   m_LastRow->m_NextRow = pRow ;
	   m_LastRow = pRow ;
	}
	else
	{
		m_FirstRow = pRow ;
		m_LastRow = pRow ;
	}
	CreateRowIndex() ;
	m_RowIndex[m_RowCount] = pRow ;

	m_RowCount ++ ;

    return m_RowCount - 1  ;
}
///////////////////////////////////////////////////////////////////////////////
void TCStringGrid::CreateRowIndex()
{
  	if (m_RowCount + 1 <= m_ExtendRow)
   	{
	  return ;
   	}

	//out of buffer pool, precreate a new buffer pool

	if ( m_ExtendRow > 10000 )
		m_ExtendRow += 5000 ;
	else
		m_ExtendRow += m_ExtendRow / 2 ;

	TCGridRow ** pBuffer ;
	try
	{
		pBuffer = new TCGridRow * [m_RowCount] ;
        ASSERT(pBuffer != NULL);
	}
	catch(...)
	{
		throw TCException("Could not allocate memory") ;
	}

	memcpy(pBuffer,m_RowIndex,m_RowCount * sizeof(TCGridRow *)) ;
	delete [] m_RowIndex ;

	try
	{
		m_RowIndex = new TCGridRow * [m_ExtendRow] ;
        ASSERT(m_RowIndex != NULL);
	}
	catch(...)
	{
		throw TCException("Could not allocate memory") ;
	}

	memcpy(m_RowIndex,pBuffer,m_RowCount * sizeof(TCGridRow *)) ;

	delete [] pBuffer ;
}
///////////////////////////////////////////////////////////////////////////////
void TCStringGrid::SetCell(int nCol,int nRow,TCString sCell)
{
	ASSERT (nCol < m_ColCount && nRow < m_RowCount)  ;
	ASSERT (nCol >= 0 && nRow >= 0)  ;

	TCGridRow *pRow = GotoRow(nRow) ;
	pRow->SetCol(nCol,sCell) ;

}
///////////////////////////////////////////////////////////////////////////////
void TCStringGrid::SetLastCell(int nCol,TCString sCell)
{
	ASSERT (nCol < m_ColCount && nCol >= 0)  ;
	ASSERT (m_LastRow != NULL) ;
	m_LastRow->SetCol(nCol,sCell) ;
}

///////////////////////////////////////////////////////////////////////////////
TCGridRow* TCStringGrid::GotoRow(int nRow)
{
	//TCGridRow * pRow = m_FirstRow ;
	//for ( int i = 0 ; i < nRow ; i++)
	//{
	//  pRow = pRow->m_NextRow ;
	//}
	//return pRow ;
	return m_RowIndex[nRow] ;
}
///////////////////////////////////////////////////////////////////////////////
TCString TCStringGrid::GetCell(int nCol,int nRow)
{
	ASSERT (nCol < m_ColCount && nRow < m_RowCount) ;
	ASSERT (nCol >= 0 && nRow >= 0)  ;

	TCGridRow *pRow = GotoRow(nRow) ;
	return pRow->GetCol(nCol) ;
}
///////////////////////////////////////////////////////////////////////////////
int TCStringGrid::Find(int nCol,TCString sStr)
{
  ASSERT(nCol < m_ColCount && nCol >= 0) ;
  if (m_RowCount <= 0) return -1 ;
  int nFoundRow = -1 ;
  for (int i = 0 ; i < m_RowCount ; i ++)
  {
     if (GetCell(nCol,i) == sStr)
     {
        nFoundRow = i ;
        break ;
     }
  }
  return nFoundRow ;
}
///////////////////////////////////////////////////////////////////////////////
int TCStringGrid::GetColCount(void)
{
	return m_ColCount ;
}
///////////////////////////////////////////////////////////////////////////////
int TCStringGrid::GetRowCount(void)
{
	return m_RowCount ;
}
///////////////////////////////////////////////////////////////////////////////

void TCStringGrid::SetColName(int nCol,TCString sColName)
{
	ASSERT (nCol >= 0 && nCol < m_ColCount) ;
	m_ColsName[nCol] = sColName ;
}
///////////////////////////////////////////////////////////////////////////////
TCString TCStringGrid::GetColName(int nCol)
{
	ASSERT (nCol >= 0 && nCol < m_ColCount) ;

	return (m_ColsName[nCol]) ;
}

