//---------------------------------------------------------------------------

#pragma hdrstop

#include "cmpublic.h"

#include "c_exception.h"
//---------------------------------------------------------------------------

TCException::TCException(TCString sMsg)
{
    m_sMessage = sMsg;
};

TCString TCException::GetMessage()
{
    return m_sMessage;
}

TCString TCException::GetExceptionMessage()
{
    return m_sMessage;
}


