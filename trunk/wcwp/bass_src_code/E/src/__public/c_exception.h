//---------------------------------------------------------------------------

#ifndef c_exceptionH
#define c_exceptionH
//---------------------------------------------------------------------------

#include "cmpublic.h"

class TCException
{
protected:
    TCString m_sMessage;
public:
    TCException(TCString sMsg);
    TCString GetMessage();
    TCString GetExceptionMessage();
};

#endif
