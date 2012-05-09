#ifndef c_add_logH
#define c_add_logH

#include "cmpublic.h"
//#include "c_critical_section.h"

class TCAddLog
{

//        static TCCriticalSection csLog;
public:
        void Add(const TCString&);
        void Add(char*);
};                                                      

#endif
