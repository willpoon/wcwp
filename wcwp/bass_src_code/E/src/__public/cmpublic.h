#ifndef _CMPUBLIC_H_
#define _CMPUBLIC_H_

#ifdef __WIN32__
#include <io.h>
#include <share.h>
#include <conio.h>
#include <dos.h>
#include <dir.h>
#else
#include <unistd.h>
#endif

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <limits.h>
#include <sys/stat.h>

/*
#ifndef __WIN32__
typedef  int bool;
const bool true = 1;
const bool false = 0;
#endif
*/

typedef unsigned char BYTE;
typedef unsigned short int WORD;
typedef unsigned long DWORD;

#include "c_string.h"
#include "c_list.h"
#include "pub_string.h"
#include "c_exception.h"
#include "c_file_stream.h"
#include "c_string_list.h"
#include "c_ini_file.h"
#include "c_time.h"
#include "pub_dir_file.h"
#include "c_foxdbf.h"
#include "c_list_file.h"
#include "c_application.h"
#include "c_system.h"
#include "app_config.h"
#include "c_app_log.h"
#include "c_app_error_log.h"
#include "c_bcp_prepare.h"

#define DEBUG

#ifndef DEBUG
#  define ASSERT(c)   ((void)NULL)
#else
#  define ASSERT(c) ((c) ? (void)0 : (void) DoAssertFail( \
                    "Assertion failed: %s, file %s, line %d", \
                    #c, __FILE__, __LINE__ ))
#endif
//#  define ASSERT(c) ((c) ? (void)0 : (void) printf(
//                    "Assertion failed: %s, file %s, line %d\n",
//                    #c, __FILE__, __LINE__ ))

const int MAX_PATH_LEN = 255;
const int MAX_LINE = 4096;

inline void ExitWithSystemFailLog(char *szFailedString, char *szFailType)
{
    try
    {
        printf("%s  %s", szFailType, szFailedString);
        printf("\nProgram Terminated.\n\n");

        TCFileStream fs;

        fs.Open(TAppPath::AppLog()+"sys_fail.log", omAppend|omText);

        fs.WriteLn(Application.GetAppName() + "  " + TCTime::Now() + "    " + szFailType);

        fs.WriteLn(szFailedString);

        fs.WriteLn("");
    }
    catch (...)
    {
        Application.DoApplicationQuitProcess(-1);
    }
    Application.DoApplicationQuitProcess(-1);
}

inline void DoAssertFail(char *sLine, char *sCond, char *sFile, long nLine)
{
    char szFailedString[MAX_LINE];

    sprintf(szFailedString, sLine, sCond, sFile, nLine);

    ExitWithSystemFailLog(szFailedString, "ASSERT");
}

#endif


