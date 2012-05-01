//~ c:\DB2EXPC\samples\c\bldrtn.bat syscall

#include <stdlib.h>
#include <sqludf.h>
void SQL_API_FN systemCall(
        SQLUDF_VARCHAR *command,     /* input */
        SQLUDF_INTEGER *result,      /* output */
        /* null indicators */
        SQLUDF_NULLIND *command_ind,
        SQLUDF_NULLIND *result_ind,
        SQLUDF_TRAIL_ARGS)
{
    int rc = 0;
    if (SQLUDF_NULL(command_ind)) {
        *result_ind = -1;
        return;
    }
    /* execute the command */
    rc = system(command);
    *result_ind = 0;
    *result = rc;
}
