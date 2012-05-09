#	This is the default standard profile provided to a user.
#	They are expected to edit it to meet their own needs.

MAIL=/usr/mail/${LOGNAME:?}
PATH=/usr/bin:/usr/sbin:/usr/ccs/bin:/usr/ucb:/etc:.:$HOME/bin:$HOME/meta_agent:/oracle/product/10.2/client/rdbms/public:/oracle/product/10.2/client

MI_ROOT_DIR=/bassdb1/etl/E
MI_ROOT_DIR_ROAM=/bassdb1/etl/E
export MI_ROOT_DIR MI_ROOT_DIR_ROAM

stty erase '^H'

# The following three lines have been added by UDB DB2.
if [ -f /bassdb1/db2home/db2inst1/sqllib/db2profile ]; then
    . /bassdb1/db2home/db2inst1/sqllib/db2profile
fi

ORACLE_BASE=/oracle/product
ORACLE_HOME=${ORACLE_BASE}/10.2/client
#ORACLE_SID=test
LD_LIBRARY_PATH=/bassdb2/etl/E/boss/lib:${ORACLE_HOME}/lib:${LD_LIBRARY_PATH}:/bassdb1/etl/E/lib
LIBPATH=/bassdb1/etl/E/lib:.
NLS_LANG=AMERICAN_AMERICA.ZHS16GBK
ORA_NLS33=${ORACLE_HOME}/nls/data
TNS_ADMIN=${ORACLE_HOME}/network/admin 
PATH=$PATH:${ORACLE_HOME}/bin:${ORACLE_HOME}/OPatch
CLASSPATH=${ORACLE_HOME}/jlib:${ORACLE_HOME}/OPatch/jlib:/oracle/product/10.2/client/rdbms/public:/bassdb1/etl/E/boss/java/crm_interface/lib/ojdbc14.jar

export ORACLE_BASE ORACLE_HOME ORA_CRS_HOME ORACLE_SID LD_LIBRARY_PATH NLS_LANG ORA_NLS33 TNS_ADMIN CLASSPATH

#add for java crm_interface
CLASSPATH=.:/bassdb1/etl/E/boss/java/crm_interface/lib/ojdbc14.jar
export CLASSPATH PROPERTY_PATH

LANG=zh_CN.GBK
export LANG

IFBOSS_HOME=/bassdb2/etl/E/
export IFBOSS_HOME
