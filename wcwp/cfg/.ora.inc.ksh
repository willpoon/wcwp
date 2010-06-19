. $PZW_INC/common.ora.inc

ORACLE_HOME=/usr/lib/oracle/xe/app/oracle/product/10.2.0/server
export ORACLE_HOME
export ORACLE_SID=XE
PATH=$PATH:$ORACLE_HOME/bin
export PATH
alias startora="sh $shroot/ora_startup.sh"
alias runo="sh $shroot/ora_startup.sh"
TNS_ADMIN=$cfgroot/cfg
export TNS_ADMIN


#alias
alias lctl='lsnrctl'
