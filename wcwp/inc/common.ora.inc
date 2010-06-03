####################################################################################
. $cfgroot/cfg/.pz*se*
. $PZW_INC/ora10g.cfg.inc

#connect to acrm database
####################
#ora_connect(){
#oracle is different from db2 , do nothing here!
#}
###################################################################################
#execute sql 
#remember to pass a sql statement with a ";" as delimiter
###################################################################################

ora_catch_err(){
if [ $? = 0 ]
then
echo "so far so good on $LINENO">$LINENO.done
else
echo "there may be some problems near line $LINENO">$LINENO.err	
fi
}

###############################
ora_get_ppid(){
echo $PPID>$PPID.PPID
}
###################
###################################################################################
#ora_exec_sql()
#execute sql 
#remember to pass a sql statement with a ";" as delimiter
###################################################################################
ora_exec_sql(){    
#echo `date +%m-%d:%H:%M:%S`  
sqlplus -s $user1/$pass1<<EOF
set echo off
--alter session force parallel query parallel 15;
--alter session force parallel dml parallel 15;
--set autocommit on
set pagesize 0
set heading on
set echo on
$1
quit
EOF
#echo `date +%m-%d:%H:%M:%S`  
}

###################################################################################
#ora_disconnect(){
#oracle is different from db2 , do nothing here!
#}

###################################################################################
#common tasks

###################################################################################
#ora_ret()
#get sql return value  
###################################################################################
ora_ret(){    
ret=`sqlplus -s $user1/$pass1 <<_eof
set pagesize 0 linesize 200 feedback off
$1
_eof`
}
