####################################################################################
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

ora(){    

#if [ $# -eq 1 ];then 
#  UnP=${UnP6} ;
#fi

case $2 in
0)  UnP=${UnP0};;
1)  UnP=${UnP1};;
6)  UnP=${UnP6};;
*)  UnP=${UnP6};;
esac

sqlplus -s  ${UnP} <<_eof
	set pagesize 0 linesize 200 feedback off
	$1
_eof

}

tm(){
echo $1
}

curr_mo(){
ora_ret "SELECT to_char(sysdate,'yyyymm') last_date FROM dual;"; echo $ret
}

today(){
ora_ret "SELECT to_char(sysdate,'yyyymmdd') last_date FROM dual;"; echo $ret
}

yesterday(){
ora_ret "SELECT to_char(sysdate-1,'yyyymmdd') last_date FROM dual;"; echo $ret
}

tomorrow(){
ora_ret "SELECT to_char(sysdate+1,'yyyymmdd') last_date FROM dual;"; echo $ret
}

first_date(){
echo $1"01"
}

last_date(){
tm_intrvl_cd=$1
ora_ret "SELECT to_char(LAST_DAY(to_date('$tm_intrvl_cd','yyyymm')),'yyyymmdd') last_date FROM dual;"; echo $ret
}

last_mo(){
tm_intrvl_cd=$1
ora_ret "SELECT to_char(add_months(to_date('$tm_intrvl_cd','yyyymm'),-1),'yyyymm') last_mo FROM dual;"; echo $ret
}
next_mo(){
tm_intrvl_cd=$1
ora_ret "SELECT to_char(add_months(to_date('$tm_intrvl_cd','yyyymm'),1),'yyyymm') last_mo FROM dual;"; echo $ret
}

ref_date(){
echo ;
}

ref_mo(){
echo ;
}

#p_n_mo yyyymmdd n
p_n_date(){
tm_intrvl_cd=$1
n=$2
ora_ret "SELECT to_char((to_date('$tm_intrvl_cd','yyyymmdd')-$n),'yyyymmdd') p_n_date FROM dual;"; echo $ret
}

#p_n_mo yyyymm n
p_n_mo(){
tm_intrvl_cd=$1
n=$2
ora_ret "SELECT to_char(add_months(to_date('$tm_intrvl_cd','yyyymm'),-$n),'yyyymm') p_n_date FROM dual;"; echo $ret
}


ft(){
arg1=$1"  ";
keyword1=$( echo "$arg1"|tr '[a-z]' '[A-Z]'|cut -d" " -f 1,1 )
keyword2=$( echo "$arg1"|tr '[a-z]' '[A-Z]'|cut -d" " -f 2,2 )
keyword3=$( echo "$arg1"|tr '[a-z]' '[A-Z]'|cut -d" " -f 3,3 )

ora "
select distinct table_name from all_tables where table_name like '%${keyword1}%${keyword2}%${keyword3}%';
" $2
}

fruno(){
#startup database instance
check_stat=`ps -ef|grep ${ORACLE_SID}|grep pmon|wc -l`;
oracle_num=`expr $check_stat`
if [ $oracle_num -gt 0 ]
then
return 0
else
sqlplus sys/oracle as sysdba <<eof             
startup;
eof
fi
}

runlsn(){
ORACLE_SID=XE
LSNR=$ORACLE_HOME/bin/lsnrctl
SU=/bin/su
ORACLE_OWNER=oracle
ret=`ps -ef|grep -i lsnr |grep -v grep|wc -l`
if [ $ret -ge 1 ]; then 
return 0
$SU -s /bin/bash $ORACLE_OWNER -c "$LSNR  stop" 
fi
if [ $ret -lt 1 ]; then 
$SU -s /bin/bash $ORACLE_OWNER -c "$LSNR  start" 
fi
}


rora(){
runlsn;
fruno;
}



ora_exec_sql2(){
orauser=`cat /tmp/orauser`

    case $orauser in 
        SYS)
            UnP=${UnP0}
        ;;
        SYSTEM)
            UnP=${UnP1}
        ;;
        PZW)
            UnP=${UnP6}
        ;;
        *)
            UnP=${UnP6}
        ;;
    esac;

sqlplus -s ${UnP}<<EOF
set echo off
set pagesize 0
set heading on
set echo on
$1
quit
EOF
}


