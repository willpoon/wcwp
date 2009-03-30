####################################################################################
#connect to acrm database
. $cfgroot/inc/connect_db2.inc
###################################################################################
#execute sql 
#remember to pass a sql statement with a ";" as delimiter
###################
db2_exec_sql(){    
echo `date +%m-%d:%H:%M:%S` 
db2 -cvt +p <<EOF
$1
 quit;   
EOF
echo `date +%m-%d:%H:%M:%S` 
}
###################################################################################
db2_disconnect(){
db2 -cvt +p <<EOF
terminate;
EOF
}
###################################################################################
#common tasks

