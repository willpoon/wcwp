#!/usr/bin/ksh
# 1. 数据文件以\t分割，并要去除空格
# 2. 

DATA_PATH=/bassapp/bihome/panzw/tmp
DATA_FILE=pkgid.txt
OUT_DATA_FILE=trans_pkgid.txt

cat ${DATA_PATH}/${DATA_FILE} |nawk 'BEGIN{FS="	";OFS="$"}
{
	if ( length($1) != 12 ){
		print field1 length($1)
		exit FNR
	}

	if ( length($3) != 2 ){
		print field3	$0
		exit FNR
	}

	if ( length($4) != 2 ){
		print field4	$0
		exit FNR
	}
	if ( length($5) != 1 ){
		print field5	$0
		exit FNR
	}

	if ( length($6) != 1 ){
		print field6	$0
		exit FNR
	}

	if ( length($7) != 1 ){
		print field7	$0
		exit FNR
	}

	if ( length($8) != 1 ){
		print field8	$0
		exit FNR
	}

	if ( length($9) != 3 ){
		print field9	$0
		exit FNR
	}

	if ( length($10) != 4 ){
		print field10	$0
		exit FNR
	}

	if ( length($11) != 3 ){
		print field11	$0
		exit FNR
	}
	
	#if ( length("$3$4$5$6$7$8$9$10$11") == 18）{
		print $1,$3$4$5$6$7$8$9$10$11,$2
	#}
}' > ${DATA_PATH}/${OUT_DATA_FILE}


ret=$?

if [ $ret -ne 0 ];then 
echo "第 $ret 行数据不符合规范!"
fi

db2 connect to bassdb user bass2 using bass2
db2 "load client from  ${DATA_PATH}/${OUT_DATA_FILE} of del modified by coldel$ replace into BASS1.G_I_02026_MONTH_LOAD"
db2 connect reset


#
#
#			CREATE TABLE "BASS1   "."G_I_02026_MONTH_LOAD"  (
#                  "OLD_PKG_ID" VARCHAR(12) NOT NULL , 
#                  "NEW_PKG_ID" VARCHAR(18) NOT NULL , 
#                  "PKG_NAME" CHAR(128) NOT NULL )   
#                 DISTRIBUTE BY HASH("OLD_PKG_ID", "NEW_PKG_ID")   
#                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 
#
#


 
 