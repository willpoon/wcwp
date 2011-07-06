--ref:D:\pzw\doc\103.tcl-sh\dsmp\load_dsmp.sh
drop table BASS1.G_A_02059_DAY_down20110321
CREATE TABLE BASS1.G_A_02059_DAY_down20110321
 (
  ENTERPRISE_ID         CHARACTER(20),
  USER_ID               CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (USER_ID
   ) USING HASHING;


/**
89303001406760      89301130041501      1140320100622113100
89102999718257      89157331738566      1140320091103113100

**/

len_val="2 21,22 41,42 45,46 46,47 54,55 55"
WORK_PATH=/bassapp/bihome/panzw/tmp
datafilename=13100_02059_20110320.txt
table_name=bass1.G_A_02059_DAY_down20110321
DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/${datafilename} of asc \\
\n
modified by timestampformat=\\\"YYYYMMDDHHMMSS\\\" dateformat=\\\"YYYYMMDD\\\" \\
\n
timeformat=\\\"HHMMSS\\\" \\
\n
method L (${len_val}) \\
\n
messages ${WORK_PATH}/${table_name}.msg \\
\n
replace into ${table_name} nonrecoverable\""

echo ${DB2_SQLCOMM}|sed -e 's/ $//g'

db2 connect to bassdb user bass1 using bass1 


 db2 "load client from /bassapp/bass2/panzw2/data/13100_02059_20110320.txt of asc \
 modified by timestampformat=\"YYYYMMDDHHMMSS\" dateformat=\"YYYYMMDD\" \
 timeformat=\"HHMMSS\" \
 method L (2 21,22 41,42 45,46 46,47 54,55 55) \
 messages /bassapp/bass2/panzw2/msg/bass1.G_A_02059_DAY_down20110321.msg \
 replace into bass1.G_A_02059_DAY_down20110321 nonrecoverable"

CREATE TABLE BASS1.G_A_02055_DAY_down20110321
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  PAY_TYPE              CHARACTER(1),
  CREATE_MODE           CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    TIME_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_A_02055_DAY_down20110321
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

 89103001169101      1340233200911161201005172999123113100
 89202999392356      1340233200911301201005172999123113100
 89103001063976      1340233200911091201005172999123113100  
#技巧：
#1.使用UE的列号标记。按”占位“计数。
#2.单个字符长度为1
#3.若字段长度为20，从第2个字符开始，那么就到第21字符结束。共20个字符。长度表示法为(2 21) 长度=#end-#begin+1
#4.对于长度为1的字段，如果从第26个字符开始截取，那么表示为(26 26) 长度=#end-#begin+1 。同上计法。
#ue标识字符的序号如何取得:将光标置于字符的左方，所示序号即字符位置序号！
len_val="38 45,2 21,22 25,26 26,27 27,28 28,29 36,37 37"
WORK_PATH=/bassapp/bass2/panzw2/data
datafilename=13100_02055_20110228.txt
table_name=bass1.G_A_02055_DAY_down20110321
DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/${datafilename} of asc \\
\n
modified by timestampformat=\\\"YYYYMMDDHHMMSS\\\" dateformat=\\\"YYYYMMDD\\\" \\
\n
timeformat=\\\"HHMMSS\\\" \\
\n
method L (${len_val}) \\
\n
messages ./${table_name}.msg \\
\n
replace into ${table_name} nonrecoverable\""

echo ${DB2_SQLCOMM}


db2 "load client from /bassapp/bass2/panzw2/data/13100_02055_20110228.txt of asc \
 modified by timestampformat=\"YYYYMMDDHHMMSS\" dateformat=\"YYYYMMDD\" \
 timeformat=\"HHMMSS\" \
 method L (38 45,2 21,22 25,26 26,27 27,28 28,29 36,37 37) \
 messages /bassapp/bass2/panzw2/msg/bass1.G_A_02055_DAY_down20110321.msg \
 replace into bass1.G_A_02055_DAY_down20110321 nonrecoverable"
 
 