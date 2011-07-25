---------------����91005�ӿ���������------------------------------
RENAME TABLE BASS2.DIM_TERM_TAC TO DIM_TERM_TAC_20110628BAK;
CREATE TABLE BASS2.DIM_TERM_TAC
 (ID             INTEGER,
  TAC_NUM        VARCHAR(15),
  TERM_ID        VARCHAR(10),
  TERM_MODEL     VARCHAR(50),
  TERMPROD_ID    VARCHAR(10),
  TERMPROD_NAME  VARCHAR(200),
  NET_TYPE       CHARACTER(1),
  TERM_TYPE      CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_DIM
  PARTITIONING KEY
   (ID
   ) USING HASHING;
ALTER TABLE BASS2.DIM_TERM_TAC
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;



drop table BASS2.DIM_TERM_TAC_MID
CREATE TABLE BASS2.DIM_TERM_TAC_MID
 (ID             INTEGER,
  TAC_NUM        VARCHAR(15),
  TERM_ID        VARCHAR(10),
  TERM_MODEL     VARCHAR(50),
  TERMPROD_ID    VARCHAR(10),
  TERMPROD_NAME  VARCHAR(200),
  NET_TYPE       CHARACTER(1),
  TERM_TYPE      CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_DIM
  PARTITIONING KEY
   (ID
   ) USING HASHING;
ALTER TABLE BASS2.DIM_TERM_TAC_MID
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
  

len_val="1 8,9 23,24 33,34 83,84 93,94 293,294 294,295 295"
WORK_PATH=/bassapp/bihome/panzw/tmp
datafilename=i_30000_201106_91005_001.dat
table_name=bass2.DIM_TERM_TAC_MID
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

echo ${DB2_SQLCOMM}|sed -e 's/ $//g'

db2 connect to bassdb user bass2 using bass2

db2 "load client from /bassapp/bihome/panzw/tmp/i_30000_201106_91005_001.dat of asc \
 modified by timestampformat=\"YYYYMMDDHHMMSS\" dateformat=\"YYYYMMDD\" \
 timeformat=\"HHMMSS\" \
 method L (1 8,9 23,24 33,34 83,84 93,94 293,294 294,295 295) \
 messages ./bass2.DIM_TERM_TAC_MID.msg \
 replace into bass2.DIM_TERM_TAC_MID nonrecoverable"
 
delete from BASS2.DIM_TERM_TAC 
insert into BASS2.DIM_TERM_TAC
select * from BASS2.DIM_TERM_TAC_MID

 insert into BASS2.DIM_TERM_TAC
select 
ID,
TAC_NUM,
TERM_ID,
TERM_MODEL,
TERMPROD_ID,
TERMPROD_NAME,
NET_TYPE,
TERM_TYPE from BASS2.DIM_TERM_TAC_20110628BAK
where net_type <>'2';


select tac_nuM,count(*) from BASS2.DIM_TERM_TAC
group by tac_nuM
having count(*)>1

select tac_nuM,count(*) from BASS2.DIM_TERM_TAC_20110628BAK
group by tac_nuM
having count(*)>1
0

/**
�������ظ���
86282100
select * from BASS2.DIM_TERM_TAC
where tac_nuM='86282100'
delete from BASS2.DIM_TERM_TAC
where tac_nuM='86282100' and term_id='25711';
commit;


**/

drop table BASS2.DIM_TERM_TAC_MID


/**

����Ϊ/bassdb1/etl/L/imei /load_imei.sh �޸������ڣ����м��ɣ������µ�91005�ӿ�ͬ��Ҳ��������⣬�˽ӿ�Ϊ��ʱ�ӿڣ���ʱ���ֹ����,
91005���ļ���91002��91003ͬһĿ¼�£�/data1/asiainfo/interface/imei/

sh load_imei.sh > load_imei.201105.out 2>&1 & 


**/