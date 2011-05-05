---------------处理91005接口数据问题------------------------------
RENAME TABLE BASS2.DIM_TERM_TAC TO DIM_TERM_TAC_20110331BAK;
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



drop table BASS2.DIM_TERM_TAC_0430
CREATE TABLE BASS2.DIM_TERM_TAC_0430
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
ALTER TABLE BASS2.DIM_TERM_TAC_0430
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
  

len_val="1 8,9 23,24 33,34 83,84 93,94 293,294 294,295 295"
WORK_PATH=/bassapp/bihome/panzw/tmp
datafilename=i_30000_201104_91005_001.dat
table_name=bass2.DIM_TERM_TAC_0430
DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/${datafilename} of asc \\
\n
modified by timestampformat=\\\"YYYYMMDDHHMMSS\\\" dateformat=\\\"YYYYMMDD\\\" \\
\n
timeformat=\\\"HHMMSS\\\" \\
\n
method L (${len_val}) \\
\n
messages /bassapp/bass2/panzw2/msg/${table_name}.msg \\
\n
replace into ${table_name} nonrecoverable\""

echo ${DB2_SQLCOMM}|sed -e 's/ $//g'

db2 connect to bassdb user bass2 using bass2

db2 "load client from /bassapp/bihome/panzw/tmp/i_30000_201104_91005_001.dat of asc \
 modified by timestampformat=\"YYYYMMDDHHMMSS\" dateformat=\"YYYYMMDD\" \
 timeformat=\"HHMMSS\" \
 method L (1 8,9 23,24 33,34 83,84 93,94 293,294 294,295 295) \
 messages ./bass2.DIM_TERM_TAC_0430.msg \
 replace into bass2.DIM_TERM_TAC_0430 nonrecoverable"
 
 
delete from BASS2.DIM_TERM_TAC 
insert into BASS2.DIM_TERM_TAC
select * from BASS2.DIM_TERM_TAC_0430

 insert into BASS2.DIM_TERM_TAC
select 
ID,
TAC_NUM,
TERM_ID,
TERM_MODEL,
TERMPROD_ID,
TERMPROD_NAME,
NET_TYPE,
TERM_TYPE from BASS2.DIM_TERM_TAC_20110331BAK
where net_type <>'2';
commit;
select tac_nuM,count(*) from BASS2.DIM_TERM_TAC_20110331BAK
group by tac_nuM
having count(*)>1
0

/**
86282100
select * from BASS2.DIM_TERM_TAC
where tac_nuM='86282100'
delete from BASS2.DIM_TERM_TAC
where tac_nuM='86282100' and term_id='25711';
commit;


**/

drop table BASS2.DIM_TERM_TAC_0430


/**

程序为/bassdb1/etl/L/imei /load_imei.sh 修改下日期，运行即可，另外新的91005接口同样也将进行入库，此接口为临时接口，暂时请手工入库,
91005的文件和91002、91003同一目录下：/data1/asiainfo/interface/imei/

**/