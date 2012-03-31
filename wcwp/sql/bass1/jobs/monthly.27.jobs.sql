#替换：
#1.DIM_TERM_TAC_20120331BAK
#2.i_30000_201108_91005_001
---------------处理91005接口数据问题------------------------------
RENAME TABLE BASS2.DIM_TERM_TAC TO DIM_TERM_TAC_20120331BAK;
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


/**
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
**/
ALTER TABLE BASS2.DIM_TERM_TAC_MID ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE

--201107 529
  

len_val="1 8,9 23,24 33,34 83,84 93,94 293,294 294,295 295"
WORK_PATH=/bassapp/bihome/panzw/tmp
datafilename=i_30000_201203_91005_001.dat
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

db2 "load client from /bassapp/bihome/panzw/tmp/i_30000_201201_91005_001.dat of asc \
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
TERM_TYPE from BASS2.DIM_TERM_TAC_20120331BAK
where net_type <>'2';


select tac_nuM,count(*) from BASS2.DIM_TERM_TAC
group by tac_nuM
having count(*)>1
2012.02:35805102       	2
2012.03:
select tac_nuM,count(*) from BASS2.DIM_TERM_TAC_20120331BAK
group by tac_nuM
having count(*)>1
0

/**
假如有重复：
35805102
select * from BASS2.DIM_TERM_TAC
where tac_nuM='35805102'
ID	TAC_NUM	TERM_ID	TERM_MODEL	TERMPROD_ID	TERMPROD_NAME	NET_TYPE	TERM_TYPE
567	35805102       	26000     	Best sonny TD902                                  	006010119 	华森                                                                                                                                                                                                    	2	1
18774	35805102	25208	SM V530	006010119	华森	1	0

select * from BASS2.DIM_TERM_TAC_20120331BAK
where tac_nuM='35805102'

ID	TAC_NUM	TERM_ID	TERM_MODEL	TERMPROD_ID	TERMPROD_NAME	NET_TYPE	TERM_TYPE
18774	35805102	25208	SM V530	006010119	华森	1	0

delete from BASS2.DIM_TERM_TAC
where tac_nuM='86282100' and term_id='25711';
commit;

delete from BASS2.DIM_TERM_TAC
where tac_nuM='35805102' and term_id='25208';

select * from BASS2.DIM_TERM_TAC
where tac_nuM='86282100'
**/

--drop table BASS2.DIM_TERM_TAC_MID

db2 RUNSTATS ON table BASS2.DIM_TERM_TAC      with distribution and detailed indexes all  


/**

程序为/bassdb1/etl/L/imei /load_imei.sh 修改下日期，运行即可，另外新的91005接口同样也将进行入库，此接口为临时接口，暂时请手工入库,
91005的文件和91002、91003同一目录下：/data1/asiainfo/interface/imei/

sh load_imei.sh > load_imei.201105.out 2>&1 & 


**/

select count(0) from    BASS2.DIM_TERM_TAC
 27790
 2012.03 27802
 select count(0) from    bass2.DIM_TERM_TAC_20120331BAK
 27776
 2012.02 27790
 
 
 
 select * from FM.FM_FILE_INTERFACE_INFO 

 
 