drop table bass2.DIM_TACNUM_DEVID_LOAD
CREATE TABLE "BASS2   "."DIM_TACNUM_DEVID_LOAD"  (
                  "TIME_ID" INTEGER , 
                  "TAC_NUM" VARCHAR(10) , 
                  "DEV_ID" VARCHAR(16) )   
                 DISTRIBUTE BY HASH("DEV_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY 

--~   alter table bass2.DIM_TACNUM_DEVID_LOAD activate not logged initially with empty table


db2 "load client from /bassapp/bihome/panzw/tmp/i_30000_201207_91002_001.dat of asc \
 modified by timestampformat=\"YYYYMMDDHHMMSS\" dateformat=\"YYYYMMDD\" \
 timeformat=\"HHMMSS\" \
 method L ( \
 1	8 \
,9	18 \
,19	34 \
) \
 messages ./bass2.DIM_TACNUM_DEVID_LOAD.msg \
 replace into bass2.DIM_TACNUM_DEVID_LOAD nonrecoverable"
 
 


rename bass2.DIM_TACNUM_DEVID to DIM_TACNUM_DEVID_B20120730

CREATE TABLE "BASS2   "."DIM_TACNUM_DEVID"  (
                  "TIME_ID" INTEGER , 
                  "TAC_NUM" VARCHAR(10) , 
                  "DEV_ID" VARCHAR(16) )   
                 DISTRIBUTE BY HASH("DEV_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY
				   
--~   alter table bass2.DIM_TACNUM_DEVID activate not logged initially with empty table

INSERT INTO  bass2.DIM_TACNUM_DEVID
select 201207
,TAC_NUM
,DEV_ID
from  bass2.DIM_TACNUM_DEVID_LOAD

