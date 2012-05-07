
CREATE TABLE "BASS1   "."G_S_04002_DAY_THIS"  (
                  "TIME_ID" INTEGER NOT NULL , 
                  "PRODUCT_NO" CHAR(15) NOT NULL , 
                  "ROAM_LOCN" CHAR(6) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "APNNI" CHAR(40) NOT NULL , 
                  "START_TIME" CHAR(14) NOT NULL , 
                  "CALL_DURATION" CHAR(8) NOT NULL , 
                  "UP_FLOWS" CHAR(14) NOT NULL , 
                  "DOWN_FLOWS" CHAR(14) NOT NULL , 
                  "ALL_FEE" CHAR(9) NOT NULL , 
                  "MNS_TYPE" CHAR(1) NOT NULL , 
                  "IMEI" CHAR(17) NOT NULL , 
                  "SERVICE_CODE" CHAR(10) NOT NULL )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PRODUCT_NO",  
                 "ROAM_LOCN",  
                 "ROAM_TYPE_ID",  
                 "APNNI",  
                 "START_TIME")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 
				   


CREATE TABLE "BASS1   "."G_S_04002_DAY_PREV"  (
                  "TIME_ID" INTEGER NOT NULL , 
                  "PRODUCT_NO" CHAR(15) NOT NULL , 
                  "ROAM_LOCN" CHAR(6) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "APNNI" CHAR(40) NOT NULL , 
                  "START_TIME" CHAR(14) NOT NULL , 
                  "CALL_DURATION" CHAR(8) NOT NULL , 
                  "UP_FLOWS" CHAR(14) NOT NULL , 
                  "DOWN_FLOWS" CHAR(14) NOT NULL , 
                  "ALL_FEE" CHAR(9) NOT NULL , 
                  "MNS_TYPE" CHAR(1) NOT NULL , 
                  "IMEI" CHAR(17) NOT NULL , 
                  "SERVICE_CODE" CHAR(10) NOT NULL )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PRODUCT_NO",  
                 "ROAM_LOCN",  
                 "ROAM_TYPE_ID",  
                 "APNNI",  
                 "START_TIME")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 
				   
				   
                   

select count(0) from bass1.g_s_04002_day where time_id = 20120427
5480819

select 
         time_id
         ,case when rule_code='R159_1' then '新增客户数'
              when rule_code='R159_2' then '客户到达数'
              when rule_code='R159_3' then '上网本客户数'
              when rule_code='R159_4' then '离网客户数'
         end index_name
         ,target1 bass2_val
         ,target2 bass1_val
from bass1.g_rule_check
where 
    rule_code in ('R159_1','R159_2','R159_3','R159_4')
and rule_code='R159_4'
and time_id  >(int(replace(char(current date - 62 days),'-','')))
order by 1 desc 
 

alter table bass1.G_S_04002_DAY_THIS activate not logged initially with empty table


select tabname from syscat.tables where tabschema  = 'BASS1'

alter table (select * from G_I_77780_DAY_B20110428 where org_type = '90') activate not logged initially with empty table

$ db2 update command options using c off 
DB20000I  The UPDATE COMMAND OPTIONS command completed successfully.
$ 
$ db2 update command options using c on
DB20000I  The UPDATE COMMAND OPTIONS command completed successfully.

select count(0) from bass1.G_S_04002_DAY_prev
select count(0) from bass1.G_S_04002_DAY_this

select * from  bass1.G_S_04002_DAY_prev
fetch first 10 rows only
select * from  bass1.G_S_04002_DAY_this
fetch first 10 rows only


select * from bass1.g_rule_check
where time_id = 20120428
order by 1,2


select count(0) from G_S_04002_DAY_THIS where time_id = 20120429
select count(0) from G_S_04002_DAY_PREV where time_id = 20120501





