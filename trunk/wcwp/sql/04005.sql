
rename bass1.G_S_04005_DAY to G_S_04005_DAY_STORE;
CREATE TABLE "BASS1   "."G_S_04005_DAY"  (
                  "TIME_ID" INTEGER NOT NULL , 
                  "PRODUCT_NO" CHAR(15) NOT NULL , 
                  "IMSI" CHAR(15) NOT NULL , 
                  "CITY_ID" CHAR(6) NOT NULL , 
                  "OPPOSITE_NO" CHAR(24) NOT NULL , 
                  "CALLTYPE_ID" CHAR(2) NOT NULL , 
                  "USERTYPE_ID" CHAR(1) NOT NULL , 
                  "SP_CODE" CHAR(12) NOT NULL , 
                  "OPER_CODE" CHAR(20) NOT NULL , 
                  "SERV_CODE" CHAR(21) NOT NULL , 
                  "PAYTYPE_ID" CHAR(2) NOT NULL , 
                  "SMS_STATUS" CHAR(1) NOT NULL , 
                  "SMS_BASEFEE" CHAR(6) NOT NULL , 
                  "INFO_FEE" CHAR(6) NOT NULL , 
                  "MONTH_FEE" CHAR(6) NOT NULL , 
                  "INFO_LEN" CHAR(3) NOT NULL , 
                  "SMSC_ID" CHAR(11) NOT NULL , 
                  "ISMG_ID" CHAR(6) NOT NULL , 
                  "FORWARD_ISMG_ID" CHAR(6) NOT NULL , 
                  "INFO_TYPE" CHAR(2) NOT NULL , 
                  "INPUT_DATE" CHAR(8) NOT NULL , 
                  "INPUT_TIME" CHAR(6) NOT NULL , 
                  "PROCESS_DATE" CHAR(8) NOT NULL , 
                  "PROCESS_TIME" CHAR(6) NOT NULL , 
                  "START_DATE" CHAR(8) NOT NULL , 
                  "START_TIME" CHAR(6) NOT NULL )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PRODUCT_NO",  
                 "SP_CODE",  
                 "OPPOSITE_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 




insert into G_S_04005_DAY
select *
from G_S_04005_DAY_STORE
where time_id >= 20111101

db2 runstats on table bass1.G_S_04005_DAY with distribution and detailed indexes all


--------------------------------------------------------------------------------------------------
select count(0) from G_S_04005_DAY_STORE

values ( int(replace(char(current date - 118 days),'-','')) )



select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_04005_DAY 
group by  time_id 
order by 1 


select count(0)
 from  
   bass2.cdr_mms_20120429
 where 
   send_status in (0,1,2,3) and send_mmsc_id is not null
   

select count(0)
 from  
   bass2.cdr_mms_20120428
 where 
   send_status in (0,1,2,3) and send_mmsc_id is not null
   
04005_20120429_s_001.dat      38561928            154538599           -75.05    %


$ cat *04005*verf 
s_13100_20120429_04005_00_001.dat       38561928            160008              2012042920120430024402
$ cat ../export_20120428/*04005*verf
s_13100_20120428_04005_00_001.dat       154538599           641239              2012042820120429024405

s_13100_20120427_04005_00_001.dat       153911276           638636              2012042720120428024316

     641239
	 160008
	 