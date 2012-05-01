
rename bass1.G_S_04004_DAY to G_S_04004_DAY_STORE;
CREATE TABLE "BASS1   "."G_S_04004_DAY"  (
                  "TIME_ID" INTEGER NOT NULL , 
                  "PRODUCT_NO" CHAR(15) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "USER_TYPE" CHAR(1) NOT NULL , 
                  "MM_BILL_TYPE" CHAR(2) NOT NULL , 
                  "CODE_OF_PROV_PREFECTURE_USERS" CHAR(6) NOT NULL , 
                  "SEND_S_ADDR" CHAR(30) NOT NULL , 
                  "RECEIVER_S_ADDR" CHAR(30) NOT NULL , 
                  "FWD_PRODUCT_NO" CHAR(15) NOT NULL , 
                  "SEND_DATE" CHAR(8) NOT NULL , 
                  "SEND_TIME" CHAR(6) NOT NULL , 
                  "INFO_TYPE" CHAR(1) NOT NULL , 
                  "APPLCN_TYPE" CHAR(1) NOT NULL , 
                  "FWD_COPY_TYPE" CHAR(1) NOT NULL , 
                  "BILLING_TYPE" CHAR(1) NOT NULL , 
                  "BEARER_MODE" CHAR(2) NOT NULL , 
                  "CALL_FEE" CHAR(6) NOT NULL , 
                  "INFO_FEE" CHAR(6) NOT NULL , 
                  "MM_LEN" CHAR(8) NOT NULL , 
                  "MM_SEND_STATUS" CHAR(2) NOT NULL , 
                  "MMSC_ID_OF_ORIG_PRTY" CHAR(6) NOT NULL , 
                  "MMSC_ID_OF_RECEIVER" CHAR(6) NOT NULL , 
                  "MM_CONTENT_TYPE" CHAR(5) NOT NULL , 
                  "SP_ENT_CODE" CHAR(12) NOT NULL , 
                  "SVC_CODE" CHAR(21) NOT NULL , 
                  "BUS_CODE" CHAR(20) NOT NULL , 
                  "MM_KIND" CHAR(1) NOT NULL , 
                  "BUS_SRV_ID" CHAR(1) NOT NULL , 
                  "IMSI" CHAR(15) NOT NULL )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 



CREATE TABLE "BASS1   "."G_S_04004_DAY_STORE2"  (
                  "TIME_ID" INTEGER NOT NULL , 
                  "PRODUCT_NO" CHAR(15) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "USER_TYPE" CHAR(1) NOT NULL , 
                  "MM_BILL_TYPE" CHAR(2) NOT NULL , 
                  "CODE_OF_PROV_PREFECTURE_USERS" CHAR(6) NOT NULL , 
                  "SEND_S_ADDR" CHAR(30) NOT NULL , 
                  "RECEIVER_S_ADDR" CHAR(30) NOT NULL , 
                  "FWD_PRODUCT_NO" CHAR(15) NOT NULL , 
                  "SEND_DATE" CHAR(8) NOT NULL , 
                  "SEND_TIME" CHAR(6) NOT NULL , 
                  "INFO_TYPE" CHAR(1) NOT NULL , 
                  "APPLCN_TYPE" CHAR(1) NOT NULL , 
                  "FWD_COPY_TYPE" CHAR(1) NOT NULL , 
                  "BILLING_TYPE" CHAR(1) NOT NULL , 
                  "BEARER_MODE" CHAR(2) NOT NULL , 
                  "CALL_FEE" CHAR(6) NOT NULL , 
                  "INFO_FEE" CHAR(6) NOT NULL , 
                  "MM_LEN" CHAR(8) NOT NULL , 
                  "MM_SEND_STATUS" CHAR(2) NOT NULL , 
                  "MMSC_ID_OF_ORIG_PRTY" CHAR(6) NOT NULL , 
                  "MMSC_ID_OF_RECEIVER" CHAR(6) NOT NULL , 
                  "MM_CONTENT_TYPE" CHAR(5) NOT NULL , 
                  "SP_ENT_CODE" CHAR(12) NOT NULL , 
                  "SVC_CODE" CHAR(21) NOT NULL , 
                  "BUS_CODE" CHAR(20) NOT NULL , 
                  "MM_KIND" CHAR(1) NOT NULL , 
                  "BUS_SRV_ID" CHAR(1) NOT NULL , 
                  "IMSI" CHAR(15) NOT NULL )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 



insert into G_S_04004_DAY
select *
from G_S_04004_DAY_STORE
where time_id >= 20120101

select count(0) from G_S_04004_DAY_STORE

values ( int(replace(char(current date - 118 days),'-','')) )



select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_04004_DAY 
group by  time_id 
order by 1 



