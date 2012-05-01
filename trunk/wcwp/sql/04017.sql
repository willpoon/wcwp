
rename bass1.G_S_04017_DAY to G_S_04017_DAY_STORE;

CREATE TABLE "BASS1   "."G_S_04017_DAY"  (
                  "TIME_ID" INTEGER NOT NULL , 
                  "PRODUCT_NO" CHAR(15) NOT NULL , 
                  "IMSI" CHAR(15) NOT NULL , 
                  "MSRN" CHAR(11) NOT NULL , 
                  "IMEI" CHAR(17) NOT NULL , 
                  "OPPOSITE_NO" CHAR(24) NOT NULL , 
                  "THIRD_NO" CHAR(24) NOT NULL , 
                  "CITY_ID" CHAR(6) NOT NULL , 
                  "ROAM_LOCN" CHAR(6) NOT NULL , 
                  "OPP_CITY_ID" CHAR(6) NOT NULL , 
                  "OPP_ROAM_LOCN" CHAR(6) NOT NULL , 
                  "ADVERSARY_ID" CHAR(6) NOT NULL , 
                  "ADVERSARY_NET_TYPE" CHAR(2) NOT NULL , 
                  "MSC_CODE" CHAR(11) NOT NULL , 
                  "CELL_ID" CHAR(10) NOT NULL , 
                  "LAC_ID" CHAR(10) NOT NULL , 
                  "OPP_CELL_ID" CHAR(10) NOT NULL , 
                  "OPP_LAC_ID" CHAR(10) NOT NULL , 
                  "OUTGO_TRNK" CHAR(15) NOT NULL , 
                  "INCOMING_TRNK" CHAR(15) NOT NULL , 
                  "START_DATE" CHAR(8) NOT NULL , 
                  "START_TIME" CHAR(6) NOT NULL , 
                  "CALL_DURATION" CHAR(6) NOT NULL , 
                  "BASE_BILL_DURATION" CHAR(6) NOT NULL , 
                  "TOLL_BILL_DURATION" CHAR(6) NOT NULL , 
                  "BILLING_ID" CHAR(1) NOT NULL , 
                  "BASE_CALL_FEE" CHAR(8) NOT NULL , 
                  "TOLL_CALL_FEE" CHAR(8) NOT NULL , 
                  "CALLFW_TOLL_FEE" CHAR(8) , 
                  "INFO_FEE" CHAR(8) NOT NULL , 
                  "FAV_BASE_CALL_FEE" CHAR(8) NOT NULL , 
                  "FAV_TOLL_CALL_FEE" CHAR(8) NOT NULL , 
                  "FAV_CALLFW_TOLL_FEE" CHAR(8) , 
                  "FAV_INFO_FEE" CHAR(8) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "TOLL_TYPE_ID" CHAR(3) NOT NULL , 
                  "SVCITEM_ID" CHAR(3) NOT NULL , 
                  "CALL_TYPE_ID" CHAR(2) NOT NULL , 
                  "SERVICE_CODE" CHAR(4) NOT NULL , 
                  "USER_TYPE" CHAR(1) NOT NULL , 
                  "FEE_TYPE" CHAR(1) NOT NULL , 
                  "END_CALL_TYPE" CHAR(1) NOT NULL , 
                  "MNS_TYPE" CHAR(1) NOT NULL , 
                  "VIDEO_TYPE" CHAR(1) NOT NULL )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 

ALTER TABLE "BASS1   "."G_S_04017_DAY" APPEND ON;

ALTER TABLE "BASS1   "."G_S_04017_DAY" LOCKSIZE TABLE;



insert into G_S_04017_DAY
select * from G_S_04017_DAY_STORE
where time_id >= 20111201
;

29417613 row(s) affected.


select time_id , count(0) 
--,  count(distinct time_id ) 
from bass1.g_s_04017_day 
group by  time_id 
order by 1 


