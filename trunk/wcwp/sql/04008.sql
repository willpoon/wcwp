
rename bass1.G_S_04008_DAY to G_S_04008_DAY_STORE;
CREATE TABLE "BASS1   "."G_S_04008_DAY"  (
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
                  "MNS_TYPE" CHAR(1) , 
                  "VIDEO_TYPE" CHAR(1) )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 

ALTER TABLE "BASS1   "."G_S_04008_DAY" APPEND ON;

ALTER TABLE "BASS1   "."G_S_04008_DAY" LOCKSIZE TABLE;

select count(0) from G_S_04008_DAY 


insert into G_S_04008_DAY
select *
from G_S_04008_DAY_STORE
where time_id >= 20120301

10449698 row(s) affected.

select count(0) from G_S_04008_DAY_STORE

values ( int(replace(char(current date - 118 days),'-','')) )



select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_04008_DAY 
group by  time_id 
order by 1 




select time_id , count(0) 
--,  count(distinct time_id ) 
,sum(bigint(TOLL_CALL_FEE))
from BASS1.G_S_04008_DAY 
where time_id / 100 = 201204
group by  time_id 
order by 1 





 select *from  bass1.G_RULE_CHECK where rule_code = 'R107'
 AND TIME_ID / 100 = 201205
 order by 1 desc 
 
 TIME_ID     RULE_CODE  TARGET1              TARGET2              TARGET3              TARGET4             
----------- ---------- -------------------- -------------------- -------------------- --------------------
   20120531 R107                   28.83000             64.03000             -0.54974              0.05000
   20120530 R107                   27.89000             62.09000             -0.55081              0.05000
   20120529 R107                   26.97000             60.13000             -0.55147              0.05000
   20120528 R107                   26.05000             58.16000             -0.55210              0.05000


TIME_ID     2           3                   
----------- ----------- --------------------
   20120501      119688                70700
   20120502      116498                47400
   20120503      121505                46600
   20120504      124475                43800
   20120505      118409                63700
   20120506      115255                49700
   20120507      118545                60000
   20120508      119968                51100
   20120509      117373                51400
   20120510      117875                48700
   20120511      121626                46700
   20120512      114251                59600
   20120513      115137                70200
   20120514      116138                65700
   20120515      115184                58800
   20120516      114740                62000
   20120517      112750                49400
   20120518      117129                67800
   20120519      111121                46700
   20120520      111039                62000
   20120521      113489                76100
   20120522      110446                55200
   20120523      110805                61100
   20120524      108795                60900
   20120525      112919                44000
   20120526      109041                48700
   20120527      105312                49500
   20120528      109169                54300
   20120529      109363                43200
   20120530      110906                73600
   20120531      112021             44865300

  31 record(s) selected.
  
  INSERT INTO bass1.G_RULE_CHECK VALUES
                        (20120531 ,
                        'R107',
                        cast (28.83 as  DECIMAL(18, 5) ),
                        cast (64.03 as  DECIMAL(18, 5) ),
                        cast (-0.54974 as  DECIMAL(18, 5) ),
                        0.05)
						
						
						
						
						
TIME_ID     2           3                   
----------- ----------- --------------------
   20120401      129486                59400
   20120402      110781                42200
   20120403      110975                58500
   20120404      110975                48400
   20120405      117286                68800
   20120406      118639                61200
   20120407      111882                55800
   20120408      112519                50000
   20120409      117453                49800
   20120410      117793                61200
   20120411      118053                56200
   20120412      122109                63400
   20120413      119586                55600
   20120414      115398                61100
   20120415      114708                61700
   20120416      119030                48200
   20120417      119215                59500
   20120418      118444                56700
   20120419      118031                54500
   20120420      121256                70300
   20120421      116422                86400
   20120422      113860                65300
   20120423      117938                48900
   20120424      115453                58700
   20120425      117101                66400
   20120426      119293                55100
   20120427      119098                48200
   20120428      122447                57400
   20120429      117880                56400
   20120430      115525             46273200

  30 record(s) selected.


