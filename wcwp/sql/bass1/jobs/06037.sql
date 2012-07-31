
db2 "load client from /bassapp/bihome/panzw/tmp/06037.txt of asc \
 modified by timestampformat=\"YYYYMMDDHHMMSS\" dateformat=\"YYYYMMDD\" \
 timeformat=\"HHMMSS\" \
 method L ( \
1	40 \
,41	45 \
,46	50 \
,51	55 \
,56	58 \
,59	61 \
,62	64 \
,65	67 \
,68	68 \
,69	69 \
,70	70 \
,71	71 \
,72	74 \
,75	77 \
,78	82 \
,83	85 \
,86	88 \
,89	91 \
,92	94 \
,95	99 \
,100	104 \
,105	105 \
,106	106 \
) \
messages ./bass1.G_A_06037_DAY_SNAPDT0630.msg \
replace into bass1.G_A_06037_DAY_SNAPDT0630 nonrecoverable"
 
 
 
 select tabname from syscat.tables where tabname like '%06037%DAY%'
 

CREATE TABLE "BASS1   "."G_A_06037_DAY_SNAPDT0630"  (                  
                  "CHANNEL_ID" CHAR(40) , 
                  "BUILD_AREA" CHAR(5) , 
                  "USE_AREA" CHAR(5) , 
                  "BUSI_AREA" CHAR(5) , 
                  "COUNTER_CNT" CHAR(3) , 
                  "STAFF_CNT" CHAR(3) , 
                  "SECURE_CNT" CHAR(3) , 
                  "CLEANER_CNT" CHAR(3) , 
                  "WAITING_MACHINE" CHAR(1) , 
                  "POS_MACHINE" CHAR(1) , 
                  "VIP_SERVANT" CHAR(1) , 
                  "IF_VIP_SCHOOL" CHAR(1) , 
                  "PRINTER_CNT" CHAR(3) , 
                  "SELFTERM_CNT" CHAR(3) , 
                  "G3_AREA" CHAR(5) , 
                  "SCREEN_CNT" CHAR(3) , 
                  "EXP_PLAT_TERM_CNT" CHAR(3) , 
                  "XINJI_TERM_CNT" CHAR(3) , 
                  "WEB_TERM_CNT" CHAR(3) , 
                  "HALL_AREA" CHAR(5) , 
                  "CMCC_BUSI_AREA" CHAR(5) , 
                  "ACCESS_WAY" CHAR(1) , 
                  "IF_KONGCONG" CHAR(1) )   
                 DISTRIBUTE BY HASH("CHANNEL_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX"
				   