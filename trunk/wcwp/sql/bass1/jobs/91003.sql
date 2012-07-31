drop table "BASS2   "."DIM_TERM_TAC_NEW_LOAD"
CREATE TABLE "BASS2   "."DIM_TERM_TAC_NEW_LOAD"  (
                  "ROW_ID" INTEGER , 
                  "DEV_ID" CHAR(16) , 
                  "TERM_TYPE" CHAR(2) , 
                  "TERM_BRAND" CHAR(100) , 
                  "TERM_MODEL" CHAR(100) , 
                  "ALIAS" CHAR(100) , 
                  "MODE_2G" CHAR(1) , 
                  "MODE_3G" CHAR(1) , 
                  "MODE_4G" CHAR(1) , 
                  "OS_ID" CHAR(3) , 
                  "OS_VERSION" CHAR(100) , 
                  "FRONT_CAM" INTEGER , 
                  "REAR_CAM" INTEGER , 
                  "SCREEN_RESOLUTION" CHAR(20) , 
                  "SCREEN_SIZE" INTEGER , 
                  "SCREEN_DEEP" INTEGER , 
                  "IFWLAN" CHAR(1) , 
                  "IFWAP" CHAR(1) , 
                  "IFWWW" CHAR(1) , 
                  "IFGPRS" CHAR(1) , 
                  "IFGPS" CHAR(1) , 
                  "IFHANDWRITE" CHAR(1) , 
                  "TOUCHTYPE" CHAR(1) , 
                  "TERM_DESIGN" CHAR(2) , 
                  "IFJAVA" CHAR(1) , 
                  "IFUSB" CHAR(1) , 
                  "IFBLUETEETH" CHAR(1) , 
                  "IFIFR" CHAR(1) , 
                  "IFMP3" CHAR(1) , 
                  "IFSTREAM" CHAR(1) , 
                  "IFEDGE" CHAR(1) , 
                  "IFUSSD" CHAR(1) )   
                 DISTRIBUTE BY HASH("ROW_ID")   
                   IN "TBS_DIM" ; 






CREATE TABLE "BASS2   "."DIM_TERM_TAC_NEW2"  (
		row_id                 integer             --��¼�к�          
        ,term_tac               Char(16)            --�ն��豸��ʶ      
        ,term_type              Char(2)             --�ն�����          
        ,term_brand             Char(100)           --�ն�Ʒ��          
        ,term_model             Char(100)           --�ն��ͺ�          
        ,alias                  Char(100)           --����              
        ,mode_2g                Char(1)             --2Gģʽ            
        ,mode_3g                Char(1)             --3Gģʽ            
        ,mode_4g                Char(1)             --4Gģʽ            
        ,os_id                  Char(3)             --����ϵͳ          
        ,os_version             Char(100)           --����ϵͳ�汾      
        ,front_cam              Char(6)             --ǰ����ͷ����      
        ,rear_cam               Char(6)             --������ͷ����      
        ,screen_resolution      Char(20)            --��Ļ����          
        ,screen_size            Char(5)             --�����ߴ�          
        ,screen_deep            Char(10)             --��ɫ��ɫ��        
        ,ifwlan                 Char(1)             --WALN����          
        ,ifwap                  Char(1)             --WAP֧��           
        ,ifwww                  Char(1)             --WWW֧��           
        ,ifgprs                 Char(1)             --GPRS����          
        ,ifgps                  Char(1)             --GPS����           
        ,ifhandwrite            Char(1)             --��д����          
        ,touchtype              Char(1)             --����������        
        ,term_design            Char(2)             --�ն˿�ʽ          
        ,ifjava                 Char(1)             --JAVA��չ          
        ,ifusb                  Char(1)             --USB               
        ,ifblueteeth            Char(1)             --����              
        ,ififr                  Char(1)             --����              
        ,ifmp3                  Char(1)             --MP3               
        ,ifstream               Char(1)             --��ý��            
        ,ifedge                 Char(1)             --EDGE              
        ,ifussd                 Char(1)             --USSD  
				  )   
                 DISTRIBUTE BY HASH("row_id")   
                   IN "TBS_DIM" 




20120730 :1.8.2���ֶ�û�䣬����ά��ֵ


CREATE TABLE "BASS2   "."DIM_TERM_TAC_NEW_LOAD"  (
                  "ROW_ID" INTEGER , 
                  "TERM_TYPE" CHAR(2) , 
                  "TERM_BRAND" CHAR(100) , 
                  "TERM_BRAND_ID" CHAR(6) , 
                  "TERM_MODEL" CHAR(100) , 
                  "ALIAS" CHAR(100) , 
                  "DEV_ID" CHAR(16) , 
                  "MODE_2G" CHAR(1) , 
                  "MODE_3G" CHAR(1) , 
                  "MODE_4G" CHAR(1) , 
                  "OS_ID" CHAR(3) , 
                  "OS_VERSION" CHAR(100) , 
                  "IFWLAN" CHAR(1) , 
                  "IFWAP" CHAR(1) , 
                  "IFWWW" CHAR(1) , 
                  "IFGPRS" CHAR(1) , 
                  "IFGPS" CHAR(1) , 
                  "IFMMS" CHAR(1) , 
                  "FRONT_CAM" INTEGER , 
                  "REAR_CAM" INTEGER , 
                  "SCREEN_RESOLUTION" CHAR(20) , 
                  "SCREEN_SIZE" CHAR(5) , 
                  "SCREEN_DEEP" INTEGER , 
                  "IFHANDWRITE" CHAR(1) , 
                  "TOUCHTYPE" CHAR(1) , 
                  "TERM_DESIGN" CHAR(2) , 
                  "IFJAVA" CHAR(1) , 
                  "IFUSB" CHAR(1) , 
                  "IFBLUETEETH" CHAR(1) , 
                  "IFIFR" CHAR(1) )   
                 DISTRIBUTE BY HASH("ROW_ID")   
                   IN "TBS_DIM"

rename bass2.DIM_TERM_TAC_NEW to DIM_TERM_TAC_NEW_LOAD

db2 "load client from /bassapp/bihome/panzw/tmp/i_30000_201205_91003_001.dat of asc \
 modified by timestampformat=\"YYYYMMDDHHMMSS\" dateformat=\"YYYYMMDD\" \
 timeformat=\"HHMMSS\" \
 method L ( \
 2	9 \
,10	25 \
,26	27 \
,28	127 \
,128	227 \
,228	327 \
,328	328 \
,329	329 \
,330	330 \
,331	333 \
,334	433 \
,434	439 \
,440	445 \
,446	465 \
,466	470 \
,471	480 \
,481	481 \
,482	482 \
,483	483 \
,484	484 \
,485	485 \
,486	486 \
,487	487 \
,488	489 \
,490	490 \
,491	491 \
,492	492 \
,493	493 \
,494	494 \
,495	495 \
,496	496 \
,497	497 \
) \
 messages ./bass2.DIM_TERM_TAC_NEW_LOAD.msg \
 replace into bass2.DIM_TERM_TAC_NEW_LOAD nonrecoverable"
 
 


536	�޸Ľӿڣ�
91003���ն�������Ϣ����XML��ʽ��Ϊ�ı���ʽ��ͬʱ��ʱ�ӿ�91005ֹͣ���䣩	1.8.1	2012-5-15	����������201205��Ч�·���5���¾ɽӿ�ͬʱ�·���6��ֻ�·��½ӿڣ�


egrep -in 'DIM_TERM_TAC|DIM_CONTROL_INFO|DIM_DEVICE_INFO|DIM_DEVICE_PROFILE|DIM_PROPERTY_INFO|DIM_PROPERTY_VALUE_RANGE' *.tcl|grep -v "#"



egrep -in 'DIM_TERM_TAC|DIM_CONTROL_INFO|DIM_DEVICE_INFO|DIM_DEVICE_PROFILE|DIM_PROPERTY_INFO|DIM_PROPERTY_VALUE_RANGE' *.tcl|grep -v "#"


select count(0) from bass2.DIM_TERM_TAC where NET_TYPE = '2'

insert into bass2.DIM_TERM_TAC
(
         ID
        ,TAC_NUM
        ,TERM_ID
        ,TERM_MODEL
        ,TERMPROD_ID
        ,TERMPROD_NAME
        ,NET_TYPE
        ,TERM_TYPE
)

select  
a.ROW_ID ID
,b.TAC_NUM
,'' TERM_ID
,a.TERM_MODEL
,'' TERMPROD_ID
,a.TERM_BRAND TERMPROD_NAME
,case when mode_2g = '1' and mode_3g = '3' then '2' else '1' end  NET_TYPE
,case 
	when a.MODE_2G in ('1') and a.MODE_3G in ('0','1')  and a.TERM_TYPE is null then '0'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '01' then '1'
	when a.MODE_2G in ('1') and a.MODE_3G in ('0')  and a.TERM_TYPE = '02' then '8'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '02' then '2'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '03' then '5'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '04' then '3'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '05' then '4'
	when a.MODE_2G in ('1') and a.MODE_3G in ('0')  and a.TERM_TYPE = '05' then '9'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '06' then '6'
	when 												a.TERM_TYPE = '01' then '0'
	end  TERM_TYPE
from  bass2.DIM_TERM_TAC_NEW_LOAD a
 ,BASS2.DIM_TACNUM_DEVID b 
where a.DEV_ID = b.DEV_ID
and MODE_3G = '3'



MODE_2G	MODE_3G	TERM_TYPE	TERM_TYPE_OLD	5	   
1	1	  	0	25	   
1	0	  	0	35	   
1	1	01	0	2200	   
2	0	01	0	204	   
2	2	01	0	58	   
1	0	01	0	24411	   
1	3	01	1	482	   
1	2	01	0	155	   
2	1	01	0	1	   
1	0	02	8	1	   
1	3	02	2	79	   
1	3	03	5	3	   
1	3	04	3	4	   
1	3	05	4	162	   
1	0	05	9	8	   
1	3	06	6	5	   
					

case 
when a.MODE_2G in ('1') and a.MODE_3G in ('0','1')  and a.TERM_TYPE is null then '0'
when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '01' then '1'
when a.MODE_2G in ('1') and a.MODE_3G in ('0')  and a.TERM_TYPE = '02' then '8'
when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '02' then '2'
when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '03' then '5'
when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '04' then '3'
when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '05' then '4'
when a.MODE_2G in ('1') and a.MODE_3G in ('0')  and a.TERM_TYPE = '05' then '9'
when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '06' then '6'
when 												a.TERM_TYPE = '01' then '0'
end 







alter table bass2.DIM_TERM_TAC_TRANS activate not logged initially with empty table

insert into  "BASS2   "."DIM_TERM_TAC_TRANS"
select  
a.ROW_ID ID
,b.TAC_NUM
,'' TERM_ID
,a.TERM_MODEL
,'' TERMPROD_ID
,a.TERM_BRAND TERMPROD_NAME
,case when mode_2g = '1' and mode_3g = '3' then '2' else '1' end  NET_TYPE
,case 
	when a.MODE_2G in ('1') and a.MODE_3G in ('0','1')  and a.TERM_TYPE is null then '0'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '01' then '1'
	when a.MODE_2G in ('1') and a.MODE_3G in ('0')  and a.TERM_TYPE = '02' then '2' --02- ���ݿ�2
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '02' then '2'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '03' then '5'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '04' then '3'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '05' then '4'
	when a.MODE_2G in ('1') and a.MODE_3G in ('0')  and a.TERM_TYPE = '05' then '4' --05- ���߹̻�4
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '06' then '6'
	when 												a.TERM_TYPE = '07' then '8' --8����Ϊ 07- ƽ�����
	when 												a.TERM_TYPE = '01' then '0'
	when 												upper(a.TERM_MODEL) like '%IPAD%' then '8'
	end  TERM_TYPE
from  bass2.DIM_TERM_TAC_NEW_LOAD a
 ,BASS2.DIM_TACNUM_DEVID b 
where a.DEV_ID = b.DEV_ID


egrep -in 'TERM_ID|TERMPROD_ID' *.tcl|grep -v "#"

û���õ���




select 
from 
bass2.DIM_TERM_TAC a
,bass2.DIM_TERM_TAC_NEW_LOAD b 
where a.
