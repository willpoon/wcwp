
rename bass1.G_S_21003_MONTH to G_S_21003_MONTH_20120707;


CREATE TABLE "BASS1   "."G_S_21003_MONTH"  (
                  "TIME_ID" INTEGER , 
                  "BILL_MONTH" CHAR(6) NOT NULL , 
                  "PRODUCT_NO" CHAR(15) NOT NULL , 
                  "BRAND_ID" CHAR(1) NOT NULL , 
                  "SVC_TYPE_ID" CHAR(3) NOT NULL , 
                  "TOLL_TYPE_ID" CHAR(3) NOT NULL , 
                  "IP_TYPE_ID" CHAR(4) NOT NULL , 
                  "ADVERSARY_ID" CHAR(6) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "CALL_TYPE_ID" CHAR(2) NOT NULL , 
                  "CALL_COUNTS" CHAR(14) NOT NULL , 
                  "BASE_BILL_DURATION" CHAR(14) NOT NULL , 
                  "TOLL_BILL_DURATION" CHAR(14) NOT NULL , 
                  "CALL_DURATION" CHAR(14) NOT NULL , 
                  "BASE_CALL_FEE" CHAR(14) NOT NULL , 
                  "TOLL_CALL_FEE" CHAR(14) NOT NULL , 
                  "CALLFW_TOLL_FEE" CHAR(14) NOT NULL , 
                  "CALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_BASECALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_TOLLCALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_CALLFW_TOLLFEE" CHAR(14) NOT NULL , 
                  "FAVOURED_CALL_FEE" CHAR(14) NOT NULL , 
                  "FREE_DURATION" CHAR(14) NOT NULL , 
                  "FAVOUR_DURATION" CHAR(14) NOT NULL , 
                  "MNS_TYPE" CHAR(1) )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" 
				   
				   
				   
/bassapp/backapp/bin/bass1_export
$ 
TOLL_TYPE_ID 2          
------------ -----------
010             91746055
021             10209088
022              9693990
040                  318
051                  478
052                 2367

  6 record(s) selected.
  
  
s_13100_201205_21003_00_001.dat         1875000000          7500000             201205  20120607173612
s_13100_201205_21003_00_002.dat         1875000000          7500000             201205  20120607174030
s_13100_201205_21003_00_003.dat         1875000000          7500000             201205  20120607174459
s_13100_201205_21003_00_004.dat         15719500            62878               201205  20120607174501




select TOLL_TYPE_ID , count(0) 
--,  count(distinct TOLL_TYPE_ID ) 
from G_S_04017_DAY 
where time_id / 100 = 201205
group by  TOLL_TYPE_ID 
order by 1 



select TOLL_TYPE_ID , count(0) 
--,  count(distinct TOLL_TYPE_ID ) 
from G_S_21003_MONTH 
group by  TOLL_TYPE_ID 
order by 1 

TOLL_TYPE_ID 2          
------------ -----------
010             12379463
021              2488536
022              2155169
040                  138
051                  204
052                  786


$ grep -i BASS_STD2_0001 *.tcl
G_S_04017_DAY.tcl:                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0001',char(tolltype_id)),'010') AS TOLL_TYPE_ID
G_S_04017_DAY.tcl:# ,TOLL_TYPE_ID             #��;���ͱ���BASS_STD2_0001
--~   G_S_21018_MONTH.tcl:                          COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0001',CHAR(tolltype_id)),'052')
--~   G_S_21018_MONTH.tcl:                           COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0001',CHAR(tolltype_id)),'052')
--~   INT_0400810_YYYYMM.tcl:                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0001',char(tolltype_id)),'010') AS TOLL_TYPE_ID
--~   INT_0400810_YYYYMM.tcl:# ,TOLL_TYPE_ID             #��;���ͱ���BASS_STD2_0001
INT_210012916_YYYYMM.tcl:          ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0001',char(tolltype_id)),'010') as toll_type_id
INT_210012916_YYYYMM.tcl:          ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0001',char(tolltype_id)),'010')
$ 


ksh  bass1_export_lite.sh G_I_21003_MONTH 201205


R146	��	04_TDҵ��	����ҵ����ʹ����������������ΪTD�����[�������͡���;���͡���������]�û����	"04017 TD��ͨ����ҵ�񻰵�
21003 GSM/TD��ͨ����ҵ����ʹ��"	����ҵ����ʹ����������������ΪTD�����[�������͡���;���͡���������]�û���ϱ�����TD���������ж�Ӧ��[�������͡���;���͡���������]�û�����д���	0.05



"��һ����ȡ04017�ӿ������������ͳ���£���������������Ϊ1��3G����[MSISDN���������͡���;���͡���������]�������أ�
�ڶ�����ȡ21003�ӿ������������ͳ���£�������������Ϊ1��3G�����Ҳ��ڵ�һ������е�[MSISDN���������͡���;���͡���������]���������غ�ĸ�����"






CREATE TABLE "BASS1   "."G_S_21003_MONTH"  (
                  "TIME_ID" INTEGER , 
                  "BILL_MONTH" CHAR(6) NOT NULL , 
                  "PRODUCT_NO" CHAR(15) NOT NULL , 
                  "BRAND_ID" CHAR(1) NOT NULL , 
                  "SVC_TYPE_ID" CHAR(3) NOT NULL , 
                  "TOLL_TYPE_ID" CHAR(3) NOT NULL , 
                  "IP_TYPE_ID" CHAR(4) NOT NULL , 
                  "ADVERSARY_ID" CHAR(6) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "CALL_TYPE_ID" CHAR(2) NOT NULL , 
                  "CALL_COUNTS" CHAR(14) NOT NULL , 
                  "BASE_BILL_DURATION" CHAR(14) NOT NULL , 
                  "TOLL_BILL_DURATION" CHAR(14) NOT NULL , 
                  "CALL_DURATION" CHAR(14) NOT NULL , 
                  "BASE_CALL_FEE" CHAR(14) NOT NULL , 
                  "TOLL_CALL_FEE" CHAR(14) NOT NULL , 
                  "CALLFW_TOLL_FEE" CHAR(14) NOT NULL , 
                  "CALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_BASECALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_TOLLCALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_CALLFW_TOLLFEE" CHAR(14) NOT NULL , 
                  "FAVOURED_CALL_FEE" CHAR(14) NOT NULL , 
                  "FREE_DURATION" CHAR(14) NOT NULL , 
                  "FAVOUR_DURATION" CHAR(14) NOT NULL , 
                  "MNS_TYPE" CHAR(1) )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 


               

select count(0) val1 
from (  select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID
from bass1.g_s_21003_month
where time_id=201206 and mns_type='1'
except
select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
from bass1.g_s_04017_day
where time_id/100=201206 and mns_type='1'
group by product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
) M
with ur


G_S_21003_MONTH_20120607







CREATE TABLE "BASS1   "."G_S_21003_MONTH_PK20120707_L2"  (
                  "TIME_ID" INTEGER , 
                  "BILL_MONTH" CHAR(6) NOT NULL , 
                  "BRAND_ID" CHAR(1) NOT NULL , 
                  "SVC_TYPE_ID" CHAR(3) NOT NULL , 
                  "TOLL_TYPE_ID" CHAR(3) NOT NULL , 
                  "IP_TYPE_ID" CHAR(4) NOT NULL , 
                  "ADVERSARY_ID" CHAR(6) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "CALL_TYPE_ID" CHAR(2) NOT NULL , 
                  "MNS_TYPE" CHAR(1),
				  COUNTS BIGINT
				  )   
                 DISTRIBUTE BY HASH("TIME_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" 




CREATE TABLE "BASS1   "."G_S_21003_MONTH_PK20120707_L3"  (
                  "TIME_ID" INTEGER , 
                  "BILL_MONTH" CHAR(6) NOT NULL , 
                  "BRAND_ID" CHAR(1) NOT NULL , 
                  "SVC_TYPE_ID" CHAR(3) NOT NULL , 
                  "TOLL_TYPE_ID" CHAR(3) NOT NULL , 
                  "IP_TYPE_ID" CHAR(4) NOT NULL , 
                  "ADVERSARY_ID" CHAR(6) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "CALL_TYPE_ID" CHAR(2) NOT NULL , 
                  "MNS_TYPE" CHAR(1),
				  COUNTS BIGINT
				  )   
                 DISTRIBUTE BY HASH("TIME_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 

insert into G_S_21003_MONTH_PK20120707_L3
select
         TIME_ID
        ,BILL_MONTH
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,TOLL_TYPE_ID
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
		,MNS_TYPE
		,count(0)
from G_S_21003_MONTH_20120707
where time_id = 201206
group by 
         TIME_ID
        ,BILL_MONTH
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,TOLL_TYPE_ID
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
		,MNS_TYPE





--~   insert into G_S_21003_MONTH_PK20120608
--~   select
         --~   TIME_ID
        --~   ,BILL_MONTH
        --~   ,BRAND_ID
        --~   ,SVC_TYPE_ID
        --~   ,TOLL_TYPE_ID
        --~   ,IP_TYPE_ID
        --~   ,ADVERSARY_ID
        --~   ,ROAM_TYPE_ID
        --~   ,CALL_TYPE_ID
		--~   ,MNS_TYPE
		--~   ,count(0)
--~   from G_S_21003_MONTH_20120607
--~   where time_id = 201205
--~   group by 
         --~   TIME_ID
        --~   ,BILL_MONTH
        --~   ,BRAND_ID
        --~   ,SVC_TYPE_ID
        --~   ,TOLL_TYPE_ID
        --~   ,IP_TYPE_ID
        --~   ,ADVERSARY_ID
        --~   ,ROAM_TYPE_ID
        --~   ,CALL_TYPE_ID
		--~   ,MNS_TYPE



ǰ�᣺
1.���� 01 �� 010 �����������
2.��֤����У��ͨ����






CREATE TABLE "BASS1   "."G_S_21003_MONTH_TP"  (
                  "TIME_ID" INTEGER , 
                  "BILL_MONTH" CHAR(6) NOT NULL , 
                  "PRODUCT_NO" CHAR(15) NOT NULL , 
                  "BRAND_ID" CHAR(1) NOT NULL , 
                  "SVC_TYPE_ID" CHAR(3) NOT NULL , 
                  "TOLL_TYPE_ID" CHAR(3) NOT NULL , 
                  "IP_TYPE_ID" CHAR(4) NOT NULL , 
                  "ADVERSARY_ID" CHAR(6) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "CALL_TYPE_ID" CHAR(2) NOT NULL , 
                  "CALL_COUNTS" CHAR(14) NOT NULL , 
                  "BASE_BILL_DURATION" CHAR(14) NOT NULL , 
                  "TOLL_BILL_DURATION" CHAR(14) NOT NULL , 
                  "CALL_DURATION" CHAR(14) NOT NULL , 
                  "BASE_CALL_FEE" CHAR(14) NOT NULL , 
                  "TOLL_CALL_FEE" CHAR(14) NOT NULL , 
                  "CALLFW_TOLL_FEE" CHAR(14) NOT NULL , 
                  "CALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_BASECALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_TOLLCALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_CALLFW_TOLLFEE" CHAR(14) NOT NULL , 
                  "FAVOURED_CALL_FEE" CHAR(14) NOT NULL , 
                  "FREE_DURATION" CHAR(14) NOT NULL , 
                  "FAVOUR_DURATION" CHAR(14) NOT NULL , 
                  "MNS_TYPE" CHAR(1) )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 
				   

alter table G_S_21003_MONTH_TP activate not logged initially with empty table
alter table G_S_21003_MONTH_TP3 activate not logged initially with empty table
alter table G_S_21003_MONTH_TP2 activate not logged initially with empty table
				   
insert into G_S_21003_MONTH_TP
select * 
from G_S_21003_MONTH_20120707
where TIME_ID = 201206



update G_S_21003_MONTH_TP 
set IP_TYPE_ID = '2199'
where  length(trim(TOLL_TYPE_ID)) = 2




CREATE TABLE "BASS1   "."G_S_21003_MONTH_TP2"  (
                  "TIME_ID" INTEGER , 
                  "BILL_MONTH" CHAR(6) NOT NULL , 
                  "PRODUCT_NO" CHAR(15) NOT NULL , 
                  "BRAND_ID" CHAR(1) NOT NULL , 
                  "SVC_TYPE_ID" CHAR(3) NOT NULL , 
                  "TOLL_TYPE_ID" CHAR(3) NOT NULL , 
                  "IP_TYPE_ID" CHAR(4) NOT NULL , 
                  "ADVERSARY_ID" CHAR(6) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "CALL_TYPE_ID" CHAR(2) NOT NULL , 
                  "CALL_COUNTS" CHAR(14) NOT NULL , 
                  "BASE_BILL_DURATION" CHAR(14) NOT NULL , 
                  "TOLL_BILL_DURATION" CHAR(14) NOT NULL , 
                  "CALL_DURATION" CHAR(14) NOT NULL , 
                  "BASE_CALL_FEE" CHAR(14) NOT NULL , 
                  "TOLL_CALL_FEE" CHAR(14) NOT NULL , 
                  "CALLFW_TOLL_FEE" CHAR(14) NOT NULL , 
                  "CALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_BASECALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_TOLLCALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_CALLFW_TOLLFEE" CHAR(14) NOT NULL , 
                  "FAVOURED_CALL_FEE" CHAR(14) NOT NULL , 
                  "FREE_DURATION" CHAR(14) NOT NULL , 
                  "FAVOUR_DURATION" CHAR(14) NOT NULL , 
                  "MNS_TYPE" CHAR(1) )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 
				   
				   




select count(0) val1 
from (  select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID
from bass1.g_s_21003_month
where time_id=201205 and mns_type='1'
except
select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
from bass1.g_s_04017_day
where time_id/100=201205 and mns_type='1'
group by product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
) M
with ur



�� ���� Ϊ 3 �� ��¼ ���޸��ض��ֶΣ�������Ψһ��


select 
         TIME_ID
        ,BILL_MONTH
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,TOLL_TYPE_ID
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
        ,MNS_TYPE
--,length(trim(TOLL_TYPE_ID))
 from G_S_21003_MONTH_PK20120608 a
where length(trim(TOLL_TYPE_ID))  = 3



select 
         *
--,length(trim(TOLL_TYPE_ID))
 from G_S_21003_MONTH_PK20120608 a
where length(trim(TOLL_TYPE_ID))  = 3



5.18.	BASS_STD2_0018  ҵ������

����	��������	����
000	����ҵ��	
001	����ҵ��	
002	����ҵ��	
003	������־����ҵ��	
004	�ƶ�ɳ��ҵ��	
005	���и���ҵ��	
006	���и���ҵ��	
007	�ƶ�800ҵ��	
008	����绰	
009	��������	
010	��Ѷҵ��	
011	���÷���	
012	�ƶ�����	
013	�ƶ�����	
014	����IVR	
017	��ҵ����ֱ������	
018	17266	�ֻ�������over CSD��ҵ�񻰵�
�޸ļ�¼
����	�޸�����
	
	

����	��������	����
1000	δʹ��IP	
2000	�й��ƶ�	
2100	    IP����	
2101	        17950	
2102	        17951	
2103	        12593	
2199	        ����	
2200	    IP��������	
2201	        17200	
2202	17201	
2203	17202	
2204	        17255	
2299	        ����	
3000	�й���ͨ	��ԭάֵ��3000 �й���ͨ���͡�5000�й���ͨ���ϲ�����3000 �й���ͨ��
3100	    IP����	��ԭάֵ��3100 IP�������͡�5100 IP�������ϲ�����3100 IP������
3101	        17910	
3102	        17911	
3103	        193	
3104	        19300	
3105	        19730	ԭάֵ��5101��19730��
3106	        17931	ԭάֵ��5102��17931��
3107	        17920	ԭάֵ��5103��17920��
3108	        17921	ԭάֵ��5104��17921��
3109	17960	ԭάֵ��5105��17960��
3110	17961	ԭάֵ��5106��17961��
3111	17968	ԭάֵ��5107��17968��
3112	17969	ԭάֵ��5108��17969��
3113	17908	ԭάֵ��5109��17908��
3114	17909	ԭάֵ��5110��17909��
3199	        ����	��ԭάֵ��5199 �������͡�3199 �������ϲ�����3199 ������
3200	    IP��������	��ԭάֵ��5200 IP�����������͡�3200 IP�����������ϲ�����3200 IP����������
4000	�й�����	��ԭάֵ��7000�й���ͨ���͡�4000�й����š��ϲ�����4000�й����š�
4100	    IP����	��ԭάֵ��7100 IP�������͡�4100 IP�������ϲ�����4100 IP������
4101	        17900	
4102	        17901	
4103	        17908	
4104	        17909	
4105	        200	
4106	        300	
4107	        190	
4108	        17968	
4109	        17969	
4110	17970	ԭάֵ��7101��17970��
4111	17971	ԭάֵ��7102��17971��
4199	        ����	
4200	    IP��������	
6000	�й���ͨ	
6100	    IP����	
6101	        17990	
6102	        17991	
6103	        068	
6104	        068300	
6105	        96168	
6106	        197	
6107	        197300	
6199	        ����	
6200	    IP��������	
9000	����	
�޸ļ�¼
����	�޸�����
	
	

1000	δʹ��IP
2000	�й��ƶ�
2100	    IP����
2101	        17950
2102	        17951
2103	        12593
2199	        ����

30608




select count(0)
from table(
select 
		BILL_MONTH
        ,PRODUCT_NO
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,TOLL_TYPE_ID
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
        ,MNS_TYPE
		,count(0)
		from G_S_21003_MONTH_TP 
		group by 
		BILL_MONTH
        ,PRODUCT_NO
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,TOLL_TYPE_ID
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
        ,MNS_TYPE
having count(0)		 > 1

    ) t







CREATE TABLE "BASS1   "."G_S_21003_MONTH_PK20120608_2A"  (
                  "TIME_ID" INTEGER , 
                  "BILL_MONTH" CHAR(6) NOT NULL , 
                  "BRAND_ID" CHAR(1) NOT NULL , 
                  "SVC_TYPE_ID" CHAR(3) NOT NULL , 
                  "TOLL_TYPE_ID" CHAR(3) NOT NULL , 
                  "IP_TYPE_ID" CHAR(4) NOT NULL , 
                  "ADVERSARY_ID" CHAR(6) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "CALL_TYPE_ID" CHAR(2) NOT NULL , 
                  "MNS_TYPE" CHAR(1),
				  COUNTS BIGINT
				  )   
                 DISTRIBUTE BY HASH("TIME_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 




insert into G_S_21003_MONTH_PK20120608_2A
select
         TIME_ID
        ,BILL_MONTH
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,TOLL_TYPE_ID
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
		,MNS_TYPE
		,count(0)
from G_S_21003_MONTH_20120607
where time_id = 201205
group by 
         TIME_ID
        ,BILL_MONTH
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,TOLL_TYPE_ID
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
		,MNS_TYPE




                   2       30608
				   
				   
				   
				   


CREATE TABLE "BASS1   "."G_S_21003_MONTH_TP3"  (
                  "TIME_ID" INTEGER , 
                  "BILL_MONTH" CHAR(6) NOT NULL , 
                  "PRODUCT_NO" CHAR(15) NOT NULL , 
                  "BRAND_ID" CHAR(1) NOT NULL , 
                  "SVC_TYPE_ID" CHAR(3) NOT NULL , 
                  "TOLL_TYPE_ID" CHAR(3) NOT NULL , 
                  "IP_TYPE_ID" CHAR(4) NOT NULL , 
                  "ADVERSARY_ID" CHAR(6) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "CALL_TYPE_ID" CHAR(2) NOT NULL , 
                  "CALL_COUNTS" CHAR(14) NOT NULL , 
                  "BASE_BILL_DURATION" CHAR(14) NOT NULL , 
                  "TOLL_BILL_DURATION" CHAR(14) NOT NULL , 
                  "CALL_DURATION" CHAR(14) NOT NULL , 
                  "BASE_CALL_FEE" CHAR(14) NOT NULL , 
                  "TOLL_CALL_FEE" CHAR(14) NOT NULL , 
                  "CALLFW_TOLL_FEE" CHAR(14) NOT NULL , 
                  "CALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_BASECALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_TOLLCALL_FEE" CHAR(14) NOT NULL , 
                  "FAVOURED_CALLFW_TOLLFEE" CHAR(14) NOT NULL , 
                  "FAVOURED_CALL_FEE" CHAR(14) NOT NULL , 
                  "FREE_DURATION" CHAR(14) NOT NULL , 
                  "FAVOUR_DURATION" CHAR(14) NOT NULL , 
                  "MNS_TYPE" CHAR(1) )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 
				   
				   
 set sql_buff "alter table bass1.G_S_21003_MONTH_TP2 activate not logged initially with empty table"				   

insert into G_S_21003_MONTH_TP2
select
         TIME_ID
        ,BILL_MONTH
        ,PRODUCT_NO
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,TOLL_TYPE_ID
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
        ,char(sum(bigint(CALL_COUNTS)))
        ,char(sum(bigint(BASE_BILL_DURATION)))
        ,char(sum(bigint(TOLL_BILL_DURATION)))
        ,char(sum(bigint(CALL_DURATION)))
        ,char(sum(bigint(BASE_CALL_FEE)))
        ,char(sum(bigint(TOLL_CALL_FEE)))
        ,char(sum(bigint(CALLFW_TOLL_FEE)))
        ,char(sum(bigint(CALL_FEE)))
        ,char(sum(bigint(FAVOURED_BASECALL_FEE)))
        ,char(sum(bigint(FAVOURED_TOLLCALL_FEE)))
        ,char(sum(bigint(FAVOURED_CALLFW_TOLLFEE)))
        ,char(sum(bigint(FAVOURED_CALL_FEE)))
        ,char(sum(bigint(FREE_DURATION)))
        ,char(sum(bigint(FAVOUR_DURATION)))
        ,MNS_TYPE
		from G_S_21003_MONTH_TP
		group by 
         TIME_ID
        ,BILL_MONTH
        ,PRODUCT_NO
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,TOLL_TYPE_ID
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
        ,MNS_TYPE		





select count(0)
from table(
select 
		BILL_MONTH
        ,PRODUCT_NO
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,TOLL_TYPE_ID
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
        ,MNS_TYPE
		,count(0)
		from G_S_21003_MONTH_TP2
		group by 
		BILL_MONTH
        ,PRODUCT_NO
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,TOLL_TYPE_ID
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
        ,MNS_TYPE
having count(0)		 > 1

    ) t







select count(0)
from table(
select 
		BILL_MONTH
        ,PRODUCT_NO
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,case when  length(trim(TOLL_TYPE_ID)) = 2 then  '0'||trim(TOLL_TYPE_ID) else  TOLL_TYPE_ID end  TOLL_TYPE_ID
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
        ,MNS_TYPE
		,count(0)
		from G_S_21003_MONTH_TP2
		group by 
		BILL_MONTH
        ,PRODUCT_NO
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,case when  length(trim(TOLL_TYPE_ID)) = 2 then  '0'||trim(TOLL_TYPE_ID) else  TOLL_TYPE_ID end 
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
        ,MNS_TYPE
having count(0)		 > 1

    ) t





               

select count(0) val1 
from (  select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID
from bass1.G_S_21003_MONTH_TP2
where time_id=201205 and mns_type='1'
except
select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
from bass1.g_s_04017_day
where time_id/100=201205 and mns_type='1'
group by product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
) M
with ur




               

select count(0) val1 
from ( 
select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
from bass1.g_s_04017_day
where time_id/100=201205 and mns_type='1'
group by product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
except
select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID
from bass1.G_S_21003_MONTH_TP2
where time_id=201205 and mns_type='1'

) M
with ur





select count(0)
from table(
select 
		BILL_MONTH
        ,PRODUCT_NO
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,case when  length(trim(TOLL_TYPE_ID)) = 2 then  '0'||trim(TOLL_TYPE_ID) else  TOLL_TYPE_ID end  TOLL_TYPE_ID
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
        ,MNS_TYPE
		,count(0)
		from G_S_21003_MONTH_TP2
		group by 
		BILL_MONTH
        ,PRODUCT_NO
        ,BRAND_ID
        ,SVC_TYPE_ID
        ,case when  length(trim(TOLL_TYPE_ID)) = 2 then  '0'||trim(TOLL_TYPE_ID) else  TOLL_TYPE_ID end 
        ,IP_TYPE_ID
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
        ,MNS_TYPE
having count(0)		 > 1

    ) t




select 
TOLL_TYPE_ID
,case when  length(trim(TOLL_TYPE_ID)) = 2 then  '0'||trim(TOLL_TYPE_ID) else  TOLL_TYPE_ID end TOLL_TYPE_ID2
,count(0)
from G_S_21003_MONTH_TP2
group by 
TOLL_TYPE_ID
,case when  length(trim(TOLL_TYPE_ID)) = 2 then  '0'||trim(TOLL_TYPE_ID) else  TOLL_TYPE_ID end 




��G_S_21003_MONTH_TP2 ������


G_S_21003_MONTH_TP2

rename bass1.G_S_21003_MONTH to G_S_21003_MONTH_bak0608

insert into G_S_21003_MONTH
select *
from G_S_21003_MONTH_TP2


ksh  bass1_export_lite.sh G_S_21003_MONTH 201206 &


22562878
$ 
$ db2 "select count(0) from G_S_21003_MONTH"

1          
-----------
   22532270

22532270


select 
TOLL_TYPE_ID
,case when  length(trim(TOLL_TYPE_ID)) = 2 then  '0'||trim(TOLL_TYPE_ID) else  TOLL_TYPE_ID end TOLL_TYPE_ID2
,count(0)
from G_S_21003_MONTH
group by 
TOLL_TYPE_ID
,case when  length(trim(TOLL_TYPE_ID)) = 2 then  '0'||trim(TOLL_TYPE_ID) else  TOLL_TYPE_ID end 




-----

select time_id,length(trim(TOLL_TYPE_ID)) L,count(0)
from g_s_21003_to_day
where time_id/100 = 201206
group by time_id,length(trim(TOLL_TYPE_ID)) 
order by 1


$ db2 "
> 
select time_id,length(trim(TOLL_TYPE_ID)) L,count(0)
> select time_id,length(trim(TOLL_TYPE_ID)) L,count(0)
> from g_s_21003_to_day
> where time_id/100 = 201206
group by time_id,length(trim(TOLL_TYPE_ID)) 
> group by time_id,length(trim(TOLL_TYPE_ID)) 
> order by 1
> "
 
TIME_ID     L           3          
----------- ----------- -----------
   20120601           2     3674952
   20120602           2     3395569
   20120603           2     3444118
   20120604           2     3672747
   20120605           2     3531684
   20120606           2     3571336
   20120607           3     3549227
   20120608           3     3612574
   20120609           3     3545683
   20120610           3     3526760
   20120611           3     3648672
   20120612           3     3630073
   20120613           3     3650290
   20120614           3     3664904
   20120615           3     3701729
   20120616           3     3571623
   20120617           3     3597994
   20120618           3     3713450
   20120619           3     3791900
   20120620           3     3708119
   20120621           3     3782329
   20120622           3     3742149
   20120623           3     3795488
   20120624           3     3602653
   20120625           3     3791588
   20120626           3     3860003
   20120627           3     3841176
   20120628           3     3869852
   20120629           3     3906354
   20120630           3     3837791

  30 record(s) selected.



$  db2 "
> 

> > select count(0)
from table(
> from table(
> select 
>               BILL_MONTH
>         ,PRODUCT_NO
>         ,BRAND_ID
        ,SVC_TYPE_ID
>         ,SVC_TYPE_ID
>         ,case when  length(trim(TOLL_TYPE_ID)) = 2 then  '0'||trim(TOLL_TYPE_ID) else  TOLL_TYPE_ID end  TOLL_TYPE_ID
>         ,case when length(trim(TOLL_TYPE_ID)) = 2 then '2199' else IP_TYPE_ID end IP_TYPE_ID
        ,ADVERSARY_ID
>         ,ADVERSARY_ID
>         ,ROAM_TYPE_ID
        ,CALL_TYPE_ID
>         ,CALL_TYPE_ID
>         ,MNS_TYPE
                ,count(0)
>               ,count(0)
>               from G_S_21003_MONTH_TP
>               group by 
                BILL_MONTH
        ,PRODUCT_NO
>               BILL_MONTH
>         ,PRODUCT_NO
>         ,BRAND_ID
        ,SVC_TYPE_ID
>         ,SVC_TYPE_ID
>         ,case when  length(trim(TOLL_TYPE_ID)) = 2 then  '0'||trim(TOLL_TYPE_ID) else  TOLL_TYPE_ID end 
>         ,case when length(trim(TOLL_TYPE_ID)) = 2 then '2199' else IP_TYPE_ID end
        ,ADVERSARY_ID
        ,ROAM_TYPE_ID
>         ,ADVERSARY_ID
>         ,ROAM_TYPE_ID
>         ,CALL_TYPE_ID
        ,MNS_TYPE
having count(0)          > 1

>         ,MNS_TYPE   ) t               
> having count(0)                > 1
>     ) t               >     ) t               "

1          
-----------
      53434
	  
	  
select ALL_AFTERS,BEFORE_CONTROL_CODE,BEGINTIME from   table( bass1.getallafter('BASS1_G_S_21003_MONTH.tcl') ) a 
where all_afters like 'BASS1%INT%'
order by BEGINTIME

										
										
db2 "select count(0) from G_S_21003_MONTH_TP"
db2 "select count(0) from G_S_21003_MONTH"
					
db2 "select count(0) from G_S_21003_MONTH_TP2"										
db2 "select count(0) from G_S_21003_MONTH_20120707"										




select count(0) val1 
from (  select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID
from bass1.G_S_21003_MONTH_TP2
where time_id=201206 and mns_type='1'
except
select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
from bass1.g_s_04017_day
where time_id/100=201206 and mns_type='1'
group by product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
) M
with ur


sh bass1_adj_sql.sh -or BASS1_INT_CHECK_VOICE_MONTH.tcl &
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_R262_MONTH.tcl &
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_TD_MONTH.tcl &
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_R221_MONTH.tcl &
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_R235_MONTH.tcl &
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_R229_MONTH.tcl &



lt *02031* *02032* *02033*

$ lt *02031* *02032* *02033*
-rw-r--r--   1 bass1    appdb       2408 2011   8��  7 G_I_02033_MONTH.tcl
-rw-r--r--   1 bass1    appdb       2116 2011   9��  7 G_I_02032_MONTH.tcl
-rw-r--r--   1 bass1    appdb       2656  7��  7�� 21:21 G_I_02031_MONTH.tcl

sh bass1_adj_sql.sh -or BASS1_G_I_02031_MONTH.tcl
sh bass1_adj_sql.sh -or BASS1_G_I_02032_MONTH.tcl
sh bass1_adj_sql.sh -or BASS1_G_I_02033_MONTH.tcl


sh bass1_adj_sql.sh -or BASS1_EXP_G_I_02031_MONTH
sh bass1_adj_sql.sh -or BASS1_EXP_G_I_02032_MONTH
sh bass1_adj_sql.sh -or BASS1_EXP_G_I_02033_MONTH


 ps -ef|grep Run_xysc.sh
 
 
 db2 "select count(0) from G_I_02031_MONTH where time_id = 201206"
 