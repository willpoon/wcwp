ailknfjdbass99##%%
D2008ABCDE
>���ļ� 21003

#export���ʹϵͳ�ڴ���ÿһ���µ�shellʱ�������������һ��������������̳�֮Ϊ���������
open 
http://172.16.5.71:4321/svn/%e9%a1%b9%e7%9b%ae%e7%bb%84%e5%9f%b9%e8%ae%ad%e8%b5%84%e6%96%99/%e9%a1%b9%e7%9b%ae%e7%bb%84%e8%b5%84%e6%96%99/%e5%ad%a6%e4%b9%a0%e8%b5%84%e6%96%99/

up:
http://172.16.5.71:4321/svn/%e9%a1%b9%e7%9b%ae%e7%bb%84%e5%9f%b9%e8%ae%ad%e8%b5%84%e6%96%99/%e9%a1%b9%e7%9b%ae%e7%bb%84%e8%b5%84%e6%96%99/



XZBI\��Ŀ����ѵ����\��Ŀ������\ѧϰ����

XZBI/��Ŀ����ѵ����/��Ŀ������/ѧϰ����


http://172.16.5.71:4321/svn/%e9%a1%b9%e7%9b%ae%e7%bb%84%e5%9f%b9%e8%ae%ad%e8%b5%84%e6%96%99/%e9%a1%b9%e7%9b%ae%e7%bb%84%e8%b5%84%e6%96%99/%e5%ad%a6%e4%b9%a0%e8%b5%84%e6%96%99/


 my_pass=bass2
 db2 "connect to bassdb user bass2 using ${my_pass}"
 
 
 select * from etl_task_running 
where	task_id	in	('I03013')	

select * from etl_task_log  
where	task_id	in	('I03013')	

 
	set ip_num ('17950','17951')
	
�ƶ�����
substr(product_no,1,3) in ('135','136','138','139','147','150','152','157','158','182','187','188')
and length(rtrim(ltrim(product_no)))=11

	

db2 => select * from DIM_FEETYPE_ITEM

ITEM_ID     ITEM_NAME                                                                                            FEETYPE_ID  FEETYPE_NAME                                                                                        
----------- ---------------------------------------------------------------------------------------------------- ----------- ----------------------------------------------------------------------------------------------------
SQL0668N  Operation not allowed for reason code "3" on table 
"BASS2.DIM_FEETYPE_ITEM".  SQLSTATE=57016
db2 => ? 57016

SQLSTATE 57016: The table cannot be accessed, because it is inactive.

	
	
	

       opp_noaccess_number  varchar(30),       --ȥIPͷ�ĶԶ˺���
       opp_roam_areacode    varchar(7),        --�Զ�����λ������
       opp_home_areacode    varchar(7),        --�Զ˹�����;����
       
svn://172.16.5.71/XZBI

XZBI\NG1-BASS1.0\Truck\05 ���߿�

E:\SVN\NG1-BASS1.0\Truck\03 ������\interface\��ȡ\doc
BOSS�ӿڹ滮v1.00.xls
E:\SVN\NG1-BASS1.0\Truck\03 ������\interface\����\doc
ODS����ű�.sql

������Ϊ��������ˡ������Ʒ+רҵ���񡱵ķ�չģʽ��

���������ƶ���ҵ�����������ܾ���������������13����Ա�����������廹������������š�
����������˵����������PSO(Professional Service Organization��רҵ�������)�ĵ�һ�ˡ�������֮ǰ���з�������ʱ����û��ר�ŵķ����Ÿ��ͻ��������е�����ȫ�����е��з��������������з�����Ա�����ֳ����������з����Ժ���������з��ǳ�������ͨ����PSO���ŵĴ��죬����ͬʱ�����˿ͻ����������Ͳ�Ʒ�з��ļ��жȡ�


NG1-BASS1.0/Truck/05 ���߿�


select * from syscat.tables where TABNAME like 'CDR_CALL%'

select * from cdr_call_20100621 fetch first 10 rows only


select * from USYS_TABLE_MAINTAIN order by 1



select substr(opp_noaccess_number,2,length(opp_home_areacode))
,opp_home_areacode
,rtrim(substr(opp_noaccess_number,length(opp_home_areacode)+2,20)) 
from cdr_call_20100621

--where substr(opp_noaccess_number,1,1)='0'
--and fetch first 10 rows only

="insert into etl_load_table_map values('"&A208&"','"&B208&"','"&C208&"',"&D208&",'"&E208&"');"

 drop table ${db_user}.cdr_call_dtl_$p_timestamp;
 create table ${db_user}.cdr_call_dtl_$p_timestamp like ${db_user}.cdr_call_dtl_yyyymmdd in $tbs_voice 
               index in tbs_index partitioning key (user_id) using hashing not logged initially;

 declare global temporary table session.t_cdr_call_dtl_1 like  ${db_user}.cdr_call_dtl_yyyymmdd partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;
               

select * from ETL_LOAD_TABLE_MAP ;
select * from sch_control_task  ;
select * from USYS_TABLE_MAINTAIN ;                              
               
               
select count(0),TABSCHEMA from syscat.tables group by TABSCHEMA order by 1

db2 catalog tcpip node bass2 remote 172.16.9.25 server 50000
db2 catalog database bassdb as bass2 at node bass2 authentication server
db2 terminate


db2 catalog tcpip node node48 remote 172.16.5.48 server 50000
db2 catalog database bassdb as bassdb48 at node node48 authentication server
db2 terminate


--ע��ڵ�
catalog tcpip node CQCRM remote 10.191.113.132 server 50000;
--ע�����ݿ�
catalog database CQCCDW at node CQCRM;
--ɾ��ע��ڵ�
uncatalog node CQCRM;
--ɾ��ע�����ݿ�
uncatalog database CQCCDW; 



 drop table ${db_user}.cdr_call_dtl_$p_timestamp;
 create table ${db_user}.cdr_call_dtl_$p_timestamp like ${db_user}.cdr_call_dtl_yyyymmdd in $tbs_voice 
               index in tbs_index partitioning key (user_id) using hashing not logged initially;

 declare global temporary table session.t_cdr_call_dtl_1 like  ${db_user}.cdr_call_dtl_yyyymmdd partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;



 insert into session.t_cdr_call_dtl_1
               (cust_id         ,user_id      ,acct_id            ,product_no     ,opp_number        ,opp_regular_number,
                product_id      ,service_id   ,drtype_id          ,province_id    ,city_id           ,county_id         ,
                roam_province_id,roam_city_id ,roam_county_id     ,opp_city_id    ,opp_roam_city_id  ,brand_id          ,
                plan_id         ,bill_mark    ,ip_mark            ,netcall_mark   ,speserver_mark    ,calltype_id       ,
                tolltype_id     ,tolltype_id2 ,roamtype_id        ,callfwtype_id  ,opp_access_type_id,opposite_id       ,
                callmoment_id   ,rate_prod_id ,msc_id             ,lac_id         ,cell_id           ,imei              ,
                start_time      ,imsi         ,msrn               ,a_number       ,scp_id            ,out_trunkid       ,
                in_trunkid	     ,stop_cause   ,moc_id		         ,mtc_id 		    ,enterprise_id     ,service_type      ,
                service_code    ,bill_indicate,sp_rela_type       ,rating_flag    ,item_code1        ,item_code2        ,
                item_code3      ,item_code4   ,free_res_code1     ,free_res_code2 ,free_res_code3    ,original_file     ,
                usertype_id     ,opp_plan_id  ,opp_noaccess_number,video_type     ,mns_type          ,user_property     ,
                opp_property    ,call_refnum  ,fci_type           ,input_time     ,rating_res        ,free_res_val1     ,
                free_res_val2   ,free_res_val3,std_unit           ,toll_std_unit  ,ori_basic_charge  ,ori_toll_charge   ,
                ori_other_charge,addup_res    ,call_duration      ,call_duration_m,call_duration_s   ,bill_duration     ,
                basecall_fee    ,toll_fee     ,info_fee           ,other_fee      ,charge1           ,charge1_disc      ,
                charge2         ,charge2_disc ,charge3            ,charge3_disc   ,charge4           ,charge4_disc)
               select cust_id           ,user_id         ,acct_id            ,product_no     ,opp_number        ,
                      case when substr(opp_noaccess_number,1,1)='0' then 
                           case when substr(opp_noaccess_number,2,length(opp_home_areacode))=opp_home_areacode then rtrim(substr(opp_noaccess_number,length(opp_home_areacode)+2,20)) 
                                else opp_noaccess_number end
               		      else opp_noaccess_number end as opp_regular_number,
                      product_id        ,service_id      ,drtype_id          ,province_id        ,city_id           ,county_id         ,
                      roam_province_id  ,roam_city_id    ,roam_county_id     ,
                      opp_home_areacode as opp_city_id,
                      opp_roam_areacode as opp_roam_city_id,
                      brand_id          ,plan_id         ,
                      case when (coalesce(charge1,0)+coalesce(charge2,0)+coalesce(charge3,0)+coalesce(charge4,0))>0 then 1 else 0 end as bill_mark,
                      case when substr(opp_number,1,5) in $ip_num then 1 else 0 end as ip_mark,
               		 case when netcall_flag=11 then 1 else 0 end as netcall_mark,
               		 0 as speserver_mark,
               		 calltype_id       ,
               		 case when substr(opp_number,1,5) in '17950' then 120
               		      when tolltype_id=1  and calltype_id=0 and substr(opp_number,1,5) in '17951' then 101
               		      when tolltype_id=10 and calltype_id=0 and substr(opp_number,1,5) in '17951' then 102
               		      when tolltype_id=3  and calltype_id=0 and substr(opp_number,1,5) in '17951' then 103
               		      when tolltype_id=4  and calltype_id=0 and substr(opp_number,1,5) in '17951' then 104
               		      when tolltype_id=5  and calltype_id=0 and substr(opp_number,1,5) in '17951' then 105
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='91' and substr(opp_number,1,5) in '17951' then 106
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='977' and substr(opp_number,1,5) in '17951' then 107
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='1' and substr(opp_number,1,5) in '17951' then 108
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='81' and substr(opp_number,1,5) in '17951' then 109
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='82' and substr(opp_number,1,5) in '17951' then 110
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='61' and substr(opp_number,1,5) in '17951' then 111
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='44' and substr(opp_number,1,5) in '17951' then 112
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='33' and substr(opp_number,1,5) in '17951' then 113
               		      when tolltype_id=2  and calltype_id=0 and substr(opp_number,1,5) in '17951' then 999
               		      when tolltype_id=0 then 0 when tolltype_id=1 then 1 when tolltype_id=10 then 2
               		      when tolltype_id=3 then 3 when tolltype_id=4 then 4 when tolltype_id=5 then 5
               		      when tolltype_id=2 and opp_home_areacode='91' then 6
               		      when tolltype_id=2 and opp_home_areacode='977' then 7
               		      when tolltype_id=2 and opp_home_areacode='1' then 8
               		      when tolltype_id=2 and opp_home_areacode='81' then 9
               		      when tolltype_id=2 and opp_home_areacode='82' then 10
               		      when tolltype_id=2 and opp_home_areacode='61' then 11
               		      when tolltype_id=2 and opp_home_areacode='44' then 12
               		      when tolltype_id=2 and opp_home_areacode='33' then 13
               		      when tolltype_id=2 then 99
               		      else 0 end as tolltype_id,
               		 tolltype_id2      ,
               		 case when roamtype_id<>5 then roamtype_id
               		      when roamtype_id=5 and roam_province_id in ('853','852','886') then 5
               		      when roamtype_id=5 then 9
               		      else 0 end as roamtype_id,
               		 case when calltype_id=2 then 1 when calltype_id=3 then 2 else 0 end as callfwtype_id,
               		 opp_access_type_id,opposite_id,
                      case when substr(char(start_time),12,2)= '00' then 1  when substr(char(start_time),12,2)= '01' then 2
               		      when substr(char(start_time),12,2)= '02' then 3  when substr(char(start_time),12,2)= '03' then 4
               		      when substr(char(start_time),12,2)= '04' then 5  when substr(char(start_time),12,2)= '05' then 6
               		      when substr(char(start_time),12,2)= '06' then 7  when substr(char(start_time),12,2)= '07' then 8
               		      when substr(char(start_time),12,2)= '08' then 9  when substr(char(start_time),12,2)= '09' then 10
               		      when substr(char(start_time),12,2)= '10' then 11 when substr(char(start_time),12,2)= '11' then 12
               		      when substr(char(start_time),12,2)= '12' then 13 when substr(char(start_time),12,2)= '13' then 14
               		      when substr(char(start_time),12,2)= '14' then 15 when substr(char(start_time),12,2)= '15' then 16
               		      when substr(char(start_time),12,2)= '16' then 17 when substr(char(start_time),12,2)= '17' then 18
               		      when substr(char(start_time),12,2)= '18' then 19 when substr(char(start_time),12,2)= '19' then 20
               		      when substr(char(start_time),12,2)= '20' then 21 when substr(char(start_time),12,2)= '21' then 22
               		      when substr(char(start_time),12,2)= '22' then 23 when substr(char(start_time),12,2)= '23' then 24
               		      else 0 end as callmoment_id,
               		 rate_prod_id      ,msc_id       ,
               		 upper(lac_id) as lac_id,
		         	    upper(cell_id) as cell_id,
		         	    imei              ,start_time      ,imsi               ,msrn               ,a_number          ,scp_id            ,
		         	    out_trunkid       ,in_trunkid	   ,stop_cause         ,moc_id		       ,mtc_id 		     ,enterprise_id     ,
		         	    service_type      ,service_code    ,bill_indicate      ,sp_rela_type       ,rating_flag       ,item_code1        ,
		         	    item_code2        ,item_code3      ,item_code4         ,free_res_code1     ,free_res_code2    ,free_res_code3    ,
		         	    original_file     ,usertype_id     ,opp_plan_id        ,opp_noaccess_number,video_type        ,mns_type          ,
		         	    user_property     ,opp_property    ,call_refnum        ,fci_type           ,input_time        ,rating_res        ,
		         	    free_res_val1     ,free_res_val2   ,free_res_val3      ,std_unit           ,toll_std_unit     ,ori_basic_charge  ,
		         	    ori_toll_charge   ,ori_other_charge,addup_res          ,call_duration      ,
               		 case when call_duration>=0 then (call_duration+59)/60 else (call_duration-59)/60 end as call_duration_m,
               		 case when tolltype_id<>0 and roamtype_id not in (1,2,4,6,7,8) and call_duration>=0 then (call_duration+59)/60
               		      when tolltype_id in (2,3,4,5) and roamtype_id in (1,2,4,6,7,8) and call_duration>=0 then (call_duration+59)/60
               		      when tolltype_id<>0 and roamtype_id not in (1,2,4,6,7,8) and call_duration<0 then (call_duration-59)/60
               		      when tolltype_id in (2,3,4,5) and roamtype_id in (1,2,4,6,7,8) and call_duration<0 then (call_duration-59)/60
               		      else 0 end as call_duration_s,
               		 case when (coalesce(charge1,0)+coalesce(charge2,0)+coalesce(charge3,0)+coalesce(charge4,0))>0 then call_duration else 0 end as bill_duration,
               		 coalesce(charge1,0)/1000.00 as basecall_fee,
               		 coalesce(charge2,0)/1000.00 as toll_fee,
               		 coalesce(charge4,0)/1000.00 as info_fee,
               		 coalesce(charge3,0)/1000.00 as other_fee,
               		 coalesce(charge1,0)/1000.00 as charge1,
               		 coalesce(charge1_disc,0)/1000.00 as charge1_disc,
               		 coalesce(charge2,0)/1000.00 as charge2,
               		 coalesce(charge2_disc,0)/1000.00 as charge2_disc,
               		 coalesce(charge3,0)/1000.00 as charge3,
               		 coalesce(charge3_disc,0)/1000.00 as charge3_disc,
               		 coalesce(charge4,0)/1000.00 as charge4,
               		 coalesce(charge4_disc,0)/1000.00 as charge4_disc
               from ${db_user}.cdr_call_$p_timestamp
               where calltype_id in (0,1,2,3);


 alter table ${db_user}.cdr_call_dtl_$p_timestamp activate not logged initially


	
	
 insert into ${db_user}.cdr_call_dtl_$p_timestamp
               (cust_id         ,user_id      ,acct_id            ,product_no     ,opp_number        ,opp_regular_number,
                product_id      ,service_id   ,drtype_id          ,province_id    ,city_id           ,county_id         ,
                roam_province_id,roam_city_id ,roam_county_id     ,opp_city_id    ,opp_roam_city_id  ,brand_id          ,
                plan_id         ,bill_mark    ,ip_mark            ,netcall_mark   ,speserver_mark    ,calltype_id       ,
                tolltype_id     ,tolltype_id2 ,roamtype_id        ,callfwtype_id  ,opp_access_type_id,opposite_id       ,
                callmoment_id   ,rate_prod_id ,msc_id             ,lac_id         ,cell_id           ,imei              ,
                start_time      ,imsi         ,msrn               ,a_number       ,scp_id            ,out_trunkid       ,
                in_trunkid	     ,stop_cause   ,moc_id		         ,mtc_id 		    ,enterprise_id     ,service_type      ,
                service_code    ,bill_indicate,sp_rela_type       ,rating_flag    ,item_code1        ,item_code2        ,
                item_code3      ,item_code4   ,free_res_code1     ,free_res_code2 ,free_res_code3    ,original_file     ,
                usertype_id     ,opp_plan_id  ,opp_noaccess_number,video_type     ,mns_type          ,user_property     ,
                opp_property    ,call_refnum  ,fci_type           ,input_time     ,rating_res        ,free_res_val1     ,
                free_res_val2   ,free_res_val3,std_unit           ,toll_std_unit  ,ori_basic_charge  ,ori_toll_charge   ,
                ori_other_charge,addup_res    ,call_duration      ,call_duration_m,call_duration_s   ,bill_duration     ,
                basecall_fee    ,toll_fee     ,info_fee           ,other_fee      ,charge1           ,charge1_disc      ,
                charge2         ,charge2_disc ,charge3            ,charge3_disc   ,charge4           ,charge4_disc)
                select cust_id         ,user_id            ,acct_id            ,product_no        ,opp_number        ,opp_regular_number,
                       product_id      ,service_id         ,drtype_id          ,province_id       ,city_id           ,county_id         ,
                       roam_province_id,roam_city_id       ,roam_county_id     ,opp_city_id       ,opp_roam_city_id  ,brand_id          ,
                       plan_id         ,bill_mark          ,ip_mark            ,netcall_mark      ,                                     
                       case when opp_regular_number in ('110','119','1860','1861','120','122','13800138000','10084','10085','10086') then 1 else 0 end as speserver_mark,
                       calltype_id     ,                                                                                     
                       case when roamtype_id=0 and tolltype_id<>0 and calltype_id=1 then 0 else tolltype_id end as tolltype_id,
                       tolltype_id2    ,roamtype_id        ,callfwtype_id      ,opp_access_type_id,opposite_id       ,callmoment_id     ,
                       rate_prod_id    ,msc_id             ,lac_id             ,cell_id           ,imei              ,start_time        ,
                       imsi            ,msrn               ,a_number           ,scp_id            ,out_trunkid       ,in_trunkid	       ,
                       stop_cause      ,moc_id		        ,mtc_id 		       ,enterprise_id     ,service_type      ,service_code      ,
                       bill_indicate   ,sp_rela_type       ,rating_flag        ,item_code1        ,item_code2        ,item_code3        ,
                       item_code4      ,free_res_code1     ,free_res_code2     ,free_res_code3    ,original_file     ,usertype_id       ,
                       opp_plan_id     ,opp_noaccess_number,video_type         ,mns_type          ,user_property     ,opp_property    ,
                       call_refnum     ,fci_type           ,input_time         ,rating_res        ,free_res_val1     ,free_res_val2     ,
                       free_res_val3   ,std_unit           ,toll_std_unit      ,ori_basic_charge  ,ori_toll_charge   ,ori_other_charge  ,
                       addup_res       ,call_duration      ,call_duration_m    ,
                       case when roamtype_id=0 and tolltype_id<>0 and calltype_id=1 then 0 else call_duration_s end as call_duration_s,
                       bill_duration   ,basecall_fee       ,toll_fee           ,info_fee          ,other_fee         ,charge1           ,
                       charge1_disc    ,charge2            ,charge2_disc       ,charge3           ,charge3_disc      ,charge4           ,
                       charge4_disc
                from session.t_cdr_call_dtl_1;


 create index ${db_user}.i_c_dtl_${p_timestamp}_1 on ${db_user}.cdr_call_dtl_${p_timestamp}(user_id);


 create index ${db_user}.i_c_dtl_${p_timestamp}_2 on ${db_user}.cdr_call_dtl_${p_timestamp}(opp_number);




db2 => select * from DIM_FEETYPE_ITEM

ITEM_ID     ITEM_NAME                                                                                            FEETYPE_ID  FEETYPE_NAME                                                                                        
----------- ---------------------------------------------------------------------------------------------------- ----------- ----------------------------------------------------------------------------------------------------
SQL0668N  Operation not allowed for reason code "3" on table 
"BASS2.DIM_FEETYPE_ITEM".  SQLSTATE=57016
db2 => ? 57016

SQLSTATE 57016: The table cannot be accessed, because it is inactive.

	
2010-10-19 17:19
workplace:

Windows IP Configuration



        Host Name . . . . . . . . . . . . : ailk-pso-100914

        Primary Dns Suffix  . . . . . . . : 

        Node Type . . . . . . . . . . . . : Unknown

        IP Routing Enabled. . . . . . . . : No

        WINS Proxy Enabled. . . . . . . . : No



Ethernet adapter ��������:



        Connection-specific DNS Suffix  . : 

        Description . . . . . . . . . . . : Broadcom 440x 10/100 Integrated Controller

        Physical Address. . . . . . . . . : 00-15-C5-7A-67-3F

        Dhcp Enabled. . . . . . . . . . . : Yes

        Autoconfiguration Enabled . . . . : Yes

        IP cress. . . . . . . . . . . . : 172.16.24.146

        Subnet Mask . . . . . . . . . . . : 255.255.255.128

        Default Gateway . . . . . . . . . : 172.16.24.250

        DHCP Server . . . . . . . . . . . : 172.16.24.250

        DNS Servers . . . . . . . . . . . : 211.139.73.34

                                            211.139.73.35

        Lease Obtained. . . . . . . . . . : 2010��10��13�� 8:55:24

        Lease Expires . . . . . . . . . . : 2010��10��13�� 16:55:24



Ethernet adapter ������������:



        Media State . . . . . . . . . . . : Media disconnected

        Description . . . . . . . . . . . : Intel(R) PRO/Wireless 3945ABG Network Connection

        Physical Address. . . . . . . . . : 00-1B-77-2C-D0-29


2010-10-19 17:20


gzproxy.asiainfo-linkage.com:8080
*.asiainfo-linkage.com,10.*,localhost,127.0.0.1

172.16.5.43/46  ifboss/ifboss
bash -o vi


C:\Documents and Settings\ailk0914>ftp 172.16.9.25
Connected to 172.16.9.25.
220 NF-TEST07 FTP server ready.
User (172.16.9.25:(none)): bass2
331 Password required for bass2.
Password:
230 User bass2 logged in.
ftp> cd /db2backup/mov





XZBI\NG1-BASS1.0\Truck\05 ���߿�

172.16.9.25 bass2 / bass2


% puts $env(AITOOLSPATH) 
/bassapp/tcl/aiomnivision/aitools


if [ $? -eq 0 ] ; then
        #���Ӷ�app.sch_control_runlog��־�ĸ���add by xufr at 2009-1-6 21:25:03
        controlCode="BASS2_$script"
        DB2_SQLCOMM="db2 \"update app.sch_control_runlog set flag = 0 where control_code = '${controlCode}'\""
        DB2_SQL_EXEC()
        {
                date
                echo $DB2_SQLCOMM
            db2 terminate
            db2 "connect to bassdb user bass2 using ${my_pass}"
            eval $DB2_SQLCOMM
            db2 commit
            db2 connect reset
        }
        DB2_SQL_EXEC > /bassapp/bass2/tcl/DB2_SQL_EXEC.log
fi


tclsh $dssfile -s $script -d "bass2/${my_pass}@bassdb" -t $optime -p $timestamp -u 0 -v 0 


set script $arg(-s)
set dbstr $arg(-d)
set op_time $arg(-t)
set timestamp $arg(-p)
set ddh $arg(-u)
set rwh $arg(-v)

set pos1 [string first "/" $dbstr] 
set pos2 [string first "@" $dbstr] 

set username [string range $dbstr 0 [expr $pos1 - 1] ]
set passwd [string range $dbstr [expr $pos1 + 1] [expr $pos2 -1 ]]
set db [string range $dbstr [expr $pos2 + 1] end ]




 
db2 "load client from /bassapp/bass2/load/boss/I0301320101020000000.AVL of del modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 messages /bassapp/bass2/load/boss/messages/db2load.msg replace into DWD_NG2_I03013_20101020"
 
 
 load client from /bassapp/bass2/load/boss/I0301320101020000000.AVL of del  modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 messages /bassapp/bass2/load/boss/messages/db2load.msg replace into DWD_NG2_I03013_20101020
 
 
 load client from /bassapp/bass2/load/boss/I0301320101020000000.AVL of del  \
 modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" \
 fastparse anyorder warningcount 1000 messages /bassapp/bass2/load/boss/messages/db2load.msg 
 replace into DWD_NG2_I03013_20101020
 
 
 
-- 2010-10-18 15:16 PANZHIWEI ADD
CREATE TABLE ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_YYYYMMDD (
		,DATA_ID					BIGINT
		,BILL_ID					VARCHAR(120)
		,CUSTOMER_ORDER_ID			BIGINT
		,BUSIOPER_ORDER_ID			BIGINT
		,BUSINESS_ID				BIGINT
		,OFFER_ID					BIGINT
		,STATE						SMALLINT
		,REGION_ID					VARCHAR(6)
		,DONE_DATE					TIMESTAMP
		,CREATE_DATE				TIMESTAMP
		,CHANEL_ID					SMALLINT
		,START_DATE					TIMESTAMP
		,OP_ID						BIGINT
		,ORG_ID						BIGINT
		,EXT1						VARCHAR(400)
		,EXT2						VARCHAR(400)
		,EXT3						VARCHAR(400)
		,EXT4						VARCHAR(400)
		,EXT5						BIGINT
		,EXT6						BIGINT
		,EXT7						BIGINT
		,NOTES						VARCHAR(400)
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( DATA_ID )
 USING HASHING;



SELECT DATA_ID , BILL_ID , CUSTOMER_ORDER_ID , BUSIOPER_ORDER_ID , BUSINESS_ID , OFFER_ID , STATE , REGION_ID , DONE_DATE , CREATE_DATE , CHANEL_ID , START_DATE , OP_ID , ORG_ID , EXT1 , EXT2 , EXT3 , EXT4 , EXT5 , EXT6 , EXT7 , NOTES FROM SO.ORD_DSMP_BAT_DATA_HIS where done_date>=to_date('$YYYYMMDD$000000','YYYYMMDDHH24MISS') AND done_date<=to_date('$YYYYMMDD$235959','YYYYMMDDHH24MISS') 


SELECT DATA_ID , BILL_ID , CUSTOMER_ORDER_ID , BUSIOPER_ORDER_ID , BUSINESS_ID , OFFER_ID , STATE , REGION_ID , to_char(DONE_DATE,'yyyymmddhh24miss')DONE_DATE , to_char(CREATE_DATE,'yyyymmddhh24miss') CREATE_DATE , CHANEL_ID , to_char(START_DATE,'yyyymmddhh24miss') START_DATE , OP_ID , ORG_ID , EXT1 , EXT2 , EXT3 , EXT4 , EXT5 , EXT6 , EXT7 , NOTES FROM SO.ORD_DSMP_BAT_DATA_HIS
;


select BATCH_DATA_DETAIL_ID	,BATCH_DATA_ID	,QUEUE_ID	,BUSINESS_ID	,CONFIG_ID	,BUSI_ORDER_ID	,BILL_ID	,FILE_CONTENT	,SUCC_FLAG	,ERROR_MSG	,ERROR_STACK	,DONE_CODE	, to_char(DONE_DATE,'YYYYMMDDHH24MISS') DONE_DATE	,  to_char(CREATE_DATE,'YYYYMMDDHH24MISS') CREATE_DATE	,  to_char(VALID_DATE,'YYYYMMDDHH24MISS')  VALID_DATE	,  to_char(EXPIRE_DATE ,'YYYYMMDDHH24MISS') EXPIRE_DATE	,OP_ID	,ORG_ID	,REGION_ID from so.ord_batch_data_detail_f_1008 where done_date>=to_date('$YYYYMMDD$000000','YYYYMMDDHH24MISS') AND done_date<=to_date('$YYYYMMDD$235959','YYYYMMDDHH24MISS') 

to_char(CREATE_DATE,'yyyymmddhh24miss')



��create table zjt_tables as 

����(select * from tables) definition only; 


set db_user BASS2
set tbs_3h tbs_3h 

for {yyyymmdd = 20101001} {yyyymmdd < 20101031 } {} 
   set sqlbuf "create table ${db_user}.dw_product_regsp_ds like ${db_user}.dw_product_regsp_${yyyymmdd} in $tbs_3h
               index in tbs_index partitioning key (user_id) using hashing not logged initially;"

create table ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101011 like ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_yyyymmdd in tbs_3h index in TBS_INDEX   PARTITIONING KEY ( BATCH_DATA_DETAIL_ID ) USING HASHING;

-- 2010-10-18 16:41 PANZHIWEI ADD
CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_YYYYMMDD ( 
		 BATCH_DATA_DETAIL_ID		bigint		
		,BATCH_DATA_ID				bigint				
		,QUEUE_ID					varchar(20)				--	����ID
		,BUSINESS_ID				bigint					--	����ID
		,CONFIG_ID					bigint					--	����ID
		,BUSI_ORDER_ID				bigint					--	��������ID
		,BILL_ID					varchar(30)				--	�ֻ�����
		,FILE_CONTENT				VARCHAR(1024)			--	�ļ�����
		,SUCC_FLAG					smallint				--	�ɹ���־
		,ERROR_MSG					VARCHAR(1000)			--	������Ϣ
		,ERROR_STACK				VARCHAR(4000)			--	����ջ��Ϣ
		,DONE_CODE					bigint					--	��������
		,DONE_DATE					TIMESTAMP				--	������־
		,CREATE_DATE				TIMESTAMP				--	����ʱ��
		,VALID_DATE					TIMESTAMP				--	��Чʱ��
		,EXPIRE_DATE				TIMESTAMP				--	ʧЧʱ��
		,OP_ID						bigint					--	����ԱID
		,ORG_ID						bigint					--	����Ա��֯ID
		,REGION_ID					VARCHAR(6)				--	��������
 )
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( BATCH_DATA_DETAIL_ID )
 USING HASHING;
 




Usage: ./crt.sh TABLE_NAME_TEMPLET TBLSPACE_NAME PARTITIONING_KEK YYYYMM
/bassapp/bass2/fuzl
crt.sh ODS_CHANNEL_LOCAL_BUSI TBS_3H OBJ_ID 201012
crt.sh ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F TBS_3H BATCH_DATA_DETAIL_ID 201007
crt.sh ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F TBS_3H BATCH_DATA_DETAIL_ID 201008 &


crt.sh ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS TBS_3H DATA_ID 201007 &
crt.sh ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS TBS_3H DATA_ID 201008 &
crt.sh ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS TBS_3H DATA_ID 201010 &




ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_YYYYMMDD




java ETLMain 20101103 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101104 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101105 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101106 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101107 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101108 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101109 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101110 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101111 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101112 taskList_tmp.properties
java ETLMain 20101113 taskList_tmp.properties
java ETLMain 20101114 taskList_tmp.properties
java ETLMain 20101115 taskList_tmp.properties
java ETLMain 20101116 taskList_tmp.properties
java ETLMain 20101117 taskList_tmp.properties
java ETLMain 20101118 taskList_tmp.properties
java ETLMain 20101119 taskList_tmp.properties
java ETLMain 20101120 taskList_tmp.properties
java ETLMain 20101121 taskList_tmp.properties
java ETLMain 20101122 taskList_tmp.properties
java ETLMain 20101123 taskList_tmp.properties
java ETLMain 20101124 taskList_tmp.properties
java ETLMain 20101125 taskList_tmp.properties
java ETLMain 20101126 taskList_tmp.properties
java ETLMain 20101127 taskList_tmp.properties
java ETLMain 20101128 taskList_tmp.properties
java ETLMain 20101129 taskList_tmp.properties
java ETLMain 20101130 taskList_tmp.properties
java ETLMain 20101131 taskList_tmp.properties
java ETLMain 20100801 taskList_tmp.properties
java ETLMain 20100802 taskList_tmp.properties
java ETLMain 20100803 taskList_tmp.properties
java ETLMain 20100804 taskList_tmp.properties
java ETLMain 20100805 taskList_tmp.properties
java ETLMain 20100806 taskList_tmp.properties
java ETLMain 20100807 taskList_tmp.properties
java ETLMain 20100808 taskList_tmp.properties
java ETLMain 20100809 taskList_tmp.properties
java ETLMain 20100810 taskList_tmp.properties
java ETLMain 20100811 taskList_tmp.properties
java ETLMain 20100812 taskList_tmp.properties
java ETLMain 20100813 taskList_tmp.properties
java ETLMain 20100814 taskList_tmp.properties
java ETLMain 20100815 taskList_tmp.properties
java ETLMain 20100816 taskList_tmp.properties
java ETLMain 20100817 taskList_tmp.properties
java ETLMain 20100818 taskList_tmp.properties
java ETLMain 20100819 taskList_tmp.properties
java ETLMain 20100820 taskList_tmp.properties
java ETLMain 20100821 taskList_tmp.properties
java ETLMain 20100822 taskList_tmp.properties
java ETLMain 20100823 taskList_tmp.properties
java ETLMain 20100824 taskList_tmp.properties
java ETLMain 20100825 taskList_tmp.properties
java ETLMain 20100826 taskList_tmp.properties
java ETLMain 20100827 taskList_tmp.properties
java ETLMain 20100828 taskList_tmp.properties
java ETLMain 20100829 taskList_tmp.properties
java ETLMain 20100830 taskList_tmp.properties
java ETLMain 20100831 taskList_tmp.properties
java ETLMain 20101001 taskList_tmp.properties
java ETLMain 20101002 taskList_tmp.properties
java ETLMain 20101003 taskList_tmp.properties
java ETLMain 20101004 taskList_tmp.properties
java ETLMain 20101005 taskList_tmp.properties
java ETLMain 20101006 taskList_tmp.properties
java ETLMain 20101007 taskList_tmp.properties
java ETLMain 20101008 taskList_tmp.properties
java ETLMain 20101009 taskList_tmp.properties
java ETLMain 20101010 taskList_tmp.properties
java ETLMain 20101011 taskList_tmp.properties
java ETLMain 20101012 taskList_tmp.properties
java ETLMain 20101013 taskList_tmp.properties
java ETLMain 20101014 taskList_tmp.properties
java ETLMain 20101015 taskList_tmp.properties
java ETLMain 20101016 taskList_tmp.properties
java ETLMain 20101017 taskList_tmp.properties
java ETLMain 20101018 taskList_tmp.properties





java ETLMain 	20100701	taskList_tmp.properties
java ETLMain 	20100702	taskList_tmp.properties
java ETLMain 	20100707	taskList_tmp.properties
java ETLMain 	20100710	taskList_tmp.properties
java ETLMain 	20100717	taskList_tmp.properties
java ETLMain 	20100719	taskList_tmp.properties
java ETLMain 	20100720	taskList_tmp.properties
java ETLMain 	20100721	taskList_tmp.properties
java ETLMain 	20100722	taskList_tmp.properties
java ETLMain 	20100728	taskList_tmp.properties
java ETLMain 	20100729	taskList_tmp.properties
java ETLMain 	20100730	taskList_tmp.properties
java ETLMain 	20100731	taskList_tmp.properties
java ETLMain 	20100802	taskList_tmp.properties
java ETLMain 	20100806	taskList_tmp.properties
java ETLMain 	20100817	taskList_tmp.properties
java ETLMain 	20100818	taskList_tmp.properties
java ETLMain 	20100819	taskList_tmp.properties
java ETLMain 	20100820	taskList_tmp.properties
java ETLMain 	20100824	taskList_tmp.properties
java ETLMain 	20100825	taskList_tmp.properties
java ETLMain 	20100826	taskList_tmp.properties
java ETLMain 	20100827	taskList_tmp.properties
java ETLMain 	20100828	taskList_tmp.properties
java ETLMain 	20100829	taskList_tmp.properties
java ETLMain 	20100830	taskList_tmp.properties
java ETLMain 	20100831	taskList_tmp.properties
java ETLMain 	20100901	taskList_tmp.properties
java ETLMain 	20100903	taskList_tmp.properties
java ETLMain 	20100910	taskList_tmp.properties
java ETLMain 	20100914	taskList_tmp.properties
java ETLMain 	20100916	taskList_tmp.properties
java ETLMain 	20100917	taskList_tmp.properties
java ETLMain 	20100925	taskList_tmp.properties
java ETLMain 	20100926	taskList_tmp.properties
java ETLMain 	20100927	taskList_tmp.properties
java ETLMain 	20100928	taskList_tmp.properties
java ETLMain 	20101002	taskList_tmp.properties
java ETLMain 	20101011	taskList_tmp.properties
java ETLMain 	20101012	taskList_tmp.properties




backup/M1103620101000000000.CHK
cat backup/M1103620101000000000.CHK
M1103620101000000000.AVL.Z$44251886$834999$20101000$20101102010239$ 
�ļ���$�ļ���С$��¼��$��������$����ʱ��


1.db2 ©���ؼ�¼



replace(replace(CONT_ADDR,chr(10)),chr(13))

replace(replace(replace(cust_name,chr(10)),chr(13)),'$','')




/**	2010/11/2 18:02	added by  panzhiwei		**/	
CREATE TABLE DWD_NG2_I03013_YYYYMMDD (					
        CHANNEL_ID          BIGINT              --������ʶ:
        ,CHANNEL_NAME        VARCHAR(100)        --��������:
        ,PARENT_CHANNEL_ID   INTEGER             --�ϼ�������ʶ:
        ,CHANNEL_LEVEL       INTEGER             --��������:�롰�������𡱽ӿڵ�Ԫ��Ӧ
        ,CHANNEL_DESC          VARCHAR(200)        --��������:
        ,START_DATE          TIMESTAMP           --��������:
        ,EXPIRE_DATE         TIMESTAMP           --��ֹ����:ȱʡ29991231
        ,REGION_CODE         VARCHAR(8)          --����������:�롰����08039���ӿڵ�Ԫ��Ӧ
        ,ORGANIZE_ID         BIGINT              --������λ��ʶ:�롰��λ���ӿڵ�Ԫ��Ӧ
        ,CHANNEL_TYPE        INTEGER             --�������:�롰�������͡��ӿڵ�Ԫ��Ӧ
        ,PROPERTY_SRC_TYPE   INTEGER             --������ҵ��̬:�������й�˾������������ҵ���������ޡ�ת��-���е�
        ,REG_FUND            BIGINT              --Ͷ�ʹ�ģ:
        ,OPEN_DATE           TIMESTAMP           --��ҵʱ��:
        ,RESPNSR_ID             INTEGER             --���������˱��:�롰���������ˡ��ӿڵ�Ԫ��Ӧ
        ,TEL_NUMBER          VARCHAR(20)         --�����칫�绰:
        ,FAX_NUMBER          VARCHAR(20)         --�������:
        ,POST_CODE           VARCHAR(20)            --�ʱ�:
        ,BUSI_BEGIN_TIME     VARCHAR(20)         --Ӫҵʱ��:
        ,CHANNEL_STYLE       INTEGER             --������Ӫ��ʽ:"0-�Խ���Ӫ���԰죩��1-�Խ���Ӫ����������2-������Ӫ������"
        ,CHANNEL_LEVEL       INTEGER             --�����Ǽ�:"0-һ�Ǽ���1-���Ǽ�"
        ,CHANNEL_USER_ID     INTEGER             --���������˱��:�롰���������ˡ��ӿڵ�Ԫ��Ӧ
        ,OWNER_TYPE          SMALLINT            --��ҵ���÷�ʽ:
        ,PWNER_PRICE         INTEGER             --��ҵ���ü۸�:
        ,FITMENT_PRICE       INTEGER             --װ��Ͷ��:
        ,TRANSFER_PRICE      INTEGER             --����Ͷ��:
        ,INVESTOR            VARCHAR(20)         --Ͷ������:
        ,LICENSE_NUMBER      VARCHAR(20)          --���̺�:
        ,INTERNET_MODE       INTEGER             --���뷽ʽ:"0-���£�1-2M���£�2-GPRS��3-CDS��4-����������5-�������ţ�6-��"
        ,CREATE_DATE         TIMESTAMP           --����ʱ��:
        ,CR_OP_ID            BIGINT              --����Ա��:
        ,OP_ID               BIGINT              --����Ա��:
        ,DONE_DATE           TIMESTAMP           --����ʱ��:
        ,CHANNEL_STATE       SMALLINT            --����״̬:"0-������1-Ԥ��Լ��2-ע��"
        ,NOTES               VARCHAR(200)            --��ע
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING;


 ��̸���������Ż��н���Ϣ�������ײ���İ���绰��

 ������  18:09:11
									#����У���ļ�
						 	 		n="${task_id}${scycleid}000000.AVL"
									s=`ls -l tmp_${task_id}${scycleid}000000.AVL |awk -F" "  '{print $5}'`
									r=`wc -l tmp_${task_id}${scycleid}000000.AVL |awk -F" "  '{print $1}'`
									d="${scycleid}"
									t=`date '+%Y%m%d%H%M%S'`
									printf "%-40s%-20s%-20s%8s%14s\n"  $n $s $r $d $t > tmp_${task_id}${scycleid}000000.CHK

 
TASK_ID=I03013
MODULE=GG
RUNTYPE=R
DATECYCLE=D
DATEPARAM=$YYYYMMDD$
AREAPARAM=891,892,893,894,895,896,897
RUNSQL=select distinct 
a.CHANNEL_ID , \
a.CHANNEL_NAME , \
b.PARENT_CHANNEL_ID , \
a.CHANNEL_LEVEL , \
a.OTHER_INFO , \
TO_CHAR(a.OPEN_DATE, \'YYYYMMDDHH24MISS') START_DATE, \
TO_CHAR(a.EXPIRE_DATE, \'YYYYMMDDHH24MISS') , \
a.REGION_CODE , \
a.ORGANIZE_ID , \
a.CHANNEL_TYPE , \
a.PROPERTY_SRC_TYPE , \
c.REG_FUND , \
TO_CHAR(a.OPEN_DATE, \'YYYYMMDDHH24MISS') , \
d.USER_ID , \
a.TEL_NUMBER , \
a.FAX_NUMBER , \
a.POST_CODE , \
a.BUSI_BEGIN_TIME , \
a.CHANNEL_STYLE , \
a.CHANNEL_LEVEL , \
null CHANNEL_USER_ID , \
null OWNER_TYPE , \
null PWNER_PRICE , \
null FITMENT_PRICE , \
null  TRANSFER_PRICE , \
' ' INVESTOR , \
c.LICENSE_NUMBER , \
a.INTERNET_MODE , \
TO_CHAR(a.CREATE_DATE, \'YYYYMMDDHH24MISS') , \
a.OP_ID CR_OP_ID , \
a.OP_ID , \
TO_CHAR(a.DONE_DATE, \'YYYYMMDDHH24MISS') , \
a.CHANNEL_STATE , \
a.NOTES \
FROM channel.channel_info a \
left join channel.gj_channel_dept b on a.channel_id = b.channel_id  \
left join channel.agent_channel c   on a.channel_id = c.channel_id \
left join channel.CHANNEL_EQUIP_INFO d on a.channel_id = d.channel_d  


="FROM "&�ӿ��б�2!B213&"."&�ӿ��б�2!C213&SUBSTITUTE(SUBSTITUTE(UPPER(#REF!D3),"X","0$FEE_AREA$",1),"YYMM","$YYMM$",1)


©�� \ �ĵط��ᱨ "java.sql.SQLException: ORA-00936: missing expression" ����



/**	2010-11-5 15:21	added by  panzhiwei		**/		
CREATE TABLE DWD_NG2_M03023_YYYYMM (						
        CHANNEL_ID          BIGINT              --������ʶ
        ,STORE_AREA         INTEGER             --Ӫҵ�����
        ,GEOGRAPHY_PROPERTY INTEGER             --����λ������
        ,LONGITUDE          BIGINT                  --����
        ,LATITUDE           BIGINT                  --γ��
        ,COVER_BUSI_REGION_CODE VARCHAR(20)         --��������ҵ����
        ,COVER_radius       INTEGER             --����뾶
        ,POEPLE_NUM         BIGINT              --�˿ڸ���
        ,POPUL_DENSITY      INTEGER             --�˿��ܶ�
        ,POEPLE_NUM         BIGINT              --�ͻ�����
        ,EMPLOYEE_NUM       INTEGER             --��Ա��Ŀ
        ,SALES_NUM          INTEGER             --ӪҵԱ����
        ,EQUIP_NUM          INTEGER             --�ն���Ŀ
        ,EQUIP_STATUS       INTEGER             --�ն�״��
        ,CHANNEL_ADDRESS    VARCHAR(120)        --��ϸ��ַ
        ,plane_graph        INTEGER             --Ӫҵ��ƽ��ṹͼ
        ,BOARD_HEIGHT       INTEGER             --���и߶ȣ��ף�
        ,BOARD_WIDTH        INTEGER             --���п�ȣ��ף�
        ,WALL_HEIGHT        INTEGER             --����ǽ�߶�
        ,WALL_WIDTH         INTEGER             --����ǽ���
        ,CREATE_DATE        TIMESTAMP           --��������
        ,CREATE_OP_ID       BIGINT              --����Ա��
        ,DONE_DATE           TIMESTAMP              --��������
        ,OP_ID             BIGINT              --����Ա��
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING;





select distinct   \
A.CHANNEL_ID ,  \
nvl(STORE_AREA,1644 + round(dbms_random.value(-200,200))) STORE_AREA ,  \
nvl(GEOGRAPHY_PROPERTY,round(dbms_random.value(0,8))+1) GEOGRAPHY_PROPERTY ,  \
 91+round(dbms_random.value(0,1),2) LONGITUDE ,  \
 29+round(dbms_random.value(0,1),2) LATITUDE ,  \
nvl(c.region_detail,100102) COVER_BUSI_REGION_CODE ,  \
round(dbms_random.value(0,10)) COVER_radius ,  \
round(dbms_random.value(0,10)) POEPLE_NUM ,  \
round(dbms_random.value(0,10)) POPUL_DENSITY ,  \
round(dbms_random.value(0,10)) CUST_NUM ,  \
round(dbms_random.value(1,15)) EMPLOYEE_NUM ,  \
round(dbms_random.value(1,15)) SALES_NUM ,  \
round(dbms_random.value(1,10)) EQUIP_NUM ,  \
round(dbms_random.value(1,4)) EQUIP_STATUS ,  \
A.CHANNEL_ADDRESS ,  \
round(dbms_random.value(0,1)) plane_graph ,  \
NVL(B.BOARD_HEIGHT,round(dbms_random.value(50,800))) BOARD_HEIGHT,  \
NVL(B.BOARD_WIDTH,round(dbms_random.value(300,1000))) BOARD_WIDTH ,  \
NVL(B.WALL_HEIGHT,round(dbms_random.value(60,500))) WALL_HEIGHT ,  \
NVL(B.WALL_HEIGHT,round(dbms_random.value(50,500))) WALL_WIDTH ,  \
NVL(TO_CHAR(B.DONE_DATE,'YYYYMMDDHH24MISS'),'19001231000000') CREATE_DATE ,  \
NVL(B.OP_ID,10000000)   CREATE_OP_ID ,  \
NVL(TO_CHAR(B.DONE_DATE,'YYYYMMDDHH24MISS'),'19001231000000') DONE_DATE ,  \
NVL(B.OP_ID,10000000) OP_ID \
from channel.channel_info a  \ 
left join channel.self_channel b on a.channel_id = b.channel_id \
left join (select distinct  channel_id ,region_detail from CHANNEL.GJ_AGENT_REGION ) c on a.channel_id = c.channel_id \
where a.channel_id is not null 




 ���Ż�ΰ(80589803)  18:01:55
���Ұ���Ҫ��������������Ͻ��ܱ����������������ӳٵ���һ�ύ
���������Ҫ���ܴ�ҵĹ����ύ��˾�ܱ����������������ܼ����� 


����ƽ(2651779)  18:03:58
15847676066���ڵĺ�


BU
����PAϵͳ

A11019	SO	ORD_CUST_F	X_YYMM	�û�	�ͻ�����	PRODUCT	��	ODS_PRODUCT_ORD_CUST_YYYYMMDD	172.16.5.43	0020	2010-4-28����������	


 ������  14:03:52
so.ord_cust_f_0891_1010
������  14:08:57
NG1-BASS1.0\Truck\03 ������\interface\��ȡ\doc


1.dim_pub_city ����ά��


nfjd1234


��д�ĵ���ģ�壺F:\XZBI\NG1-BASS1.0\Truck\04 ģ���\

�����־
          
          
1. �˽���ʷ�ӿڡ�




E:\SVN\NG1-BASS2.0\�ĵ�\���ž��ֹ淶\NG1-BASS2.0�淶(������)\�����淶\�ⲿ�ӿڷֲ�



	�й��ƶ�ʡ��NG1-BASS�����淶�ⲿ�ӿڷֲ�v2.0(������).doc
	
dblink 
	
	
	����:SYS_STAFF ����:Ա�� 
 
'

				
/**	2010-11-8 10:51	added by  panzhiwei		**/
--DROP TABLED WD_NG2_I03027_YYYYMMDD;
CREATE TABLE DWD_NG2_I03027_YYYYMMDD (				
        OPER_ID             BIGINT              		--Ա����ʶ
        ,ASSESS_CODE        VARCHAR(20)        		  --��Ա����ҵ�����ͱ�ʶ
        ,ASSESS_SCORE       INTEGER                 --����
        ,DONE_DATE          TIMESTAMP               --ʱ��
        ,OPASSESS_DTL_ID	  BIGINT									--������ϸ���
 )		
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( OPER_ID )				
 USING HASHING;				


db2 "load client from /bassapp/bass2/load/boss/I0302720101020000000.AVL of del \
 modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 \
 messages /bassapp/bass2/load/boss/messages/db2load.msg  replace into DWD_NG2_I03027_20101020
 "
 


grep -in I0302720101020000000.AVL.Z nohup.out 
 
startline=`grep -in I0302720101020000000.AVL.Z nohup.out | tail -3|head -1|awk -F: '{print $1}'`

endline=`grep -in I0302720101020000000.AVL.Z nohup.out | tail -1|awk -F: '{print $1}'`

 
awk -v STARTLN=$startline '{
           if(NR>= STARTLN && NR <= ENDLN)
               { print "========"NR,$0;}
       }' \
nohup.out




awk '{
if(NR>=455682&& NR <= 455750)
{ print "========"NR,$0;}
}' \
/bassapp/bass2/load/boss/nohup.out



awk -v STARTLN=455730  '{
           if(NR>= STARTLN && NR <= 455750)
               { print "========"NR,$0;}
       }' /bassapp/bass2/load/boss/nohup.out 
       
       


BASE=/bassapp/bass2/load/boss
grep -in I0302720101020000000.AVL.Z $BASE/nohup.out
startline=`grep -in I0302720101020000000.AVL.Z $BASE/nohup.out | tail -3|head -1|awk -F: '{print $1}'`
endline=`grep -in I0302720101020000000.AVL.Z $BASE/nohup.out | tail -1|awk -F: '{print $1}'`
sed -n "${startline},${endline}p" $BASE/nohup.out




BASE=/bassapp/bass2/load/boss
grep -in I0302720101020000000.AVL.Z $BASE/nohup.out
startline=`grep -in I0302720101020000000.AVL.Z $BASE/nohup.out | tail -3|head -1|awk -F: '{print $1}'`
endline=`grep -in I0302720101020000000.AVL.Z $BASE/nohup.out | tail -1|awk -F: '{print $1}'`

awk  -V STARTLN=${startline} -V ENDLN=${endline} '{ if(NR>= STARTLN && NR <= ENDLN) { print "========"NR,$0;} }'   $BASE/nohup.out


awk -V a=1  '{print a}' 



nawk 	 -v STARTLN=${startline}  \
			 -v ENDLN=${endline}  \
			  '{ if( NR>= STARTLN && NR <= ENDLN )
               { print "========"NR,$0;}
       }'  \
$BASE/nohup.out



sed -n "${startline},${endline}p" $BASE/nohup.out


BU
����PAϵͳ

A11019	SO	ORD_CUST_F	X_YYMM	�û�	�ͻ�����	PRODUCT	��	ODS_PRODUCT_ORD_CUST_YYYYMMDD	172.16.5.43	0020	2010-4-28����������	


 ������  14:03:52
so.ord_cust_f_0891_1010
������  14:08:57
NG1-BASS1.0\Truck\03 ������\interface\��ȡ\doc


1.dim_pub_city ����ά��


nfjd1234


��д�ĵ���ģ�壺F:\XZBI\NG1-BASS1.0\Truck\04 ģ���\

�����־
          
          
1. �˽���ʷ�ӿڡ�




E:\SVN\NG1-BASS2.0\�ĵ�\���ž��ֹ淶\NG1-BASS2.0�淶(������)\�����淶\�ⲿ�ӿڷֲ�



	�й��ƶ�ʡ��NG1-BASS�����淶�ⲿ�ӿڷֲ�v2.0(������).doc
	
dblink 
	
	
	����:SYS_STAFF ����:Ա�� 
 

SCHEME_CHANNEL_ID	NUMBER(12)	--	�������
LONGITUDE	NUMBER(12)	--	����
LATITUDE	NUMBER(12)	--	γ��
PROPERTY_SRC_TYPE	NUMBER(2)	--	��ҵ��Դ��
SEAT_NUM	NUMBER(6)	--	�����ϯ��Ŀ
USED_SEAT_NUM	NUMBER(6)	--	������ϯ��Ŀ
CHANNEL_SITE	NUMBER(8)	--	���賡��
OWNER_TYPE	NUMBER(2)	--	��ҵ���÷�ʽ
PWNER_PRICE	NUMBER(10)	--	��ҵ���ü۸�
SITE_TYPE	NUMBER(2)	--	�÷�����
HOUSE_EXPIRE	DATE	--	�ⷿ����
HIRE_COST	NUMBER(12)	--	�����Թ����ɱ���̯
FITMENT_PRICE	NUMBER(10)	--	װ��Ͷ��
TRANSFER_PRICE	NUMBER(10)	--	����Ͷ��
OTHER_PRICE	NUMBER(10)	--	����Ͷ��
MANUAL_COST	NUMBER(12)	--	�˹��ɱ�
OTHER_COST	NUMBER(12)	--	�����ɱ�
EQUIP_DEPRE	NUMBER(12)	--	�豸�۾ɷ���
FURNITURE_COST	NUMBER(12)	--	�Ҿ��۾ɷ���
FITMENT_COST	NUMBER(12)	--	װ���۾ɷ���
DOOR_TYPE	NUMBER(2)	--	��ͷ��ʽ
STORE_AREA	NUMBER(8)	--	�������
BUILD_AREA	NUMBER(10)	--	�������
DOOR_AREA	NUMBER(10)	--	�Ŷ����
USE_AREA	NUMBER(10)	--	ʹ�����
WINDOW_AREA	NUMBER(8)	--	�ٽֳ������
BUSI_AREA	NUMBER(10)	--	ҵ�����������
NEWBUSI_USE_AREA	NUMBER(10)	--	��ҵ�����������
TERMINAL_SALE_AREA	NUMBER(10)	--	�ն����������
AD_AREA	NUMBER(10)	--	�����������
OTHER_AREA	NUMBER(10)	--	�����������
FITMENT_EXPIRE	NUMBER(4)	--	װ��Ͷ���۾�����
FURNITURE_EXPIRE	NUMBER(4)	--	�칫�Ҿ��۾�����
FITMENT_VALID	DATE	--	װ��Ͷ����Чʱ��
FURNITURE_VALID	DATE	--	�칫�Ҿ���Чʱ��
WARM_FEE	NUMBER(10)	--	ȡů��
DOOR_COST	NUMBER(10)	--	��ͷ�����������
WINDOW_COST	NUMBER(10)	--	�ٽֳ��������������
WATER_COST	NUMBER(12)	--	ˮ��
ELEC_COST	NUMBER(12)	--	���
OFFICE_COST	NUMBER(12)	--	�칫��Ʒ���Ĳ�
COMM_COST	NUMBER(12)	--	��ͨѶ��
UNIT_PRICE1	NUMBER(12)	--	ͬ�ضλ����г���λ�۸�1
REBATE1	NUMBER(12)	--	�ۿ�ϵ��1
UNIT_PRICE2	NUMBER(12)	--	ͬ�ضλ��⿴�嵥λ�۸�2
REBATE2	NUMBER(12)	--	�ۿ�ϵ��2
UNIT_PRICE3	NUMBER(12)	--	ͬ�ضλ�����������λ�۸�3
REBATE3	NUMBER(12)	--	�ۿ�ϵ��3
DONE_CODE	NUMBER(12)	--	������ˮ
DONE_DATE	DATE	--	��������
OP_ID	NUMBER(12)	--	����Ա
ORG_ID	NUMBER(12)	--	������֯��Ԫ
NOTES	VARCHAR2(255)	--	��ע
STATE	NUMBER(1)	--	��¼״̬
BUY_EQUIP_SUM	NUMBER(10)	--	BUY_EQUIP_SUM
NOTA_AREA	NUMBER(8)	--	��ʾ��λ�ף��������ݿ�������
OFFICE_FIT_SUM	NUMBER(10)	--	OFFICE_FIT_SUM



					
/**	2010-11-9 11:27	added by  panzhiwei		**/	
--DROP TABLE DWD_NG2_I03028_YYYYMMDD;					
CREATE TABLE DWD_NG2_I03028_YYYYMMDD (					
        CHANNEL_ID          BIGINT              --��������
        ,USED_SEAT_NUM      INTEGER             --̨ϯ����
        ,EMPLOYEE_NUM       BIGINT              --Ա������
        ,BUILD_AREA         INTEGER             --�������
        ,USE_AREA           INTEGER             --ʹ�����
        ,AD_AREA            INTEGER             --�����������
        ,EQUIP_NUM          BIGINT                --�豸��
        ,LEGAL_PERSON       VARCHAR(20)           --��ϵ��
        ,TEL_NUMBER         VARCHAR(20)         --��ϵ�绰
        ,BUSI_TYPE          VARCHAR(20)         --����ҵ������
        ,USAGE_TYPE         VARCHAR(20)         --���ܽ綨
        ,HOUSE_EXPIRE       TIMESTAMP           --�ⷿ����
        ,HOUSE_BUY_PRICE    BIGINT              --���ݹ�����
        ,HIRE_COST          BIGINT              --�������
        ,BUY_EQUIP_SUM      INTEGER             --�豸������
        ,EQUIP_RENT_PRICE   BIGINT              --�豸���
        ,FITMENT_PRICE      INTEGER             --װ��Ͷ���ܶ�
        ,OFFICE_COST        BIGINT              --�칫�Ҿ�Ͷ���ܶ�
        ,FITMENT_EXPIRE     SMALLINT            --װ��Ͷ���۾�����
        ,FURNITURE_EXPIRE   SMALLINT            --�칫�Ҿ��۾�����
        ,FITMENT_VALID      TIMESTAMP           --װ��Ͷ����Чʱ��
        ,FURNITURE_VALID    TIMESTAMP           --�칫�Ҿ���Чʱ��
        ,WATER_COST         BIGINT                  --ˮ��
        ,WARM_FEE           INTEGER               --ȡů��
        ,ELEC_COST          BIGINT                  --���
        ,MANUAL_COST        BIGINT              --�˹��ɱ��ܶ�
        ,OTHER_COST         BIGINT              --�����ճ������ܶ�
        ,BUSI_CONSULT_RATE  BIGINT              --��ѯҵ����ռ��ҵ������ֵ
        ,BUSI_COMPLAINT_RATE BIGINT              --Ͷ��ҵ����ռ��ҵ������ֵ
        ,DOOR_HEIGHT        BIGINT              --��ͷ�߶�
        ,DOOR_WIDTH         BIGINT              --��ͷ����
        ,WINDOW_AREA        INTEGER             --�ٽֳ������
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING;



select   
a.CHANNEL_ID ,  
round(dbms_random.value(1,10))  USED_SEAT_NUM ,  
round(dbms_random.value(5,15)) EMPLOYEE_NUM ,  
nvl(STORE_AREA,1644 + round(dbms_random.value(-200,200))) BUILD_AREA ,  
nvl(STORE_AREA,1644 + round(dbms_random.value(-200,200))) USE_AREA ,  
nvl(STORE_AREA,1644 + round(dbms_random.value(-200,200))) AD_AREA ,  
round(dbms_random.value(1,10)) EQUIP_NUM ,  
nvl(c.LEGAL_PERSON,a.CHANNEL_NAME) LEGAL_PERSON ,  
a.TEL_NUMBER ,  
round(dbms_random.value(1,8)) BUSI_TYPE ,  
round(dbms_random.value(1,6)) USAGE_TYPE ,  
nvl(TO_CHAR(a.EXPIRE_DATE,'YYYYMMDDHH24MISS'),'20991231235959') HOUSE_EXPIRE ,  
round(dbms_random.value(100000,1000000)) HOUSE_BUY_PRICE ,  
round(dbms_random.value(2000,10000)) HIRE_COST ,  
round(dbms_random.value(10000,100000)) BUY_EQUIP_PRICE ,  
round(dbms_random.value(200,1000)) EQUIP_RENT_PRICE ,  
round(dbms_random.value(1000,10000)) FITMENT_PRICE ,  
round(dbms_random.value(1000,10000)) OFFICE_COST ,  
nvl(TO_CHAR(a.EXPIRE_DATE,'YYYYMMDDHH24MISS'),'20991231235959') FITMENT_EXPIRE ,  
nvl(TO_CHAR(a.EXPIRE_DATE,'YYYYMMDDHH24MISS'),'20991231235959') FURNITURE_EXPIRE ,  
nvl(TO_CHAR(a.create_DATE+round(dbms_random.value(-360,0)),'YYYYMMDDHH24MISS'),'20991231235959') FITMENT_VALID ,  
nvl(TO_CHAR(a.create_DATE+round(dbms_random.value(-360,0)),'YYYYMMDDHH24MISS'),'20991231235959') FURNITURE_VALID ,  
round(dbms_random.value(100,500)) WATER_COST ,  
round(dbms_random.value(100,500)) WARM_FEE ,  
round(dbms_random.value(100,500)) ELEC_COST ,  
round(dbms_random.value(10000,50000)) MANUAL_COST ,  
round(dbms_random.value(10000,20000)) OTHER_COST ,  
round(dbms_random.value(30,50))/100 BUSI_CONSULT_RATE ,  
round(dbms_random.value(1,15))/100 BUSI_COMPLAINT_RATE ,  
round(dbms_random.value(200,300)) DOOR_HEIGHT ,  
round(dbms_random.value(100,200)) DOOR_WIDTH ,  
round(dbms_random.value(3,9)) WINDOW_AREA  
FROM channel.channel_info a   
left join channel.gj_channel_dept b on a.channel_id = b.channel_id    
left join channel.agent_channel c   on a.channel_id = c.channel_id   
left join channel.CHANNEL_EQUIP_INFO d on a.channel_id = d.CHANNEL_ID  
where TO_CHAR(a.OPEN_DATE,'YYYYMMDDHH24MISS') < nvl(TO_CHAR(a.EXPIRE_DATE,'YYYYMMDDHH24MISS'),'20991231000000')




db2 "load client from /bassapp/bass2/load/boss/I0302820101020000000.AVL \
of del modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder \
warningcount 1000 messages /bassapp/bass2/load/boss/messages/db2load.msg  \
replace into DWD_NG2_I03028_20101020"




3.2.2.	����ʵ�ַ�ʽ
  ��ǰ�����⼸�ű����� ��dw_newbusi_gprs_201010 �������ȡ�����ݣ�
 ��dw_newbusi_gprs_201010��������Ǵ�BOSS��xzjf.dr_ggprs_l_20101104 ��ȡ�� ������cell_id �� lac_idΪ�ա�

���������⼸�ű�������Ŀ����ģ�����Ҫȡ����ȷ�� cell_id �� lac_id
����Ҫ����һ���ӿ���ר��ʵ�֡���BOSS��xzjf.DR_GGPRS_GXZ_20101104 �������ȡ��
��ע�⣬���ֻ���������Ŀ����ļ��ű��������������޸ģ�






/**	2010-11-9 17:52	added by  panzhiwei	**/
--DROP TABLE ODS_CDR_GPRS_GXZ_YYYYMMDD;		
CREATE TABLE ODS_CDR_GPRS_GXZ_YYYYMMDD (		
         DR_TYPE            INTEGER                     --
        ,SERVICE_ID         INTEGER                     --
        ,BILL_MONTH         VARCHAR(8)                  --
        ,USER_NUMBER        VARCHAR(15)                 --
        ,VPLMN1             VARCHAR(7)                  --
        ,START_TIME         TIMESTAMP                   --
        ,DURATION           INTEGER                     --
        ,LAC_ID             VARCHAR(8)                  --
        ,CELL_ID            VARCHAR(16)                 --
        ,APN_NI             VARCHAR(64)                 --
        ,DATA_FLOW_UP1      INTEGER                     --
        ,DATA_FLOW_DOWN1    INTEGER                     --
        ,DURATION1          INTEGER                     --
        ,DATA_FLOW_UP2      INTEGER                     --
        ,DATA_FLOW_DOWN2    INTEGER                     --
        ,DURATION2          INTEGER                     --
        ,DURATION3          INTEGER                     --
        ,MNS_TYPE           SMALLINT                    --
        ,INPUT_TIME         TIMESTAMP                   --
 )		
  DATA CAPTURE NONE		
 IN TBS_CDR_VOICE		
 INDEX IN TBS_INDEX		
  PARTITIONING KEY		
   ( USER_ID )		
 USING HASHING;		
		
		
		
		
		--DROP TABLE ODS_CDR_GPRS_GXZ_YYYYMMDD	
CREATE TABLE ODS_CDR_GPRS_GXZ_YYYYMMDD (		
         DR_TYPE            INTEGER                     --
        ,SERVICE_ID         INTEGER                     --
        ,BILL_MONTH         VARCHAR(8)                  --
        ,USER_NUMBER        VARCHAR(15)                 --
        ,VPLMN1             VARCHAR(7)                  --
        ,START_TIME         TIMESTAMP                   --
        ,DURATION           INTEGER                     --
        ,LAC_ID             VARCHAR(8)                  --
        ,CELL_ID            VARCHAR(16)                 --
        ,APN_NI             VARCHAR(64)                 --
        ,DATA_FLOW_UP1      INTEGER                     --
        ,DATA_FLOW_DOWN1    INTEGER                     --
        ,DURATION1          INTEGER                     --
        ,DATA_FLOW_UP2      INTEGER                     --
        ,DATA_FLOW_DOWN2    INTEGER                     --
        ,DURATION2          INTEGER                     --
        ,DURATION3          INTEGER                     --
        ,MNS_TYPE           SMALLINT                    --
        ,INPUT_TIME         TIMESTAMP                   --
 )		
  DATA CAPTURE NONE		
 IN TBS_CDR_VOICE		
 INDEX IN TBS_INDEX		
  PARTITIONING KEY		
   ( USER_NUMBER,START_TIME )		
 USING HASHING;		



db2 "load client from /bassdb2/etl/L/boss/A0502120101104000000.AVL of del modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 messages /dev/null replace into ODS_CDR_GPRS_GXZ_20101104"


FTP::172.16.5.46b\
/bassdb2/etl/E/boss/java/crm_interface/bin/config/BOSS/A05021.properties

java ETLMain 20101105 taskList_tmp_pzw.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101106 taskList_tmp_pzw.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101107 taskList_tmp_pzw.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101108 taskList_tmp_pzw.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101109 taskList_tmp_pzw.properties >> pzw_tmpExtra.out 2>&1 &



java ETLMain 20101103 taskList_tmp_pzw.properties

����ƽ(2651779)  15:37:25
2010-11-12:    ����ƽ    15002020810
2010-11-26:    ������    13631429212
2010-12-13:    ���    13560241290




DWD_NG2_I03032_20101020

DWD_NG2_I03034_20101020
DWD_NG2_I03036_20101020

DWD_NG2_I03037_20101020

select * from DWD_NG2_I03027_20101020
select * from DWD_NG2_I03032_20101020
select * from DWD_NG2_I03034_20101020
select * from DWD_NG2_I03036_20101020
select * from DWD_NG2_I03037_20101020

----------------------------------------------------------------------------------------------
Dwd_cust_manager_info_ds.tcl        Dwd_enterprise_manager_rela_ds.tcl
D:\XZBI\NG1-BASS1.0\Truck\03 ������\interface

ԭ��ʵ�ַ�ʽ:

insert into $objectTable
	(
		manager_id       
		,manager_type_id  
		,manager_status_id
		,dept_id          
		,member_type_id   
		,exam_role_id     
		,rec_status       
		,province_id      
		,city_id          
		,region_type_id   
		,region_detail    
		,manger_name      
		,gender           
		,manager_iden_id  
		,education_id     
		,email            
		,bill_id          
		,office_phone     
		,home_phone       
		,home_address     
		,postcode         
		,job_name         
		,job_desc         
		,manager_begin    
		,done_date        
		,valid_date       
		,expire_date      
		,op_id            
		,channel_id       
		,so_nbr           
	)
	select
		PARTY_ROLE_ID
		,manager_type
		,manager_status
		,dept_id
		,member_type
		,exam_role_id
		,state
		,char(province_id)
		,substr(city_id,2)
		,int(region_type)
		,region_detail
		,manager_name
		,gender
		,politics_face
		,education_level
		,email
		,bill_id
		,office_tel
		,home_tel
		,contact_address
		,postcode
		,job_position
		,job_company
		,begin_date
		,create_date
		,valid_date
		,expire_date
		,op_id
		,org_id
		,done_code
	from 
		ods_ent_en_staff_$p_timestamp
		
		
SELECT DISTINCT STAFF.STAFF_ID as manager_id,
                STAFF_NAME AS manager_NAME,
                OPERATOR_ID op_id, 
                OP.NAME AS OPREATOR_NAME, 
                ORG.ORGANIZE_ID AS ORG_ID,
                ORG.ORGANIZE_NAME AS ORG_NAME, 
                STAFF.EMAIL,
                STAFF.BILL_ID, 
                STAFF.NOTES,
                STAFF.STATE, 
                base.MANAGER_TYPE,
                base.MANAGER_STATUS,
                base.MEMBER_TYPE,
                base.EXAM_ROLE_ID,
				base.region_type, 
                base.REGION_DETAIL ,
				base.dept_id
  FROM ODS_DIM_SYS_STAFF_20101105           STAFF
   LEFT JOIN ODS_ENT_EN_STAFF_20101105 base ON STAFF.STAFF_ID =
                                             BASE.PARTY_ROLE_ID,
       ODS_DIM_SYS_OPERATOR_20101105        OP,
       ODS_DIM_SYS_ORGANIZE_20101105        ORG,
       ODS_DIM_SYS_STAFF_ORG_RELAT_20101105 RELA,
       ODS_ENT_CM_GROUP_CUSTOMER_20101105   GG
 
 WHERE OP.STAFF_ID = STAFF.STAFF_ID
   AND STAFF.STAFF_ID = RELA.STAFF_ID
   AND RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
   AND RELA.IS_BASE_ORG = 'Y'
   AND OP.STATE = 1
   AND STAFF.STATE = 1
   AND RELA.STATE = 1
   AND ORG.STATE = 1
      -- and to_number(op.code) ='100005819902'  
   AND TRIM(CHAR(GG.MGR_ID)) = OP.CODE WITH UR;
   



3.2.2.	����ʵ�ַ�ʽ
��ο������ṩ�ھ�����ɳ���ĸ��졣
-- Dwd_cust_manager_info_ds.tcl ���Ŀھ�
SELECT DISTINCT STAFF.STAFF_ID as manager_id,
                STAFF_NAME AS manager_NAME,
                OPERATOR_ID,
                OP.CODE AS CODE,
                OP.NAME AS OPREATOR_NAME,
                ORG.ORGANIZE_ID AS BASE_ORG_ID,
                ORG.ORGANIZE_NAME AS BASE_ORG_NAME,
                OP.MULTI_LOGIN_FLAG,
                ALLOW_CHANGE_PASSWORD,
                STAFF.EMAIL,
                STAFF.BILL_ID,
                ORG.DISTRICT_ID,
                STAFF.NOTES,
                STAFF.STATE,
                RELA.IS_ADMIN_STAFF,
                IS_BASE_ORG,
                base.MANAGER_TYPE,
                base.MANAGER_STATUS,
                base.MEMBER_TYPE,
                base.EXAM_ROLE_ID,
				base.region_type, 
                base.REGION_DETAIL 
  FROM ODS_DIM_SYS_STAFF_20101105           STAFF
   LEFT JOIN ODS_ENT_EN_STAFF_20101105 base ON STAFF.STAFF_ID =
                                             BASE.PARTY_ROLE_ID,
       ODS_DIM_SYS_OPERATOR_20101105        OP,
       ODS_DIM_SYS_ORGANIZE_20101105        ORG,
       ODS_DIM_SYS_STAFF_ORG_RELAT_20101105 RELA,
       ODS_ENT_CM_GROUP_CUSTOMER_20101105   GG
 
 WHERE OP.STAFF_ID = STAFF.STAFF_ID
   AND STAFF.STAFF_ID = RELA.STAFF_ID
   AND RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
   AND RELA.IS_BASE_ORG = 'Y'
   AND OP.STATE = 1
   AND STAFF.STATE = 1
   AND RELA.STATE = 1
   AND ORG.STATE = 1
      -- and to_number(op.code) ='100005819902'  
   AND TRIM(CHAR(GG.MGR_ID)) = OP.CODE WITH UR;
   
   
   --Dwd_enterprise_manager_rela_ds.tcl ���Ŀھ�
   SELECT DISTINCT STAFF.STAFF_ID as manager_id,
                 GG.GROUP_CUST_ID,
				 GG.IN_NET_DATE,
				 GG.CREATE_DATE,
				 GG.valid_date,
				 GG.EXPIRE_DATE,
				 GG.OP_ID,
				 GG.ORG_ID,
				 GG.DONE_CODE
  FROM ODS_DIM_SYS_STAFF_20101105           STAFF
   LEFT JOIN ODS_ENT_EN_STAFF_20101105 base ON STAFF.STAFF_ID =
                                             BASE.PARTY_ROLE_ID,
       ODS_DIM_SYS_OPERATOR_20101105        OP,
       ODS_DIM_SYS_ORGANIZE_20101105        ORG,
       ODS_DIM_SYS_STAFF_ORG_RELAT_20101105 RELA,
       ODS_ENT_CM_GROUP_CUSTOMER_20101105   GG
 
 WHERE OP.STAFF_ID = STAFF.STAFF_ID
   AND STAFF.STAFF_ID = RELA.STAFF_ID
   AND RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
   AND RELA.IS_BASE_ORG = 'Y'
   AND OP.STATE = 1
   AND STAFF.STATE = 1
   AND RELA.STATE = 1
   AND ORG.STATE = 1
      -- and to_number(op.code) ='100005819902'  
   AND TRIM(CHAR(GG.MGR_ID)) = OP.CODE WITH UR;


   
   
   SELECT * FROM SEC.SYS_OPERATOR
   
   
   
   
select a.*
from (   
SELECT	distinct 	
	    STAFF.STAFF_ID	manager_id
	    ,base.MANAGER_TYPE
		,base.MANAGER_STATUS
		,base.dept_id
		,base.MEMBER_TYPE
		,base.EXAM_ROLE_ID
		,STAFF.STATE
		,char(province_id)
		,substr(city_id,2)
		,base.region_type
		,base.REGION_DETAIL
		,STAFF_NAME	manager_NAME					
		,staff.gender
		,base.POLITICS_FACE
		,base.EDUCATION_LEVEL
		,STAFF.EMAIL
		,STAFF.BILL_ID
		,BASE.OFFICE_TEL
		,base.HOME_TEL
		,base.CONTACT_ADDRESS
		,base.POSTCODE
		,staff.job_position
		,base.JOB_DESC
		,base.begin_date
		,base.DONE_DATE
		,base.VALID_DATE
		,base.expire_date
		,OP.OPERATOR_ID	op_id					
		,ORG.ORGANIZE_ID	ORG_ID					
		,base.done_code
		FROM	ODS_DIM_SYS_STAFF_20101105	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101105	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101105	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101105	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101105 ) 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101105	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
			WITH	UR) a
			join 
�� select manager_id,count(0) cnt 
from ((   
SELECT	distinct 	
	    STAFF.STAFF_ID	manager_id
	    ,base.MANAGER_TYPE
		,base.MANAGER_STATUS
		,base.dept_id
		,base.MEMBER_TYPE
		,base.EXAM_ROLE_ID
		,STAFF.STATE
		,char(province_id)
		,substr(city_id,2)
		,base.region_type
		,base.REGION_DETAIL
		,STAFF_NAME	manager_NAME					
		,staff.gender
		,base.POLITICS_FACE
		,base.EDUCATION_LEVEL
		,STAFF.EMAIL
		,STAFF.BILL_ID
		,BASE.OFFICE_TEL
		,base.HOME_TEL
		,base.CONTACT_ADDRESS
		,base.POSTCODE
		,staff.job_position
		,base.JOB_DESC
		,base.begin_date
		,base.DONE_DATE
		,base.VALID_DATE
		,base.expire_date
		,OP.OPERATOR_ID	op_id					
		,ORG.ORGANIZE_ID	ORG_ID					
		,base.done_code
		FROM	ODS_DIM_SYS_STAFF_20101105	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101105	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101105	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101105	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101105 ) 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101105	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
			WITH	UR) ) group by manage_id 
			having count(0) > 1 
			) b on a.manager_id = b.manager_id 
			


db2 "create table ODS_ENT_EN_STAFF_YYYYMMDD like ODS_ENT_EN_STAFF_20100531 in tbs_3h \
  PARTITIONING KEY ( PARTY_ID ) USING HASHING not logged initially"
  


IN TBS_3H
partitioning key (PARTY_ID) using hashing
not logged initially;





  --Dwd_enterprise_manager_rela_ds.tcl ���Ŀھ�
   SELECT DISTINCT STAFF.STAFF_ID as manager_id,
                 GG.GROUP_CUST_ID,
				 GG.IN_NET_DATE,
				 GG.CREATE_DATE,
				 GG.valid_date,
				 GG.EXPIRE_DATE,
				 GG.OP_ID,
				 GG.ORG_ID,
				 GG.DONE_CODE
  FROM ODS_DIM_SYS_STAFF_20101105           STAFF
   LEFT JOIN ODS_ENT_EN_STAFF_20101105 base ON STAFF.STAFF_ID =
                                             BASE.PARTY_ROLE_ID,
       ODS_DIM_SYS_OPERATOR_20101105        OP,
       ODS_DIM_SYS_ORGANIZE_20101105        ORG,
       ODS_DIM_SYS_STAFF_ORG_RELAT_20101105 RELA,
       ODS_ENT_CM_GROUP_CUSTOMER_20101105   GG
 
 WHERE OP.STAFF_ID = STAFF.STAFF_ID
   AND STAFF.STAFF_ID = RELA.STAFF_ID
   AND RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
   AND RELA.IS_BASE_ORG = 'Y'
   AND OP.STATE = 1
   AND STAFF.STATE = 1
   AND RELA.STATE = 1


SELECT 						
DISTINCT						
STAFF.STAFF_ID		GG.IN_NET_DATE,				
		GG.CREATE_DATE,				
						
						
						
GG.valid_date,						
GG.EXPIRE_DATE,						
GG.OP_ID,						
GG.ORG_ID,						
GG.DONE_CODE						
						
FROM	ODS_DIM_SYS_STAFF_20101105	STAFF				
LEFT	JOIN	ODS_ENT_EN_STAFF_20101105	base	ON	STAFF.STAFF_ID	=
BASE.PARTY_ROLE_ID,						
ODS_DIM_SYS_OPERATOR_20101105	OP,					
ODS_DIM_SYS_ORGANIZE_20101105	ORG,					
ODS_DIM_SYS_STAFF_ORG_RELAT_20101105	RELA,					
ODS_ENT_CM_GROUP_CUSTOMER_20101105	GG					
						
WHERE	OP.STAFF_ID	=	STAFF.STAFF_ID			
AND	STAFF.STAFF_ID	=	RELA.STAFF_ID			
AND	RELA.ORGANIZE_ID	=	ORG.ORGANIZE_ID			
AND	RELA.IS_BASE_ORG	=	'Y'			
AND	OP.STATE	=	1			
AND	STAFF.STATE	=	1			
AND	RELA.STATE	=	1			
						
desc ODS_DIM_SYS_STAFF_20101105 >> f2.txt
desc ODS_ENT_EN_STAFF_20101105 >> f2.txt
desc ODS_DIM_SYS_OPERATOR_20101105 >> f2.txt
desc ODS_DIM_SYS_ORGANIZE_20101105 >> f2.txt
desc ODS_DIM_SYS_STAFF_ORG_RELAT_20101105 >> f2.txt




		FROM	ODS_DIM_SYS_STAFF_20101105	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101105	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101105	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101105	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101105 ) 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101105	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
			WITH	UR) ) group by manage_id 
			having count(0) > 1 
			) b on a.manager_id = b.manager_id 
			

1	���ű��	GROUP_ID	NUMBER(12)	
2	���ű��	CLUSTER_ID	NUMBER(10)	
3	�ͻ�������	MANAGER_ID	NUMBER(8)	
4	��������	OPER_TYPE	NUMBER(2)	0   ��ѯ1   ��ѯ������
5	��ϵ����	REL_TYPE	NUMBER(2)	1:��Ҫ����2:��������3:��ϯ�ͻ�����
6	��¼״̬	REC_STATUS	NUMBER(2)	0��ʷ1��Ч
7	��������	DONE_DATE	DATE	
8	��Ч����	VALID_DATE	DATE	
9	ʧЧ����	EXPIRE_DATE	DATE	
10	����Ա	OP_ID	NUMBER(8)	
11	��֯��Ԫ	ORG_ID	NUMBER(8)	
12	������ˮ	DONE_CODE	NUMBER(15)	


select DISTINCT
 GG.GROUP_CUST_ID
,base.dept_id CLUSTER_ID
,STAFF.STAFF_ID as manager_id
,1  OPER_TYPE 
,1 	REL_TYPE
,case when gg.expire_date<TIMESTAMP('2010-11-13 00:00:00.00000') then 0 else 1 end REC_STATUS
,GG.CREATE_DATE
,GG.valid_date
,GG.EXPIRE_DATE
,GG.OP_ID
,GG.ORG_ID
,GG.DONE_CODE
		FROM	ODS_DIM_SYS_STAFF_20101113	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101113	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101113	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101113	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN (SELECT DISTINCT MGR_ID ,GROUP_CUST_ID,EXPIRE_DATE,OP_ID,ORG_ID,DONE_CODE FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101113 ) 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101113	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
		



select DISTINCT
 count(GG.GROUP_CUST_ID)
, count(distinct GG.GROUP_CUST_ID)
, count(distinct STAFF.STAFF_ID)
, count(distinct GG.GROUP_CUST_ID||gg.GROUP_CUST_ID)
		FROM	ODS_DIM_SYS_STAFF_20101113	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101113	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101113	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101113	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN ODS_ENT_CM_GROUP_CUSTOMER_20101113 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101113	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
		


select DISTINCT
 GG.GROUP_CUST_ID
,base.dept_id CLUSTER_ID
,STAFF.STAFF_ID as manager_id
,1  OPER_TYPE 
,1 	REL_TYPE
,case when expire_date<TIMESTAMP('$p_optime 00:00:00.00000') then 0 else 1 end REC_STATUS
,GG.CREATE_DATE
,GG.valid_date
,GG.EXPIRE_DATE
,GG.OP_ID
,GG.ORG_ID
,GG.DONE_CODE
		FROM	ODS_DIM_SYS_STAFF_20101113	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101113	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101113	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101113	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN ODS_ENT_CM_GROUP_CUSTOMER_20101113 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101113	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1
	



ds Dwd_cust_manager_info_ds.tcl 2010-11-14
ds Dwd_enterprise_manager_rela_ds.tcl  2010-11-14

ǰ��������

select * from app.sch_control_before

select * from app.sch_control_task  
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  
;


insert into app.sch_control_before 
select 'BASS2_Dwd_enterprise_manager_rela_ds.tcl',control_code
from app.sch_control_task 
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  





select * from app.sch_control_before 
where control_code like '%BASS2_Dwd_enterprise_manager_rela_ds.tcl%'
;

delete from app.sch_control_before 
where control_code like '%BASS2_Dwd_enterprise_manager_rela_ds%'
;

insert into app.sch_control_before 
select 'BASS2_Dwd_enterprise_manager_rela_ds.tcl',control_code
from app.sch_control_task 
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  



select * from app.sch_control_before 
where control_code like '%Dwd_cust_manager_info_ds.tcl%'
;

delete from app.sch_control_before 
where control_code like '%BASS2_Dwd_cust_manager_info_ds%'
;

insert into app.sch_control_before 
select 'BASS2_Dwd_cust_manager_info_ds.tcl',control_code
from app.sch_control_task 
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  


  FROM ODS_DIM_SYS_STAFF_$p_timestamp staff    
  JOIN ODS_DIM_SYS_OPERATOR_$p_timestamp OP     ON      STAFF.STAFF_ID = OP.STAFF_ID
  JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_$p_timestamp RELA ON    STAFF.STAFF_ID = RELA.STAFF_ID  
  JOIN ODS_DIM_SYS_ORGANIZE_$p_timestamp ORG         ON      RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
  JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_$p_timestamp )  GG ON TRIM(CHAR(GG.MGR_ID)) = OP.CODE 
  LEFT JOIN ODS_ENT_EN_STAFF_$p_timestamp base ON STAFF.STAFF_ID = BASE.PARTY_ROLE_ID  
  WHERE RELA.IS_BASE_ORG = 'Y'   
  AND OP.STATE = 1   
  AND STAFF.STATE = 1   
  AND RELA.STATE = 1   
  AND ORG.STATE = 1   
  "
  

 SELECT distinct  
     STAFF.STAFF_ID manager_id
     ,base.MANAGER_TYPE
  ,base.MANAGER_STATUS
  ,base.dept_id
  ,base.MEMBER_TYPE
  ,base.EXAM_ROLE_ID
  ,STAFF.STATE
  ,char(province_id)
  ,substr(city_id,2)
  ,int(base.region_type)
  ,base.REGION_DETAIL
  ,STAFF_NAME manager_NAME     
  ,staff.gender
  ,base.POLITICS_FACE
  ,base.EDUCATION_LEVEL
  ,STAFF.EMAIL
  ,STAFF.BILL_ID
  ,BASE.OFFICE_TEL
  ,base.HOME_TEL
  ,base.CONTACT_ADDRESS
  ,base.POSTCODE
  ,staff.job_position
  ,base.JOB_DESC
  ,base.begin_date
  ,base.DONE_DATE
  ,base.VALID_DATE
  ,base.expire_date
  ,OP.OPERATOR_ID op_id     
  ,ORG.ORGANIZE_ID ORG_ID     
  ,base.done_code
  FROM ODS_DIM_SYS_STAFF_$p_timestamp staff    
  JOIN ODS_DIM_SYS_OPERATOR_$p_timestamp OP     ON      STAFF.STAFF_ID = OP.STAFF_ID
  JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_$p_timestamp RELA ON    STAFF.STAFF_ID = RELA.STAFF_ID  
  JOIN ODS_DIM_SYS_ORGANIZE_$p_timestamp ORG         ON      RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
  JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_$p_timestamp )  GG ON TRIM(CHAR(GG.MGR_ID)) = OP.CODE 
  LEFT JOIN ODS_ENT_EN_STAFF_$p_timestamp base ON STAFF.STAFF_ID = BASE.PARTY_ROLE_ID  
  WHERE staff.STAFF_ID=100005816062
  
  "
  
  /**	2010-11-16 11:27	added by  panzhiwei		**/	
--DROP TABLE DWD_NG2_I08112_YYYYMMDD;					
CREATE TABLE DWD_NG2_I08112_YYYYMMDD (					
			BUSI_CODE				VARCHAR(8)	--ҵ�����ʹ���			
			,BUSI_NAME			VARCHAR(60)	--ҵ����������			
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( BUSI_CODE )
 USING HASHING;


CD09NB0013







/**	2010/11/2 18:02	added by  panzhiwei		**/	
CREATE TABLE DWD_NG2_M06020_YYYYMMDD (					

 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING;



/**	2010-11-22 16:36	added by  panzhiwei		**/													
--DROP TABLE DWD_NG2_M06020_YYYYMMDD;																	
CREATE TABLE DWD_NG2_M06020_YYYYMMDD (																	
        REGION_ID           VARCHAR(7)          --�����ʶ
        ,manufacture_name   VARCHAR(100)        --�ն˳���
        ,term_model         VARCHAR(20)         --�ն��ͺ�
        ,vendor_name        VARCHAR(100)        --�ն˹�����
        ,settle_type        SMALLINT            --��������
        ,settle_info_id     BIGINT              --����֪ͨ�����
        ,pay_acct_id        BIGINT              --֧��ƾ֤��
        ,pay_state          SMALLINT            --֧��״̬
        ,done_code          BIGINT              --����������
        ,contract_id        BIGINT              --�����ͬ���
        ,valid_date         date           --��Ч����
        ,expire_date        date           --��ֹ����
        ,info_date          date           --֪ͨ����
        ,settle_date        date           --��������
        ,settle_amt         DECIMAL(12,0)       --�����ܽ��
        ,is_settle_amt      DECIMAL(12,0)       --�ѽ�����
        ,not_settle_amt     DECIMAL(12,0)       --δ������
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( settle_info_id,REGION_ID )
 USING HASHING;


TASK_ID=M06020
MODULE=GG
RUNTYPE=R
DATECYCLE=D
DATEPARAM=$YYYYMMDD$
AREAPARAM=891,892,893,894,895,896,897
RUNSQL=select \
REGION_ID                  ,\
manufacture_name ,\
term_model ,\
vendor_name ,\
settle_type ,\
settle_info_id ,\
pay_acct_id ,\
pay_state ,\
done_code ,\
contract_id ,\
valid_date ,\
expire_date ,\
info_date ,\
settle_date ,\
settle_amt ,\
is_settle_amt ,\
not_settle_amt ,\
 ,\


db2 "load client from /bassapp/bass2/load/boss/M0602020101001000000.AVL of del modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 messages /dev/null replace into DWD_NG2_M06020_201010"



db2 "load client from /bassapp/bass2/load/boss/M0602020101001000000.AVL of del \
 modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 \
 messages /bassapp/bass2/load/boss/messages/db2load.msg  replace into DWD_NG2_M06020_201010
 "
 
 
 
 --ǰ̨ӪҵԱ
--drop table dim_boss_staff;
create table dim_boss_staff (
	op_id		integer not null,	--����Ա���
	op_name		varchar(32),    	--����Ա����
	op_status	smallint,       	--����Ա��״̬
	create_date	timestamp,      	--��������
	valid_date	timestamp,      	--��Ч����
	expire_date	timestamp,      	--ʧЧ����
	channel_id	integer,        	--������֯
	city_id		varchar(7),     	--��������
	county_id	varchar(7)      	--������
)
in tbs_dim
index in tbs_dim
partitioning key (op_id) using hashing
not logged initially;

alter table dim_boss_staff add primary key (op_id);


create table tmp_pzw_dim_boss_staff like dim_boss_staff 
in tbs_dim
index in tbs_dim
partitioning key (op_id) using hashing
not logged initially;


alter table dim_boss_staff add 
 
  

db2 "export to /bassapp/bass2/panzw2/data/dim_boss_staff.txt of del \
MODIFIED BY nochardel coldel$ \
select * from bass2.dim_boss_staff";



update dim_boss_staff a 
set a.code = (select b.code from ods_dim_sys_operator_20101124 b where a.op_id = b.staff_id)


�в�ӿ�����(tcl����):
1.д��ȡ�ű�
2.дtcl
3.�������
4.����ftp
5.ִ��tcl
6.�Զ�����

7.��������
7.1�ֶ�����һ��
7.2���ڸ�ʽ����
7.3������ֶβ���
7.4����Ŀ¼����
7.5���ڸ�ʽ����

1125�չ����ز�-�ӿ��б�2-ѡ��ճ��


A02027	XG	AGENT_OPER	YYYYMM		��¼�ͷ�ϵͳ�����BOSSҵ�������־��		��	ODS_AGENT_OPER_YYYYMMDD	172.16.5.46		



java ETLMain 20101120 taskList_tmp_pzw.properties


A02027

ETL_LOAD_TABLE_MAP2

TASK_ID	TABLE_NAME_TEMPLET	TASK_NAME	LOAD_METHOD	BOSS_TABLE_NAME		

I06020	DWD_NG2_I06020_YYYYMMDD	��������嵥	0	BASS2.CDR_ISMG_YYYYMMDD		insert into etl_load_table_map values('I06020','DWD_NG2_I06020_YYYYMMDD','��������嵥',0,'BASS2.CDR_ISMG_YYYYMMDD');


########################################input
i_interface_dat_rule=$1
i_interface_id=$2
i_interface_boss_skm=$3
#ȥ����������
i_interface_boss_table=$4
i_interface_table_format=$5
i_interface_boss_table_name_cn=$6

########################################process

o_interface_code=${i_interface_table_format}${i_interface_id}
o_interface_boss_skm=${i_interface_boss_skm}
o_interface_boss_table=${i_interface_boss_table}
i_interface_table_format=${i_interface_table_format}



TASK_ID=


########################################output



awk -F" " '{printf "\t%-20s\t%-20s\n" ,$1,$3"("$4")"}' testdesc.txt   

printf "\t%-20s%-20s%10s\n" ,$1,$3"("$4")"

sed -e '1,4d'|sed -e '$d'|sed -e '$d'|sed -e '$d'

desf(){
desc VGOP_15202_20101020  |\
awk -F" " '{printf "\t%-20s\t%-20s\n" ,$1,$3"("$4")"}'|\
sed -e '1,4d'|sed -e '$d'|sed -e '$d'|sed -e '$d'   
}



svn ���������⣺
a. 1.4.5 (r25188) vs 1.4.6 (r28521)

apache2.2.11 �� 1.4.6 (r28521) ���ݣ��� 1.4.5 (r25188) ������

����apache2.2��֧��1.4.5
Ҫ����apache2.0.x

mod_authz_svn ��֧�� AuthzSVNAccessFile, ����ʹ�á�



ODS_CDR_GPRS_GXZ_YYYYMMDD


Usage: ./crt.sh TABLE_NAME_TEMPLET TBLSPACE_NAME PARTITIONING_KEK YYYYMM
/bassapp/bass2/fuzl
crt.sh ODS_CDR_GPRS_GXZ TBS_CDR_VOICE "USER_NUMBER,START_TIME" 201012




 )		
  DATA CAPTURE NONE		
 IN TBS_CDR_VOICE		
 INDEX IN TBS_INDEX		
  PARTITIONING KEY		
   ( USER_NUMBER,START_TIME )		
 USING HASHING;		


select count(0),count(distinct control_code) from app.sch_control_task


 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 or 
 content like '%A05021%'
 or 
 content like '%A05021%'
 or 
 content like '%ENTERPRISE_MANAGER_RELA%'
 or 
 content like '%CUST_MANAGER_INFO%'
 )
 
 
 TABNAME
ETL_LOAD_TABLE_MAP
ETL_LOAD_TABLE_MAP_20080201
ETL_LOAD_TABLE_MAP_20100626
ETL_ROLLBACK_TASK_MAP
ETL_SEND_MESSAGE
ETL_TASK_LOG
ETL_TASK_LOG_20080105
ETL_TASK_LOG_OLD
ETL_TASK_LOG_XZ48
ETL_TASK_RUNNING





Usage: ./crt.sh TABLE_NAME_TEMPLET TBLSPACE_NAME PARTITIONING_KEK YYYYMM
/bassapp/bass2/fuzl
crt.sh ODS_CDR_GPRS_GXZ TBS_CDR_VOICE "USER_NUMBER,START_TIME" 201012



dm ����
ds ����
dt ���¶���
ms ����
mm ����




/**	2010-12-3 10:40	added by  panzhiwei		**/
--DROP TABLE ODS_CHANNEL_LOCAL_BUSI_YYYYMMDD;				
CREATE TABLE ODS_CHANNEL_LOCAL_BUSI_YYYYMMDD (				
        ENTITY_TYPE         INTEGER             --ENTITY_TYPE
        ,OBJ_ID             INTEGER               --OBJ_ID
        ,product_no         VARCHAR(20)         --product_no
        ,city_id            VARCHAR(8)              --����
        ,county_id          BIGINT                  --����
        ,channel_type_class INTEGER             --channel_type_class
        ,channel_child_type INTEGER             --����������
        ,channel_name       VARCHAR(40)         --��������
        ,card_value         BIGINT              --card_value
        ,channel_id         BIGINT              --channel_id
        ,REC_STATUS         SMALLINT            --REC_STATUS
        ,OP_ID              BIGINT                 --OP_ID
        ,ORG_ID             BIGINT                --ORG_ID
        ,DONE_CODE          BIGINT              --DONE_CODE
        ,DONE_DATE          TIMESTAMP           --DONE_DATE
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( obj_id )
 USING HASHING;




db2 "load client from /bassapp/bass2/load/boss/I0500220101202000000.AVL of del \
 modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 \
 messages /bassapp/bass2/load/boss/messages/db2load.msg  replace into DWD_NG2_I05002_20101202
 "
 
 
 
 db2 "load client from /bassapp/bass2/load/boss/I0503720101202000000.AVL of del \
 modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 \
 messages /bassapp/bass2/load/boss/messages/db2load.msg replace into DWD_NG2_I05037_20101202"
 
 
 
 sort sort.txt|uniq -c
 
 $ ls -1 *[a-zA-Z].tcl |awk '{print substr($1,length($1)-5,length($1))}' 
 
 5.46: 
$ sqlplus bi/bi@crmtest


datatype_ibm2oracle

datatype_oracle2ibm

=IF(LEFT(UPPER(boss_desc2!B3),1)="N",
IF(INT(SUBSTITUTE(SUBSTITUTE(UPPER(boss_desc2!B3),"NUMBER(","",1),")","",1))>10,"BIGINT",
IF(INT(SUBSTITUTE(SUBSTITUTE(UPPER(boss_desc2!B3),"NUMBER(","",1),")","",1))>5,"INTEGER","SMALLINT"))
		,SUBSTITUTE(SUBSTITUTE(UPPER(boss_desc2!B3),"VARCHAR2","VARCHAR",1),"DATE","TIMESTAMP",1))

if [ $? != 0 ] 




/**	2010-12-9 16:06	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_P99924;				
CREATE TABLE DWD_NG2_P99924 (				
        dept_id              VARCHAR(8)          --���б�ʶ
        ,dept_name        VARCHAR(20)          --���ر�ʶ
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( dept_id )
 USING HASHING;


		

/**	2010-12-9 17:30	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_P99905_YYYYMMDD;				
CREATE TABLE DWD_NG2_P99905_YYYYMMDD (				
        LACCODE             VARCHAR(64)           --LAC_CD
        ,CELLCODE           VARCHAR(64)          --CELL_CD
        ,REGION             VARCHAR(8)          --��������
        ,SCHMARKER          VARCHAR(32)         --ѧУ��ʶ
        ,SCHLONG            VARCHAR(32)             --����
        ,SCHLAT             VARCHAR(32)             --γ��
        ,COVERRATE          decimal(10,2)             --��վѧУ������
        ,EFF_DATE           TIMESTAMP           --��Ч����
        ,EXP_DATE           TIMESTAMP           --ʧЧ����
        ,SCHNAME			VARCHAR(200)		--ѧУ����
 )				
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( LACCODE,CELLCODE )				
 USING HASHING;		
 
 --DROP TABLE DWD_NG2_P99905_20101202;				
CREATE TABLE DWD_NG2_P99905_20101202 (				
        LACCODE             VARCHAR(64)           --LAC_CD
        ,CELLCODE           VARCHAR(64)          --CELL_CD
        ,REGION             VARCHAR(8)          --��������
        ,SCHMARKER          VARCHAR(32)         --ѧУ��ʶ
        ,SCHLONG            VARCHAR(32)             --����
        ,SCHLAT             VARCHAR(32)             --γ��
        ,COVERRATE          decimal(10,2)             --��վѧУ������
        ,EFF_DATE           TIMESTAMP           --��Ч����
        ,EXP_DATE           TIMESTAMP           --ʧЧ����
        ,SCHNAME			VARCHAR(200)		--ѧУ����        
 )				
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( LACCODE,CELLCODE )				
 USING HASHING;		
 


 --DROP TABLE DWD_NG2_M99905_201012;				
CREATE TABLE DWD_NG2_M99905_201012 (				
        LACCODE             VARCHAR(64)           --LAC_CD
        ,CELLCODE           VARCHAR(64)          --CELL_CD
        ,REGION             VARCHAR(8)          --��������
        ,SCHMARKER          VARCHAR(32)         --ѧУ��ʶ
        ,SCHLONG            VARCHAR(32)             --����
        ,SCHLAT             VARCHAR(32)             --γ��
        ,COVERRATE          decimal(10,2)             --��վѧУ������
        ,EFF_DATE           TIMESTAMP           --��Ч����
        ,EXP_DATE           TIMESTAMP           --ʧЧ����
        ,SCHNAME			VARCHAR(200)		--ѧУ����        
 )				
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( LACCODE,CELLCODE )				
 USING HASHING;		
 





$ db2 ? 42704

SQLSTATE 42704: An undefined object or constraint name was detected.

$ db2 ? sql204


SQL0204N "<name>" is an undefined name.  

Explanation: 



CREATE TABLE "BASS2   "."DWD_NG2_M99912_20101202"  (
                  "TIME_ID" VARCHAR(8) , 
                  "DEVICE_ID" VARCHAR(16) , 
                  "PROPERTY_ID" VARCHAR(16) , 
                  "VALUE" VARCHAR(16) , 
                  "VALUE_DESC" VARCHAR(1024) )   
                 DISTRIBUTE BY HASH("DEVICE_ID",  
                 "PROPERTY_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" ; 



���ش����ų���

dsmp:

cp /bassdb1/etl/L/dsmp/data/backup/*20101213* /bassdb1/etl/L/dsmp/


139mail:

mgame :
mv $avl_filename $DATA_PATH

cp $DATA_PATH .
cp /bassdb1/etl/L/mgame/data 
cp /bassdb1/etl/L/mgame/error/I43*20101213* /bassdb1/etl/L/mgame/

./load_mgame.sh 20101213



vgop:
��������


0,5,10,15,20,25,30,35,40,45,50,55 * * * * . .profile;/bassdb1/etl/L/monitor/if_load_start.sh
0 4,14 * * * . .profile;/bassdb1/etl/L/bank/build_bank.sh
0 4,14 * * * . .profile;/bassdb1/etl/L/js/build_js.sh
3,23,43 * * * * . .profile;/bassdb1/etl/L/boss/load_boss_monitor.sh
50 22 * * * . .profile;/bassdb1/etl/L/139mail/build_139mail.sh
33 5,6,7,8,9 * * * . .profile;/bassdb1/etl/L/mr/kill_load_ftp_mr.sh
30 8 * * * . .profile;/bassdb1/etl/L/nm/ftp_nmota_day.sh
30 22 * * * . .profile;/bassdb1/etl/L/cb/build_cb_day.sh
30 8 10 * * . .profile;/bassdb1/etl/L/cb/build_cb.sh
##0 18 * * * . .profile;/bassdb1/etl/L/fetion/build_fetion_day.sh
0 13 * * * . .profile;/bassdb1/etl/L/mgame/load_mgame.sh
0 8 * * * . .profile;/bassdb1/etl/L/mobilepay/interface_mobilepay.sh
0 20 * * * . .profile;/bassdb1/etl/L/vgop/backup/autodispatch_byday.sh


$ ls -l *99008* *99014*


db2look -d bassdb56 -e -i bass2 -w bass2 -z bass1 -t  BASS1.G_MON_D_INTERFACE
  

drop table DWD_DSMP_WAP_MENU_DS_201012
CREATE TABLE "BASS2   "."DWD_DSMP_WAP_MENU_DS_201012"  (
                  "MENU_ID" VARCHAR(8) , 
                  "DOOR_TYPE_ID" INTEGER , 
                  "MENU_NAME" VARCHAR(64) , 
                  "MENU_STATUS_ID" VARCHAR(2) , 
                  "STARTUP_DATE" TIMESTAMP , 
                  "FATHER_MENU_ID" VARCHAR(8) , 
                  "MENU_TYPE_ID" VARCHAR(3) , 
                  "SP_CODE" VARCHAR(12) , 
                  "SP_OPER_CODE" VARCHAR(10) , 
                  "CREATE_DATE" TIMESTAMP , 
                  "ESP_MENU_TYPE_ID" VARCHAR(2) , 
                  "BIG_KIND_ID" INTEGER , 
                  "SMALL_KIND_ID" INTEGER , 
                  "WAP_DOOR_TYPE_ID" INTEGER , 
                  "SUP_MENU_TYPE_ID" VARCHAR(8) , 
                  "MENU_LEVEL" INTEGER , 
                  "BILL_DATE" VARCHAR(10) )   
                 DISTRIBUTE BY HASH("MENU_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 
                   

drop table DWD_DSMP_EQUIPMENT_DS_201012
CREATE TABLE "BASS2   "."DWD_DSMP_EQUIPMENT_DS_201012"  (
                  "EPM_ID" VARCHAR(10) , 
                  "EPM_NAME" VARCHAR(20) , 
                  "EPM_KIND_ID" INTEGER , 
                  "BILL_DATE" VARCHAR(10) )   
                 DISTRIBUTE BY HASH("EPM_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 

ALTER TABLE "BASS2   "."DWD_DSMP_EQUIPMENT_DS_201012" LOCKSIZE TABLE;


ls -l *99014*201012* *99008*201012*
cp *99014*201012* *99008*201012* /bassdb1/etl/L/dsmp/

                   
$ ./load_vgop.sh i_13100_20101213_VGOP-R1.6-14303_00_001.dat

                   
                   
db2look -d bassdb56 -e -i bass2 -w bass2 -z bass2 -t DWD_DSMP_mnet_user_201011  
db2look -d bassdb56 -e -i bass2 -w bass2 -z bass2 -t DWD_DSMP_sp_company_code_201011  
db2look -d bassdb56 -e -i bass2 -w bass2 -z bass2 -t DWD_DSMP_sp_oper_code_201011
  
                   
/**	2010-12-15 14:27	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_M99904_YYYYMMDD;				
CREATE TABLE DWD_NG2_M99904_YYYYMMDD (				
        SCHMARKER           VARCHAR(32)         --ѧУ��ʶ
        ,GROUPCODE          VARCHAR(32)         --���ű���
        ,SCHNAME            VARCHAR(200)        --ѧУ����
        ,PRODUCT_NO         VARCHAR(15)         --�ֻ�����
        ,CUST_NAME          VARCHAR(200)        --ѧ������
        ,SEX_ID             SMALLINT            --ѧ���Ա�
        ,OPERATOR           SMALLINT            --��Ӫ�̹���
        ,BRAND_ID           BIGINT                  --Ʒ��
        ,WORK_DEPT          VARCHAR(120)            --Ժϵ
        ,GRADE              SMALLINT                --�༶
        ,HOME_ADDRESS       VARCHAR(200)            --סַ
        ,IDEN_NBR           VARCHAR(40)         --���֤��
        ,EDUCATION_ID       SMALLINT                --ѧ��
        ,VALID_DATE         integer           --��ѧ���
        ,POSITION           SMALLINT                --ְ��
        ,EMAIL              VARCHAR(64)         --�����ʼ���ַ
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( PRODUCT_NO,GROUPCODE )
 USING HASHING;

db2 "load client from /bassapp/bass2/load/boss/M9990420101100000000.AVL.load of del modified by
 coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 messages /dev/null replace into DWD_NG2_M99904_201011"

 
 
 db2 "load client from /bassapp/bass2/load/boss/M9990420101100000000.AVL of del  \
 modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" \
 fastparse anyorder warningcount 1000 messages /bassapp/bass2/load/boss/messages/db2load.msg 
 replace into DWD_NG2_M99904_201011 "
 
 
 
 
                    
/**	2010-12-15 15:18	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_M99914_YYYYMM;				
CREATE TABLE DWD_NG2_M99914_YYYYMM (				
        SCHMARKER           VARCHAR(32)         --ѧУ��ʶ
        ,GROUPCODE	VARCHAR(32)
        ,SCHNAME            VARCHAR(200)        --ѧУ����
        ,CUST_NAME          VARCHAR(200)        --��ְ��Ա����
        ,SEX_ID             SMALLINT                --�Ա�
        ,WORK_DEPT          VARCHAR(120)            --����
        ,POSITION           SMALLINT                --ְλ
        ,PRODUCT_NO         VARCHAR(15)         --�ֻ�����
        ,OPERATOR           SMALLINT            --��Ӫ�̹���
        ,BRAND_ID           BIGINT             --Ʒ��
        ,LINK_PHONE         VARCHAR(32)         --������ϵ�绰
        ,EMAIL              VARCHAR(64)         --�����ַ
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( PRODUCT_NO,GROUPCODE )
 USING HASHING;
                    

/**	2010-12-15 15:41	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_M99903_YYYYMM;				
CREATE TABLE DWD_NG2_M99903_YYYYMM (				
        SCHMARKER           VARCHAR(32)         --ѧУ��ʶ
        ,SCHNAME            VARCHAR(200)        --ѧУ����
        ,REGION             VARCHAR(8)          --��������
        ,STUDENT_NUM        INTEGER             --ѧ������
        ,SCHADDRESS         VARCHAR(200)        --ѧУ��ַ
        ,PARENT_SCHOOL_IND  INTEGER             --�ϼ�ѧУ��ʶ
        ,MARKETING_AREA     INTEGER             --����Ӫ������
        ,GROUPCODE          VARCHAR(32)         --���ű���
        ,ADMIN_DEPT         VARCHAR(120)        --���ܲ���
        ,EMPLOYEE_NUM       INTEGER             --��ְ������
        ,RUN_TYPE           VARCHAR(8)          --��У����
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( SCHMARKER )
 USING HASHING;


У԰�г���
����->ѧУ-��Ա



db2 "load client from /bassapp/bass2/load/boss/M9990520101100000000.AVL of del \
 modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 \
 messages /bassapp/bass2/load/boss/messages/db2load.msg  replace into DWD_NG2_M99905_201011
 "
 

�в�-���ݼ���-Ǩ�ƣ�



$ ps -ef |grep sleep
   bass2 21364 26243   0 16:05:09 pts/27      0:00 grep sleep
   bass2 20174 27383   0 16:04:33 ?           0:00 sleep 60
   bass2 21273 13117   0 16:05:06 ?           0:00 sleep 60
   bass2 25557  1324   0 15:38:20 ?           0:00 sh sleep.sh
$ kill 1324
kill: 1324: Ȩ�޲���
$ kill 25557
$ kill -9 25557
$ ps -ef |grep sleep
   bass2 21912 27383   0 16:05:33 ?           0:00 sleep 60
   bass2 23042 26243   0 16:06:10 pts/27      0:00 grep sleep
   bass2 22952 13117   0 16:06:08 ?           0:00 sleep 60
   


sendstopmsg(){
#�������в���
if [ $# -eq 1 ] ; then
	if [  "$1" = "stop"   ] ; then
		if [ -f $RUNNING_FILE ] ; then
			touch $STOP_FILE
			echo "������ֹͣ��־�ļ�,�ȴ��˳�!"
			exit
		fi
		echo "����û�����У�"
		exit
	fi
fi
}

runstatus(){
if [ -f $WORK_PATH/$RUNNING_FILE ] ; then
	echo "���������У�"
	exit
fi
}

	

ifbreak(){	
	#��ֹͣ��־�ļ����ڣ�������˳�
	if [ -f $WORK_PATH/$STOP_FILE ]; then
		rm  $WORK_PATH/$RUNNING_FILE
		rm  $WORK_PATH/$STOP_FILE
		break
	fi
}		




db2 "load client from /bassapp/bass2/panzw2/data/school_org.txt \
of del modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder \
warningcount 1000 messages /bassapp/bass2/load/boss/messages/db2load.msg  \
replace into DWD_NG2_P99934"





db2 "load client from /bassapp/bass2/panzw2/data/DWD_NG2_P99924 \
of del modified by coldel, timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder \
warningcount 1000 messages /bassapp/bass2/load/boss/messages/db2load.msg  \
replace into DWD_NG2_P99924"








  

db2 "export to /bassapp/bass2/panzw2/data/dim_boss_staff.txt of del \
MODIFIED BY nochardel coldel$ \
select * from bass2.dim_boss_staff";



db2 "export to /bassapp/bass2/panzw2/data/dim_boss_staff.txt of del \
MODIFIED BY nochardel coldel$ \
select * from bass2.dim_boss_staff";



school_org.txt

db2 terminate;db2 connect to bassdb user bass2 using bass2

alter table DWD_MR_OPER_CDR_20101223 activate not logged initially with empty table

INTERFACE_LEN_98012="1 20,21 22,23 36,37 46,47 70,71 94,95 124,125 130,131 138,139 140,141 164,165 178,179 192,193 202"


db2 "load client from /bassdb1/etl/L/mr/backup/splitfile/A9801220101223000000.AVL \
of asc modified by  timestampformat=\"YYYYMMDDHHMMSS\"  \
 method L (${INTERFACE_LEN_98012}) \
warningcount 1000 messages /bassdb1/etl/L/panzw2/logs/db2load.msg  \
insert into DWD_MR_OPER_CDR_20101223 nonrecoverable"

db2 \"load client from ${WORK_PATH}/${sfilename} of asc modified by timestampformat=\\\"YYYYMMDDHHMMSS\\\" method L (${len_val}) insert into ${table_name} nonrecoverable\""




SQL0181N  The string representation of a datetime value is out of range.  
SQLSTATE=22007




nawk   '{gsub(/\t/,"");gsub(/ * /," ");print }' ./backup/splitfile/A9801220101223000000.AVL \
|nawk -F" " '{ if( NF < 9 ) { print  FNR , $0 } }'


����������ʱ������
���������ݣ�����������·�ʱ���������ݵı�Ҫд��֧����֤�����õı���ڡ��������á�

�����������������ݡ��ɻ��ֳɡ���������ʱ

interface_file_name=A0504220101202000000

interface_file_name=$1

interface_type=`echo ${interface_file_name} |awk '{print substr($1,1,1)}'`
echo $interface_type

interface_code=`echo ${interface_file_name} |awk '{print substr($1,2,5)}'`
echo $interface_code

interface_datetime=`echo ${interface_file_name} |awk '{print substr($1,7,8)}'`
echo $interface_datetime


task_id=${interface_type}${interface_code}
echo ${task_id}

TABLE_NAME_TEMPLET=$2 
TASK_NAME=$3
LOAD_METHOD=$4
BOSS_TABLE_NAME=$5
sql="INSERT INTO etl_load_table_map(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) values(${task_id},${TABLE_NAME_TEMPLET},${TASK_NAME},${LOAD_METHOD},${BOSS_TABLE_NAME})"
echo ${sql}


db2look -d bassdb -e -i bass2 -w bass2 -z syscat -t tables


D:\XZBI\NG1-BASS2.0\�ӿ�\tcl>
svn ls -r {2010-12-30}

���µ�ָ���汾�������
svn update -r2229 >d:\svnupdate.txt


 ÿ����һ����Ŀ�����һ����Ϣ�����ַ���ʾ�����Ķ������京������:

   A  ������
   D  ��ɾ��
   U  �Ѹ���
   C  �г�ͻ
   G  �Ѻϲ�
   
�г���ĳ���汾��������Ŀ��
   D:\pzw\prj\testrev>svn ls -R > d:\svnls.txt
   


 db2 reorg table NGBASS20.ODS_SJ_WAP_GATEWAY_LOG_201011
 
 db2expln -d bassdb56 -u bass2 bass2 -q "select count(0) from NGBASS20.ODS_SJ_WAP_GATEWAY_LOG_201011" -t -g      


 db2expln -d bassdb56 -u bass2 bass2 -stmtfile  expln.sql -t -g      
 
"    
resset(){
unset i
unset res
unset rs_set
rs_set="puts \$f \"\$"
i=0
while [ $i -lt $1 ]
do 
if [ $i -lt 10 ];then 
res="result0$i"
else 
res="result$i"
fi
echo "    set $res [lindex \$p_row $i]"
rs_set=${rs_set}"{"${res}"}\\\$\$"
i=`expr $i + 1`
done 
echo ${rs_set}"\""
}

TODO:
--�ύchk1.0
--����I05046YYYYMMDDXXXXXX.AVL		
--��������mtest �����ӿڿ���ǰ�˵���
--�˲�����ģ��
�������нӿڵ���ʽ��:�޸�tcl����
--׼��֪ʶ���ĵ�
Ӧ�ò���ģ������
--��������ű�����ģ���

ģ������Ψһ��
�ӿ����ά��
���нӿ��س�


    
    


crtab(){
#��������
#1.ֻ��Ҫ�ṩģ�������ڣ�����Ҫָ����ռ䣬Ĭ��Ϊģ���ñ�ռ�
#2.����֮ǰҪ�Ƚ������ݿ�����
tablename=""
TABLE_NAME_TEMPLET=""
TBSPACE=""
INDEX_TBSPACE=""
PARTITIONKEY=""
TABLE_NAME_TEMPLET=$1
DT=$2

if [ $# -ne 2 ];then 
echo run script like :
echo crt.sh DWD_NG2_A02025_YYYYMMDD 20101231
fi

#TABLE_NAME_TEMPLET=DWD_NG2_A02025_YYYYMMDD
#DT=20101231

LEN=`echo $DT|awk '{print length($1)}'`


if [ $LEN -eq 8 ];then 
tablename=`ReplaceAllSubStr $TABLE_NAME_TEMPLET 'YYYYMMDD' $DT`
else 
 	if [ $LEN -eq 6 ];then 
	tablename=`ReplaceAllSubStr $TABLE_NAME_TEMPLET 'YYYYMM' $DT` 
	else 
	echo tablename not set!
	exit
	fi
fi


TBSPACE=""
db2 "select 'xxxxx',TBSPACE from syscat.tables \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' "|\
grep xxxxx|awk '{print $2}'|read TBSPACE

if [ -z $TBSPACE ];then 
echo TBSPACE not set!
exit
fi


INDEX_TBSPACE=""
db2 "select 'xxxxx',INDEX_TBSPACE from syscat.tables \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' "|\
grep xxxxx|awk '{print $2}'|read INDEX_TBSPACE

if [ -z $INDEX_TBSPACE ];then 
echo INDEX_TBSPACE not set!
exit
fi


PARTITIONKEY=""
db2 "select 'xxxxx', case when partkeyseq = 1 then  colname  else ','||colname end colname from syscat.columns \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' and partkeyseq >= 1 order by partkeyseq"|\
grep xxxxx|awk '{print $2}'|awk 'BEGIN{a=""}{for (i = 1; i <= NF; i++) a=a$1}END {print a}'|read PARTITIONKEY


if [ -z $PARTITIONKEY ];then 
echo PARTITIONKEY not set!
exit
fi

sql="create table ${tablename} like ${TABLE_NAME_TEMPLET} in ${TBSPACE} index in ${INDEX_TBSPACE} partitioning key ( ${PARTITIONKEY} ) using hashing not logged initially "
echo $sql
#db2 $sql
}



    
 ÿ����һ����Ŀ�����һ����Ϣ�����ַ���ʾ�����Ķ������京������:

   A  ������
   D  ��ɾ��
   U  �Ѹ���
   C  �г�ͻ
   G  �Ѻϲ�
   
D    �����ĵ�ģ��
A    01 �����ĵ�
A    02 �����ĵ�ģ��
A    02 �����ĵ�ģ��\������ģ��
A    02 �����ĵ�ģ��\������ģ��\ϵͳά�������ĵ�-��������-�������˵����.doc
A    02 �����ĵ�ģ��\������ģ��
A    02 �����ĵ�ģ��\������ģ��\ϵͳά�������ĵ�-xx����-�ۺϲ��Ա��棨ģ�壩.doc
A    02 �����ĵ�ģ��\������ģ��
A    02 �����ĵ�ģ��\������ģ��\ϵͳά�������ĵ�-��������-���ݿ����˵����.doc
A    02 �����ĵ�ģ��\������ģ��\ϵͳά�������ĵ�-����BLACKBERRYҵ��ͳ�ƿھ�-��Ԫ���Ա���(�ο��ĵ�).doc
A    02 �����ĵ�ģ��\������ģ��\ϵͳά�������ĵ�-��������-��ϸ���˵����.doc
A    02 �����ĵ�ģ��\������ģ��\ϵͳά�������ĵ�-С����ֵ����������-��ϸ���˵����(�ο��ĵ�) .doc
A    02 �����ĵ�ģ��\������ģ��\ϵͳά�������ĵ�-���ݼ���Ϣҵ���������-���ݿ����˵����(�ο��ĵ�).doc
A    02 �����ĵ�ģ��\������ģ��\ϵͳά�������ĵ�-��������-��Ԫ���Ա���.doc
A    03 ά��
A    03 ά��\���ؾ���ά����ű�.sql
���µ��汾 2229��

    
    
    select 'DW_ENTERPRISE_UNIPAY_DM_201005', nodenumber(ENTERPRISE_ID) ,count(*) as using_num from bass2.DW_ENTERPRISE_UNIPAY_DM_201007 group by nodenumber(ENTERPRISE_ID) order by using_num
    
    
db2look -d bassdb56 -e -i bass2 -w bass2 -z bass2 -t DW_PRODUCT_BUSI_SPROM_DM_201006


    
    
dw_product_busi_sprom_dm_201101
    
172.16.5.48

zhaolp2(����ƽ) 10:23:36
/db2home/db2inst1/script/check_bassdb
zhaolp2(����ƽ) 10:23:55
��bass2 bass2��¼��

��Ŀ����ѵ����\��Ŀ������\�������ɼ���������


getcpuidle(){
logfile=/db2backup/log/check_db/check_bassdb_day_2011011409.log
cat $logfile |\
awk '{if($0 ~ /kthr.*cpu/){a=NR};b=a+18;if((a != "") && (NR<=b))print $22}'
}


while true 
do
vmstat|tail -1|awk '{print $4/1024/1024}'
sleep 2
done 



AITOOLSPATH=/bassapp/tcl/aiomnivision/aitools
load "$env(AITOOLSPATH)/lib/libdb2tcl.so"
source "/bassapp/bass2/panzw2/aidb_db2.tcl"
    

set date [ clock seconds ]
set currentDate [ clock format $date -format "%Y%m%d" ]
set last_date             [GetLastDay [GetLastDay [GetLastDay [GetLastDay [GetLastDay $currentDate]]]]]





select card,tabschema,tabname from syscat.tables where  tabname like '%EXPLAIN%'

select * from EXPLAIN_ARGUMENT

select count(0) from ADMINISTRATOR.EMPMDC
select * from ADMINISTRATOR.EMPMDC




select * from act
select * from empmdc
select * from emp_act

select * from org
select * from product
select * from vphone
select * from vproj
select * from syscat.dbauth
select * from syscat.tbspaceauth
select * from syscat.tabauth
select * from syscat.packageauth
select * from syscat.procedures where procname='PROC1'
select * from syscat.indexes
select * from sysibm.sysdependencies
select * from syscat.packagedep

open 
http://172.16.5.71:4321/svn/%e9%a1%b9%e7%9b%ae%e7%bb%84%e5%9f%b9%e8%ae%ad%e8%b5%84%e6%96%99/%e9%a1%b9%e7%9b%ae%e7%bb%84%e8%b5%84%e6%96%99/%e5%ad%a6%e4%b9%a0%e8%b5%84%e6%96%99/

up:
http://172.16.5.71:4321/svn/%e9%a1%b9%e7%9b%ae%e7%bb%84%e5%9f%b9%e8%ae%ad%e8%b5%84%e6%96%99/%e9%a1%b9%e7%9b%ae%e7%bb%84%e8%b5%84%e6%96%99/



XZBI\��Ŀ����ѵ����\��Ŀ������\ѧϰ����

XZBI/��Ŀ����ѵ����/��Ŀ������/ѧϰ����


http://172.16.5.71:4321/svn/%e9%a1%b9%e7%9b%ae%e7%bb%84%e5%9f%b9%e8%ae%ad%e8%b5%84%e6%96%99/%e9%a1%b9%e7%9b%ae%e7%bb%84%e8%b5%84%e6%96%99/%e5%ad%a6%e4%b9%a0%e8%b5%84%e6%96%99/


 my_pass=bass2
 db2 "connect to bassdb user bass2 using ${my_pass}"
 
 
 select * from etl_task_running 
where	task_id	in	('I03013')	

select * from etl_task_log  
where	task_id	in	('I03013')	

 
	set ip_num ('17950','17951')
	
�ƶ�����
substr(product_no,1,3) in ('135','136','138','139','147','150','152','157','158','182','187','188')
and length(rtrim(ltrim(product_no)))=11

	

db2 => select * from DIM_FEETYPE_ITEM

ITEM_ID     ITEM_NAME                                                                                            FEETYPE_ID  FEETYPE_NAME                                                                                        
----------- ---------------------------------------------------------------------------------------------------- ----------- ----------------------------------------------------------------------------------------------------
SQL0668N  Operation not allowed for reason code "3" on table 
"BASS2.DIM_FEETYPE_ITEM".  SQLSTATE=57016
db2 => ? 57016

SQLSTATE 57016: The table cannot be accessed, because it is inactive.

	
	
	

       opp_noaccess_number  varchar(30),       --ȥIPͷ�ĶԶ˺���
       opp_roam_areacode    varchar(7),        --�Զ�����λ������
       opp_home_areacode    varchar(7),        --�Զ˹�����;����
       
svn://172.16.5.71/XZBI

XZBI\NG1-BASS1.0\Truck\05 ���߿�

E:\SVN\NG1-BASS1.0\Truck\03 ������\interface\��ȡ\doc
BOSS�ӿڹ滮v1.00.xls
E:\SVN\NG1-BASS1.0\Truck\03 ������\interface\����\doc
ODS����ű�.sql

������Ϊ��������ˡ������Ʒ+רҵ���񡱵ķ�չģʽ��

���������ƶ���ҵ�����������ܾ���������������13����Ա�����������廹������������š�
����������˵����������PSO(Professional Service Organization��רҵ�������)�ĵ�һ�ˡ�������֮ǰ���з�������ʱ����û��ר�ŵķ����Ÿ��ͻ��������е�����ȫ�����е��з��������������з�����Ա�����ֳ����������з����Ժ���������з��ǳ�������ͨ����PSO���ŵĴ��죬����ͬʱ�����˿ͻ����������Ͳ�Ʒ�з��ļ��жȡ�


NG1-BASS1.0/Truck/05 ���߿�


select * from syscat.tables where TABNAME like 'CDR_CALL%'

select * from cdr_call_20100621 fetch first 10 rows only


select * from USYS_TABLE_MAINTAIN order by 1



select substr(opp_noaccess_number,2,length(opp_home_areacode))
,opp_home_areacode
,rtrim(substr(opp_noaccess_number,length(opp_home_areacode)+2,20)) 
from cdr_call_20100621

--where substr(opp_noaccess_number,1,1)='0'
--and fetch first 10 rows only

="insert into etl_load_table_map values('"&A208&"','"&B208&"','"&C208&"',"&D208&",'"&E208&"')"

 drop table ${db_user}.cdr_call_dtl_$p_timestamp
 create table ${db_user}.cdr_call_dtl_$p_timestamp like ${db_user}.cdr_call_dtl_yyyymmdd in $tbs_voice 
               index in tbs_index partitioning key (user_id) using hashing not logged initially

 declare global temporary table session.t_cdr_call_dtl_1 like  ${db_user}.cdr_call_dtl_yyyymmdd partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp
               

select * from ETL_LOAD_TABLE_MAP 
select * from sch_control_task  
select * from USYS_TABLE_MAINTAIN                               
               
               
select count(0),TABSCHEMA from syscat.tables group by TABSCHEMA order by 1

db2 catalog tcpip node bass2 remote 172.16.9.25 server 50000
db2 catalog database bassdb as bass2 at node bass2 authentication server
db2 terminate


db2 catalog tcpip node bassdb remote 172.16.5.56 server 50000
db2 catalog database bassdb as bassdb at node bassdb authentication server
db2 terminate


--ע��ڵ�
catalog tcpip node CQCRM remote 10.191.113.132 server 50000
--ע�����ݿ�
catalog database CQCCDW at node CQCRM
--ɾ��ע��ڵ�
uncatalog node CQCRM
--ɾ��ע�����ݿ�
uncatalog database CQCCDW 



 drop table ${db_user}.cdr_call_dtl_$p_timestamp
 create table ${db_user}.cdr_call_dtl_$p_timestamp like ${db_user}.cdr_call_dtl_yyyymmdd in $tbs_voice 
               index in tbs_index partitioning key (user_id) using hashing not logged initially

 declare global temporary table session.t_cdr_call_dtl_1 like  ${db_user}.cdr_call_dtl_yyyymmdd partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp



 insert into session.t_cdr_call_dtl_1
               (cust_id         ,user_id      ,acct_id            ,product_no     ,opp_number        ,opp_regular_number,
                product_id      ,service_id   ,drtype_id          ,province_id    ,city_id           ,county_id         ,
                roam_province_id,roam_city_id ,roam_county_id     ,opp_city_id    ,opp_roam_city_id  ,brand_id          ,
                plan_id         ,bill_mark    ,ip_mark            ,netcall_mark   ,speserver_mark    ,calltype_id       ,
                tolltype_id     ,tolltype_id2 ,roamtype_id        ,callfwtype_id  ,opp_access_type_id,opposite_id       ,
                callmoment_id   ,rate_prod_id ,msc_id             ,lac_id         ,cell_id           ,imei              ,
                start_time      ,imsi         ,msrn               ,a_number       ,scp_id            ,out_trunkid       ,
                in_trunkid	     ,stop_cause   ,moc_id		         ,mtc_id 		    ,enterprise_id     ,service_type      ,
                service_code    ,bill_indicate,sp_rela_type       ,rating_flag    ,item_code1        ,item_code2        ,
                item_code3      ,item_code4   ,free_res_code1     ,free_res_code2 ,free_res_code3    ,original_file     ,
                usertype_id     ,opp_plan_id  ,opp_noaccess_number,video_type     ,mns_type          ,user_property     ,
                opp_property    ,call_refnum  ,fci_type           ,input_time     ,rating_res        ,free_res_val1     ,
                free_res_val2   ,free_res_val3,std_unit           ,toll_std_unit  ,ori_basic_charge  ,ori_toll_charge   ,
                ori_other_charge,addup_res    ,call_duration      ,call_duration_m,call_duration_s   ,bill_duration     ,
                basecall_fee    ,toll_fee     ,info_fee           ,other_fee      ,charge1           ,charge1_disc      ,
                charge2         ,charge2_disc ,charge3            ,charge3_disc   ,charge4           ,charge4_disc)
               select cust_id           ,user_id         ,acct_id            ,product_no     ,opp_number        ,
                      case when substr(opp_noaccess_number,1,1)='0' then 
                           case when substr(opp_noaccess_number,2,length(opp_home_areacode))=opp_home_areacode then rtrim(substr(opp_noaccess_number,length(opp_home_areacode)+2,20)) 
                                else opp_noaccess_number end
               		      else opp_noaccess_number end as opp_regular_number,
                      product_id        ,service_id      ,drtype_id          ,province_id        ,city_id           ,county_id         ,
                      roam_province_id  ,roam_city_id    ,roam_county_id     ,
                      opp_home_areacode as opp_city_id,
                      opp_roam_areacode as opp_roam_city_id,
                      brand_id          ,plan_id         ,
                      case when (coalesce(charge1,0)+coalesce(charge2,0)+coalesce(charge3,0)+coalesce(charge4,0))>0 then 1 else 0 end as bill_mark,
                      case when substr(opp_number,1,5) in $ip_num then 1 else 0 end as ip_mark,
               		 case when netcall_flag=11 then 1 else 0 end as netcall_mark,
               		 0 as speserver_mark,
               		 calltype_id       ,
               		 case when substr(opp_number,1,5) in '17950' then 120
               		      when tolltype_id=1  and calltype_id=0 and substr(opp_number,1,5) in '17951' then 101
               		      when tolltype_id=10 and calltype_id=0 and substr(opp_number,1,5) in '17951' then 102
               		      when tolltype_id=3  and calltype_id=0 and substr(opp_number,1,5) in '17951' then 103
               		      when tolltype_id=4  and calltype_id=0 and substr(opp_number,1,5) in '17951' then 104
               		      when tolltype_id=5  and calltype_id=0 and substr(opp_number,1,5) in '17951' then 105
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='91' and substr(opp_number,1,5) in '17951' then 106
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='977' and substr(opp_number,1,5) in '17951' then 107
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='1' and substr(opp_number,1,5) in '17951' then 108
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='81' and substr(opp_number,1,5) in '17951' then 109
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='82' and substr(opp_number,1,5) in '17951' then 110
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='61' and substr(opp_number,1,5) in '17951' then 111
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='44' and substr(opp_number,1,5) in '17951' then 112
               		      when tolltype_id=2  and calltype_id=0 and opp_home_areacode='33' and substr(opp_number,1,5) in '17951' then 113
               		      when tolltype_id=2  and calltype_id=0 and substr(opp_number,1,5) in '17951' then 999
               		      when tolltype_id=0 then 0 when tolltype_id=1 then 1 when tolltype_id=10 then 2
               		      when tolltype_id=3 then 3 when tolltype_id=4 then 4 when tolltype_id=5 then 5
               		      when tolltype_id=2 and opp_home_areacode='91' then 6
               		      when tolltype_id=2 and opp_home_areacode='977' then 7
               		      when tolltype_id=2 and opp_home_areacode='1' then 8
               		      when tolltype_id=2 and opp_home_areacode='81' then 9
               		      when tolltype_id=2 and opp_home_areacode='82' then 10
               		      when tolltype_id=2 and opp_home_areacode='61' then 11
               		      when tolltype_id=2 and opp_home_areacode='44' then 12
               		      when tolltype_id=2 and opp_home_areacode='33' then 13
               		      when tolltype_id=2 then 99
               		      else 0 end as tolltype_id,
               		 tolltype_id2      ,
               		 case when roamtype_id<>5 then roamtype_id
               		      when roamtype_id=5 and roam_province_id in ('853','852','886') then 5
               		      when roamtype_id=5 then 9
               		      else 0 end as roamtype_id,
               		 case when calltype_id=2 then 1 when calltype_id=3 then 2 else 0 end as callfwtype_id,
               		 opp_access_type_id,opposite_id,
                      case when substr(char(start_time),12,2)= '00' then 1  when substr(char(start_time),12,2)= '01' then 2
               		      when substr(char(start_time),12,2)= '02' then 3  when substr(char(start_time),12,2)= '03' then 4
               		      when substr(char(start_time),12,2)= '04' then 5  when substr(char(start_time),12,2)= '05' then 6
               		      when substr(char(start_time),12,2)= '06' then 7  when substr(char(start_time),12,2)= '07' then 8
               		      when substr(char(start_time),12,2)= '08' then 9  when substr(char(start_time),12,2)= '09' then 10
               		      when substr(char(start_time),12,2)= '10' then 11 when substr(char(start_time),12,2)= '11' then 12
               		      when substr(char(start_time),12,2)= '12' then 13 when substr(char(start_time),12,2)= '13' then 14
               		      when substr(char(start_time),12,2)= '14' then 15 when substr(char(start_time),12,2)= '15' then 16
               		      when substr(char(start_time),12,2)= '16' then 17 when substr(char(start_time),12,2)= '17' then 18
               		      when substr(char(start_time),12,2)= '18' then 19 when substr(char(start_time),12,2)= '19' then 20
               		      when substr(char(start_time),12,2)= '20' then 21 when substr(char(start_time),12,2)= '21' then 22
               		      when substr(char(start_time),12,2)= '22' then 23 when substr(char(start_time),12,2)= '23' then 24
               		      else 0 end as callmoment_id,
               		 rate_prod_id      ,msc_id       ,
               		 upper(lac_id) as lac_id,
		         	    upper(cell_id) as cell_id,
		         	    imei              ,start_time      ,imsi               ,msrn               ,a_number          ,scp_id            ,
		         	    out_trunkid       ,in_trunkid	   ,stop_cause         ,moc_id		       ,mtc_id 		     ,enterprise_id     ,
		         	    service_type      ,service_code    ,bill_indicate      ,sp_rela_type       ,rating_flag       ,item_code1        ,
		         	    item_code2        ,item_code3      ,item_code4         ,free_res_code1     ,free_res_code2    ,free_res_code3    ,
		         	    original_file     ,usertype_id     ,opp_plan_id        ,opp_noaccess_number,video_type        ,mns_type          ,
		         	    user_property     ,opp_property    ,call_refnum        ,fci_type           ,input_time        ,rating_res        ,
		         	    free_res_val1     ,free_res_val2   ,free_res_val3      ,std_unit           ,toll_std_unit     ,ori_basic_charge  ,
		         	    ori_toll_charge   ,ori_other_charge,addup_res          ,call_duration      ,
               		 case when call_duration>=0 then (call_duration+59)/60 else (call_duration-59)/60 end as call_duration_m,
               		 case when tolltype_id<>0 and roamtype_id not in (1,2,4,6,7,8) and call_duration>=0 then (call_duration+59)/60
               		      when tolltype_id in (2,3,4,5) and roamtype_id in (1,2,4,6,7,8) and call_duration>=0 then (call_duration+59)/60
               		      when tolltype_id<>0 and roamtype_id not in (1,2,4,6,7,8) and call_duration<0 then (call_duration-59)/60
               		      when tolltype_id in (2,3,4,5) and roamtype_id in (1,2,4,6,7,8) and call_duration<0 then (call_duration-59)/60
               		      else 0 end as call_duration_s,
               		 case when (coalesce(charge1,0)+coalesce(charge2,0)+coalesce(charge3,0)+coalesce(charge4,0))>0 then call_duration else 0 end as bill_duration,
               		 coalesce(charge1,0)/1000.00 as basecall_fee,
               		 coalesce(charge2,0)/1000.00 as toll_fee,
               		 coalesce(charge4,0)/1000.00 as info_fee,
               		 coalesce(charge3,0)/1000.00 as other_fee,
               		 coalesce(charge1,0)/1000.00 as charge1,
               		 coalesce(charge1_disc,0)/1000.00 as charge1_disc,
               		 coalesce(charge2,0)/1000.00 as charge2,
               		 coalesce(charge2_disc,0)/1000.00 as charge2_disc,
               		 coalesce(charge3,0)/1000.00 as charge3,
               		 coalesce(charge3_disc,0)/1000.00 as charge3_disc,
               		 coalesce(charge4,0)/1000.00 as charge4,
               		 coalesce(charge4_disc,0)/1000.00 as charge4_disc
               from ${db_user}.cdr_call_$p_timestamp
               where calltype_id in (0,1,2,3)


 alter table ${db_user}.cdr_call_dtl_$p_timestamp activate not logged initially


	
	
 insert into ${db_user}.cdr_call_dtl_$p_timestamp
               (cust_id         ,user_id      ,acct_id            ,product_no     ,opp_number        ,opp_regular_number,
                product_id      ,service_id   ,drtype_id          ,province_id    ,city_id           ,county_id         ,
                roam_province_id,roam_city_id ,roam_county_id     ,opp_city_id    ,opp_roam_city_id  ,brand_id          ,
                plan_id         ,bill_mark    ,ip_mark            ,netcall_mark   ,speserver_mark    ,calltype_id       ,
                tolltype_id     ,tolltype_id2 ,roamtype_id        ,callfwtype_id  ,opp_access_type_id,opposite_id       ,
                callmoment_id   ,rate_prod_id ,msc_id             ,lac_id         ,cell_id           ,imei              ,
                start_time      ,imsi         ,msrn               ,a_number       ,scp_id            ,out_trunkid       ,
                in_trunkid	     ,stop_cause   ,moc_id		         ,mtc_id 		    ,enterprise_id     ,service_type      ,
                service_code    ,bill_indicate,sp_rela_type       ,rating_flag    ,item_code1        ,item_code2        ,
                item_code3      ,item_code4   ,free_res_code1     ,free_res_code2 ,free_res_code3    ,original_file     ,
                usertype_id     ,opp_plan_id  ,opp_noaccess_number,video_type     ,mns_type          ,user_property     ,
                opp_property    ,call_refnum  ,fci_type           ,input_time     ,rating_res        ,free_res_val1     ,
                free_res_val2   ,free_res_val3,std_unit           ,toll_std_unit  ,ori_basic_charge  ,ori_toll_charge   ,
                ori_other_charge,addup_res    ,call_duration      ,call_duration_m,call_duration_s   ,bill_duration     ,
                basecall_fee    ,toll_fee     ,info_fee           ,other_fee      ,charge1           ,charge1_disc      ,
                charge2         ,charge2_disc ,charge3            ,charge3_disc   ,charge4           ,charge4_disc)
                select cust_id         ,user_id            ,acct_id            ,product_no        ,opp_number        ,opp_regular_number,
                       product_id      ,service_id         ,drtype_id          ,province_id       ,city_id           ,county_id         ,
                       roam_province_id,roam_city_id       ,roam_county_id     ,opp_city_id       ,opp_roam_city_id  ,brand_id          ,
                       plan_id         ,bill_mark          ,ip_mark            ,netcall_mark      ,                                     
                       case when opp_regular_number in ('110','119','1860','1861','120','122','13800138000','10084','10085','10086') then 1 else 0 end as speserver_mark,
                       calltype_id     ,                                                                                     
                       case when roamtype_id=0 and tolltype_id<>0 and calltype_id=1 then 0 else tolltype_id end as tolltype_id,
                       tolltype_id2    ,roamtype_id        ,callfwtype_id      ,opp_access_type_id,opposite_id       ,callmoment_id     ,
                       rate_prod_id    ,msc_id             ,lac_id             ,cell_id           ,imei              ,start_time        ,
                       imsi            ,msrn               ,a_number           ,scp_id            ,out_trunkid       ,in_trunkid	       ,
                       stop_cause      ,moc_id		        ,mtc_id 		       ,enterprise_id     ,service_type      ,service_code      ,
                       bill_indicate   ,sp_rela_type       ,rating_flag        ,item_code1        ,item_code2        ,item_code3        ,
                       item_code4      ,free_res_code1     ,free_res_code2     ,free_res_code3    ,original_file     ,usertype_id       ,
                       opp_plan_id     ,opp_noaccess_number,video_type         ,mns_type          ,user_property     ,opp_property    ,
                       call_refnum     ,fci_type           ,input_time         ,rating_res        ,free_res_val1     ,free_res_val2     ,
                       free_res_val3   ,std_unit           ,toll_std_unit      ,ori_basic_charge  ,ori_toll_charge   ,ori_other_charge  ,
                       addup_res       ,call_duration      ,call_duration_m    ,
                       case when roamtype_id=0 and tolltype_id<>0 and calltype_id=1 then 0 else call_duration_s end as call_duration_s,
                       bill_duration   ,basecall_fee       ,toll_fee           ,info_fee          ,other_fee         ,charge1           ,
                       charge1_disc    ,charge2            ,charge2_disc       ,charge3           ,charge3_disc      ,charge4           ,
                       charge4_disc
                from session.t_cdr_call_dtl_1


 create index ${db_user}.i_c_dtl_${p_timestamp}_1 on ${db_user}.cdr_call_dtl_${p_timestamp}(user_id)


 create index ${db_user}.i_c_dtl_${p_timestamp}_2 on ${db_user}.cdr_call_dtl_${p_timestamp}(opp_number)




db2 => select * from DIM_FEETYPE_ITEM

ITEM_ID     ITEM_NAME                                                                                            FEETYPE_ID  FEETYPE_NAME                                                                                        
----------- ---------------------------------------------------------------------------------------------------- ----------- ----------------------------------------------------------------------------------------------------
SQL0668N  Operation not allowed for reason code "3" on table 
"BASS2.DIM_FEETYPE_ITEM".  SQLSTATE=57016
db2 => ? 57016

SQLSTATE 57016: The table cannot be accessed, because it is inactive.

	
2010-10-19 17:19
workplace:

Windows IP Configuration



        Host Name . . . . . . . . . . . . : ailk-pso-100914

        Primary Dns Suffix  . . . . . . . : 

        Node Type . . . . . . . . . . . . : Unknown

        IP Routing Enabled. . . . . . . . : No

        WINS Proxy Enabled. . . . . . . . : No



Ethernet adapter ��������:



        Connection-specific DNS Suffix  . : 

        Description . . . . . . . . . . . : Broadcom 440x 10/100 Integrated Controller

        Physical Address. . . . . . . . . : 00-15-C5-7A-67-3F

        Dhcp Enabled. . . . . . . . . . . : Yes

        Autoconfiguration Enabled . . . . : Yes

        IP Address. . . . . . . . . . . . : 172.16.24.146

        Subnet Mask . . . . . . . . . . . : 255.255.255.128

        Default Gateway . . . . . . . . . : 172.16.24.250

        DHCP Server . . . . . . . . . . . : 172.16.24.250

        DNS Servers . . . . . . . . . . . : 211.139.73.34

                                            211.139.73.35

        Lease Obtained. . . . . . . . . . : 2010��10��13�� 8:55:24

        Lease Expires . . . . . . . . . . : 2010��10��13�� 16:55:24



Ethernet adapter ������������:



        Media State . . . . . . . . . . . : Media disconnected

        Description . . . . . . . . . . . : Intel(R) PRO/Wireless 3945ABG Network Connection

        Physical Address. . . . . . . . . : 00-1B-77-2C-D0-29


2010-10-19 17:20


gzproxy.asiainfo-linkage.com:8080
*.asiainfo-linkage.com,10.*,localhost,127.0.0.1

172.16.5.43/46  ifboss/ifboss
bash -o vi


C:\Documents and Settings\ailk0914>ftp 172.16.9.25
Connected to 172.16.9.25.
220 NF-TEST07 FTP server ready.
User (172.16.9.25:(none)): bass2
331 Password required for bass2.
Password:
230 User bass2 logged in.
ftp> cd /db2backup/mov





XZBI\NG1-BASS1.0\Truck\05 ���߿�

172.16.9.25 bass2 / bass2


% puts $env(AITOOLSPATH) 
/bassapp/tcl/aiomnivision/aitools


if [ $? -eq 0 ]  then
        #���Ӷ�app.sch_control_runlog��־�ĸ���add by xufr at 2009-1-6 21:25:03
        controlCode="BASS2_$script"
        DB2_SQLCOMM="db2 \"update app.sch_control_runlog set flag = 0 where control_code = '${controlCode}'\""
        DB2_SQL_EXEC()
        {
                date
                echo $DB2_SQLCOMM
            db2 terminate
            db2 "connect to bassdb user bass2 using ${my_pass}"
            eval $DB2_SQLCOMM
            db2 commit
            db2 connect reset
        }
        DB2_SQL_EXEC > /bassapp/bass2/tcl/DB2_SQL_EXEC.log
fi


tclsh $dssfile -s $script -d "bass2/${my_pass}@bassdb" -t $optime -p $timestamp -u 0 -v 0 


set script $arg(-s)
set dbstr $arg(-d)
set op_time $arg(-t)
set timestamp $arg(-p)
set ddh $arg(-u)
set rwh $arg(-v)

set pos1 [string first "/" $dbstr] 
set pos2 [string first "@" $dbstr] 

set username [string range $dbstr 0 [expr $pos1 - 1] ]
set passwd [string range $dbstr [expr $pos1 + 1] [expr $pos2 -1 ]]
set db [string range $dbstr [expr $pos2 + 1] end ]




 
db2 "load client from /bassapp/bass2/load/boss/I0301320101020000000.AVL of del modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 messages /bassapp/bass2/load/boss/messages/db2load.msg replace into DWD_NG2_I03013_20101020"
 
 
 load client from /bassapp/bass2/load/boss/I0301320101020000000.AVL of del  modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 messages /bassapp/bass2/load/boss/messages/db2load.msg replace into DWD_NG2_I03013_20101020
 
 
 load client from /bassapp/bass2/load/boss/I0301320101020000000.AVL of del  \
 modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" \
 fastparse anyorder warningcount 1000 messages /bassapp/bass2/load/boss/messages/db2load.msg 
 replace into DWD_NG2_I03013_20101020
 
 
 
-- 2010-10-18 15:16 PANZHIWEI ADD
CREATE TABLE ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_YYYYMMDD (
		,DATA_ID					BIGINT
		,BILL_ID					VARCHAR(120)
		,CUSTOMER_ORDER_ID			BIGINT
		,BUSIOPER_ORDER_ID			BIGINT
		,BUSINESS_ID				BIGINT
		,OFFER_ID					BIGINT
		,STATE						SMALLINT
		,REGION_ID					VARCHAR(6)
		,DONE_DATE					TIMESTAMP
		,CREATE_DATE				TIMESTAMP
		,CHANEL_ID					SMALLINT
		,START_DATE					TIMESTAMP
		,OP_ID						BIGINT
		,ORG_ID						BIGINT
		,EXT1						VARCHAR(400)
		,EXT2						VARCHAR(400)
		,EXT3						VARCHAR(400)
		,EXT4						VARCHAR(400)
		,EXT5						BIGINT
		,EXT6						BIGINT
		,EXT7						BIGINT
		,NOTES						VARCHAR(400)
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( DATA_ID )
 USING HASHING



SELECT DATA_ID , BILL_ID , CUSTOMER_ORDER_ID , BUSIOPER_ORDER_ID , BUSINESS_ID , OFFER_ID , STATE , REGION_ID , DONE_DATE , CREATE_DATE , CHANEL_ID , START_DATE , OP_ID , ORG_ID , EXT1 , EXT2 , EXT3 , EXT4 , EXT5 , EXT6 , EXT7 , NOTES FROM SO.ORD_DSMP_BAT_DATA_HIS where done_date>=to_date('$YYYYMMDD$000000','YYYYMMDDHH24MISS') AND done_date<=to_date('$YYYYMMDD$235959','YYYYMMDDHH24MISS') 


SELECT DATA_ID , BILL_ID , CUSTOMER_ORDER_ID , BUSIOPER_ORDER_ID , BUSINESS_ID , OFFER_ID , STATE , REGION_ID , to_char(DONE_DATE,'yyyymmddhh24miss')DONE_DATE , to_char(CREATE_DATE,'yyyymmddhh24miss') CREATE_DATE , CHANEL_ID , to_char(START_DATE,'yyyymmddhh24miss') START_DATE , OP_ID , ORG_ID , EXT1 , EXT2 , EXT3 , EXT4 , EXT5 , EXT6 , EXT7 , NOTES FROM SO.ORD_DSMP_BAT_DATA_HIS



select BATCH_DATA_DETAIL_ID	,BATCH_DATA_ID	,QUEUE_ID	,BUSINESS_ID	,CONFIG_ID	,BUSI_ORDER_ID	,BILL_ID	,FILE_CONTENT	,SUCC_FLAG	,ERROR_MSG	,ERROR_STACK	,DONE_CODE	, to_char(DONE_DATE,'YYYYMMDDHH24MISS') DONE_DATE	,  to_char(CREATE_DATE,'YYYYMMDDHH24MISS') CREATE_DATE	,  to_char(VALID_DATE,'YYYYMMDDHH24MISS')  VALID_DATE	,  to_char(EXPIRE_DATE ,'YYYYMMDDHH24MISS') EXPIRE_DATE	,OP_ID	,ORG_ID	,REGION_ID from so.ord_batch_data_detail_f_1008 where done_date>=to_date('$YYYYMMDD$000000','YYYYMMDDHH24MISS') AND done_date<=to_date('$YYYYMMDD$235959','YYYYMMDDHH24MISS') 

to_char(CREATE_DATE,'yyyymmddhh24miss')



��create table zjt_tables as 

����(select * from tables) definition only 


set db_user BASS2
set tbs_3h tbs_3h 

for {yyyymmdd = 20101001} {yyyymmdd < 20101031 } {} 
   set sqlbuf "create table ${db_user}.dw_product_regsp_ds like ${db_user}.dw_product_regsp_${yyyymmdd} in $tbs_3h
               index in tbs_index partitioning key (user_id) using hashing not logged initially"

create table ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101011 like ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_yyyymmdd in tbs_3h index in TBS_INDEX   PARTITIONING KEY ( BATCH_DATA_DETAIL_ID ) USING HASHING

-- 2010-10-18 16:41 PANZHIWEI ADD
CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_YYYYMMDD ( 
		 BATCH_DATA_DETAIL_ID		bigint		
		,BATCH_DATA_ID				bigint				
		,QUEUE_ID					varchar(20)				--	����ID
		,BUSINESS_ID				bigint					--	����ID
		,CONFIG_ID					bigint					--	����ID
		,BUSI_ORDER_ID				bigint					--	��������ID
		,BILL_ID					varchar(30)				--	�ֻ�����
		,FILE_CONTENT				VARCHAR(1024)			--	�ļ�����
		,SUCC_FLAG					smallint				--	�ɹ���־
		,ERROR_MSG					VARCHAR(1000)			--	������Ϣ
		,ERROR_STACK				VARCHAR(4000)			--	����ջ��Ϣ
		,DONE_CODE					bigint					--	��������
		,DONE_DATE					TIMESTAMP				--	������־
		,CREATE_DATE				TIMESTAMP				--	����ʱ��
		,VALID_DATE					TIMESTAMP				--	��Чʱ��
		,EXPIRE_DATE				TIMESTAMP				--	ʧЧʱ��
		,OP_ID						bigint					--	����ԱID
		,ORG_ID						bigint					--	����Ա��֯ID
		,REGION_ID					VARCHAR(6)				--	��������
 )
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( BATCH_DATA_DETAIL_ID )
 USING HASHING
 




Usage: ./crt.sh TABLE_NAME_TEMPLET TBLSPACE_NAME PARTITIONING_KEK YYYYMM
/bassapp/bass2/fuzl
crt.sh ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F TBS_3H BATCH_DATA_DETAIL_ID 201010
crt.sh ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F TBS_3H BATCH_DATA_DETAIL_ID 201007
crt.sh ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F TBS_3H BATCH_DATA_DETAIL_ID 201008 &


crt.sh ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS TBS_3H DATA_ID 201007 &
crt.sh ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS TBS_3H DATA_ID 201008 &
crt.sh ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS TBS_3H DATA_ID 201010 &




ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_YYYYMMDD




java ETLMain 20101103 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101104 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101105 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101106 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101107 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101108 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101109 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101110 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101111 taskList_tmp.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101112 taskList_tmp.properties
java ETLMain 20101113 taskList_tmp.properties
java ETLMain 20101114 taskList_tmp.properties
java ETLMain 20101115 taskList_tmp.properties
java ETLMain 20101116 taskList_tmp.properties
java ETLMain 20101117 taskList_tmp.properties
java ETLMain 20101118 taskList_tmp.properties
java ETLMain 20101119 taskList_tmp.properties
java ETLMain 20101120 taskList_tmp.properties
java ETLMain 20101121 taskList_tmp.properties
java ETLMain 20101122 taskList_tmp.properties
java ETLMain 20101123 taskList_tmp.properties
java ETLMain 20101124 taskList_tmp.properties
java ETLMain 20101125 taskList_tmp.properties
java ETLMain 20101126 taskList_tmp.properties
java ETLMain 20101127 taskList_tmp.properties
java ETLMain 20101128 taskList_tmp.properties
java ETLMain 20101129 taskList_tmp.properties
java ETLMain 20101130 taskList_tmp.properties
java ETLMain 20101131 taskList_tmp.properties
java ETLMain 20100801 taskList_tmp.properties
java ETLMain 20100802 taskList_tmp.properties
java ETLMain 20100803 taskList_tmp.properties
java ETLMain 20100804 taskList_tmp.properties
java ETLMain 20100805 taskList_tmp.properties
java ETLMain 20100806 taskList_tmp.properties
java ETLMain 20100807 taskList_tmp.properties
java ETLMain 20100808 taskList_tmp.properties
java ETLMain 20100809 taskList_tmp.properties
java ETLMain 20100810 taskList_tmp.properties
java ETLMain 20100811 taskList_tmp.properties
java ETLMain 20100812 taskList_tmp.properties
java ETLMain 20100813 taskList_tmp.properties
java ETLMain 20100814 taskList_tmp.properties
java ETLMain 20100815 taskList_tmp.properties
java ETLMain 20100816 taskList_tmp.properties
java ETLMain 20100817 taskList_tmp.properties
java ETLMain 20100818 taskList_tmp.properties
java ETLMain 20100819 taskList_tmp.properties
java ETLMain 20100820 taskList_tmp.properties
java ETLMain 20100821 taskList_tmp.properties
java ETLMain 20100822 taskList_tmp.properties
java ETLMain 20100823 taskList_tmp.properties
java ETLMain 20100824 taskList_tmp.properties
java ETLMain 20100825 taskList_tmp.properties
java ETLMain 20100826 taskList_tmp.properties
java ETLMain 20100827 taskList_tmp.properties
java ETLMain 20100828 taskList_tmp.properties
java ETLMain 20100829 taskList_tmp.properties
java ETLMain 20100830 taskList_tmp.properties
java ETLMain 20100831 taskList_tmp.properties
java ETLMain 20101001 taskList_tmp.properties
java ETLMain 20101002 taskList_tmp.properties
java ETLMain 20101003 taskList_tmp.properties
java ETLMain 20101004 taskList_tmp.properties
java ETLMain 20101005 taskList_tmp.properties
java ETLMain 20101006 taskList_tmp.properties
java ETLMain 20101007 taskList_tmp.properties
java ETLMain 20101008 taskList_tmp.properties
java ETLMain 20101009 taskList_tmp.properties
java ETLMain 20101010 taskList_tmp.properties
java ETLMain 20101011 taskList_tmp.properties
java ETLMain 20101012 taskList_tmp.properties
java ETLMain 20101013 taskList_tmp.properties
java ETLMain 20101014 taskList_tmp.properties
java ETLMain 20101015 taskList_tmp.properties
java ETLMain 20101016 taskList_tmp.properties
java ETLMain 20101017 taskList_tmp.properties
java ETLMain 20101018 taskList_tmp.properties





java ETLMain 	20100701	taskList_tmp.properties
java ETLMain 	20100702	taskList_tmp.properties
java ETLMain 	20100707	taskList_tmp.properties
java ETLMain 	20100710	taskList_tmp.properties
java ETLMain 	20100717	taskList_tmp.properties
java ETLMain 	20100719	taskList_tmp.properties
java ETLMain 	20100720	taskList_tmp.properties
java ETLMain 	20100721	taskList_tmp.properties
java ETLMain 	20100722	taskList_tmp.properties
java ETLMain 	20100728	taskList_tmp.properties
java ETLMain 	20100729	taskList_tmp.properties
java ETLMain 	20100730	taskList_tmp.properties
java ETLMain 	20100731	taskList_tmp.properties
java ETLMain 	20100802	taskList_tmp.properties
java ETLMain 	20100806	taskList_tmp.properties
java ETLMain 	20100817	taskList_tmp.properties
java ETLMain 	20100818	taskList_tmp.properties
java ETLMain 	20100819	taskList_tmp.properties
java ETLMain 	20100820	taskList_tmp.properties
java ETLMain 	20100824	taskList_tmp.properties
java ETLMain 	20100825	taskList_tmp.properties
java ETLMain 	20100826	taskList_tmp.properties
java ETLMain 	20100827	taskList_tmp.properties
java ETLMain 	20100828	taskList_tmp.properties
java ETLMain 	20100829	taskList_tmp.properties
java ETLMain 	20100830	taskList_tmp.properties
java ETLMain 	20100831	taskList_tmp.properties
java ETLMain 	20100901	taskList_tmp.properties
java ETLMain 	20100903	taskList_tmp.properties
java ETLMain 	20100910	taskList_tmp.properties
java ETLMain 	20100914	taskList_tmp.properties
java ETLMain 	20100916	taskList_tmp.properties
java ETLMain 	20100917	taskList_tmp.properties
java ETLMain 	20100925	taskList_tmp.properties
java ETLMain 	20100926	taskList_tmp.properties
java ETLMain 	20100927	taskList_tmp.properties
java ETLMain 	20100928	taskList_tmp.properties
java ETLMain 	20101002	taskList_tmp.properties
java ETLMain 	20101011	taskList_tmp.properties
java ETLMain 	20101012	taskList_tmp.properties




backup/M1103620101000000000.CHK
cat backup/M1103620101000000000.CHK
M1103620101000000000.AVL.Z$44251886$834999$20101000$20101102010239$ 
�ļ���$�ļ���С$��¼��$��������$����ʱ��


1.db2 ©���ؼ�¼



replace(replace(CONT_ADDR,chr(10)),chr(13))

replace(replace(replace(cust_name,chr(10)),chr(13)),'$','')




/**	2010/11/2 18:02	added by  panzhiwei		**/	
CREATE TABLE DWD_NG2_I03013_YYYYMMDD (					
        CHANNEL_ID          BIGINT              --������ʶ:
        ,CHANNEL_NAME        VARCHAR(100)        --��������:
        ,PARENT_CHANNEL_ID   INTEGER             --�ϼ�������ʶ:
        ,CHANNEL_LEVEL       INTEGER             --��������:�롰�������𡱽ӿڵ�Ԫ��Ӧ
        ,CHANNEL_DESC          VARCHAR(200)        --��������:
        ,START_DATE          TIMESTAMP           --��������:
        ,EXPIRE_DATE         TIMESTAMP           --��ֹ����:ȱʡ29991231
        ,REGION_CODE         VARCHAR(8)          --����������:�롰����08039���ӿڵ�Ԫ��Ӧ
        ,ORGANIZE_ID         BIGINT              --������λ��ʶ:�롰��λ���ӿڵ�Ԫ��Ӧ
        ,CHANNEL_TYPE        INTEGER             --�������:�롰�������͡��ӿڵ�Ԫ��Ӧ
        ,PROPERTY_SRC_TYPE   INTEGER             --������ҵ��̬:�������й�˾������������ҵ���������ޡ�ת��-���е�
        ,REG_FUND            BIGINT              --Ͷ�ʹ�ģ:
        ,OPEN_DATE           TIMESTAMP           --��ҵʱ��:
        ,RESPNSR_ID             INTEGER             --���������˱��:�롰���������ˡ��ӿڵ�Ԫ��Ӧ
        ,TEL_NUMBER          VARCHAR(20)         --�����칫�绰:
        ,FAX_NUMBER          VARCHAR(20)         --�������:
        ,POST_CODE           VARCHAR(20)            --�ʱ�:
        ,BUSI_BEGIN_TIME     VARCHAR(20)         --Ӫҵʱ��:
        ,CHANNEL_STYLE       INTEGER             --������Ӫ��ʽ:"0-�Խ���Ӫ���԰죩��1-�Խ���Ӫ����������2-������Ӫ������"
        ,CHANNEL_LEVEL       INTEGER             --�����Ǽ�:"0-һ�Ǽ���1-���Ǽ�"
        ,CHANNEL_USER_ID     INTEGER             --���������˱��:�롰���������ˡ��ӿڵ�Ԫ��Ӧ
        ,OWNER_TYPE          SMALLINT            --��ҵ���÷�ʽ:
        ,PWNER_PRICE         INTEGER             --��ҵ���ü۸�:
        ,FITMENT_PRICE       INTEGER             --װ��Ͷ��:
        ,TRANSFER_PRICE      INTEGER             --����Ͷ��:
        ,INVESTOR            VARCHAR(20)         --Ͷ������:
        ,LICENSE_NUMBER      VARCHAR(20)          --���̺�:
        ,INTERNET_MODE       INTEGER             --���뷽ʽ:"0-���£�1-2M���£�2-GPRS��3-CDS��4-����������5-�������ţ�6-��"
        ,CREATE_DATE         TIMESTAMP           --����ʱ��:
        ,CR_OP_ID            BIGINT              --����Ա��:
        ,OP_ID               BIGINT              --����Ա��:
        ,DONE_DATE           TIMESTAMP           --����ʱ��:
        ,CHANNEL_STATE       SMALLINT            --����״̬:"0-������1-Ԥ��Լ��2-ע��"
        ,NOTES               VARCHAR(200)            --��ע
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING


 ��̸���������Ż��н���Ϣ�������ײ���İ���绰��

 ������  18:09:11
									#����У���ļ�
						 	 		n="${task_id}${scycleid}000000.AVL"
									s=`ls -l tmp_${task_id}${scycleid}000000.AVL |awk -F" "  '{print $5}'`
									r=`wc -l tmp_${task_id}${scycleid}000000.AVL |awk -F" "  '{print $1}'`
									d="${scycleid}"
									t=`date '+%Y%m%d%H%M%S'`
									printf "%-40s%-20s%-20s%8s%14s\n"  $n $s $r $d $t > tmp_${task_id}${scycleid}000000.CHK

 
TASK_ID=I03013
MODULE=GG
RUNTYPE=R
DATECYCLE=D
DATEPARAM=$YYYYMMDD$
AREAPARAM=891,892,893,894,895,896,897
RUNSQL=select distinct 
a.CHANNEL_ID , \
a.CHANNEL_NAME , \
b.PARENT_CHANNEL_ID , \
a.CHANNEL_LEVEL , \
a.OTHER_INFO , \
TO_CHAR(a.OPEN_DATE, \'YYYYMMDDHH24MISS') START_DATE, \
TO_CHAR(a.EXPIRE_DATE, \'YYYYMMDDHH24MISS') , \
a.REGION_CODE , \
a.ORGANIZE_ID , \
a.CHANNEL_TYPE , \
a.PROPERTY_SRC_TYPE , \
c.REG_FUND , \
TO_CHAR(a.OPEN_DATE, \'YYYYMMDDHH24MISS') , \
d.USER_ID , \
a.TEL_NUMBER , \
a.FAX_NUMBER , \
a.POST_CODE , \
a.BUSI_BEGIN_TIME , \
a.CHANNEL_STYLE , \
a.CHANNEL_LEVEL , \
null CHANNEL_USER_ID , \
null OWNER_TYPE , \
null PWNER_PRICE , \
null FITMENT_PRICE , \
null  TRANSFER_PRICE , \
' ' INVESTOR , \
c.LICENSE_NUMBER , \
a.INTERNET_MODE , \
TO_CHAR(a.CREATE_DATE, \'YYYYMMDDHH24MISS') , \
a.OP_ID CR_OP_ID , \
a.OP_ID , \
TO_CHAR(a.DONE_DATE, \'YYYYMMDDHH24MISS') , \
a.CHANNEL_STATE , \
a.NOTES \
FROM channel.channel_info a \
left join channel.gj_channel_dept b on a.channel_id = b.channel_id  \
left join channel.agent_channel c   on a.channel_id = c.channel_id \
left join channel.CHANNEL_EQUIP_INFO d on a.channel_id = d.channel_d  


="FROM "&�ӿ��б�2!B213&"."&�ӿ��б�2!C213&SUBSTITUTE(SUBSTITUTE(UPPER(#REF!D3),"X","0$FEE_AREA$",1),"YYMM","$YYMM$",1)


©�� \ �ĵط��ᱨ "java.sql.SQLException: ORA-00936: missing expression" ����



/**	2010-11-5 15:21	added by  panzhiwei		**/		
CREATE TABLE DWD_NG2_M03023_YYYYMM (						
        CHANNEL_ID          BIGINT              --������ʶ
        ,STORE_AREA         INTEGER             --Ӫҵ�����
        ,GEOGRAPHY_PROPERTY INTEGER             --����λ������
        ,LONGITUDE          BIGINT                  --����
        ,LATITUDE           BIGINT                  --γ��
        ,COVER_BUSI_REGION_CODE VARCHAR(20)         --��������ҵ����
        ,COVER_radius       INTEGER             --����뾶
        ,POEPLE_NUM         BIGINT              --�˿ڸ���
        ,POPUL_DENSITY      INTEGER             --�˿��ܶ�
        ,POEPLE_NUM         BIGINT              --�ͻ�����
        ,EMPLOYEE_NUM       INTEGER             --��Ա��Ŀ
        ,SALES_NUM          INTEGER             --ӪҵԱ����
        ,EQUIP_NUM          INTEGER             --�ն���Ŀ
        ,EQUIP_STATUS       INTEGER             --�ն�״��
        ,CHANNEL_ADDRESS    VARCHAR(120)        --��ϸ��ַ
        ,plane_graph        INTEGER             --Ӫҵ��ƽ��ṹͼ
        ,BOARD_HEIGHT       INTEGER             --���и߶ȣ��ף�
        ,BOARD_WIDTH        INTEGER             --���п�ȣ��ף�
        ,WALL_HEIGHT        INTEGER             --����ǽ�߶�
        ,WALL_WIDTH         INTEGER             --����ǽ���
        ,CREATE_DATE        TIMESTAMP           --��������
        ,CREATE_OP_ID       BIGINT              --����Ա��
        ,DONE_DATE           TIMESTAMP              --��������
        ,OP_ID             BIGINT              --����Ա��
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING





select distinct   \
A.CHANNEL_ID ,  \
nvl(STORE_AREA,1644 + round(dbms_random.value(-200,200))) STORE_AREA ,  \
nvl(GEOGRAPHY_PROPERTY,round(dbms_random.value(0,8))+1) GEOGRAPHY_PROPERTY ,  \
 91+round(dbms_random.value(0,1),2) LONGITUDE ,  \
 29+round(dbms_random.value(0,1),2) LATITUDE ,  \
nvl(c.region_detail,100102) COVER_BUSI_REGION_CODE ,  \
round(dbms_random.value(0,10)) COVER_radius ,  \
round(dbms_random.value(0,10)) POEPLE_NUM ,  \
round(dbms_random.value(0,10)) POPUL_DENSITY ,  \
round(dbms_random.value(0,10)) CUST_NUM ,  \
round(dbms_random.value(1,15)) EMPLOYEE_NUM ,  \
round(dbms_random.value(1,15)) SALES_NUM ,  \
round(dbms_random.value(1,10)) EQUIP_NUM ,  \
round(dbms_random.value(1,4)) EQUIP_STATUS ,  \
A.CHANNEL_ADDRESS ,  \
round(dbms_random.value(0,1)) plane_graph ,  \
NVL(B.BOARD_HEIGHT,round(dbms_random.value(50,800))) BOARD_HEIGHT,  \
NVL(B.BOARD_WIDTH,round(dbms_random.value(300,1000))) BOARD_WIDTH ,  \
NVL(B.WALL_HEIGHT,round(dbms_random.value(60,500))) WALL_HEIGHT ,  \
NVL(B.WALL_HEIGHT,round(dbms_random.value(50,500))) WALL_WIDTH ,  \
NVL(TO_CHAR(B.DONE_DATE,'YYYYMMDDHH24MISS'),'19001231000000') CREATE_DATE ,  \
NVL(B.OP_ID,10000000)   CREATE_OP_ID ,  \
NVL(TO_CHAR(B.DONE_DATE,'YYYYMMDDHH24MISS'),'19001231000000') DONE_DATE ,  \
NVL(B.OP_ID,10000000) OP_ID \
from channel.channel_info a  \ 
left join channel.self_channel b on a.channel_id = b.channel_id \
left join (select distinct  channel_id ,region_detail from CHANNEL.GJ_AGENT_REGION ) c on a.channel_id = c.channel_id \
where a.channel_id is not null 




 ���Ż�ΰ(80589803)  18:01:55
���Ұ���Ҫ��������������Ͻ��ܱ����������������ӳٵ���һ�ύ
���������Ҫ���ܴ�ҵĹ����ύ��˾�ܱ����������������ܼ����� 


����ƽ(2651779)  18:03:58
15847676066���ڵĺ�


BU
����PAϵͳ

A11019	SO	ORD_CUST_F	X_YYMM	�û�	�ͻ�����	PRODUCT	��	ODS_PRODUCT_ORD_CUST_YYYYMMDD	172.16.5.43	0020	2010-4-28����������	


 ������  14:03:52
so.ord_cust_f_0891_1010
������  14:08:57
NG1-BASS1.0\Truck\03 ������\interface\��ȡ\doc


1.dim_pub_city ����ά��


nfjd1234


��д�ĵ���ģ�壺F:\XZBI\NG1-BASS1.0\Truck\04 ģ���\

�����־
          
          
1. �˽���ʷ�ӿڡ�




E:\SVN\NG1-BASS2.0\�ĵ�\���ž��ֹ淶\NG1-BASS2.0�淶(������)\�����淶\�ⲿ�ӿڷֲ�



	�й��ƶ�ʡ��NG1-BASS�����淶�ⲿ�ӿڷֲ�v2.0(������).doc
	
dblink 
	
	
	����:SYS_STAFF ����:Ա�� 
 
'

				
/**	2010-11-8 10:51	added by  panzhiwei		**/
--DROP TABLED WD_NG2_I03027_YYYYMMDD
CREATE TABLE DWD_NG2_I03027_YYYYMMDD (				
        OPER_ID             BIGINT              		--Ա����ʶ
        ,ASSESS_CODE        VARCHAR(20)        		  --��Ա����ҵ�����ͱ�ʶ
        ,ASSESS_SCORE       INTEGER                 --����
        ,DONE_DATE          TIMESTAMP               --ʱ��
        ,OPASSESS_DTL_ID	  BIGINT									--������ϸ���
 )		
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( OPER_ID )				
 USING HASHING				


db2 "load client from /bassapp/bass2/load/boss/I0302720101020000000.AVL of del \
 modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 \
 messages /bassapp/bass2/load/boss/messages/db2load.msg  replace into DWD_NG2_I03027_20101020
 "
 


grep -in I0302720101020000000.AVL.Z nohup.out 
 
startline=`grep -in I0302720101020000000.AVL.Z nohup.out | tail -3|head -1|awk -F: '{print $1}'`

endline=`grep -in I0302720101020000000.AVL.Z nohup.out | tail -1|awk -F: '{print $1}'`

 
awk -v STARTLN=$startline '{
           if(NR>= STARTLN && NR <= ENDLN)
               { print "========"NR,$0}
       }' \
nohup.out




awk '{
if(NR>=455682&& NR <= 455750)
{ print "========"NR,$0}
}' \
/bassapp/bass2/load/boss/nohup.out



awk -v STARTLN=455730  '{
           if(NR>= STARTLN && NR <= 455750)
               { print "========"NR,$0}
       }' /bassapp/bass2/load/boss/nohup.out 
       
       


BASE=/bassapp/bass2/load/boss
grep -in I0302720101020000000.AVL.Z $BASE/nohup.out
startline=`grep -in I0302720101020000000.AVL.Z $BASE/nohup.out | tail -3|head -1|awk -F: '{print $1}'`
endline=`grep -in I0302720101020000000.AVL.Z $BASE/nohup.out | tail -1|awk -F: '{print $1}'`
sed -n "${startline},${endline}p" $BASE/nohup.out




BASE=/bassapp/bass2/load/boss
grep -in I0302720101020000000.AVL.Z $BASE/nohup.out
startline=`grep -in I0302720101020000000.AVL.Z $BASE/nohup.out | tail -3|head -1|awk -F: '{print $1}'`
endline=`grep -in I0302720101020000000.AVL.Z $BASE/nohup.out | tail -1|awk -F: '{print $1}'`

awk  -V STARTLN=${startline} -V ENDLN=${endline} '{ if(NR>= STARTLN && NR <= ENDLN) { print "========"NR,$0} }'   $BASE/nohup.out


awk -V a=1  '{print a}' 



nawk 	 -v STARTLN=${startline}  \
			 -v ENDLN=${endline}  \
			  '{ if( NR>= STARTLN && NR <= ENDLN )
               { print "========"NR,$0}
       }'  \
$BASE/nohup.out



sed -n "${startline},${endline}p" $BASE/nohup.out


BU
����PAϵͳ

A11019	SO	ORD_CUST_F	X_YYMM	�û�	�ͻ�����	PRODUCT	��	ODS_PRODUCT_ORD_CUST_YYYYMMDD	172.16.5.43	0020	2010-4-28����������	


 ������  14:03:52
so.ord_cust_f_0891_1010
������  14:08:57
NG1-BASS1.0\Truck\03 ������\interface\��ȡ\doc


1.dim_pub_city ����ά��


nfjd1234


��д�ĵ���ģ�壺F:\XZBI\NG1-BASS1.0\Truck\04 ģ���\

�����־
          
          
1. �˽���ʷ�ӿڡ�




E:\SVN\NG1-BASS2.0\�ĵ�\���ž��ֹ淶\NG1-BASS2.0�淶(������)\�����淶\�ⲿ�ӿڷֲ�



	�й��ƶ�ʡ��NG1-BASS�����淶�ⲿ�ӿڷֲ�v2.0(������).doc
	
dblink 
	
	
	����:SYS_STAFF ����:Ա�� 
 

SCHEME_CHANNEL_ID	NUMBER(12)	--	�������
LONGITUDE	NUMBER(12)	--	����
LATITUDE	NUMBER(12)	--	γ��
PROPERTY_SRC_TYPE	NUMBER(2)	--	��ҵ��Դ��
SEAT_NUM	NUMBER(6)	--	�����ϯ��Ŀ
USED_SEAT_NUM	NUMBER(6)	--	������ϯ��Ŀ
CHANNEL_SITE	NUMBER(8)	--	���賡��
OWNER_TYPE	NUMBER(2)	--	��ҵ���÷�ʽ
PWNER_PRICE	NUMBER(10)	--	��ҵ���ü۸�
SITE_TYPE	NUMBER(2)	--	�÷�����
HOUSE_EXPIRE	DATE	--	�ⷿ����
HIRE_COST	NUMBER(12)	--	�����Թ����ɱ���̯
FITMENT_PRICE	NUMBER(10)	--	װ��Ͷ��
TRANSFER_PRICE	NUMBER(10)	--	����Ͷ��
OTHER_PRICE	NUMBER(10)	--	����Ͷ��
MANUAL_COST	NUMBER(12)	--	�˹��ɱ�
OTHER_COST	NUMBER(12)	--	�����ɱ�
EQUIP_DEPRE	NUMBER(12)	--	�豸�۾ɷ���
FURNITURE_COST	NUMBER(12)	--	�Ҿ��۾ɷ���
FITMENT_COST	NUMBER(12)	--	װ���۾ɷ���
DOOR_TYPE	NUMBER(2)	--	��ͷ��ʽ
STORE_AREA	NUMBER(8)	--	�������
BUILD_AREA	NUMBER(10)	--	�������
DOOR_AREA	NUMBER(10)	--	�Ŷ����
USE_AREA	NUMBER(10)	--	ʹ�����
WINDOW_AREA	NUMBER(8)	--	�ٽֳ������
BUSI_AREA	NUMBER(10)	--	ҵ�����������
NEWBUSI_USE_AREA	NUMBER(10)	--	��ҵ�����������
TERMINAL_SALE_AREA	NUMBER(10)	--	�ն����������
AD_AREA	NUMBER(10)	--	�����������
OTHER_AREA	NUMBER(10)	--	�����������
FITMENT_EXPIRE	NUMBER(4)	--	װ��Ͷ���۾�����
FURNITURE_EXPIRE	NUMBER(4)	--	�칫�Ҿ��۾�����
FITMENT_VALID	DATE	--	װ��Ͷ����Чʱ��
FURNITURE_VALID	DATE	--	�칫�Ҿ���Чʱ��
WARM_FEE	NUMBER(10)	--	ȡů��
DOOR_COST	NUMBER(10)	--	��ͷ�����������
WINDOW_COST	NUMBER(10)	--	�ٽֳ��������������
WATER_COST	NUMBER(12)	--	ˮ��
ELEC_COST	NUMBER(12)	--	���
OFFICE_COST	NUMBER(12)	--	�칫��Ʒ���Ĳ�
COMM_COST	NUMBER(12)	--	��ͨѶ��
UNIT_PRICE1	NUMBER(12)	--	ͬ�ضλ����г���λ�۸�1
REBATE1	NUMBER(12)	--	�ۿ�ϵ��1
UNIT_PRICE2	NUMBER(12)	--	ͬ�ضλ��⿴�嵥λ�۸�2
REBATE2	NUMBER(12)	--	�ۿ�ϵ��2
UNIT_PRICE3	NUMBER(12)	--	ͬ�ضλ�����������λ�۸�3
REBATE3	NUMBER(12)	--	�ۿ�ϵ��3
DONE_CODE	NUMBER(12)	--	������ˮ
DONE_DATE	DATE	--	��������
OP_ID	NUMBER(12)	--	����Ա
ORG_ID	NUMBER(12)	--	������֯��Ԫ
NOTES	VARCHAR2(255)	--	��ע
STATE	NUMBER(1)	--	��¼״̬
BUY_EQUIP_SUM	NUMBER(10)	--	BUY_EQUIP_SUM
NOTA_AREA	NUMBER(8)	--	��ʾ��λ�ף��������ݿ�������
OFFICE_FIT_SUM	NUMBER(10)	--	OFFICE_FIT_SUM



					
/**	2010-11-9 11:27	added by  panzhiwei		**/	
--DROP TABLE DWD_NG2_I03028_YYYYMMDD					
CREATE TABLE DWD_NG2_I03028_YYYYMMDD (					
        CHANNEL_ID          BIGINT              --��������
        ,USED_SEAT_NUM      INTEGER             --̨ϯ����
        ,EMPLOYEE_NUM       BIGINT              --Ա������
        ,BUILD_AREA         INTEGER             --�������
        ,USE_AREA           INTEGER             --ʹ�����
        ,AD_AREA            INTEGER             --�����������
        ,EQUIP_NUM          BIGINT                --�豸��
        ,LEGAL_PERSON       VARCHAR(20)           --��ϵ��
        ,TEL_NUMBER         VARCHAR(20)         --��ϵ�绰
        ,BUSI_TYPE          VARCHAR(20)         --����ҵ������
        ,USAGE_TYPE         VARCHAR(20)         --���ܽ綨
        ,HOUSE_EXPIRE       TIMESTAMP           --�ⷿ����
        ,HOUSE_BUY_PRICE    BIGINT              --���ݹ�����
        ,HIRE_COST          BIGINT              --�������
        ,BUY_EQUIP_SUM      INTEGER             --�豸������
        ,EQUIP_RENT_PRICE   BIGINT              --�豸���
        ,FITMENT_PRICE      INTEGER             --װ��Ͷ���ܶ�
        ,OFFICE_COST        BIGINT              --�칫�Ҿ�Ͷ���ܶ�
        ,FITMENT_EXPIRE     SMALLINT            --װ��Ͷ���۾�����
        ,FURNITURE_EXPIRE   SMALLINT            --�칫�Ҿ��۾�����
        ,FITMENT_VALID      TIMESTAMP           --װ��Ͷ����Чʱ��
        ,FURNITURE_VALID    TIMESTAMP           --�칫�Ҿ���Чʱ��
        ,WATER_COST         BIGINT                  --ˮ��
        ,WARM_FEE           INTEGER               --ȡů��
        ,ELEC_COST          BIGINT                  --���
        ,MANUAL_COST        BIGINT              --�˹��ɱ��ܶ�
        ,OTHER_COST         BIGINT              --�����ճ������ܶ�
        ,BUSI_CONSULT_RATE  BIGINT              --��ѯҵ����ռ��ҵ������ֵ
        ,BUSI_COMPLAINT_RATE BIGINT              --Ͷ��ҵ����ռ��ҵ������ֵ
        ,DOOR_HEIGHT        BIGINT              --��ͷ�߶�
        ,DOOR_WIDTH         BIGINT              --��ͷ����
        ,WINDOW_AREA        INTEGER             --�ٽֳ������
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING



select   
a.CHANNEL_ID ,  
round(dbms_random.value(1,10))  USED_SEAT_NUM ,  
round(dbms_random.value(5,15)) EMPLOYEE_NUM ,  
nvl(STORE_AREA,1644 + round(dbms_random.value(-200,200))) BUILD_AREA ,  
nvl(STORE_AREA,1644 + round(dbms_random.value(-200,200))) USE_AREA ,  
nvl(STORE_AREA,1644 + round(dbms_random.value(-200,200))) AD_AREA ,  
round(dbms_random.value(1,10)) EQUIP_NUM ,  
nvl(c.LEGAL_PERSON,a.CHANNEL_NAME) LEGAL_PERSON ,  
a.TEL_NUMBER ,  
round(dbms_random.value(1,8)) BUSI_TYPE ,  
round(dbms_random.value(1,6)) USAGE_TYPE ,  
nvl(TO_CHAR(a.EXPIRE_DATE,'YYYYMMDDHH24MISS'),'20991231235959') HOUSE_EXPIRE ,  
round(dbms_random.value(100000,1000000)) HOUSE_BUY_PRICE ,  
round(dbms_random.value(2000,10000)) HIRE_COST ,  
round(dbms_random.value(10000,100000)) BUY_EQUIP_PRICE ,  
round(dbms_random.value(200,1000)) EQUIP_RENT_PRICE ,  
round(dbms_random.value(1000,10000)) FITMENT_PRICE ,  
round(dbms_random.value(1000,10000)) OFFICE_COST ,  
nvl(TO_CHAR(a.EXPIRE_DATE,'YYYYMMDDHH24MISS'),'20991231235959') FITMENT_EXPIRE ,  
nvl(TO_CHAR(a.EXPIRE_DATE,'YYYYMMDDHH24MISS'),'20991231235959') FURNITURE_EXPIRE ,  
nvl(TO_CHAR(a.create_DATE+round(dbms_random.value(-360,0)),'YYYYMMDDHH24MISS'),'20991231235959') FITMENT_VALID ,  
nvl(TO_CHAR(a.create_DATE+round(dbms_random.value(-360,0)),'YYYYMMDDHH24MISS'),'20991231235959') FURNITURE_VALID ,  
round(dbms_random.value(100,500)) WATER_COST ,  
round(dbms_random.value(100,500)) WARM_FEE ,  
round(dbms_random.value(100,500)) ELEC_COST ,  
round(dbms_random.value(10000,50000)) MANUAL_COST ,  
round(dbms_random.value(10000,20000)) OTHER_COST ,  
round(dbms_random.value(30,50))/100 BUSI_CONSULT_RATE ,  
round(dbms_random.value(1,15))/100 BUSI_COMPLAINT_RATE ,  
round(dbms_random.value(200,300)) DOOR_HEIGHT ,  
round(dbms_random.value(100,200)) DOOR_WIDTH ,  
round(dbms_random.value(3,9)) WINDOW_AREA  
FROM channel.channel_info a   
left join channel.gj_channel_dept b on a.channel_id = b.channel_id    
left join channel.agent_channel c   on a.channel_id = c.channel_id   
left join channel.CHANNEL_EQUIP_INFO d on a.channel_id = d.CHANNEL_ID  
where TO_CHAR(a.OPEN_DATE,'YYYYMMDDHH24MISS') < nvl(TO_CHAR(a.EXPIRE_DATE,'YYYYMMDDHH24MISS'),'20991231000000')




db2 "load client from /bassapp/bass2/load/boss/I0302820101020000000.AVL \
of del modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder \
warningcount 1000 messages /bassapp/bass2/load/boss/messages/db2load.msg  \
replace into DWD_NG2_I03028_20101020"




3.2.2.	����ʵ�ַ�ʽ
  ��ǰ�����⼸�ű����� ��dw_newbusi_gprs_201010 �������ȡ�����ݣ�
 ��dw_newbusi_gprs_201010��������Ǵ�BOSS��xzjf.dr_ggprs_l_20101104 ��ȡ�� ������cell_id �� lac_idΪ�ա�

���������⼸�ű�������Ŀ����ģ�����Ҫȡ����ȷ�� cell_id �� lac_id
����Ҫ����һ���ӿ���ר��ʵ�֡���BOSS��xzjf.DR_GGPRS_GXZ_20101104 �������ȡ��
��ע�⣬���ֻ���������Ŀ����ļ��ű��������������޸ģ�






/**	2010-11-9 17:52	added by  panzhiwei	**/
--DROP TABLE ODS_CDR_GPRS_GXZ_YYYYMMDD		
CREATE TABLE ODS_CDR_GPRS_GXZ_YYYYMMDD (		
         DR_TYPE            INTEGER                     --
        ,SERVICE_ID         INTEGER                     --
        ,BILL_MONTH         VARCHAR(8)                  --
        ,USER_NUMBER        VARCHAR(15)                 --
        ,VPLMN1             VARCHAR(7)                  --
        ,START_TIME         TIMESTAMP                   --
        ,DURATION           INTEGER                     --
        ,LAC_ID             VARCHAR(8)                  --
        ,CELL_ID            VARCHAR(16)                 --
        ,APN_NI             VARCHAR(64)                 --
        ,DATA_FLOW_UP1      INTEGER                     --
        ,DATA_FLOW_DOWN1    INTEGER                     --
        ,DURATION1          INTEGER                     --
        ,DATA_FLOW_UP2      INTEGER                     --
        ,DATA_FLOW_DOWN2    INTEGER                     --
        ,DURATION2          INTEGER                     --
        ,DURATION3          INTEGER                     --
        ,MNS_TYPE           SMALLINT                    --
        ,INPUT_TIME         TIMESTAMP                   --
 )		
  DATA CAPTURE NONE		
 IN TBS_CDR_VOICE		
 INDEX IN TBS_INDEX		
  PARTITIONING KEY		
   ( USER_ID )		
 USING HASHING		
		
		
		
		
		--DROP TABLE ODS_CDR_GPRS_GXZ_YYYYMMDD	
CREATE TABLE ODS_CDR_GPRS_GXZ_YYYYMMDD (		
         DR_TYPE            INTEGER                     --
        ,SERVICE_ID         INTEGER                     --
        ,BILL_MONTH         VARCHAR(8)                  --
        ,USER_NUMBER        VARCHAR(15)                 --
        ,VPLMN1             VARCHAR(7)                  --
        ,START_TIME         TIMESTAMP                   --
        ,DURATION           INTEGER                     --
        ,LAC_ID             VARCHAR(8)                  --
        ,CELL_ID            VARCHAR(16)                 --
        ,APN_NI             VARCHAR(64)                 --
        ,DATA_FLOW_UP1      INTEGER                     --
        ,DATA_FLOW_DOWN1    INTEGER                     --
        ,DURATION1          INTEGER                     --
        ,DATA_FLOW_UP2      INTEGER                     --
        ,DATA_FLOW_DOWN2    INTEGER                     --
        ,DURATION2          INTEGER                     --
        ,DURATION3          INTEGER                     --
        ,MNS_TYPE           SMALLINT                    --
        ,INPUT_TIME         TIMESTAMP                   --
 )		
  DATA CAPTURE NONE		
 IN TBS_CDR_VOICE		
 INDEX IN TBS_INDEX		
  PARTITIONING KEY		
   ( USER_NUMBER,START_TIME )		
 USING HASHING		



db2 "load client from /bassdb2/etl/L/boss/A0502120101104000000.AVL of del modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 messages /dev/null replace into ODS_CDR_GPRS_GXZ_20101104"


FTP::172.16.5.46b\
/bassdb2/etl/E/boss/java/crm_interface/bin/config/BOSS/A05021.properties

java ETLMain 20101105 taskList_tmp_pzw.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101106 taskList_tmp_pzw.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101107 taskList_tmp_pzw.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101108 taskList_tmp_pzw.properties >> pzw_tmpExtra.out 2>&1 &
java ETLMain 20101109 taskList_tmp_pzw.properties >> pzw_tmpExtra.out 2>&1 &



java ETLMain 20101103 taskList_tmp_pzw.properties

����ƽ(2651779)  15:37:25
2010-11-12:    ����ƽ    15002020810
2010-11-26:    ������    13631429212
2010-12-13:    ���    13560241290




DWD_NG2_I03032_20101020

DWD_NG2_I03034_20101020
DWD_NG2_I03036_20101020

DWD_NG2_I03037_20101020

select * from DWD_NG2_I03027_20101020
select * from DWD_NG2_I03032_20101020
select * from DWD_NG2_I03034_20101020
select * from DWD_NG2_I03036_20101020
select * from DWD_NG2_I03037_20101020

----------------------------------------------------------------------------------------------
Dwd_cust_manager_info_ds.tcl        Dwd_enterprise_manager_rela_ds.tcl
D:\XZBI\NG1-BASS1.0\Truck\03 ������\interface

ԭ��ʵ�ַ�ʽ:

insert into $objectTable
	(
		manager_id       
		,manager_type_id  
		,manager_status_id
		,dept_id          
		,member_type_id   
		,exam_role_id     
		,rec_status       
		,province_id      
		,city_id          
		,region_type_id   
		,region_detail    
		,manger_name      
		,gender           
		,manager_iden_id  
		,education_id     
		,email            
		,bill_id          
		,office_phone     
		,home_phone       
		,home_address     
		,postcode         
		,job_name         
		,job_desc         
		,manager_begin    
		,done_date        
		,valid_date       
		,expire_date      
		,op_id            
		,channel_id       
		,so_nbr           
	)
	select
		PARTY_ROLE_ID
		,manager_type
		,manager_status
		,dept_id
		,member_type
		,exam_role_id
		,state
		,char(province_id)
		,substr(city_id,2)
		,int(region_type)
		,region_detail
		,manager_name
		,gender
		,politics_face
		,education_level
		,email
		,bill_id
		,office_tel
		,home_tel
		,contact_address
		,postcode
		,job_position
		,job_company
		,begin_date
		,create_date
		,valid_date
		,expire_date
		,op_id
		,org_id
		,done_code
	from 
		ods_ent_en_staff_$p_timestamp
		
		
SELECT DISTINCT STAFF.STAFF_ID as manager_id,
                STAFF_NAME AS manager_NAME,
                OPERATOR_ID op_id, 
                OP.NAME AS OPREATOR_NAME, 
                ORG.ORGANIZE_ID AS ORG_ID,
                ORG.ORGANIZE_NAME AS ORG_NAME, 
                STAFF.EMAIL,
                STAFF.BILL_ID, 
                STAFF.NOTES,
                STAFF.STATE, 
                base.MANAGER_TYPE,
                base.MANAGER_STATUS,
                base.MEMBER_TYPE,
                base.EXAM_ROLE_ID,
				base.region_type, 
                base.REGION_DETAIL ,
				base.dept_id
  FROM ODS_DIM_SYS_STAFF_20101105           STAFF
   LEFT JOIN ODS_ENT_EN_STAFF_20101105 base ON STAFF.STAFF_ID =
                                             BASE.PARTY_ROLE_ID,
       ODS_DIM_SYS_OPERATOR_20101105        OP,
       ODS_DIM_SYS_ORGANIZE_20101105        ORG,
       ODS_DIM_SYS_STAFF_ORG_RELAT_20101105 RELA,
       ODS_ENT_CM_GROUP_CUSTOMER_20101105   GG
 
 WHERE OP.STAFF_ID = STAFF.STAFF_ID
   AND STAFF.STAFF_ID = RELA.STAFF_ID
   AND RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
   AND RELA.IS_BASE_ORG = 'Y'
   AND OP.STATE = 1
   AND STAFF.STATE = 1
   AND RELA.STATE = 1
   AND ORG.STATE = 1
      -- and to_number(op.code) ='100005819902'  
   AND TRIM(CHAR(GG.MGR_ID)) = OP.CODE WITH UR
   



3.2.2.	����ʵ�ַ�ʽ
��ο������ṩ�ھ�����ɳ���ĸ��졣
-- Dwd_cust_manager_info_ds.tcl ���Ŀھ�
SELECT DISTINCT STAFF.STAFF_ID as manager_id,
                STAFF_NAME AS manager_NAME,
                OPERATOR_ID,
                OP.CODE AS CODE,
                OP.NAME AS OPREATOR_NAME,
                ORG.ORGANIZE_ID AS BASE_ORG_ID,
                ORG.ORGANIZE_NAME AS BASE_ORG_NAME,
                OP.MULTI_LOGIN_FLAG,
                ALLOW_CHANGE_PASSWORD,
                STAFF.EMAIL,
                STAFF.BILL_ID,
                ORG.DISTRICT_ID,
                STAFF.NOTES,
                STAFF.STATE,
                RELA.IS_ADMIN_STAFF,
                IS_BASE_ORG,
                base.MANAGER_TYPE,
                base.MANAGER_STATUS,
                base.MEMBER_TYPE,
                base.EXAM_ROLE_ID,
				base.region_type, 
                base.REGION_DETAIL 
  FROM ODS_DIM_SYS_STAFF_20101105           STAFF
   LEFT JOIN ODS_ENT_EN_STAFF_20101105 base ON STAFF.STAFF_ID =
                                             BASE.PARTY_ROLE_ID,
       ODS_DIM_SYS_OPERATOR_20101105        OP,
       ODS_DIM_SYS_ORGANIZE_20101105        ORG,
       ODS_DIM_SYS_STAFF_ORG_RELAT_20101105 RELA,
       ODS_ENT_CM_GROUP_CUSTOMER_20101105   GG
 
 WHERE OP.STAFF_ID = STAFF.STAFF_ID
   AND STAFF.STAFF_ID = RELA.STAFF_ID
   AND RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
   AND RELA.IS_BASE_ORG = 'Y'
   AND OP.STATE = 1
   AND STAFF.STATE = 1
   AND RELA.STATE = 1
   AND ORG.STATE = 1
      -- and to_number(op.code) ='100005819902'  
   AND TRIM(CHAR(GG.MGR_ID)) = OP.CODE WITH UR
   
   
   --Dwd_enterprise_manager_rela_ds.tcl ���Ŀھ�
   SELECT DISTINCT STAFF.STAFF_ID as manager_id,
                 GG.GROUP_CUST_ID,
				 GG.IN_NET_DATE,
				 GG.CREATE_DATE,
				 GG.valid_date,
				 GG.EXPIRE_DATE,
				 GG.OP_ID,
				 GG.ORG_ID,
				 GG.DONE_CODE
  FROM ODS_DIM_SYS_STAFF_20101105           STAFF
   LEFT JOIN ODS_ENT_EN_STAFF_20101105 base ON STAFF.STAFF_ID =
                                             BASE.PARTY_ROLE_ID,
       ODS_DIM_SYS_OPERATOR_20101105        OP,
       ODS_DIM_SYS_ORGANIZE_20101105        ORG,
       ODS_DIM_SYS_STAFF_ORG_RELAT_20101105 RELA,
       ODS_ENT_CM_GROUP_CUSTOMER_20101105   GG
 
 WHERE OP.STAFF_ID = STAFF.STAFF_ID
   AND STAFF.STAFF_ID = RELA.STAFF_ID
   AND RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
   AND RELA.IS_BASE_ORG = 'Y'
   AND OP.STATE = 1
   AND STAFF.STATE = 1
   AND RELA.STATE = 1
   AND ORG.STATE = 1
      -- and to_number(op.code) ='100005819902'  
   AND TRIM(CHAR(GG.MGR_ID)) = OP.CODE WITH UR


   
   
   SELECT * FROM SEC.SYS_OPERATOR
   
   
   
   
select a.*
from (   
SELECT	distinct 	
	    STAFF.STAFF_ID	manager_id
	    ,base.MANAGER_TYPE
		,base.MANAGER_STATUS
		,base.dept_id
		,base.MEMBER_TYPE
		,base.EXAM_ROLE_ID
		,STAFF.STATE
		,char(province_id)
		,substr(city_id,2)
		,base.region_type
		,base.REGION_DETAIL
		,STAFF_NAME	manager_NAME					
		,staff.gender
		,base.POLITICS_FACE
		,base.EDUCATION_LEVEL
		,STAFF.EMAIL
		,STAFF.BILL_ID
		,BASE.OFFICE_TEL
		,base.HOME_TEL
		,base.CONTACT_ADDRESS
		,base.POSTCODE
		,staff.job_position
		,base.JOB_DESC
		,base.begin_date
		,base.DONE_DATE
		,base.VALID_DATE
		,base.expire_date
		,OP.OPERATOR_ID	op_id					
		,ORG.ORGANIZE_ID	ORG_ID					
		,base.done_code
		FROM	ODS_DIM_SYS_STAFF_20101105	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101105	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101105	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101105	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101105 ) 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101105	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
			WITH	UR) a
			join 
�� select manager_id,count(0) cnt 
from ((   
SELECT	distinct 	
	    STAFF.STAFF_ID	manager_id
	    ,base.MANAGER_TYPE
		,base.MANAGER_STATUS
		,base.dept_id
		,base.MEMBER_TYPE
		,base.EXAM_ROLE_ID
		,STAFF.STATE
		,char(province_id)
		,substr(city_id,2)
		,base.region_type
		,base.REGION_DETAIL
		,STAFF_NAME	manager_NAME					
		,staff.gender
		,base.POLITICS_FACE
		,base.EDUCATION_LEVEL
		,STAFF.EMAIL
		,STAFF.BILL_ID
		,BASE.OFFICE_TEL
		,base.HOME_TEL
		,base.CONTACT_ADDRESS
		,base.POSTCODE
		,staff.job_position
		,base.JOB_DESC
		,base.begin_date
		,base.DONE_DATE
		,base.VALID_DATE
		,base.expire_date
		,OP.OPERATOR_ID	op_id					
		,ORG.ORGANIZE_ID	ORG_ID					
		,base.done_code
		FROM	ODS_DIM_SYS_STAFF_20101105	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101105	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101105	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101105	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101105 ) 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101105	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
			WITH	UR) ) group by manage_id 
			having count(0) > 1 
			) b on a.manager_id = b.manager_id 
			


db2 "create table ODS_ENT_EN_STAFF_YYYYMMDD like ODS_ENT_EN_STAFF_20100531 in tbs_3h \
  PARTITIONING KEY ( PARTY_ID ) USING HASHING not logged initially"
  


IN TBS_3H
partitioning key (PARTY_ID) using hashing
not logged initially





  --Dwd_enterprise_manager_rela_ds.tcl ���Ŀھ�
   SELECT DISTINCT STAFF.STAFF_ID as manager_id,
                 GG.GROUP_CUST_ID,
				 GG.IN_NET_DATE,
				 GG.CREATE_DATE,
				 GG.valid_date,
				 GG.EXPIRE_DATE,
				 GG.OP_ID,
				 GG.ORG_ID,
				 GG.DONE_CODE
  FROM ODS_DIM_SYS_STAFF_20101105           STAFF
   LEFT JOIN ODS_ENT_EN_STAFF_20101105 base ON STAFF.STAFF_ID =
                                             BASE.PARTY_ROLE_ID,
       ODS_DIM_SYS_OPERATOR_20101105        OP,
       ODS_DIM_SYS_ORGANIZE_20101105        ORG,
       ODS_DIM_SYS_STAFF_ORG_RELAT_20101105 RELA,
       ODS_ENT_CM_GROUP_CUSTOMER_20101105   GG
 
 WHERE OP.STAFF_ID = STAFF.STAFF_ID
   AND STAFF.STAFF_ID = RELA.STAFF_ID
   AND RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
   AND RELA.IS_BASE_ORG = 'Y'
   AND OP.STATE = 1
   AND STAFF.STATE = 1
   AND RELA.STATE = 1


SELECT 						
DISTINCT						
STAFF.STAFF_ID		GG.IN_NET_DATE,				
		GG.CREATE_DATE,				
						
						
						
GG.valid_date,						
GG.EXPIRE_DATE,						
GG.OP_ID,						
GG.ORG_ID,						
GG.DONE_CODE						
						
FROM	ODS_DIM_SYS_STAFF_20101105	STAFF				
LEFT	JOIN	ODS_ENT_EN_STAFF_20101105	base	ON	STAFF.STAFF_ID	=
BASE.PARTY_ROLE_ID,						
ODS_DIM_SYS_OPERATOR_20101105	OP,					
ODS_DIM_SYS_ORGANIZE_20101105	ORG,					
ODS_DIM_SYS_STAFF_ORG_RELAT_20101105	RELA,					
ODS_ENT_CM_GROUP_CUSTOMER_20101105	GG					
						
WHERE	OP.STAFF_ID	=	STAFF.STAFF_ID			
AND	STAFF.STAFF_ID	=	RELA.STAFF_ID			
AND	RELA.ORGANIZE_ID	=	ORG.ORGANIZE_ID			
AND	RELA.IS_BASE_ORG	=	'Y'			
AND	OP.STATE	=	1			
AND	STAFF.STATE	=	1			
AND	RELA.STATE	=	1			
						
desc ODS_DIM_SYS_STAFF_20101105 >> f2.txt
desc ODS_ENT_EN_STAFF_20101105 >> f2.txt
desc ODS_DIM_SYS_OPERATOR_20101105 >> f2.txt
desc ODS_DIM_SYS_ORGANIZE_20101105 >> f2.txt
desc ODS_DIM_SYS_STAFF_ORG_RELAT_20101105 >> f2.txt




		FROM	ODS_DIM_SYS_STAFF_20101105	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101105	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101105	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101105	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101105 ) 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101105	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
			WITH	UR) ) group by manage_id 
			having count(0) > 1 
			) b on a.manager_id = b.manager_id 
			

1	���ű��	GROUP_ID	NUMBER(12)	
2	���ű��	CLUSTER_ID	NUMBER(10)	
3	�ͻ�������	MANAGER_ID	NUMBER(8)	
4	��������	OPER_TYPE	NUMBER(2)	0   ��ѯ1   ��ѯ������
5	��ϵ����	REL_TYPE	NUMBER(2)	1:��Ҫ����2:��������3:��ϯ�ͻ�����
6	��¼״̬	REC_STATUS	NUMBER(2)	0��ʷ1��Ч
7	��������	DONE_DATE	DATE	
8	��Ч����	VALID_DATE	DATE	
9	ʧЧ����	EXPIRE_DATE	DATE	
10	����Ա	OP_ID	NUMBER(8)	
11	��֯��Ԫ	ORG_ID	NUMBER(8)	
12	������ˮ	DONE_CODE	NUMBER(15)	


select DISTINCT
 GG.GROUP_CUST_ID
,base.dept_id CLUSTER_ID
,STAFF.STAFF_ID as manager_id
,1  OPER_TYPE 
,1 	REL_TYPE
,case when gg.expire_date<TIMESTAMP('2010-11-13 00:00:00.00000') then 0 else 1 end REC_STATUS
,GG.CREATE_DATE
,GG.valid_date
,GG.EXPIRE_DATE
,GG.OP_ID
,GG.ORG_ID
,GG.DONE_CODE
		FROM	ODS_DIM_SYS_STAFF_20101113	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101113	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101113	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101113	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN (SELECT DISTINCT MGR_ID ,GROUP_CUST_ID,EXPIRE_DATE,OP_ID,ORG_ID,DONE_CODE FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101113 ) 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101113	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
		



select DISTINCT
 count(GG.GROUP_CUST_ID)
, count(distinct GG.GROUP_CUST_ID)
, count(distinct STAFF.STAFF_ID)
, count(distinct GG.GROUP_CUST_ID||gg.GROUP_CUST_ID)
		FROM	ODS_DIM_SYS_STAFF_20101113	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101113	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101113	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101113	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN ODS_ENT_CM_GROUP_CUSTOMER_20101113 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101113	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
		


select DISTINCT
 GG.GROUP_CUST_ID
,base.dept_id CLUSTER_ID
,STAFF.STAFF_ID as manager_id
,1  OPER_TYPE 
,1 	REL_TYPE
,case when expire_date<TIMESTAMP('$p_optime 00:00:00.00000') then 0 else 1 end REC_STATUS
,GG.CREATE_DATE
,GG.valid_date
,GG.EXPIRE_DATE
,GG.OP_ID
,GG.ORG_ID
,GG.DONE_CODE
		FROM	ODS_DIM_SYS_STAFF_20101113	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101113	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101113	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101113	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN ODS_ENT_CM_GROUP_CUSTOMER_20101113 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101113	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1
	



ds Dwd_cust_manager_info_ds.tcl 2010-11-14
ds Dwd_enterprise_manager_rela_ds.tcl  2010-11-14

ǰ��������

select * from app.sch_control_before

select * from app.sch_control_task  
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  



insert into app.sch_control_before 
select 'BASS2_Dwd_enterprise_manager_rela_ds.tcl',control_code
from app.sch_control_task 
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  





select * from app.sch_control_before 
where control_code like '%BASS2_Dwd_enterprise_manager_rela_ds.tcl%'


delete from app.sch_control_before 
where control_code like '%BASS2_Dwd_enterprise_manager_rela_ds%'


insert into app.sch_control_before 
select 'BASS2_Dwd_enterprise_manager_rela_ds.tcl',control_code
from app.sch_control_task 
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  



select * from app.sch_control_before 
where control_code like '%Dwd_cust_manager_info_ds.tcl%'


delete from app.sch_control_before 
where control_code like '%BASS2_Dwd_cust_manager_info_ds%'


insert into app.sch_control_before 
select 'BASS2_Dwd_cust_manager_info_ds.tcl',control_code
from app.sch_control_task 
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  


  FROM ODS_DIM_SYS_STAFF_$p_timestamp staff    
  JOIN ODS_DIM_SYS_OPERATOR_$p_timestamp OP     ON      STAFF.STAFF_ID = OP.STAFF_ID
  JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_$p_timestamp RELA ON    STAFF.STAFF_ID = RELA.STAFF_ID  
  JOIN ODS_DIM_SYS_ORGANIZE_$p_timestamp ORG         ON      RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
  JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_$p_timestamp )  GG ON TRIM(CHAR(GG.MGR_ID)) = OP.CODE 
  LEFT JOIN ODS_ENT_EN_STAFF_$p_timestamp base ON STAFF.STAFF_ID = BASE.PARTY_ROLE_ID  
  WHERE RELA.IS_BASE_ORG = 'Y'   
  AND OP.STATE = 1   
  AND STAFF.STATE = 1   
  AND RELA.STATE = 1   
  AND ORG.STATE = 1   
  "
  

 SELECT distinct  
     STAFF.STAFF_ID manager_id
     ,base.MANAGER_TYPE
  ,base.MANAGER_STATUS
  ,base.dept_id
  ,base.MEMBER_TYPE
  ,base.EXAM_ROLE_ID
  ,STAFF.STATE
  ,char(province_id)
  ,substr(city_id,2)
  ,int(base.region_type)
  ,base.REGION_DETAIL
  ,STAFF_NAME manager_NAME     
  ,staff.gender
  ,base.POLITICS_FACE
  ,base.EDUCATION_LEVEL
  ,STAFF.EMAIL
  ,STAFF.BILL_ID
  ,BASE.OFFICE_TEL
  ,base.HOME_TEL
  ,base.CONTACT_ADDRESS
  ,base.POSTCODE
  ,staff.job_position
  ,base.JOB_DESC
  ,base.begin_date
  ,base.DONE_DATE
  ,base.VALID_DATE
  ,base.expire_date
  ,OP.OPERATOR_ID op_id     
  ,ORG.ORGANIZE_ID ORG_ID     
  ,base.done_code
  FROM ODS_DIM_SYS_STAFF_$p_timestamp staff    
  JOIN ODS_DIM_SYS_OPERATOR_$p_timestamp OP     ON      STAFF.STAFF_ID = OP.STAFF_ID
  JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_$p_timestamp RELA ON    STAFF.STAFF_ID = RELA.STAFF_ID  
  JOIN ODS_DIM_SYS_ORGANIZE_$p_timestamp ORG         ON      RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
  JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_$p_timestamp )  GG ON TRIM(CHAR(GG.MGR_ID)) = OP.CODE 
  LEFT JOIN ODS_ENT_EN_STAFF_$p_timestamp base ON STAFF.STAFF_ID = BASE.PARTY_ROLE_ID  
  WHERE staff.STAFF_ID=100005816062
  
  "
  
  /**	2010-11-16 11:27	added by  panzhiwei		**/	
--DROP TABLE DWD_NG2_I08112_YYYYMMDD					
CREATE TABLE DWD_NG2_I08112_YYYYMMDD (					
			BUSI_CODE				VARCHAR(8)	--ҵ�����ʹ���			
			,BUSI_NAME			VARCHAR(60)	--ҵ����������			
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( BUSI_CODE )
 USING HASHING


CD09NB0013







/**	2010/11/2 18:02	added by  panzhiwei		**/	
CREATE TABLE DWD_NG2_M06020_YYYYMMDD (					

 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING



/**	2010-11-22 16:36	added by  panzhiwei		**/													
--DROP TABLE DWD_NG2_M06020_YYYYMMDD																	
CREATE TABLE DWD_NG2_M06020_YYYYMMDD (																	
        REGION_ID           VARCHAR(7)          --�����ʶ
        ,manufacture_name   VARCHAR(100)        --�ն˳���
        ,term_model         VARCHAR(20)         --�ն��ͺ�
        ,vendor_name        VARCHAR(100)        --�ն˹�����
        ,settle_type        SMALLINT            --��������
        ,settle_info_id     BIGINT              --����֪ͨ�����
        ,pay_acct_id        BIGINT              --֧��ƾ֤��
        ,pay_state          SMALLINT            --֧��״̬
        ,done_code          BIGINT              --����������
        ,contract_id        BIGINT              --�����ͬ���
        ,valid_date         date           --��Ч����
        ,expire_date        date           --��ֹ����
        ,info_date          date           --֪ͨ����
        ,settle_date        date           --��������
        ,settle_amt         DECIMAL(12,0)       --�����ܽ��
        ,is_settle_amt      DECIMAL(12,0)       --�ѽ�����
        ,not_settle_amt     DECIMAL(12,0)       --δ������
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( settle_info_id,REGION_ID )
 USING HASHING


TASK_ID=M06020
MODULE=GG
RUNTYPE=R
DATECYCLE=D
DATEPARAM=$YYYYMMDD$
AREAPARAM=891,892,893,894,895,896,897
RUNSQL=select \
REGION_ID                  ,\
manufacture_name ,\
term_model ,\
vendor_name ,\
settle_type ,\
settle_info_id ,\
pay_acct_id ,\
pay_state ,\
done_code ,\
contract_id ,\
valid_date ,\
expire_date ,\
info_date ,\
settle_date ,\
settle_amt ,\
is_settle_amt ,\
not_settle_amt ,\
 ,\


db2 "load client from /bassapp/bass2/load/boss/M0602020101001000000.AVL of del modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 messages /dev/null replace into DWD_NG2_M06020_201010"



db2 "load client from /bassapp/bass2/load/boss/M0602020101001000000.AVL of del \
 modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 \
 messages /bassapp/bass2/load/boss/messages/db2load.msg  replace into DWD_NG2_M06020_201010
 "
 
 
 ��������

db2 alter table ���� add column �ֶ��� varchar(4)(�ֶ����ͣ�


tcl: format "#%02x%02x%02x" $r $g $b
% format "#%02x%02x%02x" 0 128 255
#0080ff

app.sch_control_runlog:
0   ������� 
1   ��������
-1  ���г��� 
-2  ��������


scan   $op_time "%04s-%02s-%02s" year month day

set aidbtclpath "/aitools"
set aidbtclpath "/bassapp/tcl/aiomnivision/aitools"
load "$aidbtclpath/lib/libdb2tcl.so"
source "$aidbtclpath//bin//aidb_db2_webapp.tcl"
source "/bassapp/tcl/aiomnivision/aitools/bin/aidb_db2.tcl"
set db bassdb
set username bass2
set passwd bass2
set conn [aidb_connect $db $username $passwd]

#ȫ�ֱ���:
puts $env(DATABASE)
puts $env(AITOOLSPATH)
puts $env(DB_USER)
puts $env(AGENTLOGDIR)


3.3	aidb_open 
���ܣ��������ݲ������
���������dbhandle    --���ݿ����Ӿ��
	���أ�  handle  --���ݿ�������

���ӣ�
    set  handle  [aidb_open $dbhandle]

  #ɾ����������
  set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02006_month where time_id=$op_month"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
(	 partitioning key
                      (user_id)
                      using hashing
                     with replace on commit preserve rows not logged in tbs_user_temp
)

				WriteTrace "The data_time($data_time) is not correct!" 0111



#�ڲ���������
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		#WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		#WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle


	return $result
}
#--------------------------------------------------------------------------------------------------------------



% set conn [aidb_connect bassdb bass2 bass2]
db2sql1
% set conn [aidb_connect bassdb bass2 bass2]
db2sql2
% get_single "select tabname from syscat.tables"
G_RUNLOG
% get_single "select tabname from syscat.tables where tabschema = 'BASS2'"
A_YANGFEI



select count(0) from bass1.g_rule_check 
select * from  bass2.dim_prod_up_product_item


  set RESULT_VAL [get_single $sql_buff]
  


���⣺
insert into bass1.int_program_data values ('d','G_A_06001_DAY.tcl','06001.bass1','06001_e','06001_f');
insert into app.g_unit_info values ('06001',0,'У԰������Ϣ����','bass1.g_a_06001_day',1,0,1);

��
5.5	��5�������ͽӿ�


���ĵ���STD1��ʾҵ��ά�ȷ��࣬STD2������ʾ����ά�ȷ��࣬STD3������ʾ����ָ�꣬STD4������ʾ��չָ�ꡣ

ע���½�����������˳��2��Сʱ����æʱ9:00-0��00-�����賿1:00����ʱ��1��00-9��00

insert into bass1.int_program_data values ('d','G_A_06001_DAY.tcl','06001.bass1','06001_e','06001_f');
insert into app.g_unit_info values ('06001',0,'У԰������Ϣ����','bass1.g_a_06001_day',1,0,1);

        db2 connect to bassdb user bass2 using bass2
        db2 "create table BASS2.STAT_ENTERPRISE_SNAPSHOT_${op_month} like BASS2.STAT_ENTERPRISE_SNAPSHOT_YYYYMM in TBS_REPORT index 
in TBS_INDEX PARTITIONING KEY ( ENTERPRISE_CODE ) USING HASHING NOT LOGGED INITIALLY"
        db2 "load client from '/bassapp/report/tcl/vip_rati/${target_file}' of asc method L (27 35, 36 45, 46 59, 60 67, 68 81, 82 8
2, 83 83) replace into BASS2.STAT_ENTERPRISE_SNAPSHOT_${op_month}"
        db2 terminate



ÿ������
ָ��ȡÿ��00��00---24��00�����仯�ġ��²�������������״̬���ա�
ÿ����������
ָ��ȡÿ��0��00��24��00����ʱ�䷶Χ���²��������ݡ�

?	��֤�ӿ������ļ��Ĵ�С���ܳ���2GB������ӿ������ļ�̫�󣬱��밴

2.5	�ӿڷ�����
��������ַ��172.16.5.130
��½�û���bass
���룺3jysjbx


�ƶ����ɿ�
map


�ͻ����������������Ӱ�����ֺܴ�Ĳ���Ҳ��������?
find . -name "*21007*dat" -exec ls -l {} \;

#00 19 * * * . .profile;/bassapp/bass2/tcl/batch_tclsh.sh /bassapp/bass2/tcl/batch_tclsh.cfg
30 13 * * * /bassapp/bass2/data/backup_table.sh
10 9 3 * * . .profile;/bassapp/bass2/sh/dmrn_run_dayamonth.sh
00 23 * * * . .profile;/bassapp/bass2/bak_xufr/bak.sh

1 12 6 * * /bassapp/bass2/dmpm/export_interface_data.sh > /bassapp/bass2/dmpm/nohup.out
                       

$ db2look -d bassdb56 -i bass2 -w bass2 -z bass1 -t g_s_05001_month -e

������ѯ���������ݿ���ַ�����1386������MBCS���ݣ���˶���������0x3F������|����ASCII��Ϊ0x7C����������Ч��Χ������취������Export����Load����ʱ������codepageѡ���DB2�Զ������ַ���ת����

EXPORT TO "/data/temp/card.dat" OF DEL
MODIFIED BY codepage=1208 COLDEL|
MESSAGES "/data/temp/card.log"
SELECT * FROM BI.STG_CDR_OBS_CARD

LOAD CLIENT FROM "/data/temp/card.dat" OF DEL
MODIFIED BY CODEPAGE=1208 COLDEL|
MESSAGES "/data/temp/card.log"
INSERT INTO BI.STG_CDR_OBS_CARD

                       
                       

$ int -s zhanght_test.tcl > month_rpt_201103.txt
$ cat  month_rpt_201103.txt| grep "[0-9]:" > month_rpt_201103_final.txt 



                       
SYS_TASK_RUNLOG������


alias bin='cd /bassdb2/etl/E/boss/java/crm_interface/bin'
alias pzh='cd /bassdb2/etl/E/panzw2'
alias lo='cd /bassdb2/etl/L/boss'
alias cfg='cd /bassdb2/etl/E/boss/java/crm_interface/bin/config/BOSS'
alias bak='cd /bassdb2/etl/L/boss/backup'
alias vlog='/bassdb2/etl/E/panzw2/ViewLoadLog_bassdb46.sh'

                       
                       
create bass2.t_0x1f
(cola varchar(10),colbvarchar(10))
in tbs_3h;


db2 "load client from /bassapp/bass1/panzw2/bass1/t0x1f.txt of del \
messages ./t_0x1f.msg \
modified by coldel0x1F replace into bass2.t_0x1f"


db2 load  client from "/bassapp/bass1/panzw2/bass1/a.txt" of del modified by coldel0x1F  fastparse anyorder warningcount 1000 messages ./t0x1f.msg  replace into bass2.t_0x1f
 

db2 load  client from "/bassapp/bass1/panzw2/bass1/b.txt" of del modified by coldel,  fastparse anyorder warningcount 1000 messages ./t0x1f.msg  replace into bass2.t_0x1f

create bass2.t_0x1f
(cola varchar(10),colbvarchar(10))
in tbs_3h;

db2 "
LOAD CLIENT FROM "./a.txt" OF DEL \
MODIFIED BY CODEPAGE=1208 COLDEL0x1F \
MESSAGES "./card.log" \
INSERT INTO  bass2.t_0x1f
"

db2 load client from /bassapp/bass2/a.txt of del MODIFIED BY COLDEL0x1F insert into  bass2.t_0x1f
db2 load client from /bassapp/bass2/a.txt of del MODIFIED BY COLDEL0x80 insert into  bass2.t_0x1f


06021 TB_SVC_CORCHNL_BASIC ʵ������������Ϣ 
06022 TB_SVC_CORCHNL_RENT ʵ������������������Ϣ 
06023 TB_SVC_CORCHNL_RES_CONF ʵ��������Դ������Ϣ
22061 TB_SVC_CORCHNL_BUSN_COST ʵ��������Ӫ�ɱ���Ϣ
22062 TB_SVC_CORCHNL_PROC_INFO ʵ������ҵ�������Ϣ
22063 TB_SVC_CORCHNL_SALRY_ALLOW ʵ��������𼰲�����Ϣ
22064 TB_SVC_CORCHNL_ADD_BUSN_INFO ʵ�������ص���ֵҵ�������Ϣ"									
									
							                       

Q3:R 2708~3247							                       



        select 
                20110428 as TIME_ID
                ,a.PRODUCT_INSTANCE_ID as USER_ID
                ,bass1.fn_get_all_dim('BASS_STD1_0115',char(a.offer_id)) as ADD_PKG_ID
                ,replace(char(date(a.VALID_DATE)),'-','') as VALID_DT
                from  bass2.Dw_product_ins_off_ins_prod_ds as a 
                ,                bass2.dw_product_20110428 as b
                    where b.usertype_id in (1,2,9) 
                    and b.userstatus_id in (1,2,3,6,8)
                    and b.test_mark<>1
                          and a.OP_TIME = '2011-04-28'
                          and a.state=1
                    and date(a.VALID_DATE)<='2011-04-28'
                    and a.valid_type = 1
                    and bass1.fn_get_all_dim('BASS_STD1_0115',char(a.offer_id)) is not null 
                    and  b.user_id = a.PRODUCT_INSTANCE_ID 
                    
                    

Reorganize the table for one or more of the following reasons:

 Reorganize due to excessive overflow records (F1). The total number of overflow rows in the table should be less than 5 percent of the total number of rows. Overflow rows can occur when rows are updated and the new rows no longer fit in the data page where it was originally written. This usually occurs as a result of VARCHAR columns or an ALTER TABLE statement.

 Reorganize due to excesses free space (F2). The table size in bytes (TSIZE) should be more than 70 percent of the total space allocated for the table. There should be less than 30% free space.

 Reorganize due to excessive free pages (F3). The number of pages that contain no rows at all should be less than 20% of the total number of pages. Pages can become empty after rows are deleted.

 Running a reorganization of the table may not resolve the problem(s) and subsequent advice may still identify this as a problem. Refer to the UDB Administration Guide for further information.
 

bossintf =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = 172.16.5.49)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME =bossintf) 
    )
)
testjf =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = 172.16.9.10)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = jftest)
    )
)

crmtest =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = 172.16.9.22)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = crmtest)
    )
)

nfjdzcdb =
  (DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = 172.16.6.63)(PORT = 1521))
    )
    (CONNECT_DATA =
      (SERVICE_NAME = zwdb)
    )
  )

xzkfdb =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = 10.233.30.78)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = xzkfdb)
    )
)


BASS2.ETL_TASK_RUNNING 
                     
                     

�ӿ�����
1.���ӿ��б� �ṩ�ӿ��б���Ϣ��ѯ������⣬�����á�
2. bass2.ETL_LOAD_TABLE_MAP ���ú󹩼��س�����á�
3.bass2.USYS_TABLE_MAINTAIN ���ú��Զ����������á�
4.app.sch_control_task  ���ú��Զ����ȳ�����á�



$ db2look -d bassdb -e -i app -w app -z bass2 -t DWD_DSMP_sp_oper_code_201011
test ok!

nowpts=`ps |awk '{print $2}' |grep pts|uniq`
USER=`who |grep $nowpts|awk '{print $1}'`
export USER
echo $USER


FTP::172.16.5.43\/bassdb1/etl/E/boss/java/crm_interface/bin/config/CRM|A11031.properties


1.���boss�ӿ� �����ṩ��
2.�ӿ���Ϣ�Ǽ�: ��д\NG1-BASS1.0\Truck\03 ������\interface\��ȡ\doc\BOSS�ӿڹ滮v1.00.xls 
3.���� �ӿں�.properties д��ȡ���
4.��ģ���:NG1-BASS1.0\Truck\03 ������\interface\����\doc\ODS����ű�.sql
5.���õ��ȣ�
5.1.�������ر�ӳ���ϵ��insert into etl_load_table_map
5.2. sch_control_task svn://172.16.5.71/XZBI/NG1-BASS1.0/Truck/03 ������/interface/�������/��ӵ�������.xls
insert into app.sch_control_task
insert into app.sch_control_task values ('TR1_L_11035',1,2,'ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_YYYYMMDD',0,-1,'��������������ʷ��','-','TR1_BOSS',2,'-');

6.���ء�ά����svn://172.16.5.71/XZBI/NG1-BASS1.0/Truck/03 ������/interface/����/doc/load������Ϣ.xls 
  6.1. select max(table_id) from USYS_TABLE_MAINTAIN
  	   insert into USYS_TABLE_MAINTAIN 

7.���ԣ�
7.1.���� taskList_tmp.properties
FTP::172.16.5.43\/bassdb1/etl/E/boss/java/crm_interface/bin/config/CRM|taskList_tmp.properties
7.2.���� ��������Ч��) 
7.3.�����󲹽���ʷ�ձ�͵����ձ�
7.4.ִ�м���
/bassdb1/etl/E/boss/java/crm_interface/bin 
java ETLMain 20101015 taskList_tmp.properties
7.5.�鿴�������
/bassdb1/etl/L/boss
 tail -f nohup.out

8.����OK  �����ý�tasklist
FTP::172.16.5.43\/bassdb1/etl/E/boss/java/crm_interface/bin/config/CRM|taskList.properties

9.���ش����Ų�
awk
 select * from etl_task_log  where task_id='A11034' order by stime desc


$ head -1 A1103420100818000000.AVL | awk -F'$' '{print NF}'
25
$ awk -F'$' '{if(NF!=19) print NR}' A1103420100818000000.AVL|head
38447
38448
38668
38669
$ sed -n '3,3p' A1103420100818000000.AVL


-----------------------------------------------------------------------------------

so.ORD_DSMP_BAT_DATA_HIS    ������  
so.ORD_BATCH_DATA_DETAIL_F_1008 ������  
1.ȷ���ӿں�  �����ظ�
�����ĵ� F:\XZBI\NG1-BASS1.0\Truck\03 ������\interface\��ȡ\doc\BOSS�ӿڹ滮v1.00.xls
2.���� properties 
where done_date=����   
�����ֶ�Ҫ��ʽ��
3. д�������
F:\XZBI\NG1-BASS1.0\Truck\03 ������\interface\����\doc\ODS����ű�.sql
5. ���õ���
ETL_LOAD_TABLE_MAP
sch_control_task
USYS_TABLE_MAINTAIN
6.���Գ�ȡ.
����OK  �����ý�tasklist




1. ���� taskList_tmp.properties
����
����������Ϣ ETL_LOAD_TABLE_MAP

2. /bassdb1/etl/E/boss/java/crm_interface/bin 
java ETLMain 20101015 taskList_tmp.properties

3. �ļ����Զ��ƶ� /bassdb1/etl/L/boss
 tail -f nohup.out �鿴�������
��鿴  select * from etl_task_log  where task_id='A11034' order by stime desc

/bin/sh ./load_boss_nfjd.sh


4.�������µı�
/bassapp/bass2/fuzl
$ ls
bassapp\bass2\fuzl\stat_acct_td_001_s_lkp.del  crt.sh

crt.sh ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS TBS_3H DATA_ID 201007 &
crt.sh ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS TBS_3H DATA_ID 201008 &
crt.sh ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS TBS_3H DATA_ID 201010 &

crt.sh ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS TBS_3H DATA_ID 201010 &


5.���õ���
FTP::172.16.5.43\/bassdb1/etl/E/boss/java/crm_interface/bin/config/CRM|taskList.properties

5.43 /bassdb1/etl/E/boss/java/crm_interface/bin/java_crm_interface.sh
/usr/jdk/jdk1.5.0_19/bin/java ETLMain $sday > /bassdb1/etl/E/boss/java/crm_interface/bin/log/$sday.log

6.��������
/bassdb1/etl/E/boss/java/crm_interface/bin

t.sh 
tasklist_tmp.properties


USYS_TABLE_MAINTAIN

app.sch_control_task

insert into app.sch_control_task values ('TR1_L_11035',1,2,'ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_YYYYMMDD',0,-1,'��������������ʷ��','-','TR1_BOSS',2,'-');
insert into app.sch_control_task values ('TR1_L_11034',1,2,'ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_YYYYMMDD',0,-1,'��ͨҵ�����������','-','TR1_BOSS',2,'-');

java ETLMain $sday 


insert into USYS_TABLE_MAINTAIN values(10171,'��ͨҵ�����������','ORD_BATCH_DATA_DETAIL_F','1','day',255,'0','','','BASS2','ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_YYYYMMDD','TBS_3H','TBS_INDEX','BATCH_DATA_DETAIL_ID',1);



select max(table_id) from USYS_TABLE_MAINTAIN


select * from etl_task_log  where task_id='A11034' order by stime desc


create table ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101001 like ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_yyyymmdd in tbs_3h index in TBS_INDEX   PARTITIONING KEY ( BATCH_DATA_DETAIL_ID ) USING HASHING;

<input type="hidden" awk '{if (FNR >= 2072032) print $0 }' nohup.out > pzw.log

java ETLMain    20101011        taskList_tmp.properties
----------------------------------------------------------------------------------
ds��/bassapp/bass2/tcl/ds
$ ds
Usage: ds [script-file] [yyyy-mm-dd]

ds ����
1.���û���
2.���ó���
3.������
4.д��־


TR1_L_11035	load_boss_nfjd.sh	1	�ļ�A1103520101101000000.AVL.Z����ʧ��,��ѯ�����¼��ʧ�ܶ����£�	2010/11/2 2:21:16.179672	[NULL]	-1	[NULL]
TR1_L_11034	load_boss_nfjd.sh	1	�ļ�A1103420101101000000.AVL.Z����ʧ��,��ѯ�����¼��ʧ�ܶ����£�	2010/11/2 2:20:43.128532	[NULL]	-1	[NULL]
TR1_L_11036	load_boss_nfjd.sh	1	�ӿ��ļ�:M1103620101000000000.AVL.Z����ʱ����ʧ��,ʧ�ܼ�¼��Ϊ1!	2010/11/2 1:53:46.076683	[NULL]	-1	[NULL]
TR1_L_11035	load_boss_nfjd.sh	1	�ļ�A1103520101101000000.AVL.Z����ʧ��,��ѯ�����¼��ʧ�ܶ����£�	2010/11/2 1:14:37.815960	[NULL]	-1	[NULL]
TR1_L_11034	load_boss_nfjd.sh	1	�ļ�A1103420101101000000.AVL.Z����ʧ��,��ѯ�����¼��ʧ�ܶ����£�	2010/11/2 1:13:56.955428	[NULL]	-1	[NULL]



insert into USYS_TABLE_MAINTAIN values(10173,'��������������ʷ��','ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS','1','day',255,'0','','','BASS2','ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_YYYYMMDD','TBS_3H','TBS_INDEX','DATA_ID',1);
insert into USYS_TABLE_MAINTAIN values(10172,'��ͨҵ�����������','ORD_PRODUCT_BATCH_DATA_DETAIL_F','1','day',255,'0','','','BASS2','ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_YYYYMMDD','TBS_3H','TBS_INDEX','BATCH_DATA_DETAIL_ID',1);


1
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101101;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101102;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101103;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101104;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101105;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101106;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101107;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101108;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101109;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101110;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101111;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101112;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101113;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101114;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101115;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101116;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101117;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101118;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101119;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101120;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101121;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101122;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101123;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101124;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101125;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101126;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101127;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101128;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101129;
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101130;



crt.sh ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS TBS_3H DATA_ID 201011 &


create table ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_20101124 like ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_YYYYMMDD in TBS_3H index in tbs_index partitioning key ( DATA_ID ) using hashing not logged initially



DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101101;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101102;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101103;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101104;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101105;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101106;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101107;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101108;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101109;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101110;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101111;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101112;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101113;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101114;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101115;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101116;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101117;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101118;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101119;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101120;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101121;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101122;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101123;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101124;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101125;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101126;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101127;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101128;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101129;
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101130;

crt.sh ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F TBS_3H BATCH_DATA_DETAIL_ID 201011 &


ods�� ����7��

etl_task_log ������־


2828393$89402999763207$101026162007B030138760894$$191000000013$$0894$$$$$$$$$$$$$$1$1$11$$300015690385$20101026162007$20101026162008$20101026162007$20991231235959$40400040$40400123$1$-1$0$0$b$$$89457333561893$13638947377

�޸�runsql:
FTP::172.16.5.43b\/bassdb1/etl/E/boss/java/crm_interface/bin/config/CRM|M11036.properties

RUNSQL=select CUSTOMER_ORDER_ID , CUST_PARTY_ROLE_ID , ORDER_CODE , ORDER_TYPE , BUSINESS_ID
 , replace(replace(replace(CUSTOMER_NAME,chr(10)),chr(13)),'$','') CUSTOMER_NAME 
 , REGION_ID , CEASE_REASON , AGENCY_PERSON , CERTIFI_CODE , CERTIFI_TYPE_ID , TEL , PARTNER_ID , OLD_SO_ORDER_ID , to_char(BOOKING_HOME_DT,'YYYYMMDDHH24MISS') , to_char(BOOKING_OPEN_DT,'YYYYMMDDHH24MISS') , to_char(WORKFLOW_START_DT,'YYYYMMDDHH24MISS') , IS_FAST_CHANNEL , ORDER_LEVEL , PRE_SO_ID , VALID_TYPE , STATE , ORDER_STATE , NOTES , DONE_CODE , to_char(CREATE_DATE,'YYYYMMDDHH24MISS') , to_char(DONE_DATE,'YYYYMMDDHH24MISS') , to_char(VALID_DATE,'YYYYMMDDHH24MISS') , to_char(EXPIRE_DATE,'YYYYMMDDHH24MISS') , OP_ID , ORG_ID , SOURCE_SYSTEM_ID , SALE_SOURCE , VERIFY_TYPE , PAY_TYPE , CHANNEL_TYPE , REP_FEE_PHONE_NO , WRRANT_NO , PRODUCT_INSTANCE_ID , BILL_ID  from SO.ORD_CUST_f_0$FEE_AREA$_$YYMM$

��CUSTOMER_NAME ����replace!



1.�Ϸ�����NG1-BASS1.0����-���ؾ�Ӫ����ϵͳά���ֲ� - �ӿڲ�ֲ�.doc
2.svn Ŀ¼�ṹ
3.ue fav
4.app.sch_control_alarm
5./bassapp/bass2/trace/



4.	��׺Ϊdt����Ϊday to collect����Ŀ���Ϊÿ��ִ��һ�ε����ۻ��� ( group by )
5.	��׺Ϊmt����Ϊmonth to collect����Ŀ���Ϊÿ��ִ��һ�ε����ۻ���
6.	��׺Ϊmm����Ϊmonth multiple����Ŀ���Ϊÿ��ִ��һ�εĶ��±�
7.	��׺Ϊdm����Ϊday multiple����Ŀ���Ϊÿ��ִ��һ�εĶ��ձ�



������������Ϊ172.16.5.44�����ݿ⻷��Ϊ172.16.5.56

IP: 172.16.5.44 �û���/����:	report/report	��Ŀ¼: /bassapp/report

="insert into app.sch_control_task values ('"&A67&"',"&B67&","&C67&",'"&D67&"',"&E67&","&F67&",'"&G67&"','"&H67&"','"&I67&"',"&J67&",'"&K67&"');"

����������ϸ��Ϣ���������ϴ��Ŀ¼ /bassapp/report/tcl




·����

1.��ȡ�û�home:/bassdb1/etl/E
2.tcl�����⣺/bassapp/tcl/aiomnivision/aitools
3.ģ��� :svn://172.16.5.71/XZBI/NG1-BASS1.0/Truck/04 ģ���


��ֲ���

�в�ӿ����ã�9.25 /bassapp/bass2/ifboss/crm_interface/bin 







1.���boss�ӿ� �����ṩ��
2.�ӿ���Ϣ�Ǽ�: ��д\NG1-BASS1.0\Truck\03 ������\interface\��ȡ\doc\BOSS�ӿڹ滮v1.00.xls 
3.���� �ӿں�.properties д��ȡ���
4.��ģ���:NG1-BASS1.0\Truck\03 ������\interface\����\doc\ODS����ű�.sql
5.���õ��ȣ�
5.1.�������ر�ӳ���ϵ��insert into etl_load_table_map
5.2. sch_control_task svn://172.16.5.71/XZBI/NG1-BASS1.0/Truck/03 ������/interface/�������/��ӵ�������.xls
insert into app.sch_control_task
insert into app.sch_control_task values ('TR1_L_11035',1,2,'ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_YYYYMMDD',0,-1,'��������������ʷ��','-','TR1_BOSS',2,'-');

6.���ء�ά����svn://172.16.5.71/XZBI/NG1-BASS1.0/Truck/03 ������/interface/����/doc/load������Ϣ.xls 
  6.1. select max(table_id) from USYS_TABLE_MAINTAIN
  	   insert into USYS_TABLE_MAINTAIN 

7.���ԣ�
7.1.���� taskList_tmp.properties
FTP::172.16.5.43\/bassdb1/etl/E/boss/java/crm_interface/bin/config/CRM|taskList_tmp.properties
7.2.���� ��������Ч��) 
7.3.�����󲹽���ʷ�ձ�͵����ձ�
7.4.ִ�м���
/bassdb1/etl/E/boss/java/crm_interface/bin 
java ETLMain 20101015 taskList_tmp.properties
7.5.�鿴�������
/bassdb1/etl/L/boss
 tail -f nohup.out

8.����OK  �����ý�tasklist
FTP::172.16.5.43\/bassdb1/etl/E/boss/java/crm_interface/bin/config/CRM|taskList.properties

9.���ش����Ų�
awk
 select * from etl_task_log  where task_id='A11034' order by stime desc
 
 
2.������䣬��־ΰ���03�ࡢ04�ࡢ92�ࡣ 
03	��Ա����
04	��Դ����
92	����


03��	03030	������Ч	CRM	����		CHECK_RESULT_DETAIL 
03��	03031	�������˹���	CRM	����		
03��	03032	����Υ����Ϣ	CRM	����		
03��	03033	�����ɷ���Ϣ	CRM	����		
03��	03034	�����ɱ�	CRM	����		
03��	03035	���������嵥	CRM	����		
03��	03036	������ͬ	CRM	����		
03��	03037	����Ѻ��	CRM	����		
03��	03038	����������Ϣ��ϸ	CRM	����		
03��	03039	�����������	CRM	����		


04��	04014	SIM����Դ	CRM	�޸�		
04��	04015	IMSI��Դ	CRM	�޸�		
04��	04016	�ֻ�������Դ	CRM	�޸�		
04��	04018	�ն��豸	CRM	�޸�		
04��	04021	��Դ������¼	CRM	�޸�		



http://www.open.collab.net/files/documents/60/3916/CollabNetSubversion-client-1.6.13-1.win32.exe

db2����ܵĽ���


RES_SIM_CARD_USED 








BU
����PAϵͳ




1.��д�ӿ�.properties��ʱ��Ҫע��GG,CRM,tab����
2.table_id ��Ҫ�ظ���
3.


1.���澯:

/**
1.�޸�����  alarmtime >=  timestamp('2010mmdd'||'000000') 
2.����׷�ӣ�����ӿ�:
A11035
A11034
M11036
A05021
**/

 select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 or 
 content like '%A05021%'
 )
 and alarmtime >=  timestamp('20101101'||'000000') 
 order by alarmtime desc


 select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 or 
 content like '%A05021%'
 or 
 content like '%A05021%'
 or 
 content like '%ENTERPRISE_MANAGER_RELA%'
 or 
 content like '%CUST_MANAGER_INFO%'
 )
 and alarmtime >=  timestamp('20101114'||'000000') 
 order by alarmtime desc



select * from  app.sch_control_alarm 
where alarmtime >=  timestamp('20101114'||'000000') 
order by alarmtime desc 
;


select * from app.SCH_CONTROL_RUNLOG
where 
		control_code like '%Dwd_enterprise_manager%'
or  control_code like


У���ļ���
?	�ӿ������ļ�����|?	��������|?	�ļ�����������|?	�ļ��Ĵ�С���ֽ�����|?	�ļ��а����ļ�¼��
|?	�ļ��е��м�¼�ĳ���|?	�ڴ�����
2.3.5.2.2	ҵ���߼�У�飺RXXX
2.3.5.2.2.2	ҵ��ָ��У��

9��ӿڣ�



����
����-�޸�
����

E:\bass1\�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶(V1.7.1)\һ����Ӫ����ϵͳʡ�����ݽӿڼ�ʱ��ʱ��Ҫ��1.7.1.xls


/bassapp/backapp/data/bass1/report/report_201102

/bassapp/backapp/data/bass1/export


ls ������ʾ��
-rw-r--r--   1 app      appdb    22969750  3??  2��? 13:05 i_13100_201102_02049_01_001.dat
-rw-r--r--   1 app      appdb        104  3??  2��? 13:05 i_13100_201102_02049_01.verf


ִ�з���                 : bass1_export ģʽ��.����  YYYY-MM[-DD]

key:���������

UNIX ϵͳ�ϵ� LANG ����

����� UNIX ϵͳʹ�� LANG ����ָ�����������Ի�����Ȼ������ͬ�� UNIX ����ϵͳ��Ҫ��ͬ�����Ի���������ָ����ͬ�����ԡ���ȷ��ʹ��������ʹ�õ� UNIX ����ϵͳ��֧�ֵ� LANG ֵ��

Ҫ��ȡ UNIX ϵͳ�����Ի������ƣ��������������ݣ�
locale -a

1.���Զ���
2.����ָ��
3.����У��r
int_main.tcl:set program_name $arg(-s)
                                      

for { set i 0 } {$i < [expr $argc - 1 ]} {incr i} {
	set flag [lindex $argv $i]
	set value [lindex $argv [expr $i + 1] ]
	if { [string index $flag 0] == "-" } {
		set arg($flag) $value
	}
}

set program_name $arg(-s)
set ddh $arg(-u)
set rwh $arg(-v)for { set i 0 } {$i < [expr $argc - 1 ]} {incr i} {
	set flag [lindex $argv $i]
	set value [lindex $argv [expr $i + 1] ]
	if { [string index $flag 0] == "-" } {
		set arg($flag) $value
	}
}

set program_name $arg(-s)
set ddh $arg(-u)
set rwh $arg(-v)                                      for { set i 0 } {$i < [expr $argc - 1 ]} {incr i} {
	set flag [lindex $argv $i]
	set value [lindex $argv [expr $i + 1] ]
	if { [string index $flag 0] == "-" } {
		set arg($flag) $value
	}
}

set program_name $arg(-s)
set ddh $arg(-u)
set rwh $arg(-v)


344	ȡ��ԭҵ��ָ��У���ļ������У�鱨�����	1.5.9	2009-5-20	�սӿ�������������20090601������ֹ�ϴ���
�½ӿ�����������200905����ֹ�ϴ���


cc_flag = 2 :����Ҫִ�е���


getpid()
strrchr()
sprintf()



	set sql_buff "delete  from bass1.G_A_02059_DAY a where exists
								(
									select * from
									(
									select time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,order_date,count(*)
									from bass1.G_A_02059_DAY
									where time_id = $timestamp
									group by time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,order_date
									having count(*)>1
									) as b
								where a.time_id = b.time_id
								  and a.enterprise_id = b.enterprise_id
								  and a.user_id = b.user_id
								  and a.enterprise_busi_type = b.enterprise_busi_type
								  and a.manage_mode = b.manage_mode
								  and a.order_date = b.order_date
								  and a.time_id=$timestamp
								 )"



1.ȡ��02059 1040 ȫ���޸�����
2.����02059 tcl

��׼����:���Ŷ�����ϵ

�������⣺01002 ��set sex_id ='1'������ set sex_id ='2' ��

  set handle [aidb_open $conn]
	set sql_buff "update  bass1.g_a_01002_day
                  set sex_id ='1'
                  where 
                    time_id=$timestamp and  card_type = '101' and 
                    length(card_code) = 18                    and 
                    substr(card_code,17,1) in ('2','4','5','8','0') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle

CREATE FUNCTION BASS1.FN_GET_ALL_DIM(GID VARCHAR(20),DID VARCHAR(20)) 
RETURNS VARCHAR(10) DETERMINISTIC NO EXTERNAL ACTION LANGUAGE SQL 
BEGIN ATOMIC RETURN 
	SELECT BASS1_VALUE 
	FROM BASS1.ALL_DIM_LKP 
	WHERE BASS1_TBID = GID 
	AND XZBAS_VALUE = DID; 
	END


ȥ��һ��:
 select distinct a.enterprise_id from
   (select time_id,enterprise_id,cust_statu_typ_id from G_A_01004_DAY where time_id <= ${timestamp} ) a,
   (select enterprise_id,max(time_id) as time_id  from G_A_01004_DAY 
                                                 where time_id<=${timestamp}  
                                              group by enterprise_id)b
where a.time_id=b.time_id and a.enterprise_id=b.enterprise_id and a.cust_statu_typ_id = '20'

����Ч��һ����

select distinct t.enterprise_id
from 
(
select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn 
from bass1.G_A_01004_DAY 
where time_id <= ${timestamp}   
) t where t.rn = 1 and  cust_statu_typ_id = '20'


EPOCH:

CREATE FUNCTION EPOCH (DB2TIME TIMESTAMP) \
RETURNS INTEGER \
LANGUAGE SQL \
CONTAINS SQL \
DETERMINISTIC \
RETURN CAST (DAYS(DB2TIME) - DAYS('1970-01-01') AS INTEGER) * 86400 + MIDNIGHT_SECONDS(DB2TIME) 
values epoch (current timestamp)


code mod:
02062 :test | fix 
--02054 fix
--02061 : fix 

--td ���� ����
select a.user_id,call_duration_m  from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_20110316
where MNS_TYPE=1
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_20110316 where (td_call_mark =1
            or td_gprs_mark =1
            or td_addon_mark=1)
and userstatus_id in (1,2,3,6,8) and usertype_id in (1,2,9)
and test_mark=0 ) b
on a.user_id=b.user_id 
order by call_duration_m desc

select * from bass2.dw_cust_20110316
where cust_id in
('89102999606167'
,'89103000351204'
,'89103001246538'
,'89103001345719'
,'89103001385579'
,'89103001397437'
,'89103001405597'
,'89103001419261'
,'89103001422002'
,'89103001432717'
,'89103001462079'
,'89103001462973'
,'89103001468028'
,'89103001472114'
,'89103001489911'
,'89103001497535'
,'89103001509522'
,'89103001516985'
,'89103001517803'
,'89103001527146'
,'89103001545645'
,'89103001547288'
,'89103001552042'
,'89103001557903'
,'89103001562754'
,'89103001564041'
,'89103001566045'
,'89103001579025'
,'89103001591016'
,'89103001611814'
,'89103001629021'
,'89160000049132'
,'89160000082895'
,'89160000099010'
,'89160000113502'
,'89160000140593'
,'89160000151569'
,'89160000157640'
,'89160000178619'
,'89160000185091'
,'89160000203492'
,'89160000235117'
,'89160000249193'
,'89160000257335'
,'89160000267185'
,'89160000271155'
,'89160000271384'
,'89160000271591'
,'89160000290183'
,'89160000316164'
,'89160000337360'
,'89160000339409'
,'89160000349980'
,'89160000357178'
,'89160000360284'
,'89160000369411'
,'89160000375348'
,'89160000385956'
,'89160000389268'
,'89160000392332'
,'89160000452947'
,'89160000465467'
,'89160000472349'
,'89160000472517'
,'89160000491753'
,'89160000492591'
,'89160000496262'
,'89160000509758'
,'89160000511582'
,'89160000521425'
,'89160000552112'
,'89160000560946'
,'89160000561144'
,'89160000562463'
,'89160000563182'
,'89160000569568'
,'89160000570118'
,'89160000573651'
,'89160000574154'
,'89160000581440'
,'89160000585242'
,'89160000591485'
,'89160000594576'
,'89160000595950'
,'89160000595960'
,'89160000596559'
,'89160000598156'
,'89160000598167'
,'89160000602571'
,'89160000604436')


--�����ͻ���һ���� �о�

-->>ע�������ű�


$ ./bass1_report
У�鱨�洦�����ʼ!
�û���:app,����:app,���ݿ���:BASSDB,�����ļ�Ŀ¼:/bassapp/backapp/data/bass1/report
mkdir: ����Ŀ¼ "/bassapp/backapp/data/bass1/report/report" ʧ�ܣ��ļ��Ѿ�����
���ؼ��鱨���ļ�!
/data1/asiainfo/interface/report/
/bassapp/backapp/data/bass1/report/report
Interactive mode off.
Local directory now /bassapp/backapp/data/bass1/report/report
*.verf: �޴��ļ���Ŀ¼
��ȡ���鱨����Ϣ!
/bassapp/backapp/data/bass1/report/report/f_?_13100_????????_*_??_???.verf: �޴��ļ���Ŀ¼
/bassapp/backapp/data/bass1/report/report/r_?_13100_????????_*_??.verf: �޴��ļ���Ŀ¼
/bassapp/backapp/data/bass1/report/report/o_?_13100_????????_*_??.verf: �޴��ļ���Ŀ¼
/bassapp/backapp/data/bass1/report/report/b_13100_????????_??.verf: �޴��ļ���Ŀ¼
/bassapp/backapp/data/bass1/report/report/f_?_13100_??????_*_??_???.verf: �޴��ļ���Ŀ¼
/bassapp/backapp/data/bass1/report/report/r_?_13100_??????_*_??.verf: �޴��ļ���Ŀ¼
/bassapp/backapp/data/bass1/report/report/o_?_13100_??????_*_??.verf: �޴��ļ���Ŀ¼
/bassapp/backapp/data/bass1/report/report/b_13100_??????_p_??.verf: �޴��ļ���Ŀ¼
ȡ���б�����Ϣ����[�ļ���:0]

У�鱨�洦��������,ϵͳ�˳�!
$ pwd
/bassapp/backapp/bin/bass1_report


1324 zsched
->
2778 /usr/lib/inet/inetd start
->
21040 /usr/sbin/in.telnetd
->
21056 -sh
->
21163 ksh
->
11485 ps -f


autoftp(){
HOME=/bassapp/bass2/panzw2
export HOME
ftp -v 172.16.9.25
HOME=/bassapp/bass2
export HOME
echo $$
}

autoftp(){
HOME=/bassapp/bihome/panzw
export HOME
ftp -v 172.16.9.25
HOME=/bassapp/bass1
export HOME
echo $$
}



putdatfile(){
FTPHOST=172.16.9.25
REMOTE_DIR=data
LOCAL_DIR=/bassapp/bihome/panzw
HOME=/bassapp/bihome/panzw
export HOME

ftp_mac_put_dat_file=${HOME}/put_dat.mac.ftp
ftped_file_list=${HOME}/ftped_dat.lst
#����ftp�����ļ�
echo "cd ${REMOTE_DIR}" > ${ftp_mac_put_dat_file}
echo "bin" > ${ftp_mac_put_dat_file}
echo "prompt off" > ${ftp_mac_put_dat_file}
echo "mput *.dat" > ${ftp_mac_put_dat_file}
echo "ls -lrt" >> ${ftp_mac_put_dat_file}
echo "dir *.dat ${ftped_file_list}" >> ${ftp_mac_put_dat_file}
#�ϴ�
ftp -v ${FTPHOST} < ${ftp_mac_put_dat_file}

#�뱾��У�飺�ļ���|�ļ���|�ļ���С


#�ָ�$HOME
HOME=/bassapp/bass1
export HOME
echo $$
}



putverffile(){
FTPHOST=172.16.9.25
REMOTE_DIR=data
LOCAL_DIR=/bassapp/bihome/panzw
HOME=/bassapp/bihome/panzw
export HOME

ftp_mac_put_verf_file=${HOME}/put_verf.mac.ftp


#����ftp�����ļ�
echo "cd ${REMOTE_DIR}" > ${ftp_mac_put_verf_file}
echo "bin" >> ${ftp_mac_put_verf_file}
echo "prompt off" >> ${ftp_mac_put_verf_file}
echo "mput *.verf" >> ${ftp_mac_put_verf_file}
echo "ls -lrt" >> ${ftp_mac_put_verf_file}
#�ϴ�
if [ -f ${ftp_mac_put_dat_file} ];then 
ftp -v ${FTPHOST} <  ${ftp_mac_put_verf_file}
rm ${ftp_mac_put_dat_file}
else 
echo "dat �ļ�δ�ϴ��������ϴ�dat���ٴ�verf!!"
return 1
fi

#�ָ�$HOME
HOME=/bassapp/bass1
export HOME
echo $$
}



���ȼ�:
1.9��ӿ�
1.1 9�㱨��ӿ�
2.


���⣺
��¼������Ӱ�쵽�ظ���
02012 ȡ�� ����
9��ӿ�У��
app�û��ܳ�������

�����·��嵥
ά����
�Զ��ϴ�

INTERFACE_LEN_02059="1 15,16 17,18 31,32 32,33 46,47 47,48 53"
len_val=${INTERFACE_LEN_02059}
DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/${datafilename} of asc modified by timestampformat=\\\"YYYYMMDDHHMMSS\\\" dateformat=\\\"YYYYMMDD\\\"  timeformat=\\\"HHMMSS\\\" method L (${len_val}) insert into ${table_name} nonrecoverable\""


INTERFACE_LEN_99008="1 10,11 30,31 34,35 42"

XZBOSS    XZBOSS              204 
929000    2��D??DD?            300 
1860      admin-1860          204 

 89102999523639      89157331866928      1140320080530113100  


ls sort 


ls -l *.dat|sort -k5,5 -k9,9



for dir in export_201101??
do 
cd  $dir
ls -l *.dat|sort -k5,5 -k9,9|grep "appdb          0  "|wc -l
cd ..
done
      19
      19
      20
      17
      18
      19
      17
      19
      19
      18
      19
      18
      19
      18
      20
      19
      18
      19
      17
      19
      18
      19
      20
      19
      19
      19
      19
      17
      19
      19
      17

for dir in export_201102??
do 
cd  $dir
ls -l *.dat|sort -k5,5 -k9,9|grep "appdb          0  "|wc -l
cd ..
done      
      18
      19
      20
      20
      20
      20
      20
      19
      19
      19
      19
      17
      19
      19
      18
      17
      18
      18
      19
      19
      17
      19
      19
      19
      18
      19
      19
      17
      
for dir in export_201103??
do 
cd  $dir
ls -l *.dat|sort -k5,5 -k9,9|grep "appdb          0  "|wc -l
cd ..
done  

      19
      17
      19
      19
      19
      20
      20
      20
      20
      20
      18
      17
      18
      18
      18
      17
      15
      16
      20
      19
      17
      16
      18
      
for dir in export_201012??
do 
cd  $dir
ls -l *.dat|sort -k5,5 -k9,9|grep "appdb          0  "|wc -l
cd ..
done             


ailknfjdbass99##%%

db2 "select * from app.sch_control_before " > sch_control_before.txt

grep -i BASS1_INT_0400810_YYYYMM.tcl sch_control_before.txt

db2 "
select * from  app.sch_control_alarm \
where alarmtime >=  current timestamp - 60 days \
and control_code = 'BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl' \
order by alarmtime desc with ur
"

���ҷָ����ĸ���
zcat I1700220110308000000.AVL.Z | head -1 | awk -F'$' '{print NF}'
���ҷָ�������ȷ����Ŀ������
zcat I1700220110308000000.AVL.Z | head -1 | awk -F'$' '{if(NF!=18) print NR}'
��ʾָ������
zcat I1700220110308000000.AVL.Z | sed -n '12,13p'


--�Ĵ���
--imei
--107/108
--bass1_list
�Ż�У�����
05001��05002 �ַ��ṩ


db2 runstats on table bass1.g_i_02021_month_temp1 with distribution and detailed indexes all


db2 list application


db2 connect to bassdb user bass2 using bass2
db2 "export to /bassapp/bass2/bak_xufr/sms_sender891.txt of del select distinct product_no from dw_newbusi_sms_201010 a where a.call
type_id = 0 and not exists (select 1 from (select user_id from tmp_black_user_20101105 union select distinct product_instance_id use
r_id from ODS_PRODUCT_INS_PROD_RED_20101031) b where a.user_id = b.user_id) and a.city_id = '891' order by product_no"
db2 terminate



alias bin='cd /bassdb2/etl/E/boss/java/crm_interface/bin'
alias pzh='cd /bassdb2/etl/E/panzw2'
alias lo='cd /bassdb2/etl/L/boss'
alias cfg='cd /bassdb2/etl/E/boss/java/crm_interface/bin/config/BOSS'
alias bak='cd /bassdb2/etl/L/boss/backup'
alias vlog='/bassdb2/etl/E/panzw2/ViewLoadLog_bassdb46.sh'

�ӿ��ش�����
1.��¼�����ش���ֱ�����͡�һ��۷֡�
2.��¼��δ���أ�������͡�����ֱ���ش������Ա���۷֡�

06002	У԰λ����Ϣ
#�����¼������û��ҵ����˽ӿ���ʱ�Ϳ��ļ�

G_A_06001_DAY.tcl:#�����¼������û��ҵ����˽ӿ���ʱ�Ϳ��ļ�
G_I_02017_MONTH.tcl:#�����¼������û��ҵ����˽ӿ���ʱ�Ϳ��ļ�
G_I_06002_MONTH.tcl:#�����¼������û��ҵ����˽ӿ���ʱ�Ϳ��ļ�
G_S_22401_MONTH.tcl:#�����¼������û��ҵ����˽ӿ���ʱ�Ϳ��ļ�
G_S_22403_DAY.tcl:#�����¼������û��ҵ����˽ӿ���ʱ�Ϳ��ļ�




�����ݣ�
3��ǰ�ӿڣ�
ls -alrt *02049*dat *02053*dat *03001*dat *03002*dat *03003*dat *06011*dat *06012*dat *06029*dat

5��ǰ�ӿڣ�
ls -alrt *01005*dat *02005*dat *02014*dat *02015*dat *02016*dat *02047*dat *06021*dat *06022*dat *06023*dat *22009*dat *22101*dat *22103*dat *22105*dat *22106*dat *06002*dat
8��ǰ�ӿڣ�
ls -alrt *02006*dat *02007*dat *02052*dat *03004*dat *03005*dat *03012*dat \
*03015*dat *03016*dat *03017*dat *03018*dat *21003*dat *21006*dat *21008*dat \
*21011*dat *21012*dat *21020*dat *22204*dat *22036*dat *22040*dat *22072*dat \
*22303*dat *22304*dat *22305*dat *22306*dat *22307*dat *02017*dat *22401*dat 

10��ǰ�ӿڣ�
ls -alrt  *03007*dat *21010*dat *21013*dat *21014*dat *21015*dat *05001*dat \
*05002*dat *05003*dat *22013*dat *22021*dat *22025*dat *22032*dat *22033*dat \
*22039*dat *22041*dat *22042*dat *22043*dat  

15��ǰ�ӿڣ�
ls -lart *22049*dat *22050*dat *22052*dat *22055*dat *22056*dat *22061*dat \
*22062*dat *22063*dat *22064*dat *22065*dat  

ls -lart *22049*verf *22050*verf *22052*verf *22055*verf *22056*verf *22061*verf \
*22062*verf *22063*verf *22064*verf *22065*verf  



ls -lrt *01002*dat *01004*dat *02004*dat *02008*dat *02011*dat *02053*dat *06031*dat *06032*dat|awk '{print $9,$8,$5}'|sort



--��¼�����
--ǰ������
--���ȱ�־




/bassapp/bass1/trace


select * from app.g_file_report where filename like '%20110401%02013%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04002%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04003%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04006%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04007%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04015%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04016%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04018%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%22038%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%22073%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%22102%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%22104%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04008%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04009%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04010%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04011%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04012%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%06001%' and err_code = '00'



-rw-r--r--   1 app      appdb      35112  4��  1�� 12:07 i_13100_201103_06029_00_001.dat
-rw-r--r--   1 app      appdb        104  4��  1�� 12:07 i_13100_201103_06029_00.verf
-rw-r--r--   1 app      appdb       3422  4��  1�� 12:08 i_13100_201103_06011_00_001.dat
-rw-r--r--   1 app      appdb        104  4��  1�� 12:08 i_13100_201103_06011_00.verf
-rw-r--r--   1 app      appdb       5336  4��  1�� 12:08 i_13100_201103_06012_00_001.dat
-rw-r--r--   1 app      appdb        104  4��  1�� 12:08 i_13100_201103_06012_00.verf
-rw-r--r--   1 app      appdb    48398985  4��  1�� 12:10 i_13100_201103_02053_00_001.dat
-rw-r--r--   1 app      appdb        104  4��  1�� 12:10 i_13100_201103_02053_00.verf
-rw-r--r--   1 app      appdb          0  4��  1�� 12:12 s_13100_201103_22040_00_001.dat
-rw-r--r--   1 app      appdb        104  4��  1�� 12:12 s_13100_201103_22040_00.verf
-rw-r--r--   1 app      appdb          0  4��  1�� 12:17 s_13100_201103_22042_00_001.dat
-rw-r--r--   1 app      appdb          0  4��  1�� 12:17 s_13100_201103_21010_00_001.dat
-rw-r--r--   1 app      appdb        104  4��  1�� 12:17 s_13100_201103_22042_00.verf
-rw-r--r--   1 app      appdb        104  4��  1�� 12:17 s_13100_201103_21010_00.verf
-rw-r--r--   1 app      appdb    67769352  4��  1�� 12:19 i_13100_201103_03003_00_001.dat
-rw-r--r--   1 app      appdb        104  4��  1�� 12:19 i_13100_201103_03003_00.verf
-rw-r--r--   1 app      appdb    445401774  4��  2�� 06:14 i_13100_201103_03002_00_001.dat
-rw-r--r--   1 app      appdb        104  4��  2�� 06:14 i_13100_201103_03002_00.verf
-rw-r--r--   1 app      appdb    22125250  4��  2�� 07:03 i_13100_201103_02049_00_001.dat
-rw-r--r--   1 app      appdb        104  4��  2�� 07:03 i_13100_201103_02049_00.verf
-rw-r--r--   1 app      appdb         95  4��  2�� 12:06 s_13100_201103_22105_00_001.dat
-rw-r--r--   1 app      appdb        104  4��  2�� 12:06 s_13100_201103_22105_00.verf
-rw-r--r--   1 app      appdb        145  4��  2�� 12:27 s_13100_201103_22101_00_001.dat
-rw-r--r--   1 app      appdb        104  4��  2�� 12:27 s_13100_201103_22101_00.verf
-rw-r--r--   1 app      appdb        110  4��  2�� 12:35 s_13100_201103_22103_00_001.dat
-rw-r--r--   1 app      appdb        104  4��  2�� 12:35 s_13100_201103_22103_00.verf
-rw-r--r--   1 app      appdb        110  4��  2�� 12:35 s_13100_201103_22106_00_001.dat
-rw-r--r--   1 app      appdb        104  4��  2�� 12:35 s_13100_201103_22106_00.verf


1.�µ��ȣ�
����������excel
�����ɽӿ�-У��-������˳��
��ص��ȵ�ִ���������ʱ�䲻��Ҫ���������
--2��12���ӿڡ�
--02053 У�� ʱ��



route add -p 10.233.0.0 mask 255.255.0.0 10.233.20.158
route add -p 10.10.0.0 mask 255.255.0.0 10.233.20.158
route add -p 172.16.0.0 mask 255.255.0.0 10.233.20.158




select count(0),count(distinct product_no) 
,sum(value(int(FLOWUP),0)/1024/1024+value(int(FLOWDOWN),0)/1024/1024)
from   bass1.G_S_04003_DAY
where time_id between  20101201 and 20101231

#task����ignore����9��ӿ��ӳ�ʱ��������
#task��ؼ�¼���������
#--�ֿ�ͳ�Ƽ�¼������
#��ؼ�¼��
#������δ������
#�����״̬�����8��30ǰ��û�ϴ��ӿڣ��澯��������Ҳ�������ѡ�

splfile s_13100_201103_21003_01_001 7300000 &



db2 "load from /dev/null of del terminate into  bass1.T_GS05001M"  
db2 "load from /dev/null of del terminate into  bass1.T_GS05002M"  

bass1.T_GS05002M

д��ؽű������µĿ����ĵ�



  puts "���뵽������Ϣ����ͷ"
        set sqlbuf "insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,MOBILE_NUM) 
                                                        select '��������${timestamp}����------һ���ӿ�����:${RESULT_VAL1}��,����9��ӿ���:${RESULT_VAL3}��,�����ܹ����ɵĽӿ���:${RESULT_VAL2}����',
                                                        phone_id 
                                                        from BASS2.ETL_SEND_MESSAGE where MODULE='BASS1'"
        exec_sql $sqlbuf
  puts "���뵽������Ϣ����ͷ�ɹ�"
  
  
   panzw 13737  6979   0 17:13:42 pts/31      0:00 sh bass1_mon.sh
   nohup sh bass1_mon.sh > bass1_mon.sh.out 2>&1 &
   
   
	puts [ exec date +%H%M%S ]

   
   
		#set sql_buff "
		#			create table bass1.G_S_22062_MONTH_TMP_1bak like session.G_S_22062_MONTH_TMP_1
		#			DATA CAPTURE NONE
		#			IN TBS_APP_BASS1
		#			INDEX IN TBS_INDEX
		#			PARTITIONING KEY (CHANNEL_ID,ACCEPT_TYPE) USING HASHING
		#  "
    #puts $sql_buff
    #exec_sql $sql_buff  

		#set sql_buff "
		#			insert into  bass1.G_S_22062_MONTH_TMP_1bak select * from session.G_S_22062_MONTH_TMP_1
		#  "
    #puts $sql_buff
    #exec_sql $sql_buff      

 ͳ�Ʒ���>Brio���ط�������>���ſͻ���>2010�꼯�ſͻ�ҵ��ͳ�Ʊ���
--�Ż�tcl
--�޸���������

int -s G_S_03018_MONTH.tcl > /bassapp/bihome/panzw/tmp/tclrunlog/G_S_03018_MONTH.tcl.`date +%Y%m%d%H%M%S`        


--����У�飡Ŀ¼��

--Ӧ������(�սӿڣ�ȫ���ӿ�,��������)
--Ԥ������


/bassapp/backapp/data/bass1/export/ftplog

/bassapp/bass1/trace/G_S_03017_MONTH.trace

/bassapp/bass1/log/G_S_03018_MONTH.log



02053 �գ��½ӿں��ظ���
ailknfjdbass99##%%


a_13100_20110417_01004_00_001.dat 02:57 9362400
a_13100_20110417_01002_00_001.dat 03:09 477704
a_13100_20110417_02053_00_001.dat 03:11 12895680
a_13100_20110417_02011_00_001.dat 03:14 386022
a_13100_20110417_02004_00_001.dat 03:28 325501
a_13100_20110417_02008_00_001.dat 03:28 1237702
i_13100_20110417_06031_00_001.dat 05:08 6024
i_13100_20110417_06032_00_001.dat 05:08 945


a_13100_20110418_01002_00_001.dat 02:55 500070
a_13100_20110418_01004_00_001.dat 02:58 9362400
a_13100_20110418_02053_00_001.dat 02:58 12898720
a_13100_20110418_02011_00_001.dat 02:59 422772
a_13100_20110418_02004_00_001.dat 03:08 356870
a_13100_20110418_02008_00_001.dat 03:08 1252356
i_13100_20110418_06032_00_001.dat 05:07 945
i_13100_20110418_06031_00_001.dat 05:07 6024


��ֵ�Ż�
xiaofeixianjing


for file in *.tcl
do 
echo "`wc -l $file` ""`grep -i "#" $file | wc -l|awk -F' ' '{print $1}' ` $file">>/bassapp/bihome/panzw/tmp/tmp$$.txt
done



              if [  ${exp_cnt} -eq  56 ];then 
                      MESSAGE_CONTENT="`date +%H:%M:%S` | 56���ӿ�ȫ������!"
                      echo ${MESSAGE_CONTENT}
                      sendalarmsms ${MESSAGE_CONTENT}
              fi
              
              
              

while read x y
do
if [ $y = "d" ];then 
echo "�սӿ�"
dt=yyyymmdd
echo ${x}_${dt}
fi

if [ $y = "m" ];then 
echo "�½ӿ�"
dt=yyyymm
echo ${x}_${dt}
fi
done<t.txt




while read filename
do
coarse=`echo $filename|awk -F'_' '{print $1}'`
code=`echo $filename|awk -F'_' '{print $4}'`
dt_type=`echo $filename|awk -F'_' '{print length($3)}'`
if [ $dt_type -eq 8 ];then
tabname=g_${coarse}_${code}_day
else
tabname=g_${coarse}_${code}_month
fi
echo $tabname
done<<!
s_XXXXX_yyyymmdd_22080_XX_XXX.dat
s_XXXXX_yyyymm_22081_XX_XXX.dat
s_XXXXX_yyyymmdd_22082_XX_XXX.dat
s_XXXXX_yyyymm_22083_XX_XXX.dat
s_XXXXX_yyyymmdd_22084_XX_XXX.dat
s_XXXXX_yyyymm_22085_XX_XXX.dat
i_XXXXX_yyyymmdd_02022_XX_XXX.dat
i_XXXXX_yyyymmdd_02023_XX_XXX.dat
!
              
              
D:\Databases\IBM\SQLLIB\samples\c


sprintf
strcpy
strncpy

����sprintf ��printf ���÷��ϼ���һ����ֻ�Ǵ�ӡ��Ŀ�ĵز�ͬ���ѣ�ǰ�ߴ�ӡ���ַ����У�
������ֱ�������������������Ҳ����sprintf ��printf ���õöࡣ���Ա������ؽ���sprintf����ʱ
Ҳ����������pritnf��
sprintf �Ǹ���κ������������£�
int sprintf( char *buffer, const char *format [, argument] ... );
����ǰ�����������͹̶��⣬������Խ������������������ľ�������Ȼ���ڵڶ���������
��ʽ���ַ����ϡ�



sprintf �����Ӧ��֮һĪ���ڰ�������ӡ���ַ����У����ԣ�spritnf �ڴ�������Ͽ������
itoa���磺
//������123 ��ӡ��һ���ַ���������s �С�
sprintf(s, "%d", 123); //����"123"
����ָ����ȣ��������߲��ո�
sprintf(s, "%8d%8d", 123, 4567); //������" 123 4567"
��ȻҲ��������룺
sprintf(s, "%-8d%8d", 123, 4567); //������"123 4567"
Ҳ���԰���16 ���ƴ�ӡ��
sprintf(s, "%8x", 4567); //Сд16 ���ƣ����ռ8 ��λ�ã��Ҷ���
sprintf(s, "%-8X", 4568); //��д16 ���ƣ����ռ8 ��λ�ã������

strrchr(str,'/') �൱�� substr(str,n)       n �� / ��λ�á�


������ sqlca.sqlcode = 0 ��ʲô��˼�������ѣ���¼һ��sqlca.sqlcode�ĸ���ȡֵ�����壺

    0 �������һ��sql���ִ�гɹ� 

   -1 �������һ��sql���ִ��ʧ�� 

100 �������һ��sql���û�з�������


http://xiongfeng.iteye.com/blog/627405

memset  ����ʼ��������
1��void *memset(void *s,int c,size_t n) ����
�ܵ����ã����ѿ����ڴ�ռ� s ���� n ���ֽڵ�ֵ��Ϊֵ c�� 
http://baike.baidu.com/view/982208.htm#sub982208




 http://132.32.22.9/public/cmschema/N10_New_WXKH/WXKH_ViewMonth_Total.asp?Date=2010-7


user:    chinamobile\cs891   
pwd:   abcd@1234 


route add 132.32.22.0 mask 255.255.255.0 10.233.20.113 

alter table TABLENAME ALTER column columnName SET DATA TYPE varchar(10000)

TABLENAME:����

columnName:����

free_res_val1
���յľ����շѵģ��ǿվ������
is null �շ�
is not null ���


��ռ���ײ���������������� free_res_val1 is null 

�ײ���:
���
is not null 

�ײ���:
--------------------------------------
1.���������
1.1�ײ���������free_res_val1 is not null and sum(charge1+charge2+charge3+charge4)/10 = 0  
1.2���ײ��ڵ����������free_res_val1 is  null 
2.�շ�������
2.1���ײ�������sum(charge1+charge2+charge3+charge4)/10 > 0 
2.2�ײ���������
--------------------------------------

2��ͨ�ŷѣ��ͻ�ʹ���ֻ���ҵ�������ͨ�ŷѣ���������з��á� GPRS���õȣ����������ʷѱ�׼ִ�С� 
4��Ϊʲô�����ֻ������ŵ�ʱ�������GPRS�����ѣ� 
����������£��ͻ��ڹ��ڽ����ֻ������Ų�Ʒ�ǲ������GPRS�����ѵģ���������ͻ�����ʱ�������ֻ������ţ��������������� GPRS�����ѣ��ͻ������ڳ���ǰ����10086�����˶�����ͣ���ͻ������󣬻��յ�������Ѷ��ţ������ϣ���ڹ�������ֻ������ţ��ɰ��ն�����ʾ���ݽ��в������߲�����ѿͷ�����+8613800100186���д��� 


WITH TEST(NAME_TEST, BDAY_TEST) AS   
(   
VALUES ('����','1997-7-1'),('����','1949-10-1')   
)   
SELECT NAME_TEST FROM TEST WHERE BDAY_TEST='1949-10-1'  
WITH TEST(NAME_TEST, BDAY_TEST) AS
(
VALUES ('����','1997-7-1'),('����','1949-10-1')
)
SELECT NAME_TEST FROM TEST WHERE BDAY_TEST='1949-10-1' 

--22085 ���ܴ��յġ�

while read filename
do
touch $filename.tcl
done<<!
G_S_22080_DAY
G_S_22081_MONTH
G_S_22082_DAY
G_S_22083_MONTH
G_S_22084_DAY		
G_S_22085_MONTH
G_I_02022_DAY
G_I_02023_DAY
!
drwxr-xr-x    2 500      503          4096 Apr 01 09:13 sample




/bassapp/backapp/bin/bass1_export/bass1_export bass1.G_S_02007_MONTH 2011-01 &
/bassapp/backapp/bin/bass1_export/bass1_export bass1.G_S_02007_MONTH 2011-02 &
/bassapp/backapp/bin/bass1_export/bass1_export bass1.G_S_02007_MONTH 2011-03 &




db2 connect to bassdb user bass2 using bass2

db2 "load client from /bassdb2/etl/L/boss/error/I0202520110503000000.AVL of del \
modified by coldel$   nochardel timestampformat=\"YYYYMMDDHHMMSS\" \
fastparse anyorder warningcount 1000 \
messages ./msg/ODS_AS_WORK_ACCEPT_20110503.log \
replace into ODS_AS_WORK_ACCEPT_20110503"




insert into app.sch_control_before
select 'BASS1_G_S_22062_MONTH.tcl','BASS2_Dw_channel_local_busi_ds.tcl' from bass2.dual




while read application_id
do
comm="db2 'get snapshot for application agentid "${application_id}"' | grep -i \"$1\""
echo $comm
eval $comm
done<<!
         `db2 "list applications" | awk '{print $3}'`
!


http://publib.boulder.ibm.com/infocenter/iseries/v5r3/index.jsp?topic=%2Fdb2%2Frbafzmstcksql.htm


 
$ db2advis -d bassdb -s "select count(0) from    bass2.dw_product_201004" -t 5 

Using user id as default schema name. Use -n option to specify schema
CALL SYSPROC.GET_DBSIZE_INFO failed, sqlcode = -443. Getting database size from the catalog tables.
execution started at timestamp 2011-05-06-18.35.04.315400
Recommending indexes...
I'm having trouble processing the following query, statement [1] in the workload

Please check the syntax:
SQL: <select count(0) from    bass2.dw_product_201004>

Database is reporting the following error:

error.  The db2advis tool needs the explain tables and
        the advise tables to exist.  Please create them
        using the EXPLAIN.DDL script in the misc subdirectory
        of the sqllib directory.

0 solutions were evaluated by the advisor

exiting with error code [-220]


$ db2 ? SQL1092N
SQL1092N "<authorization-ID>" does not have the authority to 
          perform the requested command.  
          
          