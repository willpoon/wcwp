open 
http://172.16.5.71:4321/svn/%e9%a1%b9%e7%9b%ae%e7%bb%84%e5%9f%b9%e8%ae%ad%e8%b5%84%e6%96%99/%e9%a1%b9%e7%9b%ae%e7%bb%84%e8%b5%84%e6%96%99/%e5%ad%a6%e4%b9%a0%e8%b5%84%e6%96%99/

up:
http://172.16.5.71:4321/svn/%e9%a1%b9%e7%9b%ae%e7%bb%84%e5%9f%b9%e8%ae%ad%e8%b5%84%e6%96%99/%e9%a1%b9%e7%9b%ae%e7%bb%84%e8%b5%84%e6%96%99/



XZBI\项目组培训资料\项目组资料\学习资料

XZBI/项目组培训资料/项目组资料/学习资料


http://172.16.5.71:4321/svn/%e9%a1%b9%e7%9b%ae%e7%bb%84%e5%9f%b9%e8%ae%ad%e8%b5%84%e6%96%99/%e9%a1%b9%e7%9b%ae%e7%bb%84%e8%b5%84%e6%96%99/%e5%ad%a6%e4%b9%a0%e8%b5%84%e6%96%99/


 my_pass=bass2
 db2 "connect to bassdb user bass2 using ${my_pass}"
 
 
 select * from etl_task_running 
where	task_id	in	('I03013')	

select * from etl_task_log  
where	task_id	in	('I03013')	

 
	set ip_num ('17950','17951')
	
移动话段
substr(product_no,1,3) in ('135','136','138','139','147','150','152','157','158','182','187','188')
and length(rtrim(ltrim(product_no)))=11

	

db2 => select * from DIM_FEETYPE_ITEM

ITEM_ID     ITEM_NAME                                                                                            FEETYPE_ID  FEETYPE_NAME                                                                                        
----------- ---------------------------------------------------------------------------------------------------- ----------- ----------------------------------------------------------------------------------------------------
SQL0668N  Operation not allowed for reason code "3" on table 
"BASS2.DIM_FEETYPE_ITEM".  SQLSTATE=57016
db2 => ? 57016

SQLSTATE 57016: The table cannot be accessed, because it is inactive.

	
	
	

       opp_noaccess_number  varchar(30),       --去IP头的对端号码
       opp_roam_areacode    varchar(7),        --对端漫游位置区号
       opp_home_areacode    varchar(7),        --对端归属长途区号
       
svn://172.16.5.71/XZBI

XZBI\NG1-BASS1.0\Truck\05 工具库

E:\SVN\NG1-BASS1.0\Truck\03 工作库\interface\抽取\doc
BOSS接口规划v1.00.xls
E:\SVN\NG1-BASS1.0\Truck\03 工作库\interface\加载\doc
ODS建表脚本.sql

张振清为亚信设计了“软件产品+专业服务”的发展模式。

　　亚信移动事业部北方区副总经理孙明洁是亚信13年老员工，比张振清还早三年进入亚信。
她是张振清说服过来开创PSO(Professional Service Organization，专业服务机构)的第一人。孙明洁之前在研发部，当时由于没有专门的服务部门给客户服务，所有的问题全部集中到研发部。这样导致研发部人员都在现场，无人做研发，对后续软件的研发非常不利。通过对PSO部门的创办，亚信同时保障了客户服务质量和产品研发的集中度。


NG1-BASS1.0/Truck/05 工具库


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


--注册节点
catalog tcpip node CQCRM remote 10.191.113.132 server 50000;
--注册数据库
catalog database CQCCDW at node CQCRM;
--删除注册节点
uncatalog node CQCRM;
--删除注册数据库
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



Ethernet adapter 本地连接:



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

        Lease Obtained. . . . . . . . . . : 2010年10月13日 8:55:24

        Lease Expires . . . . . . . . . . : 2010年10月13日 16:55:24



Ethernet adapter 无线网络连接:



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





XZBI\NG1-BASS1.0\Truck\05 工具库

172.16.9.25 bass2 / bass2


% puts $env(AITOOLSPATH) 
/bassapp/tcl/aiomnivision/aitools


if [ $? -eq 0 ] ; then
        #增加对app.sch_control_runlog标志的更新add by xufr at 2009-1-6 21:25:03
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



　create table zjt_tables as 

　　(select * from tables) definition only; 


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
		,QUEUE_ID					varchar(20)				--	队列ID
		,BUSINESS_ID				bigint					--	操作ID
		,CONFIG_ID					bigint					--	配置ID
		,BUSI_ORDER_ID				bigint					--	操作订单ID
		,BILL_ID					varchar(30)				--	手机号码
		,FILE_CONTENT				VARCHAR(1024)			--	文件内容
		,SUCC_FLAG					smallint				--	成功标志
		,ERROR_MSG					VARCHAR(1000)			--	错误信息
		,ERROR_STACK				VARCHAR(4000)			--	错误栈信息
		,DONE_CODE					bigint					--	操作编码
		,DONE_DATE					TIMESTAMP				--	结束日志
		,CREATE_DATE				TIMESTAMP				--	创建时间
		,VALID_DATE					TIMESTAMP				--	生效时间
		,EXPIRE_DATE				TIMESTAMP				--	失效时间
		,OP_ID						bigint					--	操作员ID
		,ORG_ID						bigint					--	操作员组织ID
		,REGION_ID					VARCHAR(6)				--	地区编码
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
文件名$文件大小$记录数$数据日期$创建时间


1.db2 漏加载记录



replace(replace(CONT_ADDR,chr(10)),chr(13))

replace(replace(replace(cust_name,chr(10)),chr(13)),'$','')




/**	2010/11/2 18:02	added by  panzhiwei		**/	
CREATE TABLE DWD_NG2_I03013_YYYYMMDD (					
        CHANNEL_ID          BIGINT              --渠道标识:
        ,CHANNEL_NAME        VARCHAR(100)        --渠道名称:
        ,PARENT_CHANNEL_ID   INTEGER             --上级渠道标识:
        ,CHANNEL_LEVEL       INTEGER             --渠道级别:与“渠道级别”接口单元对应
        ,CHANNEL_DESC          VARCHAR(200)        --渠道描述:
        ,START_DATE          TIMESTAMP           --启用日期:
        ,EXPIRE_DATE         TIMESTAMP           --截止日期:缺省29991231
        ,REGION_CODE         VARCHAR(8)          --归属区域编号:与“地域08039”接口单元对应
        ,ORGANIZE_ID         BIGINT              --所属单位标识:与“单位”接口单元对应
        ,CHANNEL_TYPE        INTEGER             --渠道类别:与“渠道类型”接口单元对应
        ,PROPERTY_SRC_TYPE   INTEGER             --渠道物业形态:包括上市公司够建、存续企业购建、租赁、转租-店中店
        ,REG_FUND            BIGINT              --投资规模:
        ,OPEN_DATE           TIMESTAMP           --开业时间:
        ,RESPNSR_ID             INTEGER             --渠道负责人编号:与“渠道负责人”接口单元对应
        ,TEL_NUMBER          VARCHAR(20)         --渠道办公电话:
        ,FAX_NUMBER          VARCHAR(20)         --传真号码:
        ,POST_CODE           VARCHAR(20)            --邮编:
        ,BUSI_BEGIN_TIME     VARCHAR(20)         --营业时间:
        ,CHANNEL_STYLE       INTEGER             --渠道运营方式:"0-自建自营（自办）；1-自建他营（合作）；2-他建他营（特许）"
        ,CHANNEL_LEVEL       INTEGER             --渠道星级:"0-一星级；1-二星级"
        ,CHANNEL_USER_ID     INTEGER             --渠道租赁人编号:与“渠道租赁人”接口单元对应
        ,OWNER_TYPE          SMALLINT            --物业购置方式:
        ,PWNER_PRICE         INTEGER             --物业购置价格:
        ,FITMENT_PRICE       INTEGER             --装修投入:
        ,TRANSFER_PRICE      INTEGER             --传输投入:
        ,INVESTOR            VARCHAR(20)         --投资主体:
        ,LICENSE_NUMBER      VARCHAR(20)          --工商号:
        ,INTERNET_MODE       INTEGER             --接入方式:"0-光缆；1-2M电缆；2-GPRS；3-CDS；4-拨号上网；5-无线网桥；6-无"
        ,CREATE_DATE         TIMESTAMP           --建档时间:
        ,CR_OP_ID            BIGINT              --建档员工:
        ,OP_ID               BIGINT              --操作员工:
        ,DONE_DATE           TIMESTAMP           --操作时间:
        ,CHANNEL_STATE       SMALLINT            --渠道状态:"0-正常；1-预解约；2-注销"
        ,NOTES               VARCHAR(200)            --备注
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING;


 交谈中请勿轻信汇款、中奖信息，勿轻易拨打陌生电话。

 刘智龙  18:09:11
									#生成校验文件
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


="FROM "&接口列表2!B213&"."&接口列表2!C213&SUBSTITUTE(SUBSTITUTE(UPPER(#REF!D3),"X","0$FEE_AREA$",1),"YYMM","$YYMM$",1)


漏加 \ 的地方会报 "java.sql.SQLException: ORA-00936: missing expression" 错误



/**	2010-11-5 15:21	added by  panzhiwei		**/		
CREATE TABLE DWD_NG2_M03023_YYYYMM (						
        CHANNEL_ID          BIGINT              --渠道标识
        ,STORE_AREA         INTEGER             --营业厅面积
        ,GEOGRAPHY_PROPERTY INTEGER             --地理位置属性
        ,LONGITUDE          BIGINT                  --经度
        ,LATITUDE           BIGINT                  --纬度
        ,COVER_BUSI_REGION_CODE VARCHAR(20)         --渠道覆盖业务区
        ,COVER_radius       INTEGER             --服务半径
        ,POEPLE_NUM         BIGINT              --人口覆盖
        ,POPUL_DENSITY      INTEGER             --人口密度
        ,POEPLE_NUM         BIGINT              --客户覆盖
        ,EMPLOYEE_NUM       INTEGER             --雇员数目
        ,SALES_NUM          INTEGER             --营业员人数
        ,EQUIP_NUM          INTEGER             --终端数目
        ,EQUIP_STATUS       INTEGER             --终端状况
        ,CHANNEL_ADDRESS    VARCHAR(120)        --详细地址
        ,plane_graph        INTEGER             --营业厅平面结构图
        ,BOARD_HEIGHT       INTEGER             --店招高度（米）
        ,BOARD_WIDTH        INTEGER             --店招宽度（米）
        ,WALL_HEIGHT        INTEGER             --形象墙高度
        ,WALL_WIDTH         INTEGER             --形象墙宽度
        ,CREATE_DATE        TIMESTAMP           --建档日期
        ,CREATE_OP_ID       BIGINT              --建档员工
        ,DONE_DATE           TIMESTAMP              --操作日期
        ,OP_ID             BIGINT              --操作员工
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




 亚信黄伟(80589803)  18:01:55
请大家按照要求最迟在周日晚上将周报发出来，不允许延迟到周一提交
因我这边需要汇总大家的工作提交公司周报报给华南区的汪总及徐总 


赵刘平(2651779)  18:03:58
15847676066张磊的号


BU
财务PA系统

A11019	SO	ORD_CUST_F	X_YYMM	用户	客户订单	PRODUCT	日	ODS_PRODUCT_ORD_CUST_YYYYMMDD	172.16.5.43	0020	2010-4-28刘智龙增加	


 刘智龙  14:03:52
so.ord_cust_f_0891_1010
刘智龙  14:08:57
NG1-BASS1.0\Truck\03 工作库\interface\抽取\doc


1.dim_pub_city 地市维表


nfjd1234


编写文档：模板：F:\XZBI\NG1-BASS1.0\Truck\04 模板库\

监控日志
          
          
1. 了解历史接口。




E:\SVN\NG1-BASS2.0\文档\集团经分规范\NG1-BASS2.0规范(报批稿)\技术规范\外部接口分册



	中国移动省级NG1-BASS技术规范外部接口分册v2.0(报批稿).doc
	
dblink 
	
	
	表名:SYS_STAFF 解释:员工 
 
'

				
/**	2010-11-8 10:51	added by  panzhiwei		**/
--DROP TABLED WD_NG2_I03027_YYYYMMDD;
CREATE TABLE DWD_NG2_I03027_YYYYMMDD (				
        OPER_ID             BIGINT              		--员工标识
        ,ASSESS_CODE        VARCHAR(20)        		  --店员积分业务类型标识
        ,ASSESS_SCORE       INTEGER                 --积分
        ,DONE_DATE          TIMESTAMP               --时间
        ,OPASSESS_DTL_ID	  BIGINT									--积分明细编号
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
财务PA系统

A11019	SO	ORD_CUST_F	X_YYMM	用户	客户订单	PRODUCT	日	ODS_PRODUCT_ORD_CUST_YYYYMMDD	172.16.5.43	0020	2010-4-28刘智龙增加	


 刘智龙  14:03:52
so.ord_cust_f_0891_1010
刘智龙  14:08:57
NG1-BASS1.0\Truck\03 工作库\interface\抽取\doc


1.dim_pub_city 地市维表


nfjd1234


编写文档：模板：F:\XZBI\NG1-BASS1.0\Truck\04 模板库\

监控日志
          
          
1. 了解历史接口。




E:\SVN\NG1-BASS2.0\文档\集团经分规范\NG1-BASS2.0规范(报批稿)\技术规范\外部接口分册



	中国移动省级NG1-BASS技术规范外部接口分册v2.0(报批稿).doc
	
dblink 
	
	
	表名:SYS_STAFF 解释:员工 
 

SCHEME_CHANNEL_ID	NUMBER(12)	--	渠道编号
LONGITUDE	NUMBER(12)	--	经度
LATITUDE	NUMBER(12)	--	纬度
PROPERTY_SRC_TYPE	NUMBER(2)	--	物业来源方
SEAT_NUM	NUMBER(6)	--	设计坐席数目
USED_SEAT_NUM	NUMBER(6)	--	在用坐席数目
CHANNEL_SITE	NUMBER(8)	--	建设场所
OWNER_TYPE	NUMBER(2)	--	物业购置方式
PWNER_PRICE	NUMBER(10)	--	物业购置价格
SITE_TYPE	NUMBER(2)	--	用房性质
HOUSE_EXPIRE	DATE	--	租房期限
HIRE_COST	NUMBER(12)	--	租金或自购房成本分摊
FITMENT_PRICE	NUMBER(10)	--	装修投入
TRANSFER_PRICE	NUMBER(10)	--	传输投入
OTHER_PRICE	NUMBER(10)	--	其他投入
MANUAL_COST	NUMBER(12)	--	人工成本
OTHER_COST	NUMBER(12)	--	其他成本
EQUIP_DEPRE	NUMBER(12)	--	设备折旧费用
FURNITURE_COST	NUMBER(12)	--	家具折旧费用
FITMENT_COST	NUMBER(12)	--	装修折旧费用
DOOR_TYPE	NUMBER(2)	--	门头形式
STORE_AREA	NUMBER(8)	--	店面面积
BUILD_AREA	NUMBER(10)	--	建筑面积
DOOR_AREA	NUMBER(10)	--	门额面积
USE_AREA	NUMBER(10)	--	使用面积
WINDOW_AREA	NUMBER(8)	--	临街橱窗面积
BUSI_AREA	NUMBER(10)	--	业务受理区面积
NEWBUSI_USE_AREA	NUMBER(10)	--	新业务体验区面积
TERMINAL_SALE_AREA	NUMBER(10)	--	终端销售区面积
AD_AREA	NUMBER(10)	--	宣传区域面积
OTHER_AREA	NUMBER(10)	--	其他区域面积
FITMENT_EXPIRE	NUMBER(4)	--	装修投资折旧年限
FURNITURE_EXPIRE	NUMBER(4)	--	办公家具折旧年限
FITMENT_VALID	DATE	--	装修投资生效时间
FURNITURE_VALID	DATE	--	办公家具生效时间
WARM_FEE	NUMBER(10)	--	取暖费
DOOR_COST	NUMBER(10)	--	门头单面积宣传费
WINDOW_COST	NUMBER(10)	--	临街橱窗单面积宣传费
WATER_COST	NUMBER(12)	--	水费
ELEC_COST	NUMBER(12)	--	电费
OFFICE_COST	NUMBER(12)	--	办公用品及耗材
COMM_COST	NUMBER(12)	--	月通讯费
UNIT_PRICE1	NUMBER(12)	--	同地段户外市场单位价格1
REBATE1	NUMBER(12)	--	折扣系数1
UNIT_PRICE2	NUMBER(12)	--	同地段户外看板单位价格2
REBATE2	NUMBER(12)	--	折扣系数2
UNIT_PRICE3	NUMBER(12)	--	同地段户内宣传区单位价格3
REBATE3	NUMBER(12)	--	折扣系数3
DONE_CODE	NUMBER(12)	--	操作流水
DONE_DATE	DATE	--	操作日期
OP_ID	NUMBER(12)	--	操作员
ORG_ID	NUMBER(12)	--	操作组织单元
NOTES	VARCHAR2(255)	--	备注
STATE	NUMBER(1)	--	记录状态
BUY_EQUIP_SUM	NUMBER(10)	--	BUY_EQUIP_SUM
NOTA_AREA	NUMBER(8)	--	显示单位米，保持数据库是厘米
OFFICE_FIT_SUM	NUMBER(10)	--	OFFICE_FIT_SUM



					
/**	2010-11-9 11:27	added by  panzhiwei		**/	
--DROP TABLE DWD_NG2_I03028_YYYYMMDD;					
CREATE TABLE DWD_NG2_I03028_YYYYMMDD (					
        CHANNEL_ID          BIGINT              --渠道编码
        ,USED_SEAT_NUM      INTEGER             --台席数量
        ,EMPLOYEE_NUM       BIGINT              --员工数量
        ,BUILD_AREA         INTEGER             --建筑面积
        ,USE_AREA           INTEGER             --使用面积
        ,AD_AREA            INTEGER             --宣传区域面积
        ,EQUIP_NUM          BIGINT                --设备数
        ,LEGAL_PERSON       VARCHAR(20)           --联系人
        ,TEL_NUMBER         VARCHAR(20)         --联系电话
        ,BUSI_TYPE          VARCHAR(20)         --办理业务类型
        ,USAGE_TYPE         VARCHAR(20)         --功能界定
        ,HOUSE_EXPIRE       TIMESTAMP           --租房期限
        ,HOUSE_BUY_PRICE    BIGINT              --房屋购买金额
        ,HIRE_COST          BIGINT              --房屋租金
        ,BUY_EQUIP_SUM      INTEGER             --设备购买金额
        ,EQUIP_RENT_PRICE   BIGINT              --设备租金
        ,FITMENT_PRICE      INTEGER             --装修投资总额
        ,OFFICE_COST        BIGINT              --办公家具投资总额
        ,FITMENT_EXPIRE     SMALLINT            --装修投资折旧年限
        ,FURNITURE_EXPIRE   SMALLINT            --办公家具折旧年限
        ,FITMENT_VALID      TIMESTAMP           --装修投资生效时间
        ,FURNITURE_VALID    TIMESTAMP           --办公家具生效时间
        ,WATER_COST         BIGINT                  --水费
        ,WARM_FEE           INTEGER               --取暖费
        ,ELEC_COST          BIGINT                  --电费
        ,MANUAL_COST        BIGINT              --人工成本总额
        ,OTHER_COST         BIGINT              --其他日常费用总额
        ,BUSI_CONSULT_RATE  BIGINT              --咨询业务量占总业务量比值
        ,BUSI_COMPLAINT_RATE BIGINT              --投诉业务量占总业务量比值
        ,DOOR_HEIGHT        BIGINT              --门头高度
        ,DOOR_WIDTH         BIGINT              --门头长度
        ,WINDOW_AREA        INTEGER             --临街橱窗面积
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




3.2.2.	建议实现方式
  以前我们这几张报表是 从dw_newbusi_gprs_201010 这个里面取的数据，
 （dw_newbusi_gprs_201010表的数据是从BOSS的xzjf.dr_ggprs_l_20101104 抽取的 ）导致cell_id 和 lac_id为空。

现在我们这几张报表（需求目标里的），需要取到正确的 cell_id 和 lac_id
所以要新增一个接口来专门实现。从BOSS的xzjf.DR_GGPRS_GXZ_20101104 这个表里取。
（注意，这个只是针对需求目标里的几张报表，其他报表不做修改）






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

赵刘平(2651779)  15:37:25
2010-11-12:    赵刘平    15002020810
2010-11-26:    刘智龙    13631429212
2010-12-13:    李剑杰    13560241290




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
D:\XZBI\NG1-BASS1.0\Truck\03 工作库\interface

原来实现方式:

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
   



3.2.2.	建议实现方式
请参考以下提供口径，完成程序的改造。
-- Dwd_cust_manager_info_ds.tcl 更改口径
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
   
   
   --Dwd_enterprise_manager_rela_ds.tcl 更改口径
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
（ select manager_id,count(0) cnt 
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





  --Dwd_enterprise_manager_rela_ds.tcl 更改口径
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
			

1	集团编号	GROUP_ID	NUMBER(12)	
2	部门编号	CLUSTER_ID	NUMBER(10)	
3	客户经理编号	MANAGER_ID	NUMBER(8)	
4	操作类型	OPER_TYPE	NUMBER(2)	0   查询1   查询、更改
5	关系类型	REL_TYPE	NUMBER(2)	1:主要服务，2:辅助服务，3:首席客户经理
6	记录状态	REC_STATUS	NUMBER(2)	0历史1有效
7	操作日期	DONE_DATE	DATE	
8	生效日期	VALID_DATE	DATE	
9	失效日期	EXPIRE_DATE	DATE	
10	操作员	OP_ID	NUMBER(8)	
11	组织单元	ORG_ID	NUMBER(8)	
12	操作流水	DONE_CODE	NUMBER(15)	


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

前置条件。

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
			BUSI_CODE				VARCHAR(8)	--业务类型代码			
			,BUSI_NAME			VARCHAR(60)	--业务类型名称			
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
        REGION_ID           VARCHAR(7)          --地域标识
        ,manufacture_name   VARCHAR(100)        --终端厂商
        ,term_model         VARCHAR(20)         --终端型号
        ,vendor_name        VARCHAR(100)        --终端供货商
        ,settle_type        SMALLINT            --结算类型
        ,settle_info_id     BIGINT              --结算通知单编号
        ,pay_acct_id        BIGINT              --支付凭证号
        ,pay_state          SMALLINT            --支付状态
        ,done_code          BIGINT              --租机订单编号
        ,contract_id        BIGINT              --租机合同编号
        ,valid_date         date           --生效日期
        ,expire_date        date           --截止日期
        ,info_date          date           --通知日期
        ,settle_date        date           --结算周期
        ,settle_amt         DECIMAL(12,0)       --结算总金额
        ,is_settle_amt      DECIMAL(12,0)       --已结算金额
        ,not_settle_amt     DECIMAL(12,0)       --未结算金额
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
 
 
 
 --前台营业员
--drop table dim_boss_staff;
create table dim_boss_staff (
	op_id		integer not null,	--操作员编号
	op_name		varchar(32),    	--操作员名称
	op_status	smallint,       	--操作员的状态
	create_date	timestamp,      	--创建日期
	valid_date	timestamp,      	--生效日期
	expire_date	timestamp,      	--失效日期
	channel_id	integer,        	--归属组织
	city_id		varchar(7),     	--归属地区
	county_id	varchar(7)      	--归属县
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


中测接口配置(tcl生成):
1.写抽取脚本
2.写tcl
3.配置入库
4.配置ftp
5.执行tcl
6.自动加载

7.常见错误：
7.1字段名不一致
7.2日期格式不对
7.3结果集字段不对
7.4运行目录不对
7.5日期格式不对

1125收工：必测-接口列表2-选择粘贴


A02027	XG	AGENT_OPER	YYYYMM		记录客服系统受理的BOSS业务操作日志表		日	ODS_AGENT_OPER_YYYYMMDD	172.16.5.46		



java ETLMain 20101120 taskList_tmp_pzw.properties


A02027

ETL_LOAD_TABLE_MAP2

TASK_ID	TABLE_NAME_TEMPLET	TASK_NAME	LOAD_METHOD	BOSS_TABLE_NAME		

I06020	DWD_NG2_I06020_YYYYMMDD	彩铃结算清单	0	BASS2.CDR_ISMG_YYYYMMDD		insert into etl_load_table_map values('I06020','DWD_NG2_I06020_YYYYMMDD','彩铃结算清单',0,'BASS2.CDR_ISMG_YYYYMMDD');


########################################input
i_interface_dat_rule=$1
i_interface_id=$2
i_interface_boss_skm=$3
#去掉地市日期
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



svn 兼容性问题：
a. 1.4.5 (r25188) vs 1.4.6 (r28521)

apache2.2.11 与 1.4.6 (r28521) 兼容，与 1.4.5 (r25188) 不兼容

由于apache2.2不支持1.4.5
要换成apache2.0.x

mod_authz_svn 不支持 AuthzSVNAccessFile, 不能使用。



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



dm 多月
ds 单日
dt 单月多日
ms 单月
mm 多月




/**	2010-12-3 10:40	added by  panzhiwei		**/
--DROP TABLE ODS_CHANNEL_LOCAL_BUSI_YYYYMMDD;				
CREATE TABLE ODS_CHANNEL_LOCAL_BUSI_YYYYMMDD (				
        ENTITY_TYPE         INTEGER             --ENTITY_TYPE
        ,OBJ_ID             INTEGER               --OBJ_ID
        ,product_no         VARCHAR(20)         --product_no
        ,city_id            VARCHAR(8)              --地市
        ,county_id          BIGINT                  --县区
        ,channel_type_class INTEGER             --channel_type_class
        ,channel_child_type INTEGER             --渠道子类型
        ,channel_name       VARCHAR(40)         --渠道名称
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
        dept_id              VARCHAR(8)          --地市标识
        ,dept_name        VARCHAR(20)          --区县标识
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
        ,REGION             VARCHAR(8)          --归属地市
        ,SCHMARKER          VARCHAR(32)         --学校标识
        ,SCHLONG            VARCHAR(32)             --经度
        ,SCHLAT             VARCHAR(32)             --纬度
        ,COVERRATE          decimal(10,2)             --基站学校覆盖率
        ,EFF_DATE           TIMESTAMP           --生效日期
        ,EXP_DATE           TIMESTAMP           --失效日期
        ,SCHNAME			VARCHAR(200)		--学校名称
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
        ,REGION             VARCHAR(8)          --归属地市
        ,SCHMARKER          VARCHAR(32)         --学校标识
        ,SCHLONG            VARCHAR(32)             --经度
        ,SCHLAT             VARCHAR(32)             --纬度
        ,COVERRATE          decimal(10,2)             --基站学校覆盖率
        ,EFF_DATE           TIMESTAMP           --生效日期
        ,EXP_DATE           TIMESTAMP           --失效日期
        ,SCHNAME			VARCHAR(200)		--学校名称        
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
        ,REGION             VARCHAR(8)          --归属地市
        ,SCHMARKER          VARCHAR(32)         --学校标识
        ,SCHLONG            VARCHAR(32)             --经度
        ,SCHLAT             VARCHAR(32)             --纬度
        ,COVERRATE          decimal(10,2)             --基站学校覆盖率
        ,EFF_DATE           TIMESTAMP           --生效日期
        ,EXP_DATE           TIMESTAMP           --失效日期
        ,SCHNAME			VARCHAR(200)		--学校名称        
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



加载错误排除：

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
二经程序：


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
        SCHMARKER           VARCHAR(32)         --学校标识
        ,GROUPCODE          VARCHAR(32)         --集团编码
        ,SCHNAME            VARCHAR(200)        --学校名称
        ,PRODUCT_NO         VARCHAR(15)         --手机号码
        ,CUST_NAME          VARCHAR(200)        --学生姓名
        ,SEX_ID             SMALLINT            --学生性别
        ,OPERATOR           SMALLINT            --运营商归属
        ,BRAND_ID           BIGINT                  --品牌
        ,WORK_DEPT          VARCHAR(120)            --院系
        ,GRADE              SMALLINT                --班级
        ,HOME_ADDRESS       VARCHAR(200)            --住址
        ,IDEN_NBR           VARCHAR(40)         --身份证号
        ,EDUCATION_ID       SMALLINT                --学历
        ,VALID_DATE         integer           --入学年份
        ,POSITION           SMALLINT                --职务
        ,EMAIL              VARCHAR(64)         --电子邮件地址
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
        SCHMARKER           VARCHAR(32)         --学校标识
        ,GROUPCODE	VARCHAR(32)
        ,SCHNAME            VARCHAR(200)        --学校名称
        ,CUST_NAME          VARCHAR(200)        --教职人员姓名
        ,SEX_ID             SMALLINT                --性别
        ,WORK_DEPT          VARCHAR(120)            --部门
        ,POSITION           SMALLINT                --职位
        ,PRODUCT_NO         VARCHAR(15)         --手机号码
        ,OPERATOR           SMALLINT            --运营商归属
        ,BRAND_ID           BIGINT             --品牌
        ,LINK_PHONE         VARCHAR(32)         --其它联系电话
        ,EMAIL              VARCHAR(64)         --邮箱地址
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
        SCHMARKER           VARCHAR(32)         --学校标识
        ,SCHNAME            VARCHAR(200)        --学校名称
        ,REGION             VARCHAR(8)          --归属地市
        ,STUDENT_NUM        INTEGER             --学生人数
        ,SCHADDRESS         VARCHAR(200)        --学校地址
        ,PARENT_SCHOOL_IND  INTEGER             --上级学校标识
        ,MARKETING_AREA     INTEGER             --归属营销区域
        ,GROUPCODE          VARCHAR(32)         --集团编码
        ,ADMIN_DEPT         VARCHAR(120)        --主管部门
        ,EMPLOYEE_NUM       INTEGER             --教职工人数
        ,RUN_TYPE           VARCHAR(8)          --办校属性
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( SCHMARKER )
 USING HASHING;


校园市场：
渠道->学校-成员



db2 "load client from /bassapp/bass2/load/boss/M9990520101100000000.AVL of del \
 modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder warningcount 1000 \
 messages /bassapp/bass2/load/boss/messages/db2load.msg  replace into DWD_NG2_M99905_201011
 "
 

中测-数据加载-迁移：



$ ps -ef |grep sleep
   bass2 21364 26243   0 16:05:09 pts/27      0:00 grep sleep
   bass2 20174 27383   0 16:04:33 ?           0:00 sleep 60
   bass2 21273 13117   0 16:05:06 ?           0:00 sleep 60
   bass2 25557  1324   0 15:38:20 ?           0:00 sh sleep.sh
$ kill 1324
kill: 1324: 权限不够
$ kill 25557
$ kill -9 25557
$ ps -ef |grep sleep
   bass2 21912 27383   0 16:05:33 ?           0:00 sleep 60
   bass2 23042 26243   0 16:06:10 pts/27      0:00 grep sleep
   bass2 22952 13117   0 16:06:08 ?           0:00 sleep 60
   


sendstopmsg(){
#处理运行参数
if [ $# -eq 1 ] ; then
	if [  "$1" = "stop"   ] ; then
		if [ -f $RUNNING_FILE ] ; then
			touch $STOP_FILE
			echo "已生成停止标志文件,等待退出!"
			exit
		fi
		echo "程序没在运行！"
		exit
	fi
fi
}

runstatus(){
if [ -f $WORK_PATH/$RUNNING_FILE ] ; then
	echo "程序已运行！"
	exit
fi
}

	

ifbreak(){	
	#若停止标志文件存在，则程序退出
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


表遇到跨年时，遇到
引用月数据，遇到跨年的月份时，存月数据的表要写分支，保证所引用的表存在、可以引用。

日增量：当主题数据【可划分成】按日生成时

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


D:\XZBI\NG1-BASS2.0\接口\tcl>
svn ls -r {2010-12-30}

更新到指定版本并输出：
svn update -r2229 >d:\svnupdate.txt


 每更新一个条目就输出一行信息，首字符表示发生的动作，其含义如下:

   A  已增加
   D  已删除
   U  已更新
   C  有冲突
   G  已合并
   
列出到某个版本的所有条目：
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
--提交chk1.0
--开发I05046YYYYMMDDXXXXXX.AVL		
--重启调度mtest 少量接口可以前端调起
--核查其他模型
部署所有接口到正式库:修改tcl参数
--准备知识库文档
应用测评模型入数
--开发建表脚本，仿模板表

模型主键唯一性
接口相关维表
已有接口重抽


    
    


crtab(){
#建表函数：
#1.只需要提供模板表和日期，不需要指定表空间，默认为模板表得表空间
#2.运行之前要先建立数据库连接
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



    
 每更新一个条目就输出一行信息，首字符表示发生的动作，其含义如下:

   A  已增加
   D  已删除
   U  已更新
   C  有冲突
   G  已合并
   
D    技术文档模板
A    01 管理文档
A    02 技术文档模板
A    02 技术文档模板\需求类模板
A    02 技术文档模板\需求类模板\系统维护开发文档-需求名称-需求分析说明书.doc
A    02 技术文档模板\测试类模板
A    02 技术文档模板\测试类模板\系统维护开发文档-xx需求-综合测试报告（模板）.doc
A    02 技术文档模板\开发类模板
A    02 技术文档模板\开发类模板\系统维护开发文档-需求名称-数据库设计说明书.doc
A    02 技术文档模板\开发类模板\系统维护开发文档-更新BLACKBERRY业务统计口径-单元测试报告(参考文档).doc
A    02 技术文档模板\开发类模板\系统维护开发文档-需求名称-详细设计说明书.doc
A    02 技术文档模板\开发类模板\系统维护开发文档-小区价值报表开发需求-详细设计说明书(参考文档) .doc
A    02 技术文档模板\开发类模板\系统维护开发文档-数据及信息业务分析报表-数据库设计说明书(参考文档).doc
A    02 技术文档模板\开发类模板\系统维护开发文档-需求名称-单元测试报告.doc
A    03 维表
A    03 维表\西藏经分维表建表脚本.sql
更新到版本 2229。

    
    
    select 'DW_ENTERPRISE_UNIPAY_DM_201005', nodenumber(ENTERPRISE_ID) ,count(*) as using_num from bass2.DW_ENTERPRISE_UNIPAY_DM_201007 group by nodenumber(ENTERPRISE_ID) order by using_num
    
    
db2look -d bassdb56 -e -i bass2 -w bass2 -z bass2 -t DW_PRODUCT_BUSI_SPROM_DM_201006


    
    
dw_product_busi_sprom_dm_201101
    
172.16.5.48

zhaolp2(赵刘平) 10:23:36
/db2home/db2inst1/script/check_bassdb
zhaolp2(赵刘平) 10:23:55
用bass2 bass2登录吧

项目组培训资料\项目组资料\工作技巧及交接内容


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



XZBI\项目组培训资料\项目组资料\学习资料

XZBI/项目组培训资料/项目组资料/学习资料


http://172.16.5.71:4321/svn/%e9%a1%b9%e7%9b%ae%e7%bb%84%e5%9f%b9%e8%ae%ad%e8%b5%84%e6%96%99/%e9%a1%b9%e7%9b%ae%e7%bb%84%e8%b5%84%e6%96%99/%e5%ad%a6%e4%b9%a0%e8%b5%84%e6%96%99/


 my_pass=bass2
 db2 "connect to bassdb user bass2 using ${my_pass}"
 
 
 select * from etl_task_running 
where	task_id	in	('I03013')	

select * from etl_task_log  
where	task_id	in	('I03013')	

 
	set ip_num ('17950','17951')
	
移动话段
substr(product_no,1,3) in ('135','136','138','139','147','150','152','157','158','182','187','188')
and length(rtrim(ltrim(product_no)))=11

	

db2 => select * from DIM_FEETYPE_ITEM

ITEM_ID     ITEM_NAME                                                                                            FEETYPE_ID  FEETYPE_NAME                                                                                        
----------- ---------------------------------------------------------------------------------------------------- ----------- ----------------------------------------------------------------------------------------------------
SQL0668N  Operation not allowed for reason code "3" on table 
"BASS2.DIM_FEETYPE_ITEM".  SQLSTATE=57016
db2 => ? 57016

SQLSTATE 57016: The table cannot be accessed, because it is inactive.

	
	
	

       opp_noaccess_number  varchar(30),       --去IP头的对端号码
       opp_roam_areacode    varchar(7),        --对端漫游位置区号
       opp_home_areacode    varchar(7),        --对端归属长途区号
       
svn://172.16.5.71/XZBI

XZBI\NG1-BASS1.0\Truck\05 工具库

E:\SVN\NG1-BASS1.0\Truck\03 工作库\interface\抽取\doc
BOSS接口规划v1.00.xls
E:\SVN\NG1-BASS1.0\Truck\03 工作库\interface\加载\doc
ODS建表脚本.sql

张振清为亚信设计了“软件产品+专业服务”的发展模式。

　　亚信移动事业部北方区副总经理孙明洁是亚信13年老员工，比张振清还早三年进入亚信。
她是张振清说服过来开创PSO(Professional Service Organization，专业服务机构)的第一人。孙明洁之前在研发部，当时由于没有专门的服务部门给客户服务，所有的问题全部集中到研发部。这样导致研发部人员都在现场，无人做研发，对后续软件的研发非常不利。通过对PSO部门的创办，亚信同时保障了客户服务质量和产品研发的集中度。


NG1-BASS1.0/Truck/05 工具库


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


--注册节点
catalog tcpip node CQCRM remote 10.191.113.132 server 50000
--注册数据库
catalog database CQCCDW at node CQCRM
--删除注册节点
uncatalog node CQCRM
--删除注册数据库
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



Ethernet adapter 本地连接:



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

        Lease Obtained. . . . . . . . . . : 2010年10月13日 8:55:24

        Lease Expires . . . . . . . . . . : 2010年10月13日 16:55:24



Ethernet adapter 无线网络连接:



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





XZBI\NG1-BASS1.0\Truck\05 工具库

172.16.9.25 bass2 / bass2


% puts $env(AITOOLSPATH) 
/bassapp/tcl/aiomnivision/aitools


if [ $? -eq 0 ]  then
        #增加对app.sch_control_runlog标志的更新add by xufr at 2009-1-6 21:25:03
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



　create table zjt_tables as 

　　(select * from tables) definition only 


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
		,QUEUE_ID					varchar(20)				--	队列ID
		,BUSINESS_ID				bigint					--	操作ID
		,CONFIG_ID					bigint					--	配置ID
		,BUSI_ORDER_ID				bigint					--	操作订单ID
		,BILL_ID					varchar(30)				--	手机号码
		,FILE_CONTENT				VARCHAR(1024)			--	文件内容
		,SUCC_FLAG					smallint				--	成功标志
		,ERROR_MSG					VARCHAR(1000)			--	错误信息
		,ERROR_STACK				VARCHAR(4000)			--	错误栈信息
		,DONE_CODE					bigint					--	操作编码
		,DONE_DATE					TIMESTAMP				--	结束日志
		,CREATE_DATE				TIMESTAMP				--	创建时间
		,VALID_DATE					TIMESTAMP				--	生效时间
		,EXPIRE_DATE				TIMESTAMP				--	失效时间
		,OP_ID						bigint					--	操作员ID
		,ORG_ID						bigint					--	操作员组织ID
		,REGION_ID					VARCHAR(6)				--	地区编码
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
文件名$文件大小$记录数$数据日期$创建时间


1.db2 漏加载记录



replace(replace(CONT_ADDR,chr(10)),chr(13))

replace(replace(replace(cust_name,chr(10)),chr(13)),'$','')




/**	2010/11/2 18:02	added by  panzhiwei		**/	
CREATE TABLE DWD_NG2_I03013_YYYYMMDD (					
        CHANNEL_ID          BIGINT              --渠道标识:
        ,CHANNEL_NAME        VARCHAR(100)        --渠道名称:
        ,PARENT_CHANNEL_ID   INTEGER             --上级渠道标识:
        ,CHANNEL_LEVEL       INTEGER             --渠道级别:与“渠道级别”接口单元对应
        ,CHANNEL_DESC          VARCHAR(200)        --渠道描述:
        ,START_DATE          TIMESTAMP           --启用日期:
        ,EXPIRE_DATE         TIMESTAMP           --截止日期:缺省29991231
        ,REGION_CODE         VARCHAR(8)          --归属区域编号:与“地域08039”接口单元对应
        ,ORGANIZE_ID         BIGINT              --所属单位标识:与“单位”接口单元对应
        ,CHANNEL_TYPE        INTEGER             --渠道类别:与“渠道类型”接口单元对应
        ,PROPERTY_SRC_TYPE   INTEGER             --渠道物业形态:包括上市公司够建、存续企业购建、租赁、转租-店中店
        ,REG_FUND            BIGINT              --投资规模:
        ,OPEN_DATE           TIMESTAMP           --开业时间:
        ,RESPNSR_ID             INTEGER             --渠道负责人编号:与“渠道负责人”接口单元对应
        ,TEL_NUMBER          VARCHAR(20)         --渠道办公电话:
        ,FAX_NUMBER          VARCHAR(20)         --传真号码:
        ,POST_CODE           VARCHAR(20)            --邮编:
        ,BUSI_BEGIN_TIME     VARCHAR(20)         --营业时间:
        ,CHANNEL_STYLE       INTEGER             --渠道运营方式:"0-自建自营（自办）；1-自建他营（合作）；2-他建他营（特许）"
        ,CHANNEL_LEVEL       INTEGER             --渠道星级:"0-一星级；1-二星级"
        ,CHANNEL_USER_ID     INTEGER             --渠道租赁人编号:与“渠道租赁人”接口单元对应
        ,OWNER_TYPE          SMALLINT            --物业购置方式:
        ,PWNER_PRICE         INTEGER             --物业购置价格:
        ,FITMENT_PRICE       INTEGER             --装修投入:
        ,TRANSFER_PRICE      INTEGER             --传输投入:
        ,INVESTOR            VARCHAR(20)         --投资主体:
        ,LICENSE_NUMBER      VARCHAR(20)          --工商号:
        ,INTERNET_MODE       INTEGER             --接入方式:"0-光缆；1-2M电缆；2-GPRS；3-CDS；4-拨号上网；5-无线网桥；6-无"
        ,CREATE_DATE         TIMESTAMP           --建档时间:
        ,CR_OP_ID            BIGINT              --建档员工:
        ,OP_ID               BIGINT              --操作员工:
        ,DONE_DATE           TIMESTAMP           --操作时间:
        ,CHANNEL_STATE       SMALLINT            --渠道状态:"0-正常；1-预解约；2-注销"
        ,NOTES               VARCHAR(200)            --备注
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING


 交谈中请勿轻信汇款、中奖信息，勿轻易拨打陌生电话。

 刘智龙  18:09:11
									#生成校验文件
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


="FROM "&接口列表2!B213&"."&接口列表2!C213&SUBSTITUTE(SUBSTITUTE(UPPER(#REF!D3),"X","0$FEE_AREA$",1),"YYMM","$YYMM$",1)


漏加 \ 的地方会报 "java.sql.SQLException: ORA-00936: missing expression" 错误



/**	2010-11-5 15:21	added by  panzhiwei		**/		
CREATE TABLE DWD_NG2_M03023_YYYYMM (						
        CHANNEL_ID          BIGINT              --渠道标识
        ,STORE_AREA         INTEGER             --营业厅面积
        ,GEOGRAPHY_PROPERTY INTEGER             --地理位置属性
        ,LONGITUDE          BIGINT                  --经度
        ,LATITUDE           BIGINT                  --纬度
        ,COVER_BUSI_REGION_CODE VARCHAR(20)         --渠道覆盖业务区
        ,COVER_radius       INTEGER             --服务半径
        ,POEPLE_NUM         BIGINT              --人口覆盖
        ,POPUL_DENSITY      INTEGER             --人口密度
        ,POEPLE_NUM         BIGINT              --客户覆盖
        ,EMPLOYEE_NUM       INTEGER             --雇员数目
        ,SALES_NUM          INTEGER             --营业员人数
        ,EQUIP_NUM          INTEGER             --终端数目
        ,EQUIP_STATUS       INTEGER             --终端状况
        ,CHANNEL_ADDRESS    VARCHAR(120)        --详细地址
        ,plane_graph        INTEGER             --营业厅平面结构图
        ,BOARD_HEIGHT       INTEGER             --店招高度（米）
        ,BOARD_WIDTH        INTEGER             --店招宽度（米）
        ,WALL_HEIGHT        INTEGER             --形象墙高度
        ,WALL_WIDTH         INTEGER             --形象墙宽度
        ,CREATE_DATE        TIMESTAMP           --建档日期
        ,CREATE_OP_ID       BIGINT              --建档员工
        ,DONE_DATE           TIMESTAMP              --操作日期
        ,OP_ID             BIGINT              --操作员工
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




 亚信黄伟(80589803)  18:01:55
请大家按照要求最迟在周日晚上将周报发出来，不允许延迟到周一提交
因我这边需要汇总大家的工作提交公司周报报给华南区的汪总及徐总 


赵刘平(2651779)  18:03:58
15847676066张磊的号


BU
财务PA系统

A11019	SO	ORD_CUST_F	X_YYMM	用户	客户订单	PRODUCT	日	ODS_PRODUCT_ORD_CUST_YYYYMMDD	172.16.5.43	0020	2010-4-28刘智龙增加	


 刘智龙  14:03:52
so.ord_cust_f_0891_1010
刘智龙  14:08:57
NG1-BASS1.0\Truck\03 工作库\interface\抽取\doc


1.dim_pub_city 地市维表


nfjd1234


编写文档：模板：F:\XZBI\NG1-BASS1.0\Truck\04 模板库\

监控日志
          
          
1. 了解历史接口。




E:\SVN\NG1-BASS2.0\文档\集团经分规范\NG1-BASS2.0规范(报批稿)\技术规范\外部接口分册



	中国移动省级NG1-BASS技术规范外部接口分册v2.0(报批稿).doc
	
dblink 
	
	
	表名:SYS_STAFF 解释:员工 
 
'

				
/**	2010-11-8 10:51	added by  panzhiwei		**/
--DROP TABLED WD_NG2_I03027_YYYYMMDD
CREATE TABLE DWD_NG2_I03027_YYYYMMDD (				
        OPER_ID             BIGINT              		--员工标识
        ,ASSESS_CODE        VARCHAR(20)        		  --店员积分业务类型标识
        ,ASSESS_SCORE       INTEGER                 --积分
        ,DONE_DATE          TIMESTAMP               --时间
        ,OPASSESS_DTL_ID	  BIGINT									--积分明细编号
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
财务PA系统

A11019	SO	ORD_CUST_F	X_YYMM	用户	客户订单	PRODUCT	日	ODS_PRODUCT_ORD_CUST_YYYYMMDD	172.16.5.43	0020	2010-4-28刘智龙增加	


 刘智龙  14:03:52
so.ord_cust_f_0891_1010
刘智龙  14:08:57
NG1-BASS1.0\Truck\03 工作库\interface\抽取\doc


1.dim_pub_city 地市维表


nfjd1234


编写文档：模板：F:\XZBI\NG1-BASS1.0\Truck\04 模板库\

监控日志
          
          
1. 了解历史接口。




E:\SVN\NG1-BASS2.0\文档\集团经分规范\NG1-BASS2.0规范(报批稿)\技术规范\外部接口分册



	中国移动省级NG1-BASS技术规范外部接口分册v2.0(报批稿).doc
	
dblink 
	
	
	表名:SYS_STAFF 解释:员工 
 

SCHEME_CHANNEL_ID	NUMBER(12)	--	渠道编号
LONGITUDE	NUMBER(12)	--	经度
LATITUDE	NUMBER(12)	--	纬度
PROPERTY_SRC_TYPE	NUMBER(2)	--	物业来源方
SEAT_NUM	NUMBER(6)	--	设计坐席数目
USED_SEAT_NUM	NUMBER(6)	--	在用坐席数目
CHANNEL_SITE	NUMBER(8)	--	建设场所
OWNER_TYPE	NUMBER(2)	--	物业购置方式
PWNER_PRICE	NUMBER(10)	--	物业购置价格
SITE_TYPE	NUMBER(2)	--	用房性质
HOUSE_EXPIRE	DATE	--	租房期限
HIRE_COST	NUMBER(12)	--	租金或自购房成本分摊
FITMENT_PRICE	NUMBER(10)	--	装修投入
TRANSFER_PRICE	NUMBER(10)	--	传输投入
OTHER_PRICE	NUMBER(10)	--	其他投入
MANUAL_COST	NUMBER(12)	--	人工成本
OTHER_COST	NUMBER(12)	--	其他成本
EQUIP_DEPRE	NUMBER(12)	--	设备折旧费用
FURNITURE_COST	NUMBER(12)	--	家具折旧费用
FITMENT_COST	NUMBER(12)	--	装修折旧费用
DOOR_TYPE	NUMBER(2)	--	门头形式
STORE_AREA	NUMBER(8)	--	店面面积
BUILD_AREA	NUMBER(10)	--	建筑面积
DOOR_AREA	NUMBER(10)	--	门额面积
USE_AREA	NUMBER(10)	--	使用面积
WINDOW_AREA	NUMBER(8)	--	临街橱窗面积
BUSI_AREA	NUMBER(10)	--	业务受理区面积
NEWBUSI_USE_AREA	NUMBER(10)	--	新业务体验区面积
TERMINAL_SALE_AREA	NUMBER(10)	--	终端销售区面积
AD_AREA	NUMBER(10)	--	宣传区域面积
OTHER_AREA	NUMBER(10)	--	其他区域面积
FITMENT_EXPIRE	NUMBER(4)	--	装修投资折旧年限
FURNITURE_EXPIRE	NUMBER(4)	--	办公家具折旧年限
FITMENT_VALID	DATE	--	装修投资生效时间
FURNITURE_VALID	DATE	--	办公家具生效时间
WARM_FEE	NUMBER(10)	--	取暖费
DOOR_COST	NUMBER(10)	--	门头单面积宣传费
WINDOW_COST	NUMBER(10)	--	临街橱窗单面积宣传费
WATER_COST	NUMBER(12)	--	水费
ELEC_COST	NUMBER(12)	--	电费
OFFICE_COST	NUMBER(12)	--	办公用品及耗材
COMM_COST	NUMBER(12)	--	月通讯费
UNIT_PRICE1	NUMBER(12)	--	同地段户外市场单位价格1
REBATE1	NUMBER(12)	--	折扣系数1
UNIT_PRICE2	NUMBER(12)	--	同地段户外看板单位价格2
REBATE2	NUMBER(12)	--	折扣系数2
UNIT_PRICE3	NUMBER(12)	--	同地段户内宣传区单位价格3
REBATE3	NUMBER(12)	--	折扣系数3
DONE_CODE	NUMBER(12)	--	操作流水
DONE_DATE	DATE	--	操作日期
OP_ID	NUMBER(12)	--	操作员
ORG_ID	NUMBER(12)	--	操作组织单元
NOTES	VARCHAR2(255)	--	备注
STATE	NUMBER(1)	--	记录状态
BUY_EQUIP_SUM	NUMBER(10)	--	BUY_EQUIP_SUM
NOTA_AREA	NUMBER(8)	--	显示单位米，保持数据库是厘米
OFFICE_FIT_SUM	NUMBER(10)	--	OFFICE_FIT_SUM



					
/**	2010-11-9 11:27	added by  panzhiwei		**/	
--DROP TABLE DWD_NG2_I03028_YYYYMMDD					
CREATE TABLE DWD_NG2_I03028_YYYYMMDD (					
        CHANNEL_ID          BIGINT              --渠道编码
        ,USED_SEAT_NUM      INTEGER             --台席数量
        ,EMPLOYEE_NUM       BIGINT              --员工数量
        ,BUILD_AREA         INTEGER             --建筑面积
        ,USE_AREA           INTEGER             --使用面积
        ,AD_AREA            INTEGER             --宣传区域面积
        ,EQUIP_NUM          BIGINT                --设备数
        ,LEGAL_PERSON       VARCHAR(20)           --联系人
        ,TEL_NUMBER         VARCHAR(20)         --联系电话
        ,BUSI_TYPE          VARCHAR(20)         --办理业务类型
        ,USAGE_TYPE         VARCHAR(20)         --功能界定
        ,HOUSE_EXPIRE       TIMESTAMP           --租房期限
        ,HOUSE_BUY_PRICE    BIGINT              --房屋购买金额
        ,HIRE_COST          BIGINT              --房屋租金
        ,BUY_EQUIP_SUM      INTEGER             --设备购买金额
        ,EQUIP_RENT_PRICE   BIGINT              --设备租金
        ,FITMENT_PRICE      INTEGER             --装修投资总额
        ,OFFICE_COST        BIGINT              --办公家具投资总额
        ,FITMENT_EXPIRE     SMALLINT            --装修投资折旧年限
        ,FURNITURE_EXPIRE   SMALLINT            --办公家具折旧年限
        ,FITMENT_VALID      TIMESTAMP           --装修投资生效时间
        ,FURNITURE_VALID    TIMESTAMP           --办公家具生效时间
        ,WATER_COST         BIGINT                  --水费
        ,WARM_FEE           INTEGER               --取暖费
        ,ELEC_COST          BIGINT                  --电费
        ,MANUAL_COST        BIGINT              --人工成本总额
        ,OTHER_COST         BIGINT              --其他日常费用总额
        ,BUSI_CONSULT_RATE  BIGINT              --咨询业务量占总业务量比值
        ,BUSI_COMPLAINT_RATE BIGINT              --投诉业务量占总业务量比值
        ,DOOR_HEIGHT        BIGINT              --门头高度
        ,DOOR_WIDTH         BIGINT              --门头长度
        ,WINDOW_AREA        INTEGER             --临街橱窗面积
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




3.2.2.	建议实现方式
  以前我们这几张报表是 从dw_newbusi_gprs_201010 这个里面取的数据，
 （dw_newbusi_gprs_201010表的数据是从BOSS的xzjf.dr_ggprs_l_20101104 抽取的 ）导致cell_id 和 lac_id为空。

现在我们这几张报表（需求目标里的），需要取到正确的 cell_id 和 lac_id
所以要新增一个接口来专门实现。从BOSS的xzjf.DR_GGPRS_GXZ_20101104 这个表里取。
（注意，这个只是针对需求目标里的几张报表，其他报表不做修改）






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

赵刘平(2651779)  15:37:25
2010-11-12:    赵刘平    15002020810
2010-11-26:    刘智龙    13631429212
2010-12-13:    李剑杰    13560241290




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
D:\XZBI\NG1-BASS1.0\Truck\03 工作库\interface

原来实现方式:

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
   



3.2.2.	建议实现方式
请参考以下提供口径，完成程序的改造。
-- Dwd_cust_manager_info_ds.tcl 更改口径
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
   
   
   --Dwd_enterprise_manager_rela_ds.tcl 更改口径
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
（ select manager_id,count(0) cnt 
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





  --Dwd_enterprise_manager_rela_ds.tcl 更改口径
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
			

1	集团编号	GROUP_ID	NUMBER(12)	
2	部门编号	CLUSTER_ID	NUMBER(10)	
3	客户经理编号	MANAGER_ID	NUMBER(8)	
4	操作类型	OPER_TYPE	NUMBER(2)	0   查询1   查询、更改
5	关系类型	REL_TYPE	NUMBER(2)	1:主要服务，2:辅助服务，3:首席客户经理
6	记录状态	REC_STATUS	NUMBER(2)	0历史1有效
7	操作日期	DONE_DATE	DATE	
8	生效日期	VALID_DATE	DATE	
9	失效日期	EXPIRE_DATE	DATE	
10	操作员	OP_ID	NUMBER(8)	
11	组织单元	ORG_ID	NUMBER(8)	
12	操作流水	DONE_CODE	NUMBER(15)	


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

前置条件。

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
			BUSI_CODE				VARCHAR(8)	--业务类型代码			
			,BUSI_NAME			VARCHAR(60)	--业务类型名称			
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
        REGION_ID           VARCHAR(7)          --地域标识
        ,manufacture_name   VARCHAR(100)        --终端厂商
        ,term_model         VARCHAR(20)         --终端型号
        ,vendor_name        VARCHAR(100)        --终端供货商
        ,settle_type        SMALLINT            --结算类型
        ,settle_info_id     BIGINT              --结算通知单编号
        ,pay_acct_id        BIGINT              --支付凭证号
        ,pay_state          SMALLINT            --支付状态
        ,done_code          BIGINT              --租机订单编号
        ,contract_id        BIGINT              --租机合同编号
        ,valid_date         date           --生效日期
        ,expire_date        date           --截止日期
        ,info_date          date           --通知日期
        ,settle_date        date           --结算周期
        ,settle_amt         DECIMAL(12,0)       --结算总金额
        ,is_settle_amt      DECIMAL(12,0)       --已结算金额
        ,not_settle_amt     DECIMAL(12,0)       --未结算金额
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
 
 
 表增加列

db2 alter table 表名 add column 字段名 varchar(4)(字段类型）


tcl: format "#%02x%02x%02x" $r $g $b
% format "#%02x%02x%02x" 0 128 255
#0080ff

app.sch_control_runlog:
0   运行完成 
1   正在运行
-1  运行出错 
-2  重新运行


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

#全局变量:
puts $env(DATABASE)
puts $env(AITOOLSPATH)
puts $env(DB_USER)
puts $env(AGENTLOGDIR)


3.3	aidb_open 
功能：创建数据操作句柄
输入参数：dbhandle    --数据库连接句柄
	返回：  handle  --数据库操作句柄

例子：
    set  handle  [aidb_open $dbhandle]

  #删除本期数据
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



#内部函数部分
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
  


问题：
insert into bass1.int_program_data values ('d','G_A_06001_DAY.tcl','06001.bass1','06001_e','06001_f');
insert into app.g_unit_info values ('06001',0,'校园基本信息导出','bass1.g_a_06001_day',1,0,1);

：
5.5	第5步：传送接口


本文档中STD1表示业务维度分类，STD2用来表示话单维度分类，STD3用来表示基本指标，STD4用来表示扩展指标。

注：新疆、西藏往后顺延2个小时。即忙时9:00-0：00-次日凌晨1:00，闲时：1：00-9：00

insert into bass1.int_program_data values ('d','G_A_06001_DAY.tcl','06001.bass1','06001_e','06001_f');
insert into app.g_unit_info values ('06001',0,'校园基本信息导出','bass1.g_a_06001_day',1,0,1);

        db2 connect to bassdb user bass2 using bass2
        db2 "create table BASS2.STAT_ENTERPRISE_SNAPSHOT_${op_month} like BASS2.STAT_ENTERPRISE_SNAPSHOT_YYYYMM in TBS_REPORT index 
in TBS_INDEX PARTITIONING KEY ( ENTERPRISE_CODE ) USING HASHING NOT LOGGED INITIALLY"
        db2 "load client from '/bassapp/report/tcl/vip_rati/${target_file}' of asc method L (27 35, 36 45, 46 59, 60 67, 68 81, 82 8
2, 83 83) replace into BASS2.STAT_ENTERPRISE_SNAPSHOT_${op_month}"
        db2 terminate



每日增量
指抽取每日00：00---24：00发生变化的、新产生的数据最新状态快照。
每日新增数据
指抽取每日0：00至24：00，此时间范围内新产生的数据。

?	保证接口数据文件的大小不能超过2GB，如果接口数据文件太大，必须按

2.5	接口服务器
服务器地址：172.16.5.130
登陆用户：bass
密码：3jysjbx


移动轻松卡
map


客户到达数受批量拆机影响会出现很大的波动也是正常的?
find . -name "*21007*dat" -exec ls -l {} \;

#00 19 * * * . .profile;/bassapp/bass2/tcl/batch_tclsh.sh /bassapp/bass2/tcl/batch_tclsh.cfg
30 13 * * * /bassapp/bass2/data/backup_table.sh
10 9 3 * * . .profile;/bassapp/bass2/sh/dmrn_run_dayamonth.sh
00 23 * * * . .profile;/bassapp/bass2/bak_xufr/bak.sh

1 12 6 * * /bassapp/bass2/dmpm/export_interface_data.sh > /bassapp/bass2/dmpm/nohup.out
                       

$ db2look -d bassdb56 -i bass2 -w bass2 -z bass1 -t g_s_05001_month -e

经过查询，我们数据库的字符集是1386，属于MBCS数据，因此定界符最大是0x3F，而“|”的ASCII码为0x7C，超过了有效范围。解决办法，是做Export或者Load操作时，增加codepage选项，让DB2自动进行字符集转换：

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



                       
SYS_TASK_RUNLOG？？？


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


06021 TB_SVC_CORCHNL_BASIC 实体渠道基础信息 
06022 TB_SVC_CORCHNL_RENT 实体渠道购建或租赁信息 
06023 TB_SVC_CORCHNL_RES_CONF 实体渠道资源配置信息
22061 TB_SVC_CORCHNL_BUSN_COST 实体渠道运营成本信息
22062 TB_SVC_CORCHNL_PROC_INFO 实体渠道业务办理信息
22063 TB_SVC_CORCHNL_SALRY_ALLOW 实体渠道酬金及补贴信息
22064 TB_SVC_CORCHNL_ADD_BUSN_INFO 实体渠道重点增值业务办理信息"									
									
							                       

Q3:R 2708~3247							                       

