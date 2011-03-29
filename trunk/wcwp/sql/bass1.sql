--int(replace(char(current date - 1 days),'-',''))

select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabname = 'G_S_04002_DAY_BAK'

G_S_04002_DAY	95829.84 635661807
G_S_04002_DAY	49864.68 330794180

    rename bass1.G_S_04002_DAY to bass1.G_S_04002_DAY_BAK
rename BASS1.G_S_04002_DAY to BASS1.G_S_04002_DAY_BAK
select * from
BASS1.G_S_04002_DAY

rename BASS1.G_S_04002_DAY to G_S_04002_DAY_BAK
 create table BASS1.G_S_04002_DAY like BASS1.G_S_04002_DAY_BAK 
     DISTRIBUTE BY HASH(TIME_ID,  
                 PRODUCT_NO,  
                 ROAM_LOCN,  
                 ROAM_TYPE_ID,  
                 APNNI,  
                 START_TIME)   
     IN TBS_APP_BASS1 INDEX IN TBS_INDEX 

	  insert into BASS1.G_S_04002_DAY 
	  select * from  BASS1.G_S_04002_DAY_BAK
	  where time_id >= 20101101       

	  --RUNSTATS ON table G_S_04002_DAY	with distribution and detailed indexes all  
--runstats on table bass1.G_S_04002_DAY with distribution and detailed indexes all            
select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabname = 'G_S_21003_MONTH'

select * from bass1.G_S_04002_DAY_bak
select tabname , card from syscat.tables where tabname in ('G_S_04002_DAY','G_S_04002_DAY_BAK')            
AND tabschema = 'BASS1'
SELECT COUNT(0) FROM BASS1.G_S_04002_DAY_BAK

--告警 alarm

select * from  app.sch_control_alarm 
where alarmtime >=  timestamp('20110311'||'000000') 
--and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc 

select * from syscat.functions where funcname = 'FN_GET_ALL_DIM'

select * from   BASS1.ALL_DIM_LKP 

select count(0),count(distinct user_id)  from   bass1.g_a_02004_day

bass1.fn_get_all_dim

BODY
CREATE FUNCTION BASS1.FN_GET_ALL_DIM(GID VARCHAR(20),DID VARCHAR(20)) RETURNS VARCHAR(10) DETERMINISTIC NO EXTERNAL ACTION LANGUAGE SQL BEGIN ATOMIC RETURN SELECT BASS1_VALUE FROM BASS1.ALL_DIM_LKP WHERE BASS1_TBID = GID AND XZBAS_VALUE = DID END


select * from syscat.tables
select * from app.sch_control_runlog 
where control_code = 'TR1_VGOP_D_14303'
 select * from app.sch_control_runlog
 
 select * from app.sch_control_alarm
 select min(deal_time) from APP.G_FILE_REPORT where length(deal_time)=14
 select substr(deal_time,1,6) , count(0)
 from app.g_file_report 
 group by substr(deal_time,1,6)
 
 select tabname from syscat.tables where tabschema = 'BASS1'
and tabname like 'G_%' 
 select * from app.sch_control_map
 where module = 2 
 
 
 select * from
 BASS1.G_USER_LST
 
 select * from BASS2.ETL_TASK_LOG where task_id in ( 'I11101')
and cycle_id='20110228'

select * from APP.G_RUNLOG

select * from
APP.G_FILE_REPORT_ERR

select * from
APP.G_REC_REPORT_ERR


select time_id,count(0) cnt from BASS1.G_USER_LST
group by time_id 

select * from
app.g_file_report where deal_time like '%20110301%'
and err_code = '00'


select * from  bass1.G_S_22073_DAY

select  * 
--distinct left(right(filename,16),5),err_code 
from APP.G_FILE_REPORT
where filename like '%_201103_%' 
and err_code='00'


select * from app.g_runlog 
where time_id=20110117
and return_flag=1



select count(0)
select min(deal_time)
from app.g_file_report
where deal_time > '19990412211032'


select  * 
--distinct left(right(filename,16),5),err_code 
from APP.G_FILE_REPORT
where filename like '%_201103_%' 
and err_code='00'
and length(filename)=length('s_13100_201002_03014_01_001.dat')
order by deal_time desc

select * from 
bass1.g_rule_check where rule_code = 'R161_15'
where time_id = 20110317



select 
 time_id,
 case when rule_code='R159_1' then '新增客户数'
      when rule_code='R159_2' then '客户到达数'
      when rule_code='R159_3' then '上网本客户数'
      when rule_code='R159_4' then '离网客户数'
 end,
 target1,
 target2,
 target3
from bass1.g_rule_check
where rule_code in ('R159_1','R159_2','R159_3','R159_4')
  and time_id=20110301
  

select count(0) from
bass1.G_S_21003_TO_DAY
  
select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabname = 'G_S_21003_TO_DAY'

select tbsp_id,substr(tbsp_name,1,30) tbsp_name,TBSP_UTILIZATION_PERCENT from SYSIBMADM.TBSP_UTILIZATION order by tbsp_id,dbpartitionnum

select * from syscat.tables where tabname like '%04005%'
and tabschema = 'BASS1'

G_S_04005_DAY


select * from  BASS1.G_S_04005_DAY 


select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabname = 'G_S_04005_DAY'

    rename BASS1.G_S_04005_DAY to G_S_04005_DAY_BAK

    create table BASS1.G_S_04005_DAY like BASS1.G_S_04005_DAY_BAK 
                 DISTRIBUTE BY HASH(TIME_ID,  
                 PRODUCT_NO,  
                 SP_CODE,  
                 OPPOSITE_NO)   
                   IN TBS_APP_BASS1 INDEX IN TBS_INDEX NOT LOGGED INITIALLY

	  insert into BASS1.G_S_04005_DAY 
	  select * from  BASS1.G_S_04005_DAY_BAK
	  where time_id >= 20101001

select tabname , card from syscat.tables where tabname in ('G_S_04005_DAY','G_S_04005_DAY_BAK')            
AND tabschema = 'BASS1'
select time_id,count(0) cnt 
from BASS1.G_S_04005_DAY
group by  time_id
                 
select tabname , card from syscat.tables where tabname in ('G_S_04005_DAY','G_S_04005_DAY_BAK')            
AND tabschema = 'BASS1'
select * from BASS1.G_S_04005_DAY
                 
drop table BASS1.G_S_04005_DAY_BAK


select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabname = 'G_S_21003_MONTH'

select * from  bass1.G_S_21003_MONTH 

select TIME_ID , count(0) 
--, count(distinct TIME_ID ) 
from bass1.G_S_21003_MONTH 
group by  TIME_ID 
order by 1 

SQL0911N

select * from
g_s_04003_day

select count(distinct WLAN_ATTESTATION_CODE)
from BASS1.G_S_04005_DAY
where time_id between 20101201 and 20101231 
and

select * from bass2.dim_roam_type

			
select count(0),count(distinct WLAN_ATTESTATION_CODE)
from BASS1.G_S_04003_DAY
where time_id between 20101201 and 20101231 
and ROAM_TYPE_ID in ('500','110')

select *  
from BASS1.G_S_04003_DAY
where time_id between 20101201 and 20101231 
and ROAM_TYPE_ID in ('500','110')

select * from app.sch_control_before 
where control_code like '%21007%'

BASS1_EXP_G_I_03007_MONTH	BASS1_G_I_03007_MONTH.tcl
BASS1_EXP_G_I_03007_MONTH	BASS1_INT_CHECK_F7_MONTH.tcl
BASS1_EXP_G_I_03007_MONTH	BASS1_INT_CHECK_R028_MONTH.tcl
BASS1_EXP_G_I_03007_MONTH	BASS1_INT_CHECK_R029_MONTH.tcl

select * from app.sch_control_before 
where control_code = 'BASS1_INT_CHECK_R029_MONTH.tcl'


select * from app.sch_control_task 
where control_code = 'BASS1_EXP_G_I_03007_MONTH'


select * from
APP.G_UNIT_INFO 

select * from APP.G_RUNLOG  order by 1 desc 

select * from bass1.g_bus_check_all_day

select * from bass1.g_bus_check_bill_month


select count(distinct HOTSPOT_AREA_ID)
from BASS1.G_S_04003_DAY
where time_id between 20101201 and 20101231 
and ROAM_TYPE_ID in ('500','110')


select distinct HOTSPOT_AREA_ID
from BASS1.G_S_04003_DAY
where time_id between 20101201 and 20101231 
and ROAM_TYPE_ID in ('500','110')


select * from   bass2.DW_ENTERPRISE_MEMBER_201102 where enterprise_id = '89103000041929'
select * from   bass2.DW_ENTERPRISE_MEMBER_HIS_201102 where enterprise_id = '89103000041929'
select * from   bass2.DW_ENTERPRISE_MEMBER_MID_20110323 where enterprise_id = '89103000041929'

select * from    bass2.DW_ENTERPRISE_SUB_DS
where ENTERPRISE_ID = '89103000041929'

select count(0),count(distinct enterprise_id) from   bass2.DW_ENTERPRISE_MEMBER_201012
select count(0),count(distinct enterprise_id) from   bass2.DW_ENTERPRISE_MEMBER_HIS_201012
select count(0),count(distinct enterprise_id) from   bass2.DW_ENTERPRISE_MEMBER_MID_201012

select * from bass2.CDR_WLAN_20100304
select * from  BASS1.G_S_04003_DAY

select count(distinct HOTSPOT_AREA_ID)
from BASS1.G_S_04003_DAY
where 
--time_id between 20101201 and 20101231 
--and 
ROAM_TYPE_ID in ('500','110')

select tabname,card  from syscat.tables where tabname like '%WLAN%'

SELECT * FROM app.sch_control_task where function_desc like '%WLAN%'

select * from G_I_02016_MONTH
select * from bass2.Dw_newbusi_wlan_20110302



select distinct HOTSPOT_AREA_ID
select *from BASS1.G_S_04003_DAY
where ROAM_TYPE_ID in ('500','110')

select min(time_id) 
from BASS1.G_S_04003_DAY


select * from BASS1.G_S_04003_DAY
where product_no not like '1%'

select  count(product_no),count(distinct product_no),count(distinct enterprise_id),count(distinct product_no||enterprise_id)
from  bass2.DW_ENTERPRISE_MEMBER_MID_201012

select sum(int(flowup)+int(flowdown))/1024 
from (
select a.product_no,a.HOTSPOT_AREA_ID,b.enterprise_id,c.ENTERPRISE_NAME
,a.flowup,a.flowdown,a.call_fee,a.info_fee,a.CALL_DURATION
from (select * from BASS1.G_S_04003_DAY 
        where time_id between 20101201 and 20101231 
        and ROAM_TYPE_ID in ('500','110')	
) a
left join  (select distinct enterprise_id,product_no 
            from bass2.DW_ENTERPRISE_MEMBER_MID_201012
            ) b 
            on a.product_no = b.product_no
left join   bass2.dw_enterprise_msg_201012 c 
            on b.enterprise_id = c.enterprise_id
) t 
where enterprise_id is not null


select max(time_id) from g_s_05001_month
select * from g_s_05001_month

select time_id,count(0) from g_s_05002_month
group by time_id




select tabname from syscat.tables where tabname like '%05002%'

select time_id,count(0)
from G_S_05001_MONTH
group by time_id 


select * from app.sch_control_task where control_code = 'BASS1_EXP_G_S_03005_MONTH'

select count(0) from G_A_02052_MONTH where time_id = 201101



select * from APP.SCH_CONTROL_ALARM  
where flag in(1,-1) and control_code like 'BASS1_INT_CHECK_INDEX_WAVE_DAY.tcl%'
and date(alarmtime) = '2011-03-05'

CONTENT
R161_1 波动性检查新增客户数超出15%
R161_7 波动性检查当月累计使用TD网络的手机客户数超出5%
R161_9 波动性检查当月累计使用TD网络的数据卡客户数超出5%
R161_10 波动性检查当月累计使用TD网络的上网本客户数超出5%
R161_13 波动性检查联通移动新增客户数超出8%
R161_14 波动性检查电信移动新增客户数超出8%


select * from bass1.g_rule_check where rule_code in ('R161_1') order by time_id desc

select * from g_file_report

select * from app.g_runlog 


select time_id,count(0) from bass1.g_user_lst
group by time_id 

select * from syscat.tables where tabname like '%22012%'

select * from G_S_22012_DAY

select count(0) from APP.SMS_SEND_INFO



select * from    bass1.T_GS05001M

 select * from      bass1.T_GS05001M
 
 select * from bass1.T_GS05001M
 

select * from   bass1.g_s_05001_month where time_id = 201012
except
select * from bass1.T_GS05001M


CREATE TABLE "BASS1   "."T_GS05002M"  (
                  "TIME_ID" INTEGER , 
                  "BILL_MONTH" CHAR(6) , 
                  "SELF_CMCC_CODE" CHAR(5) , 
                  "SELF_SVC_BRND_ID" CHAR(1) , 
                  "OTHER_CMCC_CODE" CHAR(6) , 
                  "OTHER_SVC_BRND_ID" CHAR(6) , 
                  "IN_COUNT" CHAR(12) , 
                  "OUT_COUNT" CHAR(12) , 
                  "STLMNT_FEE" CHAR(12) , 
                  "PAY_STLMNT_FEE" CHAR(12) )   
                 DISTRIBUTE BY HASH("TIME_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  

select * from   g_s_05002_month where time_id = 201012
except
select * from T_GS05002M

select count(0) from app.sch_control_alarm
where control_code like 'BASS1%'
select * from app.sch_control_task where upper(cmd_line) like '%BASS1%SH%'

SCH_CONTROL_TASK


select CONTROL_CODE,'bass1_lst.sh',1,'${ALARM_CONTENT}',current timestamp,-1 
from app.SCH_CONTROL_TASK where upper(replace(CMD_LINE,' ',''))=upper('bass1_lst.sh')

 select * from app.sch_control_groupinfo
 
 dmk	BI-M01-01-MARKDB	2011-03-08 16:53:42.240912
olap_load	BI-M02-01-OLAP	2011-03-08 16:53:41.055375
app	BI-M02-01-OLAP	2011-03-08 16:53:42.407528


select mobile_num,count(0)
 from app.sms_send_info
 group by mobile_num
 


select * from SYS_TASK_RUNLOG

select * from syscat.tables where tabname like '%RUNLOG%'

select * from SYS_TASK_RUNLOG


select a.custstatus_id,coalesce(bass1.fn_get_all_dim('BASS_STD1_0028',char(a.custstatus_id)),'20') as custstatus_id 
from bass2.dwd_cust_msg_20110310 a

select * from 


select * from BASS1.ALL_DIM_LKP 


select time_id,count(0)
from G_A_02004_DAY
where time_id >= 20110101
group by time_id 
order by 1 desc 



select * from G_A_02004_DAY


select count(distinct user_id) from G_A_02004_DAY with ur
-- =2088474
select count(distinct user_id) from G_A_02008_DAY with ur

2088474


select * from   product_xhx

select * from 

select * from bass1.G_REPORT_CHECK


SELECT sum(bigint(BILLING_CAT_OF_WAP_INFO))
                  FROM BASS1.G_S_04006_DAY
                 WHERE TIME_ID/100=201102
1118057
                 
SELECT sum(bigint(INFO_FEE))
                  FROM BASS1.G_S_04006_DAY
                 WHERE TIME_ID/100=201102

5621188

select * from   app.sch_control_task where control_code like '%CHECK%MONTH%'

select * from   BASS1.G_S_04006_DAY

select * from    bass2.tmp_zcg
select * from app.SYS_TASK_RUNLOG


select * from bass2.dw_product_ins_off_ins_prod_201102where product_instance_id='89157333132742'  and offer_id=111099001926
                
select * from   bass1.g_s_22012_day
                
SELECT * FROM BASS2.ETL_TASK_LOG
                
select * from   app.sch_control_task where control_code like '%00000%'                

select * from   bass1.G_BUS_CHECK_BILL_MONTH

select * from   app.sch_control_task

select * from   app.sch_control_task where function_desc like '%作废%'
and control_code like 'BASS1%'
select * from   app.sch_control_task where cmd_type = 1



select cmd_type , count(0) , count(distinct cmd_type ) 
from app.sch_control_task 
group by  cmd_type 
order by 1 

select * from   APP.SCH_PERSON_PHONE

select userid , count(0) , count(distinct userid ) 
from APP.SCH_PERSON_PHONE 
group by  userid 
order by 1 

select * from   app.sch_control_groupinfo
select * from   app.sch_control_mogrpinfo




BASS2.ETL_SEND_MESSAGE

select * from BASS2.ETL_SEND_MESSAGE where phone_id = '13989094821'



select       
c.MO_GROUP_DESC,--模块名
count( case when date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 then a.control_code end ) not_run_cnt,--未完成数
count( case when   b.flag=1  then a.control_code end ) running_cnt,--执行数
count( case when   b.flag=-1 then a.control_code end ) run_err_cnt, --执行出错
count( case when date(current timestamp)=date(b.begintime)    and  b.flag=0 then a.control_code end ) done_cnt	--完成数
from APP.SCH_CONTROL_TASK a,
      APP.SCH_CONTROL_RUNLOG  b,
      APP.SCH_CONTROL_MOGRPINFO  c
where  a.CONTROL_CODE=b.CONTROL_CODE  
   and a.MO_GROUP_CODE = c.MO_GROUP_CODE 
   and a.deal_time in (2)
group by c.MO_GROUP_DESC,c.sort_id
order by c.sort_id
with ur	


select deal_time , count(0) , count(distinct deal_time ) 
from app.sch_control_task 
group by  deal_time 
order by 1 


select deal_time , count(0) 
--,  count(distinct deal_time ) 
from app.sch_control_task 
group by  deal_time 
order by 1 

select * from   app.sch_control_task  where deal_time = 3


select a.*,b.*
from APP.SCH_CONTROL_TASK a,
      APP.SCH_CONTROL_RUNLOG  b,
      APP.SCH_CONTROL_MOGRPINFO  c
where  a.CONTROL_CODE=b.CONTROL_CODE  
   and a.MO_GROUP_CODE = c.MO_GROUP_CODE 
   and a.deal_time in (2)
   and (date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2)
   and c.MO_GROUP_DESC = '一经程序'
order by c.sort_id
with ur	



select       
a.MO_GROUP_CODE,
c.MO_GROUP_DESC,
a.CONTROL_CODE,
a.CMD_LINE,
a.FUNCTION_DESC,
case  when date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 then '未完成' 
      when   b.flag=1   then '执行中'
      when   b.flag=-1  then '执行出错'
      when   date(current timestamp)=date(b.begintime) and  b.flag=0 then '完成'
      else  '未知'
end,
b.*
from  APP.SCH_CONTROL_TASK a,
      APP.SCH_CONTROL_RUNLOG  b,
      APP.SCH_CONTROL_MOGRPINFO  c
where  a.CONTROL_CODE=b.CONTROL_CODE  
   and a.MO_GROUP_CODE = c.MO_GROUP_CODE 
   and a.deal_time in (1)
   and a.MO_GROUP_CODE = 'BASS1'
order by c.sort_id
with ur




select count(0),count(distinct ENTERPRISE_ID) 
select t2.*
from 
(
select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
from (
select *
from G_A_02055_DAY where STATUS_ID ='1' and MANAGE_MODE = '2'
and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340'
) t 
) t2 where rn = 1 

select count(0)
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from g_a_02059_day a
where TIME_ID < 20110301
and MANAGE_MODE = '2'
and STATUS_ID ='1'
and ENTERPRISE_BUSI_TYPE ='1340'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 




select * from g_a_02059_day a
where TIME_ID < 20110301
and MANAGE_MODE = '2'
and STATUS_ID ='1'
and ENTERPRISE_BUSI_TYPE ='1340'

select * from G_A_02061_DAY
where ENTERPRISE_BUSI_TYPE = '1220'
and  MANAGE_MODE = '2'
and STATUS_ID ='1'





select t2.*
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from G_A_02061_DAY
where ENTERPRISE_BUSI_TYPE = '1220'
and  MANAGE_MODE = '2'
and STATUS_ID ='1'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 





select * from g_a_02054_day


select count(0),count(distinct ENTERPRISE_ID) 
select count(0)
from 
(
select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
from (
select *
from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'
and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220'
) t 
) t2 where rn = 1 




select * from   g_a_02059_day



select tabname from   syscat.tables where tabname like '%ENTERPRISE_MEMBER_MID%'

select '20110313',a.ENTERPRISE_ID,a.USER_ID,'1340','2','20110313','1' from   bass2.DW_ENTERPRISE_MEMBER_MID_20110313


select * from   bass2.DW_ENTERPRISE_msg_20110313
where ENTERPRISE_ID in (
select distinct ENTERPRISE_ID
from 
(
select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
from (
select *
from G_A_02055_DAY where STATUS_ID ='1' and MANAGE_MODE = '2'
and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340'
) t 
) t2 where rn = 1 
)


select * from   bass2.DW_ENTERPRISE_msg_20110313
where ENTERPRISE_ID in (
select distinct ENTERPRISE_ID
from 
(
select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
from (
select *
from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'
and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220'
) t 
) t2 where rn = 1 


select * from   bass2.DW_ENTERPRISE_msg_20110313
where ENTERPRISE_ID in (
select  distinct ENTERPRISE_ID
from 
(
select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
from (
select *
from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'
and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220'
) t 
) t2 where rn = 1 
)


select * from   app.sch_control_task where control_code like '%02031%'

--1040	无线商务电话	
select * from    g_a_02059_day  where ENTERPRISE_BUSI_TYPE = '1040'
--02054	集团客户业务订购关系
select * from   g_a_02054_day where  ENTERPRISE_BUSI_TYPE = '1040'

select * from  bass2.dw_enterprise_msg_201102 where enterprise_id = '89100000000705'

select * from   

select * from bass1.ALL_DIM_LKP_160 c  where bass1_tbid='BASS_STD1_0108'
and C.BASS1_VALUE  in ('1230','1241','1249','1220','1320','1040')

select * from bass2.dim_enterprise_product
where service_name like  '%话%'

select * from app.sch_control_before
where control_code = 'BASS1_G_A_02059_DAY.tcl'

select * from app.sch_control_before
where control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl'

select * from app.sch_control_before
where control_code in (
select BEFORE_CONTROL_CODE from app.sch_control_before
where control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl'
)

select * from app.sch_control_before
where control_code
in (
select BEFORE_CONTROL_CODE from app.sch_control_before
where control_code in (
select BEFORE_CONTROL_CODE from app.sch_control_before
where control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl'
)
)

select * from app.sch_control_before
where control_code
in
(
select BEFORE_CONTROL_CODE from app.sch_control_before
where control_code
in (
select BEFORE_CONTROL_CODE from app.sch_control_before
where control_code in (
select BEFORE_CONTROL_CODE from app.sch_control_before
where control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl'
)
)
)


select * from app.sch_control_before
where control_code like '%enterprise%'


select * from app.sch_control_before
where before_control_code like '%BASS2_Dw_product_ds.tcl%'

BASS1_G_A_02059_DAY.tcl	BASS2_Dw_enterprise_extsub_rela_ds.tcl

select * from app.sch_control_task
where control_code like '%w_product%'

insert into app.sch_control_before
values('BASS1_G_A_02059_DAY.tcl','BASS2_Dw_product_regsp_ds.tcl')
insert into app.sch_control_before
values('BASS1_G_A_02059_DAY.tcl','BASS1_G_A_02054_DAY.tcl')
insert into app.sch_control_before
values('BASS1_G_A_02059_DAY.tcl','BASS2_Dw_product_ds.tcl')
insert into app.sch_control_before
values('BASS1_G_A_02059_DAY.tcl','BASS2_Dw_enterprise_member_mid_ds.tcl')
insert into app.sch_control_before
values('BASS1_G_A_02059_DAY.tcl','BASS2_Dw_enterprise_msg_ds.tcl')
commit



select * from   app.sch_control_task where upper(control_code) like '%02054%'

BASS2_Dw_enterprise_member_mid_ds.tcl
dw_enterprise_member_mid_yyyymmdd


BEFORE_CONTROL_CODE
BASS2_Dw_enterprise_sub_ds.tcl
BASS2_Dw_enterprise_membersub_ds.tcl
BASS2_Dw_product_ds.tcl


select * from app.sch_control_before
where control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl'


select * from   app.sch_control_task where upper(control_code) like '%DW_PRODUCT_REGSP_DS%'

DW_PRODUCT_REGSP_DS



BASS1_G_A_02059_DAY.tcl	BASS2_Dw_enterprise_extsub_rela_ds.tcl

select * from G_A_02059_DAY_0315modify
except
select * from   G_A_02059_DAY


select '20110314'
											 ,a.enterprise_id
											 ,b.user_id
											 ,'1040'
											 ,'3'
											 ,'20110314'
											 ,a.status_id
								from 	bass1.g_a_02054_day a,
								      bass2.dw_enterprise_member_mid_20110314 b,
								      bass2.dw_enterprise_msg_20110314 c,
								      bass2.dw_product_20110314 d
								where a.enterprise_id=b.enterprise_id
								  and a.enterprise_id=c.enterprise_id
								  and b.user_id=d.user_id
								  and d.usertype_id in (1,2,3,6)
								  and a.enterprise_busi_type='1040'
								  and a.time_id=20110314
								  and a.status_id='1'



select 20110314
											 ,a.enterprise_id
											 ,b.user_id
											 ,'1040'
											 ,'3'
											 ,'20110314'
											 ,a.status_id
								from 	bass1.g_a_02054_day a,
								      (select distinct enterprise_id,user_id 
								        from BASS1.G_A_02059_DAY 
								       where enterprise_busi_type='1040' 
								         and time_id<20110314) b
								where a.enterprise_id=b.enterprise_id
								  and a.time_id=20110314
								  and a.enterprise_busi_type='1040'
								  and a.status_id='2'
                                  
select A.*,char(a.RUNTIME/60)||'min',char(a.RUNTIME/60/60)||'hr' from   app.sch_control_runlog A
where control_code like 'BASS1%'
and a.RUNTIME/60 > 10
ORDER BY RUNTIME DESC 


select * from   g_s_02059_day

                                  
                                  
select * from g_a_02059_day
where time_id = 20110315
and ENTERPRISE_BUSI_TYPE = '1040'

select * from   g_a_01004_day


select * from   BASS2.DW_ENTERPRISE_ACCOUNT_20110315

select * from   bass2.dwd_cust_msg_20110315
select a.CUSTTYPE_ID, a.* from   bass2.dwd_cust_msg_20110315 a

select * from   app.sch_control_runlog where control_code like '%01004%'                                  

select count(0)
from (
 select distinct a.enterprise_id from
   (select time_id,enterprise_id,cust_statu_typ_id from bass1.G_A_01004_DAY where time_id <= 20110301 ) a,
   (select enterprise_id,max(time_id) as time_id  from bass1.G_A_01004_DAY 
                                                 where time_id<=20110301
                                              group by enterprise_id) b
where a.time_id=b.time_id and a.enterprise_id=b.enterprise_id and a.cust_statu_typ_id = '20'
) t

10:04 9075
select count(0)
from (
select distinct t.enterprise_id
from 
(
select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn 
from bass1.G_A_01004_DAY 
where time_id <= 20110301 
) t where t.rn = 1 and  cust_statu_typ_id = '20'
) tt 


10537


select enterprise_id,max(time_id) as time_id  from bass1.G_A_01004_DAY 
                                                 where time_id<=20110301
                                              group by enterprise_id
                                              
select  from   G_A_02004_DAY

select count(0) from   G_A_02062_DAY where char(time_id) like '201102%'

select count(0),count(distinct user_id) from   G_A_02062_DAY where time_id < 20110301 and time_id >= 20110201


02004



select time_id,count(0) from    G_A_02051_DAY
group by time_id
order by 1 desc 

select count(0),count(distinct user_id ) from    G_A_02004_DAY

select a.brand_id, count(0),count(distinct a.user_id) from  
(select user_id,BRAND_ID from G_A_02004_DAY  where SIM_CODE = '1' and USERTYPE_ID = '1') a 
,(select distinct user_id from   G_A_02062_DAY where time_id <20110301
 and STATUS_ID = '1') b 
where a.user_id = b.user_id 
group by a.brand_id

2	5190

2	6438	4663


(select user_id,BRAND_ID ,row_number()over(partition by )
from G_A_02004_DAY  
where SIM_CODE = '1' and USERTYPE_ID = '1') a 


4622

6438
time_id <20110301

select * from   bass1.G_A_02062_DAY



char(time_id) like '201102%'



select SIM_CODE ,BRAND_ID, count(0) 
,  count(distinct USER_ID ) 
from bass1.G_A_02004_DAY 
group by  SIM_CODE ,BRAND_ID
order by 1 





declare global temporary table session.int_check_user_status
    (
   user_id        CHARACTER(15),
   product_no     CHARACTER(15),
   test_flag      CHARACTER(1),
   sim_code       CHARACTER(15),
   usertype_id    CHARACTER(4),
   create_date    CHARACTER(15),
   brand_id       CHARACTER(4),
   time_id        int
    )                            
partitioning key           
 (
   user_id    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp


--抓取用户资料入表
insert into session.int_check_user_status (
     user_id    
    ,product_no 
    ,test_flag  
    ,sim_code   
    ,usertype_id  
    ,create_date
    ,brand_id
    ,time_id )
select e.user_id
    ,e.product_no  
    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
    ,e.sim_code
    ,f.usertype_id  
    ,e.create_date  
    ,e.brand_id
    ,f.time_id       
from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id
                ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.g_a_02004_day
where time_id<=20110324 ) e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where time_id<=20110324 ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1
commit


select a.sim_code,a.brand_id, count(0),count(distinct a.user_id) from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <20110301
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and usertype_id NOT IN ('2010','2020','2030','9000')
        and test_flag='0'
group by a.sim_code,a.brand_id
order by 1,2



4622

2   	4622	4622
西藏	7610	2658	193	7	2858		4622		4622	7480	130


select a.sim_code,a.brand_id, count(0),count(distinct a.user_id) from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <20110301
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and usertype_id NOT IN ('2010','2020','2030','9000')
        and test_flag='0'
group by a.sim_code,a.brand_id
order by 1,2



select distinct user_id from   G_A_02062_DAY where time_id <20110301
 and STATUS_ID = '1'
 
 select count(0),count(distinct user_id ) from   G_A_02062_DAY
 where time_id <20110301
 and STATUS_ID = '1'
 
 7610
 
 select distinct user_id from  G_A_02062_DAY
  where time_id <20110301
 and STATUS_ID = '1'
 except
 select distinct a.user_id from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <20110301
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and usertype_id NOT IN ('2010','2020','2030','9000')
        and test_flag='0'

 
 select * from  G_A_02062_DAY where user_id = '89460000193920'
 select * from  session.int_check_user_status where user_id = '89460000193920'
 
  select * from  session.int_check_user_status where user_id
  in (select distinct user_id from  G_A_02062_DAY
  where time_id <20110301
 and STATUS_ID = '1'
 except
 select distinct a.user_id from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <20110301
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and usertype_id NOT IN ('2010','2020','2030','9000')
        and test_flag='0')
 
 
 select * from  G_A_02062_DAY
 where user_id = '89657334067011'
 select OP_TIME,count(0) from  
 bass2.DW_ENTERPRISE_SUB_ds
 group by 
 OP_TIME
 
 select count(0) from  bass2.DW_ENTERPRISE_SUB_ds
 bass2.DW_ENTERPRISE_SUB_201011
 select * from  bass2.DW_ENTERPRISE_SUB_201011where user_id = '89657334067011'

 select count(0) from  bass2.DW_ENTERPRISE_SUB_201102
 

select * from   bass2.dw_product_20110314
where user_id = '89657334067011'
13638968395
89657334067011	89603001340010	89601002853991	100	6	0	0	1	0	0	[NULL]	13638968395	460008961120700	[NULL]	[NULL]	896	896	1059	10590005	96011006	4	[NULL]	300001911090	0	279	10	1	1	10	101	89610012	5	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	2010-02-24 	2010-05-25 	2010-11-29 	2010-11-29 	0	0	0	0	0.00	0.00	0.00	0.00	0	0	0	0	0.00	0.00	0.00	0.00	0.00	0.00

 89657334067011 	13638968395    	0	0              	2020	20100224       	1   	20101129
89657334006049 	13638960960    	0	0              	2020	20100118       	1   	20101130


 select * from  session.int_check_user_status where user_id
  in 
  ('89157333603327'
,'89257332979496'
,'89257332980422'
,'89457332326899'
,'89460000193947'
,'89560000363718'
,'89560000363941'
,'89657334006210'
,'89157334021842'
,'89257332979460'
,'89257332980702'
,'89257333374671'
,'89457333600524'
,'89460000193798'
,'89460000339034'
,'89560000363916'
,'89560000363965'
,'89657333802412'
,'89157332844445'
,'89257332979510'
,'89360000266593'
,'89457332319392'
,'89457333729869'
,'89460000193999'
,'89560000363735'
,'89560000363905'
,'89657333043867'
,'89657333685357'
,'89657333695270'
,'89157332768972'
,'89157333603386'
,'89257332980520'
,'89257332980594'
,'89360000266440'
,'89360000266468'
,'89457333386157'
,'89460000193789'
,'89460000270137'
,'89560000363897'
,'89657333689064'
,'89157333346941'
,'89157333942045'
,'89257332979480'
,'89360000266538'
,'89360000266610'
,'89360000266686'
,'89460000193802'
,'89560000363758'
,'89657333802374'
,'89657334006049'
,'89657334067011'
,'89360000266456'
,'89360000266532'
,'89457332240376'
,'89457333035055'
,'89460000193749'
,'89657333688890'
,'89701170013122'
,'89457333038292'
,'89460000193714'
,'89560000363747'
,'89560000363894'
,'89657333685472'
,'89157332834451'
,'89157333603404'
,'89360000266628'
,'89460000193679'
,'89460000193777'
,'89460000193932'
,'89460000221143'
,'89657333695116'
,'89157332768979'
,'89157332993008'
,'89157332994577'
,'89357333334596'
,'89457332341379'
,'89460000193825'
,'89657333693001'
,'89157334002261'
,'89157334021848'
,'89360000266437'
,'89360000266524'
,'89457332341392'
,'89457333038299'
,'89460000194023'
,'89560000363859'
,'89657333043881'
,'89657334037819'
,'89157332826412'
,'89360000266462'
,'89360000266559'
,'89460000193616'
,'89460000193810'
,'89657333562860'
,'89757334249044'
,'89157332993069'
,'89157333346957'
,'89157333942188'
,'89460000193858'
,'89657333696722'
,'89657333832637'
,'89157332895961'
,'89157333603323'
,'89157333942125'
,'89360000266273'
,'89457333338562'
,'89560000363911'
,'89657333295749'
,'89657333736478'
,'89157333346922'
,'89157333872113'
,'89360000266286'
,'89360000266674'
,'89360000266703'
,'89460000193774'
,'89460000193920'
,'89460000193937'
,'89460000193954'
,'89560000363769'
,'89560000363789'
,'89657334006083'
,'89757334252690'
,'89157333942117'
,'89157334021855'
,'89357333334573'
,'89360000266167'
,'89360000266306'
,'89360000266516'
,'89560000363889'
,'89657333800018')
order by 3


 select count(0) from  bass2.DW_ENTERPRISE_SUB_ds





declare global temporary table session.int_check_user_status
    (
   user_id        CHARACTER(15),
   product_no     CHARACTER(15),
   test_flag      CHARACTER(1),
   sim_code       CHARACTER(15),
   usertype_id    CHARACTER(4),
   create_date    CHARACTER(15),
   brand_id       CHARACTER(4),
   time_id        int
    )                            
partitioning key           
 (
   user_id    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp


--抓取用户资料入表
insert into session.int_check_user_status (
     user_id    
    ,product_no 
    ,test_flag  
    ,sim_code   
    ,usertype_id  
    ,create_date
    ,brand_id
    ,time_id )
select e.user_id
    ,e.product_no  
    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
    ,e.sim_code
    ,f.usertype_id  
    ,e.create_date  
    ,e.brand_id
    ,f.time_id       
from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id
                ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.g_a_02004_day
where time_id<=20110231 ) e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where time_id<=20110231 ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1
commit


select a.sim_code,a.brand_id, count(distinct a.user_id) user_cnt from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <20110301
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and usertype_id NOT IN ('2010','2020','2030','9000')
        and test_flag='0'
group by a.sim_code,a.brand_id
order by 1,2


select case when a.sim_code = '1' then '数据SIM卡用户' else '非数据SIM卡用户' end 
,a.brand_id, count(distinct a.user_id) user_cnt from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <20110301
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and usertype_id NOT IN ('2010','2020','2030','9000')
        and test_flag='0'
group by case when a.sim_code = '1' then '数据SIM卡用户' else '非数据SIM卡用户' end 
,a.brand_id
order by 1,2



 select distinct user_id from  G_A_02062_DAY
  where time_id <20110301
 and STATUS_ID = '1'
 except
 select distinct a.user_id from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <20110301
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and usertype_id NOT IN ('2010','2020','2030','9000')
        and test_flag='0'

select count(0)
from (       
 select distinct user_id from  G_A_02062_DAY
  where time_id <20110301
 and STATUS_ID = '1'
 except
 select distinct a.user_id from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <20110301
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and a.usertype_id NOT IN ('2010','2020','2030','9000')
        and a.test_flag='0'     
) t        


 select test_flag,usertype_id,a.user_id
 from  session.int_check_user_status a
 where user_id
  in (select distinct user_id from  G_A_02062_DAY
  where time_id <20110301
 and STATUS_ID = '1'
 except
 select distinct a.user_id from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <20110301
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and a.usertype_id NOT IN ('2010','2020','2030','9000')
        and a.test_flag='0'  )
 order by 1,2

结果:
TEST_FLAG	USERTYPE_ID	3
0	2010	20
0	2020	33
1	1010	67
1	2020	10

select * from bass2.dw_enterprise_membersub_ds a
where a.USER_ID in 
('89157333942045'
,'89157333603327'
,'89257332980422'
,'89157332993008'
,'89157333942117'
,'89257332980702'
,'89460000339034'
,'89257333374671'
,'89157332993069'
,'89157333942188'
,'89157332895961'
,'89157333942125'
,'89157333603323'
,'89157334002261'
,'89157333872113'
,'89157333603386'
,'89257332980594'
,'89257332980520'
,'89157332834451'
,'89157333603404'
,'89460000221143'
,'89657333695116'
,'89157332768972'
,'89657333689064'
,'89460000270137'
,'89457333386157'
,'89657334006083'
,'89757334252690'
,'89657333688890'
,'89657334037819'
,'89457333338562'
,'89657333736478'
,'89657333295749'
,'89657333696722'
,'89657333832637'
,'89157332826412'
,'89757334249044'
,'89657333562860'
,'89457333600524'
,'89657333802412'
,'89657333800018'
,'89157332768979'
,'89657333693001'
,'89157332994577'
,'89157332844445'
,'89657333695270'
,'89657333685357'
,'89457333729869'
,'89657334006210'
,'89657333685472'
,'89657333802374'
,'89657334067011'
,'89657334006049')



select *
from 
 (
 select a.*,row_number()over(partition by user_id order by time_id desc) rn 
 from  BASS1.G_A_02062_DAY a
 where
 a.USER_ID in 
('89157333942045'
,'89157333603327'
,'89257332980422'
,'89157332993008'
,'89157333942117'
,'89257332980702'
,'89460000339034'
,'89257333374671'
,'89157332993069'
,'89157333942188'
,'89157332895961'
,'89157333942125'
,'89157333603323'
,'89157334002261'
,'89157333872113'
,'89157333603386'
,'89257332980594'
,'89257332980520'
,'89157332834451'
,'89157333603404'
,'89460000221143'
,'89657333695116'
,'89157332768972'
,'89657333689064'
,'89460000270137'
,'89457333386157'
,'89657334006083'
,'89757334252690'
,'89657333688890'
,'89657334037819'
,'89457333338562'
,'89657333736478'
,'89657333295749'
,'89657333696722'
,'89657333832637'
,'89157332826412'
,'89757334249044'
,'89657333562860'
,'89457333600524'
,'89657333802412'
,'89657333800018'
,'89157332768979'
,'89657333693001'
,'89157332994577'
,'89157332844445'
,'89657333695270'
,'89657333685357'
,'89457333729869'
,'89657334006210'
,'89657333685472'
,'89657333802374'
,'89657334067011'
,'89657334006049')
) t where rn = 1 

20110212	89401560000169      	89460000270137      	1249	3	13908940254    	4 	1	cmm.hn                                                      	20110212	1	1

select * from   g_a_02008_day
where user_id = '89460000270137'


20110226	89460000270137      	2020
20101123	89460000270137      	1022

20110212	89401560000169      	89460000270137      	1249	3	13908940254    	4 	1	cmm.hn                                                      	20110212	1	1
20110127

89157333942188      

select * from   g_a_02008_day
where user_id = '89157333942188'
20110201	89157333942188      	2010


20110212
20110127
20110127
20110127
20110127
20101231



select * from    bass2.dwd_enterprise_sub_20101231 a
where
 a.SUB_ID in 
('89157333942045'
,'89157333603327'
,'89257332980422'
,'89157332993008'
,'89157333942117'
,'89257332980702'
,'89460000339034'
,'89257333374671'
,'89157332993069'
,'89157333942188'
,'89157332895961'
,'89157333942125'
,'89157333603323'
,'89157334002261'
,'89157333872113'
,'89157333603386'
,'89257332980594'
,'89257332980520'
,'89157332834451'
,'89157333603404'
,'89460000221143'
,'89657333695116'
,'89157332768972'
,'89657333689064'
,'89460000270137'
,'89457333386157'
,'89657334006083'
,'89757334252690'
,'89657333688890'
,'89657334037819'
,'89457333338562'
,'89657333736478'
,'89657333295749'
,'89657333696722'
,'89657333832637'
,'89157332826412'
,'89757334249044'
,'89657333562860'
,'89457333600524'
,'89657333802412'
,'89657333800018'
,'89157332768979'
,'89657333693001'
,'89157332994577'
,'89157332844445'
,'89657333695270'
,'89657333685357'
,'89457333729869'
,'89657334006210'
,'89657333685472'
,'89657333802374'
,'89657334067011'
,'89657334006049')



bass2.ods_product_ins_prod_20110315


select count(0)
from 
(
select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
from (
select *
from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'
and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220'
) t 
) t2 where rn = 1 

select count(0)
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from G_A_02061_DAY
where ENTERPRISE_BUSI_TYPE = '1220'
and  MANAGE_MODE = '2'
and STATUS_ID ='1'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 

select * from   bass2.dw_enterprise_msg_his_20110316
where enterprise_id in (
'89108911013886'      
,'89100000000645'     )



select
         TIME_ID
        ,ENTERPRISE_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,ORDER_DATE
        ,2 STATUS_ID
from          
(
select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
from (
select *
from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'
and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220'
) t 
) t2 where rn = 1 


select *
from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'
and time_id >=20110301 and ENTERPRISE_BUSI_TYPE = '1220'


select count(0) from G_A_02054_DAY


select * from BASS1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108'

select count(0)
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from G_A_02061_DAY
where ENTERPRISE_BUSI_TYPE = '1220'
and  MANAGE_MODE = '2'
and STATUS_ID ='1'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 


select * from G_A_02061_DAY
where ENTERPRISE_BUSI_TYPE = '1220'
and  MANAGE_MODE = '2'
and STATUS_ID ='1'



CREATE TABLE BASS1.G_A_02054_DAY_20110317BAK
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    TIME_ID
   ) USING HASHING


insert into BASS1.G_A_02054_DAY_20110317BAK
select * from G_A_02054_DAY


select count(0) from    G_A_02054_DAY_20110317BAK

54697

select count(0) from    G_A_02054_DAY


CREATE TABLE BASS1.G_A_02054_DAY_0317_1220repair
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    TIME_ID
   ) USING HASHING




   ALTER TABLE BASS1.G_A_02054_DAY_0317_1220repair
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
  

insert into BASS1.G_A_02054_DAY_0317_1220repair
select
         20110317
        ,ENTERPRISE_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,ORDER_DATE
        ,'2' STATUS_ID
from g_a_02054_day 
where STATUS_ID ='1' 
			and MANAGE_MODE = '2'
			and time_id <20110301 
			and ENTERPRISE_BUSI_TYPE = '1220'

            


CREATE TABLE BASS1.G_A_02061_DAY_20110317BAK
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  USER_ID               CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  EXPIRE_DATE           CHARACTER(8),
  PAY_TYPE              CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (USER_ID,
    TIME_ID
   ) USING HASHING

ALTER TABLE BASS1.G_A_02061_DAY_20110317BAK
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE

insert into   bass1.G_A_02061_DAY_20110317BAK
select * from  bass1.G_A_02061_DAY



select count(0) from     bass1.G_A_02061_DAY

select count(0) from    bass1.G_A_02061_DAY_20110317BAK



CREATE TABLE BASS1.G_A_02061_DAY_0317repair
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  USER_ID               CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  EXPIRE_DATE           CHARACTER(8),
  PAY_TYPE              CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (USER_ID,
    TIME_ID
   ) USING HASHING

ALTER TABLE BASS1.G_A_02061_DAY_0317repair
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
  
insert into   bass1.G_A_02061_DAY_0317repair
select * from G_A_02061_DAY
where ENTERPRISE_BUSI_TYPE = '1220'
and  MANAGE_MODE = '2'
and STATUS_ID ='1'



delete from   bass1.G_A_02061_DAY_0317repair
insert into   bass1.G_A_02061_DAY_0317repair
select 
         20110317
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,'20110317' EXPIRE_DATE
        ,PAY_TYPE
        ,ORDER_DATE
        ,'2' STATUS_ID
from G_A_02061_DAY
where ENTERPRISE_BUSI_TYPE = '1220'
and  MANAGE_MODE = '2'
and STATUS_ID ='1'




select * from    bass1.G_A_02061_DAY_0317repair


	select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=20110316
    571304
    
	select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=20110315
240017

select * from   bass1.G_RULE_CHECK
where time_id = 20110316

20110316	R161_15	571304.00000	240017.00000	1.38026	0.00000



select * from   bass1.g_s_22202_day where time_id = 20110316
20110316	20110316	876691      	571304      	217         



delete from BASS1.G_A_02054_DAY_0317_1220repair
insert into BASS1.G_A_02054_DAY_0317_1220repair
select
         20110317
        ,ENTERPRISE_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,ORDER_DATE
        ,'2' STATUS_ID
from 
(
select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
from (
select *
from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'
and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220'
) t 
) t2 where rn = 1 



delete from   bass1.G_A_02061_DAY_0317repair
insert into   bass1.G_A_02061_DAY_0317repair
select 
         20110317
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,'20110317' EXPIRE_DATE
        ,PAY_TYPE
        ,ORDER_DATE
        ,'2' STATUS_ID
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from G_A_02061_DAY
where ENTERPRISE_BUSI_TYPE = '1220'
and  MANAGE_MODE = '2'
and STATUS_ID ='1'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 

select count(0) from   bass2.dw_product_td_20110315

select *    
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from G_A_02061_DAY
where ENTERPRISE_BUSI_TYPE = '1220'
and  MANAGE_MODE = '2'
and length(trim(user_id)) = 14
) t
) t2 
where rn = 1 
and STATUS_ID ='1'


select * from   bass1.G_A_02061_DAY_0317repair

delete from   bass1.G_A_02061_DAY_0317repair
insert into   bass1.G_A_02061_DAY_0317repair
select 
         20110317
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,'20110317' EXPIRE_DATE
        ,PAY_TYPE
        ,ORDER_DATE
        ,'2' STATUS_ID
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from G_A_02061_DAY
where ENTERPRISE_BUSI_TYPE = '1220'
and  MANAGE_MODE = '2'
and length(trim(user_id)) = 14
) t
) t2 
where rn = 1 
and STATUS_ID ='1'


select
         20110317
        ,ENTERPRISE_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,ORDER_DATE
        ,'2' STATUS_ID
from 
(
select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
from (
select *
from g_a_02054_day where  MANAGE_MODE = '2'
and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220'
) t 
) t2 where rn = 1 and STATUS_ID ='1' 


delete from BASS1.G_A_02054_DAY_0317_1220repair
insert into BASS1.G_A_02054_DAY_0317_1220repair
select
         20110317
        ,ENTERPRISE_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,ORDER_DATE
        ,'2' STATUS_ID
from 
(
select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
from (
select *
from g_a_02054_day where  MANAGE_MODE = '2'
and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220'
) t 
) t2 where rn = 1 and STATUS_ID ='1' 


select * from    BASS1.G_A_02061_DAY


insert into BASS1.G_A_02061_DAY
select * from bass1.G_A_02061_DAY_0317repair



insert into BASS1.G_A_02054_DAY 
		select * from bass1.G_A_02054_DAY_0317_1220repair
        
select * from   bass1.G_A_02054_DAY_0317_1220repair
except
select * from   BASS1.G_A_02054_DAY 


select test_mark , count(0) 
--,  count(distinct test_mark ) 
from bass2.dw_product_20110315 
group by  test_mark 
order by 1 

select count(0) from  bass2.dw_product_20110315 
where   test_mark = 0
        
        

declare global temporary table session.int_check_user_status
    (
   user_id        CHARACTER(15),
   product_no     CHARACTER(15),
   test_flag      CHARACTER(1),
   sim_code       CHARACTER(15),
   usertype_id    CHARACTER(4),
   create_date    CHARACTER(15),
   brand_id       CHARACTER(4),
   time_id        int
    )                            
partitioning key           
 (
   user_id    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp
--抓取用户资料入表
insert into session.int_check_user_status (
     user_id    
    ,product_no 
    ,test_flag  
    ,sim_code   
    ,usertype_id  
    ,create_date
    ,brand_id
    ,time_id )
select e.user_id
    ,e.product_no  
    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
    ,e.sim_code
    ,f.usertype_id  
    ,e.create_date  
    ,e.brand_id
    ,f.time_id       
from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id
                ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.g_a_02004_day
where time_id<=20110316 ) e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where time_id<=20110316 ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1


select case when a.sim_code = '1' then '数据SIM卡用户' else '非数据SIM卡用户' end 
,a.brand_id, count(distinct a.user_id) user_cnt from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <=20110316
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and usertype_id NOT IN ('2010','2020','2030','9000')
        and test_flag='0'
group by case when a.sim_code = '1' then '数据SIM卡用户' else '非数据SIM卡用户' end 
,a.brand_id
order by 1,2



select count(0)
from (       
 select distinct user_id from  G_A_02062_DAY
  where time_id <=20110316
 and STATUS_ID = '1'
 except
 select distinct a.user_id from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <=20110316
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and a.usertype_id NOT IN ('2010','2020','2030','9000')
        and a.test_flag='0'     
) t    

select 
         TIME_ID
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,PRODUCT_NO
        ,INDUSTRY_ID
        ,GPRS_TYPE
        ,DATA_SOURCE
        ,CREATE_DATE
        ,STATUS_ID
from 
(
select a.*,row_number()over(partition by user_id order by time_id desc ) rn  
from   G_A_02062_DAY a
) t where rn = 1 and STATUS_ID = '1'
 except
 select distinct a.user_id from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <=20110316
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and a.usertype_id NOT IN ('2010','2020','2030','9000')
        and a.test_flag='0'     


select * from   G_A_02062_DAY



select 
         20110317 TIME_ID
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,PRODUCT_NO
        ,INDUSTRY_ID
        ,GPRS_TYPE
        ,DATA_SOURCE
        ,CREATE_DATE
        ,'2' STATUS_ID
from 
(
select a.*,row_number()over(partition by user_id order by time_id desc ) rn  
from   G_A_02062_DAY a
) t where rn = 1 and STATUS_ID = '1'
and user_id in 
(
select USER_ID
from 
(
select a.*,row_number()over(partition by user_id order by time_id desc ) rn  
from   G_A_02062_DAY a
) t where rn = 1 and STATUS_ID = '1'
except
select user_id 
from session.int_check_user_status a
        where a.usertype_id NOT IN ('2010','2020','2030','9000')
        and a.test_flag='0'
)        




CREATE TABLE BASS1.G_A_02062_DAY_20110317bak
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  USER_ID               CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  PRODUCT_NO            CHARACTER(15),
  INDUSTRY_ID           CHARACTER(2),
  GPRS_TYPE             CHARACTER(1),
  DATA_SOURCE           CHARACTER(60),
  CREATE_DATE           CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (USER_ID,
    TIME_ID
   ) USING HASHING

ALTER TABLE BASS1.G_A_02062_DAY_20110317bak
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE

insert into BASS1.G_A_02062_DAY_20110317bak
select * from  BASS1.G_A_02062_DAY
       74990 row(s) affected.
select count(0) from  BASS1.G_A_02062_DAY
     

CREATE TABLE BASS1.G_A_02062_DAY_20110317repair
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  USER_ID               CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  PRODUCT_NO            CHARACTER(15),
  INDUSTRY_ID           CHARACTER(2),
  GPRS_TYPE             CHARACTER(1),
  DATA_SOURCE           CHARACTER(60),
  CREATE_DATE           CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (USER_ID,
    TIME_ID
   ) USING HASHING

ALTER TABLE BASS1.G_A_02062_DAY_20110317repair
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE




delete from  BASS1.G_A_02062_DAY_20110317repair 
insert into  BASS1.G_A_02062_DAY_20110317repair
select 
         20110317 TIME_ID
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,PRODUCT_NO
        ,INDUSTRY_ID
        ,GPRS_TYPE
        ,DATA_SOURCE
        ,CREATE_DATE
        ,'2' STATUS_ID
from 
(
select a.*,row_number()over(partition by user_id order by time_id desc ) rn  
from   G_A_02062_DAY a
) t where rn = 1 and STATUS_ID = '1'
and user_id in 
(
select USER_ID
from 
(
select a.*,row_number()over(partition by user_id order by time_id desc ) rn  
from   G_A_02062_DAY a
) t where rn = 1 and STATUS_ID = '1'
except
select user_id 
from session.int_check_user_status a
        where a.usertype_id NOT IN ('2010','2020','2030','9000')
        and a.test_flag='0'
)        




delete from  BASS1.G_A_02062_DAY_20110317repair 
insert into  BASS1.G_A_02062_DAY_20110317repair
select 
         20110317 TIME_ID
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,PRODUCT_NO
        ,INDUSTRY_ID
        ,GPRS_TYPE
        ,DATA_SOURCE
        ,CREATE_DATE
        ,'2' STATUS_ID
from 
(
select a.*,row_number()over(partition by user_id order by time_id desc ) rn  
from   G_A_02062_DAY a where TIME_ID <=20110316
) t where rn = 1 and STATUS_ID = '1'
and user_id in 
(
select USER_ID
from 
(
select a.*,row_number()over(partition by user_id order by time_id desc ) rn  
from   G_A_02062_DAY a  where TIME_ID <=20110316
) t where rn = 1 and STATUS_ID = '1'
except
select user_id 
from session.int_check_user_status a
        where a.usertype_id NOT IN ('2010','2020','2030','9000')
        and a.test_flag='0'
)        




select  from   BASS1.G_A_02059_DAY where time_id = 20110321
except
select * from   BASS1.G_A_02059_DAY_20110321fix1340

594

select count(0),count(distinct user_id) from   BASS1.G_A_02059_DAY_20110321fix1340


select * from   APP.SCH_CONTROL_RUNLOG 
where control_code like 'BASS1%'
order by endtime desc

select * from   app.sch_control_before where control_code like '%02059%'

select * from   G_A_02008_DAY where time_id > 20110101

select * from   G_A_01008_DAY
select * from   APP.SMS_SEND_INFO


select * from  app.sch_control_alarm 
where alarmtime >=  timestamp('20110311'||'000000') 
--and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc 

select * from   bass1.int_program_data

select * from   
bass1.int_verf_err_list

select * from   
bass1.int_all_job_log

select * from   USYS_INT_CONTROL



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
(
select cust_id from bass2.dw_product_20110316
where user_id in
('89160000208968'
,'89160000409386'
,'89160000688192'
,'89157334175039'
,'89160000523755'
,'89160000454937'
,'89160000604528'
,'89157334216159'
,'89160000696074'
,'89157334323731')
)
 



select * from   g_a_02059_day


select ENTERPRISE_BUSI_TYPE , count(0) 
--,  count(distinct ENTERPRISE_BUSI_TYPE ) 
from bass1.g_a_02059_day 
group by  ENTERPRISE_BUSI_TYPE 
order by 1 


select count(0)
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from g_a_02059_day a
where TIME_ID < 20110301
and MANAGE_MODE = '2'
and ENTERPRISE_BUSI_TYPE ='1340'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 and STATUS_ID ='1'


select count(0)
from 
(
select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
from (
select *
from G_A_02055_DAY where MANAGE_MODE = '2'
and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340'
) t 
) t2 where rn = 1  and  STATUS_ID ='1'  



select ENTERPRISE_ID
								from 
										(
										select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
										from (
													select *
													from G_A_02055_DAY where MANAGE_MODE = '2'
													and time_id <=20110321 and ENTERPRISE_BUSI_TYPE = '1340'
													) t 
										) t2 where rn = 1  and  STATUS_ID ='1'  
                                        
                                        
select * from   G_A_02059_DAY where      ENTERPRISE_BUSI_TYPE = '1340'
                                   
select * from   bass1.g_a_02054_day
                                   
                                   


CREATE TABLE BASS1.G_A_02059_DAY_20110321bak
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  USER_ID               CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (USER_ID,
    TIME_ID
   ) USING HASHING

ALTER TABLE BASS1.G_A_02059_DAY_20110321bak
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE

insert into G_A_02059_DAY_20110321bak
select * from G_A_02059_DAY
                                   
select count(0) from    G_A_02059_DAY_20110321bak
select count(0) from    G_A_02059_DAY


drop table BASS1.G_A_02059_DAY_20110321fix1340

CREATE TABLE BASS1.G_A_02059_DAY_20110321fix1340
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  USER_ID               CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (USER_ID,
    TIME_ID
   ) USING HASHING

ALTER TABLE BASS1.G_A_02059_DAY_20110321fix1340
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE

select * from   BASS1.G_A_02059_DAY_20110321fix1340

select * from   G_A_02055_DAY




insert into BASS1.G_A_02059_DAY_20110321fix1340
select  20110321
				,a.ENTERPRISE_ID
				,b.USER_ID
				,'1340'
				,'2'
				,'20110321'
				,'1' from   bass2.DW_ENTERPRISE_MEMBER_MID_20110320 a
				where a.ENTERPRISE_ID 
				in (
							select  ENTERPRISE_ID
								from 
										(
										select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
										from (
													select *
													from G_A_02055_DAY where MANAGE_MODE = '2'
													and time_id <=20110320 and ENTERPRISE_BUSI_TYPE = '1340'
													) t 
										) t2 where rn = 1  and  STATUS_ID ='1'  
					)



select count(0),count(distinct user_id ) from     BASS1.G_A_02059_DAY_20110321fix1340
7297	7297

select * from   BASS1.G_A_02059_DAY_20110321fix1340

select count(0) from    BASS1.G_A_02059_DAY_20110321fix1340

insert into BASS1.G_A_02059_DAY_20110321fix1340
select  20110321
				,a.ENTERPRISE_ID
				,b.USER_ID
				,'1340'
				,'2'
				,'20110321'
				,'1'                
--select count(0),count(distinct   b.USER_ID)              
from  (
						select  ENTERPRISE_ID,
							from 
									(
									select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
									from (
												select *
												from G_A_02055_DAY where MANAGE_MODE = '2'
												and time_id <=20110320 and ENTERPRISE_BUSI_TYPE = '1340'
												) t 
									) t2 where rn = 1  and  STATUS_ID ='1'  
					) a,
				  bass2.dw_enterprise_member_mid_20110320 b,
		      bass2.dw_enterprise_msg_20110320 c,
		      bass2.dw_product_20110320 d
				where a.enterprise_id=b.enterprise_id
				  and a.enterprise_id=c.enterprise_id
				  and b.user_id=d.user_id
				  and d.usertype_id in (1,2,3,6)


7039 row(s) affected.

select * from   BASS1.G_A_02059_DAY_20110321fix1340

                                   
select * from   	bass1.g_a_02055_day

select count(0) from    (
select * from  G_A_02059_DAY where time_id = 20110321
except 
select * from   G_A_02059_DAY_20110321fix1340
) t

594+7037
select count(0) from   G_A_02059_DAY where time_id = 20110321

7631


select count(0)
from 
(
select user_id
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from g_a_02059_day a
where TIME_ID < 20110301
and MANAGE_MODE = '2'
and ENTERPRISE_BUSI_TYPE ='1340'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 and STATUS_ID ='1'
) t 
join G_A_02059_DAY_20110321fix1340 b on t.user_id = b.user_id 

select test_mark,count(0) from   G_A_02059_DAY_20110321fix1340 a
join bass2.dw_product_20110320 b on a.user_id = b.user_id       
group by test_mark

select a.enterprise_id,b.ENTERPRISE_NAME,count(0) from   G_A_02059_DAY_20110321fix1340 a 
join bass2.dw_enterprise_msg_20110320 b on a.enterprise_id = b.enterprise_id
group by a.enterprise_id,b.ENTERPRISE_NAME



delete from BASS1.G_A_02059_DAY_20110321fix1340
insert into BASS1.G_A_02059_DAY_20110321fix1340
select  20110321
				,a.ENTERPRISE_ID
				,b.USER_ID
				,'1340'
				,'2'
				,'20110321'
				,'1' 
from  (
				select  ENTERPRISE_ID
					from 
							(
							select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
							from (
										select *
										from G_A_02055_DAY where MANAGE_MODE = '2'
										and time_id <=20110320 and ENTERPRISE_BUSI_TYPE = '1340'
										) t 
							) t2 where rn = 1  and  STATUS_ID ='1'  
			) a,
		  bass2.dw_enterprise_member_mid_20110320 b,
      bass2.dw_enterprise_msg_20110320 c,
      bass2.dw_product_20110320 d
where a.enterprise_id=b.enterprise_id
			and a.enterprise_id=c.enterprise_id
			and b.user_id=d.user_id
			and d.usertype_id in (1,2,3,6)
			and d.test_mark = 0 



								select 20110320
											 ,a.enterprise_id
											 ,b.user_id
											 ,'1340'
											 ,'3'
											 ,'20110320'
											 ,a.status_id
								from 	bass1.g_a_02055_day a,
								      bass2.dw_enterprise_member_mid_20110320 b,
								      bass2.dw_enterprise_msg_20110320 c,
								      bass2.dw_product_20110320 d
								where a.enterprise_id=b.enterprise_id
								  and a.enterprise_id=c.enterprise_id
								  and b.user_id=d.user_id
								  and d.usertype_id in (1,2,3,6)
								  and a.enterprise_busi_type='1340'
								  and a.time_id=20110320
								  and a.status_id='1'
								  and d.test_mark = 0		                             
                                  
select * from                                     
 BASS1.G_A_02059_DAY


select time_id , count(0) 
--,  count(distinct time_id ) 
from BASS1.G_A_02059_DAY 
group by  time_id 
order by 1 


select * from   app.sch_control_map where program_name like '%test%'

select * from   bass1.int_program_data where program_name like '%02059%'

select count(0) from   syscat.tables 
                                  
135515
9234


rename BASS2.DW_ENTERPRISE_MEMBER_MID_20100520 to DW_ENTERPRISE_MEMBER_MID_20110320

rename BASS2.DW_ENTERPRISE_MEMBER_MID_20110320 to DW_ENTERPRISE_MEMBER_MID_20100520
select * from   G_A_02059_DAY_0315modify

insert into app.sch_control_before
values('BASS1_G_A_02059_DAY.tcl','BASS1_G_A_02055_DAY.tcl')

select * from   app.sch_control_before where control_code = 'BASS1_G_A_02059_DAY.tcl'

                                 

select count(0)
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from g_a_02059_day a
where TIME_ID < 20110301
and MANAGE_MODE = '2'
and ENTERPRISE_BUSI_TYPE ='1340'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 and STATUS_ID ='1'
                                 
                                 
select * from g_a_02059_day a
where TIME_ID < 20110321
and MANAGE_MODE = '3'
and ENTERPRISE_BUSI_TYPE ='1040'
and length(trim(user_id)) = 14
                                 
                                 
select count(0)
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from g_a_02059_day a
where TIME_ID < 20110316
and MANAGE_MODE = '3'
and ENTERPRISE_BUSI_TYPE ='1040'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 and STATUS_ID ='1'



select TIME_ID , count(0) 
--,  count(distinct TIME_ID ) 
from bass1.g_a_02059_day 
group by  TIME_ID 
order by 1 
                                 
20110321	7631

594
7039

select count(0)
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from g_a_02059_day a
where TIME_ID <= 20110321
and MANAGE_MODE = '2'
and ENTERPRISE_BUSI_TYPE ='1340'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 and STATUS_ID ='1'
                                 
                                                                  
                                                                  
select * from g_a_02059_day a
where TIME_ID <= 20110321
and MANAGE_MODE = '2'
and ENTERPRISE_BUSI_TYPE ='1340'
and length(trim(user_id)) = 14

select count(0),count(distinct user_id) from g_a_02059_day a
where TIME_ID <= 20110321
and MANAGE_MODE = '2'
and ENTERPRISE_BUSI_TYPE ='1340'
and length(trim(user_id)) = 14


select  count(0),count(distinct b.user_id)
from  (
				select  ENTERPRISE_ID
					from 
							(
							select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
							from (
										select *
										from G_A_02055_DAY where MANAGE_MODE = '2'
										and time_id <=20110320 and ENTERPRISE_BUSI_TYPE = '1340'
										) t 
							) t2 where rn = 1  and  STATUS_ID ='1'  
			) a,
		  bass2.dw_enterprise_member_mid_20110320 b,
      bass2.dw_enterprise_msg_20110320 c,
      bass2.dw_product_20110320 d
where a.enterprise_id=b.enterprise_id
			and a.enterprise_id=c.enterprise_id
			and b.user_id=d.user_id
			and d.usertype_id in (1,2,3,6)
			and d.test_mark = 0                                                                   

select count(0) from G_A_02059_DAY_20110321fix1340
            

select count(0)
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from g_a_02059_day a
where TIME_ID <= 20110321
and MANAGE_MODE = '2'
and ENTERPRISE_BUSI_TYPE ='1340'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 and STATUS_ID ='1'
                                 


select count(0)
from 
(
select t.*,row_number()over(partition by user_id order by time_id ) rn 
from 
(
select * from g_a_02059_day a
where TIME_ID < 20110316
and MANAGE_MODE = '3'
and ENTERPRISE_BUSI_TYPE ='1040'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 and STATUS_ID ='1'

--44 条

select * from   bass1.g_i_06032_day


                                             
select count(0)
from 
(
select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
from 
(
select * from G_A_02061_DAY
where ENTERPRISE_BUSI_TYPE = '1220'
and  MANAGE_MODE = '2'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 and  STATUS_ID ='1'

select * from   BASS1.G_A_02061_DAY

 select * from bass2.DWD_PS_NET_NUMBER_20110321 where number_segment='1820808'

select * from    g_a_02059_day 


select count(0) 
from   
(
select a.*,row_number()over(partition by user_id order by time_id desc )  rn 
from g_a_02059_day a  where time_id <= 20110320  
) t where rn = 1 and status_id = '1'



BASS2.DIM_CONTROL_INFO.del
BASS2.DIM_DEVICE_INFO.del
BASS2.DIM_DEVICE_PROFILE.del
BASS2.DIM_PROPERTY_INFO.del
BASS2.DIM_PROPERTY_VALUE_RANGE.del
select * from   g_i_77780_day

select count(0) from BASS2.DIM_CONTROL_INFO
select count(0) from BASS2.DIM_DEVICE_INFO
18386
select count(0) from BASS2.DIM_DEVICE_PROFILE
1319690
select count(0) from BASS2.DIM_PROPERTY_INFO
select count(0) from BASS2.DIM_PROPERTY_VALUE_RANGE

select * from   BASS2.DIM_DEVICE_PROFILE


CREATE TABLE BASS1.G_A_02059_DAY_down20110321
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  USER_ID               CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (USER_ID,
    TIME_ID
   ) USING HASHING



drop table BASS1.G_A_02059_DAY_DOWN20110321

select * from   BASS1.G_A_02059_DAY_DOWN20110321


select * from BASS2.DIM_CONTROL_INFO        
select * from BASS2.DIM_DEVICE_INFO         
select * from BASS2.DIM_DEVICE_PROFILE      
select * from BASS2.DIM_PROPERTY_INFO       
select * from BASS2.DIM_PROPERTY_VALUE_RANGE
select tabschema,tabname from   syscat.tables where tabname like '%TAC%'

select * from   BASS2.DIM_TACNUM_DEVID
select count(0),count(distinct device_id) from BASS2.DIM_DEVICE_INFO         
18386	18385

select count(0),count(distinct device_id) from BASS2.DIM_DEVICE_PROFILE      
1319690	18385


select time_id ,count(distinct tac_num)
--,  count(distinct time_id ) 
from BASS2.DIM_TACNUM_DEVID 
group by  time_id 
order by 1 



select a.time_id ,count(distinct tac_num)
--,  count(distinct time_id ) 
from BASS2.DIM_TACNUM_DEVID  a , 
BASS2.DIM_DEVICE_INFO        b 
where b.device_id = a.dev_id 
group by  a.time_id 
order by 1 
201102	33619



select time_id , count(0) ,count(distinct tac_num),count(distinct dev_id)
--,  count(distinct time_id ) 
from BASS2.DIM_TACNUM_DEVID 
group by  time_id 
order by 1 
201102	33622	33620	15165


select * from   BASS2.DIM_TERM_TAC
                                             
                                             
select count(0) ,count(distinct tac_num) from    BASS2.DIM_TERM_TAC
27564	27564

select int(replace(char(current date - 1 days),'-','')) from bass2.dual

select * from  bass2.dw_product_imei_rela_ds          
                                   
select * from   BASS1.G_I_02047_MONTH 

select count(0),count(distinct device_id) from BASS2.DIM_DEVICE_PROFILE      


select 1701 as kpi_id,city_id,count(distinct user_id) as kpi_value,brand_id
from bass2.
where td_user_mark = 1 and usertype_id in (1,2,9) and userstatus_id in (1,2,3,6,8) and test_mark<>1



select count(user_id) , count(distinct user_id)
                    from bass2.dw_product_201102
                   where usertype_id in (1,2,9) 
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1


select count(distinct substr(imei,1,8)) from    bass2.dw_product_mobilefunc_201102
where  
usertype_id in (1,2,9) 
 and userstatus_id in (1,2,3,6,8)
 
26774
 
 
33019
                                   
vs 33620
                                   

select  count(distinct substr(a.imei,1,8))
--,  count(distinct time_id ) 
from  bass2.dw_product_mobilefunc_201102  a , 
BASS2.DIM_TACNUM_DEVID        b 
where  substr(a.imei,1,8) = b.tac_num 
                   and usertype_id in (1,2,9) 
                     and userstatus_id in (1,2,3,6,8)
                     --and test_mark<>1





declare global temporary table session.t_imei
    (
        imei varchar(18)
        ,tac_num varchar(10)
    )                            
partitioning key           
 (
   imei    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp

insert into  session.t_imei
select imei,substr(a.imei,1,8) 
from   bass2.dw_product_mobilefunc_201102 a
  
select count(0),count(distinct tac_num ) from    session.t_imei                                   
4110313	33019

select count(distinct a.tac_num)
from 
( select distinct tac_num from session.t_imei      ) a ,
BASS2.DIM_TACNUM_DEVID        b 
where a.tac_num = b.TAC_NUM

19618

 select  from   bass2.dw_product_imei_rela_ds
 
 
 declare global temporary table session.t_imei2
    (
        imei varchar(18)
        ,tac_num varchar(10)
    )                            
partitioning key           
 (
   imei    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp

insert into session.t_imei2
 select distinct imei ,substr(a.imei,1,8)  from   bass2.dw_product_imei_rela_ds  a where op_time = '2011-03-22'
 and last_use_date >= '2010-12-22'

 select count(0),count(distinct tac_num ) from    session.t_imei2                                   
1701113	30058


 
select count(distinct a.tac_num)
from 
( select distinct tac_num from session.t_imei2      ) a ,
BASS2.DIM_TACNUM_DEVID        b 
where a.tac_num = b.TAC_NUM
17760


02047

select * from    bass1.G_S_02047_MONTH

select time_id,count(0) from   bass1.G_S_02047_MONTH
group by time_id




select * from   syscat.tables where tabname like '%G_I_02006_MONTH%'



CREATE TABLE BASS1.G_I_77780_DAY
 (
	 TIME_ID            		NUMBER(8)      NOT NULL       ----数据日期        
	,ENTERPRISE_ID      		CHAR(20)       NOT NULL       ----集团客户标识    
	,ID                 		CHAR(9)        NOT NULL 				----ID              
	,ENTERPRISE_NAME    		CHAR(60)            					----集团客户名称    
	,ORG_TYPE           		CHAR(5)             					----机构类型        
	,ADDR_CODE          		CHAR(6)             					----地址代码        
	,CITY               		CHAR(20)            					----城市地区        
	,REGION             		CHAR(20)            					----区县            
	,COUNTY             		CHAR(20)            					----乡镇            
	,DOOR_NO            		CHAR(60)            					----门牌            
	,AREA_CODE          		CHAR(5)             					----区号            
	,PHONE_NO1          		CHAR(11)            					----电话1           
	,PHONE_NO2          		CHAR(10)            					----电话2           
	,POST_CODE          		CHAR(6)             					----邮政编码        
	,INDUSTRY_TYPE      		CHAR(4)             					----行业类型        
	,EMPLOYEE_CNT       		CHAR(8)             					----职员            
	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----产业单位数      
	,ECONOMIC_TYPE      		CHAR(3)             					----经济类型        
	,OPEN_YEAR          		CHAR(4)             					----开业1           
	,OPEN_MONTH         		CHAR(2)             					----开业2           
	,SHAREHOLDER        		CHAR(1)             					----控股            
	,GROUP_TYPE         		CHAR(1)             					----集团类型        
	,MANAGE_STYLE       		CHAR(1)             					----经营形式        
	,OPERATE_REVENUE_CLASS	CHAR(2)           						----营业收入分类    
	,CAPITAL_CLASS      		CHAR(2)           						----资产分类        
	,INDUSTRY_CLASS_CODE		CHAR(2)    NOT NULL         	----行业分类编码    
	,CUST_STATUS        		CHAR(2)             					----集团客户状态    
	,CUST_INFO_SRC_ID   		CHAR(1)             					----集团客户资料来源
	,UPLOAD_TYPE_ID     		CHAR(1)    NOT NULL         	----上传种类标识    
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,ENTERPRISE_ID) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;





CREATE TABLE BASS1.G_I_77780_DAY
 (
	 TIME_ID            		INTEGER        NOT NULL       ----数据日期        
	,ENTERPRISE_ID      		CHAR(20)       NOT NULL       ----集团客户标识    
	,ID                 		CHAR(9)             					----ID              
	,ENTERPRISE_NAME    		CHAR(60)            					----集团客户名称    
	,ORG_TYPE           		CHAR(5)             					----机构类型        
	,ADDR_CODE          		CHAR(6)             					----地址代码        
	,CITY               		CHAR(20)            					----城市地区        
	,REGION             		CHAR(20)            					----区县            
	,COUNTY             		CHAR(20)            					----乡镇            
	,DOOR_NO            		CHAR(60)            					----门牌            
	,AREA_CODE          		CHAR(5)             					----区号            
	,PHONE_NO1          		CHAR(11)            					----电话1           
	,PHONE_NO2          		CHAR(10)            					----电话2           
	,POST_CODE          		CHAR(6)             					----邮政编码        
	,INDUSTRY_TYPE      		CHAR(4)             					----行业类型        
	,EMPLOYEE_CNT       		CHAR(8)             					----职员            
	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----产业单位数      
	,ECONOMIC_TYPE      		CHAR(3)             					----经济类型        
	,OPEN_YEAR          		CHAR(4)             					----开业1           
	,OPEN_MONTH         		CHAR(2)             					----开业2           
	,SHAREHOLDER        		CHAR(1)             					----控股            
	,GROUP_TYPE         		CHAR(1)             					----集团类型        
	,MANAGE_STYLE       		CHAR(1)             					----经营形式        
	,OPERATE_REVENUE_CLASS	CHAR(2)           						----营业收入分类    
	,CAPITAL_CLASS      		CHAR(2)           						----资产分类        
	,INDUSTRY_CLASS_CODE		CHAR(2)    NOT NULL         	----行业分类编码    
	,CUST_STATUS        		CHAR(2)             					----集团客户状态    
	,CUST_INFO_SRC_ID   		CHAR(1)             					----集团客户资料来源
	,UPLOAD_TYPE_ID     		CHAR(1)    NOT NULL         	----上传种类标识    
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,ENTERPRISE_ID) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;


select * from   BASS1.G_I_77780_DAY




CREATE TABLE BASS1.G_I_77780_DAY_MID
 (
	 TIME_ID            		INTEGER        NOT NULL       ----数据日期        
	,ENTERPRISE_ID      		CHAR(20)       NOT NULL       ----集团客户标识    
	,ID                 		CHAR(9)             					----ID              
	,ENTERPRISE_NAME    		CHAR(60)            					----集团客户名称    
	,ORG_TYPE           		CHAR(5)             					----机构类型        
	,ADDR_CODE          		CHAR(6)             					----地址代码        
	,CITY               		CHAR(20)            					----城市地区        
	,REGION             		CHAR(20)            					----区县            
	,COUNTY             		CHAR(20)            					----乡镇            
	,DOOR_NO            		CHAR(60)            					----门牌            
	,AREA_CODE          		CHAR(5)             					----区号            
	,PHONE_NO1          		CHAR(11)            					----电话1           
	,PHONE_NO2          		CHAR(10)            					----电话2           
	,POST_CODE          		CHAR(6)             					----邮政编码        
	,INDUSTRY_TYPE      		CHAR(4)             					----行业类型        
	,EMPLOYEE_CNT       		CHAR(8)             					----职员            
	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----产业单位数      
	,ECONOMIC_TYPE      		CHAR(3)             					----经济类型        
	,OPEN_YEAR          		CHAR(4)             					----开业1           
	,OPEN_MONTH         		CHAR(2)             					----开业2           
	,SHAREHOLDER        		CHAR(1)             					----控股            
	,GROUP_TYPE         		CHAR(1)             					----集团类型        
	,MANAGE_STYLE       		CHAR(1)             					----经营形式        
	,OPERATE_REVENUE_CLASS	CHAR(2)           						----营业收入分类    
	,CAPITAL_CLASS      		CHAR(2)           						----资产分类        
	,INDUSTRY_CLASS_CODE		CHAR(2)    NOT NULL         	----行业分类编码    
	,CUST_STATUS        		CHAR(2)             					----集团客户状态    
	,CUST_INFO_SRC_ID   		CHAR(1)             					----集团客户资料来源
	,UPLOAD_TYPE_ID     		CHAR(1)    NOT NULL         	----上传种类标识    
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,ENTERPRISE_ID) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY_MID
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
  
  
  select dbpartitionnum(PRODUCT_NO) as partitionnum_num, count(*)
as rows from G_S_21003_MONTH
group by dbpartitionnum(PRODUCT_NO)
order by dbpartitionnum (PRODUCT_NO)


select * from   syscat.tables where tabname like '%777%'



--目标表
drop table BASS1.G_I_77780_DAY
CREATE TABLE BASS1.G_I_77780_DAY
 (
	 TIME_ID            		INTEGER        NOT NULL       ----数据日期        
	,ENTERPRISE_ID      		CHAR(20)       NOT NULL       ----集团客户标识    
	,ID                 		CHAR(9)        								----ID*              
	,ENTERPRISE_NAME    		CHAR(60)       								----集团客户名称*    
	,ORG_TYPE           		CHAR(5)             					----机构类型 							BASS_STD_0001       
	,ADDR_CODE          		CHAR(6)             					----地址代码*        
	,CITY               		CHAR(20)            					----城市地区*        
	,REGION             		CHAR(20)            					----区县*            
	,COUNTY             		CHAR(20)            					----乡镇*            
	,DOOR_NO            		CHAR(60)            					----门牌*            
	,AREA_CODE          		CHAR(5)             					----区号*            
	,PHONE_NO1          		CHAR(11)            					----电话1*           
	,PHONE_NO2          		CHAR(10)            					----电话2*           
	,POST_CODE          		CHAR(6)             					----邮政编码*        
	,INDUSTRY_TYPE      		CHAR(4)             					----行业类型 							BASS_STD1_0113       
	,EMPLOYEE_CNT       		CHAR(8)             					----职员            
	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----产业单位数*      
	,ECONOMIC_TYPE      		CHAR(3)             					----经济类型 							BASS_STD_0002       
	,OPEN_YEAR          		CHAR(4)             					----开业1           
	,OPEN_MONTH         		CHAR(2)             					----开业2           
	,SHAREHOLDER        		CHAR(1)             					----控股  								BASS_STD_0005          
	,GROUP_TYPE         		CHAR(1)             					----集团类型 							BASS_STD_0003       
	,MANAGE_STYLE       		CHAR(1)             					----经营形式      				BASS_STD_0004  
	,OPERATE_REVENUE_CLASS	CHAR(2)           						----营业收入分类 					BASS_STD_0006   
	,CAPITAL_CLASS      		CHAR(2)           						----资产分类     					BASS_STD_0007   
	,INDUSTRY_CLASS_CODE		CHAR(2)    NOT NULL         	----行业分类编码 					BASS_STD1_0043   
	,CUST_STATUS        		CHAR(2)             					----集团客户状态    
	,CUST_INFO_SRC_ID   		CHAR(1)             					----集团客户资料来源
	,UPLOAD_TYPE_ID     		CHAR(1)    NOT NULL         	----上传种类标识    
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,ENTERPRISE_ID) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

select * from   app.sch_control_before 
where before_control_code like '%22012%'


select 'xxxxx',count(0) from app.sch_control_alarm where alarmtime >= timestamp('20110324'||'000000') and flag = -1 and control_code like 'BASS1%'

select * from    bass1.int_02011_snapshot

select * from   app.sch_control_task where control_code like '%02011%'

--自动点告警 BASS1_INT_CHECK_INDEX_WAVE_DAY.tcl
select * from app.sch_control_runlog
where control_code like '%INT_02011_SNAPSHOT.tcl%'








select 'insert into BASS1.G_MON_D_INTERFACE values('||unit_code||','||substr(data_file,1,1)||')' from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=1



select TABNAME  from   syscat.tables where tabschema = 'BASS1'

select control_code 
						            from app.sch_control_runlog 
						            where date(endtime)=date(current date)
						                  and flag=0

select flag from   app.sch_control_runlog where control_code like '%INT_CHECK_RUNLOG_DAY%'
and date(endtime)=date(current date) and flag=0



select a.unit_code ,substr(b.TABNAME,3,1) ,b.TABNAME
from 
( select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=1 ) a,
 syscat.tables b where a.unit_code = substr(b.TABNAME,5,5)
and b.tabschema = 'BASS1'
and tabname like '%DAY'
g_i_01

select * from    APP.G_UNIT_INFO 

select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 2 days
--and flag = -1
--and control_code like 'BASS1%'
order by alarmtime desc 

select * from   app.sch_control_task where control_code like '%15304%'

select * from   bass2.VGOP_15304_20110324


select count(0)
                    from bass2.dw_product_20110324 a ,bass2.dwd_cust_msg_20110324 b 
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1
                     and a.cust_id = b.cust_id
                     
select b.cust_name , count(0)
                    from bass2.dw_product_20110324 a ,bass2.dwd_cust_msg_20110324 b 
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1
                     and a.cust_id = b.cust_id
group by    b.cust_name
order by 2 desc 
                   
select a.*,b.*
                    from bass2.dw_product_20110324 a ,bass2.dwd_cust_msg_20110324 b 
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1
                     and a.cust_id = b.cust_id

select count(0) from session.int_check_user_status where create_date = '20110324'
and test_flag = '0'

                     
4231

 3月23日	 3月24日
2880	4239

select * from   g_s_22012_day
20110324	20110324	`4231      	1636785     	23021010    	412022      	3873747     	3424      	335706      

20110323	20110323	`2878      	1635972     	22901457    	389311      	3975018     	120       	329440      


select b.cust_name,a.CRM_BRAND_ID1, count(0)
                    from bass2.dw_product_20110323 a ,bass2.dwd_cust_msg_20110323 b 
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1
                     and a.cust_id = b.cust_id
and  b.CHANNEL_ID in
(91000047
,91000048
,91000049
,91000046
,91000050
,91000035
,97000019
,91000012
,97000023)
group by    b.cust_name, a.CRM_BRAND_ID1
order by 3 desc                      





select b.cust_name,b.CHANNEL_ID, count(0)
                    from bass2.dw_product_20110322 a ,bass2.dwd_cust_msg_20110322 b 
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1
                     and a.cust_id = b.cust_id
group by    b.cust_name, b.CHANNEL_ID
order by 3 desc 

拉萨市东城区代理店	91000046	126
拉萨市南城区代理店	91000049	114
拉萨市西城区代理店	91000047	113
拉萨市开发区代理店	91000048	78
拉萨市北城区代理店	91000050	77
拉萨市邮政局	91000012	38
拉萨万利文体商贸有限公司	91000035	11
巴桑	97000019	1
王玉萍	97000023	1
索南顿珠	97000023	1
普布次仁	97000019	1
欧珠	97000019	1
洛桑顿珠	97000019	1
洪吉	97000023	1
次仁桑珠	97000019	1
次仁德吉	97000019	1
巴桑	97000023	1




declare global temporary table session.int_check_user_status    (   user_id        CHARACTER(15),   product_no     CHARACTER(15),   test_flag      CHARACTER(1),   sim_code       CHARACTER(15),   usertype_id    CHARACTER(4),   create_date    CHARACTER(15),   time_id        int)                            partitioning key            (   user_id     ) using hashing           with replace on commit preserve rows not logged in tbs_user_temp;insert into session.int_check_user_status (         user_id            ,product_no         ,test_flag          ,sim_code           ,usertype_id          ,create_date        ,time_id )select e.user_id        ,e.product_no          ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag        ,e.sim_code        ,f.usertype_id          ,e.create_date          ,f.time_id       from (select user_id,create_date,product_no,sim_code,usertype_id                    ,row_number() over(partition by user_id order by time_id desc ) row_id     from bass1.g_a_02004_day  where time_id<=20110324) einner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id                  from bass1.g_a_02008_day               where time_id<=20110324 ) f on f.user_id=e.user_idwhere e.row_id=1 and f.row_id=1;commit;select * from bass2.ODS_PRODUCT_ORD_CUST_20110324where bill_id in(select product_no from bass2.dw_product_20110324where cust_id in(   select cust_id from bass2.dw_cust_20110324    where cust_id in        (        select cust_id from bass2.dw_product_20110324        where user_id in            (            select user_id from session.int_check_user_status             where create_date = '20110324'            and test_flag='0'            )        )    and cust_name like '%代理%'));select business_id,count(*) from bass2.ODS_PRODUCT_ORD_CUST_20110324where bill_id in(select product_no from bass2.dw_product_20110324where cust_id in(   select cust_id from bass2.dw_cust_20110324    where cust_id in        (        select cust_id from bass2.dw_product_20110324        where user_id in            (            select user_id from session.int_check_user_status             where create_date = '20110324'            and test_flag='0'            )        )    and cust_name like '%代理%'))group by business_id;BUSINESS_ID	    2191000000007	31191000000008	2191000000012	1191000000145	2063191000001017	45191000001024	116193000000001	12select * from bass2.ODS_PROD_UP_PRODUCT_ITEM_20110324where product_item_id in (191000000145);



select b.CHANNEL_ID, count(0)
                    from bass2.dw_product_20110324 a ,bass2.dwd_cust_msg_20110324 b 
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1
                     and a.cust_id = b.cust_id
and b.cust_name like '%代理%'
group by    b.CHANNEL_ID
order by 2 desc 

91000047	568
91000048	535
91000049	231
91000046	177
91000050	173



select b.CHANNEL_ID, count(0)
                    from bass2.dw_product_20110321 a ,bass2.dwd_cust_msg_20110321 b 
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1
                     and a.cust_id = b.cust_id
and b.CHANNEL_ID in (
91000047
,91000048
,91000049
,91000046
,91000050
)
group by    b.CHANNEL_ID
order by 1 desc 


select * from bass2.ODS_PROD_UP_PRODUCT_ITEM_20110324
where product_item_id in (191000000145);

select * from BASS2.ODS_CHANNEL_INFO_20110324 b
where 
 b.CHANNEL_ID in (
91000047
,91000048
,91000049
,91000046
,91000050
)
order by 1 desc 



drop table BASS1.G_A_02059_DAY_down20110321
CREATE TABLE BASS1.G_A_02059_DAY_down20110321
 (
  ENTERPRISE_ID         CHARACTER(20),
  USER_ID               CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (USER_ID
   ) USING HASHING;

select count(0)
from 
(
select 
        ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,ORDER_DATE
        ,STATUS_ID from bass1.G_A_02059_DAY
except 
select 
        ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,ORDER_DATE
        ,STATUS_ID from  bass1.G_A_02059_DAY_down20110321
        ) t

select ENTERPRISE_ID
from 
(
select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
from (
select *
from BASS1.G_A_02055_DAY_down20110321 where MANAGE_MODE = '2'
and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340'
) t 
) t2 where rn = 1  and  STATUS_ID ='1'  
except
select ENTERPRISE_ID
from 
(
select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
from (
select *
from BASS1.G_A_02055_DAY where MANAGE_MODE = '2'
and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340'
) t 
) t2 where rn = 1  and  STATUS_ID ='1'  


      

select * from   G_A_02055_DAY where ENTERPRISE_ID = '89103000041929'

select * from   G_A_02055_DAY_down20110321 where ENTERPRISE_ID = '89103000041929'
select * from   
bass2.dw_enterprise_member_mid_20110327 where enterprise_id = '89103000041929'


select count(0)
from (
select distinct trim(ENTERPRISE_ID) ENTERPRISE_ID,trim(USER_ID) from bass1.G_A_02059_DAY_down20110321 a 
except
select distinct trim(ENTERPRISE_ID) ENTERPRISE_ID,trim(USER_ID) from bass1.G_A_02059_DAY b
) t
select count(0) , count(distinct trim(ENTERPRISE_ID)) ENTERPRISE_ID from bass1.G_A_02059_DAY_down20110321

bass1.G_A_02059_DAY_down20110321

select ascii(ENTERPRISE_ID),ENTERPRISE_ID
from  bass1.G_A_02059_DAY_down20110321 
where ENTERPRISE_ID like  '%89403000904884%'
fetch first 1 rows only

     
select ascii(ENTERPRISE_ID)
from  bass1.G_A_02059_DAY where ENTERPRISE_ID = '89403000904884'
fetch first 1 rows only


drop table BASS1.G_A_02059_DAY_down20110321
CREATE TABLE BASS1.G_A_02059_DAY_down20110321
 (
  ENTERPRISE_ID         CHARACTER(20),
  USER_ID               CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (USER_ID
   ) USING HASHING;
   
   
   
  select * from BASS1.G_A_02055_DAY where enterprise_id = '89202999392356'
  
  
  
  
CREATE TABLE BASS1.G_A_02055_DAY_down20110321
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  PAY_TYPE              CHARACTER(1),
  CREATE_MODE           CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    TIME_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_A_02055_DAY_down20110321
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

select * from   BASS1.G_A_02055_DAY_down20110321


select * from   bass2.dw_enter

bass2.DWD_GROUP_ORDER_FEATUR_${timestamp}

select tabname from syscat.tables where tabname like '%DWD_ENTERPRISE_SUB%'

select * from   G_A_02055_DAY_down20110321 where ENTERPRISE_ID = '89103000041929'


select date(DONE_DATE) from   bass2.DWD_ENTERPRISE_SUB_20101001 WHERE GROUP_ID = '89103000041929'
AND DONE_DATE LIKE '2009-11%'

select 20110328
,a.ENTERPRISE_ID
,a.ENTERPRISE_BUSI_TYPE
,a.MANAGE_MODE
,a.PAY_TYPE
,a.CREATE_MODE
,'20110328' ORDER_DATE
,'2' STATUS_ID
from bass1.G_A_02055_DAY_down20110321 a
where  ENTERPRISE_ID = '89103000041929'


select * from G_A_02055_DAY
where ENTERPRISE_ID = '89103000041929'
and ENTERPRISE_BUSI_TYPE = '1340'

delete from   G_A_02055_DAY where ENTERPRISE_ID = '89103000041929'

select * from app.sch_control_alarm 

