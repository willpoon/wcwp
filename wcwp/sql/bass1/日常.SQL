 
---查看调度告警
select * from APP.SCH_CONTROL_ALARM  
where flag=-1 and control_code like '%BASS1%'
order by alarmtime  desc;

select * from APP.sch_control_task
where control_code like '%csvc%'

select * from g_rule_check
where rule_code='C1'
order by time_id desc

select * from g_rule_check where rule_code in ('R173') order by time_id desc

---一致性检查
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
  and time_id=20110228;


select * from bass2.dim_enterprise_product where time_id=20110228

---调整
update bass1.g_s_22012_day set m_off_users='68' 
where time_id=20110228;
commit;

update bass1.g_s_22012_day set m_new_users='2037' 
where time_id=20110228;
commit;


update bass1.g_s_22201_day set mtl_td_3gbook_mark='112' 
where time_id=20110228;
commit;

select sum(bigint(mtl_td_3gbook_mark)) from bass1.g_s_22201_day where time_id=20110228


---接口文件级返回检查
select  * from APP.G_FILE_REPORT
where filename like '%20110201%' and err_code='00';


---接口记录集返回检查
select * from app.g_runlog
where time_id=20110228
and return_flag=1



---月接口文件级返回检查
select  * 
--distinct left(right(filename,16),5),err_code 
from APP.G_FILE_REPORT
where filename like '%_201101_%' 
and err_code='00'
and length(filename)=length('s_13100_201002_03014_01_001.dat')
order by deal_time desc;


---集团下发tac码个数检查
select count(distinct tac_num) from bass2.DIM_TACNUM_DEVID d
,bass2.DIM_DEVICE_PROFILE e
where d.time_id=201102
  and e.time_id='201102'
  and d.dev_id = e.DEVICE_ID
  and e.value in ('001001008','001001006','001012004');


---申请重送接口，需处理返回日志
delete from app.g_runlog
where time_id=20110228 and unit_code='04015';
commit;

--将调度置为超时
insert into  APP.SCH_CONTROL_ALARM(control_code,alarmtime,flag,content) values('TR1_L_02027',current timestamp,-1,'超时');
commit;

select * from APP.SCH_CONTROL_RUNLOG
where control_code like '%BASS1_G_S_04015_DAY%'

CONTROL_CODE	BEGINTIME	ENDTIME	RUNTIME	FLAG
BASS1_G_S_04015_DAY.tcl	2010-9-1 7:46:17.232083	2010-9-1 7:46:57.507259	40	-1


update  APP.SCH_CONTROL_RUNLOG  set ENDTIME=current timestamp
where control_code like '%BASS1_G_S_04015_DAY%';
commit;



--重新运行
update APP.SCH_CONTROL_ALARM  set flag=1  where flag=-1 and  control_code in ('BASS1_INT_CHECK_C034_DAY.tcl');
update APP.SCH_CONTROL_RUNLOG set flag=-2 where flag=-1 and  control_code in ('BASS1_INT_CHECK_C034_DAY.tcl');
---处理但不重新运行
update APP.SCH_CONTROL_ALARM  set flag=1  where flag=-1 and  control_code in ('TR1_L_A98012');
update APP.SCH_CONTROL_RUNLOG set flag=0 where  flag=-1 and  control_code in ('TR1_L_A98012');

commit;

select * from APP.SCH_CONTROL_RUNLOG
where control_code like '%TR1_L_A98012%'


select * from APP.SCH_CONTROL_ALARM
where control_code like '%TR1_L_A98012%'

select user_id,product_no,count(*)
from bass2.dwd_product_20110228
group by user_id,product_no
having  count(*)>1
where product_no='D13889092915'
89160000160057
89160000163403

select * from g_a_02008_day
where user_id in ('89160000160057')
and time_id=20100818;


delete from g_a_02008_day
where user_id in ('89160000160057')
and time_id=20100818;
commit;


select * from bass2.dw_acct_shoulditem_20100818
where user_id in ('89160000160057')


select * from bass2.dw_product_20100818
where product_no='13908919762'
USER_ID	CUST_ID	ACCT_ID
89101110014707	89101110415697	89101110394686
89157332064925	89102999613771	89101000706428
89157332716529	89103000152024	89101001406855

select * from bass2.dw_cust_20100818
where cust_id='89103000152024'


SELECT * FROM BASS2.ETL_TASK_LOG
WHERE CYCLE_ID='20100818'

SELECT * FROM BASS2.ETL_TASK_RUNNING
WHERE CYCLE_ID='20100818'
  AND TASK_ID='I43004'


select * from app.sch_control_before
where before_control_code like '%BASS2_Dw_cust_ds.tcl%'
and 

select * from APP.SCH_CONTROL_ALARM  
where flag=-1
order by alarmtime  desc;

update APP.SCH_CONTROL_ALARM set flag=1
where control_code='TR1_L_I43004'
  and date(alarmtime)='2010-08-18';

update app.SCH_CONTROL_RUNLOG set flag=0
where control_code='TR1_L_I43004';
commit;



select time_id,count(*) from G_S_04015_DAY
where time_id>=20100630
group by time_id


select * from app.sch_control_before
where before_control_code like '%BASS2_Dwd_cust_msg_ds.tcl%'


select * from app.sch_control_before
where before_control_code in
(
select control_code from app.sch_control_before
where before_control_code like '%BASS2_Dwd_cust_msg_ds.tcl%'
)


BASS2_Dwd_cust_msg_ds.tcl

select * from bass2.DWD_MR_OPER_CDR_20110228
where time_id=20110228


select distinct custtype_id from bass2.dw_product_20100921


select * from app.sch_control_task
where control_code like '%5044%'


select distinct user_id from g_a_02008_day
except
select distinct user_id from g_a_02004_day

select * from G_S_04015_DAY
where time_id=20110228


delete from APP.G_FILE_REPORT
where filename like '%20110228_04015%';
commit;


select * from bass2.dw_comp_all_20110228

select char(count(distinct comp_product_no)) c02
         from  bass2.dw_comp_all_20110228
         where comp_brand_id in(3,4)
           and in_call_counts>0

delete from app.g_runlog 
where time_id=20110228
and unit_code='04015';
commit;


select * from  g_a_02053_day
where time_id=20110228
and product_no is null

delete from  g_a_02053_day
where time_id=20100908
and product_no is null;
commit;

select * from bass2.dwd_product_regsp_20100908
where id_value='13908996463'

select * from bass2.dwd_product_regsp_20100908
where user_id='89160000086954'

select * from bass2.dw_product_20100908
where user_id='89160000086954'

select * from bass2.DW_NEWBUSI_GPRS_200912

select * from  BASS1.G_A_02054_DAY
WHERE ENTERPRISE_BUSI_TYPE='1040'

select tbsp_id,substr(tbsp_name,1,30) tbsp_name,TBSP_UTILIZATION_PERCENT from SYSIBMADM.TBSP_UTILIZATION order by tbsp_id,dbpartitionnum

select * from bass2.dim_service_config where config_id=1000027

SELECT name,npages,fpages FROM SYSIBM.SYSTABLES 
--where tbspace='TBS_CDR_DATA'
where tbspace='TBS_ODS_OTHER '
--and name like '%_BAKNGGJ'
--order by name
order by npages desc,fpages desc

select * from bass2.dim_prod_up_product_item where name like '%来电%'
select * from bass2.ods_prod_up_prod_item_catalog_20110228

select * from bass2.dim_busi_type where busi_type_name like '%来电%'

select distinct busi_type from bass2.Dw_cm_busi_radius_201011


