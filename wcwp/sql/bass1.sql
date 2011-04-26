/**标志位	FLAG	INTEGER		0   运行完成 1   正在运行-1  运行出错 -2  重新运行 
**/--int(replace(char(current date - 1 days),'-',''))select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mbfrom syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspaceand a.tabname = 'G_S_04002_DAY_BAK'   kkkkkkkkkk;;lG_S_04002_DAY	95829.84 635661807G_S_04002_DAY	49864.68 330794180    rename bass1.G_S_04002_DAY to bass1.G_S_04002_DAY_BAKrename BASS1.G_S_04002_DAY to BASS1.G_S_04002_DAY_BAKselect * fromBASS1.G_S_04002_DAYrename BASS1.G_S_04002_DAY to G_S_04002_DAY_BAK create table BASS1.G_S_04002_DAY like BASS1.G_S_04002_DAY_BAK      DISTRIBUTE BY HASH(TIME_ID,                   PRODUCT_NO,                   ROAM_LOCN,                   ROAM_TYPE_ID,                   APNNI,                   START_TIME)        IN TBS_APP_BASS1 INDEX IN TBS_INDEX 	  insert into BASS1.G_S_04002_DAY 	  select * from  BASS1.G_S_04002_DAY_BAK	  where time_id >= 20101101       --RUNSTATS ON table G_S_04002_DAY	with distribution and detailed indexes all  --runstats on table bass1.G_S_04002_DAY with distribution and detailed indexes all            select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mbfrom syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspaceand a.tabname = 'G_S_21003_MONTH'select * from bass1.G_S_04002_DAY_bakselect tabname , card from syscat.tables where tabname in ('G_S_04002_DAY','G_S_04002_DAY_BAK')            AND tabschema = 'BASS1'SELECT COUNT(0) FROM BASS1.G_S_04002_DAY_BAK--告警 alarmselect * from  app.sch_control_alarm where alarmtime >=  timestamp('20110311'||'000000') --and flag = -1and control_code like 'BASS1%'order by alarmtime desc select * from syscat.functions where funcname = 'FN_GET_ALL_DIM'select * from   BASS1.ALL_DIM_LKP select count(0),count(distinct user_id)  from   bass1.g_a_02004_daybass1.fn_get_all_dimBODYCREATE FUNCTION BASS1.FN_GET_ALL_DIM(GID VARCHAR(20),DID VARCHAR(20)) RETURNS VARCHAR(10) DETERMINISTIC NO EXTERNAL ACTION LANGUAGE SQL BEGIN ATOMIC RETURN SELECT BASS1_VALUE FROM BASS1.ALL_DIM_LKP WHERE BASS1_TBID = GID AND XZBAS_VALUE = DID ENDselect * from syscat.tablesselect * from app.sch_control_runlog where control_code = 'TR1_VGOP_D_14303' select * from app.sch_control_runlog  select * from app.sch_control_alarm select min(deal_time) from APP.G_FILE_REPORT where length(deal_time)=14 select substr(deal_time,1,6) , count(0) from app.g_file_report  group by substr(deal_time,1,6)  select tabname from syscat.tables where tabschema = 'BASS1'and tabname like 'G_%'  select * from app.sch_control_map where module = 2    select * from BASS1.G_USER_LST  select * from BASS2.ETL_TASK_LOG where task_id in ( 'I11101')and cycle_id='20110228'select * from APP.G_RUNLOGselect * fromAPP.G_FILE_REPORT_ERRselect * fromAPP.G_REC_REPORT_ERRselect * fromapp.g_file_report where deal_time like '%20110301%'and err_code = '00'select * from  bass1.G_S_22073_DAYselect  * --distinct left(right(filename,16),5),err_code from APP.G_FILE_REPORTwhere filename like '%_201103_%' and err_code='00'select * from app.g_runlog where time_id=20110117and return_flag=1select count(0)select min(deal_time)from app.g_file_reportwhere deal_time > '19990412211032'select  * --distinct left(right(filename,16),5),err_code from APP.G_FILE_REPORTwhere filename like '%_201103_%' and err_code='00'and length(filename)=length('s_13100_201002_03014_01_001.dat')order by deal_time descselect * from bass1.g_rule_check where rule_code = 'C1'and time_id between  20110301 and 20110307select  time_id, case when rule_code='R159_1' then '新增客户数'      when rule_code='R159_2' then '客户到达数'      when rule_code='R159_3' then '上网本客户数'      when rule_code='R159_4' then '离网客户数' end, target1, target2, target3from bass1.g_rule_checkwhere rule_code in ('R159_1','R159_2','R159_3','R159_4')  and time_id=20110301  select count(0) frombass1.G_S_21003_TO_DAY  select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mbfrom syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspaceand a.tabname = 'G_S_21003_TO_DAY'select tbsp_id,substr(tbsp_name,1,30) tbsp_name,TBSP_UTILIZATION_PERCENT from SYSIBMADM.TBSP_UTILIZATION order by tbsp_id,dbpartitionnumselect * from syscat.tables where tabname like '%04005%'and tabschema = 'BASS1'G_S_04005_DAYselect * from  BASS1.G_S_04005_DAY select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mbfrom syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspaceand a.tabname = 'G_S_04005_DAY'    rename BASS1.G_S_04005_DAY to G_S_04005_DAY_BAK    create table BASS1.G_S_04005_DAY like BASS1.G_S_04005_DAY_BAK                  DISTRIBUTE BY HASH(TIME_ID,                   PRODUCT_NO,                   SP_CODE,                   OPPOSITE_NO)                      IN TBS_APP_BASS1 INDEX IN TBS_INDEX NOT LOGGED INITIALLY	  insert into BASS1.G_S_04005_DAY 	  select * from  BASS1.G_S_04005_DAY_BAK	  where time_id >= 20101001select tabname , card from syscat.tables where tabname in ('G_S_04005_DAY','G_S_04005_DAY_BAK')            AND tabschema = 'BASS1'select time_id,count(0) cnt from BASS1.G_S_04005_DAYgroup by  time_id                 select tabname , card from syscat.tables where tabname in ('G_S_04005_DAY','G_S_04005_DAY_BAK')            AND tabschema = 'BASS1'select * from BASS1.G_S_04005_DAY                 drop table BASS1.G_S_04005_DAY_BAKselect a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mbfrom syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspaceand a.tabname = 'G_S_21003_MONTH'select * from  bass1.G_S_21003_MONTH select TIME_ID , count(0) --, count(distinct TIME_ID ) from bass1.G_S_21003_MONTH group by  TIME_ID order by 1 SQL0911Nselect * fromg_s_04003_dayselect count(distinct WLAN_ATTESTATION_CODE)from BASS1.G_S_04005_DAYwhere time_id between 20101201 and 20101231 andselect * from bass2.dim_roam_type			select count(0),count(distinct WLAN_ATTESTATION_CODE)from BASS1.G_S_04003_DAYwhere time_id between 20101201 and 20101231 and ROAM_TYPE_ID in ('500','110')select *  from BASS1.G_S_04003_DAYwhere time_id between 20101201 and 20101231 and ROAM_TYPE_ID in ('500','110')select * from app.sch_control_before where control_code like '%21007%'BASS1_EXP_G_I_03007_MONTH	BASS1_G_I_03007_MONTH.tclBASS1_EXP_G_I_03007_MONTH	BASS1_INT_CHECK_F7_MONTH.tclBASS1_EXP_G_I_03007_MONTH	BASS1_INT_CHECK_R028_MONTH.tclBASS1_EXP_G_I_03007_MONTH	BASS1_INT_CHECK_R029_MONTH.tclselect * from app.sch_control_before where control_code = 'BASS1_INT_CHECK_R029_MONTH.tcl'select * from app.sch_control_task where control_code = '%bak%'select * fromAPP.G_UNIT_INFO select * from APP.G_RUNLOG  order by 1 desc select * from bass1.g_bus_check_all_dayselect * from bass1.g_bus_check_bill_monthselect count(distinct HOTSPOT_AREA_ID)from BASS1.G_S_04003_DAYwhere time_id between 20101201 and 20101231 and ROAM_TYPE_ID in ('500','110')select distinct HOTSPOT_AREA_IDfrom BASS1.G_S_04003_DAYwhere time_id between 20101201 and 20101231 and ROAM_TYPE_ID in ('500','110')select * from   bass2.DW_ENTERPRISE_MEMBER_201102 where enterprise_id = '89103000041929'select * from   bass2.DW_ENTERPRISE_MEMBER_HIS_201102 where enterprise_id = '89103000041929'select * from   bass2.DW_ENTERPRISE_MEMBER_MID_20110323 where enterprise_id = '89103000041929'select * from    bass2.DW_ENTERPRISE_SUB_DSwhere ENTERPRISE_ID = '89103000041929'select count(0),count(distinct enterprise_id) from   bass2.DW_ENTERPRISE_MEMBER_201012select count(0),count(distinct enterprise_id) from   bass2.DW_ENTERPRISE_MEMBER_HIS_201012select count(0),count(distinct enterprise_id) from   bass2.DW_ENTERPRISE_MEMBER_MID_201012select * from bass2.CDR_WLAN_20100304select * from  BASS1.G_S_04003_DAYselect count(distinct HOTSPOT_AREA_ID)from BASS1.G_S_04003_DAYwhere --time_id between 20101201 and 20101231 --and ROAM_TYPE_ID in ('500','110')select tabname,card  from syscat.tables where tabname like '%WLAN%'SELECT * FROM app.sch_control_task where function_desc like '%WLAN%'select * from G_I_02016_MONTHselect * from bass2.Dw_newbusi_wlan_20110302select distinct HOTSPOT_AREA_IDselect *from BASS1.G_S_04003_DAYwhere ROAM_TYPE_ID in ('500','110')select min(time_id) from BASS1.G_S_04003_DAYselect * from BASS1.G_S_04003_DAYwhere product_no not like '1%'select  count(product_no),count(distinct product_no),count(distinct enterprise_id),count(distinct product_no||enterprise_id)from  bass2.DW_ENTERPRISE_MEMBER_MID_201012select sum(int(flowup)+int(flowdown))/1024 from (select a.product_no,a.HOTSPOT_AREA_ID,b.enterprise_id,c.ENTERPRISE_NAME,a.flowup,a.flowdown,a.call_fee,a.info_fee,a.CALL_DURATIONfrom (select * from BASS1.G_S_04003_DAY         where time_id between 20101201 and 20101231         and ROAM_TYPE_ID in ('500','110')	) aleft join  (select distinct enterprise_id,product_no             from bass2.DW_ENTERPRISE_MEMBER_MID_201012            ) b             on a.product_no = b.product_noleft join   bass2.dw_enterprise_msg_201012 c             on b.enterprise_id = c.enterprise_id) t where enterprise_id is not nullselect max(time_id) from g_s_05001_monthselect * from g_s_05001_monthselect time_id,count(0) from g_s_05002_monthgroup by time_idselect tabname from syscat.tables where tabname like '%05002%'select time_id,count(0)from G_S_05001_MONTHgroup by time_id select * from app.sch_control_task where control_code = 'BASS1_EXP_G_S_03005_MONTH'select count(0) from G_A_02052_MONTH where time_id = 201101select * from APP.SCH_CONTROL_ALARM  where flag in(1,-1) and control_code like 'BASS1_INT_CHECK_INDEX_WAVE_DAY.tcl%'and date(alarmtime) = '2011-03-05'CONTENTR161_1 波动性检查新增客户数超出15%R161_7 波动性检查当月累计使用TD网络的手机客户数超出5%R161_9 波动性检查当月累计使用TD网络的数据卡客户数超出5%R161_10 波动性检查当月累计使用TD网络的上网本客户数超出5%R161_13 波动性检查联通移动新增客户数超出8%R161_14 波动性检查电信移动新增客户数超出8%select * from bass1.g_rule_check where rule_code in ('R161_1') order by time_id descselect * from g_file_reportselect * from app.g_runlog select time_id,count(0) from bass1.g_user_lstgroup by time_id select * from syscat.tables where tabname like '%22012%'select * from G_S_22012_DAYselect count(0) from APP.SMS_SEND_INFOselect * from   APP.SMS_SEND_INFOorder by message_id desc select send_time,mobile_num,message_content from   APP.SMS_SEND_INFOwhere send_time is not nulland mobile_num = '15913269062'order by send_time desc insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,MOBILE_NUM) select 'test','15913269062'from bass2.dualselect * from bass1.mon_user_mobileselect * from    bass1.T_GS05001M select * from      bass1.T_GS05001M  select * from bass1.T_GS05001M select * from   bass1.g_s_05001_month where time_id = 201012exceptselect * from bass1.T_GS05001MCREATE TABLE "BASS1   "."T_GS05002M"  (                  "TIME_ID" INTEGER ,                   "BILL_MONTH" CHAR(6) ,                   "SELF_CMCC_CODE" CHAR(5) ,                   "SELF_SVC_BRND_ID" CHAR(1) ,                   "OTHER_CMCC_CODE" CHAR(6) ,                   "OTHER_SVC_BRND_ID" CHAR(6) ,                   "IN_COUNT" CHAR(12) ,                   "OUT_COUNT" CHAR(12) ,                   "STLMNT_FEE" CHAR(12) ,                   "PAY_STLMNT_FEE" CHAR(12) )                    DISTRIBUTE BY HASH("TIME_ID")                      IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  select * from   g_s_05002_month where time_id = 201012exceptselect * from T_GS05002Mselect count(0) from app.sch_control_alarmwhere control_code like 'BASS1%'select * from app.sch_control_task where upper(cmd_line) like '%BASS1%SH%'SCH_CONTROL_TASKselect CONTROL_CODE,'bass1_lst.sh',1,'${ALARM_CONTENT}',current timestamp,-1 from app.SCH_CONTROL_TASK where upper(replace(CMD_LINE,' ',''))=upper('bass1_lst.sh') select * from app.sch_control_groupinfo  dmk	BI-M01-01-MARKDB	2011-03-08 16:53:42.240912olap_load	BI-M02-01-OLAP	2011-03-08 16:53:41.055375app	BI-M02-01-OLAP	2011-03-08 16:53:42.407528select mobile_num,count(0) from app.sms_send_info group by mobile_num select * from SYS_TASK_RUNLOGselect * from syscat.tables where tabname like '%RUNLOG%'select * from SYS_TASK_RUNLOGselect a.custstatus_id,coalesce(bass1.fn_get_all_dim('BASS_STD1_0028',char(a.custstatus_id)),'20') as custstatus_id from bass2.dwd_cust_msg_20110310 aselect * from select * from BASS1.ALL_DIM_LKP select time_id,count(0)from G_A_02004_DAYwhere time_id >= 20110101group by time_id order by 1 desc select * from G_A_02004_DAYselect count(distinct user_id) from G_A_02004_DAY with ur-- =2088474select count(distinct user_id) from G_A_02008_DAY with ur2088474select * from   product_xhxselect * from select * from bass1.G_REPORT_CHECKSELECT sum(bigint(BILLING_CAT_OF_WAP_INFO))                  FROM BASS1.G_S_04006_DAY                 WHERE TIME_ID/100=2011021118057                 SELECT sum(bigint(INFO_FEE))                  FROM BASS1.G_S_04006_DAY                 WHERE TIME_ID/100=2011025621188select * from   app.sch_control_task where control_code like '%CHECK%MONTH%'select * from   BASS1.G_S_04006_DAYselect * from    bass2.tmp_zcgselect * from app.SYS_TASK_RUNLOGselect * from bass2.dw_product_ins_off_ins_prod_201102where product_instance_id='89157333132742'  and offer_id=111099001926                select * from   bass1.g_s_22012_day                SELECT * FROM BASS2.ETL_TASK_LOG                select * from   app.sch_control_task where control_code like '%00000%'                select * from   bass1.G_BUS_CHECK_BILL_MONTHselect * from   app.sch_control_taskselect * from   app.sch_control_task where function_desc like '%作废%'and control_code like 'BASS1%'select * from   app.sch_control_task where cmd_type = 1select cmd_type , count(0) , count(distinct cmd_type ) from app.sch_control_task group by  cmd_type order by 1 select * from   APP.SCH_PERSON_PHONEselect userid , count(0) , count(distinct userid ) from APP.SCH_PERSON_PHONE group by  userid order by 1 select * from   app.sch_control_groupinfoselect * from   app.sch_control_mogrpinfoBASS2.ETL_SEND_MESSAGEselect * from BASS2.ETL_SEND_MESSAGE where phone_id = '13989094821'select       c.MO_GROUP_DESC,--模块名count( case when date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 then a.control_code end ) not_run_cnt,--未完成数count( case when   b.flag=1  then a.control_code end ) running_cnt,--执行数count( case when   b.flag=-1 then a.control_code end ) run_err_cnt, --执行出错count( case when date(current timestamp)=date(b.begintime)    and  b.flag=0 then a.control_code end ) done_cnt	--完成数from APP.SCH_CONTROL_TASK a,      APP.SCH_CONTROL_RUNLOG  b,      APP.SCH_CONTROL_MOGRPINFO  cwhere  a.CONTROL_CODE=b.CONTROL_CODE     and a.MO_GROUP_CODE = c.MO_GROUP_CODE    and a.deal_time in (2)group by c.MO_GROUP_DESC,c.sort_idorder by c.sort_idwith ur	select deal_time , count(0) , count(distinct deal_time ) from app.sch_control_task group by  deal_time order by 1 select deal_time , count(0) --,  count(distinct deal_time ) from app.sch_control_task group by  deal_time order by 1 select * from   app.sch_control_task  where deal_time = 3select a.*,b.*from APP.SCH_CONTROL_TASK a,      APP.SCH_CONTROL_RUNLOG  b,      APP.SCH_CONTROL_MOGRPINFO  cwhere  a.CONTROL_CODE=b.CONTROL_CODE     and a.MO_GROUP_CODE = c.MO_GROUP_CODE    and a.deal_time in (2)   and (date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2)   and c.MO_GROUP_DESC = '一经程序'order by c.sort_idwith ur	select       a.MO_GROUP_CODE,c.MO_GROUP_DESC,a.CONTROL_CODE,a.CMD_LINE,a.FUNCTION_DESC,case  when date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 then '未完成'       when   b.flag=1   then '执行中'      when   b.flag=-1  then '执行出错'      when   date(current timestamp)=date(b.begintime) and  b.flag=0 then '完成'      else  '未知'end,b.*from  APP.SCH_CONTROL_TASK a,      APP.SCH_CONTROL_RUNLOG  b,      APP.SCH_CONTROL_MOGRPINFO  cwhere  a.CONTROL_CODE=b.CONTROL_CODE     and a.MO_GROUP_CODE = c.MO_GROUP_CODE    and a.deal_time in (1)   and a.MO_GROUP_CODE = 'BASS1'order by c.sort_idwith urselect count(0),count(distinct ENTERPRISE_ID) select t2.*from (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from G_A_02055_DAY where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340') t ) t2 where rn = 1 select count(0)from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from g_a_02059_day awhere TIME_ID < 20110301and MANAGE_MODE = '2'and STATUS_ID ='1'and ENTERPRISE_BUSI_TYPE ='1340'and length(trim(user_id)) = 14) t) t2where rn = 1 select * from g_a_02059_day awhere TIME_ID < 20110301and MANAGE_MODE = '2'and STATUS_ID ='1'and ENTERPRISE_BUSI_TYPE ='1340'select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'select t2.*from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'and length(trim(user_id)) = 14) t) t2where rn = 1 select * from g_a_02054_dayselect count(0),count(distinct ENTERPRISE_ID) select count(0)from (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220') t ) t2 where rn = 1 select * from   g_a_02059_dayselect tabname from   syscat.tables where tabname like '%ENTERPRISE_MEMBER_MID%'select '20110313',a.ENTERPRISE_ID,a.USER_ID,'1340','2','20110313','1' from   bass2.DW_ENTERPRISE_MEMBER_MID_20110313select * from   bass2.DW_ENTERPRISE_msg_20110313where ENTERPRISE_ID in (select distinct ENTERPRISE_IDfrom (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from G_A_02055_DAY where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340') t ) t2 where rn = 1 )select * from   bass2.DW_ENTERPRISE_msg_20110313where ENTERPRISE_ID in (select distinct ENTERPRISE_IDfrom (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220') t ) t2 where rn = 1 select * from   bass2.DW_ENTERPRISE_msg_20110313where ENTERPRISE_ID in (select  distinct ENTERPRISE_IDfrom (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220') t ) t2 where rn = 1 )select * from   app.sch_control_task where control_code like '%02031%'--1040	无线商务电话	select * from    g_a_02059_day  where ENTERPRISE_BUSI_TYPE = '1040'--02054	集团客户业务订购关系select * from   g_a_02054_day where  ENTERPRISE_BUSI_TYPE = '1040'select * from  bass2.dw_enterprise_msg_201102 where enterprise_id = '89100000000705'select * from   select * from bass1.ALL_DIM_LKP_160 c  where bass1_tbid='BASS_STD1_0108'and C.BASS1_VALUE  in ('1230','1241','1249','1220','1320','1040')select * from bass2.dim_enterprise_productwhere service_name like  '%话%'select * from app.sch_control_beforewhere control_code = 'BASS1_G_A_02059_DAY.tcl'select * from app.sch_control_beforewhere control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl'select * from app.sch_control_beforewhere control_code in (select BEFORE_CONTROL_CODE from app.sch_control_beforewhere control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl')select * from app.sch_control_beforewhere control_codein (select BEFORE_CONTROL_CODE from app.sch_control_beforewhere control_code in (select BEFORE_CONTROL_CODE from app.sch_control_beforewhere control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl'))select * from app.sch_control_beforewhere control_codein(select BEFORE_CONTROL_CODE from app.sch_control_beforewhere control_codein (select BEFORE_CONTROL_CODE from app.sch_control_beforewhere control_code in (select BEFORE_CONTROL_CODE from app.sch_control_beforewhere control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl')))select * from app.sch_control_beforewhere control_code like '%enterprise%'select * from app.sch_control_beforewhere before_control_code like '%BASS2_Dw_product_ds.tcl%'BASS1_G_A_02059_DAY.tcl	BASS2_Dw_enterprise_extsub_rela_ds.tclselect * from app.sch_control_taskwhere control_code like '%w_product%'insert into app.sch_control_beforevalues('BASS1_G_A_02059_DAY.tcl','BASS2_Dw_product_regsp_ds.tcl')insert into app.sch_control_beforevalues('BASS1_G_A_02059_DAY.tcl','BASS1_G_A_02054_DAY.tcl')insert into app.sch_control_beforevalues('BASS1_G_A_02059_DAY.tcl','BASS2_Dw_product_ds.tcl')insert into app.sch_control_beforevalues('BASS1_G_A_02059_DAY.tcl','BASS2_Dw_enterprise_member_mid_ds.tcl')insert into app.sch_control_beforevalues('BASS1_G_A_02059_DAY.tcl','BASS2_Dw_enterprise_msg_ds.tcl')commitselect * from   app.sch_control_task where upper(control_code) like '%02054%'BASS2_Dw_enterprise_member_mid_ds.tcldw_enterprise_member_mid_yyyymmddBEFORE_CONTROL_CODEBASS2_Dw_enterprise_sub_ds.tclBASS2_Dw_enterprise_membersub_ds.tclBASS2_Dw_product_ds.tclselect * from app.sch_control_beforewhere control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl'select * from   app.sch_control_task where upper(control_code) like '%DW_PRODUCT_REGSP_DS%'DW_PRODUCT_REGSP_DSBASS1_G_A_02059_DAY.tcl	BASS2_Dw_enterprise_extsub_rela_ds.tclselect * from G_A_02059_DAY_0315modifyexceptselect * from   G_A_02059_DAYselect '20110314'											 ,a.enterprise_id											 ,b.user_id											 ,'1040'											 ,'3'											 ,'20110314'											 ,a.status_id								from 	bass1.g_a_02054_day a,								      bass2.dw_enterprise_member_mid_20110314 b,								      bass2.dw_enterprise_msg_20110314 c,								      bass2.dw_product_20110314 d								where a.enterprise_id=b.enterprise_id								  and a.enterprise_id=c.enterprise_id								  and b.user_id=d.user_id								  and d.usertype_id in (1,2,3,6)								  and a.enterprise_busi_type='1040'								  and a.time_id=20110314								  and a.status_id='1'select 20110314											 ,a.enterprise_id											 ,b.user_id											 ,'1040'											 ,'3'											 ,'20110314'											 ,a.status_id								from 	bass1.g_a_02054_day a,								      (select distinct enterprise_id,user_id 								        from BASS1.G_A_02059_DAY 								       where enterprise_busi_type='1040' 								         and time_id<20110314) b								where a.enterprise_id=b.enterprise_id								  and a.time_id=20110314								  and a.enterprise_busi_type='1040'								  and a.status_id='2'                                  select A.*,char(a.RUNTIME/60)||'min',char(a.RUNTIME/60/60)||'hr' from   app.sch_control_runlog Awhere control_code like 'BASS1%'and a.RUNTIME/60 > 10ORDER BY RUNTIME DESC select * from   g_s_02059_day                                                                    select * from g_a_02059_daywhere time_id = 20110315and ENTERPRISE_BUSI_TYPE = '1040'select * from   g_a_01004_dayselect * from   BASS2.DW_ENTERPRISE_ACCOUNT_20110315select * from   bass2.dwd_cust_msg_20110315select a.CUSTTYPE_ID, a.* from   bass2.dwd_cust_msg_20110315 aselect * from   app.sch_control_runlog where control_code like '%01004%'                                  select count(0)from ( select distinct a.enterprise_id from   (select time_id,enterprise_id,cust_statu_typ_id from bass1.G_A_01004_DAY where time_id <= 20110301 ) a,   (select enterprise_id,max(time_id) as time_id  from bass1.G_A_01004_DAY                                                  where time_id<=20110301                                              group by enterprise_id) bwhere a.time_id=b.time_id and a.enterprise_id=b.enterprise_id and a.cust_statu_typ_id = '20') t10:04 9075select count(0)from (select distinct t.enterprise_idfrom (select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn from bass1.G_A_01004_DAY where time_id <= 20110301 ) t where t.rn = 1 and  cust_statu_typ_id = '20') tt 10537select enterprise_id,max(time_id) as time_id  from bass1.G_A_01004_DAY                                                  where time_id<=20110301                                              group by enterprise_id                                              select  from   G_A_02004_DAYselect count(0) from   G_A_02062_DAY where char(time_id) like '201102%'select count(0),count(distinct user_id) from   G_A_02062_DAY where time_id < 20110301 and time_id >= 2011020102004select time_id,count(0) from    G_A_02051_DAYgroup by time_idorder by 1 desc select count(0),count(distinct user_id ) from    G_A_02004_DAYselect a.brand_id, count(0),count(distinct a.user_id) from  (select user_id,BRAND_ID from G_A_02004_DAY  where SIM_CODE = '1' and USERTYPE_ID = '1') a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where a.user_id = b.user_id group by a.brand_id2	51902	6438	4663(select user_id,BRAND_ID ,row_number()over(partition by )from G_A_02004_DAY  where SIM_CODE = '1' and USERTYPE_ID = '1') a 46226438time_id <20110301select * from   bass1.G_A_02062_DAYchar(time_id) like '201102%'select SIM_CODE ,BRAND_ID, count(0) ,  count(distinct USER_ID ) from bass1.G_A_02004_DAY group by  SIM_CODE ,BRAND_IDorder by 1 declare global temporary table session.int_check_user_status    (   user_id        CHARACTER(15),   product_no     CHARACTER(15),   test_flag      CHARACTER(1),   sim_code       CHARACTER(15),   usertype_id    CHARACTER(4),   create_date    CHARACTER(15),   brand_id       CHARACTER(4),   time_id        int    )                            partitioning key            (   user_id     ) using hashing           with replace on commit preserve rows not logged in tbs_user_temp--抓取用户资料入表insert into session.int_check_user_status (     user_id        ,product_no     ,test_flag      ,sim_code       ,usertype_id      ,create_date    ,brand_id    ,time_id )select e.user_id    ,e.product_no      ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag    ,e.sim_code    ,f.usertype_id      ,e.create_date      ,e.brand_id    ,f.time_id       from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id                ,row_number() over(partition by user_id order by time_id desc ) row_id   from bass1.g_a_02004_daywhere time_id<=20110331 ) einner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id              from bass1.g_a_02008_day           where time_id<=20110331 ) f on f.user_id=e.user_idwhere e.row_id=1 and f.row_id=1and f.usertype_id NOT IN ('2010','2020','2030','9000')commitselect a.sim_code,a.brand_id, count(0),count(distinct a.user_id) from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0'group by a.sim_code,a.brand_idorder by 1,246222   	4622	4622西藏	7610	2658	193	7	2858		4622		4622	7480	130select a.sim_code,a.brand_id, count(0),count(distinct a.user_id) from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0'group by a.sim_code,a.brand_idorder by 1,2select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1'  select count(0),count(distinct user_id ) from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1'  7610  select distinct user_id from  G_A_02062_DAY  where time_id <20110301 and STATUS_ID = '1' except select distinct a.user_id from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0'  select * from  G_A_02062_DAY where user_id = '89460000193920' select * from  session.int_check_user_status where user_id = '89460000193920'   select * from  session.int_check_user_status where user_id  in (select distinct user_id from  G_A_02062_DAY  where time_id <20110301 and STATUS_ID = '1' except select distinct a.user_id from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0')   select * from  G_A_02062_DAY where user_id = '89657334067011' select OP_TIME,count(0) from   bass2.DW_ENTERPRISE_SUB_ds group by  OP_TIME  select count(0) from  bass2.DW_ENTERPRISE_SUB_ds bass2.DW_ENTERPRISE_SUB_201011 select * from  bass2.DW_ENTERPRISE_SUB_201011where user_id = '89657334067011' select count(0) from  bass2.DW_ENTERPRISE_SUB_201102 select * from   bass2.dw_product_20110314where user_id = '89657334067011'1363896839589657334067011	89603001340010	89601002853991	100	6	0	0	1	0	0	[NULL]	13638968395	460008961120700	[NULL]	[NULL]	896	896	1059	10590005	96011006	4	[NULL]	300001911090	0	279	10	1	1	10	101	89610012	5	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	2010-02-24 	2010-05-25 	2010-11-29 	2010-11-29 	0	0	0	0	0.00	0.00	0.00	0.00	0	0	0	0	0.00	0.00	0.00	0.00	0.00	0.00 89657334067011 	13638968395    	0	0              	2020	20100224       	1   	2010112989657334006049 	13638960960    	0	0              	2020	20100118       	1   	20101130 select * from  session.int_check_user_status where user_id  in   ('89157333603327','89257332979496','89257332980422','89457332326899','89460000193947','89560000363718','89560000363941','89657334006210','89157334021842','89257332979460','89257332980702','89257333374671','89457333600524','89460000193798','89460000339034','89560000363916','89560000363965','89657333802412','89157332844445','89257332979510','89360000266593','89457332319392','89457333729869','89460000193999','89560000363735','89560000363905','89657333043867','89657333685357','89657333695270','89157332768972','89157333603386','89257332980520','89257332980594','89360000266440','89360000266468','89457333386157','89460000193789','89460000270137','89560000363897','89657333689064','89157333346941','89157333942045','89257332979480','89360000266538','89360000266610','89360000266686','89460000193802','89560000363758','89657333802374','89657334006049','89657334067011','89360000266456','89360000266532','89457332240376','89457333035055','89460000193749','89657333688890','89701170013122','89457333038292','89460000193714','89560000363747','89560000363894','89657333685472','89157332834451','89157333603404','89360000266628','89460000193679','89460000193777','89460000193932','89460000221143','89657333695116','89157332768979','89157332993008','89157332994577','89357333334596','89457332341379','89460000193825','89657333693001','89157334002261','89157334021848','89360000266437','89360000266524','89457332341392','89457333038299','89460000194023','89560000363859','89657333043881','89657334037819','89157332826412','89360000266462','89360000266559','89460000193616','89460000193810','89657333562860','89757334249044','89157332993069','89157333346957','89157333942188','89460000193858','89657333696722','89657333832637','89157332895961','89157333603323','89157333942125','89360000266273','89457333338562','89560000363911','89657333295749','89657333736478','89157333346922','89157333872113','89360000266286','89360000266674','89360000266703','89460000193774','89460000193920','89460000193937','89460000193954','89560000363769','89560000363789','89657334006083','89757334252690','89157333942117','89157334021855','89357333334573','89360000266167','89360000266306','89360000266516','89560000363889','89657333800018')order by 3 select count(0) from  bass2.DW_ENTERPRISE_SUB_dsdeclare global temporary table session.int_check_user_status    (   user_id        CHARACTER(15),   product_no     CHARACTER(15),   test_flag      CHARACTER(1),   sim_code       CHARACTER(15),   usertype_id    CHARACTER(4),   create_date    CHARACTER(15),   brand_id       CHARACTER(4),   time_id        int    )                            partitioning key            (   user_id     ) using hashing           with replace on commit preserve rows not logged in tbs_user_temp--抓取用户资料入表insert into session.int_check_user_status (     user_id        ,product_no     ,test_flag      ,sim_code       ,usertype_id      ,create_date    ,brand_id    ,time_id )select e.user_id    ,e.product_no      ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag    ,e.sim_code    ,f.usertype_id      ,e.create_date      ,e.brand_id    ,f.time_id       from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id                ,row_number() over(partition by user_id order by time_id desc ) row_id   from bass1.g_a_02004_daywhere time_id<=20110231 ) einner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id              from bass1.g_a_02008_day           where time_id<=20110231 ) f on f.user_id=e.user_idwhere e.row_id=1 and f.row_id=1commitselect a.sim_code,a.brand_id, count(distinct a.user_id) user_cnt from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0'group by a.sim_code,a.brand_idorder by 1,2select case when a.sim_code = '1' then '数据SIM卡用户' else '非数据SIM卡用户' end ,a.brand_id, count(distinct a.user_id) user_cnt from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0'group by case when a.sim_code = '1' then '数据SIM卡用户' else '非数据SIM卡用户' end ,a.brand_idorder by 1,2 select distinct user_id from  G_A_02062_DAY  where time_id <20110301 and STATUS_ID = '1' except select distinct a.user_id from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0'select count(0)from (        select distinct user_id from  G_A_02062_DAY  where time_id <20110301 and STATUS_ID = '1' except select distinct a.user_id from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and a.usertype_id NOT IN ('2010','2020','2030','9000')        and a.test_flag='0'     ) t         select test_flag,usertype_id,a.user_id from  session.int_check_user_status a where user_id  in (select distinct user_id from  G_A_02062_DAY  where time_id <20110301 and STATUS_ID = '1' except select distinct a.user_id from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and a.usertype_id NOT IN ('2010','2020','2030','9000')        and a.test_flag='0'  ) order by 1,2结果:TEST_FLAG	USERTYPE_ID	30	2010	200	2020	331	1010	671	2020	10select * from bass2.dw_enterprise_membersub_ds awhere a.USER_ID in ('89157333942045','89157333603327','89257332980422','89157332993008','89157333942117','89257332980702','89460000339034','89257333374671','89157332993069','89157333942188','89157332895961','89157333942125','89157333603323','89157334002261','89157333872113','89157333603386','89257332980594','89257332980520','89157332834451','89157333603404','89460000221143','89657333695116','89157332768972','89657333689064','89460000270137','89457333386157','89657334006083','89757334252690','89657333688890','89657334037819','89457333338562','89657333736478','89657333295749','89657333696722','89657333832637','89157332826412','89757334249044','89657333562860','89457333600524','89657333802412','89657333800018','89157332768979','89657333693001','89157332994577','89157332844445','89657333695270','89657333685357','89457333729869','89657334006210','89657333685472','89657333802374','89657334067011','89657334006049')select *from  ( select a.*,row_number()over(partition by user_id order by time_id desc) rn  from  BASS1.G_A_02062_DAY a where a.USER_ID in ('89157333942045','89157333603327','89257332980422','89157332993008','89157333942117','89257332980702','89460000339034','89257333374671','89157332993069','89157333942188','89157332895961','89157333942125','89157333603323','89157334002261','89157333872113','89157333603386','89257332980594','89257332980520','89157332834451','89157333603404','89460000221143','89657333695116','89157332768972','89657333689064','89460000270137','89457333386157','89657334006083','89757334252690','89657333688890','89657334037819','89457333338562','89657333736478','89657333295749','89657333696722','89657333832637','89157332826412','89757334249044','89657333562860','89457333600524','89657333802412','89657333800018','89157332768979','89657333693001','89157332994577','89157332844445','89657333695270','89657333685357','89457333729869','89657334006210','89657333685472','89657333802374','89657334067011','89657334006049')) t where rn = 1 20110212	89401560000169      	89460000270137      	1249	3	13908940254    	4 	1	cmm.hn                                                      	20110212	1	1select * from   g_a_02008_daywhere user_id = '89460000270137'20110226	89460000270137      	202020101123	89460000270137      	102220110212	89401560000169      	89460000270137      	1249	3	13908940254    	4 	1	cmm.hn                                                      	20110212	1	12011012789157333942188      select * from   g_a_02008_daywhere user_id = '89157333942188'20110201	89157333942188      	2010201102122011012720110127201101272011012720101231select * from    bass2.dwd_enterprise_sub_20101231 awhere a.SUB_ID in ('89157333942045','89157333603327','89257332980422','89157332993008','89157333942117','89257332980702','89460000339034','89257333374671','89157332993069','89157333942188','89157332895961','89157333942125','89157333603323','89157334002261','89157333872113','89157333603386','89257332980594','89257332980520','89157332834451','89157333603404','89460000221143','89657333695116','89157332768972','89657333689064','89460000270137','89457333386157','89657334006083','89757334252690','89657333688890','89657334037819','89457333338562','89657333736478','89657333295749','89657333696722','89657333832637','89157332826412','89757334249044','89657333562860','89457333600524','89657333802412','89657333800018','89157332768979','89657333693001','89157332994577','89157332844445','89657333695270','89657333685357','89457333729869','89657334006210','89657333685472','89657333802374','89657334067011','89657334006049')bass2.ods_product_ins_prod_20110315select count(0)from (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220') t ) t2 where rn = 1 select count(0)from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'and length(trim(user_id)) = 14) t) t2where rn = 1 select * from   bass2.dw_enterprise_msg_his_20110316where enterprise_id in ('89108911013886'      ,'89100000000645'     )select         TIME_ID        ,ENTERPRISE_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,ORDER_DATE        ,2 STATUS_IDfrom          (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220') t ) t2 where rn = 1 select *from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id >=20110301 and ENTERPRISE_BUSI_TYPE = '1220'select count(0) from G_A_02054_DAYselect * from BASS1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108'select count(0)from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'and length(trim(user_id)) = 14) t) t2where rn = 1 select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'CREATE TABLE BASS1.G_A_02054_DAY_20110317BAK (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (ENTERPRISE_ID,    TIME_ID   ) USING HASHINGinsert into BASS1.G_A_02054_DAY_20110317BAKselect * from G_A_02054_DAYselect count(0) from    G_A_02054_DAY_20110317BAK54697select count(0) from    G_A_02054_DAYCREATE TABLE BASS1.G_A_02054_DAY_0317_1220repair (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (ENTERPRISE_ID,    TIME_ID   ) USING HASHING   ALTER TABLE BASS1.G_A_02054_DAY_0317_1220repair  LOCKSIZE ROW  APPEND OFF  NOT VOLATILE  insert into BASS1.G_A_02054_DAY_0317_1220repairselect         20110317        ,ENTERPRISE_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,ORDER_DATE        ,'2' STATUS_IDfrom g_a_02054_day where STATUS_ID ='1' 			and MANAGE_MODE = '2'			and time_id <20110301 			and ENTERPRISE_BUSI_TYPE = '1220'            CREATE TABLE BASS1.G_A_02061_DAY_20110317BAK (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  USER_ID               CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  EXPIRE_DATE           CHARACTER(8),  PAY_TYPE              CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (USER_ID,    TIME_ID   ) USING HASHINGALTER TABLE BASS1.G_A_02061_DAY_20110317BAK  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEinsert into   bass1.G_A_02061_DAY_20110317BAKselect * from  bass1.G_A_02061_DAYselect count(0) from     bass1.G_A_02061_DAYselect count(0) from    bass1.G_A_02061_DAY_20110317BAKCREATE TABLE BASS1.G_A_02061_DAY_0317repair (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  USER_ID               CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  EXPIRE_DATE           CHARACTER(8),  PAY_TYPE              CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (USER_ID,    TIME_ID   ) USING HASHINGALTER TABLE BASS1.G_A_02061_DAY_0317repair  LOCKSIZE ROW  APPEND OFF  NOT VOLATILE  insert into   bass1.G_A_02061_DAY_0317repairselect * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'delete from   bass1.G_A_02061_DAY_0317repairinsert into   bass1.G_A_02061_DAY_0317repairselect          20110317        ,ENTERPRISE_ID        ,USER_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,'20110317' EXPIRE_DATE        ,PAY_TYPE        ,ORDER_DATE        ,'2' STATUS_IDfrom G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'select * from    bass1.G_A_02061_DAY_0317repair	select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=20110316    571304    
	select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=20110315240017select * from   bass1.G_RULE_CHECKwhere time_id = 2011031620110316	R161_15	571304.00000	240017.00000	1.38026	0.00000select * from   bass1.g_s_22202_day where time_id = 2011031620110316	20110316	876691      	571304      	217         delete from BASS1.G_A_02054_DAY_0317_1220repairinsert into BASS1.G_A_02054_DAY_0317_1220repairselect         20110317        ,ENTERPRISE_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,ORDER_DATE        ,'2' STATUS_IDfrom (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220') t ) t2 where rn = 1 delete from   bass1.G_A_02061_DAY_0317repairinsert into   bass1.G_A_02061_DAY_0317repairselect          20110317        ,ENTERPRISE_ID        ,USER_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,'20110317' EXPIRE_DATE        ,PAY_TYPE        ,ORDER_DATE        ,'2' STATUS_IDfrom (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'and length(trim(user_id)) = 14) t) t2where rn = 1 select count(0) from   bass2.dw_product_td_20110315select *    from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and length(trim(user_id)) = 14) t) t2 where rn = 1 and STATUS_ID ='1'select * from   bass1.G_A_02061_DAY_0317repairdelete from   bass1.G_A_02061_DAY_0317repairinsert into   bass1.G_A_02061_DAY_0317repairselect          20110317        ,ENTERPRISE_ID        ,USER_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,'20110317' EXPIRE_DATE        ,PAY_TYPE        ,ORDER_DATE        ,'2' STATUS_IDfrom (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and length(trim(user_id)) = 14) t) t2 where rn = 1 and STATUS_ID ='1'select         20110317        ,ENTERPRISE_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,ORDER_DATE        ,'2' STATUS_IDfrom (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from g_a_02054_day where  MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220') t ) t2 where rn = 1 and STATUS_ID ='1' delete from BASS1.G_A_02054_DAY_0317_1220repairinsert into BASS1.G_A_02054_DAY_0317_1220repairselect         20110317        ,ENTERPRISE_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,ORDER_DATE        ,'2' STATUS_IDfrom (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from g_a_02054_day where  MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220') t ) t2 where rn = 1 and STATUS_ID ='1' select * from    BASS1.G_A_02061_DAYinsert into BASS1.G_A_02061_DAYselect * from bass1.G_A_02061_DAY_0317repairinsert into BASS1.G_A_02054_DAY 
		select * from bass1.G_A_02054_DAY_0317_1220repair        select * from   bass1.G_A_02054_DAY_0317_1220repairexceptselect * from   BASS1.G_A_02054_DAY select test_mark , count(0) --,  count(distinct test_mark ) from bass2.dw_product_20110315 group by  test_mark order by 1 select count(0) from  bass2.dw_product_20110315 where   test_mark = 0                declare global temporary table session.int_check_user_status    (   user_id        CHARACTER(15),   product_no     CHARACTER(15),   test_flag      CHARACTER(1),   sim_code       CHARACTER(15),   usertype_id    CHARACTER(4),   create_date    CHARACTER(15),   brand_id       CHARACTER(4),   time_id        int    )                            partitioning key            (   user_id     ) using hashing           with replace on commit preserve rows not logged in tbs_user_temp--抓取用户资料入表insert into session.int_check_user_status (     user_id        ,product_no     ,test_flag      ,sim_code       ,usertype_id      ,create_date    ,brand_id    ,time_id )select e.user_id    ,e.product_no      ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag    ,e.sim_code    ,f.usertype_id      ,e.create_date      ,e.brand_id    ,f.time_id       from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id                ,row_number() over(partition by user_id order by time_id desc ) row_id   from bass1.g_a_02004_daywhere time_id<=20110316 ) einner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id              from bass1.g_a_02008_day           where time_id<=20110316 ) f on f.user_id=e.user_idwhere e.row_id=1 and f.row_id=1select case when a.sim_code = '1' then '数据SIM卡用户' else '非数据SIM卡用户' end ,a.brand_id, count(distinct a.user_id) user_cnt from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <=20110316 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0'group by case when a.sim_code = '1' then '数据SIM卡用户' else '非数据SIM卡用户' end ,a.brand_idorder by 1,2select count(0)from (        select distinct user_id from  G_A_02062_DAY  where time_id <=20110316 and STATUS_ID = '1' except select distinct a.user_id from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <=20110316 and STATUS_ID = '1') b where   a.user_id = b.user_id         and a.usertype_id NOT IN ('2010','2020','2030','9000')        and a.test_flag='0'     ) t    select          TIME_ID        ,ENTERPRISE_ID        ,USER_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,PRODUCT_NO        ,INDUSTRY_ID        ,GPRS_TYPE        ,DATA_SOURCE        ,CREATE_DATE        ,STATUS_IDfrom (select a.*,row_number()over(partition by user_id order by time_id desc ) rn  from   G_A_02062_DAY a) t where rn = 1 and STATUS_ID = '1' except select distinct a.user_id from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <=20110316 and STATUS_ID = '1') b where   a.user_id = b.user_id         and a.usertype_id NOT IN ('2010','2020','2030','9000')        and a.test_flag='0'     select * from   G_A_02062_DAYselect          20110317 TIME_ID        ,ENTERPRISE_ID        ,USER_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,PRODUCT_NO        ,INDUSTRY_ID        ,GPRS_TYPE        ,DATA_SOURCE        ,CREATE_DATE        ,'2' STATUS_IDfrom (select a.*,row_number()over(partition by user_id order by time_id desc ) rn  from   G_A_02062_DAY a) t where rn = 1 and STATUS_ID = '1'and user_id in (select USER_IDfrom (select a.*,row_number()over(partition by user_id order by time_id desc ) rn  from   G_A_02062_DAY a) t where rn = 1 and STATUS_ID = '1'exceptselect user_id from session.int_check_user_status a        where a.usertype_id NOT IN ('2010','2020','2030','9000')        and a.test_flag='0')        CREATE TABLE BASS1.G_A_02062_DAY_20110317bak (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  USER_ID               CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  PRODUCT_NO            CHARACTER(15),  INDUSTRY_ID           CHARACTER(2),  GPRS_TYPE             CHARACTER(1),  DATA_SOURCE           CHARACTER(60),  CREATE_DATE           CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (USER_ID,    TIME_ID   ) USING HASHINGALTER TABLE BASS1.G_A_02062_DAY_20110317bak  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEinsert into BASS1.G_A_02062_DAY_20110317bakselect * from  BASS1.G_A_02062_DAY       74990 row(s) affected.select count(0) from  BASS1.G_A_02062_DAY     CREATE TABLE BASS1.G_A_02062_DAY_20110317repair (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  USER_ID               CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  PRODUCT_NO            CHARACTER(15),  INDUSTRY_ID           CHARACTER(2),  GPRS_TYPE             CHARACTER(1),  DATA_SOURCE           CHARACTER(60),  CREATE_DATE           CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (USER_ID,    TIME_ID   ) USING HASHINGALTER TABLE BASS1.G_A_02062_DAY_20110317repair  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEdelete from  BASS1.G_A_02062_DAY_20110317repair insert into  BASS1.G_A_02062_DAY_20110317repairselect          20110317 TIME_ID        ,ENTERPRISE_ID        ,USER_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,PRODUCT_NO        ,INDUSTRY_ID        ,GPRS_TYPE        ,DATA_SOURCE        ,CREATE_DATE        ,'2' STATUS_IDfrom (select a.*,row_number()over(partition by user_id order by time_id desc ) rn  from   G_A_02062_DAY a) t where rn = 1 and STATUS_ID = '1'and user_id in (select USER_IDfrom (select a.*,row_number()over(partition by user_id order by time_id desc ) rn  from   G_A_02062_DAY a) t where rn = 1 and STATUS_ID = '1'exceptselect user_id from session.int_check_user_status a        where a.usertype_id NOT IN ('2010','2020','2030','9000')        and a.test_flag='0')        delete from  BASS1.G_A_02062_DAY_20110317repair insert into  BASS1.G_A_02062_DAY_20110317repairselect          20110317 TIME_ID        ,ENTERPRISE_ID        ,USER_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,PRODUCT_NO        ,INDUSTRY_ID        ,GPRS_TYPE        ,DATA_SOURCE        ,CREATE_DATE        ,'2' STATUS_IDfrom (select a.*,row_number()over(partition by user_id order by time_id desc ) rn  from   G_A_02062_DAY a where TIME_ID <=20110316) t where rn = 1 and STATUS_ID = '1'and user_id in (select USER_IDfrom (select a.*,row_number()over(partition by user_id order by time_id desc ) rn  from   G_A_02062_DAY a  where TIME_ID <=20110316) t where rn = 1 and STATUS_ID = '1'exceptselect user_id from session.int_check_user_status a        where a.usertype_id NOT IN ('2010','2020','2030','9000')        and a.test_flag='0')        select  from   BASS1.G_A_02059_DAY where time_id = 20110321exceptselect * from   BASS1.G_A_02059_DAY_20110321fix1340594select count(0),count(distinct user_id) from   BASS1.G_A_02059_DAY_20110321fix1340select * from   APP.SCH_CONTROL_RUNLOG where control_code like 'BASS1%'order by endtime descselect * from   app.sch_control_before where control_code like '%02059%'select * from   G_A_02008_DAY where time_id > 20110101select * from   G_A_01008_DAYselect * from   APP.SMS_SEND_INFOselect * from  app.sch_control_alarm where alarmtime >=  timestamp('20110311'||'000000') --and flag = -1and control_code like 'BASS1%'order by alarmtime desc select * from   bass1.int_program_dataselect * from   bass1.int_verf_err_listselect * from   bass1.int_all_job_logselect * from   USYS_INT_CONTROLselect a.user_id,call_duration_m  from(select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_20110316where MNS_TYPE=1group by user_id) ainner join  (select distinct user_id from bass2.dw_product_td_20110316 where (td_call_mark =1            or td_gprs_mark =1            or td_addon_mark=1)and userstatus_id in (1,2,3,6,8) and usertype_id in (1,2,9)and test_mark=0 ) bon a.user_id=b.user_id order by call_duration_m desc select * from bass2.dw_cust_20110316where cust_id in(select cust_id from bass2.dw_product_20110316where user_id in('89160000208968','89160000409386','89160000688192','89157334175039','89160000523755','89160000454937','89160000604528','89157334216159','89160000696074','89157334323731')) select * from   g_a_02059_dayselect ENTERPRISE_BUSI_TYPE , count(0) --,  count(distinct ENTERPRISE_BUSI_TYPE ) from bass1.g_a_02059_day group by  ENTERPRISE_BUSI_TYPE order by 1 select count(0)from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from g_a_02059_day awhere TIME_ID < 20110301and MANAGE_MODE = '2'and ENTERPRISE_BUSI_TYPE ='1340'and length(trim(user_id)) = 14) t) t2where rn = 1 and STATUS_ID ='1'select count(0)from (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from G_A_02055_DAY where MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340') t ) t2 where rn = 1  and  STATUS_ID ='1'  select ENTERPRISE_ID								from 										(										select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 										from (													select *													from G_A_02055_DAY where MANAGE_MODE = '2'													and time_id <=20110321 and ENTERPRISE_BUSI_TYPE = '1340'													) t 										) t2 where rn = 1  and  STATUS_ID ='1'                                                                                  select * from   G_A_02059_DAY where      ENTERPRISE_BUSI_TYPE = '1340'                                   select * from   bass1.g_a_02054_day                                                                      CREATE TABLE BASS1.G_A_02059_DAY_20110321bak (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  USER_ID               CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (USER_ID,    TIME_ID   ) USING HASHINGALTER TABLE BASS1.G_A_02059_DAY_20110321bak  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEinsert into G_A_02059_DAY_20110321bakselect * from G_A_02059_DAY                                   select count(0) from    G_A_02059_DAY_20110321bakselect count(0) from    G_A_02059_DAYdrop table BASS1.G_A_02059_DAY_20110321fix1340CREATE TABLE BASS1.G_A_02059_DAY_20110321fix1340 (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  USER_ID               CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (USER_ID,    TIME_ID   ) USING HASHINGALTER TABLE BASS1.G_A_02059_DAY_20110321fix1340  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEselect * from   BASS1.G_A_02059_DAY_20110321fix1340select * from   G_A_02055_DAYinsert into BASS1.G_A_02059_DAY_20110321fix1340select  20110321				,a.ENTERPRISE_ID				,b.USER_ID				,'1340'				,'2'				,'20110321'				,'1' from   bass2.DW_ENTERPRISE_MEMBER_MID_20110320 a				where a.ENTERPRISE_ID 				in (							select  ENTERPRISE_ID								from 										(										select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 										from (													select *													from G_A_02055_DAY where MANAGE_MODE = '2'													and time_id <=20110320 and ENTERPRISE_BUSI_TYPE = '1340'													) t 										) t2 where rn = 1  and  STATUS_ID ='1'  					)select count(0),count(distinct user_id ) from     BASS1.G_A_02059_DAY_20110321fix13407297	7297select * from   BASS1.G_A_02059_DAY_20110321fix1340select count(0) from    BASS1.G_A_02059_DAY_20110321fix1340insert into BASS1.G_A_02059_DAY_20110321fix1340select  20110321				,a.ENTERPRISE_ID				,b.USER_ID				,'1340'				,'2'				,'20110321'				,'1'                --select count(0),count(distinct   b.USER_ID)              from  (						select  ENTERPRISE_ID,							from 									(									select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 									from (												select *												from G_A_02055_DAY where MANAGE_MODE = '2'												and time_id <=20110320 and ENTERPRISE_BUSI_TYPE = '1340'												) t 									) t2 where rn = 1  and  STATUS_ID ='1'  					) a,				  bass2.dw_enterprise_member_mid_20110320 b,		      bass2.dw_enterprise_msg_20110320 c,		      bass2.dw_product_20110320 d				where a.enterprise_id=b.enterprise_id				  and a.enterprise_id=c.enterprise_id				  and b.user_id=d.user_id				  and d.usertype_id in (1,2,3,6)7039 row(s) affected.select * from   BASS1.G_A_02059_DAY_20110321fix1340                                   select * from   	bass1.g_a_02055_dayselect count(0) from    (select * from  G_A_02059_DAY where time_id = 20110321except select * from   G_A_02059_DAY_20110321fix1340) t594+7037select count(0) from   G_A_02059_DAY where time_id = 201103217631select count(0)from (select user_idfrom (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from g_a_02059_day awhere TIME_ID < 20110301and MANAGE_MODE = '2'and ENTERPRISE_BUSI_TYPE ='1340'and length(trim(user_id)) = 14) t) t2where rn = 1 and STATUS_ID ='1') t join G_A_02059_DAY_20110321fix1340 b on t.user_id = b.user_id select test_mark,count(0) from   G_A_02059_DAY_20110321fix1340 ajoin bass2.dw_product_20110320 b on a.user_id = b.user_id       group by test_markselect a.enterprise_id,b.ENTERPRISE_NAME,count(0) from   G_A_02059_DAY_20110321fix1340 a join bass2.dw_enterprise_msg_20110320 b on a.enterprise_id = b.enterprise_idgroup by a.enterprise_id,b.ENTERPRISE_NAMEdelete from BASS1.G_A_02059_DAY_20110321fix1340insert into BASS1.G_A_02059_DAY_20110321fix1340select  20110321				,a.ENTERPRISE_ID				,b.USER_ID				,'1340'				,'2'				,'20110321'				,'1' from  (				select  ENTERPRISE_ID					from 							(							select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 							from (										select *										from G_A_02055_DAY where MANAGE_MODE = '2'										and time_id <=20110320 and ENTERPRISE_BUSI_TYPE = '1340'										) t 							) t2 where rn = 1  and  STATUS_ID ='1'  			) a,		  bass2.dw_enterprise_member_mid_20110320 b,      bass2.dw_enterprise_msg_20110320 c,      bass2.dw_product_20110320 dwhere a.enterprise_id=b.enterprise_id			and a.enterprise_id=c.enterprise_id			and b.user_id=d.user_id			and d.usertype_id in (1,2,3,6)			and d.test_mark = 0 								select 20110320											 ,a.enterprise_id											 ,b.user_id											 ,'1340'											 ,'3'											 ,'20110320'											 ,a.status_id								from 	bass1.g_a_02055_day a,								      bass2.dw_enterprise_member_mid_20110320 b,								      bass2.dw_enterprise_msg_20110320 c,								      bass2.dw_product_20110320 d								where a.enterprise_id=b.enterprise_id								  and a.enterprise_id=c.enterprise_id								  and b.user_id=d.user_id								  and d.usertype_id in (1,2,3,6)								  and a.enterprise_busi_type='1340'								  and a.time_id=20110320								  and a.status_id='1'								  and d.test_mark = 0		                                                               select * from                                      BASS1.G_A_02059_DAYselect time_id , count(0) --,  count(distinct time_id ) from BASS1.G_A_02059_DAY group by  time_id order by 1 select * from   app.sch_control_map where program_name like '%test%'select * from   bass1.int_program_data where program_name like '%02059%'select count(0) from   syscat.tables                                   1355159234rename BASS2.DW_ENTERPRISE_MEMBER_MID_20100520 to DW_ENTERPRISE_MEMBER_MID_20110320rename BASS2.DW_ENTERPRISE_MEMBER_MID_20110320 to DW_ENTERPRISE_MEMBER_MID_20100520select * from   G_A_02059_DAY_0315modifyinsert into app.sch_control_beforevalues('BASS1_G_A_02059_DAY.tcl','BASS1_G_A_02055_DAY.tcl')select * from   app.sch_control_before where control_code = 'BASS1_G_A_02059_DAY.tcl'                                 select count(0)from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from g_a_02059_day awhere TIME_ID < 20110301and MANAGE_MODE = '2'and ENTERPRISE_BUSI_TYPE ='1340'and length(trim(user_id)) = 14) t) t2where rn = 1 and STATUS_ID ='1'                                                                  select * from g_a_02059_day awhere TIME_ID < 20110321and MANAGE_MODE = '3'and ENTERPRISE_BUSI_TYPE ='1040'and length(trim(user_id)) = 14                                                                  select count(0)from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from g_a_02059_day awhere TIME_ID < 20110316and MANAGE_MODE = '3'and ENTERPRISE_BUSI_TYPE ='1040'and length(trim(user_id)) = 14) t) t2where rn = 1 and STATUS_ID ='1'select TIME_ID , count(0) --,  count(distinct TIME_ID ) from bass1.g_a_02059_day group by  TIME_ID order by 1                                  20110321	76315947039select count(0)from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from g_a_02059_day awhere TIME_ID <= 20110321and MANAGE_MODE = '2'and ENTERPRISE_BUSI_TYPE ='1340'and length(trim(user_id)) = 14) t) t2where rn = 1 and STATUS_ID ='1'                                                                                                                                                                     select * from g_a_02059_day awhere TIME_ID <= 20110321and MANAGE_MODE = '2'and ENTERPRISE_BUSI_TYPE ='1340'and length(trim(user_id)) = 14select count(0),count(distinct user_id) from g_a_02059_day awhere TIME_ID <= 20110321and MANAGE_MODE = '2'and ENTERPRISE_BUSI_TYPE ='1340'and length(trim(user_id)) = 14select  count(0),count(distinct b.user_id)from  (				select  ENTERPRISE_ID					from 							(							select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 							from (										select *										from G_A_02055_DAY where MANAGE_MODE = '2'										and time_id <=20110320 and ENTERPRISE_BUSI_TYPE = '1340'										) t 							) t2 where rn = 1  and  STATUS_ID ='1'  			) a,		  bass2.dw_enterprise_member_mid_20110320 b,      bass2.dw_enterprise_msg_20110320 c,      bass2.dw_product_20110320 dwhere a.enterprise_id=b.enterprise_id			and a.enterprise_id=c.enterprise_id			and b.user_id=d.user_id			and d.usertype_id in (1,2,3,6)			and d.test_mark = 0                                                                   select count(0) from G_A_02059_DAY_20110321fix1340            select count(0)from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from g_a_02059_day awhere TIME_ID <= 20110321and MANAGE_MODE = '2'and ENTERPRISE_BUSI_TYPE ='1340'and length(trim(user_id)) = 14) t) t2where rn = 1 and STATUS_ID ='1'                                 select count(0)from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from g_a_02059_day awhere TIME_ID < 20110316and MANAGE_MODE = '3'and ENTERPRISE_BUSI_TYPE ='1040'and length(trim(user_id)) = 14) t) t2where rn = 1 and STATUS_ID ='1'--44 条select * from   bass1.g_i_06032_day                                             select count(0)from (select t.*,row_number()over(partition by user_id order by time_id desc ) rn from (select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and length(trim(user_id)) = 14) t) t2where rn = 1 and  STATUS_ID ='1'select * from   BASS1.G_A_02061_DAY select * from bass2.DWD_PS_NET_NUMBER_20110321 where number_segment='1820808'select * from    g_a_02059_day select count(0) from   (select a.*,row_number()over(partition by user_id order by time_id desc )  rn from g_a_02059_day a  where time_id <= 20110320  ) t where rn = 1 and status_id = '1'BASS2.DIM_CONTROL_INFO.delBASS2.DIM_DEVICE_INFO.delBASS2.DIM_DEVICE_PROFILE.delBASS2.DIM_PROPERTY_INFO.delBASS2.DIM_PROPERTY_VALUE_RANGE.delselect * from   g_i_77780_dayselect count(0) from BASS2.DIM_CONTROL_INFOselect count(0) from BASS2.DIM_DEVICE_INFO18386select count(0) from BASS2.DIM_DEVICE_PROFILE1319690select count(0) from BASS2.DIM_PROPERTY_INFOselect count(0) from BASS2.DIM_PROPERTY_VALUE_RANGEselect * from   BASS2.DIM_DEVICE_PROFILECREATE TABLE BASS1.G_A_02059_DAY_down20110321 (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  USER_ID               CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (USER_ID,    TIME_ID   ) USING HASHINGdrop table BASS1.G_A_02059_DAY_DOWN20110321select * from   BASS1.G_A_02059_DAY_DOWN20110321select * from BASS2.DIM_CONTROL_INFO        select * from BASS2.DIM_DEVICE_INFO         select * from BASS2.DIM_DEVICE_PROFILE      select * from BASS2.DIM_PROPERTY_INFO       select * from BASS2.DIM_PROPERTY_VALUE_RANGEselect tabschema,tabname from   syscat.tables where tabname like 'DIM_TERM_TAC%'select * from   BASS2.DIM_TACNUM_DEVIDselect count(0),count(distinct device_id) from BASS2.DIM_DEVICE_INFO         18386	18385select count(0),count(distinct device_id) from BASS2.DIM_DEVICE_PROFILE      1319690	18385select time_id ,count(distinct tac_num)--,  count(distinct time_id ) from BASS2.DIM_TACNUM_DEVID group by  time_id order by 1 select a.time_id ,count(distinct tac_num)--,  count(distinct time_id ) from BASS2.DIM_TACNUM_DEVID  a , BASS2.DIM_DEVICE_INFO        b where b.device_id = a.dev_id group by  a.time_id order by 1 201102	33619select time_id , count(0) ,count(distinct tac_num),count(distinct dev_id)--,  count(distinct time_id ) from BASS2.DIM_TACNUM_DEVID group by  time_id order by 1 201102	33622	33620	15165select * from   BASS2.DIM_TERM_TAC                                                                                          select count(0) ,count(distinct tac_num) from    BASS2.DIM_TERM_TAC27564	27564select int(replace(char(current date - 1 days),'-','')) from bass2.dualselect * from  bass2.dw_product_imei_rela_ds                                             select * from   BASS1.G_I_02047_MONTH select count(0),count(distinct device_id) from BASS2.DIM_DEVICE_PROFILE      select 1701 as kpi_id,city_id,count(distinct user_id) as kpi_value,brand_idfrom bass2.where td_user_mark = 1 and usertype_id in (1,2,9) and userstatus_id in (1,2,3,6,8) and test_mark<>1select count(user_id) , count(distinct user_id)                    from bass2.dw_product_201102                   where usertype_id in (1,2,9)                      and userstatus_id in (1,2,3,6,8)                     and test_mark<>1select count(distinct substr(imei,1,8)) from    bass2.dw_product_mobilefunc_201102where  usertype_id in (1,2,9)  and userstatus_id in (1,2,3,6,8) 26774  33019                                   vs 33620                                   select  count(distinct substr(a.imei,1,8))--,  count(distinct time_id ) from  bass2.dw_product_mobilefunc_201102  a , BASS2.DIM_TACNUM_DEVID        b where  substr(a.imei,1,8) = b.tac_num                    and usertype_id in (1,2,9)                      and userstatus_id in (1,2,3,6,8)                     --and test_mark<>1declare global temporary table session.t_imei    (        imei varchar(18)        ,tac_num varchar(10)    )                            partitioning key            (   imei     ) using hashing           with replace on commit preserve rows not logged in tbs_user_tempinsert into  session.t_imeiselect imei,substr(a.imei,1,8) from   bass2.dw_product_mobilefunc_201102 a  select count(0),count(distinct tac_num ) from    session.t_imei                                   4110313	33019select count(distinct a.tac_num)from ( select distinct tac_num from session.t_imei      ) a ,BASS2.DIM_TACNUM_DEVID        b where a.tac_num = b.TAC_NUM19618 select  from   bass2.dw_product_imei_rela_ds   declare global temporary table session.t_imei2    (        imei varchar(18)        ,tac_num varchar(10)    )                            partitioning key            (   imei     ) using hashing           with replace on commit preserve rows not logged in tbs_user_tempinsert into session.t_imei2 select distinct imei ,substr(a.imei,1,8)  from   bass2.dw_product_imei_rela_ds  a where op_time = '2011-03-22' and last_use_date >= '2010-12-22' select count(0),count(distinct tac_num ) from    session.t_imei2                                   1701113	30058 select count(distinct a.tac_num)from ( select distinct tac_num from session.t_imei2      ) a ,BASS2.DIM_TACNUM_DEVID        b where a.tac_num = b.TAC_NUM1776002047select * from    bass1.G_S_02047_MONTHselect time_id,count(0) from   bass1.G_S_02047_MONTHgroup by time_idselect * from   syscat.tables where tabname like '%G_I_02006_MONTH%'CREATE TABLE BASS1.G_I_77780_DAY (	 TIME_ID            		NUMBER(8)      NOT NULL       ----数据日期        	,ENTERPRISE_ID      		CHAR(20)       NOT NULL       ----集团客户标识    	,ID                 		CHAR(9)        NOT NULL 				----ID              	,ENTERPRISE_NAME    		CHAR(60)            					----集团客户名称    	,ORG_TYPE           		CHAR(5)             					----机构类型        	,ADDR_CODE          		CHAR(6)             					----地址代码        	,CITY               		CHAR(20)            					----城市地区        	,REGION             		CHAR(20)            					----区县            	,COUNTY             		CHAR(20)            					----乡镇            	,DOOR_NO            		CHAR(60)            					----门牌            	,AREA_CODE          		CHAR(5)             					----区号            	,PHONE_NO1          		CHAR(11)            					----电话1           	,PHONE_NO2          		CHAR(10)            					----电话2           	,POST_CODE          		CHAR(6)             					----邮政编码        	,INDUSTRY_TYPE      		CHAR(4)             					----行业类型        	,EMPLOYEE_CNT       		CHAR(8)             					----职员            	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----产业单位数      	,ECONOMIC_TYPE      		CHAR(3)             					----经济类型        	,OPEN_YEAR          		CHAR(4)             					----开业1           	,OPEN_MONTH         		CHAR(2)             					----开业2           	,SHAREHOLDER        		CHAR(1)             					----控股            	,GROUP_TYPE         		CHAR(1)             					----集团类型        	,MANAGE_STYLE       		CHAR(1)             					----经营形式        	,OPERATE_REVENUE_CLASS	CHAR(2)           						----营业收入分类    	,CAPITAL_CLASS      		CHAR(2)           						----资产分类        	,INDUSTRY_CLASS_CODE		CHAR(2)    NOT NULL         	----行业分类编码    	,CUST_STATUS        		CHAR(2)             					----集团客户状态    	,CUST_INFO_SRC_ID   		CHAR(1)             					----集团客户资料来源	,UPLOAD_TYPE_ID     		CHAR(1)    NOT NULL         	----上传种类标识     )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (TIME_ID,ENTERPRISE_ID) USING HASHINGALTER TABLE BASS1.G_I_77780_DAY  LOCKSIZE ROW  APPEND OFF  NOT VOLATILECREATE TABLE BASS1.G_I_77780_DAY (	 TIME_ID            		INTEGER        NOT NULL       ----数据日期        	,ENTERPRISE_ID      		CHAR(20)       NOT NULL       ----集团客户标识    	,ID                 		CHAR(9)             					----ID              	,ENTERPRISE_NAME    		CHAR(60)            					----集团客户名称    	,ORG_TYPE           		CHAR(5)             					----机构类型        	,ADDR_CODE          		CHAR(6)             					----地址代码        	,CITY               		CHAR(20)            					----城市地区        	,REGION             		CHAR(20)            					----区县            	,COUNTY             		CHAR(20)            					----乡镇            	,DOOR_NO            		CHAR(60)            					----门牌            	,AREA_CODE          		CHAR(5)             					----区号            	,PHONE_NO1          		CHAR(11)            					----电话1           	,PHONE_NO2          		CHAR(10)            					----电话2           	,POST_CODE          		CHAR(6)             					----邮政编码        	,INDUSTRY_TYPE      		CHAR(4)             					----行业类型        	,EMPLOYEE_CNT       		CHAR(8)             					----职员            	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----产业单位数      	,ECONOMIC_TYPE      		CHAR(3)             					----经济类型        	,OPEN_YEAR          		CHAR(4)             					----开业1           	,OPEN_MONTH         		CHAR(2)             					----开业2           	,SHAREHOLDER        		CHAR(1)             					----控股            	,GROUP_TYPE         		CHAR(1)             					----集团类型        	,MANAGE_STYLE       		CHAR(1)             					----经营形式        	,OPERATE_REVENUE_CLASS	CHAR(2)           						----营业收入分类    	,CAPITAL_CLASS      		CHAR(2)           						----资产分类        	,INDUSTRY_CLASS_CODE		CHAR(2)    NOT NULL         	----行业分类编码    	,CUST_STATUS        		CHAR(2)             					----集团客户状态    	,CUST_INFO_SRC_ID   		CHAR(1)             					----集团客户资料来源	,UPLOAD_TYPE_ID     		CHAR(1)    NOT NULL         	----上传种类标识     )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (TIME_ID,ENTERPRISE_ID) USING HASHINGALTER TABLE BASS1.G_I_77780_DAY  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEselect * from   BASS1.G_I_77780_DAYCREATE TABLE BASS1.G_I_77780_DAY_MID (	 TIME_ID            		INTEGER        NOT NULL       ----数据日期        	,ENTERPRISE_ID      		CHAR(20)       NOT NULL       ----集团客户标识    	,ID                 		CHAR(9)             					----ID              	,ENTERPRISE_NAME    		CHAR(60)            					----集团客户名称    	,ORG_TYPE           		CHAR(5)             					----机构类型        	,ADDR_CODE          		CHAR(6)             					----地址代码        	,CITY               		CHAR(20)            					----城市地区        	,REGION             		CHAR(20)            					----区县            	,COUNTY             		CHAR(20)            					----乡镇            	,DOOR_NO            		CHAR(60)            					----门牌            	,AREA_CODE          		CHAR(5)             					----区号            	,PHONE_NO1          		CHAR(11)            					----电话1           	,PHONE_NO2          		CHAR(10)            					----电话2           	,POST_CODE          		CHAR(6)             					----邮政编码        	,INDUSTRY_TYPE      		CHAR(4)             					----行业类型        	,EMPLOYEE_CNT       		CHAR(8)             					----职员            	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----产业单位数      	,ECONOMIC_TYPE      		CHAR(3)             					----经济类型        	,OPEN_YEAR          		CHAR(4)             					----开业1           	,OPEN_MONTH         		CHAR(2)             					----开业2           	,SHAREHOLDER        		CHAR(1)             					----控股            	,GROUP_TYPE         		CHAR(1)             					----集团类型        	,MANAGE_STYLE       		CHAR(1)             					----经营形式        	,OPERATE_REVENUE_CLASS	CHAR(2)           						----营业收入分类    	,CAPITAL_CLASS      		CHAR(2)           						----资产分类        	,INDUSTRY_CLASS_CODE		CHAR(2)    NOT NULL         	----行业分类编码    	,CUST_STATUS        		CHAR(2)             					----集团客户状态    	,CUST_INFO_SRC_ID   		CHAR(1)             					----集团客户资料来源	,UPLOAD_TYPE_ID     		CHAR(1)    NOT NULL         	----上传种类标识     )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (TIME_ID,ENTERPRISE_ID) USING HASHINGALTER TABLE BASS1.G_I_77780_DAY_MID  LOCKSIZE ROW  APPEND OFF  NOT VOLATILE        select dbpartitionnum(PRODUCT_NO) as partitionnum_num, count(*)as rows from G_S_21003_MONTHgroup by dbpartitionnum(PRODUCT_NO)order by dbpartitionnum (PRODUCT_NO)select * from   syscat.tables where tabname like '%777%'select count(0) from   G_I_77778_DAY--目标表drop table BASS1.G_I_77780_DAYCREATE TABLE BASS1.G_I_77780_DAY (	 TIME_ID            		INTEGER        NOT NULL       ----数据日期        	,ENTERPRISE_ID      		CHAR(20)       NOT NULL       ----集团客户标识    	,ID                 		CHAR(9)        								----ID*              	,ENTERPRISE_NAME    		CHAR(60)       								----集团客户名称*    	,ORG_TYPE           		CHAR(5)             					----机构类型 							BASS_STD_0001       	,ADDR_CODE          		CHAR(6)             					----地址代码*        	,CITY               		CHAR(20)            					----城市地区*        	,REGION             		CHAR(20)            					----区县*            	,COUNTY             		CHAR(20)            					----乡镇*            	,DOOR_NO            		CHAR(60)            					----门牌*            	,AREA_CODE          		CHAR(5)             					----区号*            	,PHONE_NO1          		CHAR(11)            					----电话1*           	,PHONE_NO2          		CHAR(10)            					----电话2*           	,POST_CODE          		CHAR(6)             					----邮政编码*        	,INDUSTRY_TYPE      		CHAR(4)             					----行业类型 							BASS_STD1_0113       	,EMPLOYEE_CNT       		CHAR(8)             					----职员            	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----产业单位数*      	,ECONOMIC_TYPE      		CHAR(3)             					----经济类型 							BASS_STD_0002       	,OPEN_YEAR          		CHAR(4)             					----开业1           	,OPEN_MONTH         		CHAR(2)             					----开业2           	,SHAREHOLDER        		CHAR(1)             					----控股  								BASS_STD_0005          	,GROUP_TYPE         		CHAR(1)             					----集团类型 							BASS_STD_0003       	,MANAGE_STYLE       		CHAR(1)             					----经营形式      				BASS_STD_0004  	,OPERATE_REVENUE_CLASS	CHAR(2)           						----营业收入分类 					BASS_STD_0006   	,CAPITAL_CLASS      		CHAR(2)           						----资产分类     					BASS_STD_0007   	,INDUSTRY_CLASS_CODE		CHAR(2)    NOT NULL         	----行业分类编码 					BASS_STD1_0043   	,CUST_STATUS        		CHAR(2)             					----集团客户状态    	,CUST_INFO_SRC_ID   		CHAR(1)             					----集团客户资料来源	,UPLOAD_TYPE_ID     		CHAR(1)    NOT NULL         	----上传种类标识     )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (TIME_ID,ENTERPRISE_ID) USING HASHINGALTER TABLE BASS1.G_I_77780_DAY  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEselect * from   app.sch_control_before where before_control_code like '%22012%'select 'xxxxx',count(0) from app.sch_control_alarm where alarmtime >= timestamp('20110324'||'000000') and flag = -1 and control_code like 'BASS1%'select * from    bass1.int_02011_snapshotselect * from   app.sch_control_task where control_code like '%02011%'--自动点告警 BASS1_INT_CHECK_INDEX_WAVE_DAY.tclselect * from app.sch_control_runlogwhere control_code like '%INT_02011_SNAPSHOT.tcl%'select 'insert into BASS1.G_MON_D_INTERFACE values('||unit_code||','||substr(data_file,1,1)||')' from app.g_runlog where time_id=int(replace(char(current date - 1 days),'-',''))and return_flag=1select TABNAME  from   syscat.tables where tabschema = 'BASS1'select control_code 
						            from app.sch_control_runlog 
						            where date(endtime)=date(current date)
						                  and flag=0select flag from   app.sch_control_runlog where control_code like '%INT_CHECK_RUNLOG_DAY%'and date(endtime)=date(current date) and flag=0select a.unit_code ,substr(b.TABNAME,3,1) ,b.TABNAMEfrom ( select unit_code from app.g_runlog where time_id=int(replace(char(current date - 1 days),'-',''))and return_flag=1 ) a, syscat.tables b where a.unit_code = substr(b.TABNAME,5,5)and b.tabschema = 'BASS1'and tabname like '%DAY'g_i_01select * from    APP.G_UNIT_INFO select * from  app.sch_control_alarm where alarmtime >=  current timestamp - 2 days--and flag = -1--and control_code like 'BASS1%'order by alarmtime desc select * from   app.sch_control_task where control_code like '%15304%'select * from   bass2.VGOP_15304_20110324select count(0)
                    from bass2.dw_product_20110324 a ,bass2.dwd_cust_msg_20110324 b 
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1                     and a.cust_id = b.cust_id                     select b.cust_name , count(0)
                    from bass2.dw_product_20110324 a ,bass2.dwd_cust_msg_20110324 b 
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1                     and a.cust_id = b.cust_idgroup by    b.cust_nameorder by 2 desc                    select a.*,b.*
                    from bass2.dw_product_20110324 a ,bass2.dwd_cust_msg_20110324 b 
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1                     and a.cust_id = b.cust_idselect count(0) from session.int_check_user_status where create_date = '20110324'and test_flag = '0'                     4231 3月23日	 3月24日2880	4239select * from   g_s_22012_day20110324	20110324	`4231      	1636785     	23021010    	412022      	3873747     	3424      	335706      20110323	20110323	`2878      	1635972     	22901457    	389311      	3975018     	120       	329440      select b.cust_name,a.CRM_BRAND_ID1, count(0)                    from bass2.dw_product_20110323 a ,bass2.dwd_cust_msg_20110323 b                    where usertype_id in (1,2,9)                      and day_new_mark = 1 and test_mark<>1                     and a.cust_id = b.cust_idand  b.CHANNEL_ID in(91000047,91000048,91000049,91000046,91000050,91000035,97000019,91000012,97000023)group by    b.cust_name, a.CRM_BRAND_ID1order by 3 desc                      select b.cust_name,b.CHANNEL_ID, count(0)                    from bass2.dw_product_20110322 a ,bass2.dwd_cust_msg_20110322 b                    where usertype_id in (1,2,9)                      and day_new_mark = 1 and test_mark<>1                     and a.cust_id = b.cust_idgroup by    b.cust_name, b.CHANNEL_IDorder by 3 desc 拉萨市东城区代理店	91000046	126拉萨市南城区代理店	91000049	114拉萨市西城区代理店	91000047	113拉萨市开发区代理店	91000048	78拉萨市北城区代理店	91000050	77拉萨市邮政局	91000012	38拉萨万利文体商贸有限公司	91000035	11巴桑	97000019	1王玉萍	97000023	1索南顿珠	97000023	1普布次仁	97000019	1欧珠	97000019	1洛桑顿珠	97000019	1洪吉	97000023	1次仁桑珠	97000019	1次仁德吉	97000019	1巴桑	97000023	1declare global temporary table session.int_check_user_status    (   user_id        CHARACTER(15),   product_no     CHARACTER(15),   test_flag      CHARACTER(1),   sim_code       CHARACTER(15),   usertype_id    CHARACTER(4),   create_date    CHARACTER(15),   time_id        int)                            partitioning key            (   user_id     ) using hashing           with replace on commit preserve rows not logged in tbs_user_tempinsert into session.int_check_user_status (         user_id            ,product_no         ,test_flag          ,sim_code           ,usertype_id          ,create_date        ,time_id )select e.user_id        ,e.product_no          ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag        ,e.sim_code        ,f.usertype_id          ,e.create_date          ,f.time_id       from (select user_id,create_date,product_no,sim_code,usertype_id                    ,row_number() over(partition by user_id order by time_id desc ) row_id     from bass1.g_a_02004_day  where time_id<=20110324) einner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id                  from bass1.g_a_02008_day               where time_id<=20110324 ) f on f.user_id=e.user_idwhere e.row_id=1 and f.row_id=1commitselect * from bass2.ODS_PRODUCT_ORD_CUST_20110324where bill_id in(select product_no from bass2.dw_product_20110324where cust_id in(   select cust_id from bass2.dw_cust_20110324    where cust_id in        (        select cust_id from bass2.dw_product_20110324        where user_id in            (            select user_id from session.int_check_user_status             where create_date = '20110324'            and test_flag='0'            )        )    and cust_name like '%代理%'))select business_id,count(*) from bass2.ODS_PRODUCT_ORD_CUST_20110324where bill_id in(select product_no from bass2.dw_product_20110324where cust_id in(   select cust_id from bass2.dw_cust_20110324    where cust_id in        (        select cust_id from bass2.dw_product_20110324        where user_id in            (            select user_id from session.int_check_user_status             where create_date = '20110324'            and test_flag='0'            )        )    and cust_name like '%代理%'))group by business_idBUSINESS_ID	    2191000000007	31191000000008	2191000000012	1191000000145	2063191000001017	45191000001024	116193000000001	12select * from bass2.ODS_PROD_UP_PRODUCT_ITEM_20110324where product_item_id in (191000000145)select b.CHANNEL_ID, count(0)                    from bass2.dw_product_20110324 a ,bass2.dwd_cust_msg_20110324 b                    where usertype_id in (1,2,9)                      and day_new_mark = 1 and test_mark<>1                     and a.cust_id = b.cust_idand b.cust_name like '%代理%'group by    b.CHANNEL_IDorder by 2 desc 91000047	56891000048	53591000049	23191000046	17791000050	173select b.CHANNEL_ID, count(0)                    from bass2.dw_product_20110321 a ,bass2.dwd_cust_msg_20110321 b                    where usertype_id in (1,2,9)                      and day_new_mark = 1 and test_mark<>1                     and a.cust_id = b.cust_idand b.CHANNEL_ID in (91000047,91000048,91000049,91000046,91000050)group by    b.CHANNEL_IDorder by 1 desc select * from bass2.ODS_PROD_UP_PRODUCT_ITEM_20110324where product_item_id in (191000000145)select * from BASS2.ODS_CHANNEL_INFO_20110324 bwhere  b.CHANNEL_ID in (91000047,91000048,91000049,91000046,91000050)order by 1 desc drop table BASS1.G_A_02059_DAY_down20110321CREATE TABLE BASS1.G_A_02059_DAY_down20110321 (  ENTERPRISE_ID         CHARACTER(20),  USER_ID               CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (USER_ID   ) USING HASHINGselect count(0)from (select         ENTERPRISE_ID        ,USER_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,ORDER_DATE        ,STATUS_ID from bass1.G_A_02059_DAYexcept select         ENTERPRISE_ID        ,USER_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,ORDER_DATE        ,STATUS_ID from  bass1.G_A_02059_DAY_down20110321        ) tselect ENTERPRISE_IDfrom (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from BASS1.G_A_02055_DAY_down20110321 where MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340') t ) t2 where rn = 1  and  STATUS_ID ='1'  exceptselect ENTERPRISE_IDfrom (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from BASS1.G_A_02055_DAY where MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340') t ) t2 where rn = 1  and  STATUS_ID ='1'        select * from   G_A_02055_DAY where ENTERPRISE_ID = '89103000041929'select * from   G_A_02055_DAY_down20110321 where ENTERPRISE_ID = '89103000041929'select * from   bass2.dw_enterprise_member_mid_20110327 where enterprise_id = '89103000041929'select count(0)from (select distinct trim(ENTERPRISE_ID) ENTERPRISE_ID,trim(USER_ID) from bass1.G_A_02059_DAY_down20110321 a exceptselect distinct trim(ENTERPRISE_ID) ENTERPRISE_ID,trim(USER_ID) from bass1.G_A_02059_DAY b) tselect count(0) , count(distinct trim(ENTERPRISE_ID)) ENTERPRISE_ID from bass1.G_A_02059_DAY_down20110321bass1.G_A_02059_DAY_down20110321select ascii(ENTERPRISE_ID),ENTERPRISE_IDfrom  bass1.G_A_02059_DAY_down20110321 where ENTERPRISE_ID like  '%89403000904884%'fetch first 1 rows only     select ascii(ENTERPRISE_ID)from  bass1.G_A_02059_DAY where ENTERPRISE_ID = '89403000904884'fetch first 1 rows onlydrop table BASS1.G_A_02059_DAY_down20110321CREATE TABLE BASS1.G_A_02059_DAY_down20110321 (  ENTERPRISE_ID         CHARACTER(20),  USER_ID               CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (USER_ID   ) USING HASHING           select * from BASS1.G_A_02055_DAY where enterprise_id = '89202999392356'        CREATE TABLE BASS1.G_A_02055_DAY_down20110321 (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  PAY_TYPE              CHARACTER(1),  CREATE_MODE           CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (ENTERPRISE_ID,    TIME_ID   ) USING HASHINGALTER TABLE BASS1.G_A_02055_DAY_down20110321  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEselect * from   BASS1.G_A_02055_DAY_down20110321select * from   bass2.dw_enterbass2.DWD_GROUP_ORDER_FEATUR_${timestamp}select tabname from syscat.tables where tabname like '%DWD_ENTERPRISE_SUB%'select * from   G_A_02055_DAY_down20110321 where ENTERPRISE_ID = '89103000041929'select date(DONE_DATE) from   bass2.DWD_ENTERPRISE_SUB_20101001 WHERE GROUP_ID = '89103000041929'AND DONE_DATE LIKE '2009-11%'select 20110328,a.ENTERPRISE_ID,a.ENTERPRISE_BUSI_TYPE,a.MANAGE_MODE,a.PAY_TYPE,a.CREATE_MODE,'20110328' ORDER_DATE,'2' STATUS_IDfrom bass1.G_A_02055_DAY_down20110321 awhere  ENTERPRISE_ID = '89103000041929'select * from G_A_02055_DAYwhere ENTERPRISE_ID = '89103000041929'and ENTERPRISE_BUSI_TYPE = '1340'delete from   G_A_02055_DAY where ENTERPRISE_ID = '89103000041929'select * from app.sch_control_alarm select * from   G_A_02055_DAYselect * from app.sch_control_alarm where alarmtime >= timestamp('20110329'||'000000') --and flag = -1 and control_code like 'BASS1%'select * from bass2.dim_test_enterpriseselect * from syscat.tables where tabname like 'DIM%ENTERPRISE%'select * from    BASS1.t_grp_id_old_new_map--目标表drop table BASS1.t_grp_id_old_new_mapCREATE TABLE BASS1.t_grp_id_old_new_map (	 area_id            		INTEGER              ----数据日期        	,OLD_ENTERPRISE_ID      		CHAR(20)             ----old集团客户标识    	,NEW_ENTERPRISE_ID      		CHAR(20)            ----new集团客户标识    	,ENTERPRISE_NAME    		CHAR(60)       								----集团客户名称*     )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (OLD_ENTERPRISE_ID,NEW_ENTERPRISE_ID) USING HASHINGALTER TABLE BASS1.t_grp_id_old_new_map  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEselect distinct length(trim(NEW_ENTERPRISE_ID)) from   BASS1.t_grp_id_old_new_mapwhere  length(trim(NEW_ENTERPRISE_ID)) <> 14select * from   BASS1.t_grp_id_old_new_mapwhere  length(trim(NEW_ENTERPRISE_ID)) = 14select * from   BASS1.t_grp_id_old_new_mapwhere  length(trim(NEW_ENTERPRISE_ID)) = 14and NEW_ENTERPRISE_ID like '8%'and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_ID  CREATE TABLE BASS1.grp_id_old_new_map_20110330 like BASS1.t_grp_id_old_new_map  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (OLD_ENTERPRISE_ID,NEW_ENTERPRISE_ID) USING HASHINGALTER TABLE BASS1.grp_id_old_new_map_20110330  LOCKSIZE ROW  APPEND OFF  NOT VOLATILE      insert into   BASS1.grp_id_old_new_map_20110330select * from   BASS1.t_grp_id_old_new_mapwhere  length(trim(NEW_ENTERPRISE_ID)) = 14and NEW_ENTERPRISE_ID like '8%'and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_IDselect * from   BASS1.grp_id_old_new_map_20110330select count(0),count(distinct OLD_ENTERPRISE_ID) from   BASS1.grp_id_old_new_map_20110330select count(0),count(distinct NEW_ENTERPRISE_ID) from   BASS1.grp_id_old_new_map_20110330select *from (        select a.*,row_number()over(partition by OLD_ENTERPRISE_ID order by ENTERPRISE_NAME desc ) rn  from   BASS1.t_grp_id_old_new_map awhere  length(trim(NEW_ENTERPRISE_ID)) = 14and NEW_ENTERPRISE_ID like '8%'and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_ID) torder by 2,5delete from  BASS1.grp_id_old_new_map_20110330insert into   BASS1.grp_id_old_new_map_20110330select          AREA_ID        ,OLD_ENTERPRISE_ID        ,NEW_ENTERPRISE_ID        ,ENTERPRISE_NAMEfrom (        select a.*,row_number()over(partition by OLD_ENTERPRISE_ID order by ENTERPRISE_NAME desc ) rn  from   BASS1.t_grp_id_old_new_map awhere  length(trim(NEW_ENTERPRISE_ID)) = 14and NEW_ENTERPRISE_ID like '8%'and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_ID) t where rn = 1select * from    BASS1.grp_id_old_new_map_20110330select count(0) from app.g_unit_info where unit_code='04002'select * from app.g_unit_info where unit_code='77780'delete from app.g_unit_info where unit_code='77780'insert into app.g_unit_info values ('77780',0,'重要集团客户拍照置换清单','BASS1.G_I_77780_DAY',1,0,0)CREATE TABLE BASS1.dim_bass1_std_map (	 interface_code       INTEGER            	,dim_table_id      		CHAR(20)       NOT NULL      	,code                 CHAR(9)        NOT NULL				       	,code_name    		    CHAR(60)       NOT NULL											 )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (dim_table_id,code) USING HASHINGALTER TABLE BASS1.dim_bass1_std_map  LOCKSIZE ROW  APPEND OFF  NOT VOLATILE      select * from   BASS1.dim_bass1_std_map where dim_table_id = 'BASS_STD1_0001'    select ENTER_TYPE_ID from   G_I_77778_DAYgroup by ENTER_TYPE_IDselect * from   G_I_77778_DAYselect count(0)from (select distinct ENTER_TYPE_ID ENTER_TYPE_ID from   G_I_77778_DAY) a where ENTER_TYPE_ID  not in (select code from BASS1.dim_bass1_std_map where interface_code = '77780' and dim_table_id ='BASS_STD1_0001')group by ENTER_TYPE_IDdrop table BASS1.dim_bass1_std_mapCREATE TABLE BASS1.dim_bass1_std_map (	 interface_code       CHAR(5)            	,dim_table_id      		CHAR(20)       NOT NULL      	,code                 CHAR(9)        NOT NULL				       	,code_name    		    CHAR(60)       NOT NULL											 )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (dim_table_id,code) USING HASHINGALTER TABLE BASS1.dim_bass1_std_map  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEselect * from  bass1.G_I_77778_DAY create nickname xzbass1.G_I_77778_DAY 
for db25.bass1.G_I_77778_DAYinsert into bass1.G_I_77778_DAY select * from  xzbass1.G_I_77778_DAY  drop nickname xzbass1.G_I_77778_DAY select * from syscat.tables where tabschema = 'XZBASS2'------------nick name select * from  bass1.G_I_77778_DAY create nickname xzbass1.G_I_77778_DAY for db25.bass1.G_I_77778_DAYinsert into bass1.G_I_77778_DAY select * from  xzbass1.G_I_77778_DAY  drop nickname xzbass1.G_I_77778_DAY ------------nick name select count(0)
from 
(select distinct EC_TYPE EC_TYPE from   G_I_77778_DAY) a 
where EC_TYPE   in 
	(select code from BASS1.dim_bass1_std_map 
		where interface_code = '77780' 
		and dim_table_id ='BASS_STD1_0002'
  )
      select count(0)
from 
(select distinct CONTROL_TYPE CONTROL_TYPE from   G_I_77778_DAY) a 
where CONTROL_TYPE  not in 
	(select code from BASS1.dim_bass1_std_map 
		where interface_code = '77780' 
		and dim_table_id ='BASS_STD1_0005'
  )
       select count(distinct ENTERPRISE_ID),count(distinct ID),count(0) from bass1.G_I_77778_DAY    CREATE TABLE BASS1.G_I_77780_DAY_MID like bass1.G_I_77780_DAY
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,ENTERPRISE_ID) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY_MID
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
         
---中间表:
drop table  BASS1.G_I_77780_DAY_MID 

drop table  BASS1.G_I_77780_DAY_MID 
CREATE TABLE BASS1.G_I_77780_DAY_MID (
	 TIME_ID            		INTEGER               ----数据日期        
	,ENTERPRISE_ID      		CHAR(20)              ----集团客户标识    
	,ID                 		CHAR(9)        								----ID*              
	,ENTERPRISE_NAME    		CHAR(60)       								----集团客户名称*    
	,ORG_TYPE           		CHAR(5)             					----机构类型 							BASS_STD_0001       
		--GROUP_TYPE不规范CHAR(6) 
	,ADDR_CODE          		VARCHAR(10)             					----地址代码*        
	,CITY               		CHAR(20)            					----城市地区*        
	---REGION 不规范            		CHAR(20) 
	,REGION             		VARCHAR(100)            					----区县*            
	---COUNTY 不规范            		CHAR(20) 	
	,COUNTY             		VARCHAR(100)            					----乡镇*            
	,DOOR_NO            		CHAR(60)            					----门牌*            
	,AREA_CODE          		CHAR(5)             					----区号*            
	--PHONE_NO1 不规范  12       		CHAR(11)
	,PHONE_NO1          		varCHAR(111)            					----电话1*           
	--,PHONE_NO2  不规范  13        		CHAR(10) 
	,PHONE_NO2          		VARCHAR(110)            					----电话2*           
	--POST_CODE 不规范  14         		CHAR(6)
	,POST_CODE          		varCHAR(16)             					----邮政编码*        
--	,INDUSTRY_TYPE  不规范  15    		CHAR(4) 
	,INDUSTRY_TYPE      		VARCHAR(14)             					----行业类型 							BASS_STD1_0113       
	,EMPLOYEE_CNT       		CHAR(8)             					----职员            
	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----产业单位数*      
	--ECONOMIC_TYPE  不规范  18    		CHAR(3)
	,ECONOMIC_TYPE      		CHAR(13)             					----经济类型 							BASS_STD_0002       
	--OPEN_YEAR  不规范  19        		CHAR(4)
	,OPEN_YEAR          		varCHAR(14)             					----开业1           
	--OPEN_MONTH 不规范  20        		CHAR(2) 
	,OPEN_MONTH         		varCHAR(22)             					----开业2           
	--SHAREHOLDER不规范CHAR(1) 
	,SHAREHOLDER        		VARCHAR(4)             					----控股  								BASS_STD_0005          
	--GROUP_TYPE不规范CHAR(1) 
	,GROUP_TYPE         		VARCHAR(2)             					----集团类型 							BASS_STD_0003       
	--MANAGE_STYLE不规范CHAR(1) 
	,MANAGE_STYLE       		VARCHAR(4)             					----经营形式      				BASS_STD_0004  
	,OPERATE_REVENUE_CLASS	CHAR(2)           						----营业收入分类 					BASS_STD_0006   
	,CAPITAL_CLASS      		CHAR(2)           						----资产分类     					BASS_STD_0007   
	,INDUSTRY_CLASS_CODE		CHAR(2)            	----行业分类编码 					BASS_STD1_0043   
	,CUST_STATUS        		CHAR(2)             					----集团客户状态    
	,CUST_INFO_SRC_ID   		CHAR(1)             					----集团客户资料来源
	,UPLOAD_TYPE_ID     		CHAR(1)            	----上传种类标识    
)
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,ENTERPRISE_ID) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY_MID
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
  
  
         select * from bass1.G_I_77780_DAY_MID   select ADDR_CODE , count(0) 
--,  count(distinct ADDR_CODE ) 
from bass1.G_I_77780_DAY_MID 
group by  ADDR_CODE 
order by 1 


select length(ADDR_CODE) , count(0) 
--,  count(distinct length(ADDR_CODE) ) 
from bass1.G_I_77780_DAY_MID 
group by  length(ADDR_CODE) 
order by 1 

drop table BASS1.G_I_77780_DAY
CREATE TABLE BASS1.G_I_77780_DAY
 (
	 TIME_ID            		INTEGER        NOT NULL       ----数据日期        
	,ENTERPRISE_ID      		CHAR(20)       					       ----集团客户标识    
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
   (ENTERPRISE_ID,ID) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE    
select length(REGION) , count(0) 
--,  count(distinct length(REGION) ) 
from bass1.G_I_77780_DAY_MID 
group by  length(REGION) 
order by 1 

select REGION , count(0) 
--,  count(distinct REGION ) 
from bass1.G_I_77780_DAY_MID 
group by  REGION 
order by 1 


select COUNTY , count(0) 
--,  count(distinct COUNTY ) 
from bass1.G_I_77780_DAY_MID 
group by  COUNTY 
order by 1 

select PHONE_NO1 , count(0) 
--,  count(distinct PHONE_NO1 ) 
from bass1.G_I_77780_DAY_MID 
group by  PHONE_NO1 
order by 1 
select PHONE_NO2 , count(0) 
--,  count(distinct PHONE_NO2 ) 
from bass1.G_I_77780_DAY_MID 
group by  PHONE_NO2 
order by 1 
select POST_CODE , count(0) 
--,  count(distinct POST_CODE ) 
from bass1.G_I_77780_DAY_MID 
group by  POST_CODE 
order by 1 


select INDUSTRY_TYPE , count(0) 
--,  count(distinct INDUSTRY_TYPE ) 
from bass1.G_I_77780_DAY_MID 
group by  INDUSTRY_TYPE 
order by 1 

select INDUSTRY_TYPE , count(0) 
--,  count(distinct INDUSTRY_TYPE ) 
from bass1.G_I_77780_DAY_MID 
where  length(INDUSTRY_TYPE)  = 6group by  INDUSTRY_TYPE 
order by 1 

select length(INDUSTRY_TYPE) , count(0) 
--,  count(distinct length(INDUSTRY_TYPE) ) 
from bass1.G_I_77780_DAY_MID 
group by  length(INDUSTRY_TYPE) 
order by 1 select length(trim(ECONOMIC_TYPE)) , count(0) 
--,  count(distinct length(ECONOMIC_TYPE) ) 
from bass1.G_I_77780_DAY_MID 
group by  length(trim(ECONOMIC_TYPE))
order by 1 select ECONOMIC_TYPE , count(0) 
--,  count(distinct ECONOMIC_TYPE ) 
from bass1.G_I_77780_DAY_MID --where length(trim(ECONOMIC_TYPE)) <> 3
group by  ECONOMIC_TYPE 
order by 1 
select ECONOMIC_TYPE , count(0) 
--,  count(distinct ECONOMIC_TYPE ) 
from bass1.G_I_77780_DAY_MID where length(trim(ECONOMIC_TYPE)) <> 3
group by  ECONOMIC_TYPE 
order by 1 

select OPEN_YEAR , count(0) 
--,  count(distinct OPEN_YEAR ) 
from bass1.G_I_77780_DAY_MID 
group by  OPEN_YEAR 
order by 1 

select length(OPEN_YEAR) , count(0) 
--,  count(distinct length(OPEN_YEAR) ) 
from bass1.G_I_77780_DAY_MID 
group by  length(OPEN_YEAR) 
order by 1 

select OPEN_YEAR , count(0) 
--,  count(distinct OPEN_YEAR ) 
from bass1.G_I_77780_DAY_MID where length(OPEN_YEAR)  <> 4 or OPEN_YEAR is null 
group by  OPEN_YEAR 
order by 1 
select length(OPEN_MONTH) , count(0) 
--,  count(distinct length(OPEN_MONTH) ) 
from bass1.G_I_77780_DAY_MID 
group by  length(OPEN_MONTH) 
order by 1 

select open_MONTH , count(0) 
--,  count(distinct open_MONTH ) 
from bass1.G_I_77780_DAY_MID 
where  length(OPEN_MONTH)  = 1group by  open_MONTH 
order by 1 
select SHAREHOLDER , count(0) 
--,  count(distinct SHAREHOLDER ) 
from bass1.G_I_77780_DAY_MID 
group by  SHAREHOLDER 
order by 1 

select MANAGE_STYLE , count(0) 
--,  count(distinct MANAGE_STYLE ) 
from bass1.G_I_77780_DAY_MID 
group by  MANAGE_STYLE 
order by 1 create view v_G_I_77780_DAY_MIDasselect 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
      ,case when ADDR_CODE is null or length(ADDR_CODE) <> 6 then '540102' else ADDR_CODE end ADDR_CODE
        ,CITY
        ,substr(REGION,1,20) REGION
        ,substr(COUNTY,1,20) COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,replace(PHONE_NO1,'-','') PHONE_NO1
        ,replace(PHONE_NO2,'-','') PHONE_NO2
        ,case when POST_CODE is null or length(POST_CODE) <> 6 then '850000' else POST_CODE end POST_CODE
        ,case when INDUSTRY_TYPE is null or length(INDUSTRY_TYPE) <> 4 then '9421' else INDUSTRY_TYPE end INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,case when ECONOMIC_TYPE is null or length(trim(ECONOMIC_TYPE)) <> 3 then '190' else trim(ECONOMIC_TYPE) end ECONOMIC_TYPE
        ,case when OPEN_YEAR is null then '2002' when  length(OPEN_YEAR) <> 4  then substr(OPEN_YEAR,1,4) else OPEN_YEAR end OPEN_YEAR
        ,case when length(OPEN_MONTH) =1 then '0'||trim(OPEN_MONTH) when length(OPEN_MONTH) <> 2 then '01' else OPEN_MONTH end OPEN_MONTH
        ,case when SHAREHOLDER not in ('1','2','3','4','5') then '9' else SHAREHOLDER end SHAREHOLDER
        ,GROUP_TYPE
        ,case when MANAGE_STYLE is null then '9' 
        	when length(MANAGE_STYLE) = 2 and  substr(MANAGE_STYLE,2,1) in ('1','2','3') then substr(MANAGE_STYLE,2,1)
        	when length(MANAGE_STYLE) = 1 and  MANAGE_STYLE in ('1','2','3') then MANAGE_STYLE 
      		else '9' end MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from bass1.G_I_77780_DAY_MID a

insert into bass1.G_I_77780_DAY
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
      ,case when ADDR_CODE is null or length(ADDR_CODE) <> 6 then '540102' else ADDR_CODE end ADDR_CODE
        ,CITY
        ,substr(REGION,1,20) REGION
        ,substr(COUNTY,1,20) COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,replace(PHONE_NO1,'-','') PHONE_NO1
        ,replace(PHONE_NO2,'-','') PHONE_NO2
        ,case when POST_CODE is null or length(POST_CODE) <> 6 then '850000' else POST_CODE end POST_CODE
        ,case when INDUSTRY_TYPE is null or length(INDUSTRY_TYPE) <> 4 then '9421' else INDUSTRY_TYPE end INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,case when ECONOMIC_TYPE is null or length(trim(ECONOMIC_TYPE)) <> 3 then '190' else trim(ECONOMIC_TYPE) end ECONOMIC_TYPE
        ,case when OPEN_YEAR is null then '2002' when  length(OPEN_YEAR) <> 4  then substr(OPEN_YEAR,1,4) else OPEN_YEAR end OPEN_YEAR
        ,case when length(OPEN_MONTH) =1 then '0'||trim(OPEN_MONTH) when length(OPEN_MONTH) <> 2 then '01' else OPEN_MONTH end OPEN_MONTH
        ,case when SHAREHOLDER is null then '9' 
        	when SHAREHOLDER not in ('1','2','3','4','5') then '9' else SHAREHOLDER end SHAREHOLDER
        ,case when GROUP_TYPE not in ('1','2') then '2' else GROUP_TYPE end GROUP_TYPE
        ,case when MANAGE_STYLE is null then '9' 
        	when length(MANAGE_STYLE) = 2 and  substr(MANAGE_STYLE,2,1) in ('1','2','3') then substr(MANAGE_STYLE,2,1)
        	when length(MANAGE_STYLE) = 1 and  MANAGE_STYLE in ('1','2','3') then MANAGE_STYLE 
      		else '9' end MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from bass1.G_I_77780_DAY_MID a

13908934887select SHAREHOLDER , count(0) 
--,  count(distinct SHAREHOLDER ) 
from bass1.v_G_I_77780_DAY_MID 
group by  SHAREHOLDER 
order by 1 
select GROUP_TYPE , count(0) 
--,  count(distinct GROUP_TYPE ) 
from bass1.v_G_I_77780_DAY_MID 
group by  GROUP_TYPE 
order by 1 select * from bass1.v_G_I_77780_DAY_MID  where GROUP_TYPE not in ('1','2')select * from G_I_77780_DAY_MIDwhere enterprise_id = '89302999648433'
insert into bass1.G_I_77780_DAY
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
      ,case when ADDR_CODE is null or length(ADDR_CODE) <> 6 then '540102' else ADDR_CODE end ADDR_CODE
        ,CITY
        ,substr(REGION,1,20) REGION
        ,substr(COUNTY,1,20) COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,replace(PHONE_NO1,'-','') PHONE_NO1
        ,substr(replace(PHONE_NO2,'-',''),1,10) PHONE_NO2
        ,case when POST_CODE is null or length(POST_CODE) <> 6 then '850000' else POST_CODE end POST_CODE
        ,case when INDUSTRY_TYPE is null or length(INDUSTRY_TYPE) <> 4 then '9421' else INDUSTRY_TYPE end INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,case when ECONOMIC_TYPE is null or length(trim(ECONOMIC_TYPE)) <> 3 then '190' else trim(ECONOMIC_TYPE) end ECONOMIC_TYPE
        ,case when OPEN_YEAR is null then '2002' when  length(OPEN_YEAR) <> 4  then substr(OPEN_YEAR,1,4) else OPEN_YEAR end OPEN_YEAR
        ,case when length(OPEN_MONTH) =1 then '0'||trim(OPEN_MONTH) when length(OPEN_MONTH) <> 2 then '01' else OPEN_MONTH end OPEN_MONTH
        ,case when SHAREHOLDER is null then '9' 
        	when SHAREHOLDER not in ('1','2','3','4','5') then '9' else SHAREHOLDER end SHAREHOLDER
        ,case when GROUP_TYPE not in ('1','2') then '2' else GROUP_TYPE end GROUP_TYPE
        ,case when MANAGE_STYLE is null then '9' 
        	when length(MANAGE_STYLE) = 2 and  substr(MANAGE_STYLE,2,1) in ('1','2','3') then substr(MANAGE_STYLE,2,1)
        	when length(MANAGE_STYLE) = 1 and  MANAGE_STYLE in ('1','2','3') then MANAGE_STYLE 
      		else '9' end MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from bass1.G_I_77780_DAY_MID a


select UPLOAD_TYPE_ID , count(0) 
--,  count(distinct UPLOAD_TYPE_ID ) 
from bass1.v_G_I_77780_DAY_MID 
group by  UPLOAD_TYPE_ID 
order by 1 

insert into bass1.G_I_77780_DAY
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
      ,case when ADDR_CODE is null or length(ADDR_CODE) <> 6 then '540102' else ADDR_CODE end ADDR_CODE
        ,CITY
        ,substr(REGION,1,20) REGION
        ,substr(COUNTY,1,20) COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,replace(PHONE_NO1,'-','') PHONE_NO1
        ,substr(replace(PHONE_NO2,'-',''),1,10) PHONE_NO2
        ,case when POST_CODE is null or length(POST_CODE) <> 6 then '850000' else POST_CODE end POST_CODE
        ,case when INDUSTRY_TYPE is null or length(INDUSTRY_TYPE) <> 4 then '9421' else INDUSTRY_TYPE end INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,case when ECONOMIC_TYPE is null or length(trim(ECONOMIC_TYPE)) <> 3 then '190' else trim(ECONOMIC_TYPE) end ECONOMIC_TYPE
        ,case when OPEN_YEAR is null then '2002' when  length(OPEN_YEAR) <> 4  then substr(OPEN_YEAR,1,4) else OPEN_YEAR end OPEN_YEAR
        ,case when length(OPEN_MONTH) =1 then '0'||trim(OPEN_MONTH) when length(OPEN_MONTH) <> 2 then '01' else OPEN_MONTH end OPEN_MONTH
        ,case when SHAREHOLDER is null then '9' 
        	when SHAREHOLDER not in ('1','2','3','4','5') then '9' else SHAREHOLDER end SHAREHOLDER
        ,case when GROUP_TYPE not in ('1','2') then '2' else GROUP_TYPE end GROUP_TYPE
        ,case when MANAGE_STYLE is null then '9' 
        	when length(MANAGE_STYLE) = 2 and  substr(MANAGE_STYLE,2,1) in ('1','2','3') then substr(MANAGE_STYLE,2,1)
        	when length(MANAGE_STYLE) = 1 and  MANAGE_STYLE in ('1','2','3') then MANAGE_STYLE 
      		else '9' end MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
      ,case when INDUSTRY_CLASS_CODE is null then '99' else  INDUSTRY_CLASS_CODE end INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
      ,case when UPLOAD_TYPE_ID is null then '3' else UPLOAD_TYPE_ID end UPLOAD_TYPE_ID
from bass1.G_I_77780_DAY_MID a



select INDUSTRY_CLASS_CODE , count(0) 
--,  count(distinct INDUSTRY_CLASS_CODE ) 
from bass1.v_G_I_77780_DAY_MID 
group by  INDUSTRY_CLASS_CODE 
order by 1 select * from  bass1.G_I_77780_DAY
select count(0),count(distinct value(ENTERPRISE_ID,'a')||value(id,'a')) 
from  bass1.G_I_77780_DAY
1006	786
select ENTERPRISE_ID||id , count(0)from bass1.G_I_77780_DAYgroup by  ENTERPRISE_ID||idhaving count(0) > 1select * from from bass1.G_I_77780_DAYwhere ENTERPRISE_ID||id  is null 89301560000178      71090731X	2
89301560000994      724901496	2
89302999434694      43320587X	2
select * from bass1.G_I_77780_DAY where ENTERPRISE_ID in ('89301560000178','89301560000994','89302999434694')create table bass1.G_I_77780_DAY_MID2 like bass1.G_I_77780_DAY
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,ID) USING HASHING        ALTER TABLE BASS1.G_I_77780_DAY_MID2
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
         
insert into bass1.G_I_77780_DAY_MID2
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
      ,case when ADDR_CODE is null or length(ADDR_CODE) <> 6 then '540102' else ADDR_CODE end ADDR_CODE
        ,CITY
        ,substr(REGION,1,20) REGION
        ,substr(COUNTY,1,20) COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,replace(PHONE_NO1,'-','') PHONE_NO1
        ,substr(replace(PHONE_NO2,'-',''),1,10) PHONE_NO2
        ,case when POST_CODE is null or length(POST_CODE) <> 6 then '850000' else POST_CODE end POST_CODE
        ,case when INDUSTRY_TYPE is null or length(INDUSTRY_TYPE) <> 4 then '9421' else INDUSTRY_TYPE end INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,case when ECONOMIC_TYPE is null or length(trim(ECONOMIC_TYPE)) <> 3 then '190' else trim(ECONOMIC_TYPE) end ECONOMIC_TYPE
        ,case when OPEN_YEAR is null then '2002' when  length(OPEN_YEAR) <> 4  then substr(OPEN_YEAR,1,4) else OPEN_YEAR end OPEN_YEAR
        ,case when length(OPEN_MONTH) =1 then '0'||trim(OPEN_MONTH) when length(OPEN_MONTH) <> 2 then '01' else OPEN_MONTH end OPEN_MONTH
        ,case when SHAREHOLDER is null then '9' 
        	when SHAREHOLDER not in ('1','2','3','4','5') then '9' else SHAREHOLDER end SHAREHOLDER
        ,case when GROUP_TYPE not in ('1','2') then '2' else GROUP_TYPE end GROUP_TYPE
        ,case when MANAGE_STYLE is null then '9' 
        	when length(MANAGE_STYLE) = 2 and  substr(MANAGE_STYLE,2,1) in ('1','2','3') then substr(MANAGE_STYLE,2,1)
        	when length(MANAGE_STYLE) = 1 and  MANAGE_STYLE in ('1','2','3') then MANAGE_STYLE 
      		else '9' end MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
      ,case when INDUSTRY_CLASS_CODE is null then '99'  else INDUSTRY_CLASS_CODE end INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
      ,case when UPLOAD_TYPE_ID is null then '3' else UPLOAD_TYPE_ID end UPLOAD_TYPE_ID
from bass1.G_I_77780_DAY_MID a
    select * from  BASS1.grp_id_old_new_map_20110330   
create table bass1.G_I_77780_DAY_MID3 like bass1.G_I_77780_DAY
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,ID) USING HASHING   
insert into bass1.G_I_77780_DAY_MID3
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from   (select a.* , row_number()over(PARTITION by ENTERPRISE_ID,id order by  UPLOAD_TYPE_ID asc ) rn 
		from  bass1.G_I_77780_DAY_MID2 a ) t where t.rn = 1 
		                               select UPLOAD_TYPE_ID , count(0) 
--,  count(distinct UPLOAD_TYPE_ID ) 
from bass1.G_I_77780_DAY_MID3 
group by  UPLOAD_TYPE_ID 
order by 1                                 
 select count(0)
 from bass1.G_I_77780_DAY_MID3 a ,
  BASS1.grp_id_old_new_map_20110330  b where a.ENTERPRISE_ID = b.old_enterprise_id
        select a.ENTERPRISE_ID , b.old_enterprise_id
 from bass1.G_I_77780_DAY_MID3 a ,
  BASS1.grp_id_old_new_map_20110330  b where a.ENTERPRISE_ID = b.old_enterprise_id
            select a.ENTERPRISE_ID , b.old_enterprise_id,b.new_enterprise_id
 from bass1.G_I_77780_DAY_MID3 a ,
  BASS1.grp_id_old_new_map_20110330  b where a.ENTERPRISE_ID = b.old_enterprise_id
               select a.ENTERPRISE_ID , b.old_enterprise_id,b.new_enterprise_id,a.ENTERPRISE_NAME,b.ENTERPRISE_NAME
 from bass1.G_I_77780_DAY_MID3 a ,
  BASS1.grp_id_old_new_map_20110330  b where a.ENTERPRISE_ID = b.old_enterprise_id
 and  a.ENTERPRISE_NAME<>b.ENTERPRISE_NAME       select a.ENTERPRISE_ID , b.old_enterprise_id,b.new_enterprise_id,a.ENTERPRISE_NAME,b.ENTERPRISE_NAME
 from bass1.G_I_77780_DAY_MID3 a ,
  BASS1.grp_id_old_new_map_20110330  b where a.ENTERPRISE_ID = b.old_enterprise_id
 and  a.ENTERPRISE_NAME<>b.ENTERPRISE_NAME    select count(0),count(distinct a.ENTERPRISE_ID) ,count(distinct  b.old_enterprise_id)    ,count(distinct b.new_enterprise_id)    --,a.ENTERPRISE_NAME,b.ENTERPRISE_NAME
 from bass1.G_I_77780_DAY_MID3 a ,
  BASS1.grp_id_old_new_map_20110330  b where a.ENTERPRISE_ID = b.old_enterprise_id  select count(0),count(distinct old_enterprise_id ) from      BASS1.grp_id_old_new_map_20110330       select count(0),count(distinct a.ENTERPRISE_ID ) from  bass1.G_I_77780_DAY_MID3 a    select ENTERPRISE_ID, count(0)from bass1.G_I_77780_DAY_MID3group by  ENTERPRISE_IDhaving count(0) > 1
select count(0),count(distinct value(ENTERPRISE_ID,'a')||value(id,'a')) 
from  bass1.G_I_77780_DAY_MID3
select * from bass1.G_I_77780_DAY_MID3 update bass1.G_I_77780_DAY_MID3 a
 set a.ENTERPRISE_ID = (select b.new_enterprise_id from  BASS1.grp_id_old_new_map_20110330  b 
 				where   a.ENTERPRISE_ID = b.old_enterprise_id )                delete from bass1.G_I_77780_DAY_MID3
insert into bass1.G_I_77780_DAY_MID3
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from   (select a.* , row_number()over(PARTITION by ENTERPRISE_ID,id order by  UPLOAD_TYPE_ID asc ) rn 
				from  bass1.G_I_77780_DAY_MID2 a
			 ) t where t.rn = 1 
		                                select a.*from  
 bass1.G_I_77780_DAY_MID3 a
 where  a.ENTERPRISE_ID = (select b.new_enterprise_id from  BASS1.grp_id_old_new_map_20110330 b 
 				where   a.ENTERPRISE_ID = b.old_enterprise_id )
 				                                
delete from bass1.G_I_77780_DAY_MID3
insert into bass1.G_I_77780_DAY_MID3
select 
         TIME_ID
        ,NEW_ENTERPRISE_ID
        ,ID
        ,NEW_ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from   (select a.* 
		,VALUE(b.new_enterprise_id,a.ENTERPRISE_ID) NEW_ENTERPRISE_ID
		,VALUE(b.ENTERPRISE_NAME,a.ENTERPRISE_NAME) NEW_ENTERPRISE_NAME
		, row_number()over(PARTITION by VALUE(b.new_enterprise_id,a.ENTERPRISE_ID),id order by  UPLOAD_TYPE_ID asc ) rn 
				from  bass1.G_I_77780_DAY_MID2 a
				left join  BASS1.grp_id_old_new_map_20110330  b on a.ENTERPRISE_ID = b.old_enterprise_id
			 ) t where t.rn = 1 
		                                 select count(0),count(distinct value(ENTERPRISE_ID,'a')||value(id,'a')) 
from  bass1.G_I_77780_DAY_MID3
                select UPLOAD_TYPE_ID , count(0) 
--,  count(distinct UPLOAD_TYPE_ID ) 
from bass1.G_I_77780_DAY_MID3 
group by  UPLOAD_TYPE_ID 
order by 1 
select 
         TIME_ID
        ,NEW_ENTERPRISE_ID
        ,ID
        ,NEW_ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from   (select a.* 
		,VALUE(b.new_enterprise_id,a.ENTERPRISE_ID) NEW_ENTERPRISE_ID
		,VALUE(b.ENTERPRISE_NAME,a.ENTERPRISE_NAME) NEW_ENTERPRISE_NAME
		, row_number()over(PARTITION by VALUE(b.new_enterprise_id,a.ENTERPRISE_ID),id order by  UPLOAD_TYPE_ID asc ) rn 
				from  bass1.G_I_77780_DAY_MID2 a
				left join  BASS1.grp_id_old_new_map_20110330  b on a.ENTERPRISE_ID = b.old_enterprise_id
			 ) t where           t.new_enterprise_id is null    
		                select count(0)				from  bass1.G_I_77780_DAY_MID2 a
				left join  BASS1.grp_id_old_new_map_20110330  b on a.ENTERPRISE_ID = b.old_enterprise_id                                select * from bass1.G_I_77780_DAY_MID3                                select * from BASS1.grp_id_old_new_map_20110330                                 select count(0),count(distinct value(ENTERPRISE_ID,'a')||value(id,'a')) 
from  bass1.G_I_77780_DAY_MID3

 
delete from bass1.G_I_77780_DAY_MID3
insert into bass1.G_I_77780_DAY_MID3
select 
         TIME_ID
        ,NEW_ENTERPRISE_ID
        ,ID
        ,NEW_ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from   (select a.* 
		,VALUE(b.new_enterprise_id,a.ENTERPRISE_ID) NEW_ENTERPRISE_ID
		,VALUE(b.ENTERPRISE_NAME,a.ENTERPRISE_NAME) NEW_ENTERPRISE_NAME
		, row_number()over(PARTITION by ENTERPRISE_ID,id order by  UPLOAD_TYPE_ID asc ) rn 
				from  bass1.G_I_77780_DAY_MID2 a
				left join  BASS1.grp_id_old_new_map_20110330  b on a.ENTERPRISE_ID = b.old_enterprise_id
			 ) t where t.rn = 1 
		        select * from G_I_77780_DAY_MID3where ENTERPRISE_ID in ('89402999933152'
,'89401560000324'
,'89401560000324'
,'89402999933152'
,'89401560000324'
,'89403000381097'
)                select * from G_I_77780_DAY_MID2where id in (   select id     from  G_I_77780_DAY_MID3    where ENTERPRISE_ID in ('89402999933152'
                            ,'89401560000324'
                            ,'89401560000324'
                            ,'89402999933152'
                            ,'89401560000324'
                            ,'89403000381097'
                            )                ) select value(ENTERPRISE_ID,'a')||value(id,'a')from G_I_77780_DAY_MID3group by value(ENTERPRISE_ID,'a')||value(id,'a')having count(0) > 1)              1
89402999933152      724904515
89401560000324      710905728
89401560000324      DX093350X
89402999933152      741930176
89401560000324      DX0933489
89403000381097      DX0933251
select * from   bass1.G_I_77780_DAY_MID2create table bass1.G_I_77780_DAY_MID4 like bass1.G_I_77780_DAY
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,ID) USING HASHING      select * from     bass1.G_I_77780_DAY_MID4        insert into     bass1.G_I_77780_DAY_MID4
select * from bass1.G_I_77780_DAY_MID3
where ENTERPRISE_ID in ('89402999933152'
,'89401560000324'
,'89401560000324'
,'89402999933152'
,'89401560000324'
,'89403000381097'
)        

select UPLOAD_TYPE_ID , count(0) 
--,  count(distinct UPLOAD_TYPE_ID ) 
from bass1.G_I_77780_DAY_MID3 
group by  UPLOAD_TYPE_ID 
order by 1 
1	242 +4
2	243 +3
3	505

delete from  bass1.G_I_77780_DAY_MID3
where ENTERPRISE_ID in ('89402999933152'
,'89401560000324'
,'89401560000324'
,'89402999933152'
,'89401560000324'
,'89403000381097'
)    
		select distinct ENTERPRISE_ID,id ,UPLOAD_TYPE_ID from bass1.G_I_77780_DAY_MID4
where ENTERPRISE_ID in ('89402999933152'
,'89401560000324'
,'89401560000324'
,'89402999933152'
,'89401560000324'
,'89403000381097'
)    select * from (select a.*,row_number()over(partition by ENTERPRISE_ID,id ) rn from  bass1.G_I_77780_DAY_MID4 a )t where rn = 1 		        
insert into     bass1.G_I_77780_DAY_MID3
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
 from 
(
select a.*,row_number()over(partition by ENTERPRISE_ID,id ) rn 
from  bass1.G_I_77780_DAY_MID4 a )
t where rn = 1 

		        --主键校验  select count(0),count(distinct value(ENTERPRISE_ID,'a')||value(id,'a')) 
from  bass1.G_I_77780_DAY_MID3
997	997
ok                                                select count(0)
from 
(select distinct ORG_TYPE ORG_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ORG_TYPE  not in (select code from BASS1.dim_bass1_std_map where interface_code = '77780' and dim_table_id ='BASS_STD1_0001')

select *
from 
(select distinct ORG_TYPE ORG_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ORG_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0001')

                
select ORG_TYPE , count(0) 
--,  count(distinct ORG_TYPE ) 
from bass1.G_I_77780_DAY_MID3 
group by  ORG_TYPE 
order by 1 
                
update  bass1.G_I_77780_DAY_MID3
set ORG_TYPE = '51'
where ORG_TYPE = '5'

                                select INDUSTRY_TYPE , count(0) 
--,  count(distinct INDUSTRY_TYPE ) 
from bass1.G_I_77780_DAY_MID3 
group by  INDUSTRY_TYPE 
order by 1 
                                
drop table BASS1.t_bass1_std_0113
CREATE TABLE BASS1.t_bass1_std_0113
 (
	 code    char(4)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (code) USING HASHING                 select count(0)
from 
(select distinct trim(INDUSTRY_TYPE) INDUSTRY_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_TYPE  not in 
(select code from BASS1.t_bass1_std_0113 )                                
select EMPLOYEE_CNT , count(0) 
--,  count(distinct EMPLOYEE_CNT ) 
from bass1.G_I_77780_DAY_MID3 
group by  EMPLOYEE_CNT 
order by 1 
                               select int(EMPLOYEE_CNT)+ 0 from bass1.G_I_77780_DAY_MID3                update  bass1.G_I_77780_DAY_MID3
set EMPLOYEE_CNT = '0'
where EMPLOYEE_CNT is null 
select *
from 
(select distinct trim(ECONOMIC_TYPE) ECONOMIC_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ECONOMIC_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0002')
100
101
150
170
110
select ECONOMIC_TYPE , count(0) 
--,  count(distinct EMPLOYEE_CNT ) 
from bass1.G_I_77780_DAY_MID3 
group by  ECONOMIC_TYPE 
order by 1 
select *  from  BASS1.dim_bass1_std_map update G_I_77780_DAY_MID3 
set ECONOMIC_TYPE = '110'
where ECONOMIC_TYPE in 
('100'
,'101'
,'150'
,'170'
)
select count(0)
from 
(select distinct SHAREHOLDER SHAREHOLDER from   bass1.G_I_77780_DAY_MID3) a 
where SHAREHOLDER  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0005')

select count(0)
from 
(select distinct GROUP_TYPE GROUP_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where GROUP_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0003')

select count(0)
from 
(select distinct MANAGE_STYLE MANAGE_STYLE from   bass1.G_I_77780_DAY_MID3) a 
where MANAGE_STYLE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0004')
select count(0)
from 
(select distinct OPERATE_REVENUE_CLASS OPERATE_REVENUE_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where OPERATE_REVENUE_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0006')



update G_I_77780_DAY_MID3 
set OPERATE_REVENUE_CLASS = '0'||OPERATE_REVENUE_CLASS
where length(trim(OPERATE_REVENUE_CLASS)) = 1


update G_I_77780_DAY_MID3 
set OPERATE_REVENUE_CLASS = '10'
where OPERATE_REVENUE_CLASS is null 
select count(0)
from 
(select distinct OPERATE_REVENUE_CLASS OPERATE_REVENUE_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where OPERATE_REVENUE_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0006')

select *
from 
(select distinct CAPITAL_CLASS CAPITAL_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where CAPITAL_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')




update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '0'||trim(CAPITAL_CLASS)
where length(trim(CAPITAL_CLASS)) = 1


update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '05'
where CAPITAL_CLASS is null 

update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '01'
where CAPITAL_CLASS = '00'


SELECT * FROM  BASS1.dim_bass1_std_map select distinct CAPITAL_CLASS CAPITAL_CLASS from   bass1.G_I_77780_DAY_MID3select distinct UPLOAD_TYPE_ID UPLOAD_TYPE_ID from   bass1.G_I_77780_DAY_MID3

update G_I_77780_DAY_MID3 
set INDUSTRY_CLASS_CODE = '0'||TRIM(INDUSTRY_CLASS_CODE)
where length(trim(INDUSTRY_CLASS_CODE)) = 1


update G_I_77780_DAY_MID3 
set CUST_STATUS = '36'
where  CUST_STATUS IS NULL 


select CUST_INFO_SRC_ID , count(0) 
--,  count(distinct CUST_INFO_SRC_ID ) 
from bass1.G_I_77780_DAY_MID3 
group by  CUST_INFO_SRC_ID 
order by 1 

update G_I_77780_DAY_MID3 
set CUST_INFO_SRC_ID = '1'
where  CUST_INFO_SRC_ID IS NULL 

select distinct  from    select count(0)
from 
(select distinct trim(INDUSTRY_TYPE) INDUSTRY_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_TYPE  not in 
(select code from BASS1.t_bass1_std_0113 )
delete from BASS1.G_I_77780_DAY
insert into BASS1.G_I_77780_DAY
select * from bass1.G_I_77780_DAY_MID3select upload_type_id,count(0)from BASS1.G_I_77780_DAYgroup by upload_type_idselect * from  BASS1.G_I_77780_DAY
drop table BASS1.G_I_77780_DAY
CREATE TABLE BASS1.G_I_77780_DAY
 (
	 TIME_ID            		INTEGER        NOT NULL       ----数据日期        
	,ENTERPRISE_ID      		CHAR(20)       					       ----集团客户标识    
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
   (ENTERPRISE_ID,ID) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE          select count(0) from BASS1.G_I_77780_DAY    select * from app.g_unit_info where unit_code='77780'
select * from   app.sch_control_task          CONTROL_CODE
        ,DEAL_TIME
        ,CMD_TYPE
        ,CMD_LINE
        ,PRIORITY_VAL
        ,TIME_VALUE
        ,FUNCTION_DESC
        ,CC_GROUP_CODE
        ,MO_GROUP_CODE
        ,CC_FLAG
        ,APP_DIR        select * from   app.sch_control_task where control_code like 'BASS1%DAY.tcl'BASS1_EXP_G_A_02008_DAY	1	2	bass1_export bass1.g_a_02008_day YESTERDAY()	10000	2	02008用户状态抽取	app	BASS1	1	/bassapp/backapp/bin/bass1_export/
delete from  app.sch_control_task  where control_code = 'BASS1_EXP_G_I_77780_DAY'select * from   app.sch_control_task delete from app.sch_control_before where control_code = 'BASS1_EXP_G_I_77780_DAY'insert into app.sch_control_before values ('BASS1_EXP_G_I_77780_DAY','BASS1_EXP_G_S_04008_DAY')delete from  app.sch_control_task where control_code = 'BASS1_EXP_G_I_77780_DAY'insert into app.sch_control_task values ('BASS1_EXP_G_I_77780_DAY',1,2,'bass1_export bass1.g_i_77780_day YESTERDAY()',0,-1,'重要集团客户拍照置换清单','app','BASS1',1,'/bassapp/backapp/bin/bass1_export/')
update  app.sch_control_taskset cc_flag = 2where control_code = 'BASS1_EXP_G_I_77780_DAY'delete from app.g_unit_info where unit_code='77780'
insert into app.g_unit_info values ('77780',0,'重要集团客户拍照置换清单','bass1.g_i_77780_day',1,0,0)
select * from   app.sch_control_map where control_code like '%7778%'select * from   bass1.int_program_data where program_name like '%7778%'select * from   app.sch_control_before where control_code like '%7778%'select ENTERPRISE_ID||ID||ENTERPRISE_NAME||ORG_TYPE||ADDR_CODE||CITY||REGION||COUNTY||DOOR_NO||AREA_CODE||PHONE_NO1||PHONE_NO2||POST_CODE||INDUSTRY_TYPE||EMPLOYEE_CNT||INDUSTRY_UNIT_CNT||ECONOMIC_TYPE||OPEN_YEAR||OPEN_MONTH||SHAREHOLDER||GROUP_TYPE||MANAGE_STYLE||OPERATE_REVENUE_CLASS||CAPITAL_CLASS||INDUSTRY_CLASS_CODE||CUST_STATUS||CUST_INFO_SRC_ID||UPLOAD_TYPE_ID from bass1.g_i_77780_day where time_id=20101231select *  from bass1.g_i_77780_day where time_id=20101231select ENTERPRISE_ID||ID||ENTERPRISE_NAME||ORG_TYPE||ADDR_CODE||CITY||REGION||COUNTY||DOOR_NO||AREA_CODE||PHONE_NO1||PHONE_NO2||POST_CODE||INDUSTRY_TYPE||EMPLOYEE_CNT||INDUSTRY_UNIT_CNT||ECONOMIC_TYPE||OPEN_YEAR||OPEN_MONTH||SHAREHOLDER||GROUP_TYPE||MANAGE_STYLE||OPERATE_REVENUE_CLASS||CAPITAL_CLASS||INDUSTRY_CLASS_CODE||CUST_STATUS||CUST_INFO_SRC_ID||UPLOAD_TYPE_ID from bass1.g_i_77780_day where time_id=20101231select * from g_i_77778_day
update BASS1.G_I_77780_DAY
set ENTERPRISE_ID = ' '
where ENTERPRISE_ID is null 


update BASS1.G_I_77780_DAY
set ID = ' '
where ID is null 


update BASS1.G_I_77780_DAY
set ENTERPRISE_NAME = ' '
where ENTERPRISE_NAME is null 


update BASS1.G_I_77780_DAY
set ORG_TYPE = ' '
where ORG_TYPE is null 



update BASS1.G_I_77780_DAY
set CITY = ' '
where CITY is null 


update BASS1.G_I_77780_DAY
set REGION = ' '
where REGION is null 


update BASS1.G_I_77780_DAY
set COUNTY = ' '
where COUNTY is null 


update BASS1.G_I_77780_DAY
set DOOR_NO = ' '
where DOOR_NO is null 


update BASS1.G_I_77780_DAY
set AREA_CODE = ' '
where AREA_CODE is null 


update BASS1.G_I_77780_DAY
set PHONE_NO1 = ' '
where  PHONE_NO1 is null 


update BASS1.G_I_77780_DAY
set PHONE_NO2 = ' '
where PHONE_NO2 is null 


update BASS1.G_I_77780_DAY
set POST_CODE = ' '
where POST_CODE is null 



update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = ' '
where INDUSTRY_TYPE is null 


update BASS1.G_I_77780_DAY
set EMPLOYEE_CNT = ' '
where EMPLOYEE_CNT is null 


update BASS1.G_I_77780_DAY
set INDUSTRY_UNIT_CNT = ' '
where INDUSTRY_UNIT_CNT is null 


update BASS1.G_I_77780_DAY
set ECONOMIC_TYPE = ' '
where ECONOMIC_TYPE is null 


update BASS1.G_I_77780_DAY
set OPEN_YEAR = ' '
where OPEN_YEAR is null 


update BASS1.G_I_77780_DAY
set OPEN_MONTH = ' '
where OPEN_MONTH  is null 


update BASS1.G_I_77780_DAY
set SHAREHOLDER = ' '
where SHAREHOLDER is null 


update BASS1.G_I_77780_DAY
set GROUP_TYPE = ' '
where GROUP_TYPE is null 



update BASS1.G_I_77780_DAY
set MANAGE_STYLE = ' '
where MANAGE_STYLE is null 


update BASS1.G_I_77780_DAY
set OPERATE_REVENUE_CLASS = ' '
where OPERATE_REVENUE_CLASS is null 


update BASS1.G_I_77780_DAY
set CAPITAL_CLASS = ' '
where CAPITAL_CLASS is null 


update BASS1.G_I_77780_DAY
set INDUSTRY_CLASS_CODE = ' '
where INDUSTRY_CLASS_CODE is null 



update BASS1.G_I_77780_DAY
set CUST_STATUS = ' '
where CUST_STATUS is null 


update BASS1.G_I_77780_DAY
set CUST_INFO_SRC_ID = ' '
where CUST_INFO_SRC_ID is null 


update BASS1.G_I_77780_DAY
set UPLOAD_TYPE_ID = ' '
where UPLOAD_TYPE_ID is null 

select * from    BASS1.G_I_77780_DAY select count(0),count(distinct value(ENTERPRISE_ID,'a')||value(id,'a')) 
from   BASS1.G_I_77780_DAY
select * from app.g_runlog 
where time_id=20101231
and return_flag=0
select count(0)
from 
(select distinct CAPITAL_CLASS CAPITAL_CLASS from    BASS1.G_I_77780_DAY) a 
where CAPITAL_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')


select INDUSTRY_TYPE from    BASS1.G_I_77780_DAYwhere enterprise_idin ('89103000135199'
,'89108911013693'
,'89103001215411'
,'89101560001610'
,'89100000003719'
)2050
3000
7214
 select distinct CUST_STATUS CUST_STATUS from   BASS1.G_I_77780_DAY 
CUST_STATUS
10
12
20
30
36select * from 
update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '7210'
where INDUSTRY_TYPE = '7214'

select CUST_STATUS , count(0) 
--,  count(distinct CUST_STATUS ) 
from BASS1.G_I_77780_DAY 
group by  CUST_STATUS 
order by 1 
10	47
12	7
20	842
30	72
36	29
10-潜在
 11-离网
 12-未入网
20-在网
30-无效集团
 31-已破产
 32-被兼并
 33-企业迁移外地
 34-已结业
 35-个体户、聚类集团
 36-其他 update BASS1.G_I_77780_DAY
set CUST_STATUS = '11'
where CUST_STATUS = '10'
update BASS1.G_I_77780_DAY
set CUST_STATUS = '31'
where CUST_STATUS = '30'
select * from  BASS1.t_bass1_std_0113where code = '9011'
 select count(0)
from 
(select distinct trim(INDUSTRY_TYPE) INDUSTRY_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_TYPE  not in 
(select code from BASS1.t_bass1_std_0113 )


update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '9010'
where INDUSTRY_TYPE = '9011'

8910300013519989108911013693891030012154118910156000161089100000003719
update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '3010'
where INDUSTRY_TYPE = '3000'


update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '2040'
where INDUSTRY_TYPE = '2050'


select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
--and flag = -1
and control_code not  like 'BASS1%'
order by alarmtime desc 
select * from   BASS1.G_S_04002_DAY_20100501BAKselect * from   app.sch_control_before where control_code = 'BASS1_G_S_04015_DAY.tcl'BASS1_G_S_04015_DAY.tcl	TR1_L_A98012
select * from   app.sch_control_task where control_code = 'TR1_L_A98012'DWD_MR_OPER_CDR_YYYYMMDDselect * from  BASS2.DWD_MR_OPER_CDR_20110405 where op_time = '20110405'select * from   app.sch_control_runlog where control_code = 'BASS1_G_S_04015_DAY.tcl'TR1_L_A98012	2011-03-30 6:35:13.182254	2011-03-30 6:35:50.654050	37	0
select * from   app.sch_control_before where control_code = 'TR1_L_A98012'select * from   app.sch_control_task where control_code = 'TR1_L_A98012'TR1_L_A98012	1	2	DWD_MR_OPER_CDR_YYYYMMDD	0	-1	彩铃业务话单信息	-	TR1_MR	2	-
select * from   app.sch_control_runlog where control_code like 'TR1_%98%'select hour(begintime),count(0) from   app.sch_control_runlog where flag = 0 and date(begintime) = '2011-04-01 ' group by hour(begintime) select * from   syscat.tables where tabname like '%DWD_MR_OPER_CDR%'select * from app.sch_control_task where 
control_code in (select  control_code from   app.sch_control_runlog where flag= 1)update app.sch_control_taskset priority_val = 0
 where control_code = 'TR1_L_A98012'   select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabschema = 'BASS1'

 select sum(decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2)) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabschema = 'BASS1'
select * from   app.sch_control_task where control_code like '%G_S_22204_MONTH%'select * from   bass1.G_S_04002_DAY_20100501bakselect * from   bass1.G_S_04018_DAY_20100501bak    		select product_no,imei,count(*) from 
		(
			select product_no,imei from bass1.G_S_04002_DAY
			where time_id between 20110101 and 20110301
		) a
		group by product_no,imei                RENAME TABLE BASS2.DIM_TERM_TAC TO DIM_TERM_TAC_20110331BAK
CREATE TABLE BASS2.DIM_TERM_TAC
 (ID             INTEGER,
  TAC_NUM        VARCHAR(15),
  TERM_ID        VARCHAR(10),
  TERM_MODEL     VARCHAR(50),
  TERMPROD_ID    VARCHAR(10),
  TERMPROD_NAME  VARCHAR(200),
  NET_TYPE       CHARACTER(1),
  TERM_TYPE      CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_DIM
  PARTITIONING KEY
   (ID
   ) USING HASHING
ALTER TABLE BASS2.DIM_TERM_TAC
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
       select * from   bass2.dim_term_tac              

CREATE TABLE BASS2.DIM_TERM_TAC_0331
 (ID             INTEGER,
  TAC_NUM        VARCHAR(15),
  TERM_ID        VARCHAR(10),
  TERM_MODEL     VARCHAR(50),
  TERMPROD_ID    VARCHAR(10),
  TERMPROD_NAME  VARCHAR(200),
  NET_TYPE       CHARACTER(1),
  TERM_TYPE      CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_DIM
  PARTITIONING KEY
   (ID
   ) USING HASHING
ALTER TABLE BASS2.DIM_TERM_TAC_0331
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
  select * from   bass2.DIM_TERM_TACexcept select * from   bass2.DIM_TERM_TAC_0331  insert into BASS2.DIM_TERM_TAC
select 
ID,
TAC_NUM,
TERM_ID,
TERM_MODEL,
TERMPROD_ID,
TERMPROD_NAME,
NET_TYPE,
TERM_TYPE from BASS2.DIM_TERM_TAC_20110331BAK
where net_type <>'2'       select count(0) ,count(distinct TAC_NUM) from BASS2.DIM_TERM_TAC_20110331BAK       select count(0) ,count(distinct TAC_NUM) from  BASS2.DIM_TERM_TAC27571	27571
select tac_nuM,count(*) from BASS2.DIM_TERM_TAC_20110331BAK
group by tac_nuM
having count(*)>1       select tac_nuM,count(*) from BASS2.DIM_TERM_TAC_20110331BAK
group by tac_nuM
having count(*)>1       drop table BASS2.DIM_TERM_TAC_0331
 
delete from BASS2.DIM_TERM_TAC 
insert into BASS2.DIM_TERM_TAC
select * from BASS2.DIM_TERM_TAC_0331
 insert into BASS2.DIM_TERM_TAC
select 
ID,
TAC_NUM,
TERM_ID,
TERM_MODEL,
TERMPROD_ID,
TERMPROD_NAME,
NET_TYPE,
TERM_TYPE from BASS2.DIM_TERM_TAC_20110331BAK
where net_type <>'2'       select count(0)from (		select product_no,imei,count(*) 
		from bass1.G_S_04002_DAY
		where time_id between  20110101 and 20110331
		  and mns_type='1'
		group by product_no,imei) t4.02select count(0)from (select product_no,imei,count(*) from 
(
select product_no,imei from bass1.G_S_04002_DAY
where time_id between  20110101 and 20110331
  and mns_type='1'
) a
group by product_no,imei) t 3.05       select * from   syscat.tables where tabname like '%EXPLAIN%'       select distinct intra_product_no,product_no 
		  from bass1.G_S_04018_DAY a
		  where time_id between $last_last_month_day and $this_month_last_day          select * from   app.sch_control_before where before_control_code like '%G_S_22204_MONTH%'          select * from   bass1.g_s_22204_monthselect * from    bass1.td_check_user_flow                select 
                sum(bigint(flows))
                from bass1.g_s_04002_day_flows
                   select * from   bass1.g_s_22204_monthselect * from   bass1.g_s_04002_day_td                         select product_no,imei,count(0) 
                from bass1.G_S_04002_DAY a
                where time_id between 20101201 and 20110228
                group by product_no,imei
          select time_id , count(0) 
--,  count(distinct time_id ) 
from bass1.g_s_22204_month 
group by  time_id 
order by 1           select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
--and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc 
                    select * from   bass1.td_check_user_status_ls          select * from    bass1.g_s_22204_month_tmp3          select * from   bass1.g_s_22204_month_tmp3 select * from  bass1.G_I_77778_DAY create nickname xzbass1.sch_control_map 
for db25.app.sch_control_mapselect * from   app.sch_control_map insert into app.sch_control_map select * from xzbass1.sch_control_map where lower(control_code) like '%report_key_index_month%' drop nickname xzbass1.sch_control_map create nickname xzbass1.sch_control_task 
for db25.app.sch_control_taskinsert into app.sch_control_task select * from xzbass1.sch_control_task where lower(control_code) like '%report_key_index_month%' drop nickname xzbass1.sch_control_task create nickname xzbass1.int_program_data 
for db25.bass1.int_program_dataselect * from  xzbass1.int_program_data where lower(program_name) like '%report_key_index_month%'  insert into bass1.int_program_data select * from xzbass1.int_program_data where lower(control_code) like '%report_key_index_month%' drop nickname xzbass1.int_program_data bass1.int_program_data          select * from    bass1.usys_int_control                     	      SELECT 
	        case 
	          when para_value='0' then char(current date - 1 days)
	          else para_value
	        end
	      FROM bass1.usys_int_control 
	      WHERE para_name='op_time'select char(count(distinct comp_product_no)) c41
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)
 select time_id,
TEL_MOBILE_NEW_ADD_CNT,
UNION_MOBILE_NEW_ADD_CNT
from 
bass1.G_S_22073_DAY
WHERE time_id >= 20110324 select * from g_rule_check where rule_code in ('R159_1') and time_id >=20110324

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





select * from   APP.G_rule_check

select * from   APP.G_UNIT_INFO


select * from 
bass1.g_rule_check where rule_code = 'R159_4'
and time_id = 20110322


select * from 
bass1.g_rule_check where rule_code = 'R161_15'
where time_id = 20110317

select * from   app.sch_control_task where lower(cmd_line) like '%net%hlr%'

select * from   app.sch_control_runlog where control_code like '%06031%'

select count(0) from   BASS2.DIM_TACNUM_DEVID 
select count(0) from   bass1.G_A_02059_DAY_down20110321

select * from   G_A_02055_DAY


select * from   BASS2.DIM_CONTROL_INFO

select * from   BASS2.DIM_PROPERTY_VALUE_RANGE

select * from   BASS2.DIM_PROPERTY_INFO

select * from   BASS2.DIM_DEVICE_PROFILE

select * from   BASS2.DIM_DEVICE_INFO

select * from   BASS2.DIM_CONTROL_INFO

select * from   BASS2.DIM_TACNUM_DEVID

select count(0) from    BASS2.DIM_TERM_TAC

select * from   BASS2.DIM_TERM_TAC 
          create nickname xzbass1.G_S_04008_DAY 
for db25.bass1.G_S_04008_DAYinsert into bass1.G_S_04008_DAYselect * from  xzbass1.G_S_04008_DAY  where time_id = 20110331 --drop nickname xzbass1.sch_control_task 
CREATE TABLE "BASS1   "."G_S_04008_DAY0331"  (
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
		   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  

ALTER TABLE "BASS1   "."G_S_04008_DAY0331" APPEND ON

ALTER TABLE "BASS1   "."G_S_04008_DAY0331" LOCKSIZE TABLE

insert into  bass1.G_S_04008_DAY0331select * from   bass1.G_S_04008_DAYwhere time_id = 20110331 select * from  BASS1.G_S_04008_DAY where time_id = 20110331 select time_id,count(0)from    BASS1.G_S_04008_DAY0331group by     time_id  20110331	94470
 select * from   bass1.G_RULE_CHECK where 
rule_code in ('R107','R108')   select * from  syscat.tables where tabname like 'SCH_CONTROL_RUN%'  select * from  app.SCH_CONTROL_RUNLOG_BAKNGGJ where control_code = 'BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl'  select * from  bass1.G_RULE_CHECK   WHERE TIME_ID= 20110331
  AND RULE_CODE IN ('R107','R108')  select * from   syscat.indexes where tabschema = 'BASS2'drop table "BASS1"."DUAL"CREATE TABLE "BASS1"."DUAL" like sysibm.SYSdummy1 
 IN "TBS_APP_BASS1"

ALTER TABLE "BASS1"."DUAL"
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
select 2 from bass2.dualunion allselect 'a' from "BASS1"."DUAL"select * from   syscat.tables where tabname like '%DUMM%'select * from     BASS2.ETL_SEND_MESSAGEselect a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mbfrom syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspaceand a.tabschema = 'BASS1'select * from   G_S_04002_DAY fetch 10 rows onlyselect * from G_S_04002_DAY fetch first 10 rows only  create nickname xzbass1.G_S_04002_DAY 
for db25.bass1.G_S_04002_DAYinsert into bass1.G_S_04002_DAY select * from  xzbass1.G_S_04002_DAY  drop nickname xzbass1.G_S_04002_DAY select time_id,count(0) from   bass1.G_S_04002_DAY group by time_idselect * from   bass1.T_GS05001M
BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl	int -s INT_CHECK_SAMPLE_TO_DAY.tcl	2	扣分性指标R108(原R139)超出集团考核范围	2011-04-01 4:32:33.235691	[NULL]	-1	[NULL]
BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl	int -s INT_CHECK_SAMPLE_TO_DAY.tcl	2	扣分性指标R107(原R138)超出集团考核范围	2011-04-01 4:20:19.310685	[NULL]	-1	[NULL]


select * from   bass1.G_RULE_CHECK where 
rule_code in ('R107')


select time_id,count(0),count(distinct user_id) from    BASS1.g_user_lst
group by time_id

201104	25702	25702
201103	24776	24776
201102	24247	24247

db2 runstats on table BASS1.g_user_lst with distribution and detailed indexes all
select * from   syscat.tables where tabname = 'G_USER_LST'


select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mbfrom syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspaceand a.tabschema = 'BASS1'


select time_id,count(0) 
from   G_S_05002_MONTH
group by time_id

select * from   app.sch_control_before where control_code like '%05002%'select  * from APP.G_FILE_REPORT
where filename like '%_201103_%' and err_code='00'CREATE TABLE BASS1.DIM_NOT_NULL_INTERFACE
 (
 	interface_code varchar(5)
 	,interface_name varchar(128)
 )
  DATA CAPTURE NONE
		 DISTRIBUTE BY HASH(interface_code)   
		   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  
ALTER TABLE BASS1.DIM_NOT_NULL_INTERFACE
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
      select * from  BASS1.DIM_NOT_NULL_INTERFACE  where interface_code   in
('01006'
,'01007'
,'02013'
,'02055'
,'02056'
,'02057'
,'02058'
,'02060'
,'02061'
,'02064'
,'06001'
,'04009'
,'04010'
,'04012'
,'21004'
,'21005'
,'21009'
,'21016')  select * from  BASS1.DIM_NOT_NULL_INTERFACE
  where interface_code 
  in
('06001'
,'02017'
,'06002'
,'22401'
,'22403')
select count(0),count(distinct interface_code ) from    BASS1.DIM_NOT_NULL_INTERFACEselect * from   app.sch_control_before a,app.sch_control_task b  where a.CONTROL_CODE = b.CONTROL_CODE and b.CC_FLAG = 1 and before_control_code like '%Dw_product_ds.tcl%'and a.control_code like 'BASS1%DAY%'select * from   app.sch_control_runlog where control_code like '%INT_CHECK_INDEX_SAME_DA%'BASS1_INT_CHECK_INDEX_SAME_DAY.tcl	2011-04-02 5:06:55.734633	2011-04-02 5:08:23.158191	87	1
select * from   app.sch_control_runlog where flag  = 1select * from   G_A_02062_DAY where time_id = 20110401select * from   app.sch_control_task where control_code like '%04013%'update app.sch_control_runlog
set flag = 0  
 where control_code in 
(
'BASS1_G_I_06031_DAY.tcl'
,'BASS1_G_I_06032_DAY.tcl'
,'BASS1_EXP_G_I_06032_DAY'
,'BASS1_EXP_G_I_06031_DAY'
)select * from app.sch_control_runlog where control_code in 
(
'BASS1_G_I_06031_DAY.tcl'
,'BASS1_G_I_06032_DAY.tcl'
,'BASS1_EXP_G_I_06032_DAY'
,'BASS1_EXP_G_I_06031_DAY'
)select control_code from   app.sch_control_before where control_code = 'BASS2_Dw_acct_should_dtl_today_ds.tcl'select control_code from   app.sch_control_before where control_code = 'BASS2_Dw_acct_should_dtl_today_ds.tcl'select control_code from   app.sch_control_before where control_code in (select control_code from   app.sch_control_before where before_control_code = 'BASS2_Dw_acct_should_dtl_today_ds.tcl')select distinct control_code from   app.sch_control_before where control_code in (select control_code from   app.sch_control_before where before_control_code in (select control_code from   app.sch_control_before where before_control_code in (select control_code from   app.sch_control_before where before_control_code = 'BASS2_Dw_acct_should_dtl_today_ds.tcl')))BASS1_G_A_02059_DAY.tcl
BASS1_G_S_22301_DAY.tcl
BASS1_G_S_22302_DAY.tcl
select distinct  control_code from   app.sch_control_before where control_code in (select  control_code from   app.sch_control_before where before_control_code in (select control_code from   app.sch_control_before where before_control_code in (select control_code from   app.sch_control_before where before_control_code in (select control_code from   app.sch_control_before where before_control_code = 'BASS2_Dw_acct_should_dtl_today_ds.tcl'))))BASS1_EXP_G_A_02059_DAY
BASS1_EXP_G_S_22301_DAY
BASS1_EXP_G_S_22302_DAY
BASS1_G_S_03013_MONTH.tcl
BASS1_G_S_03017_MONTH.tcl
BASS1_G_S_22013_MONTH.tcl
BASS1_G_S_22035_MONTH.tcl
BASS1_G_S_22037_MONTH.tcl
BASS1_G_S_22303_MONTH.tcl
BASS1_G_S_22305_MONTH.tcl
select distinct  control_code from   app.sch_control_before where control_code in (select distinct  control_code from   app.sch_control_before where before_control_code in (select  control_code from   app.sch_control_before where before_control_code in (select control_code from   app.sch_control_before where before_control_code in (select control_code from   app.sch_control_before where before_control_code in (select control_code from   app.sch_control_before where before_control_code = 'BASS2_Dw_acct_should_dtl_today_ds.tcl')))))select * from   app.sch_control_task where control_code = 'BASS1_G_S_22302_DAY.tcl'select * from   app.sch_control_runlog where flag = 1 select * from app.sch_control_task where control_code in (select control_code from   app.sch_control_runlog where flag = 1 )select * from app.sch_control_task a,app.sch_control_runlog b  where a.control_code=b.control_code  and flag = 1 select time_id,count(0) from    bass1.g_rule_check where  time_id >= 20110101group by time_idselect * from    bass1.g_rule_checkwhere time_id = 20110401select time_id,count(0) from   G_S_22302_DAY  where time_id > 20110301group by time_idselect time_id,count(0) from   app.sch_control_before where control_code = 'BASS1_G_S_22302_DAY.tcl'group by select * from   app.sch_control_beforewhere control_code = 'BASS1_G_S_04015_DAY.tcl'select * from   app.sch_control_runlog where control_code = 'TR1_L_A98012'select * from   app.sch_control_runlog where flag = 1select * from   app.sch_control_taskwhere control_code = 'BASS1_G_S_04015_DAY.tcl'PRIORITY_VAL
100
update app.sch_control_taskset PRIORITY_VAL = 200 where control_code = 'BASS1_G_S_04015_DAY.tcl'select  * from APP.G_FILE_REPORT
where filename like '%20110401%' and err_code='00'in ('02013'
,'04002'
,'04003'
,'04006'
,'04007'
,'04015'
,'04016'
,'04018'
,'22038'
,'22073'
,'22102'
,'22104'
,'04008'
,'04009'
,'04010'
,'04011'
,'04012'
,'06001')WITH n(control_code, before_control_code) AS 
          (SELECT control_code, before_control_code 
             FROM app.sch_control_before
             WHERE before_control_code = 'BASS2_Dw_acct_should_dtl_today_ds.tcl'
           UNION ALL
           SELECT b.control_code,b.before_control_code 
             FROM app.sch_control_before as b, n
             WHERE b.before_control_code = n.control_code)
SELECT distinct c.control_code FROM n,app.sch_control_task c
where n.control_code = c.control_code
and c.deal_time = 1
and c.control_code not like 'OLAP_%'select * from  app.sch_control_alarm 
where alarmtime >=  timestamp('20110402'||'000000') and alarmtime <=  timestamp('20110403'||'000000') 
--and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc select * from app.g_rule_check where select *from bass1.g_rule_check where time_id = 20110301 
	                    and rule_code in ('R161_1','R161_2','R161_3','R161_4','R161_5','R161_6','R161_7','R161_8','R161_9','R161_10','R161_11','R161_12','R161_13','R161_14','R161_15','R161_16','R161_17')                                               select count(0) from G_I_02018_MONTH where time_id = 201103select count(0) from G_I_02019_MONTH where time_id = 201103select count(0) from G_I_03002_MONTH where time_id = 201103select count(0) from G_I_03003_MONTH where time_id = 201103select count(0) from G_I_06011_MONTH where time_id = 201103select count(0) from G_I_06012_MONTH where time_id = 201103select count(0) from G_I_06029_MONTH where time_id = 201103select * from app.sch_control_before where control_code in 
('BASS1_G_I_02018_MONTH.tcl'
,'BASS1_G_I_02019_MONTH.tcl'
,'BASS1_G_I_02020_MONTH.tcl'
,'BASS1_G_I_02021_MONTH.tcl'
,'BASS1_G_I_02049_MONTH.tcl'
,'BASS1_G_I_02053_MONTH.tcl'
,'BASS1_G_I_03001_MONTH.tcl'
,'BASS1_G_I_03002_MONTH.tcl'
,'BASS1_G_I_03003_MONTH.tcl'
,'BASS1_G_I_06011_MONTH.tcl'
,'BASS1_G_I_06012_MONTH.tcl'
,'BASS1_G_I_06029_MONTH.tcl'
,'BASS1_INT_CHECK_0204902004_MONTH.tcl'
)
                                              select count(0) from   BASS1.G_I_02049_MONTH where time_id = 201103select count(0),count(distinct filename ) from  APP.G_FILE_REPORT
where filename like '%20110401%' and err_code='00'                       select * from   app.sch_control_task where control_code like '%BASS1%MONTH.tcl%'and cc_flag = 1select * from  G_S_03004_MONTHselect * from G_S_03004_MONTH fetch first 10 rows only  select * from G_S_03005_MONTH fetch first 10 rows only  select time_id,count(0) from   G_S_03004_MONTHgroup by time_idselect time_id,count(0) from   G_S_03005_MONTHgroup by time_id201012	6548920
select * from   bass1.G_S_04003_DAYselect count(0),count(distinct product_no) ,sum(value(int(FLOWUP),0)/1024/1024)+value(int(FLOWDOWN),0)/1024/1024))from   bass1.G_S_04003_DAYwhere time_id between  20101201 and 20101231205	39
select * from    bass1.G_S_04003_DAY西藏	39	18.85	37	18.85	2	0						
select count(0),count(distinct product_no) ,round(sum(bigint(flowup)+bigint(flowdown))*1.00/1024/1024/1024,2),round(sum(bigint(flowup))*1.00/1024/1024/1024,2)
,round(sum(bigint(flowdown))*1.00/1024/1024/1024,2)
from   bass1.G_S_04003_DAY
where time_id between  20101201 and 20101231
select round(1.595,2) from bass2.dualselect sum(bigint(FEE_RECEIVABLE)) from  G_S_03004_MONTH where time_id = 20101212643326679select sum(bigint(SHOULD_FEE)) from  G_S_03005_MONTH where time_id = 20101212643326679select count(0) from  G_S_03005_MONTH where time_id = 2010126548920select count(0) from  G_S_03004_MONTH where time_id = 2010129293551select t.*from (select a.*,row_number()over(partition by unit_code order by export_num desc )  rn from app.g_runlog a where char(time_id) = '201103') t where t.rn =1 and and unit_code = '03002'
--抓取用户资料入表insert into session.int_check_user_status (     user_id        ,product_no     ,test_flag      ,sim_code       ,usertype_id      ,create_date    ,brand_id    ,time_id )select e.user_id    ,e.product_no      ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag    ,e.sim_code    ,f.usertype_id      ,e.create_date      ,e.brand_id    ,f.time_id       from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id                ,row_number() over(partition by user_id order by time_id desc ) row_id   from bass1.g_a_02004_daywhere time_id<=20101231 ) einner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id              from bass1.g_a_02008_day           where time_id<=20101231 ) f on f.user_id=e.user_idwhere e.row_id=1 and f.row_id=1commit
select  case when a.wlan_fee > 0 and b.test_flag = '0' then  '付费用户' else '免费用户' end user_type
,count(distinct a.user_id) user_cnt,count(distinct c.product_no) user_cnt2
,round(sum(wlan_flow),2) wlan_flow
from 
( select user_id,sum(int(FEE_RECEIVABLE))  wlan_fee
from BASS1.G_S_03004_MONTH 
where time_id  = 201012 and BILL_CYC_ID = '201012'
and ACCT_ITEM_ID in ('0715','0716')
group by user_id 
 ) a 
 ,session.int_check_user_status b 
 ,(
 		select  product_no 
 		,sum(bigint(flowup)+bigint(flowdown))*1.00/1024/1024/1024 wlan_flow
        from   bass1.G_S_04003_DAY
		where time_id between  20101201 and 20101231        group by product_no
	) c 
 where a.user_id = b.user_id and b.product_no = c.product_no
group by   case when a.wlan_fee > 0 and b.test_flag = '0' then  '付费用户' else '免费用户' end  select count(0)from 
( select user_id,sum(int(SHOULD_FEE))  wlan_fee
from BASS1.G_S_03005_MONTH 
where time_id  = 201012 and BILL_CYC_ID = '201012'
and ITEM_ID in ('0715','0716')
group by user_id 
 ) a 
 ,session.int_check_user_status b   where trim(a.user_id) = trim(b.user_id)   select count(0),count(distinct b.product_no)  from session.int_check_user_status b ,(
 		select  product_no 
 		,round(sum(bigint(flowup)+bigint(flowdown))*1.00/1024/1024/1024,2) wlan_flow
        from   bass1.G_S_04003_DAY
		where time_id between  20101201 and 20101231        group by product_no
	) c where b.product_no = c.product_no            select count(0)from 
( select user_id,sum(int(SHOULD_FEE))  wlan_fee
from BASS1.G_S_03004_MONTH 
where time_id  = 201012 and BILL_CYC_ID = '201012'
and ACCT_ITEM_ID in ('0715','0716')
group by user_id 
 ) a 
 ,session.int_check_user_status b   where trim(a.user_id) = trim(b.user_id)   select * from   bass1.G_S_03005_MONTH  select count(0)    from BASS1.G_S_03005_MONTH 
where ITEM_ID in ('0715','0716')select ACCT_ITEM_ID,count(0)from  bass1.G_S_03004_MONTH  group by ACCT_ITEM_IDselect case when a.wlan_fee > 0 or b.test_flag = '2' then  '付费用户' else '免费用户' end user_type
,a.user_id,a.wlan_fee,c.product_no,wlan_flow,b.*
--,count(distinct a.user_id) user_cnt
--,sum(wlan_flow) wlan_flow
from 
( select user_id,sum(int(FEE_RECEIVABLE))  wlan_fee
from BASS1.G_S_03004_MONTH 
where time_id  = 201012 and BILL_CYC_ID = '201012'
and ACCT_ITEM_ID in ('0715','0716')
group by user_id 
 ) a 
 ,session.int_check_user_status b 
 ,(
 		select  product_no 
 		,sum(bigint(flowup)+bigint(flowdown))*1.00/1024/1024/1024,2 wlan_flow
        from   bass1.G_S_04003_DAY
		where time_id between  20101201 and 20101231
        group by product_no
	) c 
 where a.user_id = b.user_id and b.product_no = c.product_no  select * from g_i_02053_month  where time_id = 201103 and  VALID_DATE> EXPIRE_DATE  update  g_i_02053_month  set EXPIRE_DATE = VALID_DATE where time_id = 201103 and  VALID_DATE> EXPIRE_DATE   se 20101209 20101201   select * from  bass1.G_RULE_CHECK where rule_code = 'C1' and time_id > 20110101    select * from  app.sch_control_task where control_code = 'BASS1_G_I_02005_MONTH.tcl'   select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
--and flag = -1
--and control_code like 'BASS1%'
order by alarmtime desc 
select count(0),count(distinct user_id) from   G_I_02005_MONTH where time_id in (201103)where time_id in (201102,201103)select time_id,count(0) from   G_I_02005_MONTH group by time_id201103	10956
201102	18688
select time_id,count(0) from   G_I_02005_MONTH group by time_idselect count(0),count(distinct user_id||prod_id) from   G_I_02015_MONTH where time_id = 201103select OWNER_TYPE,RENT_END_DATE,count(0)from G_I_06022_MONTHgroup by OWNER_TYPE,RENT_END_DATEselect * from app.sch_control_alarm where control_code like 'BASS1_%01005%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%02005%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%02014%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%02015%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%02016%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%02047%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%06021%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%06022%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%06023%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%22009%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%22101%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%22103%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%22105%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%22106%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%06002%' 
114101932119419select count(0)  from bass2.dwd_cust_vip_card_201101  where vip_source=0 and rec_status=1select count(0)  from bass2.dwd_cust_vip_card_201102  where vip_source=0 and rec_status=119321select count(0)  from bass2.dwd_cust_vip_card_201103  where vip_source=0 and rec_status=111410                                                                                       select * from   syscat.tables where tabname like 'G_MON%'                             
CREATE TABLE BASS1.MON_ALL_INTERFACE
 (
  type						CHAR(1)
  ,INTERFACE_CODE  CHAR(5)
  ,INTERFACE_NAME  VARCHAR(100)
  ,COARSE_TYPE				  VARCHAR(30)
  ,UPLOAD_TIME					  VARCHAR(20)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (INTERFACE_CODE) USING HASHING     ALTER TABLE BASS1.MON_ALL_INTERFACE
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
    select * from  BASS1.MON_ALL_INTERFACE    select a.*  from   (select  * from BASS1.MON_ALL_INTERFACE where type = 'm') a   ,  (select distinct unit_code from app.g_runlog 
where time_id= 201103
and return_flag=1 ) b where a.interface_code = b.unit_code    select vip_source,count(0) from bass2.dwd_cust_vip_card_201101 a  group by vip_source          select * from  BASS1.G_RULE_CHECK    where rule_code = 'R019'      select * from
                (select * from 
                (
                select user_id,chg_vip_time,row_number()over(partition by user_id order by time_id) row_id from BASS1.G_I_02005_MONTH
                where time_id=201103
                ) k
                where k.row_id =1) a
                left outer join 
                (
                select * from
                (
                select user_id,create_date,row_number()over(partition by user_id order by time_id desc) row_id 
                from BASS1.G_A_02004_DAY
                where time_id<=20110331
                ) k
                where k.row_id=1) b
                on a.user_id=b.user_id
                where bigint(chg_vip_time)<bigint(create_date)
                with ur                select * from   bass2.dw_product_201103 where user_id in ('89460000740915','89160000265019')                select * from    bass2.dwd_cust_vip_card_201103 where  user_id in ('89460000740915','89160000265019')       select CUSTCLASS_ID,count(0) from   BASS1.G_I_02005_MONTHwhere time_id  = 201103group by CUSTCLASS_ID bass2.dwd_cust_vip_card_201103 89460000740915      	20110320	1	89460000740915      	20110321	1
89160000265019      	20100901	1	89160000265019      	20100917	1
update BASS1.G_I_02005_MONTHset CHG_VIP_TIME = '20110321'where user_id = '89460000740915'update BASS1.G_I_02005_MONTHset CHG_VIP_TIME = '20100917'where user_id = '89160000265019'select count(0) from   (select distinct user_id from   BASS1.G_I_02005_MONTH where time_id  = 201103) a, (select distinct user_id from   BASS1.G_I_02005_MONTH where time_id  = 201102) bwhere a.user_id = b.user_id 18589select count(0) from   BASS1.G_I_02005_MONTH where time_id  = 20110218688select count(0) from    select distinct user_id from   BASS1.G_I_02005_MONTH where time_id  = 201103exceptselect distinct user_id from   BASS1.G_I_02005_MONTH where time_id  = 20110289101110029853      
89101110031954      
89101110038097      
 
select * from   bass2.dw_product_201103 where user_id in ('89101110029853','89101110031954','89101110038097')                select max(create_date) from    bass2.dw_product_201103  where user_id in (select distinct user_id from   BASS1.G_I_02005_MONTH where time_id  = 201103exceptselect distinct user_id from   BASS1.G_I_02005_MONTH where time_id  = 201102)order by user_id10189	10188
select count(0),count(distinct user_id) from   BASS1.G_I_02005_MONTH where time_id  = 201103select time_id,count(0) from   bass1.G_I_02006_MONTHgroup by time_idorder by 1201103	788668
201102	754626
201101	733898
201012	713602
201011	676340
select * from syscat.tables where tabname like '%RUNLOG%' select count(0) from    app.SCH_CONTROL_RUNLOGselect * from   g_rule_check where rule_code in ('R076','R077','R078','R079','R080','R081','R082')and  time_id = 201103 select * from   g_rule_check where rule_code in ('R002')and  time_id = 201103 select count(0),count(distinct user_id) from    bass1.G_A_02052_MONTHselect cast(sum(case when b.user_id is null then 0 else 1 end) as decimal(15,2))/cast(count(a.user_id) as decimal (15,2))
                 from
                (
                 select user_id,usertype_id from
                 (
                  select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id
                  from bass1.G_A_02008_DAY
                  where time_id<=20110331
                 ) k
                where k.row_id=1 
                ) a
                inner join (select distinct user_id from G_A_02004_DAY
                where usertype_id<>'3'
                and time_id<=20110331               
                ) c
                 on a.user_id=c.user_id
                
                left outer join (select user_id,region_flag
                from
                (
                select user_id,region_flag,row_number()over(partition by user_id order by time_id desc) row_id 
                from bass1.G_A_02052_MONTH
                ) k
                where k.row_id=1) b
                 on a.user_id=b.user_id
                  and b.REGION_FLAG in('1','2','3')
                where  a.usertype_id not in ('2010','2020','2030','9000')
                with urBASS1_G_S_02007_MONTH.tclselect a.*,b.CONTROL_CODE from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'm'and b.control_code like '%MONTH%'select b.CONTROL_CODE from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'd'and b.control_code like '%DAY%'--接口列表select b.CONTROL_CODE from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'm'and b.control_code like '%MONTH%'and upload_time = '每月10日前'--update interfaceupdate  app.sch_control_runlog set flag = -2 where control_code in (select b.CONTROL_CODE from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'm'and b.control_code like '%MONTH%'and upload_time = '每月10日前')--后续校验程序select distinct control_code from   app.sch_control_before where  before_control_code in (select b.CONTROL_CODE from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'm'and b.control_code like '%MONTH%'and upload_time = '每月10日前')and control_code like '%CHECK%'and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)--runlog 表的情况select * from app.sch_control_runlog where control_code in (select distinct control_code from   app.sch_control_before where  before_control_code in (select b.CONTROL_CODE from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'm'and b.control_code like '%MONTH%'and upload_time = '每月10日前')and control_code like '%CHECK%'and  control_code in (select control_code from app.sch_control_task where cc_flag = 1))and flag = 0and date(endtime) < current date--update checkupdate  app.sch_control_runlog set flag = -2 where control_code in (select control_code from app.sch_control_runlog where control_code in (select distinct control_code from   app.sch_control_before where  before_control_code in (select b.CONTROL_CODE from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'm'and b.control_code like '%MONTH%'and upload_time = '每月10日前')and control_code like '%CHECK%'and  control_code in (select control_code from app.sch_control_task where cc_flag = 1))and flag = 0and date(endtime) < current date)--后续导出程序select distinct control_code from   app.sch_control_before where  before_control_code in (select b.CONTROL_CODE from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'm'and b.control_code like '%MONTH%'and upload_time = '每月10日前')and control_code like 'BASS1%EXP%'select * from app.sch_control_runlog where control_code in (select distinct control_code from   app.sch_control_before where  before_control_code in (    select b.CONTROL_CODE from        BASS1.MON_ALL_INTERFACE a    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)    and a.TYPE = 'm'    and b.control_code like '%MONTH%'    and upload_time = '每月8日前'    )    and control_code like 'BASS1%EXP%')and flag = 0and date(endtime) < current date--update expupdate  app.sch_control_runlog set flag = -2 where control_code in (select control_code from app.sch_control_runlog where control_code in (select distinct control_code from   app.sch_control_before where  before_control_code in (    select b.CONTROL_CODE from        BASS1.MON_ALL_INTERFACE a    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)    and a.TYPE = 'm'    and b.control_code like '%MONTH%'    and upload_time = '每月10日前'    )    and control_code like 'BASS1%EXP%')and flag = 0and date(endtime) < current date)select control_code from app.sch_control_runlog where control_code in (select distinct control_code from   app.sch_control_before where  before_control_code in (    select b.CONTROL_CODE from        BASS1.MON_ALL_INTERFACE a    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)    and a.TYPE = 'm'    and b.control_code like '%MONTH%'    and upload_time = '每月8日前'    )    and control_code like 'BASS1%EXP%')--and flag = 0and date(endtime) = current dateselect * from  app.sch_control_task  where control_code = 'BASS1_INT_CHECK_IMPORTSERV_MONTH'select count(0) from   bass2.stat_zd_village_users_2011021391237select count(0) from   bass2.stat_zd_village_users_201103select distinct control_code from   app.sch_control_before 
where  before_control_code in (
select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
and upload_time = '每月8日前'
)
and control_code like '%CHECK%'
and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
and control_code
in
('BASS1_INT_CHECK_02006_MONTH.tcl'
,'BASS1_INT_CHECK_0205202008_MONTH.tcl'
,'BASS1_INT_CHECK_B67_MONTH.tcl'
,'BASS1_INT_CHECK_D9E234F2_651TO56_MONTH.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_L5_MONTH.tcl'
,'BASS1_INT_CHECK_R031_MONTH.tcl'
,'BASS1_INT_CHECK_R034_MONTH.tcl'
,'BASS1_INT_CHECK_R036_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
,'BASS1_INT_CHECK_R030_MONTH.tcl'
,'BASS1_INT_CHECK_R032_MONTH.tcl'
,'BASS1_INT_CHECK_R037toR039_MONTH.tcl'
,'BASS1_INT_CHECK_R040_MONTH.tcl'
,'BASS1_INT_CHECK_0200803005_MONTH.tcl'
,'BASS1_INT_CHECK_R008_MONTH.tcl'
,'BASS1_INT_CHECK_R009_MONTH.tcl'
,'BASS1_INT_CHECK_SAMPLE_MONTH.tcl'
,'BASS1_INT_CHECK_VOICE_MONTH.tcl'
)select * from syscat.tables where tabname like '%21012%'select time_id,count(0) from G_S_21012_MONTHgroup by time_idselect * from   app.sch_control_before where control_code like '%G_S_21012_MONTH%'select count(0) from                                bass2.dw_call_opposite_201103
194419329                            select  substr(opp_number,1,5)         , count(0)from  bass2.dw_call_opposite_201103where  --substr(opp_number,1,5)     like '125%'substr(opp_number,1,5) in ('12590','12596','12559')group by  substr(opp_number,1,5)     order by 1--3月1	2
12559	2
12590	42846
12596	1400
select  substr(opp_number,1,5)         , count(0)from  bass2.dw_call_opposite_201101where  --substr(opp_number,1,5)     like '125%'substr(opp_number,1,5) in ('12590','12596','12559')group by  substr(opp_number,1,5)     order by 1--2月1	2
12559	1
12590	120052
12596	1089
--1月12590	161846
12596	1760
--警讯通统付收入：select sum(int(income)) from   G_S_03017_MONTHwhere ent_busi_id = '1360'and time_id = 2011037220,00--无非统付收入select * from   G_S_03017_MONTHwhere ent_busi_id = '1360'--财信通通非统付收入：select sum(int(income)) from   bass1.G_S_03018_MONTH where ent_busi_id = '1300' and time_id = 201103 6007,00select * from   app.sch_control_task where control_code like '%INT_CHECK_R029_MONTH.tcl'BASS1_INT_CHECK_R029_MONTH.tclselect * from   app.sch_control_before where control_code like 'BASS1_INT_CHECK_R029_MONTH.tcl'                  update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select control_code from app.sch_control_runlog 
where control_code in (
select distinct control_code from   app.sch_control_before 
where  before_control_code in (
    select b.CONTROL_CODE from    
    BASS1.MON_ALL_INTERFACE a
    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
    and a.TYPE = 'm'
    and b.control_code like '%MONTH%'
    and upload_time = '每月10日前'
    )
    and control_code like 'BASS1%EXP%'
)
and flag = 0
and date(endtime) < current date
)
and control_code not in ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_S_05002_MONTH.tcl')  update                select * from   app.sch_control_runlog where control_code in ('BASS1_EXP_G_S_05001_MONTH','BASS1_EXP_G_S_05002_MONTH')BASS1_EXP_G_S_05001_MONTH
BASS1_G_S_05002_MONTH.tcl
BASS1_EXP_G_S_05002_MONTH
update  app.sch_control_runlog set flag = 0where control_code in ('BASS1_EXP_G_S_05001_MONTH','BASS1_EXP_G_S_05002_MONTH')BASS1_EXP_G_S_05002_MONTH	2011-04-01 12:09:57.682329	2011-04-01 12:11:32.054193	94	-2
BASS1_EXP_G_S_05001_MONTH	2011-04-07 10:12:04.572475	[NULL]	[NULL]	1

db2 "load from /dev/null of del terminate into  bass1.T_GS05001M"  
db2 "load from /dev/null of del terminate into  bass1.T_GS05002M"  
select * from   bass1.T_GS05001M where time_id = 201012select * from   bass1.T_GS05002M where time_id = 201012select sum(bigint(OUT_DURN))*1.00/sum(bigint(IN_DURN)) from   bass1.T_GS05001M where time_id = 2010120.83121280782select sum(bigint(OUT_DURN))*1.00/sum(bigint(IN_DURN)) from   bass1.T_GS05001M where time_id = 2011030.84929345800select sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) from   bass1.T_GS05001M where time_id = 2010120.85943690519select sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) from   bass1.T_GS05001M where time_id = 2011030.86976532371select sum(bigint(IN_COUNT))*1.00/sum(bigint(OUT_COUNT)) from   bass1.T_GS05002M where time_id = 2010120.83121280782select sum(bigint(IN_COUNT))*1.00/sum(bigint(OUT_COUNT)) from   bass1.T_GS05002M where time_id = 2011030.84929345800select sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) from   bass1.T_GS05002M where time_id = 2010120.85943690519select sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) from   bass1.T_GS05002M where time_id = 2011030.8697653237160113021select sum(bigint(IN_DURN)) from   bass1.T_GS05001M where time_id = 20101272319652select  60113021*1.00/72319652 from bass2.dualselect (select sum(bigint(OUT_DURN)) from   bass1.T_GS05001M where time_id = 201012)/(select sum(bigint(IN_DURN)) from   bass1.T_GS05001M where time_id = 201012)from bass2.dualselect * from   bass1.T_GS05002M where time_id = 201012select * from   bass1.T_GS05001M where time_id = 201103select * from   bass1.T_GS05002M where time_id = 201103 g_s_05001_month/g_s_05002_month select * from   bass1.g_s_05001_month where time_id = 201102 select * from   bass1.g_s_05001_month where time_id = 201103select * from   bass1.g_s_05002_month where time_id = 201103select * from   bass1.g_s_05002_month where time_id = 201102insert into bass1.g_s_05001_monthselect * from  bass1.T_GS05001M where time_id = 201103insert into bass1.g_s_05002_monthselect * from  bass1.T_GS05002M where time_id = 201103select time_id,count(0) from   G_S_05003_MONTHgroup by time_idselect * from   G_S_05003_MONTH where time_id = 201103select * from   G_S_05003_MONTH where time_id = 201102select count(0),count(distinct SP_SVR_CODE)  from   G_S_05003_MONTH where time_id = 201103select count(0),count(distinct SP_SVR_CODE)  from   G_S_05003_MONTH where time_id = 201102select SP_BUS_CODE,count(0),sum(bigint(PAY_FEE)),sum(bigint(PAY_BALANCE_FEE)),sum(bigint(STLMNT_FEE)),sum(bigint(INFO_FEE))from G_S_05003_MONTH where time_id = 201103group by SP_BUS_CODE01	98	0	1573500	57048187	356660330
02	805	0	0	100650555	671003700
03	91	0	0	28266454	188443030
04	563	0	0	15880785	105871900
08	21	0	0	19579200	32632000
16	103	0	0	632300940	770946200

select SP_BUS_CODE,count(0),sum(bigint(PAY_FEE)),sum(bigint(PAY_BALANCE_FEE)),sum(bigint(STLMNT_FEE)),sum(bigint(INFO_FEE))from G_S_05003_MONTH where time_id = 201102group by SP_BUS_CODE01	99	0	1520150	50849135	338797900
02	767	0	0	101497245	676648300
03	101	0	0	10459780	69731880
04	921	0	0	7151325	47675500
08	21	0	0	19620000	32700000
16	99	0	0	611069783	744881850


select SP_BUS_CODE,count(0),sum(bigint(PAY_FEE)),sum(bigint(PAY_BALANCE_FEE)),sum(bigint(STLMNT_FEE)),sum(bigint(INFO_FEE))from G_S_05003_MONTH where time_id = 201101group by SP_BUS_CODE01	98	0	1575600	195599455	538036700
02	798	0	0	117195616	781304110
03	110	0	0	11710603	78070700
04	1133	0	0	10629885	70865900
08	21	0	0	20071800	33453000
16	104	0	0	591129135	720477760

select * from   g_s_22025_month where time_id = 201011select * from   g_s_22025_month where time_id = 201103select time_id,count(0) from   g_s_22025_month group by time_idselect * from   G_S_22033_MONTH where time_id = 201103 select * from   G_S_22033_MONTH where time_id = 201102select time_id,count(0) from   G_S_22033_MONTH group by time_idselect sum(bigint(PAY_FEE)),sum(bigint(PAY_BALANCE_FEE)),sum(bigint(STLMNT_FEE)),sum(bigint(INFO_FEE)) from   bass1.G_S_05003_MONTH where time_id = 2011030	1573500	853726121	2125557160
select sum(bigint(PAY_FEE)),sum(bigint(PAY_BALANCE_FEE)),sum(bigint(STLMNT_FEE)),sum(bigint(INFO_FEE)) from   bass1.G_S_05003_MONTH where time_id = 2011020	1520150	800647268	1910435430
select * from  app.sch_control_alarm 
where 
 control_code  in (
    select b.CONTROL_CODE from    
    BASS1.MON_ALL_INTERFACE a
    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
    and a.TYPE = 'm'
    and b.control_code like '%MONTH%'
    and upload_time = '每月15日前'
    )select time_id,count(0) from   bass1.G_S_22039_MONTHgroup by time_idselect * from   bass1.G_S_22039_MONTH where time_id in( 201103,201102,201101)order by 1,3select * from   G_S_22050_MONTHselect * from   G_S_22055_MONTHselect * from   bass1.G_S_22050_MONTH where time_id = 201102
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
and upload_time = '每月15日前'
)

--update check
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select control_code from app.sch_control_runlog 
where control_code in (
select distinct control_code from   app.sch_control_before 
where  before_control_code in (
select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
and upload_time = '每月15日前'
)
and control_code like '%CHECK%'
and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
)
and flag = 0
and date(endtime) < current date
)


--update exp
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select control_code from app.sch_control_runlog 
where control_code in (
select distinct control_code from   app.sch_control_before 
where  before_control_code in (
    select b.CONTROL_CODE from    
    BASS1.MON_ALL_INTERFACE a
    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
    and a.TYPE = 'm'
    and b.control_code like '%MONTH%'
    and upload_time = '每月15日前'
    )
    and control_code like 'BASS1%EXP%'
)
and flag = 0
and date(endtime) < current date
)select * from   BASS1.G_S_22062_MONTHselect time_id,count(0) from   G_S_22062_MONTHgroup by time_id select * from   BASS1.G_S_22064_MONTHwhere time_id = 201103select * from app.sch_control_task where control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)CC_FLAG
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
select * from   app.sch_control_before where control_code = 'BASS1_INT_CHECK_L0_TO_DAY.tcl'update  app.sch_control_taskset CC_FLAG = 2 where control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)20 row(s) affected.--改回去update  app.sch_control_taskset CC_FLAG = 1 where control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)20 row(s) affected.update  app.sch_control_taskset function_desc = '[作废]'||function_desc where control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)update  app.sch_control_taskset function_desc = replace(function_desc,'[作废]','') where control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)select * from    app.sch_control_taskwhere control_code ='BASS1_EXP_G_S_09903_DAY'select * from   app.sch_control_before where before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)26delete from  app.sch_control_beforewhere before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)26 row(s) affected.BASS1_EXP_G_I_03007_MONTH	BASS1_INT_CHECK_F7_MONTH.tcl
BASS1_EXP_G_S_04006_DAY	BASS1_INT_CHECK_F1_TO_DAY.tcl
BASS1_EXP_G_S_21001_DAY	BASS1_INT_CHECK_C567_DAY.tcl
BASS1_EXP_G_S_21001_DAY	BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl
BASS1_EXP_G_S_03005_MONTH	BASS1_INT_CHECK_G047TO50_MONTH.tcl
BASS1_EXP_G_S_03005_MONTH	BASS1_INT_CHECK_L34_MONTH.tcl
BASS1_EXP_G_S_04002_DAY	BASS1_INT_CHECK_L0_TO_DAY.tcl
BASS1_EXP_G_S_04014_DAY	BASS1_INT_CHECK_L1K9_TO_DAY.tcl
BASS1_EXP_G_A_01004_DAY	BASS1_INT_CHECK_L2_TO_DAY.tcl
BASS1_EXP_G_A_02004_DAY	BASS1_INT_CHECK_33TO40_DAY.tcl
BASS1_EXP_G_A_02004_DAY	INT_CHECK_DATARULE_DAY.tcl
BASS1_EXP_G_S_21004_DAY	BASS1_INT_CHECK_C567_DAY.tcl
BASS1_EXP_G_A_02008_DAY	INT_CHECK_DATARULE_DAY.tcl
BASS1_EXP_G_A_02008_DAY	BASS1_INT_CHECK_33TO40_DAY.tcl
BASS1_EXP_G_S_03012_MONTH	BASS1_INT_CHECK_E5_MONTH.tcl
BASS1_EXP_G_S_03012_MONTH	BASS1_INT_CHECK_G047TO50_MONTH.tcl
BASS1_EXP_G_S_03012_MONTH	BASS1_INT_CHECK_L34_MONTH.tcl
BASS1_EXP_G_S_03004_MONTH	BASS1_INT_CHECK_E5_MONTH.tcl
BASS1_EXP_G_S_22038_DAY	BASS1_INT_CHECK_C567_DAY.tcl
BASS1_EXP_G_S_21009_DAY	BASS1_INT_CHECK_C567_DAY.tcl
BASS1_EXP_G_S_02047_MONTH	BASS1_INT_CHECK_98Z6_MONTH.tcl
BASS1_EXP_G_S_04004_DAY	BASS1_INT_CHECK_A0L694B5_DAY.tcl
BASS1_EXP_G_S_04005_DAY	BASS1_INT_CHECK_L1K9_TO_DAY.tcl
BASS1_EXP_G_S_04005_DAY	BASS1_INT_CHECK_A12E6_DAY.tcl
BASS1_EXP_G_S_21008_MONTH	BASS1_INT_CHECK_8895_MONTH.tcl
BASS1_EXP_G_S_21007_DAY	BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl
insert into app.sch_control_before values ('BASS1_EXP_G_I_03007_MONTH','BASS1_INT_CHECK_F7_MONTH.tcl')insert into app.sch_control_before values ('BASS1_EXP_G_S_04006_DAY','BASS1_INT_CHECK_F1_TO_DAY.tcl')               
insert into app.sch_control_before values ('BASS1_EXP_G_S_21001_DAY','BASS1_INT_CHECK_C567_DAY.tcl')                
insert into app.sch_control_before values ('BASS1_EXP_G_S_21001_DAY','BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl')     
insert into app.sch_control_before values ('BASS1_EXP_G_S_03005_MONTH','BASS1_INT_CHECK_G047TO50_MONTH.tcl')        
insert into app.sch_control_before values ('BASS1_EXP_G_S_03005_MONTH','BASS1_INT_CHECK_L34_MONTH.tcl')             
insert into app.sch_control_before values ('BASS1_EXP_G_S_04002_DAY','BASS1_INT_CHECK_L0_TO_DAY.tcl')               
insert into app.sch_control_before values ('BASS1_EXP_G_S_04014_DAY','BASS1_INT_CHECK_L1K9_TO_DAY.tcl')             
insert into app.sch_control_before values ('BASS1_EXP_G_A_01004_DAY','BASS1_INT_CHECK_L2_TO_DAY.tcl')               
insert into app.sch_control_before values ('BASS1_EXP_G_A_02004_DAY','BASS1_INT_CHECK_33TO40_DAY.tcl')              
insert into app.sch_control_before values ('BASS1_EXP_G_A_02004_DAY','INT_CHECK_DATARULE_DAY.tcl')                  
insert into app.sch_control_before values ('BASS1_EXP_G_S_21004_DAY','BASS1_INT_CHECK_C567_DAY.tcl')                
insert into app.sch_control_before values ('BASS1_EXP_G_A_02008_DAY','INT_CHECK_DATARULE_DAY.tcl')                  
insert into app.sch_control_before values ('BASS1_EXP_G_A_02008_DAY','BASS1_INT_CHECK_33TO40_DAY.tcl')              
insert into app.sch_control_before values ('BASS1_EXP_G_S_03012_MONTH','BASS1_INT_CHECK_E5_MONTH.tcl')              
insert into app.sch_control_before values ('BASS1_EXP_G_S_03012_MONTH','BASS1_INT_CHECK_G047TO50_MONTH.tcl')        
insert into app.sch_control_before values ('BASS1_EXP_G_S_03012_MONTH','BASS1_INT_CHECK_L34_MONTH.tcl')             
insert into app.sch_control_before values ('BASS1_EXP_G_S_03004_MONTH','BASS1_INT_CHECK_E5_MONTH.tcl')              
insert into app.sch_control_before values ('BASS1_EXP_G_S_22038_DAY','BASS1_INT_CHECK_C567_DAY.tcl')                
insert into app.sch_control_before values ('BASS1_EXP_G_S_21009_DAY','BASS1_INT_CHECK_C567_DAY.tcl')                
insert into app.sch_control_before values ('BASS1_EXP_G_S_02047_MONTH','BASS1_INT_CHECK_98Z6_MONTH.tcl')            
insert into app.sch_control_before values ('BASS1_EXP_G_S_04004_DAY','BASS1_INT_CHECK_A0L694B5_DAY.tcl')            
insert into app.sch_control_before values ('BASS1_EXP_G_S_04005_DAY','BASS1_INT_CHECK_L1K9_TO_DAY.tcl')             
insert into app.sch_control_before values ('BASS1_EXP_G_S_04005_DAY','BASS1_INT_CHECK_A12E6_DAY.tcl')               
insert into app.sch_control_before values ('BASS1_EXP_G_S_21008_MONTH','BASS1_INT_CHECK_8895_MONTH.tcl')            
insert into app.sch_control_before values ('BASS1_EXP_G_S_21007_DAY','BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl')     
select * from   app.sch_control_before where before_control_code in ('BASS1_G_A_06030_DAY.tcl') select distinct control_code from   app.sch_control_before where before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)select * from    app.sch_control_taskwhere control_codein (select control_code from   app.sch_control_before where before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
))CONTROL_CODE
BASS1_EXP_G_A_01004_DAY
BASS1_EXP_G_A_02004_DAY
BASS1_EXP_G_A_02008_DAY
BASS1_EXP_G_I_03007_MONTH
BASS1_EXP_G_S_02047_MONTH
BASS1_EXP_G_S_03004_MONTH
BASS1_EXP_G_S_03005_MONTH
BASS1_EXP_G_S_03012_MONTH
BASS1_EXP_G_S_04002_DAY
BASS1_EXP_G_S_04004_DAY
BASS1_EXP_G_S_04005_DAY
BASS1_EXP_G_S_04006_DAY
BASS1_EXP_G_S_04014_DAY
BASS1_EXP_G_S_21001_DAY
BASS1_EXP_G_S_21004_DAY
BASS1_EXP_G_S_21007_DAY
BASS1_EXP_G_S_21008_MONTH
BASS1_EXP_G_S_21009_DAY
BASS1_EXP_G_S_22038_DAY
select distinct control_code from    app.sch_control_beforewhere control_codein 
('BASS1_EXP_G_A_01004_DAY    '
,'BASS1_EXP_G_A_02004_DAY    '
,'BASS1_EXP_G_A_02008_DAY    '
,'BASS1_EXP_G_I_03007_MONTH  '
,'BASS1_EXP_G_S_02047_MONTH  '
,'BASS1_EXP_G_S_03004_MONTH  '
,'BASS1_EXP_G_S_03005_MONTH  '
,'BASS1_EXP_G_S_03012_MONTH  '
,'BASS1_EXP_G_S_04002_DAY    '
,'BASS1_EXP_G_S_04004_DAY    '
,'BASS1_EXP_G_S_04005_DAY    '
,'BASS1_EXP_G_S_04006_DAY    '
,'BASS1_EXP_G_S_04014_DAY    '
,'BASS1_EXP_G_S_21001_DAY    '
,'BASS1_EXP_G_S_21004_DAY    '
,'BASS1_EXP_G_S_21007_DAY    '
,'BASS1_EXP_G_S_21008_MONTH  '
,'BASS1_EXP_G_S_21009_DAY    '
,'BASS1_EXP_G_S_22038_DAY')select * from    app.sch_control_beforewhere control_codein (select control_code from   app.sch_control_before where before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
))select * from  app.sch_control_taskwhere control_code in (select control_code from   app.sch_control_before where before_control_code in (select control_code from app.sch_control_task where function_desc like '%作废%'and control_code like '%CHECK%'AND control_code NOT in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)))select * from   app.sch_control_before where before_control_code ='BASS1_INT_CHECK_R004_MONTH.tcl'
select distinct control_code from    app.sch_control_before
where control_code                                          
in                                                          
('BASS1_EXP_G_A_01004_DAY    '                             
,'BASS1_EXP_G_A_02004_DAY    '                             
,'BASS1_EXP_G_A_02008_DAY    '                             
,'BASS1_EXP_G_I_03007_MONTH  '                             
,'BASS1_EXP_G_S_02047_MONTH  '                             
,'BASS1_EXP_G_S_03004_MONTH  '                             
,'BASS1_EXP_G_S_03005_MONTH  '                             
,'BASS1_EXP_G_S_03012_MONTH  '                             
,'BASS1_EXP_G_S_04002_DAY    '                             
,'BASS1_EXP_G_S_04004_DAY    '                             
,'BASS1_EXP_G_S_04005_DAY    '                             
,'BASS1_EXP_G_S_04006_DAY    '                             
,'BASS1_EXP_G_S_04014_DAY    '                             
,'BASS1_EXP_G_S_21001_DAY    '                             
,'BASS1_EXP_G_S_21004_DAY    '                             
,'BASS1_EXP_G_S_21007_DAY    '                             
,'BASS1_EXP_G_S_21008_MONTH  '                             
,'BASS1_EXP_G_S_21009_DAY    '                             
,'BASS1_EXP_G_S_22038_DAY')                                
and 
before_control_code not in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)

select distinct control_code from    app.sch_control_before
where control_code                                          
in                                                          
('BASS1_EXP_G_A_01004_DAY    '                             
,'BASS1_EXP_G_A_02004_DAY    '                             
,'BASS1_EXP_G_A_02008_DAY    '                             
,'BASS1_EXP_G_I_03007_MONTH  '                             
,'BASS1_EXP_G_S_02047_MONTH  '                             
,'BASS1_EXP_G_S_03004_MONTH  '                             
,'BASS1_EXP_G_S_03005_MONTH  '                             
,'BASS1_EXP_G_S_03012_MONTH  '                             
,'BASS1_EXP_G_S_04002_DAY    '                             
,'BASS1_EXP_G_S_04004_DAY    '                             
,'BASS1_EXP_G_S_04005_DAY    '                             
,'BASS1_EXP_G_S_04006_DAY    '                             
,'BASS1_EXP_G_S_04014_DAY    '                             
,'BASS1_EXP_G_S_21001_DAY    '                             
,'BASS1_EXP_G_S_21004_DAY    '                             
,'BASS1_EXP_G_S_21007_DAY    '                             
,'BASS1_EXP_G_S_21008_MONTH  '                             
,'BASS1_EXP_G_S_21009_DAY    '                             
,'BASS1_EXP_G_S_22038_DAY')       
except
select distinct control_code 
from    app.sch_control_before
where control_code                                          
in                                                          
('BASS1_EXP_G_A_01004_DAY    '                             
,'BASS1_EXP_G_A_02004_DAY    '                             
,'BASS1_EXP_G_A_02008_DAY    '                             
,'BASS1_EXP_G_I_03007_MONTH  '                             
,'BASS1_EXP_G_S_02047_MONTH  '                             
,'BASS1_EXP_G_S_03004_MONTH  '                             
,'BASS1_EXP_G_S_03005_MONTH  '                             
,'BASS1_EXP_G_S_03012_MONTH  '                             
,'BASS1_EXP_G_S_04002_DAY    '                             
,'BASS1_EXP_G_S_04004_DAY    '                             
,'BASS1_EXP_G_S_04005_DAY    '                             
,'BASS1_EXP_G_S_04006_DAY    '                             
,'BASS1_EXP_G_S_04014_DAY    '                             
,'BASS1_EXP_G_S_21001_DAY    '                             
,'BASS1_EXP_G_S_21004_DAY    '                             
,'BASS1_EXP_G_S_21007_DAY    '                             
,'BASS1_EXP_G_S_21008_MONTH  '                             
,'BASS1_EXP_G_S_21009_DAY    '                             
,'BASS1_EXP_G_S_22038_DAY')                                
and 
before_control_code not in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
select * from app.sch_control_before where control_code in 
(
'BASS1_EXP_G_A_01004_DAY'
,'BASS1_EXP_G_S_04005_DAY'
,'BASS1_EXP_G_S_04006_DAY'
,'BASS1_EXP_G_S_22038_DAY')select * from app.sch_control_before where control_code in (select before_control_code from app.sch_control_before where control_code in 
(
'BASS1_EXP_G_A_01004_DAY'
,'BASS1_EXP_G_S_04005_DAY'
,'BASS1_EXP_G_S_04006_DAY'
,'BASS1_EXP_G_S_22038_DAY'))select * from    bass1.G_REPORT_CHECK select * from app.sch_control_before where control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)select * from syscat.tables where tabname like '%BEFORE%' create table bass2.t_0x1f
(cola varchar(10),colb varchar(10))
in tbs_3hselect * from   bass2.t_0x1fselect hex('�') from bass2.dual 06021
06022
06023
22061
22062
22063
22064
select * from   bass1.mon_interface_infoselect b.CONTROL_CODE,a.* from  BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
AND INTERFACE_CODE in 									
('06021'
,'06022'
,'06023'
,'22061'
,'22062'
,'22063'
,'22064')select * from   BASS1.G_I_06021_MONTHselect count(0),count(distinct CHANNEL_ID) from   BASS1.G_I_06021_MONTH where time_id = 201102select count(0) from   BASS1.G_I_06022_MONTH where time_id = 201102select time_id , count(0) 
--,  count(distinct CHANNEL_TYPE ) 
from BASS1.G_I_06021_MONTH 
group by  time_id 
order by 1 
select CHANNEL_TYPE , count(0) 
--,  count(distinct CHANNEL_TYPE ) 
from BASS1.G_I_06021_MONTH 
group by  CHANNEL_TYPE 
order by 1 
select SELF_CHANNEL_ID , count(0) 
--,  count(distinct SELF_CHANNEL_ID ) 
from BASS1.G_I_06021_MONTH 
group by  SELF_CHANNEL_ID 
order by 1 

select POSITION , count(0) 
--,  count(distinct POSITION ) 
from BASS1.G_I_06021_MONTH 
group by  POSITION 
order by 1
select CHANNEL_B_TYPE , count(0) 
--,  count(distinct CHANNEL_B_TYPE ) 
from BASS1.G_I_06021_MONTH 
group by  CHANNEL_B_TYPE 
order by 1 
 select CHANNEL_STATUS , count(0) 
--,  count(distinct CHANNEL_STATUS ) 
from BASS1.G_I_06021_MONTH where time_id = 201102
group by  CHANNEL_STATUS 
order by 1 CHANNEL_STATUS	2
1	6466
2	29
3	740
1：正常运营
2：暂停营业/预注销（自营厅、委托经营厅和24小时自助营业厅为暂停营业，社会代理网点为预注销）
3：已关店/注销（自营厅、委托经营厅和24小时自助营业厅为已关店，社会代理网点为注销）
 select  channel_type, count(0) 
from BASS1.G_I_06021_MONTH where time_id = 201102 and CHANNEL_STATUS = '1' and channel_type = '1'
group by   channel_type
order by 1 ,21	1	133
 select  channel_type, count(0) 
from BASS1.G_I_06021_MONTH where time_id = 201102 and CHANNEL_STATUS = '1' 
group by   channel_type
order by 1 ,2CHANNEL_B_TYPE select  CHANNEL_B_TYPE, count(0) 
from BASS1.G_I_06021_MONTH where time_id = 201102 and CHANNEL_STATUS = '1' 
group by   CHANNEL_B_TYPE
order by 1 ,2CHANNEL_STAR select  CHANNEL_STAR, count(0) 
from BASS1.G_I_06021_MONTH where time_id = 201102 and CHANNEL_STATUS = '1' 
group by   CHANNEL_STAR
order by 1 ,2一星级	二星级	三星级	四星级	五六星级
646	578	2078	1784	1240
 	133
1	646
2	578
3	2078
4	1784
5	572
6	668
7	7
select count(0) from   BASS1.G_I_06022_MONTH where time_id = 2011027235select count(*) from bass1.g_i_06022_month
where channel_id not in 
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102 )
  and time_id =201102  06021中正常运营的自营渠道不在06022中的数量
select count(0) from     BASS1.G_I_06021_MONTH where  CHANNEL_STATUS = '1' and channel_type = '1'and  time_id =201102and channel_id not in 
(select distinct channel_id from bass1.g_i_06022_month where time_id =201102 )在06022中但不在06021中的渠道数量select count(0) from   (select channel_id from   BASS1.G_I_06022_MONTH where time_id =201102exceptselect channel_id from   BASS1.G_I_06021_MONTH where time_id =201102) a select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.g_i_06022_month where time_id =201102)
  and time_id =201102
  and channel_type <>'3'
  and channel_status='1'   select count(0) from   BASS1.G_I_06023_MONTH where time_id = 201102select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.g_i_06023_month where time_id =201102)
  and time_id =201102
  and channel_status='1'       select count(*) from bass1.g_s_22061_month where  time_id =201102   select count(*) from bass1.g_s_22061_month
where channel_id not in 
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102 )
  and time_id =201102       select * from bass1.g_i_06022_month where  time_id =201102select OWNER_TYPE , count(0) 
--,  count(distinct PROPERTY_SRC_TYPE ) 
from BASS1.G_I_06022_MONTH where  time_id =201102
group by  OWNER_TYPE 
order by 1 
22061中社会代理网点数量
 select count(*) from bass1.g_s_22061_month where channel_id  in 
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102 and channel_type = '3' )
  and time_id =201102 7075   select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.g_s_22061_month where time_id =201102)
  and time_id =201102
  and channel_type<>'3'
  and channel_status='1'
0select * from    bass1.g_s_22062_month a where a.ACCEPT_TYPEACCEPT_TYPEselect count(0),count(distinct b.CHANNEL_ID||b.ACCEPT_TYPE) from    bass1.g_s_22062_month bwhere  b.TIME_ID = 201102 1065	1065
select count(distinct b.CHANNEL_ID) from    bass1.g_s_22062_month a, bass1.g_i_06021_month b  where a.time_id =201102 and b.TIME_ID = 201102 and a.CHANNEL_ID = b.CHANNEL_ID1065	918
select count(distinct a.CHANNEL_ID) from    bass1.g_s_22062_month  awhere a.time_id =201102
select count(*) from bass1.g_s_22062_month
where channel_id not in 
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102 )
  and time_id =201102    select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.g_s_22062_month where time_id =201102)
  and time_id =201102
  and channel_status='1'
5554  select count(0) from      bass1.g_s_22062_month where time_id =201102select count(distinct b.CHANNEL_ID) from    bass1.g_s_22062_month a, bass1.g_i_06021_month b  where a.time_id =201102 and b.TIME_ID = 201102 and a.CHANNEL_ID = b.CHANNEL_IDselect * from   bass1.g_s_22063_month a where a.time_id =201102select count(0),count(distinct a.CHANNEL_ID) from    bass1.g_s_22063_month  awhere a.time_id =201102select count(*) from bass1.g_s_22063_month
where channel_id not in
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102 and channel_type<>'1')
  and time_id =201102        select count(*) from bass1.g_s_22063_month
where channel_id in
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102 and channel_type='1')
  and time_id =201102
select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month where time_id =201102)
  and time_id =201102
  and channel_type in ('2','3')
  and channel_status='1'
5481select time_id,count(0)from bass1.G_S_22038_DAYwhere time_id > 20110101group by time_idselect *from bass1.G_S_22038_DAYwhere time_id > 20110101group by time_idCREATE TABLE BASS1.mon_user_mobile
 (
MOBILE_NUM  VARCHAR(11)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (MOBILE_NUM) USING HASHING
   
  ALTER TABLE BASS1.mon_user_mobile
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE    insert into BASS1.mon_user_mobile  select '15913269062'  from bass2.dual      select * from BASS1.mon_user_mobile    	select  ('${MESSAGE_CONTENT}'),MOBILE_NUM from bass1.mon_user_mobileselect 'xxxxx',count(0) from ( select a.* ,row_number()over(partition by substr(filename,18,5) order by deal_time desc ) rn from APP.G_FILE_REPORT a where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00' ) t where rn = 1    select * from    APP.G_FILE_REPORTs_13100_20100307_04003_00_001.dat    select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE where upload_time = '每日9点前'substr(filename,18,5)in    select 'xxxxx',count(0) from ( select a.* ,row_number()over(partition by substr(filename,18,5) order by deal_time desc ) rn from APP.G_FILE_REPORT a where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'  and substr(filename,18,5) in (select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE where upload_time = '每日9点前')) t where rn = 1          select  'xxxxx',count(0)\
						from \
						( \
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn \
						from APP.G_FILE_REPORT a \
						where substr(filename,9,8) = replace(char(current date - 1 days),'-','') \
						and err_code='00' \
						and substr(filename,18,5) \
						in (  select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE \
								   where upload_time = '每日11点前' \
								) \
						) t where rn = 1                                                
   
   select  'xxxxx',count(0)
						from 
						( 
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn 
						from APP.G_FILE_REPORT a 
						where substr(filename,9,8) = replace(char(current date - 1 days),'-','') 
						and err_code='00' 
						and substr(filename,18,5) 
						in (  select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE 
								   where upload_time = '每日11点前' 
								) 
						) t where rn = 1                                                select upload_time , count(0) from    BASS1.MON_ALL_INTERFACE group by  upload_time                       select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month where time_id =201102)
  and time_id =201102
  and channel_type in ('2','3')
  and channel_status='1'      select * from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month where time_id =201102)
  and time_id =201102
  and channel_type in ('2','3')
  and channel_status='1'   
select * from bass2.stat_channel_reward_0002
where channel_name like '%便民%' declare global temporary table session.int_check_user_status    (   user_id        CHARACTER(15),   product_no     CHARACTER(15),   test_flag      CHARACTER(1),   sim_code       CHARACTER(15),   usertype_id    CHARACTER(4),   create_date    CHARACTER(15),   brand_id       CHARACTER(4),   time_id        int,   CHANNEL_ID     varchar(25)    )                            partitioning key            (   user_id     ) using hashing           with replace on commit preserve rows not logged in tbs_user_temp--抓取用户资料入表insert into session.int_check_user_status (     user_id        ,product_no     ,test_flag      ,sim_code       ,usertype_id      ,create_date    ,brand_id    ,time_id    ,CHANNEL_ID    )select e.user_id    ,e.product_no      ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag    ,e.sim_code    ,f.usertype_id      ,e.create_date      ,e.brand_id    ,f.time_id      ,e.CHANNEL_ID     from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id,CHANNEL_ID                ,row_number() over(partition by user_id order by time_id desc ) row_id   from bass1.g_a_02004_daywhere time_id<=20110231 ) einner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id              from bass1.g_a_02008_day           where time_id<=20110231 ) f on f.user_id=e.user_idwhere e.row_id=1 and f.row_id=1commitselect count(distinct channel_id) from session.int_check_user_status
where create_date between '20110201' and '20110231'
and test_flag='0'select count(distinct user_id) from session.int_check_user_status
where create_date between '20110201' and '20110231'
and test_flag='0'select count(*) from 
(
select distinct channel_id from session.int_check_user_status
where create_date between '20110201' and '20110231'
and test_flag='0'
) aa
where aa.channel_id not in
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102)
    select * from BASS2.DW_CHANNEL_INFO_201102 where organize_id in (30300102,380235)
" select CHANNEL_STATUS , count(0) 
--,  count(distinct CHANNEL_STATUS ) 
from BASS1.G_I_06021_MONTH 
where time_id = 201102
group by  CHANNEL_STATUS 
order by 1 "
select channel_status,count(channel_id) from bass1.g_i_06021_month
where time_id =201102
group by channel_statusselect count(channel_id) from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_b_type in ('1','2','3')        select a.channel_type_id id1,a.type_name  一级类型, b.channel_type_id id2,b.type_name 二级类型,
c.channel_type_id id3,c.type_name 三级类型
from 
(select * from bass2.ods_channel_type_20110209 a where parent_type_id=-1) a ,
(select * from bass2.ods_channel_type_20110209 
where parent_type_id in
  (select channel_type_id from bass2.ods_channel_type_20110209 a where parent_type_id=-1 )) b,
(select * from bass2.ods_channel_type_20110209 
where parent_type_id in
  (select channel_type_id from bass2.ods_channel_type_20110209 a 
    where parent_type_id in( select channel_type_id from bass2.ods_channel_type_20110209 a where parent_type_id =-1))) c                  
where  b.parent_type_id=a.channel_type_id
and  c.parent_type_id=b.channel_type_id
order by id1,id2,id3select count(*) from
(
select distinct channel_star from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_type<>'3'
) aa
where channel_star <>''select channel_type,channel_star,count(0) from    bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'  group by  channel_type,channel_star   select count(*) from
(
select distinct channel_star from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_type='3'
) aa
where channel_star=''   select count(distinct channel_id) from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_type='1'     select channel_type,count(distinct channel_id) from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and (longitude='0' or longitude='' or latitude='0' or latitude='')  group by channel_type  order by 1,2    select b.CHANNEL_STATUS, count(distinct a.CHANNEL_ID) from    bass1.g_s_22062_month  a  ,bass1.g_i_06021_month b   where  a.CHANNEL_ID = b.CHANNEL_ID  and  a.time_id =201102  and b.time_id =201102  group by b.CHANNEL_STATUS  order by 1,2      select accept_type,
     sum(bigint(new_users)),
    sum(bigint(hand_cnt)),
    sum(bigint(card_sale_cnt)),
    sum(bigint(accept_cnt)),
    --,sum(bigint(imp_accept_cnt))
    sum(bigint(term_sale_cnt)),
    --,sum(bigint(other_sale_cnt))
    sum(bigint(accept_bas_cnt))
    --,sum(bigint(query_bas_cnt))
from g_s_22062_month
where time_id =201102
group by accept_type    select 
     sum(bigint(new_users)),
    sum(bigint(hand_cnt)),
    sum(bigint(card_sale_cnt)),
    sum(bigint(accept_cnt)),
    --,sum(bigint(imp_accept_cnt))
    sum(bigint(term_sale_cnt)),
    --,sum(bigint(other_sale_cnt))
    sum(bigint(accept_bas_cnt))
    --,sum(bigint(query_bas_cnt))
from g_s_22062_month
where time_id =201102
    select   CHANNEL_STATUS,sum(cnt)
  from (
select accept_type,CHANNEL_STATUS,
     sum(bigint(new_users))+
    sum(bigint(hand_cnt))+
    sum(bigint(card_sale_cnt))+
    sum(bigint(accept_cnt))+
    --+sum(bigint(imp_accept_cnt))
    sum(bigint(term_sale_cnt))+
    --+sum(bigint(other_sale_cnt))
    sum(bigint(accept_bas_cnt)) cnt
    --+sum(bigint(query_bas_cnt)) 
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201102
  and b.time_id =201102
group by accept_type,CHANNEL_STATUS
) t group by CHANNEL_STATUSorder by 1,2
  select   accept_type,sum(cnt)
  from (
select accept_type,CHANNEL_STATUS,
     sum(bigint(new_users))+
    sum(bigint(hand_cnt))+
    sum(bigint(card_sale_cnt))+
    sum(bigint(accept_cnt))+
    --+sum(bigint(imp_accept_cnt))
    sum(bigint(term_sale_cnt))+
    --+sum(bigint(other_sale_cnt))
    sum(bigint(accept_bas_cnt)) cnt
    --+sum(bigint(query_bas_cnt)) 
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201102
  and b.time_id =201102
group by accept_type,CHANNEL_STATUS
) t group by accept_typeorder by 1,2
select max(cnt)from (select a.CHANNEL_ID,
     sum(bigint(new_users))+
    sum(bigint(hand_cnt))+
    sum(bigint(card_sale_cnt))+
    sum(bigint(accept_cnt))+
    --+sum(bigint(imp_accept_cnt))
    sum(bigint(term_sale_cnt))+
    --+sum(bigint(other_sale_cnt))
    sum(bigint(accept_bas_cnt)) cnt
    --+sum(bigint(query_bas_cnt)) 
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201102
  and b.time_id =201102group by a.CHANNEL_ID) t select count(*) from
(
select sum(bigint(card_sale_cnt)) card_sale_cnt
from g_s_22062_month
where time_id =201102
) aa
where card_sale_cnt<=0    select   sum(cnt)
  from (
select accept_type,CHANNEL_STATUS,
     sum(bigint(new_users))+
    sum(bigint(hand_cnt))+
    sum(bigint(card_sale_cnt))+
    sum(bigint(accept_cnt))+
    --sum(bigint(imp_accept_cnt))+
    sum(bigint(term_sale_cnt))+
    --sum(bigint(other_sale_cnt))+
    sum(bigint(accept_bas_cnt)) +
    sum(bigint(query_bas_cnt)) cnt
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201102
  and b.time_id =201102
group by accept_type,CHANNEL_STATUS
) t 
			
--1723732    select 
     sum(bigint(new_users))*1.0000/1723732*100     ,sum(bigint(hand_cnt))*1.0000/1723732*100     ,sum(bigint(card_sale_cnt))*1.0000/1723732*100     ,sum(bigint(accept_cnt))*1.0000/1723732*100     ,sum(bigint(term_sale_cnt))*1.0000/1723732*100     ,sum(bigint(accept_bas_cnt))*1.0000/1723732*100          ,sum(bigint(query_bas_cnt))*1.0000/1723732*100     
from g_s_22062_month
where time_id =201102
  select 
sum(bigint(imp_accept_cnt))
from g_s_22062_month
where time_id =201102
select 
sum(bigint(CNT))
from g_s_22064_month
where time_id =201102
select   CHANNEL_STATUS,sum(cnt)
  from (
select accept_type,CHANNEL_STATUS,
     sum(bigint(new_users))+
    sum(bigint(hand_cnt))+
    sum(bigint(card_sale_cnt))+
    sum(bigint(accept_cnt))+
    --sum(bigint(imp_accept_cnt))+
    sum(bigint(term_sale_cnt))+
    --sum(bigint(other_sale_cnt))+
    sum(bigint(accept_bas_cnt)) +
    sum(bigint(query_bas_cnt)) cnt
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201102
  and b.time_id =201102
group by accept_type,CHANNEL_STATUS
) t group by CHANNEL_STATUS
order by 1,2
			
   select IMP_ACCEPTTYPE , sum(bigint(cnt)) 
--,  count(distinct IMP_ACCEPTTYPE ) 
from g_s_22064_month 
group by  IMP_ACCEPTTYPE 
order by 1 
  select ACCEPT_TYPE , count(0) 
--,  count(distinct IMP_ACCEPTTYPE ) 
from g_s_22064_month 
group by  ACCEPT_TYPE 
order by 1 
g_s_22065_monthselect sum(bigint(cnt))
from g_s_22064_month
where time_id =201102
  and imp_accepttype='23'    select sum(bigint(cnt))
from g_s_22064_month
where time_id =201102
  and imp_accepttype='23'       select busi_type,apply_type,count(distinct product_no) from g_i_02053_month
where time_id =201102
  and busi_type in ('09','14','15')
  and sts='0'
  and VALID_DATE between '20110201' and '20110231'
  and EXPIRE_DATE>='20110201'
group by busi_type,apply_type
    select busi_type,apply_type,count(distinct product_no) from g_i_02053_month
where time_id =201102
  and busi_type in ('14')
  and sts='0'
  and VALID_DATE between '20110201' and '20110231'
  and EXPIRE_DATE>='20110201'
group by busi_type,apply_type--14	00	2671
  select busi_type,apply_type,sts,count(distinct product_no) from g_i_02053_month
where time_id =201102
  and busi_type in ('14')
  --and sts='0'
 and VALID_DATE like '201102%'
  --and EXPIRE_DATE>='20110201'
group by busi_type,apply_type,sts2695  select count(0), count(distinct product_no) from g_i_02053_month
where time_id =201102
  and busi_type in ('14')   and VALID_DATE like '201102%'     select 
sum(bigint(CNT))
from g_s_22064_month
where time_id =201102and IMP_ACCEPTTYPE = '06'  select 
sum(bigint(CNT))
from g_s_22064_month
where time_id =201102and IMP_ACCEPTTYPE = '07' --2731select 
         TIME_ID
        ,sum(bigint(ZZHI_CNT          ))
        +sum(bigint(TERM_CNT          ))
        +sum(bigint(CARD_CNT          ))
        +sum(bigint(PAYMENT_CNT       ))
        +sum(bigint(OTHER_CNT         ))
        /**,sum(bigint(E_PAY_AMOUNT      ))
        ,sum(bigint(O_TERM_AMOUNT     ))
        ,sum(bigint(ZHI_CUST_CNT      ))
        ,sum(bigint(E_CUST_CNT        ))
        ,sum(bigint(TX_CUST_CNT       ))**/
from g_s_22065_month
where TIME_ID = 201102
group by   TIME_ID  select 
         TIME_ID
        ,sum(bigint(ZZHI_CNT          ))
        +sum(bigint(TERM_CNT          ))
        +sum(bigint(CARD_CNT          ))
        +sum(bigint(PAYMENT_CNT       ))
        +sum(bigint(OTHER_CNT         ))
from g_s_22065_month
where TIME_ID = 201102
group by   TIME_ID
select   accept_type,sum(cnt)
  from (
select accept_type,CHANNEL_STATUS,
     sum(bigint(new_users))+
    sum(bigint(hand_cnt))+
    sum(bigint(card_sale_cnt))+
    sum(bigint(accept_cnt))+
    --+sum(bigint(imp_accept_cnt))
    sum(bigint(term_sale_cnt))+
    --+sum(bigint(other_sale_cnt))
    sum(bigint(accept_bas_cnt))+
    sum(bigint(query_bas_cnt)) cnt
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201102
  and b.time_id =201102
group by accept_type,CHANNEL_STATUS
) t where accept_type = '1'group by accept_type
order by 1,2
      select length(trim(channel_id)),count(0) from   bass1.g_i_06021_month  where time_id = 201102  group by length(trim(channel_id))          select * from   bass1.g_i_06021_month  where time_id = 201102 and length(trim(channel_id)) = 7     
declare global temporary table session.int_check_user_status
    (
   user_id        CHARACTER(15),
   product_no     CHARACTER(15),
   test_flag      CHARACTER(1),
   sim_code       CHARACTER(15),
   usertype_id    CHARACTER(4),
   create_date    CHARACTER(15),
   brand_id       CHARACTER(4),
   time_id        int,
   CHANNEL_ID     varchar(25)
    )                            
partitioning key           
 (
   user_id    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp


select * from   bass1.g_a_02004_day

--抓取用户资料入表
insert into session.int_check_user_status (
     user_id    
    ,product_no 
    ,test_flag  
    ,sim_code   
    ,usertype_id  
    ,create_date
    ,brand_id
    ,time_id
    ,CHANNEL_ID    )
select e.user_id
    ,e.product_no  
    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
    ,e.sim_code
    ,f.usertype_id  
    ,e.create_date  
    ,e.brand_id
    ,f.time_id  
    ,e.CHANNEL_ID     
from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id,CHANNEL_ID
                ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.g_a_02004_day
where time_id<=20110231 ) e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where time_id<=20110231 ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1
commit


select count(*) from 
(
select distinct channel_id from session.int_check_user_status
where create_date between '20110201' and '20110231'
and test_flag='0'
) aa
where aa.channel_id not in
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102)
      select distinct channel_id from session.int_check_user_status
where create_date between '20110201' and '20110231'
and test_flag='0'except select distinct channel_id from bass1.g_i_06021_month where time_id =201102113select count(0),count(distinct CHANNEL_ID),count(distinct ORGANIZE_ID) from    bass2.dw_channel_info_2011027411	7411	7404
select * from    bass2.dw_channel_info_201102where char(CHANNEL_ID) in  ( select distinct channel_id from session.int_check_user_status
where create_date between '20110201' and '20110231'
and test_flag='0'except select distinct channel_id from bass1.g_i_06021_month where time_id =201102)select * from    g_s_22065_month
where TIME_ID = 201102select distinct e.channel_id				  from bass2.dw_product_ord_cust_201102 a,
				       bass2.dw_product_ord_offer_dm_201102 b,
				       bass2.dim_prod_up_product_item d,
				       bass2.dim_pub_channel e,
				       bass2.dim_sys_org_role_type f,
				       bass2.dim_cfg_static_data g,
				       bass2.dw_channel_info_201102 h
				 where a.customer_order_id = b.customer_order_id
				   and b.offer_id = d.product_item_id
				   and a.org_id = e.channel_id
				   and a.channel_type = g.code_value
				   and g.code_type = '911000'
				   and e.channeltype_id = f.org_role_type_id
				   and a.org_id = h.channel_id
				   and h.channel_type_class in (90105,90102)
				   and a.channel_type in ('Q','L','I','e','D','B','6','4')select * from bass2.dw_product_201102 where char(channel_id) = '381716'                                                        select * from   bass2.dw_channel_info_201102 where   char(channel_id) = '381716'              select * from   bass2.dw_channel_info_201102 where   char(organize_id) = '381716'                 select * from    session.int_check_user_status where      char(channel_id) = '381716'       select * from   bass2.dw_channel_info_201102 where   char(channel_id) = '381716'        select * from   bass2.ODS_DIM_SYS_ORGANIZE_20110410 where   char(organize_id) = '381716'     select count(channel_id) from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_type='4'

select count(channel_id) from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_b_type in ('4')
select count(channel_id) from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_b_type in ('5','6')
--6551

select count(channel_id) from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_type='3'
--6312
select count(channel_id) from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_b_type in ('1','2','3')

133select count(channel_id) from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_type in ('1','2')  select staff_org_id
    ,case when opt_code in ('4464','4465','4864','4865') then '2' else '1' end
    ,sum(case when CERTIFICATE_TYPE='6' then 1 else 0 end )
    ,sum(case when CERTIFICATE_TYPE='6' then amount else 0 end )
from BASS2.dw_acct_payment_dm_201102
group by staff_org_id
        ,case when opt_code in ('4464','4465','4864','4865') then '2' else '1' end

insert into  SESSION.G_S_22062_MONTH_TMP_1
          (  	CHANNEL_ID
						, ACCEPT_TYPE
						, NEW_USERS
						, HAND_CNT
						, HAND_FEE
						, CARD_SALE_CNT
						, CARD_SALE_FEE
						, ACCEPT_CNT
						, IMP_ACCEPT_CNT
						, TERM_SALE_CNT
						, OTHER_SALE_CNT
						, ACCEPT_BAS_CNT
						, QUERY_BAS_CNT
						, OFF_ACCEPT_CNT    )
			select channel_id
						,'1'
						,count(1)
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
			from bass2.dw_product_$op_month
			where month_new_mark = 1
			group by Channel_ID            select * from   g_i_06021_monthselect count(0)from (select char(a.CHANNEL_ID),char(user_id)from bass2.dw_product_201102 a,g_i_06021_month b 
where month_new_mark = 1and char(a.CHANNEL_ID) = trim(b.CHANNEL_ID)and b.time_id = 201102) t --52916select count(0)from (select  char(CHANNEL_ID),char(user_id)from session.int_check_user_statuswhere create_date between '20110201' and '20110231'
and test_flag='0') t 52906select char(CHANNEL_ID),char(user_id)from bass2.dw_product_201102
where month_new_mark = 1exceptselect  char(CHANNEL_ID),char(user_id)from session.int_check_user_statuswhere create_date between '20110201' and '20110231'
and test_flag='0'select  char(CHANNEL_ID),char(user_id)from session.int_check_user_statuswhere create_date between '20110201' and '20110231'
and test_flag='0'exceptselect char(CHANNEL_ID),char(user_id)from bass2.dw_product_201102
where month_new_mark = 1select count(distinct user_id) from session.int_check_user_status
where create_date between '20110201' and '20110231'
and test_flag='0'select  char(CHANNEL_ID),char(user_id)from session.int_check_user_statuswhere create_date between '20110201' and '20110231'
and test_flag='0'exceptselect char(a.CHANNEL_ID),char(user_id)from bass2.dw_product_201102 a,g_i_06021_month b 
where month_new_mark = 1and char(a.CHANNEL_ID) = trim(b.CHANNEL_ID)and b.time_id = 201102select * from BASS2.ETL_SEND_MESSAGE
select * from   BASS1.G_I_06023_MONTHselect * from   bass2.ods_channel_info_20110410select * from   bass2.Dim_channel_infoselect * from   g_s_22061_monthselect * from   g_i_06023_month22062select * from   g_s_22062_monthselect * from   g_a_01004_dayCREATE TABLE BASS1.g_a_01004_tmp1
	              (
	               enterprise_id       varchar(20),
                   numbers             int
	              )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (enterprise_id) USING HASHING

ALTER TABLE BASS1.g_a_01004_tmp1
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE        CREATE TABLE BASS1.g_a_01004_tmp2
	              (
	               enterprise_id       varchar(20),
                   manager_id          varchar(20)
	              )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (enterprise_id) USING HASHING

ALTER TABLE BASS1.g_a_01004_tmp2
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE       CREATE TABLE BASS1.g_a_01004_tmp10
	              (
	               enterprise_id       varchar(20)	              
	               )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (enterprise_id) USING HASHING

ALTER TABLE BASS1.g_a_01004_tmp10
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
    select count(0),count(distinct enterprise_id)  from  bass2.dwd_enterprise_msg_his_20110411    
去重一法:
 select count(0) from
   (select time_id,enterprise_id,cust_statu_typ_id from G_A_01004_DAY where time_id <= 20110411 ) a,
   (select enterprise_id,max(time_id) as time_id  from G_A_01004_DAY 
                                                 where time_id<=20110411  
                                              group by enterprise_id)b
where a.time_id=b.time_id and a.enterprise_id=b.enterprise_id and a.cust_statu_typ_id = '20'

更有效的一法：

 select count(0) 
from 
(
select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn 
from bass1.G_A_01004_DAY 
where time_id <= 20110411 
) t where t.rn = 1 and  cust_statu_typ_id = '20'
  select enterprise_id
from 
(
select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn 
from bass1.G_A_01004_DAY 
where time_id <= 20110411 
) t where t.rn = 1 and  cust_statu_typ_id = '20'except  select enterprise_id
from 
(
select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn 
from bass1.G_A_01004_DAY 
where time_id <= 20110411 
) t where t.rn = 1 and  cust_statu_typ_id = '20'select distinct t.enterprise_id
from 
(
select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn 
from bass1.G_A_01004_DAY 
where time_id <= ${timestamp}   
) t where t.rn = 1 and  cust_statu_typ_id = '20'select count(distinct a.enterprise_id) from
                 (select time_id,enterprise_id,cust_statu_typ_id from G_A_01004_DAY where time_id <= 20110411) a,
                 (select enterprise_id,max(time_id) as time_id  from G_A_01004_DAY 
                                                                 where time_id<=20110411
															                                group by enterprise_id)b
                where a.time_id=b.time_id and a.enterprise_id=b.enterprise_id and a.cust_statu_typ_id = '20' CREATE table  bass1.g_a_01004_tmp11 like bass2.dwd_enterprise_msg_his_20110411
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (CUST_ID
   ) USING HASHING     	select 
  	     ENTERPRISE_ID
        ,GROUP_NAME
        ,PASSWORD
        ,GROUP_LEVEL
        ,GROUP_TYPE
        ,REGION_SPECIA
        ,GROUP_STATUS
        ,LEVEL_DEF_MODE
        ,REC_STATUS
        ,VOCATION
        ,VOCATION2
        ,VOCATION3
        ,GROUP_COUNTRY
        ,GROUP_PROVINCE
        ,GROUP_CITY
        ,REGION_TYPE
        ,REGION_DETAIL
        ,GROUP_ADDRESS
        ,GROUP_POSTCODE
        ,POST_PROVINCE
        ,POST_CITY
        ,POST_ADDRESS
        ,POST_POSTCODE
        ,PHONE_ID
        ,FAX_ID
        ,IDEN_ID
        ,IDEN_NBR
        ,OWNER_NAME
        ,TAX_ID
        ,NET_ADDRESS
        ,PAY_TYPE
        ,EMAIL
        ,CREATE_DATE
        ,DONE_DATE
        ,VALID_DATE
        ,EXPIRE_DATE
        ,OP_ID
        ,ORG_ID
        ,SO_NBR
        ,CUST_ID
        ,NOTES
        ,VPMN_ID
        ,EXT_FIELD1
        ,EXT_FIELD2
        ,EXT_FIELD3
        ,EXT_FIELD4
        ,EXT_FIELD5
        ,EXT_FIELD6
        ,EXT_FIELD7
        ,EXT_FIELD8
        ,EXT_FIELD9
        ,EXT_FIELD10
				from 
				(
				select a.*,row_number()over(partition by enterprise_id order by done_date desc) rn 
				from bass2.dwd_enterprise_msg_his_20110411 a
				) t 
				where t.rn = 1 
					and  t.rec_status = 0 
					and t.enterprise_id in (select enterprise_id from bass1.g_a_01004_tmp10)                                         select count(0) from     bass1.g_a_01004_day6681157select count(0) from     bass1.g_a_01004_day_20110412bak6681157                                        select time_id , count(0) 
from bass1.g_a_01004_day where time_id > 20110101
group by  time_id 
order by 1 desc           20110411	11215
           20110411	11705
20110410	11703
select time_id , count(0) 
--,  count(distinct time_id ) 
from bass1.g_a_01004_day_20110412bak where time_id > 20110101
group by  time_id 
order by 1 20110411	11705
20110410	11703
select * from   bass1.g_a_01004_day_20110412bak where time_id = 20110411exceptselect * from   bass1.g_a_01004_day where time_id = 20110411select * from   bass1.g_a_01004_day where time_id = 20110411exceptselect * from   bass1.g_a_01004_day_20110412bak where time_id = 20110411select count(0)
                                from BASS2.DW_ENTERPRISE_ACCOUNT_20110411 where REC_STATUS=1select * from   bass1.g_a_01004_tmp10 where ENTERPRISE_ID = '891930005308'select * from   bass1.g_a_01004_tmp11 where ENTERPRISE_ID = '891930005308'                                        select *    				from 
				(
				select a.*,row_number()over(partition by enterprise_id order by done_date desc) rn 
				from bass2.dwd_enterprise_msg_his_20110411 a
				) t                                 where ENTERPRISE_ID = '891930005308'                                        				and t.rn = 1 
					and  t.rec_status = 0                    select *    				from 
				(
				select a.*,row_number()over(partition by enterprise_id order by done_date desc) rn 
				from bass2.dwd_enterprise_msg_his_20110411 a
				) t                                 where ENTERPRISE_ID = '891930005308'                                        				and t.rn = 1 
					and  t.rec_status = 0 select rec_statusfrom (select a.* from 
(select * from bass2.dwd_enterprise_msg_his_20110411)  a,
(select enterprise_id,max(done_date) as done_date from bass2.dwd_enterprise_msg_his_20110411  group by enterprise_id) b
where a.done_date = b.done_date and a.enterprise_id = b.enterprise_id and rec_status = 0 and  a.enterprise_id in (select * from bass1.g_a_01004_tmp10) 
)a where ENTERPRISE_ID = '891930005308'                                        					and  a.rec_status = 0 select enterprise_id,max(done_date) as done_date,rec_status from bass2.dwd_enterprise_msg_his_20110411 where ENTERPRISE_ID = '891930005308'   group by enterprise_id ,rec_status                     
CREATE TABLE BASS1.g_a_01004_tmp3
 (TIME_ID            INTEGER,
  ENTERPRISE_ID      CHARACTER(20),
  ENT_DEF_MODE       CHARACTER(1),
  PRT_GRP_CUST_ID    CHARACTER(20),
  ENTERPRISE_NAME    CHARACTER(60),
  OWNER_NAME         CHARACTER(20),
  NET_ADDRESS        CHARACTER(250),
  FAX_NO             CHARACTER(20),
  ENT_SCALE_ID       CHARACTER(1),
  MEMBER_NUMS        CHARACTER(8),
  ENT_REGION_TYPE    CHARACTER(2),
  ENT_INDUSTRY_ID    CHARACTER(2),
  GRP_AREA_SPEC_ID   CHARACTER(2),
  ENT_MANAGER_ID     CHARACTER(20),
  CMCC_ID            CHARACTER(5),
  CREATE_DATE        CHARACTER(8),
  LINKMAN_NAME       CHARACTER(20),
  TELEPHONE_NUM      CHARACTER(20),
  MOBILE_NUM         CHARACTER(15),
  LINKMAN_TITLE      CHARACTER(20),
  LINKMAN_FAX        CHARACTER(20),
  LINKMAN_MAIL       CHARACTER(50),
  LINKMAN_ADDR       CHARACTER(70),
  POST_CODE          CHARACTER(6),
  CUST_STATU_TYP_ID  CHARACTER(2),
  UNITE_PAY_FLAG     CHARACTER(1),
  IND_RES_SCHEMA     CHARACTER(100),
  CRT_CHNL_ID        CHARACTER(25),
  ENTER_TYPE_ID      CHARACTER(2)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,
    ENTERPRISE_ID
   ) USING HASHING

ALTER TABLE BASS1.g_a_01004_tmp3
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE

                                                                            
CREATE TABLE BASS1.g_a_01004_tmp12
 (TIME_ID            INTEGER,
  ENTERPRISE_ID      CHARACTER(20),
  ENT_DEF_MODE       CHARACTER(1),
  PRT_GRP_CUST_ID    CHARACTER(20),
  ENTERPRISE_NAME    CHARACTER(60),
  OWNER_NAME         CHARACTER(20),
  NET_ADDRESS        CHARACTER(250),
  FAX_NO             CHARACTER(20),
  ENT_SCALE_ID       CHARACTER(1),
  MEMBER_NUMS        CHARACTER(8),
  ENT_REGION_TYPE    CHARACTER(2),
  ENT_INDUSTRY_ID    CHARACTER(2),
  GRP_AREA_SPEC_ID   CHARACTER(2),
  ENT_MANAGER_ID     CHARACTER(20),
  CMCC_ID            CHARACTER(5),
  CREATE_DATE        CHARACTER(8),
  LINKMAN_NAME       CHARACTER(20),
  TELEPHONE_NUM      CHARACTER(20),
  MOBILE_NUM         CHARACTER(15),
  LINKMAN_TITLE      CHARACTER(20),
  LINKMAN_FAX        CHARACTER(20),
  LINKMAN_MAIL       CHARACTER(50),
  LINKMAN_ADDR       CHARACTER(70),
  POST_CODE          CHARACTER(6),
  CUST_STATU_TYP_ID  CHARACTER(2),
  UNITE_PAY_FLAG     CHARACTER(1),
  IND_RES_SCHEMA     CHARACTER(100),
  CRT_CHNL_ID        CHARACTER(25),
  ENTER_TYPE_ID      CHARACTER(2)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,
    ENTERPRISE_ID
   ) USING HASHING

ALTER TABLE BASS1.g_a_01004_tmp12
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
select count(0) from    bass1.g_a_01004_tmp10 select 
  	     ENTERPRISE_ID
        ,GROUP_NAME
        ,PASSWORD
        ,GROUP_LEVEL
        ,GROUP_TYPE
        ,REGION_SPECIA
        ,GROUP_STATUS
        ,LEVEL_DEF_MODE
        ,REC_STATUS
        ,VOCATION
        ,VOCATION2
        ,VOCATION3
        ,GROUP_COUNTRY
        ,GROUP_PROVINCE
        ,GROUP_CITY
        ,REGION_TYPE
        ,REGION_DETAIL
        ,GROUP_ADDRESS
        ,GROUP_POSTCODE
        ,POST_PROVINCE
        ,POST_CITY
        ,POST_ADDRESS
        ,POST_POSTCODE
        ,PHONE_ID
        ,FAX_ID
        ,IDEN_ID
        ,IDEN_NBR
        ,OWNER_NAME
        ,TAX_ID
        ,NET_ADDRESS
        ,PAY_TYPE
        ,EMAIL
        ,CREATE_DATE
        ,DONE_DATE
        ,VALID_DATE
        ,EXPIRE_DATE
        ,OP_ID
        ,ORG_ID
        ,SO_NBR
        ,CUST_ID
        ,NOTES
        ,VPMN_ID
        ,EXT_FIELD1
        ,EXT_FIELD2
        ,EXT_FIELD3
        ,EXT_FIELD4
        ,EXT_FIELD5
        ,EXT_FIELD6
        ,EXT_FIELD7
        ,EXT_FIELD8
        ,EXT_FIELD9
        ,EXT_FIELD10
				from bass2.dwd_enterprise_msg_his_20110411 a,
        (select enterprise_id,max(done_date) as done_date from bass2.dwd_enterprise_msg_his_20110411  group by enterprise_id) b
        where a.done_date = b.done_date 
        			and a.enterprise_id = b.enterprise_id and rec_status = 0 
        			and  a.enterprise_id in (select t.enterprise_id from bass1.g_a_01004_tmp10 t) 
				with ur                                       
select 
  	     a.ENTERPRISE_ID
        ,a.GROUP_NAME
        ,a.PASSWORD
        ,a.GROUP_LEVEL
        ,a.GROUP_TYPE
        ,a.REGION_SPECIA
        ,a.GROUP_STATUS
        ,a.LEVEL_DEF_MODE
        ,a.REC_STATUS
        ,a.VOCATION
        ,a.VOCATION2
        ,a.VOCATION3
        ,a.GROUP_COUNTRY
        ,a.GROUP_PROVINCE
        ,a.GROUP_CITY
        ,a.REGION_TYPE
        ,a.REGION_DETAIL
        ,a.GROUP_ADDRESS
        ,a.GROUP_POSTCODE
        ,a.POST_PROVINCE
        ,a.POST_CITY
        ,a.POST_ADDRESS
        ,a.POST_POSTCODE
        ,a.PHONE_ID
        ,a.FAX_ID
        ,a.IDEN_ID
        ,a.IDEN_NBR
        ,a.OWNER_NAME
        ,a.TAX_ID
        ,a.NET_ADDRESS
        ,a.PAY_TYPE
        ,a.EMAIL
        ,a.CREATE_DATE
        ,a.DONE_DATE
        ,a.VALID_DATE
        ,a.EXPIRE_DATE
        ,a.OP_ID
        ,a.ORG_ID
        ,a.SO_NBR
        ,a.CUST_ID
        ,a.NOTES
        ,a.VPMN_ID
        ,a.EXT_FIELD1
        ,a.EXT_FIELD2
        ,a.EXT_FIELD3
        ,a.EXT_FIELD4
        ,a.EXT_FIELD5
        ,a.EXT_FIELD6
        ,a.EXT_FIELD7
        ,a.EXT_FIELD8
        ,a.EXT_FIELD9
        ,a.EXT_FIELD10
				from bass2.dwd_enterprise_msg_his_20110411 a,
        (select enterprise_id,max(done_date) as done_date from bass2.dwd_enterprise_msg_his_20110411  group by enterprise_id) b
        where a.done_date = b.done_date 
        			and a.enterprise_id = b.enterprise_id and rec_status = 0 
        			and  a.enterprise_id in (select t.enterprise_id from bass1.g_a_01004_tmp10 t) 
				with ur                                                  
CREATE TABLE BASS1.g_a_01004_tmp4
 (TIME_ID            INTEGER,
  ENTERPRISE_ID      CHARACTER(20),
  ENT_DEF_MODE       CHARACTER(1),
  PRT_GRP_CUST_ID    CHARACTER(20),
  ENTERPRISE_NAME    CHARACTER(60),
  OWNER_NAME         CHARACTER(20),
  NET_ADDRESS        CHARACTER(250),
  FAX_NO             CHARACTER(20),
  ENT_SCALE_ID       CHARACTER(1),
  MEMBER_NUMS        CHARACTER(8),
  ENT_REGION_TYPE    CHARACTER(2),
  ENT_INDUSTRY_ID    CHARACTER(2),
  GRP_AREA_SPEC_ID   CHARACTER(2),
  ENT_MANAGER_ID     CHARACTER(20),
  CMCC_ID            CHARACTER(5),
  CREATE_DATE        CHARACTER(8),
  LINKMAN_NAME       CHARACTER(20),
  TELEPHONE_NUM      CHARACTER(20),
  MOBILE_NUM         CHARACTER(15),
  LINKMAN_TITLE      CHARACTER(20),
  LINKMAN_FAX        CHARACTER(20),
  LINKMAN_MAIL       CHARACTER(50),
  LINKMAN_ADDR       CHARACTER(70),
  POST_CODE          CHARACTER(6),
  CUST_STATU_TYP_ID  CHARACTER(2),
  UNITE_PAY_FLAG     CHARACTER(1),
  IND_RES_SCHEMA     CHARACTER(100),
  CRT_CHNL_ID        CHARACTER(25),
  ENTER_TYPE_ID      CHARACTER(2)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,
    ENTERPRISE_ID
   ) USING HASHING

ALTER TABLE BASS1.g_a_01004_tmp4
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE                    			DECLARE GLOBAL TEMPORARY TABLE SESSION.G_S_22062_MONTH_TMP_1
          (  	CHANNEL_ID      		BIGINT   ,
              ACCEPT_TYPE         CHAR(1) ,
              NEW_USERS          bigint		,
						  HAND_CNT           bigint		,
						  HAND_FEE           bigint		,
						  CARD_SALE_CNT      bigint		,
						  CARD_SALE_FEE      bigint		,
						  ACCEPT_CNT         bigint		,
						  IMP_ACCEPT_CNT     bigint		,
						  TERM_SALE_CNT      bigint		,
						  OTHER_SALE_CNT     bigint		,
						  ACCEPT_BAS_CNT     bigint		,
						  QUERY_BAS_CNT      bigint		,
						  OFF_ACCEPT_CNT     bigint
           )
      PARTITIONING KEY (CHANNEL_ID,ACCEPT_TYPE) USING HASHING
      WITH REPLACE ON COMMIT PRESERVE ROWS NOT LOGGED IN TBS_USER_TEMP    					create table bass1.G_S_22062_MONTH_TMP_1bak like session.G_S_22062_MONTH_TMP_1
					DATA CAPTURE NONE
					IN TBS_APP_BASS1
					INDEX IN TBS_INDEX
					PARTITIONING KEY (CHANNEL_ID,ACCEPT_TYPE) USING HASHING                                        					insert into  bass1.G_S_22062_MONTH_TMP_1bak select * from session.G_S_22062_MONTH_TMP_1
select count(0),count(distinct a.channel_id) from    bass1.G_S_22062_MONTH_TMP_1bak a                    select distinct char(a.channel_id) from    bass1.G_S_22062_MONTH_TMP_1bak aleft join BASS2.DW_CHANNEL_INFO_201102 b on a.channel_id = b.channel_id where b.channel_id is nullunion   select distinct char(channel_id) from session.int_check_user_status
where create_date between '20110201' and '20110231'
and test_flag='0'except select distinct channel_id from bass1.g_i_06021_month where time_id =201102                    ods_channel_info_                    --DW_CHANNEL_INFO_201103 有，在ODS_DIM_SYS_ORGANIZE_20110411找不到的select * from BASS2.DW_CHANNEL_INFO_201103 a where not exists (select 1 from BASS2.ODS_DIM_SYS_ORGANIZE_20110411 b where a.organize_id = b.organize_id and b.STATE=1)and CHANNEL_STATE=1--在ODS_DIM_SYS_ORGANIZE_20110411有，但DW_CHANNEL_INFO_201103 没有的select * from BASS2.ODS_DIM_SYS_ORGANIZE_20110411 a where not exists (select 1 from BASS2.DW_CHANNEL_INFO_201103 b where a.organize_id = b.organize_id and b.CHANNEL_STATE=1)and a.state=1--在dw_product_201103有，但ODS_DIM_SYS_ORGANIZE_20110411没有的select distinct a.channel_id from BASS2.dw_product_201103 a where not exists (select 1 from bass2.ODS_DIM_SYS_ORGANIZE_20110411 b where a.channel_id = b.organize_id)and userstatus_id in (1,2,3,6,8) and test_mark=0--在dw_product_201103有，但DW_CHANNEL_INFO_201103没有的select distinct a.channel_id from BASS2.dw_product_201103 a where not exists (select 1 from BASS2.DW_CHANNEL_INFO_201103 b where a.channel_id = b.channel_id)and userstatus_id in (1,2,3,6,8) and test_mark=0                    select * from BASS2.ODS_CHANNEL_INFO_20110411 a where not exists (select 1 from BASS2.ODS_DIM_SYS_ORGANIZE_20110411 b where a.organize_id = b.organize_id and b.STATE=1)
and CHANNEL_STATE=1
--DW_CHANNEL_INFO有，在ODS_DIM_SYS_ORGANIZE找不到的,即无相应的organize_id信息
select a.channel_id,a.organize_id from BASS2.ODS_CHANNEL_INFO_20110411 a where not exists (select 1 from BASS2.ODS_DIM_SYS_ORGANIZE_20110411 b where a.organize_id = b.organize_id and b.STATE=1)
and CHANNEL_STATE=1

--在dw_product有，但DW_CHANNEL_INFO没有的,(导致开户渠道标识不在06021中,校验失败)
select distinct a.channel_id from BASS2.dw_product_201103 a where not exists (select 1 from BASS2.ODS_CHANNEL_INFO_20110411 b where a.channel_id = b.channel_id)
and userstatus_id in (1,2,3,6,8) and test_mark=0
        
        
--在ODS_DIM_SYS_ORGANIZE有，但DW_CHANNEL_INFO没有的 ,即无相应的channel_id信息
select a.organize_id from BASS2.ODS_DIM_SYS_ORGANIZE_20110411 a where not exists (select 1 from BASS2.ODS_CHANNEL_INFO_20110411 b where a.organize_id = b.organize_id and b.CHANNEL_STATE=1)
and a.state=1

--在dw_product有，但ODS_DIM_SYS_ORGANIZE没有的
select distinct a.channel_id from BASS2.dw_product_201103 a where not exists (select 1 from bass2.ODS_DIM_SYS_ORGANIZE_20110411 b where a.channel_id = b.organize_id)
and userstatus_id in (1,2,3,6,8) and test_mark=0

                        
CREATE TABLE BASS1.dim_21003_ip_type
 (
 	ip_type_id varchar(4)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ip_type_id) USING HASHING
   
ALTER TABLE BASS1.dim_21003_ip_type
LOCKSIZE ROW
APPEND OFF
NOT VOLATILE
     select * from   BASS1.dim_21003_ip_type            
select count(0)
from (
select
                                        1
					,PRODUCT_NO
					,case when ip_type_id not in ('1000', '2000', '2100', '2101', '2102', '2199', '2200', '2201', 
                                        '2202', '2203', '2204', '2299', '3000', '3100', '3101', '3102', 
                                        '3103', '3104', '3105', '3106', '3107', '3108', '3109', '3110', 
                                        '3111', '3112', '3113', '3114', '3199', '3200', '4000', '4100', 
                                        '4101', '4102', '4103', '4104', '4105', '4106', '4107', '4108', 
                                        '4109', '4110', '4111', '4199', '4200', '6000', '6100', '6101', 
                                        '6102', '6103', '6104', '6105', '6106', '6107', '6199', '6200', 
                                        '9000' ) 
                then '9000' else ip_type_id end
				FROM
					bass1.int_210012916_201103 a 
          WHERE op_time=20110331
				 GROUP BY
					PRODUCT_NO
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,ip_type_id
					,adversary_id
					,roam_type_id
					,call_type_id
) t             31126113：56lite1:14

select count(0)
from (
select
                                        1
					,PRODUCT_NO
				  ,case when b.ip_type_id is not null then b.ip_type_id else '9000' end 
					/**,case when ip_type_id not in ('1000', '2000', '2100', '2101', '2102', '2199', '2200', '2201', 
                                        '2202', '2203', '2204', '2299', '3000', '3100', '3101', '3102', 
                                        '3103', '3104', '3105', '3106', '3107', '3108', '3109', '3110', 
                                        '3111', '3112', '3113', '3114', '3199', '3200', '4000', '4100', 
                                        '4101', '4102', '4103', '4104', '4105', '4106', '4107', '4108', 
                                        '4109', '4110', '4111', '4199', '4200', '6000', '6100', '6101', 
                                        '6102', '6103', '6104', '6105', '6106', '6107', '6199', '6200', 
                                        '9000' ) 
                then '9000' else ip_type_id end **/
				FROM
					bass1.int_210012916_201103 a 
					left join bass1.dim_21003_ip_type b on a.ip_type_id = b.ip_type_id
          WHERE a.op_time=20110331
				 GROUP BY
					PRODUCT_NO
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,case when b.ip_type_id is not null then b.ip_type_id else '9000' end 
					,toll_type_id
					,adversary_id
					,roam_type_id
					,call_type_id
) t 					2:033112611lite1:09select ip_type_id,count(0)from bass1.int_210012916_201103  awhere  a.op_time=20110331group by ip_type_idIP_TYPE_ID	2
1000	7269836
2102	57771
2202	1
4101	2
4102	5
4103	1
4104	2
6101	1
6104	28
select * from   bass1.int_210012916_201103 select ip_type_id,count(0)from bass1.int_210012916_201103  awhere  a.op_time=20110331group by ip_type_id

select ip_type_id,count(0)
from (
select
                                        1
					,PRODUCT_NO
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
				  ,case when b.ip_type_id is not null then b.ip_type_id else '9000' end  ip_type_id
					/**,case when ip_type_id not in ('1000', '2000', '2100', '2101', '2102', '2199', '2200', '2201', 
                                        '2202', '2203', '2204', '2299', '3000', '3100', '3101', '3102', 
                                        '3103', '3104', '3105', '3106', '3107', '3108', '3109', '3110', 
                                        '3111', '3112', '3113', '3114', '3199', '3200', '4000', '4100', 
                                        '4101', '4102', '4103', '4104', '4105', '4106', '4107', '4108', 
                                        '4109', '4110', '4111', '4199', '4200', '6000', '6100', '6101', 
                                        '6102', '6103', '6104', '6105', '6106', '6107', '6199', '6200', 
                                        '9000' ) 
                then '9000' else ip_type_id end **/
					,adversary_id
					,roam_type_id
					,call_type_id
					,char(bigint(sum(call_counts		)))
					,char(bigint(sum(base_bill_duration	)))
					,char(bigint(sum(toll_bill_duration	)))
					,char(bigint(sum(call_duration		)))
					,char(bigint(sum(base_call_fee		)))
					,char(bigint(sum(toll_call_fee		)))
					,char(bigint(sum(callfw_toll_fee	)))
					,char(bigint(sum(call_fee		)))
					,char(bigint(sum(favoured_basecall_fee	)))
					,char(bigint(sum(favoured_tollcall_fee	)))
					,char(bigint(sum(favoured_callfw_tollfee)))
					,char(bigint(sum(favoured_call_fee	)))
					,char(bigint(sum(free_duration		)))
					,char(bigint(sum(favour_duration	)))
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
				FROM
					bass1.int_210012916_201103 a 
					left join bass1.dim_21003_ip_type b on a.ip_type_id = b.ip_type_id
          WHERE a.op_time=20110331
				 GROUP BY
					PRODUCT_NO
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,case when b.ip_type_id is not null then b.ip_type_id else '9000' end 
					,adversary_id
					,roam_type_id
					,call_type_id
) t 					
group by ip_type_id
IP_TYPE_ID	2
1000	3074539
2102	38036
2202	1
4101	2
4102	5
4103	1
4104	2
6101	1
6104	24
2:26
select ip_type_id,count(0)
from (
select
                                        1
					,PRODUCT_NO
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,case when ip_type_id not in ('1000', '2000', '2100', '2101', '2102', '2199', '2200', '2201', 
                                        '2202', '2203', '2204', '2299', '3000', '3100', '3101', '3102', 
                                        '3103', '3104', '3105', '3106', '3107', '3108', '3109', '3110', 
                                        '3111', '3112', '3113', '3114', '3199', '3200', '4000', '4100', 
                                        '4101', '4102', '4103', '4104', '4105', '4106', '4107', '4108', 
                                        '4109', '4110', '4111', '4199', '4200', '6000', '6100', '6101', 
                                        '6102', '6103', '6104', '6105', '6106', '6107', '6199', '6200', 
                                        '9000' ) 
                then '9000' else ip_type_id end ip_type_id
					,adversary_id
					,roam_type_id
					,call_type_id
					,char(bigint(sum(call_counts		)))
					,char(bigint(sum(base_bill_duration	)))
					,char(bigint(sum(toll_bill_duration	)))
					,char(bigint(sum(call_duration		)))
					,char(bigint(sum(base_call_fee		)))
					,char(bigint(sum(toll_call_fee		)))
					,char(bigint(sum(callfw_toll_fee	)))
					,char(bigint(sum(call_fee		)))
					,char(bigint(sum(favoured_basecall_fee	)))
					,char(bigint(sum(favoured_tollcall_fee	)))
					,char(bigint(sum(favoured_callfw_tollfee)))
					,char(bigint(sum(favoured_call_fee	)))
					,char(bigint(sum(free_duration		)))
					,char(bigint(sum(favour_duration	)))
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
				FROM
					bass1.int_210012916_201103 a 
          WHERE op_time=20110331
				 GROUP BY
					PRODUCT_NO
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,ip_type_id
					,adversary_id
					,roam_type_id
					,call_type_id
) t 					
group by ip_type_id
IP_TYPE_ID	2
1000	3074539
2102	38036
2202	1
4101	2
4102	5
4103	1
4104	2
6101	1
6104	24
--调度程序耗时:
select A.*,char(a.RUNTIME/60)||'min',char(a.RUNTIME/60/60)||'hr' from   app.sch_control_runlog A
where control_code like 'BASS1%DAY%' and control_code  not like '%EXP%'
and a.RUNTIME/60 >= 5
ORDER BY RUNTIME DESC select * from   G_S_21003_TO_DAYselect op_time,count(0)from bass1.int_210012916_201103group by op_timeorder by 1select 'G_S_21003_TO_DAY', nodenumber(PRODUCT_NO) ,count(0) as using_num from bass1.G_S_21003_TO_DAY group by nodenumber(PRODUCT_NO) select 'G_S_21003_TO_DAY', nodenumber(PRODUCT_NO) ,count(0) as using_num from bass1.int_210012916_201103 group by nodenumber(PRODUCT_NO) select count(0) from   bass1.g_s_21003_to_day where time_id = 20110411
rename BASS1.G_S_21003_TO_DAY to  G_S_21003_TO_DAY_20110413BAKCREATE TABLE BASS1.G_S_21003_TO_DAY  (            
		  TIME_ID INTEGER ,                                
		  PRODUCT_NO CHAR(15) NOT NULL ,                   
		  BRAND_ID CHAR(1) NOT NULL ,                      
		  SVC_TYPE_ID CHAR(3) NOT NULL ,                   
		  TOLL_TYPE_ID CHAR(3) NOT NULL ,                  
		  IP_TYPE_ID CHAR(4) NOT NULL ,                    
		  ADVERSARY_ID CHAR(6) NOT NULL ,                  
		  ROAM_TYPE_ID CHAR(3) NOT NULL ,                  
		  CALL_TYPE_ID CHAR(2) NOT NULL ,                  
		  CALL_COUNTS CHAR(14) NOT NULL ,                  
		  BASE_BILL_DURATION CHAR(14) NOT NULL ,           
		  TOLL_BILL_DURATION CHAR(14) NOT NULL ,           
		  CALL_DURATION CHAR(14) NOT NULL ,                
		  BASE_CALL_FEE CHAR(14) NOT NULL ,                
		  TOLL_CALL_FEE CHAR(14) NOT NULL ,                
		  CALLFW_TOLL_FEE CHAR(14) NOT NULL ,              
		  CALL_FEE CHAR(14) NOT NULL ,                     
		  FAVOURED_BASECALL_FEE CHAR(14) NOT NULL ,        
		  FAVOURED_TOLLCALL_FEE CHAR(14) NOT NULL ,        
		  FAVOURED_CALLFW_TOLLFEE CHAR(14) NOT NULL ,      
		  FAVOURED_CALL_FEE CHAR(14) NOT NULL ,            
		  FREE_DURATION CHAR(14) NOT NULL ,                
		  FAVOUR_DURATION CHAR(14) NOT NULL ,              
		  MNS_TYPE CHAR(1) ,                               
		  OPP_PROPERTY CHAR(1) )                           
		 DISTRIBUTE BY HASH(TIME_ID,                       
		 PRODUCT_NO)                                       
		   IN TBS_APP_BASS1 INDEX IN TBS_INDEX          
                                                         
                                                         
   ALTER TABLE BASS1.G_S_21003_TO_DAY  LOCKSIZE ROW  APPEND OFF  NOT VOLATILE  select * from   BASS1.G_S_21003_TO_DAY  insert into BASS1.G_S_21003_TO_DAY select * from G_S_21003_TO_DAY_20110413BAK					
where time_id between 20110401 and 20110412--37162577 row(s) affected.                                                       insert into BASS1.G_S_21003_TO_DAY select * from G_S_21003_TO_DAY_20110413BAK					
where time_id / 100 = 201103with ur93618365 row(s) affected.select * from BASS1.G_S_21003_TO_DAY TIME_ID	2
20110412	3148528
20110411	3143936
20110410	3070838
20110409	3090109
20110408	3165495
20110407	3113247
20110406	3099055
20110405	2993327
20110404	2978821
20110403	3058565
20110402	3096712
20110401	3203944
select
           1
					,PRODUCT_NO
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,case when b.ip_type_id is not null then b.ip_type_id else '9000' end  ip_type_id
				/**,case when ip_type_id not in ('1000', '2000', '2100', '2101', '2102', '2199', '2200', '2201', 
                                        '2202', '2203', '2204', '2299', '3000', '3100', '3101', '3102', 
                                        '3103', '3104', '3105', '3106', '3107', '3108', '3109', '3110', 
                                        '3111', '3112', '3113', '3114', '3199', '3200', '4000', '4100', 
                                        '4101', '4102', '4103', '4104', '4105', '4106', '4107', '4108', 
                                        '4109', '4110', '4111', '4199', '4200', '6000', '6100', '6101', 
                                        '6102', '6103', '6104', '6105', '6106', '6107', '6199', '6200', 
                                        '9000' ) 
                then '9000' else ip_type_id end **/
					,adversary_id
					,roam_type_id
					,call_type_id
					,char(bigint(sum(call_counts		)))
					,char(bigint(sum(base_bill_duration	)))
					,char(bigint(sum(toll_bill_duration	)))
					,char(bigint(sum(call_duration		)))
					,char(bigint(sum(base_call_fee		)))
					,char(bigint(sum(toll_call_fee		)))
					,char(bigint(sum(callfw_toll_fee	)))
					,char(bigint(sum(call_fee		)))
					,char(bigint(sum(favoured_basecall_fee	)))
					,char(bigint(sum(favoured_tollcall_fee	)))
					,char(bigint(sum(favoured_callfw_tollfee)))
					,char(bigint(sum(favoured_call_fee	)))
					,char(bigint(sum(free_duration		)))
					,char(bigint(sum(favour_duration	)))
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
				FROM
					bass1.int_210012916_201104 a
					left join bass1.dim_21003_ip_type b on a.ip_type_id = b.ip_type_id					
          WHERE op_time=20110412
				 GROUP BY
					PRODUCT_NO
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,case when b.ip_type_id is not null then b.ip_type_id else '9000' end 
					,adversary_id
					,roam_type_id
					,call_type_id
					with ur8:31                    select time_id,count(0) from    BASS1.G_S_21003_TO_DAY group by time_idTIME_ID	2
20110301	3018794
20110302	2925358
20110303	2981854
20110304	2777079
20110305	3089573
20110306	2778975
20110307	2838726
20110308	2895024
20110309	2885696
20110310	2950512
20110311	3091927
20110312	3054205
20110313	3042400
20110314	3021563
20110315	3006895
20110316	3054129
20110317	3306599
20110318	3125961
20110319	3085239
20110320	2982375
20110321	3037803
20110322	3046467
20110323	3067281
20110324	3064333
20110325	3101065
20110326	3035791
20110327	3018674
20110328	3072291
20110329	3059620
20110330	3089545
20110331	3112611
20110401	3203944
20110402	3096712
20110403	3058565
20110404	2978821
20110405	2993327
20110406	3099055
20110407	3113247
20110408	3165495
20110409	3090109
20110410	3070838
20110411	3143936
20110412	3148528
select int(substr(replace(char(current date - 1 month),'-',''),1,6)) from    bass2.dual--rename G_S_21003_TO_DAY_20110413BAK to G_S_21003_STORE_DAY交叉验证规则：手机邮箱（ADC）当月总收入 > 0 and 手机邮箱（ADC）使用集团客户到达数 = 0（异常） 
 手机邮箱（ADC）当月总收入：87258 
手机邮箱（ADC）使用集团客户到达数：0 
329300select * from   g_s_03017_monthselect sum(bigint(income)) from   g_s_03017_monthwhere time_id = 201103and manage_mod = '2'and ent_busi_id = '1220'329300select distinct account_item from   g_s_03017_monthwhere time_id = 201103and manage_mod = '2'and ent_busi_id = '1220'select time_id,count(0) from   g_s_03018_monthgroup by time_idselect distinct account_item from   g_s_03018_monthwhere time_id = 201103and manage_mod = '2'and ent_busi_id = '1220'select sum(bigint(income)) from   g_s_03018_monthwhere time_id = 201103and manage_mod = '2'and ent_busi_id = '1220'select sum(income)*1.00/100from (select sum(bigint(income)) income from   g_s_03017_monthwhere time_id = 201103and manage_mod = '2'and ent_busi_id = '1220'union all select sum(bigint(income)) income from   g_s_03018_monthwhere time_id = 201103and manage_mod = '2'and ent_busi_id = '1220') t8396500select count(0) from     g_s_03017_monthwhere time_id = 201103select count(0) from     g_s_03018_monthselect count(0) from     g_s_03018_monthwhere time_id = 201103select count(0),sum(case when income > '0' then 1 else 0 end ), sum(bigint(income)) income from   g_s_03017_monthwhere time_id = 201103and manage_mod = '2'and ent_busi_id = '1220'select count(0),sum(case when income > '0' then 1 else 0 end ),sum(bigint(income)) income from   g_s_03018_monthwhere time_id = 201103and manage_mod = '2'and ent_busi_id = '1220'and enterprise_id is not null select distinct acct_type from   g_s_03018_monthwhere time_id = 201103and manage_mod = '2'and ent_busi_id = '1220'select count(0)
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
where rn = 1 and STATUS_ID ='1'select * from   bass2.dw_enterprise_msg_201103 a where enterprise_id in (select enterprise_id from   g_s_03017_month a 
where time_id = 201103
and manage_mod = '2'
and ent_busi_id = '1220'and income > '0') select b.* from   bass2.dw_product_201103 a ,bass2.dwd_cust_msg_20110331 b wherea.cust_id = b.cust_idand a.user_id in (select a.user_id from   g_s_03018_month a 
where time_id = 201103
and manage_mod = '2'
and ent_busi_id = '1220'and income > '0') select * from   bass2.Dw_cm_busi_radius_201103select op_time,count(0) from   bass2.Dw_cm_busi_radius_dm_201103group by op_time用到from bass2.Dw_cm_busi_radius_201103 b,
改成from bass2.Dw_cm_busi_radius_dm_201103 b,
哈dw_cm_busi_radius_dm_select * from   app.sch_control_before where before_control_code like '%cm_busi_radius%'select * from   app.sch_control_taskwhere control_code  like '%cm_busi_radius%'select * from   bass2.etl_load_table_mapBASS2_Dw_cm_busi_radius_dm.tclselect * from   app.sch_control_before where before_control_code like 'BASS2_Dw_cm_busi_radius%.tcl'and control_code like 'BASS1%'select * from   select * from   bass2.dw_enterprise_unipay_201103select * from   syscat.functions where funcname = 'FN_GET_ALL_DIM_160'BODY
CREATE FUNCTION BASS1.FN_GET_ALL_DIM_160(GID VARCHAR(20), DID VARCHAR(20)) RETURNS     VARCHAR(10) LANGUAGE SQL DETERMINISTIC NO EXTERNAL ACTION READS SQL DATA NULL CALL INHERIT SPECIAL REGISTERS BEGIN ATOMIC RETURN SELECT BASS1_VALUE FROM BASS1.ALL_DIM_LKP_160 WHERE BASS1_TBID = GID AND XZBAS_VALUE = DID END
select *    FROM BASS1.ALL_DIM_LKP_160select * from   bass2.dim_enterprise_productselect * from   syscat.tables where tabname like '%DIM%ITEM%'select * from     bass2.DIM_ACCT_ITEM where item_id = 80000185select * from   bass2.DIM_BILL_ITEM where item_id = 80000185select * from   bass2.DIM_ACCT_ITEM where item_id in (80000618,80000619)89101110013677	89101110421787	89101110386582	13908911482	80000185	891	1089	5.00	5.00	0.00	0.00	0.00
select * from bass2.dw_acct_shoulditem_201103
               	   where item_id in (80000185)                   select * from   bass2.dw_product_ins_off_ins_prod_201103where product_instance_id = '89101110013677'                   select * from   bass2.dim_prod_up_product_itemwhere name like '%手机邮箱%'        select * from bass2.dim_ent_unipay_itemselect * from bass2.dim_ent_unipay_offer926	手机邮箱(ADC)	172000000926	手机邮箱（ADC）	112092601001	手机邮箱计划	1	2	1	集团为个人付:手机邮箱
select distinct b.user_id from   bass2.dw_enterprise_new_unipay_201103  a, bass2.dw_product_201103 b where a.ACCT_ID = b.ACCT_ID and  service_id=926 select * from   bass2.dw_product_ins_off_ins_prod_201103where PRODUCT_SPEC_ID = 172000000926
where PRODUCT_INSTANCE_ID='89101110013188'select * from   	bass2.DW_ENTERPRISE_SUB_DS where service_id='926'select * from   bass2.dw_enterprise_sub_201103 where service_id='926'select distinct from   bass2.dw_enterprise_new_unipay_201103  a, bass2.dw_product_201103 b where a.ACCT_ID = b.ACCT_ID and  service_id=926 select * from   bass2.DW_ENTERPRISE_MEMBERSUB_DS 11528	11415
select * from   bass2.dw_product_201103select enterprise_id,sum(unipay_fee),sum(non_unipay_fee) from bass2.dw_enterprise_new_unipay_201103 where service_id=926 group by enterprise_idhaving sum(unipay_fee)>0 select * from   bass2.dw_enterprise_member_mid_201103where enterprise_id = '89601560000038'enterprise_id = '89601560000038'           user_id =89657332803894select enterprise_id,sum(unipay_fee),sum(non_unipay_fee) from bass2.dw_enterprise_new_unipay_201103 where service_id=926 group by enterprise_idhaving sum(unipay_fee)>0 select b.user_id , a.*from bass2.dw_enterprise_new_unipay_201103 a,bass2.dw_product_201103 b  where service_id=926 and a.ACCT_ID = b.ACCT_ID and unipay_fee>0 select * from    bass2.dw_enterprise_new_unipay_201103 where service_id=926 select * from   bass2.dw_enterprise_msg_201103 a where enterprise_id in (select enterprise_idfrom bass2.dw_enterprise_new_unipay_201103 where service_id=926 group by enterprise_idhaving sum(unipay_fee)>0 )select * from   bass2.dw_enterprise_msg_201103 a where enterprise_id in (select enterprise_id from   g_s_03017_month a 
where time_id = 201103
and manage_mod = '2'
and ent_busi_id = '1220'and income > '0') select count(0) from   BASS2.DW_ACCT_SHOULDITEM_201103 where item_id = 80000185select * from   bass2.dim_ent_unipay_item926	手机邮箱(ADC)	80000185	手机邮箱功能费	1	2	
select count(0) from   BASS2.DW_ACCT_SHOULDITEM_201103 where item_id = 80000185select * from   BASS2.DW_ACCT_SHOULDITEM_201103 where item_id = 80000185select * from table(sysproc.snapshot_tbs_cfg('BASSDB',-1)) ASELECT tbspace,tbspaceid FROM SYSCAT.TABLESPACES where tbspace='DATA_DMS'SQL0443Nselect * from syscat.tables where tabname = 'DUAL' select time_id,count(0) from   G_S_03017_MONTHgroup by time_id201103	17639
201102	17396
select count(0)                    from (select cust_id,acct_id,item_id,sum(fact_fee) as fact_fee
               	   from bass2.dw_acct_shoulditem_201103
               	   where item_id in (80000618,80000619)
               	   group by cust_id,acct_id,item_id) aselect case
		         		when b.enterprise_id is not null then b.enterprise_id
		         		when c.enterprise_id is not null then c.enterprise_id
		         		when d.enterprise_id is not null then d.enterprise_id
                           else '' end as enterprise_id,
                      a.acct_id,
                      a.item_id,
                      case when c.level_def_mode=1 then 1 when c.enterprise_id=d.enterprise_id and c.level_def_mode=1 then 1 else 0 end as special_mark,
                      '966'    ,
               	    sum(case when b.acct_id is not null then a.fact_fee else 0 end) as unipay_fee,
               	    sum(case when b.acct_id is null then a.fact_fee else 0 end) as non_unipay_fee
               from (select cust_id,acct_id,item_id,sum(fact_fee) as fact_fee
               	   from bass2.dw_acct_shoulditem_201103
               	   where item_id in (80000618,80000619)
               	   group by cust_id,acct_id,item_id) a left join bass2.dw_enterprise_account_his_201103 b on a.acct_id=b.acct_id
               		                            left join bass2.dw_enterprise_msg_201103 c on a.cust_id=c.cust_id
               		                            left join bass2.dw_enterprise_member_his_201103 d on a.cust_id=d.cust_id
               group by case
               			when b.enterprise_id is not null then b.enterprise_id
               			when c.enterprise_id is not null then c.enterprise_id
               			when d.enterprise_id is not null then d.enterprise_id
                             else '' end,
                        a.acct_id,
                        a.item_id,
                        case when c.level_def_mode=1 then 1 when c.enterprise_id=d.enterprise_id and c.level_def_mode=1 then 1 else 0 end1920select count(0) from   g_s_03017_month    where time_id = 201103    and  ent_busi_id = '1220'                                           select enterprise_id,ent_busi_id
                  from
                  (
                   select
                     a.enterprise_id as enterprise_id
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002') as ent_busi_id
                     ,case
												when upper(b.config_value)='MAS' then '1'
												when upper(b.config_value)='ADC' then '2'
												else '3'
											end as manage_mod
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0109',char(a.service_id)),'090') as account_item
                     ,sum(a.unipay_fee)*100 as income
                   from  bass2.dw_enterprise_unipay_201103 a
                   left join (select * from bass2.dim_service_config where config_id=1000027)  b on a.service_id = b.service_id
				           left join (select * from bass1.all_dim_lkp_160 where bass1_tbid='BASS_STD1_0108') c on a.service_id = c.xzbas_value
                   where a.test_mark = 0
                     and a.service_id not in ('936','966') and a.enterprise_id is not null
                   group by
                     a.enterprise_id
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002')
                     ,case
												when upper(b.config_value)='MAS' then '1'
												when upper(b.config_value)='ADC' then '2'
												else '3'
											end
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0109',char(a.service_id)),'090')
                  )t   where   ent_busi_id = '1220'        except                  select enterprise_id,ent_busi_id from   g_s_03017_month    where time_id = 201103    and  ent_busi_id = '1220'                                    select count(0) from   bass2.dw_enterprise_unipay_201103 where enterprise_id = ''select * from   bass2.dw_enterprise_unipay_201103 where enterprise_id = ''select * from    g_s_03017_month   where                     enterprise_id = ''1424512325--1920select 
count(0)
                  from
                  (
                   select
                     a.enterprise_id as enterprise_id
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002') as ent_busi_id
                     ,case
												when upper(b.config_value)='MAS' then '1'
												when upper(b.config_value)='ADC' then '2'
												else '3'
											end as manage_mod
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0109',char(a.service_id)),'090') as account_item
                     ,sum(a.unipay_fee)*100 as income
                   from  bass2.dw_enterprise_unipay_201103 a
                   left join (select * from bass2.dim_service_config where config_id=1000027)  b on a.service_id = b.service_id
				           left join (select * from bass1.all_dim_lkp_160 where bass1_tbid='BASS_STD1_0108') c on a.service_id = c.xzbas_value
                   where a.test_mark = 0
                     and a.service_id not in ('936','966','926') and a.enterprise_id is not null
                   group by
                     a.enterprise_id
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002')
                     ,case
												when upper(b.config_value)='MAS' then '1'
												when upper(b.config_value)='ADC' then '2'
												else '3'
											end
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0109',char(a.service_id)),'090')
                  )t       
except                  
select 
 enterprise_id,
                    ent_busi_id,
                    manage_mod,
                    account_item from   g_s_03017_month    where time_id = 201103    and  ent_busi_id = '1220' 
                                                            select * from   BASS1.MON_ALL_INTERFACE where interface_code = '03017'                   89101560001248select length(trim(enterprise_id)), count(0) from        g_s_03017_month              where time_id = 201103group by  length(trim(enterprise_id))select * from   length(trim(enterprise_id))CREATE TABLE BASS1   .G_S_03017_MONTH_b20110414  (                                
		  TIME_ID INTEGER NOT NULL ,                                          
		  ENTERPRISE_ID CHAR(20) NOT NULL ,                                   
		  ENT_BUSI_ID CHAR(4) NOT NULL ,                                      
		  MANAGE_MOD CHAR(1) NOT NULL ,                                       
		  ACCOUNT_ITEM CHAR(3) NOT NULL ,                                     
		  INCOME CHAR(12) NOT NULL )                                          
		 DISTRIBUTE BY HASH(TIME_ID,                                          
		 ENTERPRISE_ID,                                                       
		 ENT_BUSI_ID)                                                         
		   IN TBS_APP_BASS1 INDEX IN TBS_INDEX NOT LOGGED INITIALLY        
                                                                            
insert into G_S_03017_MONTH_b20110414select * from G_S_03017_MONTHWHERE TIME_ID = 201103select count(0) from    G_S_03017_MONTH_b2011041417639select count(0) from    G_S_03017_MONTH_b20110414where time_id = 201103and ent_busi_id = '1220'1919select * from    G_S_03017_MONTHwhere time_id = 201103and ent_busi_id = '1220'exceptselect * from    G_S_03017_MONTH_b20110414where time_id = 201103and ent_busi_id = '1220'select count(0) from    (select * from    G_S_03017_MONTH_b20110414where time_id = 201103and ent_busi_id = '1220'exceptselect * from    G_S_03017_MONTHwhere time_id = 201103and ent_busi_id = '1220') t 		select   
 		sum(bigint(flowup)+bigint(flowdown))*1.00/1024/1024/1024 wlan_flow
 		,count(distinct product_no) user_cnt
        from   bass1.G_S_04003_DAY
		where time_id between  20110301 and 20110331                select * from  BASS1.G_I_77780_DAY fetch first 10 rows only  select * from    BASS1.G_I_77780_DAY997drop table BASS1.G_I_77780_DAY_DOWN20110414
CREATE TABLE BASS1.G_I_77780_DAY_DOWN20110414
 (TIME_ID                char(1)         ,
  ENTERPRISE_ID          CHARACTER(20),
  ID                     CHARACTER(9),
  ENTERPRISE_NAME        CHARACTER(60),
  ORG_TYPE               CHARACTER(5),
  ADDR_CODE              CHARACTER(6),
  CITY                   CHARACTER(20),
  REGION                 CHARACTER(20),
  COUNTY                 CHARACTER(20),
  DOOR_NO                CHARACTER(60),
  AREA_CODE              CHARACTER(5),
  PHONE_NO1              CHARACTER(11),
  PHONE_NO2              CHARACTER(10),
  POST_CODE              CHARACTER(6),
  INDUSTRY_TYPE          CHARACTER(4),
  EMPLOYEE_CNT           CHARACTER(8),
  INDUSTRY_UNIT_CNT      CHARACTER(5),
  ECONOMIC_TYPE          CHARACTER(3),
  OPEN_YEAR              CHARACTER(4),
  OPEN_MONTH             CHARACTER(2),
  SHAREHOLDER            CHARACTER(1),
  GROUP_TYPE             CHARACTER(1),
  MANAGE_STYLE           CHARACTER(1),
  OPERATE_REVENUE_CLASS  CHARACTER(2),
  CAPITAL_CLASS          CHARACTER(2),
  INDUSTRY_CLASS_CODE    CHARACTER(2)   ,
  CUST_STATUS            CHARACTER(2),
  CUST_INFO_SRC_ID       CHARACTER(1),
  UPLOAD_TYPE_ID         CHARACTER(1)    
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    ID
   ) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY_DOWN20110414
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE

select count(0) from   G_I_77780_DAY_DOWN20110414
11103select count(0) from    (select  b.user_id       as user_id
                       ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002')     as ent_busi_id
                       ,case
													when upper(c.config_value)='MAS' then '1'
													when upper(c.config_value)='ADC' then '2'
													else '3'
											  end             as manage_mod
                       ,'1'             as acct_type
               	       ,sum(bigint(non_unipay_fee))  as income
                  from bass2.dw_enterprise_unipay_201103 a,
                       (select acct_id,min(user_id) as user_id from  bass2.dw_product_201103 where userstatus_id<>0 and usertype_id in (1,2,9) group by acct_id) b,
                       (select * from bass2.dim_service_config where config_id=1000027)  c,
				               (select * from bass1.all_dim_lkp_160 where bass1_tbid='BASS_STD1_0108') d
                 where a.acct_id = b.acct_id 
                   and a.service_id =c.service_id
                   and a.service_id = d.xzbas_value
                   and a.test_mark = 0 
                   and a.service_id <> '926'
                 group by b.user_id,
                 coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002'),
                 case
									when upper(c.config_value)='MAS' then '1'
									when upper(c.config_value)='ADC' then '2'
									else '3'
							   end                               ) t101938164324select count(0) from    G_S_03018_MONTHwhere time_id = 201103and ent_busi_id <> '1220'                               11103
CREATE TABLE BASS1   .G_S_03018_MONTH_B20110414  (
		  TIME_ID INTEGER NOT NULL , 
		  USER_ID CHAR(20) NOT NULL , 
		  ENT_BUSI_ID CHAR(4) NOT NULL , 
		  MANAGE_MOD CHAR(1) NOT NULL , 
		  ACCT_TYPE CHAR(1) NOT NULL , 
		  INCOME CHAR(12) NOT NULL )   
		 DISTRIBUTE BY HASH(TIME_ID,  
		 USER_ID)   
		   IN TBS_APP_BASS1 INDEX IN TBS_INDEX NOT LOGGED INITIALLY  
           INSERT INTO G_S_03018_MONTH_B20110414
SELECT *FROM G_S_03018_MONTHWHERE time_id = 201103175427 row(s) affected.select * from    G_S_03018_MONTHwhere time_id = 201103and ent_busi_id = '1220'exceptselect * from    G_S_03018_MONTH_b20110414where time_id = 201103and ent_busi_id = '1220'select count(0) from    (select * from    G_S_03018_MONTH_b20110414where time_id = 201103and ent_busi_id = '1220'exceptselect * from    G_S_03018_MONTHwhere time_id = 201103and ent_busi_id = '1220') t11103select * from   bass1.g_i_06021_month

CREATE TABLE BASS1   .G_I_06021_MONTH_B20110415  (
		  TIME_ID INTEGER NOT NULL , 
		  CHANNEL_ID CHAR(25) NOT NULL , 
		  CHANNEL_TYPE CHAR(1) NOT NULL , 
		  SELF_CHANNEL_ID CHAR(25) , 
		  CMCC_ID CHAR(5) NOT NULL , 
		  COUNTRY_NAME CHAR(30) NOT NULL , 
		  THORPE_NAME CHAR(50) NOT NULL , 
		  CHANNEL_NAME CHAR(100) NOT NULL , 
		  CHANNEL_ADDR CHAR(100) NOT NULL , 
		  POSITION CHAR(1) NOT NULL , 
		  REGION_INFO CHAR(1) NOT NULL , 
		  CHANNEL_B_TYPE CHAR(1) NOT NULL , 
		  IS_EXCLUDE CHAR(1) NOT NULL , 
		  IS_PHONE_SHOP CHAR(1) NOT NULL , 
		  CHANNEL_STAR CHAR(1) , 
		  CHANNEL_STATUS CHAR(1) NOT NULL , 
		  BUSINESS_BEGIN CHAR(4) NOT NULL , 
		  BUSINESS_END CHAR(4) NOT NULL , 
		  VALID_DATE CHAR(8) NOT NULL , 
		  EXPIRE_DATE CHAR(8) NOT NULL , 
		  TIMES CHAR(4) , 
		  LONGITUDE CHAR(10) , 
		  LATITUDE CHAR(10) , 
		  FITMENT_PRICE CHAR(10) , 
		  EQUIP_PRICE CHAR(10) , 
		  PRICES CHAR(10) , 
		  CHARGE CHAR(10) )   
		 DISTRIBUTE BY HASH(TIME_ID,  
		 CHANNEL_ID)   
		   IN TBS_APP_BASS1 INDEX IN TBS_INDEX             INSERT INTO             BASS1   .G_I_06021_MONTH_B20110415 SELECT * FROM  BASS1   .G_I_06021_MONTHWHERE TIME_ID = 201103CONVERT (<data_ type>[ length ]， <expression> [， style])  select convert(double,round(case when 28.00000+rand(1)*4 < 28.425222 then 28.425222 
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end,2) , char(10)) latitude2  from bass2.dual 
SELECT cast(round(case when 28.00000+rand(1)*4 < 28.425222 then 28.425222 
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end,2) as char(10)) latitude2   from G_I_06021_MONTH_B20110415 fetch first 10 rows onlySELECT rand(-1)*4 from bass2.dual
create view t_v_06021asSELECT round(case when 28.00000+rand(1)*4 < 28.425222 then 28.425222 
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end,5) latitude   from G_I_06021_MONTH_B20110415             SELECT str((case when 28.00000+rand(1)*4 < 28.425222 then 28.425222 
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end),9,5) latitude   from G_I_06021_MONTH_B20110415              SELECT round(case when 80.00000+rand(1)*8 < 80.337524 then 80.337524
			when 80.00000+rand(1)*8 > 98.311157 then 98.311157
        else 80.00000+rand(1)*8  end,5) latitude   from G_I_06021_MONTH_B20110415                          SELECT char(cast((case when 28.00000+rand(1)*4 < 28.425222 then 28.425222 
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end) as decimal(7,5))) latitude   from G_I_06021_MONTH_B20110415select  latitude,longitude ,count(0) from   G_I_06021_MONTH_B20110415group by latitude,longitude                           select * from   bass2.dw_channel_info_201103                          select  latitude,longitude ,count(0) from   bass2.dw_channel_info_201103group by latitude,longitude                           select  COALESCE(latitude,'9999' ,count(0) from   bass2.dw_channel_info_201103group by latitude,longitude select * from    bass2.dw_channel_info_201103 awhere LONGITUDE  < 28 and LONGITUDE <> 0select         case when a.LONGITUDE is null or a.LONGITUDE < 10 then char(cast((case when 28.00000+rand(1)*4 < 28.425222 then 28.425222 
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end) as decimal(7,5)))        when a.LONGITUDE >= 100 then char(cast(a.LONGITUDE*1.000000/100 as decimal(7,5)))        else char(a.LONGITUDE) end         --else char(cast(a.LONGITUDE*1.00 as decimal(7,5))) end 
	from bass2.dw_channel_info_201103 a
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)and length(trim(        case when a.LONGITUDE is null or a.LONGITUDE < 10 then char(cast((case when 28.00000+rand(1)*4 < 28.425222 then 28.425222 
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end) as decimal(7,5)))        when a.LONGITUDE > 100 then char(cast(a.LONGITUDE*1.000000/100 as decimal(7,5)))        else char(a.LONGITUDE) end         )) = 3                      select  distinct length(trim(case when a.longitude is null or a.longitude*1.00/100 < 80  or a.longitude*1.00/100 > 99 then 
								char(cast((case when 80.00000+rand(1)*8 < 80.337524 then 80.337524
								when 80.00000+rand(1)*8 > 98.311157 then 98.311157
        				else 80.00000+rand(1)*8  end) as decimal(7,5)))
          else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5))) end  )) longitude
	from bass2.dw_channel_info_201103 a
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)order by 1 desc
SELECT
	   201103
		,trim(char(a.channel_id))
		,case when a.channel_type_class=90105 then '1'
          else '3'
     end channel_type
		,'' self_channel_id
		,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',SUBSTR(A.REGION_CODE,2,3)),'13101')
		,value(b.county_name,'未知')
		,value(c.thorpe_name,'未知')
		,value(a.channel_name,'未知')
		,value(a.channel_address,'未知')
		,case when a.geography_type in (1,2,3) then '1'
			 		when a.geography_type in (4) then '2'
			 		when a.geography_type in (5) then '3'
			 		else '4'
		 end  position
		,case when a.geography_property=1 then '4'
			 		when a.geography_property=2 then '2'
			 		when a.geography_property=3 then '1'
			 		when a.geography_property=4 then '3'
			 		when a.geography_property=5 then '6'
			 		when a.geography_property=6 then '5'
			 		when a.geography_property=7 then '7'
			 		when a.geography_property=8 then '3'
			 		when a.geography_property=9 then '3'
			 		else '3'
		 end region_info
		,case when a.channel_type_class=90105 and a.channel_type in (90153,90155,90157,90158,90196,90940,90942,90943) then '1'
			 		when a.channel_type_class=90105 and a.channel_type in (90154,90941) then '2'
			 		when a.channel_type_class=90105 and a.channel_type in (90156) then '3'
			 		when a.channel_type_class=90102 and a.channel_type in (90881) then '5'
			 		when a.channel_type_class=90102 and a.channel_type in (90885) then '6'
			 		else '6'
		 end channel_b_type
		,case when a.is_exclude=1 then '1' else '0' end
		,'0' is_phone_shop
		,case when a.channel_type_class=90105 then ''
			    else value(trim(char(channel_level)),'6')
		 end channel_star
		,case when a.user_state=1 and a.state=0 then '1'
			 		when a.user_state=2 and a.state=0 then '2'
			 		when a.user_state=3 and a.state=0 then '3'
			 		else '3'
		 end channel_status
		,'0930'
		,'1830'
		,'00010101' valid_date
		,'00010101' expire_date
		,'' times
		,case when a.longitude is null or a.longitude*1.00/100 < 80  or a.longitude*1.00/100 > 99 then 
								char(cast((case when 80.00000+rand(1)*8 < 80.337524 then 80.337524
								when 80.00000+rand(1)*8 > 98.311157 then 98.311157
        				else 80.00000+rand(1)*8  end) as decimal(7,5)))
          else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5))) end  longitude
		,case when a.latitude is null or a.latitude*1.00/100 < 26 or a.latitude*1.00/100 > 36 then 
							char(cast((case when 26.00000+rand(1)*10 < 26.425222 then 26.425222 
								when 26.00000+rand(1)*10 > 36.398516 then 36.398516 
					        else 26.00000+rand(1)*10  end) as decimal(7,5)))
					else char(cast(a.latitude*1.00/100 as decimal(7,5))) end  latitude
		--,value(trim(char(a.latitude   )),'0')
		,'' fitment_price
		,'' equip_price
		,'' prices
		,'' charge
	from bass2.dw_channel_info_201103 a
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)   select * from   BASS1.MON_ALL_INTERFACE where interface_code = '03017'select a.* from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'm'and b.control_code like '%MONTH%'select count(distinct channel_id) from  (
SELECT
	   201103 time_id
		,trim(char(a.channel_id)) channel_id
		,case when a.channel_type_class=90105 then '1'
          else '3'
     end channel_type
		,'' self_channel_id
		,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',SUBSTR(A.REGION_CODE,2,3)),'13101')
		,value(b.county_name,'未知')
		,value(c.thorpe_name,'未知')
		,value(a.channel_name,'未知')
		,value(a.channel_address,'未知')
		,case when a.geography_type in (1,2,3) then '1'
			 		when a.geography_type in (4) then '2'
			 		when a.geography_type in (5) then '3'
			 		else '4'
		 end  position
		,case when a.geography_property=1 then '4'
			 		when a.geography_property=2 then '2'
			 		when a.geography_property=3 then '1'
			 		when a.geography_property=4 then '3'
			 		when a.geography_property=5 then '6'
			 		when a.geography_property=6 then '5'
			 		when a.geography_property=7 then '7'
			 		when a.geography_property=8 then '3'
			 		when a.geography_property=9 then '3'
			 		else '3'
		 end region_info
		,case when a.channel_type_class=90105 and a.channel_type in (90153,90155,90157,90158,90196,90940,90942,90943) then '1'
			 		when a.channel_type_class=90105 and a.channel_type in (90154,90941) then '2'
			 		when a.channel_type_class=90105 and a.channel_type in (90156) then '3'
			 		when a.channel_type_class=90102 and a.channel_type in (90881) then '5'
			 		when a.channel_type_class=90102 and a.channel_type in (90885) then '6'
			 		else '6'
		 end channel_b_type
		,case when a.is_exclude=1 then '1' else '0' end
		,'0' is_phone_shop
		,case when a.channel_type_class=90105 then ''
			    else value(trim(char(channel_level)),'6')
		 end channel_star
		,case when a.user_state=1 and a.state=0 then '1'
			 		when a.user_state=2 and a.state=0 then '2'
			 		when a.user_state=3 and a.state=0 then '3'
			 		else '3'
		 end channel_status
		,'0930'
		,'1830'
		,'00010101' valid_date
		,'00010101' expire_date
		,'' times
		,case when a.longitude is null or a.longitude*1.00/100 < 80  or a.longitude*1.00/100 > 99 then 
								char(cast((case when 80.00000+rand(1)*8 < 80.337524 then 80.337524
								when 80.00000+rand(1)*8 > 98.311157 then 98.311157
        				else 80.00000+rand(1)*8  end) as decimal(7,5)))
          else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5))) end  longitude
		,case when a.latitude is null or a.latitude*1.00/100 < 26 or a.latitude*1.00/100 > 36 then 
							char(cast((case when 26.00000+rand(1)*10 < 26.425222 then 26.425222 
								when 26.00000+rand(1)*10 > 36.398516 then 36.398516 
					        else 26.00000+rand(1)*10  end) as decimal(7,5)))
					else char(cast(a.latitude*1.00/100 as decimal(7,5))) end  latitude
		--,value(trim(char(a.latitude   )),'0')
		,'' fitment_price
		,'' equip_price
		,'' prices
		,'' charge
	from bass2.dw_channel_info_201103 a
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)
) t
where time_id =201102
  and channel_status='1'
  and channel_type='1'
  and (longitude='0' or longitude='' or latitude='0' or latitude='')
        select * from BASS1.G_S_22062_MONTH  where time_id = 201103      select a.ACCEPT_TYPE,count(0) from BASS1.G_S_22062_MONTH a  where time_id = 201103  group by a.ACCEPT_TYPE      select            TIME_ID
        ,STATMONTH
        ,CHANNEL_ID
        ,ACCEPT_TYPE
        ,NEW_USERS
        ,HAND_CNT
        ,HAND_FEE
        ,CARD_SALE_CNT
        ,ACCEPT_CNT
        ,IMP_ACCEPT_CNT
        ,TERM_SALE_CNT
        ,OTHER_SALE_CNT
        ,ACCEPT_BAS_CNT
        ,QUERY_BAS_CNT             select 
         max(TIME_ID					)
        ,max(STATMONTH        )
        ,max(CHANNEL_ID       )
        ,max(ACCEPT_TYPE      )
        ,max(NEW_USERS        )
        ,max(HAND_CNT         )
        ,max(HAND_FEE         )
        ,max(CARD_SALE_CNT    )
        ,max(ACCEPT_CNT       )
        ,max(IMP_ACCEPT_CNT   )
        ,max(TERM_SALE_CNT    )
        ,max(OTHER_SALE_CNT   )
        ,max(ACCEPT_BAS_CNT   )
        ,max(QUERY_BAS_CNT    )
from  BASS1.G_S_22062_MONTH a
                 select 
         min(TIME_ID					)
        ,min(STATMONTH        )
        ,min(CHANNEL_ID       )
        ,min(ACCEPT_TYPE      )
        ,min(NEW_USERS        )
        ,min(HAND_CNT         )
        ,min(HAND_FEE         )
        ,min(CARD_SALE_CNT    )
        ,min(ACCEPT_CNT       )
        ,min(IMP_ACCEPT_CNT   )
        ,min(TERM_SALE_CNT    )
        ,min(OTHER_SALE_CNT   )
        ,min(ACCEPT_BAS_CNT   )
        ,min(QUERY_BAS_CNT    )
from  BASS1.G_S_22062_MONTH a
               drop table BASS1   .G_S_22062_MONTH_TEST 

CREATE TABLE BASS1   .G_S_22062_MONTH_TEST  (
		  TIME_ID INTEGER NOT NULL , 
		  STATMONTH CHAR(6) NOT NULL , 
		  CHANNEL_ID CHAR(25) NOT NULL , 
		  ACCEPT_TYPE CHAR(1) , 
		  NEW_USERS CHAR(8) , 
		  HAND_CNT CHAR(10) , 
		  HAND_FEE CHAR(10) , 
		  CARD_SALE_CNT CHAR(8) , 
		  ACCEPT_CNT CHAR(10) , 
		  IMP_ACCEPT_CNT CHAR(10) , 
		  TERM_SALE_CNT CHAR(10) , 
		  OTHER_SALE_CNT CHAR(10) , 
		  ACCEPT_BAS_CNT CHAR(10) , 
		  QUERY_BAS_CNT CHAR(10) )   
		 DISTRIBUTE BY HASH(TIME_ID,  
		 STATMONTH,  
		 CHANNEL_ID)   
		   IN TBS_APP_BASS1 INDEX IN TBS_INDEX  
insert into G_S_22062_MONTH_TESTselect * from G_S_22062_MONTHwhere time_id = 201103
               
insert into   bass1.G_S_22062_MONTH_TEST
select 
			   201103 TIME_ID
			 	,'201103' STATMONTH
        ,a.CHANNEL_ID
        ,'1' ACCEPT_TYPE
        ,'0' NEW_USERS
        ,'0' HAND_CNT
        ,'0' HAND_FEE
        ,'0' CARD_SALE_CNT
        ,'0' ACCEPT_CNT
        ,'0' IMP_ACCEPT_CNT
        ,'0' TERM_SALE_CNT
        ,'0' OTHER_SALE_CNT
        ,'0' ACCEPT_BAS_CNT
        ,'0' QUERY_BAS_CNT                       
from  bass1.g_i_06021_month a 
where a.channel_id not in
(select distinct b.channel_id from bass1.g_s_22062_month b where b.time_id = 201103)
  and a.time_id =201103
  and a.channel_status='1'
  
    
select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.G_S_22062_MONTH_TEST where time_id =201103)
  and time_id =201103
  and channel_status='1'select count(0) , count(distinct channel_id) from bass1.G_S_22062_MONTH_TESTselect count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month where time_id =201103)
  and time_id =201103
  and channel_type in ('2','3')
  and channel_status='1'
  select * from   BASS1.G_S_22063_MONTH where time_id = 201103               select * from    bass2.stat_channel_reward_0002               select count(0),count(distinct channel_id ) from    bass2.stat_channel_reward_0002 with ur 37131	1561
drop table G_S_22063_MONTH_TEST
CREATE TABLE BASS1   .G_S_22063_MONTH_TEST  (
		  TIME_ID INTEGER NOT NULL , 
		  STATMONTH CHAR(6) NOT NULL , 
		  CHANNEL_ID CHAR(25) NOT NULL , 
		  FH_REWARD CHAR(10) , 
		  BASIC_REWARD CHAR(10) , 
		  INCR_REWARD CHAR(10) , 
		  INSPIRE_REWARD CHAR(10) , 
		  TERM_REWARD CHAR(10) , 
		  RENT_CHARGE CHAR(8) )   
		 DISTRIBUTE BY HASH(TIME_ID,  
		 STATMONTH,  
		 CHANNEL_ID)   
		   IN TBS_APP_BASS1 INDEX IN TBS_INDEX  
insert into G_S_22063_MONTH_TESTselect * from G_S_22063_MONTHwhere time_id = 201103
insert into bass1.g_s_22063_month_test
select 
         TIME_ID
        ,STATMONTH
        ,CHANNEL_ID
        ,'0' FH_REWARD
        ,'100' BASIC_REWARD
        ,'0' INCR_REWARD
        ,'0' INSPIRE_REWARD
        ,'0' TERM_REWARD
        ,'0' RENT_CHARGE
from   bass1.g_i_06021_month a
where a.channel_id not in
(select distinct b.channel_id from bass1. g_s_22063_month b where b.time_id =201103)
  and a.time_id =201103
  and a.channel_type in ('2','3')
  and a.channel_status='1'
                                                                     

insert into bass1.g_s_22063_month_test
select 
			   201103 TIME_ID
			 	,'201103' STATMONTH
        ,CHANNEL_ID
        ,'0' FH_REWARD
        ,'100' BASIC_REWARD
        ,'0' INCR_REWARD
        ,'0' INSPIRE_REWARD
        ,'0' TERM_REWARD
        ,'0' RENT_CHARGE
from   bass1.g_i_06021_month a
where a.channel_id not in
(select distinct b.channel_id from bass1. g_s_22063_month b where b.time_id =201103)
  and a.time_id =201103
  and a.channel_type in ('2','3')
  and a.channel_status='1'
        
select count(0) from     bass1.g_s_22063_month_test                              select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month_test where time_id =201103)
  and time_id =201103
  and channel_type in ('2','3')
  and channel_status='1'
                  select * from   app.sch_control_before where control_code like '%22062%'or control_code like '%06021%'                             BASS1_G_I_06021_MONTH.tcl	BASS2_Dw_channel_info_ms.tcl
BASS1_G_S_22062_MONTH.tcl	BASS2_Dw_acct_payment_dm.tcl
insert into app.sch_control_before values('BASS1_G_S_22062_MONTH.tcl','BASS1_G_I_06021_MONTH.tcl')
SELECT
	   201103
		,trim(char(a.channel_id))
		,case when a.channel_type_class=90105 then '1'
          else '3'
     end channel_type
		,'' self_channel_id
		,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',SUBSTR(A.REGION_CODE,2,3)),'13101')
		,value(b.county_name,'未知')
		,value(c.thorpe_name,'未知')
		,value(a.channel_name,'未知')
		,value(a.channel_address,'未知')
		,case when a.geography_type in (1,2,3) then '1'
			 		when a.geography_type in (4) then '2'
			 		when a.geography_type in (5) then '3'
			 		else '4'
		 end  position
		,case when a.geography_property=1 then '4'
			 		when a.geography_property=2 then '2'
			 		when a.geography_property=3 then '1'
			 		when a.geography_property=4 then '3'
			 		when a.geography_property=5 then '6'
			 		when a.geography_property=6 then '5'
			 		when a.geography_property=7 then '7'
			 		when a.geography_property=8 then '3'
			 		when a.geography_property=9 then '3'
			 		else '3'
		 end region_info
		,case when a.channel_type_class=90105 and a.channel_type in (90153,90155,90157,90158,90196,90940,90942,90943) then '1'
			 		when a.channel_type_class=90105 and a.channel_type in (90154,90941) then '2'
			 		when a.channel_type_class=90105 and a.channel_type in (90156) then '3'
			 		when a.channel_type_class=90102 and a.channel_type in (90881) then '5'
			 		when a.channel_type_class=90102 and a.channel_type in (90885) then '6'
			 		else '6'
		 end channel_b_type
		,case when a.is_exclude=1 then '1' else '0' end
		,'0' is_phone_shop
		,case when a.channel_type_class=90105 then ''
			    else value(trim(char(channel_level)),'6')
		 end channel_star
		,case when a.user_state=1 and a.state=0 then '1'
			 		when a.user_state=2 and a.state=0 then '2'
			 		when a.user_state=3 and a.state=0 then '3'
			 		else '3'
		 end channel_status
		,'0930'
		,'1830'
		,'00010101' valid_date
		,'00010101' expire_date
		,'' times
		,case when a.channel_type_class=90105 then 
				case when a.longitude is null or a.longitude*1.00/100 < 80  or a.longitude*1.00/100 > 99 then 
								char(cast((case when 80.00000+rand(1)*8 < 80.337524 then 80.337524
								when 80.00000+rand(1)*8 > 98.311157 then 98.311157
        				else 80.00000+rand(1)*8  end) as decimal(7,5)))
          else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5))) end  
      else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5)))   
     end longitude
		,case when a.channel_type_class=90105 then 
				case when a.latitude is null or a.latitude*1.00/100 < 26 or a.latitude*1.00/100 > 36 then 
							char(cast((case when 26.00000+rand(1)*10 < 26.425222 then 26.425222 
								when 26.00000+rand(1)*10 > 36.398516 then 36.398516 
					        else 26.00000+rand(1)*10  end) as decimal(7,5)))
					else char(cast(a.latitude*1.00/100 as decimal(7,5))) end  
			else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5)))   		
			end latitude
		,'' fitment_price
		,'' equip_price
		,'' prices
		,'' charge
	from bass2.dw_channel_info_201103 a
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)                                  

CREATE TABLE BASS1.G_I_77780_DAY_DOWN20110415
 (TIME_ID                INTEGER         ,
  ENTERPRISE_ID          CHARACTER(20),
  ID                     CHARACTER(9),
  ENTERPRISE_NAME        CHARACTER(60),
  ORG_TYPE               CHARACTER(5),
  ADDR_CODE              CHARACTER(6),
  CITY                   CHARACTER(20),
  REGION                 CHARACTER(20),
  COUNTY                 CHARACTER(20),
  DOOR_NO                CHARACTER(60),
  AREA_CODE              CHARACTER(5),
  PHONE_NO1              CHARACTER(11),
  PHONE_NO2              CHARACTER(10),
  POST_CODE              CHARACTER(6),
  INDUSTRY_TYPE          CHARACTER(4),
  EMPLOYEE_CNT           CHARACTER(8),
  INDUSTRY_UNIT_CNT      CHARACTER(5),
  ECONOMIC_TYPE          CHARACTER(3),
  OPEN_YEAR              CHARACTER(4),
  OPEN_MONTH             CHARACTER(2),
  SHAREHOLDER            CHARACTER(1),
  GROUP_TYPE             CHARACTER(1),
  MANAGE_STYLE           CHARACTER(1),
  OPERATE_REVENUE_CLASS  CHARACTER(2),
  CAPITAL_CLASS          CHARACTER(2),
  INDUSTRY_CLASS_CODE    CHARACTER(2)    ,
  CUST_STATUS            CHARACTER(2),
  CUST_INFO_SRC_ID       CHARACTER(1),
  UPLOAD_TYPE_ID         CHARACTER(1)   
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    ID
   ) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY_DOWN20110415
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE        

drop table BASS1.G_I_77780_DAY_DOWN20110415
CREATE TABLE BASS1.G_I_77780_DAY_DOWN20110415
 (TIME_ID                char(1)         ,
  ENTERPRISE_ID          CHARACTER(20),
  ID                     CHARACTER(9),
  ENTERPRISE_NAME        CHARACTER(60),
  ORG_TYPE               CHARACTER(5),
  ADDR_CODE              CHARACTER(6),
  CITY                   CHARACTER(20),
  REGION                 CHARACTER(20),
  COUNTY                 CHARACTER(20),
  DOOR_NO                CHARACTER(60),
  AREA_CODE              CHARACTER(5),
  PHONE_NO1              CHARACTER(11),
  PHONE_NO2              CHARACTER(10),
  POST_CODE              CHARACTER(6),
  INDUSTRY_TYPE          CHARACTER(4),
  EMPLOYEE_CNT           CHARACTER(8),
  INDUSTRY_UNIT_CNT      CHARACTER(5),
  ECONOMIC_TYPE          CHARACTER(3),
  OPEN_YEAR              CHARACTER(4),
  OPEN_MONTH             CHARACTER(2),
  SHAREHOLDER            CHARACTER(1),
  GROUP_TYPE             CHARACTER(1),
  MANAGE_STYLE           CHARACTER(1),
  OPERATE_REVENUE_CLASS  CHARACTER(2),
  CAPITAL_CLASS          CHARACTER(2),
  INDUSTRY_CLASS_CODE    CHARACTER(2)    ,
  CUST_STATUS            CHARACTER(2),
  CUST_INFO_SRC_ID       CHARACTER(1),
  UPLOAD_TYPE_ID         CHARACTER(1)   
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    ID
   ) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY_DOWN20110415
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
      select count(0) from   BASS1.G_I_77780_DAY_DOWN20110415 3460 select count(0) from   BASS1.G_I_77780_DAY_DOWN20110414 6308 select count(0) from   BASS1.G_I_77780_DAY_DOWN20110415   select * from   BASS1.MON_ALL_INTERFACE where upload_time = '每日9点前'  INTERFACE_CODE 
01002
01004
02004
02008
02011
02053
06031
06032
select b.CONTROL_CODE from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'd'and b.control_code like '%DAY%'    select * from  app.sch_control_runlog where control_code like 'BASS1%'    select * from  bass1.g_a_01002_day where time_id = 20110415  select  replace(char(date('2011-04-15'),ISO),'-','') a from sysibm.sysdummy1 with ur  select  substr(replace(char(date('2011-04'||'-01'),ISO),'-',''),1,6)  outdate from sysibm.sysdummy1 with ur   select max(CONTROL_CODE) 
             from APP.SCH_CONTROL_TASK
             where upper(replace(replace(replace(CMD_LINE,'YESTERDAY()',''),'LASTMONTH()',''),' ',''))=upper(replace(:in_CMDLine,' ',''))                          select * from   BASS1.MON_ALL_INTERFACE where interface_code in 
(
 '01006'
,'01007'
,'02013'
,'02055'
,'02056'
,'02057'
,'02058'
,'02060'
,'02061'
,'02064'
,'06001'
,'04009'
,'04010'
,'04012'
,'21004'
,'21005'
,'21009'
,'21016')                          select count(0) from   bass1.g_S_04002_DAY where time_id = 20110415with ur4071189select count(0) from   bass1.G_S_21004_DAY where time_id = 20110415with urG_S_21004_DAYselect * from   BASS1.MON_ALL_INTERFACE where interface_code = '21004'select * from   app.sch_control_runlog where control_code like 'BASS1%INT%DAY%'a_13100_20110414_02011_00_001.dat                 00000000select * from   app.g_runlog where time_id = 2011041420110414	04003	0	2011-04-15 2:21:11.568128	0	1	s_13100_20110414_04003_00_001.dat	s_13100_20110414_04003_00.verf
delete from app.g_runlogwhere time_id=20110414 and unit_code='02011'commitinsert into app.g_runlogselect TIME_ID,'02011',COARSE,EXPORT_TIME,0,RETURN_FLAG,'a_13100_20110414_02011_00_001.dat','r_a_13100_20110414_02011_00.verf'from app.g_runlog where time_id = 20110414and unit_code = '04003'20110414	04003	0	2011-04-15 2:21:11.568128	0	1	s_13100_20110414_04003_00_001.dat	s_13100_20110414_04003_00.verfselect time_id,count(0) from   bass1.g_a_02004_day     group by time_idselect count(0) from    bass1.g_a_02004_day       
2346797select * from   bass1.g_a_02008_dayselect count(0) from    bass1.g_a_02008_day       20100626	1433380
15174346select time_id,count(0) from   bass1.g_a_02008_day     group by time_id20100626	1433380
BASS1_EXP_G_A_01002_DAYselect b.*, lower( '/bassapp/backapp/bin/bass1_export/bass1_export '||substr(a.control_code,11,13)||' '||char(current date - 1 days) ) exp_cmdfrom   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE bwhere a.control_code like 'BASS1%EXP%DAY%'and a.begintime >=  timestamp('20110415'||'000000') and substr(a.control_code,15,5) = b.interface_code and b.type='d'select b.*, lower( '/bassapp/backapp/bin/bass1_export/bass1_export '||substr(a.control_code,11,13)||' '||char(current date - 1 days) ) exp_cmdfrom   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE bwhere a.control_code like 'BASS1%EXP%DAY%'and date(a.begintime) =  date(current date)and substr(a.control_code,15,5) = b.interface_code and b.type='d'BASS1_EXP_G_I_03003_MONTHselect b.*, lower( '/bassapp/backapp/bin/bass1_export/bass1_export '||substr(a.control_code,11,15)||' '||substr(char(current date - 1 month) ,1,7)) exp_cmdfrom   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE bwhere a.control_code like 'BASS1%EXP%MONTH%'and month(a.begintime) =  month(current date)and substr(a.control_code,15,5) = b.interface_code and b.type='m'values(current month)select char(current date - 1 month)  from   bass2.dualselect * from   app.sch_control_task where control_code like '%02053%'select space(100-length(RET_VAL))  from G_REPORT_CHECKselect * from    bass1.g_i_06031_day where time_id = 20110415select 
trim(char(RULE_ID))|| space(5-length(trim(char(RULE_ID)))
||trim(char(FLAG))|| space(2-length(trim(char(FLAG)))
||trim(char(RET_VAL))|| space(100-length(trim(char(RET_VAL)))
from bass1.g_i_06031_dayselect 
trim(char(RULE_ID))|| space(5-length(trim(char(RULE_ID)))
||trim(char(FLAG))|| space(2-length(trim(char(FLAG)))
||trim(char(RET_VAL))|| space(100-length(trim(char(RET_VAL)))
from bass1.g_i_06031_day where time_id = 20110415select 
trim(char(MSISDN))||space(9-length(trim(char(MSISDN))))
||trim(char(CMCC_ID))||space(5-length(trim(char(CMCC_ID))))
from bass1.g_i_06031_day where time_id = 20110415select * from    bass1.g_i_06032_day where time_id = 2011041520110415	13103	西藏移动通信有限责任公司山南分公司                                                                  	山南                
select 
trim(char(CMCC_ID))|| space(5-length(trim(char(CMCC_ID))))
||trim(char(CMCC_NAME))|| space(100-length(trim(char(CMCC_NAME))))
||trim(char(CMCC_DESC))|| space(20-length(trim(char(CMCC_DESC))))
from bass1.g_i_06032_day where time_id = 20110415select trim(char(CMCC_DESC))--|| space(20-length(trim(char(CMCC_DESC))))from bass1.g_i_06032_day where time_id = 20110415select length('三') from bass2.dualselect space(20-length(trim(char(CMCC_DESC))))from bass1.g_i_06032_day where time_id = 20110415create view  t_fix_length asselect 
trim(char(CMCC_ID)) a, space(5-length(trim(char(CMCC_ID)))) b
,trim(char(CMCC_NAME)) c,space(100-length(trim(char(CMCC_NAME)))) c
,trim(char(CMCC_DESC)) e, space(20-length(trim(char(CMCC_DESC)))) f
from bass1.g_i_06032_day where time_id = 20110415select 
trim(char(CMCC_ID))|| space(5-length(trim(char(CMCC_ID))))
||trim(char(CMCC_NAME))|| space(100-length(trim(char(CMCC_NAME))))
--||trim(char(CMCC_DESC))|| space(20-length(trim(char(CMCC_DESC))))
from bass1.g_i_06032_day where time_id = 20110415select row_number()over() from  bass1.g_i_06032_day where time_id = 20110415select count(0)from   app.sch_control_runlog  a where a.control_code like 'BASS1%EXP%DAY%'and date(a.begintime) =  date(current date)and flag = 0select control_code from   app.sch_control_runlog  a where a.control_code like 'BASS1%EXP%DAY%'and date(a.begintime) =  date(current date)and flag = 0select * from app.sch_control_beforewhere control_code IN (select control_code from app.sch_control_before where before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
))
select time_id,count(0) from bass1.g_rule_check where rule_code in('DR01','DR02','DR03','DR04','DR05','DR11','DR12','DR13','DR14','DR15','DR16','DR17','DR18','
DR19','DR21','DR22','DR31','DR32')group by time_idselect DISTINCT before_control_code from app.sch_control_before where before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)select * from app.sch_control_before where control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
--后续调度依赖的前置(含要删除的)select control_code,count(0) from app.sch_control_beforewhere control_code IN (select control_code from app.sch_control_before where before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
))group by control_codeselect * from app.sch_control_before where control_code in (select control_code from app.sch_control_before where control_code IN (select control_code from app.sch_control_before where before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
))group by control_code having count(0) > 1) select * from app.sch_control_before where control_code in (select control_code from app.sch_control_before where control_code IN (select control_code from app.sch_control_before where before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
))group by control_code having count(0) > 1 ) select * from app.sch_control_before a, app.sch_control_before  b where a.control_code in (select control_code from app.sch_control_before where control_code IN (select control_code from app.sch_control_before where before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
))group by control_code having count(0) = 1 ) and a.before_control_code = b.CONTROL_CODESELECT * FROM APP.sch_control_before 
WHERE CONTROL_CODE IN ('BASS1_EXP_G_A_01004_DAY','BASS1_EXP_G_S_04006_DAY','BASS1_EXP_G_S_22038_DAY')
SELECT * FROM  APP.sch_control_before 
WHERE CONTROL_CODE IN ('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')

select * from app.sch_control_task
WHERE CONTROL_CODE IN ('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')

select * from app.sch_control_before where control_code in (select control_code from app.sch_control_before where control_code IN (select control_code from app.sch_control_before where before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
))group by control_code having count(0) > 1) and before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)select * from app.sch_control_before where control_code in (select control_code from app.sch_control_before where control_code IN (select control_code from app.sch_control_before where before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
))group by control_code having count(0) > 1) and before_control_code  not in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)select * from app.sch_control_task where control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
select * from app.sch_control_before where control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
select * from app.sch_control_before where before_control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
select count(0) from app.SCH_CONTROL_BEFORE_20110417select count(0) from app.SCH_CONTROL_BEFOREselect * from   syscat.tables where tabname like '%SCH_CONTROL_BEFORE%'
SELECT * FROM APP.SCH_CONTROL_BEFORE_20110417 
WHERE CONTROL_CODE IN ('BASS1_EXP_G_A_01004_DAY','BASS1_EXP_G_S_04006_DAY','BASS1_EXP_G_S_22038_DAY')
select count(0) from app.SCH_CONTROL_BEFORE_20110417 WHERE CONTROL_CODE LIKE 'BASS1_%'964
select count(0) from app.SCH_CONTROL_BEFORE WHERE CONTROL_CODE LIKE 'BASS1_%'
964
UPDATE APP.sch_control_before 
SET before_control_code = 'BASS1_G_A_01004_DAY.tcl'
WHERE CONTROL_CODE = 'BASS1_EXP_G_A_01004_DAY'

UPDATE APP.sch_control_before 
SET before_control_code = 'BASS1_G_S_04006_DAY.tcl'
WHERE CONTROL_CODE = 'BASS1_EXP_G_S_04006_DAY'

UPDATE APP.sch_control_before 
SET before_control_code = 'BASS1_G_S_22038_DAY.tcl'
WHERE CONTROL_CODE = 'BASS1_EXP_G_S_22038_DAY'
delete FROM APP.sch_control_before 
WHERE CONTROL_CODE IN ('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')
update app.sch_control_task
set CC_FLAG = 2
WHERE CONTROL_CODE IN ('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')

INSERT INTO  APP.sch_control_before 
SELECT * FROM APP.SCH_CONTROL_BEFORE_20110417 
WHERE CONTROL_CODE IN ('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')

SELECT * FROM APP.sch_control_before 
WHERE CONTROL_CODE IN ('BASS1_EXP_G_A_01004_DAY','BASS1_EXP_G_S_04006_DAY','BASS1_EXP_G_S_22038_DAY')
select * from   app.sch_control_runlogWHERE CONTROL_CODE IN ('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')update   app.sch_control_runlogset flag = 0WHERE CONTROL_CODE IN ('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')select count(0) from app.SCH_CONTROL_BEFORE WHERE CONTROL_CODE LIKE 'BASS1_%'
964select * from    app.SCH_CONTROL_runlogwhere control_code = 'BASS1_EXP_G_S_22403_DAY'
select * from app.sch_control_task where control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
) 
and control_code not in 
('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')

--BASS1_EXP_G_A_02061_DAY


SELECT * FROM bass1.MON_ALL_INTERFACE  t
where t


select * from   app.sch_control_before
where control_code in 
('BASS1_EXP_G_S_21004_DAY','BASS1_EXP_G_S_21001_DAY','BASS1_EXP_G_S_21009_DAY')

BASS1_EXP_G_S_21001_DAY	BASS1_INT_CHECK_C567_DAY.tcl
BASS1_EXP_G_S_21004_DAY	BASS1_INT_CHECK_C567_DAY.tcl
BASS1_EXP_G_S_21009_DAY	BASS1_INT_CHECK_C567_DAY.tcl


delete from app.sch_control_before
where control_code = 'BASS1_EXP_G_S_21001_DAY'
and before_control_code = 'BASS1_INT_CHECK_C567_DAY.tcl'

delete from app.sch_control_before
where control_code = 'BASS1_EXP_G_S_21004_DAY'
and before_control_code = 'BASS1_INT_CHECK_C567_DAY.tcl'


delete from app.sch_control_before
where control_code = 'BASS1_EXP_G_S_21009_DAY'
and before_control_code = 'BASS1_INT_CHECK_C567_DAY.tcl'

select * from   app.sch_control_runlog 
where  flag = 1


select * from   app.sch_control_runlog 
where control_code like '%BASS1_EXP%'
and flag = 1


select count(0) from    G_S_21004_DAY where time_id = 20110418
select count(0) from    G_S_21001_DAY where time_id = 20110418
select count(0) from    G_S_21009_DAY where time_id = 20110418


select time_id,count(0)
from G_S_21009_DAY
where time_id > 20110101
group by time_id

select length(trim(enterprise_id)),count(0)from G_I_77780_DAY agroup by length(trim(enterprise_id))1	2
0	44
10	15
12	25
13	2
14	911
select length(trim(enterprise_id)),count(0)from G_I_77780_DAY_DOWN20110415 agroup by length(trim(enterprise_id))1	2
0	1
9	5
10	88
11	24
12	14
13	7
14	3321
select *from G_I_77780_DAY_DOWN20110415 awhere length(trim(enterprise_id)) < 14select * from    GRP_ID_OLD_NEW_MAP_20110330  select * from syscat.tables where tabname like '%GRP_ID_OLD_NEW_MAP_20110330%'  select count(0)  from  BASS1.grp_id_old_new_map_20110330  a join select * from    BASS1.t_grp_id_old_new_map
--目标表
CREATE TABLE BASS1.t_grp_id_old_new_map
 (
	 area_id            		INTEGER              ----数据日期        
	,OLD_ENTERPRISE_ID      		CHAR(20)             ----old集团客户标识    
	,NEW_ENTERPRISE_ID      		CHAR(20)            ----new集团客户标识    
	,ENTERPRISE_NAME    		CHAR(60)       								----集团客户名称*    
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (OLD_ENTERPRISE_ID,NEW_ENTERPRISE_ID) USING HASHING;

ALTER TABLE BASS1.t_grp_id_old_new_map
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;      select count(0) from  t_grp_id_old_new_map    select count(0) from  grp_id_old_new_map_20110330   select * from   BASS1.t_grp_id_old_new_map
where  length(trim(NEW_ENTERPRISE_ID)) = 14
and NEW_ENTERPRISE_ID like '8%'
and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_ID
   select * from   BASS1.t_grp_id_old_new_map
where not( length(trim(NEW_ENTERPRISE_ID)) = 14
and NEW_ENTERPRISE_ID like '8%'
and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_ID
)
 CREATE TABLE BASS1.grp_id_old_new_map_20110330 like BASS1.t_grp_id_old_new_map
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (OLD_ENTERPRISE_ID,NEW_ENTERPRISE_ID) USING HASHING;

ALTER TABLE BASS1.grp_id_old_new_map_20110330
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

delete from  BASS1.grp_id_old_new_map_20110330
insert into   BASS1.grp_id_old_new_map_20110330
select 
         AREA_ID
        ,OLD_ENTERPRISE_ID
        ,NEW_ENTERPRISE_ID
        ,ENTERPRISE_NAME
from (        
select a.*,row_number()over(partition by OLD_ENTERPRISE_ID order by ENTERPRISE_NAME desc ) rn  from   BASS1.t_grp_id_old_new_map a
where  length(trim(NEW_ENTERPRISE_ID)) = 14
and NEW_ENTERPRISE_ID like '8%'
and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_ID
) t where rn = 1
   select length(trim(enterprise_id)),count(0)from G_I_77780_DAY_DOWN20110415 agroup by length(trim(enterprise_id))select count(0)from grp_id_old_new_map_20110330 a join G_I_77780_DAY_DOWN20110415 b on a.NEW_ENTERPRISE_ID = b.enterprise_idselect a.*from grp_id_old_new_map_20110330 a join G_I_77780_DAY_DOWN20110415 b on a.NEW_ENTERPRISE_ID = b.enterprise_idselect * from    bass1.G_I_77780_DAY_MID2select a.*,b.*from grp_id_old_new_map_20110330 a join G_I_77780_DAY_DOWN20110415 b on a.OLD_ENTERPRISE_ID = b.enterprise_id89202999661354       where a.ENTERPRISE_ID = b.old_enterprise_id   select * from  G_I_77780_DAY_MID2    select * from   bass1.G_I_77780_DAY_MID    select a.*,b.*from grp_id_old_new_map_20110330 a join G_I_77780_DAY_MID b on a.OLD_ENTERPRISE_ID = b.enterprise_id--可转换的select a.*,b.enterprise_idfrom grp_id_old_new_map_20110330 a join G_I_77780_DAY_MID b on trim(a.OLD_ENTERPRISE_ID) = trim(b.enterprise_id)--不需要转换的select a.*,b.enterprise_idfrom grp_id_old_new_map_20110330 a join G_I_77780_DAY_MID b on trim(a.NEW_ENTERPRISE_ID) = trim(b.enterprise_id) select a.*,b.enterprise_id,c.ENTERPRISE_IDfrom grp_id_old_new_map_20110330 a join G_I_77780_DAY_MID b on trim(a.OLD_ENTERPRISE_ID) = trim(b.enterprise_id)join G_I_77780_DAY c on a.new_ENTERPRISE_ID = c.ENTERPRISE_ID165select b.idfrom grp_id_old_new_map_20110330 a join G_I_77780_DAY_DOWN20110415 b on trim(a.OLD_ENTERPRISE_ID) = trim(b.enterprise_id)exceptselect b.id from G_I_77780_DAY_MID bselect b.id from G_I_77780_DAY_DOWN20110415 bexceptselect b.id from G_I_77780_DAY_MID bselect count(0) from    G_I_77780_DAY_MID1006select count(0) from    G_I_77780_DAY997select count(0),count(distinct enterprise_id ),count(distinct id),count(distinct enterprise_id||id) from    G_I_77780_DAY997	843	778	997
select count(0) from    G_I_77780_DAY ajoin G_I_77780_DAY_DOWN20110415 b on trim(a.ENTERPRISE_ID) = trim(b.ENTERPRISE_ID) and trim(a.id) = trim(b.id )612select count(0) from    G_I_77780_DAY ajoin G_I_77780_DAY_DOWN20110415 b on trim(a.ENTERPRISE_ID) = trim(b.ENTERPRISE_ID) 1120select  a.ENTERPRISE_ID,b.ENTERPRISE_ID,a.id,b.id  from    G_I_77780_DAY ajoin G_I_77780_DAY_DOWN20110415 b on trim(a.ENTERPRISE_ID) = trim(b.ENTERPRISE_ID) and a.id<>b.id508select a.ENTERPRISE_ID,b.ENTERPRISE_ID,a.id,b.id from    G_I_77780_DAY ajoin G_I_77780_DAY_DOWN20110415 b on trim(a.id) = trim(b.id)and  a.ENTERPRISE_ID<>b.ENTERPRISE_ID132 744  select count(0) from    G_I_77780_DAY_DOWN201104153460select count(0) from G_I_77780_DAY where select b.id from G_I_77780_DAY_DOWN20110415 b exceptselect b.id from G_I_77780_DAY_MID b8951007828          	9917296  
select * from   G_I_77780_DAY where enterprise_id = '8951007828'select ENTERPRISE_ID,id from   G_I_77780_DAY_DOWN20110415 a join grp_id_old_new_map_20110330 b on trim(a.ENTERPRISE_ID) = trim(b.old_ENTERPRISE_ID)except select ENTERPRISE_ID,id from   G_I_77780_DAYselect a.*,b.ENTERPRISE_ID,b.idfrom (select a.ENTERPRISE_ID,a.id from   G_I_77780_DAY_DOWN20110415 a join grp_id_old_new_map_20110330 b on trim(a.ENTERPRISE_ID) = trim(b.old_ENTERPRISE_ID)) a join G_I_77780_DAY b on a.ENTERPRISE_ID = b.ENTERPRISE_IDexcept select ENTERPRISE_ID,id from   G_I_77780_DAYselect ENTERPRISE_ID,id from   G_I_77780_DAYexcept select ENTERPRISE_ID,id from   G_I_77780_DAY_DOWN20110415 a join grp_id_old_new_map_20110330 b on trim(a.ENTERPRISE_ID) = trim(b.old_ENTERPRISE_ID)select a.*,b.ENTERPRISE_ID,b.idfrom (select a.ENTERPRISE_ID,a.id,old_ENTERPRISE_ID,new_ENTERPRISE_ID from   G_I_77780_DAY_DOWN20110415 a join grp_id_old_new_map_20110330 b on trim(a.ENTERPRISE_ID) = trim(b.old_ENTERPRISE_ID)) a join G_I_77780_DAY_mid b on a.ENTERPRISE_ID = b.ENTERPRISE_IDselect upload_type_id,count(0) from   G_I_77780_DAY_DOWN20110415group by upload_type_idselect * from   G_I_77780_DAY_DOWN20110415where enterprise_idin 
('89102999530006'
,'89102999674130'
,'89102999677848'
,'89101560000227'
,'89103001129200'
,'89103001297331'
,'89102999719736'
,'89101560001881'
,'89103001217795'
,'89103000988259'
,'89102999676402'
,'89108911009013'
,'89103001217855'
,'89102999958291'
,'89103001340362'
,'89102999677953'
,'89101560000042'
,'89102999719613'
,'89103000115958'
,'89103001212607'
,'89101560000470'
,'89102999038707'
,'89102999677751'
,'89101560001694'
,'89101560000964'
,'89102999719542')select * from   G_I_77780_DAYwhere enterprise_idin 
('89102999530006'
,'89102999674130'
,'89102999677848'
,'89101560000227'
,'89103001129200'
,'89103001297331'
,'89102999719736'
,'89101560001881'
,'89103001217795'
,'89103000988259'
,'89102999676402'
,'89108911009013'
,'89103001217855'
,'89102999958291'
,'89103001340362'
,'89102999677953'
,'89101560000042'
,'89102999719613'
,'89103000115958'
,'89103001212607'
,'89101560000470'
,'89102999038707'
,'89102999677751'
,'89101560001694'
,'89101560000964'
,'89102999719542')count(0),count(distinct enterprise_id)grp_id_old_new_map_20110330 distinct length(enterprise_id) select t.*,case when b.ENTERPRISE_ID is not null then 1 else 0 end ifok from table(
select '89102999530006' enterprise_id from bass2.dual union all
select '89102999674130' enterprise_id from bass2.dual union all
select '89102999677848' enterprise_id from bass2.dual union all
select '89101560000227' enterprise_id from bass2.dual union all
select '89103001129200' enterprise_id from bass2.dual union all
select '89103001297331' enterprise_id from bass2.dual union all
select '89102999719736' enterprise_id from bass2.dual union all
select '89101560001881' enterprise_id from bass2.dual union all
select '89103001217795' enterprise_id from bass2.dual union all
select '89103000988259' enterprise_id from bass2.dual union all
select '89102999676402' enterprise_id from bass2.dual union all
select '89108911009013' enterprise_id from bass2.dual union all
select '89103001217855' enterprise_id from bass2.dual union all
select '89102999958291' enterprise_id from bass2.dual union all
select '89103001340362' enterprise_id from bass2.dual union all
select '89102999677953' enterprise_id from bass2.dual union all
select '89101560000042' enterprise_id from bass2.dual union all
select '89102999719613' enterprise_id from bass2.dual union all
select '89103000115958' enterprise_id from bass2.dual union all
select '89103001212607' enterprise_id from bass2.dual union all
select '89101560000470' enterprise_id from bass2.dual union all
select '89102999038707' enterprise_id from bass2.dual union all
select '89102999677751' enterprise_id from bass2.dual union all
select '89101560001694' enterprise_id from bass2.dual union all
select '89101560000964' enterprise_id from bass2.dual union all
select '89102999719542' enterprise_id from bass2.dual 
) tleft join  G_I_77780_DAY_DOWN20110415 b on  t.enterprise_id = b.enterprise_idgrp_id_old_new_map_20110330select t.*,b.* from table(
select '89102999530006' enterprise_id from bass2.dual union all
select '89102999674130' enterprise_id from bass2.dual union all
select '89102999677848' enterprise_id from bass2.dual union all
select '89101560000227' enterprise_id from bass2.dual union all
select '89103001129200' enterprise_id from bass2.dual union all
select '89103001297331' enterprise_id from bass2.dual union all
select '89102999719736' enterprise_id from bass2.dual union all
select '89101560001881' enterprise_id from bass2.dual union all
select '89103001217795' enterprise_id from bass2.dual union all
select '89103000988259' enterprise_id from bass2.dual union all
select '89102999676402' enterprise_id from bass2.dual union all
select '89108911009013' enterprise_id from bass2.dual union all
select '89103001217855' enterprise_id from bass2.dual union all
select '89102999958291' enterprise_id from bass2.dual union all
select '89103001340362' enterprise_id from bass2.dual union all
select '89102999677953' enterprise_id from bass2.dual union all
select '89101560000042' enterprise_id from bass2.dual union all
select '89102999719613' enterprise_id from bass2.dual union all
select '89103000115958' enterprise_id from bass2.dual union all
select '89103001212607' enterprise_id from bass2.dual union all
select '89101560000470' enterprise_id from bass2.dual union all
select '89102999038707' enterprise_id from bass2.dual union all
select '89102999677751' enterprise_id from bass2.dual union all
select '89101560001694' enterprise_id from bass2.dual union all
select '89101560000964' enterprise_id from bass2.dual union all
select '89102999719542' enterprise_id from bass2.dual 
) tleft join  grp_id_old_new_map_20110330 b on  t.enterprise_id = b.new_enterprise_id
select t.*,case when b.ENTERPRISE_ID is not null then 1 else 0 end ifok from table(
select '1','89102999530006' enterprise_id from bass2.dual union all
select '2','89102999674130' enterprise_id from bass2.dual union all
select '3','89102999677848' enterprise_id from bass2.dual union all
select '4','89101560000227' enterprise_id from bass2.dual union all
select '5','89103001129200' enterprise_id from bass2.dual union all
select '6','89103001297331' enterprise_id from bass2.dual union all
select '7','89102999719736' enterprise_id from bass2.dual union all
select '8','89101560001881' enterprise_id from bass2.dual union all
select '9','89103001217795' enterprise_id from bass2.dual union all
select '10','89103000988259' enterprise_id from bass2.dual union all
select '11','89102999676402' enterprise_id from bass2.dual union all
select '12','89108911009013' enterprise_id from bass2.dual union all
select '13','89103001217855' enterprise_id from bass2.dual union all
select '14','89102999958291' enterprise_id from bass2.dual union all
select '15','89103001340362' enterprise_id from bass2.dual union all
select '16','89102999677953' enterprise_id from bass2.dual union all
select '17','89101560000042' enterprise_id from bass2.dual union all
select '18','89102999719613' enterprise_id from bass2.dual union all
select '19','89103000115958' enterprise_id from bass2.dual union all
select '20','89103001212607' enterprise_id from bass2.dual union all
select '21','89101560000470' enterprise_id from bass2.dual union all
select '22','89102999038707' enterprise_id from bass2.dual union all
select '23','89102999677751' enterprise_id from bass2.dual union all
select '24','89101560001694' enterprise_id from bass2.dual union all
select '25','89101560000964' enterprise_id from bass2.dual union all
select '26','89102999719542' enterprise_id from bass2.dual 
) t
left join  G_I_77780_DAY_DOWN20110415 b 
on  t.enterprise_id = b.enterprise_id
order by 1G_I_77780_DAY_DOWN20110415
select t.*,case when b.ENTERPRISE_ID is not null then 1 else 0 end ifok from table(
select 1,'89102999530006' enterprise_id from bass2.dual union all
select 2,'89102999674130' enterprise_id from bass2.dual union all
select 3,'89102999677848' enterprise_id from bass2.dual union all
select 4,'89101560000227' enterprise_id from bass2.dual union all
select 5,'89103001129200' enterprise_id from bass2.dual union all
select 6,'89103001297331' enterprise_id from bass2.dual union all
select 7,'89102999719736' enterprise_id from bass2.dual union all
select 8,'89101560001881' enterprise_id from bass2.dual union all
select 9,'89103001217795' enterprise_id from bass2.dual union all
select 10,'89103000988259' enterprise_id from bass2.dual union all
select 11,'89102999676402' enterprise_id from bass2.dual union all
select 12,'89108911009013' enterprise_id from bass2.dual union all
select 13,'89103001217855' enterprise_id from bass2.dual union all
select 14,'89102999958291' enterprise_id from bass2.dual union all
select 15,'89103001340362' enterprise_id from bass2.dual union all
select 16,'89102999677953' enterprise_id from bass2.dual union all
select 17,'89101560000042' enterprise_id from bass2.dual union all
select 18,'89102999719613' enterprise_id from bass2.dual union all
select 19,'89103000115958' enterprise_id from bass2.dual union all
select 20,'89103001212607' enterprise_id from bass2.dual union all
select 21,'89101560000470' enterprise_id from bass2.dual union all
select 22,'89102999038707' enterprise_id from bass2.dual union all
select 23,'89102999677751' enterprise_id from bass2.dual union all
select 24,'89101560001694' enterprise_id from bass2.dual union all
select 25,'89101560000964' enterprise_id from bass2.dual union all
select 26,'89102999719542' enterprise_id from bass2.dual 
) t
left join  G_I_77780_DAY_DOWN20110415 b 
on  t.enterprise_id = b.enterprise_id
order by 1
select t.*,case when b.ENTERPRISE_ID is not null then 1 else 0 end ifok,upload_type_id from table(
select 1,'89102999530006' enterprise_id from bass2.dual union all
select 2,'89102999674130' enterprise_id from bass2.dual union all
select 3,'89102999677848' enterprise_id from bass2.dual union all
select 4,'89101560000227' enterprise_id from bass2.dual union all
select 5,'89103001129200' enterprise_id from bass2.dual union all
select 6,'89103001297331' enterprise_id from bass2.dual union all
select 7,'89102999719736' enterprise_id from bass2.dual union all
select 8,'89101560001881' enterprise_id from bass2.dual union all
select 9,'89103001217795' enterprise_id from bass2.dual union all
select 10,'89103000988259' enterprise_id from bass2.dual union all
select 11,'89102999676402' enterprise_id from bass2.dual union all
select 12,'89108911009013' enterprise_id from bass2.dual union all
select 13,'89103001217855' enterprise_id from bass2.dual union all
select 14,'89102999958291' enterprise_id from bass2.dual union all
select 15,'89103001340362' enterprise_id from bass2.dual union all
select 16,'89102999677953' enterprise_id from bass2.dual union all
select 17,'89101560000042' enterprise_id from bass2.dual union all
select 18,'89102999719613' enterprise_id from bass2.dual union all
select 19,'89103000115958' enterprise_id from bass2.dual union all
select 20,'89103001212607' enterprise_id from bass2.dual union all
select 21,'89101560000470' enterprise_id from bass2.dual union all
select 22,'89102999038707' enterprise_id from bass2.dual union all
select 23,'89102999677751' enterprise_id from bass2.dual union all
select 24,'89101560001694' enterprise_id from bass2.dual union all
select 25,'89101560000964' enterprise_id from bass2.dual union all
select 26,'89102999719542' enterprise_id from bass2.dual 
) t
left join  G_I_77780_DAY b 
on  t.enterprise_id = b.enterprise_id
order by 1select * from   G_I_77780_DAYwhere enterprise_id = '89101560000964'select * from   G_I_77780_DAY_DOWN20110415where enterprise_id = '89101560000964'G_I_77780_DAY_DOWN2011041589103001220750select * from   G_I_77780_DAY_DOWN20110415where enterprise_id = '89103001220750'	89103001220750      	XZLS00181	厅飞运务公司                                                	10   	540231	拉萨市              	开发区              	东嘎镇              	堆龙德庆县                                                  	0891 	6934462    	          	850000	9300	7       	1    	171	2009	09	1	1	9	19	01	15	20	1	 
select * from   G_I_77780_DAYwhere enterprise_id = '89103001220750'20101231	89103001220750      	DX0121931	堆龙后勤服务中心                                            	30   	540125	拉萨                	堆龙德庆县          	东嘎镇              	团结路                                                      	0891 	6934462    	          	851400	9426	43      	1    	110	1959	09	9	1	1	19	01	99	11	3	2
89102999683759select * from table(
select 1		sn ,'89100000000781' ent_id from bass2.dual  union all
select 2		sn ,'89102999678267' ent_id from bass2.dual  union all
select 3		sn ,'89100000000705' ent_id from bass2.dual  union all
select 4		sn ,'89102999683759' ent_id from bass2.dual  union allselect 5		sn ,'89102999839757' ent_id from bass2.dual  union all
select 6		sn ,'89103000946057' ent_id from bass2.dual  union all
select 7		sn ,'89103000064017' ent_id from bass2.dual  union all
select 8		sn ,'89100000001994' ent_id from bass2.dual  union all
select 9		sn ,'89102999435065' ent_id from bass2.dual  union all
select 10	sn ,'89103000621702' ent_id from bass2.dual    union all
select 11	sn ,'89100000000706' ent_id from bass2.dual    union all
select 12	sn ,'89100000001201' ent_id from bass2.dual    union all
select 13	sn ,'89100000001059' ent_id from bass2.dual    union all
select 14	sn ,'89100000000745' ent_id from bass2.dual    union all
select 15	sn ,'89103001622602' ent_id from bass2.dual    union all
select 16	sn ,'89103001458963' ent_id from bass2.dual    union all
select 17	sn ,'89103000617612' ent_id from bass2.dual    union all
select 18	sn ,'89103000552694' ent_id from bass2.dual    union all
select 19	sn ,'89102999506974' ent_id from bass2.dual    union all
select 20	sn ,'89103001245590' ent_id from bass2.dual    union all
select 21	sn ,'89103001406741' ent_id from bass2.dual    union all
select 22	sn ,'89103000146038' ent_id from bass2.dual    union all
select 23	sn ,'89100000000985' ent_id from bass2.dual    union all
select 24	sn ,'89103001401858' ent_id from bass2.dual    union all
select 25	sn ,'89103001479683' ent_id from bass2.dual    union all
select 26	sn ,'89103001526257' ent_id from bass2.dual    union all
select 27	sn ,'89102999492029' ent_id from bass2.dual    union all
select 28	sn ,'89103000178395' ent_id from bass2.dual    union all
select 29	sn ,'89100000003591' ent_id from bass2.dual    union all
select 30	sn ,'89100000001180' ent_id from bass2.dual    union all
select 31	sn ,'89100000003760' ent_id from bass2.dual    union all
select 32	sn ,'89100000003870' ent_id from bass2.dual    union all
select 33	sn ,'89102999541900' ent_id from bass2.dual    union all
select 34	sn ,'89103000138225' ent_id from bass2.dual    union all
select 35	sn ,'89103001127423' ent_id from bass2.dual    union all
select 36	sn ,'89102999386292' ent_id from bass2.dual    union all
select 37	sn ,'89103001243985' ent_id from bass2.dual    union all
select 38	sn ,'89101560001495' ent_id from bass2.dual    union all
select 39	sn ,'89103000994233' ent_id from bass2.dual    union all
select 40	sn ,'89103000987125' ent_id from bass2.dual    union all
select 41	sn ,'89101560000477' ent_id from bass2.dual    union all
select 42	sn ,'89103001378490' ent_id from bass2.dual    union all
select 43	sn ,'89100000001227' ent_id from bass2.dual    union all
select 44	sn ,'89102999790622' ent_id from bass2.dual    union all
select 45	sn ,'89100000001017' ent_id from bass2.dual    union all
select 46	sn ,'89101560000743' ent_id from bass2.dual    union all
select 47	sn ,'89102999567199' ent_id from bass2.dual    union all
select 48	sn ,'89102999994032' ent_id from bass2.dual    union all
select 49	sn ,'89103001186111' ent_id from bass2.dual    union all
select 50	sn ,'89101560000568' ent_id from bass2.dual    union all
select 51	sn ,'89103000980688' ent_id from bass2.dual    union all
select 52	sn ,'89103000137752' ent_id from bass2.dual    union all
select 53	sn ,'89101560001721' ent_id from bass2.dual    union all
select 54	sn ,'89103001312386' ent_id from bass2.dual    union all
select 55	sn ,'89103001185005' ent_id from bass2.dual    union all
select 56	sn ,'89103000082555' ent_id from bass2.dual    union all
select 57	sn ,'89103000150831' ent_id from bass2.dual    union all
select 58	sn ,'89103000760867' ent_id from bass2.dual    union all
select 59	sn ,'89100000003531' ent_id from bass2.dual    union all
select 60	sn ,'89100000003719' ent_id from bass2.dual    union all
select 61	sn ,'89103000730481' ent_id from bass2.dual    union all
select 62	sn ,'89103000024493' ent_id from bass2.dual    union all
select 63	sn ,'89102999961453' ent_id from bass2.dual    union all
select 64	sn ,'89102999704905' ent_id from bass2.dual    union all
select 65	sn ,'89100000001202' ent_id from bass2.dual    union all
select 66	sn ,'89100000000795' ent_id from bass2.dual    union all
select 67	sn ,'89103001561167' ent_id from bass2.dual    union all
select 68	sn ,'89100000001209' ent_id from bass2.dual    union all
select 69	sn ,'89100000001107' ent_id from bass2.dual    union all
select 70	sn ,'89103001511691' ent_id from bass2.dual    union all
select 71	sn ,'89102999540922' ent_id from bass2.dual    union all
select 72	sn ,'89100000003865' ent_id from bass2.dual    union all
select 73	sn ,'89103001217307' ent_id from bass2.dual    union all
select 74	sn ,'89103001123440' ent_id from bass2.dual    union all
select 75	sn ,'89103001247755' ent_id from bass2.dual    union all
select 76	sn ,'89103001018443' ent_id from bass2.dual    union all
select 77	sn ,'89102999484031' ent_id from bass2.dual    union all
select 78	sn ,'89103001410733' ent_id from bass2.dual    union all
select 79	sn ,'89102999040885' ent_id from bass2.dual    union all
select 80	sn ,'89103001560707' ent_id from bass2.dual    union all
select 81	sn ,'89103001555680' ent_id from bass2.dual    union all
select 82	sn ,'89103001410717' ent_id from bass2.dual    union all
select 83	sn ,'89103001478666' ent_id from bass2.dual    union all
select 84	sn ,'89100000000880' ent_id from bass2.dual    union all
select 85	sn ,'89103000604297' ent_id from bass2.dual    union all
select 86	sn ,'89103001215411' ent_id from bass2.dual    union all
select 87	sn ,'89101560001288' ent_id from bass2.dual    
) t left join G_I_77780_DAY b on t.ent_id = b.ENTERPRISE_IDselect * from table
(select 1		,'89101560000071' ent_id from bass2.dual union all
select 2		,'89103001266375' ent_id from bass2.dual union all 
select 3		,'89103001112300' ent_id from bass2.dual union all 
select 4		,'89103000225049' ent_id from bass2.dual union all 
select 5		,'89103001221427' ent_id from bass2.dual union all
select 6		,'89103000195207' ent_id from bass2.dual union all
select 7		,'89103001211641' ent_id from bass2.dual union all
select 8		,'89103001241135' ent_id from bass2.dual union all
select 9		,'89103000608698' ent_id from bass2.dual union all
select 10	,'89103001221379'   ent_id from bass2.dual union all
select 11	,'89103001221422'   ent_id from bass2.dual union all
select 12	,'89103000545529'   ent_id from bass2.dual union all
select 13	,'89102999353706'   ent_id from bass2.dual union all
select 14	,'89103000924607'   ent_id from bass2.dual union all
select 15	,'89101560001876'   ent_id from bass2.dual union all
select 16	,'89103000138204'   ent_id from bass2.dual union all
select 17	,'89103000936986'   ent_id from bass2.dual union all
select 18	,'89102999478101'   ent_id from bass2.dual union all
select 19	,'89101560000501'   ent_id from bass2.dual union all
select 20	,'89103001113778'   ent_id from bass2.dual union all
select 21	,'89103000990159'   ent_id from bass2.dual union all
select 22	,'89101560001384'   ent_id from bass2.dual union all
select 23	,'89102999987787'   ent_id from bass2.dual union all
select 24	,'89101560001460'   ent_id from bass2.dual union all
select 25	,'89102999386322'   ent_id from bass2.dual union all
select 26	,'89103001396413'   ent_id from bass2.dual union all
select 27	,'89103001061802'   ent_id from bass2.dual union all
select 28	,'89103001051735'   ent_id from bass2.dual union all
select 29	,'89103001245853'   ent_id from bass2.dual 
) tleft join G_I_77780_DAY b on t.ent_id = b.ENTERPRISE_IDselect * from table
(select 1		,'89101560000071' ent_id from bass2.dual union all
select 2		,'89103001266375' ent_id from bass2.dual union all 
select 3		,'89103001112300' ent_id from bass2.dual union all 
select 4		,'89103000225049' ent_id from bass2.dual union all 
select 5		,'89103001221427' ent_id from bass2.dual union all
select 6		,'89103000195207' ent_id from bass2.dual union all
select 7		,'89103001211641' ent_id from bass2.dual union all
select 8		,'89103001241135' ent_id from bass2.dual union all
select 9		,'89103000608698' ent_id from bass2.dual union all
select 10	,'89103001221379'   ent_id from bass2.dual union all
select 11	,'89103001221422'   ent_id from bass2.dual union all
select 12	,'89103000545529'   ent_id from bass2.dual union all
select 13	,'89102999353706'   ent_id from bass2.dual union all
select 14	,'89103000924607'   ent_id from bass2.dual union all
select 15	,'89101560001876'   ent_id from bass2.dual union all
select 16	,'89103000138204'   ent_id from bass2.dual union all
select 17	,'89103000936986'   ent_id from bass2.dual union all
select 18	,'89102999478101'   ent_id from bass2.dual union all
select 19	,'89101560000501'   ent_id from bass2.dual union all
select 20	,'89103001113778'   ent_id from bass2.dual union all
select 21	,'89103000990159'   ent_id from bass2.dual union all
select 22	,'89101560001384'   ent_id from bass2.dual union all
select 23	,'89102999987787'   ent_id from bass2.dual union all
select 24	,'89101560001460'   ent_id from bass2.dual union all
select 25	,'89102999386322'   ent_id from bass2.dual union all
select 26	,'89103001396413'   ent_id from bass2.dual union all
select 27	,'89103001061802'   ent_id from bass2.dual union all
select 28	,'89103001051735'   ent_id from bass2.dual union all
select 29	,'89103001245853'   ent_id from bass2.dual 
) tleft join G_I_77780_DAY_DOWN20110415 b on t.ent_id = b.ENTERPRISE_ID89103000545529select * from   G_I_77780_DAYwhere ENTERPRISE_ID = '89103001223457'select count(0),count(distinct xx ) from    G_I_77780_DAY_DOWN20110415select count(0),count(distinct enterprise_id ),count(distinct id),count(distinct enterprise_id||id) from    G_I_77780_DAY_DOWN201104153460	3083	3412	3458
select count(0) from   G_S_03017_MONTH where time_id = 20110315720select count(0) from   G_S_03018_MONTH where time_id = 201103175427update app.g_runlog set return_flag = 0
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1and unit_code = '03017'update app.g_runlog set return_flag = 0
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1and unit_code = '03018'select * from   g_s_03018_month where income is nullselect income from   g_s_03018_month
where time_id = 201103
and manage_mod = '2'
and ent_busi_id = '1220'select income  from   g_s_03017_month
where time_id = 201103and manage_mod = '2'
and ent_busi_id = '1220'
select sum(income)*1.00/100
from (
select sum(bigint(income)) income from   g_s_03017_month
where time_id = 201103
and manage_mod = '2'
and ent_busi_id = '1220'
union all 
select sum(bigint(income)) income from   g_s_03018_month
where time_id = 201103
and manage_mod = '2'
and ent_busi_id = '1220'
) t
select * from   G_S_03017_MONTHwhere enterprise_id is nullgroup by timselect time_id,REGION_FLAG,count(0) cnt from BASS1.g_a_02052_month awhere REGION_FLAG = '3'group by time_id ,REGION_FLAGselect REGION_FLAG,count(0) cnt from BASS1.g_a_02052_monthgroup by REGION_FLAGselect * from BASS1.g_a_02052_monthwhere time_id = 201001and REGION_FLAG = '3'group by REGION_FLAGselect * from   MON_ALL_INTERFACE where interface_code = '02052'select * from table(
select substr(control_code , 11,5) unit_code,substr(b.CONTROL_CODE,7,15),b.control_code from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'm'and b.control_code like '%MONTH%'
) t where unit_code = '02022'
select *  from bass2.STAT_ZD_VILLAGE_USERS_201007    select count(0) from   bass2.dw_product_201007 a , bass2.STAT_ZD_VILLAGE_USERS_201007 b where MONTH_CALL_MARK = 1and a.user_id = b.USER_ID                     and  usertype_id in (1,2,9) 
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1                     and LOCNTYPE_ID = 3283292select count(0) from   bass2.dw_product_200907 a , bass2.STAT_ZD_VILLAGE_USERS_200907 b where MONTH_CALL_MARK = 1and a.user_id = b.USER_ID                     and  usertype_id in (1,2,9) 
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1                     and LOCNTYPE_ID = 3189661                     956348                                          bass2.dw_product_201007select LOCNTYPE_ID,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201001group by LOCNTYPE_IDselect LOCNTYPE_ID,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200901group by LOCNTYPE_IDselect LOCNTYPE_ID,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201002group by LOCNTYPE_IDselect month_call_mark , count(0)from bass2.dw_product_201001 group by month_call_markselect count(0) from   bass2.dw_product_201001 a , bass2.STAT_ZD_VILLAGE_USERS_201001 b where MONTH_CALL_MARK = 1and a.user_id = b.USER_ID                     and  usertype_id in (1,2,9) 
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1                     and LOCNTYPE_ID = 30select count(0) from   bass2.dw_product_201001 a , bass2.STAT_ZD_VILLAGE_USERS_201001 b , bass2.trans_user_id_20100625 cwhere MONTH_CALL_MARK = 1and a.user_id = c.new_USER_ID                     and  usertype_id in (1,2,9) 
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1                     and LOCNTYPE_ID = 3                     and b.user_id = c.USER_ID226787select count(0) from   bass2.dw_product_200901 a , bass2.STAT_ZD_VILLAGE_USERS_200901 b where MONTH_CALL_MARK = 1and a.user_id = b.USER_ID                     and  usertype_id in (1,2,9) 
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1                     and LOCNTYPE_ID = 3128420select * from    bass2.STAT_ZD_VILLAGE_USERS_201002 select * from   bass2.trans_user_id_20100625select * from    bass2.dw_product_201002 0227870select count(0) from   bass2.dw_product_200901 a , bass2.STAT_ZD_VILLAGE_USERS_200901 b where MONTH_CALL_MARK = 1and a.user_id = b.USER_ID                     and  usertype_id in (1,2,9) 
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1                     and LOCNTYPE_ID = 3                    select * from syscat.tables where tabname like  '%STAT_ZD_VILLAGE_USERS%'                                         select LOCNTYPE_ID,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200901group by LOCNTYPE_ID3	140439
select * from   bass2.STAT_ZD_VILLAGE_USERS_2009
select count(*) from bass2.dw_product_200907 a,
(
select user_id,region_flag
from
(
select time_id,user_id,region_flag
        ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.G_A_02052_MONTH
where time_id<=200907
) a
where row_id=1
  and region_flag='3'
) b,
bass2.trans_user_id_20100625 c
where b.user_id=c.new_USER_ID
  and a.user_id = c.USER_ID
  and a.usertype_id in (1,2,9) 
  and a.userstatus_id in (1,2,3,6,8)
  and a.test_mark<>1
215866select * from     bass2.STAT_ZD_VILLAGE_USERS_200901select '200901' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200901 where LOCNTYPE_ID = 3 union all
select '200902' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200902 where LOCNTYPE_ID = 3 union all
select '200903' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200903 where LOCNTYPE_ID = 3 union all
select '200904' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200904 where LOCNTYPE_ID = 3 union all
select '200905' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200905 where LOCNTYPE_ID = 3 union all
select '200906' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200906 where LOCNTYPE_ID = 3 union all
select '200907' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200907 where LOCNTYPE_ID = 3 union all
select '200908' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200908 where LOCNTYPE_ID = 3 union all
select '200909' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200909 where LOCNTYPE_ID = 3 union all
select '200910' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200910 where LOCNTYPE_ID = 3 union all
select '200911' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200911 where LOCNTYPE_ID = 3 union all
select '200912' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200912 where LOCNTYPE_ID = 3 union all
select '201001' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201001 where LOCNTYPE_ID = 3 union all
select '201002' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201002 where LOCNTYPE_ID = 3 union all
select '201003' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201003 where LOCNTYPE_ID = 3 union all
select '201004' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201004 where LOCNTYPE_ID = 3 union all
select '201005' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201005 where LOCNTYPE_ID = 3 union all
select '201006' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201006 where LOCNTYPE_ID = 3 union all
select '201007' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201007 where LOCNTYPE_ID = 3 
                               CREATE TABLE BASS1.G_S_22080_DAY
 (
	 TIME_ID            	INTEGER             ----记录行号        
	,OP_TIME            	CHAR(8)             ----日期 主键       
	,QRY_CNT            	CHAR(12)            ----查询量 单位：次 
	,CANCEL_CNT         	CHAR(12)            ----退订量 单位：次 
	,CANCEL_FAIL_CNT    	CHAR(12)            ----退订失败量 单位：次
	,COMPLAINT_CNT      	CHAR(12)            ----投诉量 单位：次 
	,ALL_CANCEL_CNT     	CHAR(12)            ----当天退订业务总数 单位：次
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22080_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
                   select * from    app.sch_control_task where priority_val > 100                   select time_id,count(0) from    bass1.G_A_02052_MONTHwhere region_flag='3'group by time_id                   select * from    bass2.dw_product_unite_cancel_order_201103 where sts = 0select  sts,count(0) from   bass2.dw_product_unite_cancel_order_201103group by stsselect * from table(
select substr(control_code , 11,5) unit_code,substr(b.CONTROL_CODE,7,13) from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'd'and b.control_code like '%DAY%'
) t where unit_code = '22012'select * from     bass1.G_S_22012_DAYselect time_id/100,sum(bigint(M_BILL_DURATION)) from   bass1.G_S_22012_DAYgroup by time_id/100order by 1 desc2100321006
select * from table(
select substr(control_code , 11,5) unit_code,substr(b.CONTROL_CODE,7,15),b.control_code from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'm'and b.control_code like '%MONTH%'
) t where unit_code = '21006'


 G_S_21003_MONTHG_S_21006_MONTHselect  sum(bigint(BASE_BILL_DURATION)) d from G_S_21003_MONTH where time_id = 201101select  sum(bigint(BASE_BILL_DURATION)) d from G_S_21003_MONTH where time_id = 201102select  sum(bigint(BASE_BILL_DURATION)) d from G_S_21003_MONTH where time_id = 201103--680598277select  sum(bigint(BASE_BILL_DURATION)) d from G_S_21003_MONTH where time_id = 201104select  sum(bigint(BASE_BILL_DURATION)) d from G_S_21003_MONTH where time_id = 201105select  sum(bigint(BASE_BILL_DURATION)) d from G_S_21003_MONTH where time_id = 201106select  sum(bigint(BASE_BILL_DURATION)) d from G_S_21003_MONTH where time_id = 201107201103	680378057
680598277	5651836140
201103	680378057
西藏	670515699	684457694	669302779	680432919	674032989	676852669	608161793	527503654	680598277	
608161793	4822228380
BASE_BILL_DURATIONTOLL_BILL_DURATIONselect time_id,sum(d) from (select time_id, bigint(BASE_BILL_DURATION) d from G_S_21003_MONTHunion all select time_id,bigint(TOLL_BILL_DURATION) d from G_S_21003_MONTHunion all select time_id,bigint(BASE_BILL_DURATION) d from G_S_21006_MONTHunion all select time_id,bigint(TOLL_BILL_DURATION) d from G_S_21006_MONTH) t group by time_idselect * from    bass2.dw_enterprise_msg_201103where enterprise_idin
('891910006907'
,'891910006537'
,'891910005097'
,'891910005160'
,'891910006292'
,'891910005743'
,'891910006739'
,'891910006669')
CREATE TABLE BASS1.G_I_77780_DAY_b20110421
 (
	 TIME_ID            		INTEGER        NOT NULL       ----数据日期        
	,ENTERPRISE_ID      		CHAR(20)       					       ----集团客户标识    
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
   (ENTERPRISE_ID,ID) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY_b20110421
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;        
insert into  BASS1.G_I_77780_DAY_b20110421
select * from BASS1.G_I_77780_DAYdelete from  BASS1.G_I_77780_DAY 

drop table  BASS1.G_I_77780_DAY_MID_a ;
CREATE TABLE BASS1.G_I_77780_DAY_MID_a (
	 TIME_ID            		INTEGER               ----数据日期        
	,ENTERPRISE_ID      		CHAR(20)              ----集团客户标识    
	,ID                 		CHAR(9)        								----ID*              
	,ENTERPRISE_NAME    		CHAR(60)       								----集团客户名称*    
	,ORG_TYPE           		CHAR(5)             					----机构类型 							BASS_STD_0001       
		--ADDR_CODE不规范6 CHAR(6) 
	,ADDR_CODE          		VARCHAR(10)             					----地址代码*        
	,CITY               		CHAR(20)            					----城市地区*        
	---REGION 不规范 9           		CHAR(20) 
	,REGION             		VARCHAR(100)            					----区县*            
	---COUNTY 不规范 10            		CHAR(20) 	
	,COUNTY             		VARCHAR(100)            					----乡镇*            
	,DOOR_NO            		CHAR(60)            					----门牌*            
	,AREA_CODE          		CHAR(5)             					----区号*            
	--PHONE_NO1 不规范  12       		CHAR(11)
	,PHONE_NO1          		varCHAR(111)            					----电话1*           
	--,PHONE_NO2  不规范  13        		CHAR(10) 
	,PHONE_NO2          		VARCHAR(110)            					----电话2*           
	--POST_CODE 不规范  14         		CHAR(6)
	,POST_CODE          		varCHAR(16)             					----邮政编码*        
--	,INDUSTRY_TYPE  不规范  15    		CHAR(4) 
	,INDUSTRY_TYPE      		VARCHAR(14)             					----行业类型 							BASS_STD1_0113       
	,EMPLOYEE_CNT       		CHAR(8)             					----职员            
	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----产业单位数*      
	--ECONOMIC_TYPE  不规范  18    		CHAR(3)
	,ECONOMIC_TYPE      		CHAR(13)             					----经济类型 							BASS_STD_0002       
	--OPEN_YEAR  不规范  19        		CHAR(4)
	,OPEN_YEAR          		varCHAR(14)             					----开业1           
	--OPEN_MONTH 不规范  20        		CHAR(2) 
	,OPEN_MONTH         		varCHAR(22)             					----开业2           
	--SHAREHOLDER不规范CHAR(1) 
	,SHAREHOLDER        		VARCHAR(4)             					----控股  								BASS_STD_0005          
	--GROUP_TYPE不规范CHAR(1) 
	,GROUP_TYPE         		VARCHAR(2)             					----集团类型 							BASS_STD_0003       
	--MANAGE_STYLE不规范CHAR(1) 
	,MANAGE_STYLE       		VARCHAR(4)             					----经营形式      				BASS_STD_0004  
	,OPERATE_REVENUE_CLASS	CHAR(2)           						----营业收入分类 					BASS_STD_0006   
	,CAPITAL_CLASS      		CHAR(2)           						----资产分类     					BASS_STD_0007   
	,INDUSTRY_CLASS_CODE		CHAR(2)            	----行业分类编码 					BASS_STD1_0043   
	,CUST_STATUS        		CHAR(2)             					----集团客户状态    
	,CUST_INFO_SRC_ID   		CHAR(1)             					----集团客户资料来源
	,UPLOAD_TYPE_ID     		CHAR(1)            	----上传种类标识    
)
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,ENTERPRISE_ID) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY_MID_a
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
      select * from BASS1.dim_bass1_std_map        select * from  BASS1.G_I_77780_DAY_MID_a        CREATE TABLE BASS1.dim_bass1_std_map
 (
	 interface_code       CHAR(5)            
	,dim_table_id      		CHAR(20)       NOT NULL      
	,code                 CHAR(9)        NOT NULL				       
	,code_name    		    CHAR(60)       NOT NULL											
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (dim_table_id,code) USING HASHING;

ALTER TABLE BASS1.dim_bass1_std_map
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
create table bass1.G_I_77780_DAY_MID2 like bass1.G_I_77780_DAY
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,ID) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY_MID2
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;


insert into bass1.G_I_77780_DAY_MID2
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
      ,case when ADDR_CODE is null or length(ADDR_CODE) <> 6 then '540102' else ADDR_CODE end ADDR_CODE
        ,CITY
        ,substr(REGION,1,20) REGION
        ,substr(COUNTY,1,20) COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,replace(PHONE_NO1,'-','') PHONE_NO1
        ,substr(replace(PHONE_NO2,'-',''),1,10) PHONE_NO2
        ,case when POST_CODE is null or length(POST_CODE) <> 6 then '850000' else POST_CODE end POST_CODE
        ,case when INDUSTRY_TYPE is null or length(INDUSTRY_TYPE) <> 4 then '9421' else INDUSTRY_TYPE end INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,case when ECONOMIC_TYPE is null or length(trim(ECONOMIC_TYPE)) <> 3 then '190' else trim(ECONOMIC_TYPE) end ECONOMIC_TYPE
        ,case when OPEN_YEAR is null then '2002' when  length(OPEN_YEAR) <> 4  then substr(OPEN_YEAR,1,4) else OPEN_YEAR end OPEN_YEAR
        ,case when length(OPEN_MONTH) =1 then '0'||trim(OPEN_MONTH) when length(OPEN_MONTH) <> 2 then '01' else OPEN_MONTH end OPEN_MONTH
        ,case when SHAREHOLDER is null then '9' 
        	when SHAREHOLDER not in ('1','2','3','4','5') then '9' else SHAREHOLDER end SHAREHOLDER
        ,case when GROUP_TYPE not in ('1','2') then '2' else GROUP_TYPE end GROUP_TYPE
        ,case when MANAGE_STYLE is null then '9' 
        	when length(MANAGE_STYLE) = 2 and  substr(MANAGE_STYLE,2,1) in ('1','2','3') then substr(MANAGE_STYLE,2,1)
        	when length(MANAGE_STYLE) = 1 and  MANAGE_STYLE in ('1','2','3') then MANAGE_STYLE 
      		else '9' end MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
      ,case when INDUSTRY_CLASS_CODE is null then '99'  else INDUSTRY_CLASS_CODE end INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
      ,case when UPLOAD_TYPE_ID is null then '3' else UPLOAD_TYPE_ID end UPLOAD_TYPE_ID
from bass1.G_I_77780_DAY_MID_a a

create table bass1.G_I_77780_DAY_MID3 like bass1.G_I_77780_DAY
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,ID) USING HASHING;
   
select a.*				from  bass1.G_I_77780_DAY_MID2 a
				 join  BASS1.grp_id_old_new_map_20110330  b on a.ENTERPRISE_ID = b.old_enterprise_id

delete from bass1.G_I_77780_DAY_MID3;
insert into bass1.G_I_77780_DAY_MID3
select 
         TIME_ID
        ,NEW_ENTERPRISE_ID
        ,ID
        ,NEW_ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from   (select a.* 
		,VALUE(b.new_enterprise_id,a.ENTERPRISE_ID) NEW_ENTERPRISE_ID
		,VALUE(b.ENTERPRISE_NAME,a.ENTERPRISE_NAME) NEW_ENTERPRISE_NAME
		, row_number()over(PARTITION by ENTERPRISE_ID,id order by  UPLOAD_TYPE_ID asc ) rn 
				from  bass1.G_I_77780_DAY_MID2 a
				left join  BASS1.grp_id_old_new_map_20110330  b on a.ENTERPRISE_ID = b.old_enterprise_id
			 ) t where t.rn = 1 
insert into bass1.G_I_77780_DAY_MID3select * from 		 bass1.G_I_77780_DAY_MID2                        select * from    bass1.G_I_77780_DAY_MID3                 select count(0),count(distinct value(ENTERPRISE_ID,'a')||value(id,'a')) 
from  bass1.G_I_77780_DAY_MID3

 select value(ENTERPRISE_ID,'a')||value(id,'a'),count(0),count(distinct value(ENTERPRISE_ID,'a')||value(id,'a'))   from    G_I_77780_DAY_MID3     group by value(ENTERPRISE_ID,'a')||value(id,'a')having count(distinct value(ENTERPRISE_ID,'a')||value(id,'a'))  > 189103001221422      219667969	2	1
89102999676402      XZ0309285	2	1
select * from G_I_77780_DAY_MID3where ENTERPRISE_ID in ('89103001221422','89102999676402')and id in ('219667969','XZ0309285')update G_I_77780_DAY_MID3set  upload_type_id = '3'where  ENTERPRISE_ID = '89102999676402      'delete from G_I_77780_DAY_MID3where enterprise_id = '89103001221422'and city = '拉萨'delete from G_I_77780_DAY_MID3where enterprise_id = '89102999676402'and enterprise_name = '拉萨市八一小校'select upload_type_id,count(0)from G_I_77780_DAY_MID3group by upload_type_id select count(0),count(distinct value(ENTERPRISE_ID,'a')||value(id,'a')) 
from  bass1.G_I_77780_DAY_MID3
select count(0)
from 
(select distinct ORG_TYPE ORG_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ORG_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0001')
select count(0)
from 
(select distinct ORG_TYPE ORG_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ORG_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0001')



drop table BASS1.t_bass1_std_0113
CREATE TABLE BASS1.t_bass1_std_0113
 (
	 code    char(4)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (code) USING HASHING;


 select count(0)
from 
(select distinct trim(INDUSTRY_TYPE) INDUSTRY_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_TYPE  not in 
(select code from BASS1.t_bass1_std_0113 )
select count(0)
from 
(select distinct ECONOMIC_TYPE ECONOMIC_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ECONOMIC_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD_0002')
select ECONOMIC_TYPE,count(0)from  bass1.G_I_77780_DAY_MID3group by ECONOMIC_TYPEselect * from  BASS1.dim_bass1_std_map select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0002'select count(0)
from 
(select distinct ECONOMIC_TYPE ECONOMIC_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ECONOMIC_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0002')
select *
from 
(select distinct ECONOMIC_TYPE ECONOMIC_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ECONOMIC_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0002')
update  bass1.G_I_77780_DAY_MID3
update G_I_77780_DAY_MID3 
set ECONOMIC_TYPE = '171'
where ECONOMIC_TYPE in 
(
'170'
)
select * from    G_I_77780_DAY_MID3 
where ECONOMIC_TYPE in 
('100'
,'101'
,'150'
,'170'
)

select count(0)
from 
(select distinct ECONOMIC_TYPE ECONOMIC_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ECONOMIC_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0002')

select count(0)
from 
(select distinct SHAREHOLDER SHAREHOLDER from   bass1.G_I_77780_DAY_MID3) a 
where SHAREHOLDER  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0005')

select count(0)
from 
(select distinct GROUP_TYPE GROUP_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where GROUP_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0003')


select count(0)
from 
(select distinct MANAGE_STYLE MANAGE_STYLE from   bass1.G_I_77780_DAY_MID3) a 
where MANAGE_STYLE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0004')

select count(0)
from 
(select distinct OPERATE_REVENUE_CLASS OPERATE_REVENUE_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where OPERATE_REVENUE_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0006')

select count(0)
from 
(select distinct OPERATE_REVENUE_CLASS OPERATE_REVENUE_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where OPERATE_REVENUE_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0006')


update G_I_77780_DAY_MID3 
set OPERATE_REVENUE_CLASS = '0'||OPERATE_REVENUE_CLASS
where  OPERATE_REVENUE_CLASSin (select *
from 
(select distinct OPERATE_REVENUE_CLASS OPERATE_REVENUE_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where OPERATE_REVENUE_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0006')
)
select '0'||OPERATE_REVENUE_CLASSfrom G_I_77780_DAY_MID3where OPERATE_REVENUE_CLASSin (select *
from 
(select distinct OPERATE_REVENUE_CLASS OPERATE_REVENUE_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where OPERATE_REVENUE_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0006')
)
select *
from 
(select distinct CAPITAL_CLASS CAPITAL_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where CAPITAL_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')

select * from  bass1.G_I_77780_DAY_MID3where CAPITAL_CLASSin (
select *
from 
(select distinct CAPITAL_CLASS CAPITAL_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where CAPITAL_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')
)
update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '0'||CAPITAL_CLASS
where  CAPITAL_CLASSin (
select *
from 
(select distinct CAPITAL_CLASS CAPITAL_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where CAPITAL_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')
)select count(0)
from 
(select distinct CAPITAL_CLASS CAPITAL_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where CAPITAL_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')


update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '0'||trim(CAPITAL_CLASS)
where length(trim(CAPITAL_CLASS)) = 1



update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '05'
where CAPITAL_CLASS is null 



update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '01'
where CAPITAL_CLASS = '00'

select count(0)
from 
(select distinct INDUSTRY_CLASS_CODE INDUSTRY_CLASS_CODE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_CLASS_CODE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')


select *
from 
(select distinct INDUSTRY_CLASS_CODE INDUSTRY_CLASS_CODE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_CLASS_CODE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')

select distinct INDUSTRY_CLASS_CODE INDUSTRY_CLASS_CODE from   bass1.G_I_77780_DAY_MID3

update G_I_77780_DAY_MID3 
set INDUSTRY_CLASS_CODE = '0'||TRIM(INDUSTRY_CLASS_CODE)
where length(trim(INDUSTRY_CLASS_CODE)) = 1

select INDUSTRY_CLASS_CODE,count(0)from G_I_77780_DAY_MID3
group by INDUSTRY_CLASS_CODE

update G_I_77780_DAY_MID3 
set INDUSTRY_CLASS_CODE = '08'
where  INDUSTRY_CLASS_CODE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')


select count(0)
from 
(select distinct INDUSTRY_CLASS_CODE INDUSTRY_CLASS_CODE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_CLASS_CODE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')
select *
from 
(select distinct INDUSTRY_CLASS_CODE INDUSTRY_CLASS_CODE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_CLASS_CODE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007') select distinct CUST_STATUS CUST_STATUS from   bass1.G_I_77780_DAY_MID3
select * from  bass1.G_I_77780_DAY_MID3where CUST_STATUS = '10'

update G_I_77780_DAY_MID3 
set CUST_STATUS = '11'
where  CUST_STATUS = '10'



update G_I_77780_DAY_MID3 
set CUST_INFO_SRC_ID = '1'
where  CUST_INFO_SRC_ID IS NULL 
select * from   bass1.G_I_77780_DAY_MID3select * from    bass1.g_i_77780_day
select * from bass1.g_i_77780_day_mid3 where TIME_ID                   is null 
select * from bass1.g_i_77780_day_mid3 where ENTERPRISE_ID             is null 
select * from bass1.g_i_77780_day_mid3 where ID                        is null --update bass1.g_i_77780_day set id = ' ' where ID                        is null 
select * from bass1.g_i_77780_day_mid3 where ORG_TYPE                  is null 
select * from bass1.g_i_77780_day_mid3 where ADDR_CODE                 is null 
select * from bass1.g_i_77780_day_mid3 where CITY                      is null 
select * from bass1.g_i_77780_day_mid3 where REGION                    is null 
select * from bass1.g_i_77780_day_mid3 where COUNTY                    is null --update  bass1.g_i_77780_day set COUNTY = ' ' where COUNTY                        is null 
select * from bass1.g_i_77780_day_mid3 where DOOR_NO                   is null --update  bass1.g_i_77780_day set DOOR_NO = ' ' where DOOR_NO                        is null 
select * from bass1.g_i_77780_day_mid3 where AREA_CODE                 is null 
select * from bass1.g_i_77780_day_mid3 where PHONE_NO1                 is null --update  bass1.g_i_77780_day set PHONE_NO1 = ' ' where PHONE_NO1                        is null 
select * from bass1.g_i_77780_day_mid3 where PHONE_NO2                 is null --update  bass1.g_i_77780_day set PHONE_NO2 = ' ' where PHONE_NO2                        is null 
select * from bass1.g_i_77780_day_mid3 where POST_CODE                 is null --update  bass1.g_i_77780_day set POST_CODE = ' ' where POST_CODE                        is null 
select * from bass1.g_i_77780_day_mid3 where INDUSTRY_TYPE             is null --update  bass1.g_i_77780_day set INDUSTRY_TYPE = ' ' where INDUSTRY_TYPE                        is null 
select * from bass1.g_i_77780_day_mid3 where EMPLOYEE_CNT              is null --
select * from bass1.g_i_77780_day_mid3 where INDUSTRY_UNIT_CNT         is null 
select * from bass1.g_i_77780_day_mid3 where ECONOMIC_TYPE             is null 
select * from bass1.g_i_77780_day_mid3 where OPEN_YEAR                 is null 
select * from bass1.g_i_77780_day_mid3 where OPEN_MONTH                is null 
select * from bass1.g_i_77780_day_mid3 where SHAREHOLDER               is null 
select * from bass1.g_i_77780_day_mid3 where GROUP_TYPE                is null 
select * from bass1.g_i_77780_day_mid3 where MANAGE_STYLE              is null 
select * from bass1.g_i_77780_day_mid3 where OPERATE_REVENUE_CLASS     is null 
select * from bass1.g_i_77780_day_mid3 where CAPITAL_CLASS             is null 
select * from bass1.g_i_77780_day_mid3 where INDUSTRY_CLASS_CODE       is null 
select * from bass1.g_i_77780_day_mid3 where CUST_STATUS               is null 
select * from bass1.g_i_77780_day_mid3 where CUST_INFO_SRC_ID          is null 
select * from bass1.g_i_77780_day_mid3 where UPLOAD_TYPE_ID            is null OPEN_YEARselect GROUP_TYPE from g_i_77780_day_mid3update g_i_77780_day_mid3set GROUP_TYPE = '1'where GROUP_TYPE is null select OPEN_YEAR,count(0)from g_i_77780_day_mid3group by OPEN_YEARselect OPEN_MONTH,count(0)from g_i_77780_day_mid3group by OPEN_MONTHselect OPEN_MONTH,count(0)from g_i_77780_day_mid3group by OPEN_MONTHupdate g_i_77780_day_mid3set OPEN_MONTH = '01'where OPEN_MONTH is null select * from   g_i_77780_day_mid3delete from g_i_77780_dayinsert into g_i_77780_dayselect * from g_i_77780_day_mid3select * from app.g_runlog 
where return_flag=1and unit_code = '77780'update app.g_runlog set return_flag = 0
where return_flag=1and unit_code = '77780'
update BASS1.G_I_77780_DAY
set CUST_STATUS = ' '
where CUST_STATUS is null 
;

update BASS1.G_I_77780_DAY
set CUST_INFO_SRC_ID = ' '
where CUST_INFO_SRC_ID is null 
;

update BASS1.G_I_77780_DAY
set UPLOAD_TYPE_ID = ' '
where UPLOAD_TYPE_ID is null 
;

update BASS1.G_I_77780_DAY
set GROUP_TYPE = ' '
where GROUP_TYPE is null 
;


update BASS1.G_I_77780_DAY
set MANAGE_STYLE = ' '
where MANAGE_STYLE is null 
;

update BASS1.G_I_77780_DAY
set OPERATE_REVENUE_CLASS = ' '
where OPERATE_REVENUE_CLASS is null 
;

update BASS1.G_I_77780_DAY
set CAPITAL_CLASS = ' '
where CAPITAL_CLASS is null 
;

update BASS1.G_I_77780_DAY
set INDUSTRY_CLASS_CODE = ' '
where INDUSTRY_CLASS_CODE is null 
;
select * from bass1.g_i_77780_day_mid3 where TIME_ID                   is null            ;
select * from bass1.g_i_77780_day_mid3 where ENTERPRISE_ID             is null            ;
select * from bass1.g_i_77780_day_mid3 where ID                        is null            ;
--update bass1.g_i_77780_day set id = ' ' where ID                        is null         ;
select * from bass1.g_i_77780_day_mid3 where ORG_TYPE                  is null            ;
select * from bass1.g_i_77780_day_mid3 where ADDR_CODE                 is null            ;
select * from bass1.g_i_77780_day_mid3 where CITY                      is null            ;
select * from bass1.g_i_77780_day_mid3 where REGION                    is null            ;
select * from bass1.g_i_77780_day_mid3 where COUNTY                    is null            ;
--update  bass1.g_i_77780_day set COUNTY = ' ' where COUNTY    is null                    ;
select * from bass1.g_i_77780_day_mid3 where DOOR_NO                   is null            ;
--update  bass1.g_i_77780_day set DOOR_NO = ' ' where DOOR_NO  is null                    ;
select * from bass1.g_i_77780_day_mid3 where AREA_CODE                 is null            ;
select * from bass1.g_i_77780_day_mid3 where PHONE_NO1                 is null            ;
--update  bass1.g_i_77780_day set PHONE_NO1 = ' ' where PHONE_NO1  is null                ;
select * from bass1.g_i_77780_day_mid3 where PHONE_NO2                 is null            ;
--update  bass1.g_i_77780_day set PHONE_NO2 = ' ' where PHONE_NO2  is null                ;
select * from bass1.g_i_77780_day_mid3 where POST_CODE                 is null            ;
--update  bass1.g_i_77780_day set POST_CODE = ' ' where POST_CODE   is null               ;
select * from bass1.g_i_77780_day_mid3 where INDUSTRY_TYPE             is null            ;
--update  bass1.g_i_77780_day set INDUSTRY_TYPE = ' ' where INDUSTRY_TYPE  is null        ;
select * from bass1.g_i_77780_day_mid3 where EMPLOYEE_CNT              is null            ;
--                                                                                        ;
select * from bass1.g_i_77780_day_mid3 where INDUSTRY_UNIT_CNT         is null            ;
select * from bass1.g_i_77780_day_mid3 where ECONOMIC_TYPE             is null            ;
select * from bass1.g_i_77780_day_mid3 where OPEN_YEAR                 is null            ;
select * from bass1.g_i_77780_day_mid3 where OPEN_MONTH                is null            ;
select * from bass1.g_i_77780_day_mid3 where SHAREHOLDER               is null            ;
select * from bass1.g_i_77780_day_mid3 where GROUP_TYPE                is null            ;
select * from bass1.g_i_77780_day_mid3 where MANAGE_STYLE              is null            ;
select * from bass1.g_i_77780_day_mid3 where OPERATE_REVENUE_CLASS     is null            ;
select * from bass1.g_i_77780_day_mid3 where CAPITAL_CLASS             is null            ;
select * from bass1.g_i_77780_day_mid3 where INDUSTRY_CLASS_CODE       is null            ;
select * from bass1.g_i_77780_day_mid3 where CUST_STATUS               is null            ;
select * from bass1.g_i_77780_day_mid3 where CUST_INFO_SRC_ID          is null            ;
select * from bass1.g_i_77780_day_mid3 where UPLOAD_TYPE_ID            is null            ;

select * from bass1.g_i_77780_day where TIME_ID                   is null            ;
select * from bass1.g_i_77780_day where ENTERPRISE_ID             is null            ;
select * from bass1.g_i_77780_day where ID                        is null            ;
--update bass1.g_i_77780_day set id = ' ' where ID                        is null         ;
select * from bass1.g_i_77780_day where ORG_TYPE                  is null            ;
select * from bass1.g_i_77780_day where ADDR_CODE                 is null            ;
select * from bass1.g_i_77780_day where CITY                      is null            ;
select * from bass1.g_i_77780_day where REGION                    is null            ;
select * from bass1.g_i_77780_day where COUNTY                    is null            ;
--update  bass1.g_i_77780_day set COUNTY = ' ' where COUNTY    is null                    ;
select * from bass1.g_i_77780_day where DOOR_NO                   is null            ;
--update  bass1.g_i_77780_day set DOOR_NO = ' ' where DOOR_NO  is null                    ;
select * from bass1.g_i_77780_day where AREA_CODE                 is null            ;
select * from bass1.g_i_77780_day where PHONE_NO1                 is null            ;
--update  bass1.g_i_77780_day set PHONE_NO1 = ' ' where PHONE_NO1  is null                ;
select * from bass1.g_i_77780_day where PHONE_NO2                 is null            ;
--update  bass1.g_i_77780_day set PHONE_NO2 = ' ' where PHONE_NO2  is null                ;
select * from bass1.g_i_77780_day where POST_CODE                 is null            ;
--update  bass1.g_i_77780_day set POST_CODE = ' ' where POST_CODE   is null               ;
select * from bass1.g_i_77780_day where INDUSTRY_TYPE             is null            ;
--update  bass1.g_i_77780_day set INDUSTRY_TYPE = ' ' where INDUSTRY_TYPE  is null        ;
select * from bass1.g_i_77780_day where EMPLOYEE_CNT              is null            ;
--                                                                                        ;
select * from bass1.g_i_77780_day where INDUSTRY_UNIT_CNT         is null            ;
select * from bass1.g_i_77780_day where ECONOMIC_TYPE             is null            ;
select * from bass1.g_i_77780_day where OPEN_YEAR                 is null            ;
select * from bass1.g_i_77780_day where OPEN_MONTH                is null            ;
select * from bass1.g_i_77780_day where SHAREHOLDER               is null            ;
select * from bass1.g_i_77780_day where GROUP_TYPE                is null            ;
select * from bass1.g_i_77780_day where MANAGE_STYLE              is null            ;
select * from bass1.g_i_77780_day where OPERATE_REVENUE_CLASS     is null            ;
select * from bass1.g_i_77780_day where CAPITAL_CLASS             is null            ;
select * from bass1.g_i_77780_day where INDUSTRY_CLASS_CODE       is null            ;
select * from bass1.g_i_77780_day where CUST_STATUS               is null            ;
select * from bass1.g_i_77780_day where CUST_INFO_SRC_ID          is null            ;
select * from bass1.g_i_77780_day where UPLOAD_TYPE_ID            is null            ;
update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '2040'
where INDUSTRY_TYPE = '2050'
;


update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '3010'
where INDUSTRY_TYPE = '3000'
;
 select count(0),count(distinct value(ENTERPRISE_ID,'a')||value(id,'a')) 
from   BASS1.G_I_77780_DAY



update BASS1.G_I_77780_DAY
set CUST_STATUS = ' '
where CUST_STATUS is null 
;

update BASS1.G_I_77780_DAY
set CUST_INFO_SRC_ID = ' '
where CUST_INFO_SRC_ID is null 
;

update BASS1.G_I_77780_DAY
set UPLOAD_TYPE_ID = ' '
where UPLOAD_TYPE_ID is null 
;

update BASS1.G_I_77780_DAY
set GROUP_TYPE = ' '
where GROUP_TYPE is null 
;


update BASS1.G_I_77780_DAY
set MANAGE_STYLE = ' '
where MANAGE_STYLE is null 
;

update BASS1.G_I_77780_DAY
set OPERATE_REVENUE_CLASS = ' '
where OPERATE_REVENUE_CLASS is null 
;

update BASS1.G_I_77780_DAY
set CAPITAL_CLASS = ' '
where CAPITAL_CLASS is null 
;

update BASS1.G_I_77780_DAY
set INDUSTRY_CLASS_CODE = ' '
where INDUSTRY_CLASS_CODE is null 
;


update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '7210'
where INDUSTRY_TYPE = '7214'
;

update G_I_77780_DAY_MID3 
set CUST_STATUS = '36'
where  CUST_STATUS IS NULL 





update G_I_77780_DAY_MID3 
set CUST_INFO_SRC_ID = '1'
where  CUST_INFO_SRC_ID IS NULL 


update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '0'||trim(CAPITAL_CLASS)
where length(trim(CAPITAL_CLASS)) = 1




update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '05'
where CAPITAL_CLASS is null 



update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '01'
where CAPITAL_CLASS = '00'
update  bass1.G_I_77780_DAY_MID3
set ORG_TYPE = '51'
where ORG_TYPE = '5'


update G_I_77780_DAY_MID3 
set ECONOMIC_TYPE = '110'
where ECONOMIC_TYPE in 
('100'
,'101'
,'150'
,'170'
)

update G_I_77780_DAY_MID3 
set OPERATE_REVENUE_CLASS = '10'
where OPERATE_REVENUE_CLASS is null 
select * from APP.G_FILE_REPORTwhere filename like '%77780%'select * from   app.g_runlog where unit_code = '77780'session.int_check_user_status select product_no, count(0) from session.int_check_user_status  where usertype_id NOT IN ('2010','2020','2030','9000')group by product_nohaving count(0) >  113518902680    select * from    session.int_check_user_status where product_no = '13518902680'G_S_21003_MONTH                 select user_id,region_flag,row_number()over(partition by user_id order by time_id desc) row_id 
                from bass1.G_A_02052_MONTHselect * from    bass1.G_A_02052_MONTHdeclare global temporary table session.BASE_BILL_DURATION    (   product_no     CHARACTER(15) ,    BASE_BILL_DURATION bigint    )                            partitioning key            (   product_no     ) using hashing           with replace on commit preserve rows not logged in tbs_user_tempinsert into session.BASE_BILL_DURATIONselect PRODUCT_NO,sum(bigint(BASE_BILL_DURATION)) BASE_BILL_DURATIONfrom G_S_21003_MONTHwhere time_id = 201103group by PRODUCT_NO1251046 row(s) affected.select sum(BASE_BILL_DURATION) from session.BASE_BILL_DURATIONdeclare global temporary table session.region_flag    (   user_id     CHARACTER(20) ,    REGION_FLAG  CHARACTER(1)     )                            partitioning key            (   user_id     ) using hashing           with replace on commit preserve rows not logged in tbs_user_tempinsert into session.region_flagselect user_id,region_flagfrom (                select user_id,region_flag,row_number()over(partition by user_id order by time_id desc) row_id 
                from bass1.G_A_02052_MONTH                ) t where row_id = 1create index session.idx_region_flag on  session.region_flag(user_id)create index session.int_check_user_status on  session.int_check_user_status(user_id)create index session.idx_BASE_BILL_DURATION    on    session.BASE_BILL_DURATION(PRODUCT_NO)          select count(0),count(distinct user_id ) from    session.region_flag
select   region_flag,sum(c.BASE_BILL_DURATION) BASE_BILL_DURATION
from         session.region_flag a
inner join session.int_check_user_status b on a.user_id = b.user_id
inner join       session.BASE_BILL_DURATION c on b.PRODUCT_NO = c.PRODUCT_NO
group by region_flag
                
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
insert into t_int_check_user_status(
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
where time_id<=20110331 ) e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where time_id<=20110331 ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1
and f.usertype_id NOT IN ('2010','2020','2030','9000')

create table t_int_check_user_status like     session.int_check_user_status
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING


            

declare global temporary table session.BASE_BILL_DURATION
    (
   product_no     CHARACTER(15) ,
    BASE_BILL_DURATION bigint
    )                            
partitioning key           
 (
   product_no    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp

                        

declare global temporary table session.BASE_BILL_DURATION
    (
   product_no     CHARACTER(15) ,
    BASE_BILL_DURATION bigint
    )                            
partitioning key           
 (
   product_no    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp

             create table t_BASE_BILL_DURATION like     session.int_check_user_status
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (product_no
   ) USING HASHING
           
create table t_int_check_user_status like     session.int_check_user_status
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (product_no
   ) USING HASHING
                        
insert into t_BASE_BILL_DURATION
select PRODUCT_NO,sum(bigint(BASE_BILL_DURATION)) BASE_BILL_DURATION
from G_S_21003_MONTH
where time_id = 201103
group by PRODUCT_NOcreate index idx on  session.region_flag(user_id)create index session.int_check_user_status on  session.int_check_user_status(user_id)create index session.idx_BASE_BILL_DURATION    on    session.BASE_BILL_DURATION(PRODUCT_NO)                              drop table t_BASE_BILL_DURATION 
create table t_BASE_BILL_DURATION like     session.BASE_BILL_DURATION
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (product_no
   ) USING HASHING

declare global temporary table session.region_flag
    (
   user_id     CHARACTER(20) ,
    REGION_FLAG  CHARACTER(1) 
    )                            
partitioning key           
 (
   user_id    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp
drop table t_region_flag 
create table t_region_flag like     session.region_flag
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING

insert into t_region_flag
select user_id,region_flag
from 
(
                select user_id,region_flag,row_number()over(partition by user_id order by time_id desc) row_id 
                from bass1.G_A_02052_MONTH
                ) t where row_id = 1
                
create index idx_region_flag on  t_region_flag(user_id)create index idx_check_user_status on  t_int_check_user_status(user_id)create index idx_BASE_BILL_DURATION    on   t_BASE_BILL_DURATION(PRODUCT_NO)                              db2 runstats on table bass1.t_region_flag with distribution and detailed indexes all      db2 runstats on table bass1.t_int_check_user_status with distribution and detailed indexes all      db2 runstats on table bass1.t_BASE_BILL_DURATION with distribution and detailed indexes all                  
select   region_flag,sum(c.BASE_BILL_DURATION) BASE_BILL_DURATION
from         t_region_flag a
inner join t_int_check_user_status b on a.user_id = b.user_id
inner join       t_int_check_user_status c on b.PRODUCT_NO = c.PRODUCT_NO
 where usertype_id NOT IN ('2010','2020','2030','9000')
group by region_flag

select   region_flag,sum(c.BASE_BILL_DURATION) BASE_BILL_DURATION
from         t_region_flag a
inner join t_int_check_user_status b on a.user_id = b.user_id
inner join      t_BASE_BILL_DURATION c on b.PRODUCT_NO = c.PRODUCT_NO
group by region_flag
select sum(BASE_BILL_DURATION) from t_BASE_BILL_DURATION680598277select count(0),count(distinct user_id  ) from  t_region_flag  select count(0),count(distinct user_id  ) from  t_int_check_user_statusselect count(0),count(distinct PRODUCT_NO  ) from  t_BASE_BILL_DURATION                        select  count(0)
from         t_region_flag a
inner join t_int_check_user_status b on a.user_id = b.user_id
inner join      t_BASE_BILL_DURATION c on b.PRODUCT_NO = c.PRODUCT_NO1217756select  time_id,target1,target3 from 
bass1.g_rule_check where rule_code = 'C1'
and time_id between  20110301 and 20110307order by 1 desc select * from   bass2.ODS_UP_SP_SERVICE_20110421select bill_flag,operator_name,count(0) from    bass2.ODS_DIM_UP_SP_SERVICE_20110421group by bill_flag,operator_nameorder by 1 desc select * from    bass2.ODS_DIM_UP_SP_SERVICE_20110421select count(0),count(distinct operator_code) from   bass2.ODS_DIM_UP_SP_SERVICE_20110421select * from   bass2.dw_cdr_select * from    bass2.CDR_GPRS_LOCAL_20110421  select * from    bass2.CDR_GPRS_roamin_20110421  select * from   dw_newbusi_gprs_   SELECT * FROM bass2.DIM_PROD_UP_PRODUCT_ITEMWHERE ITEM_TYPE='OFFER_PLAN'	  AND DEL_FLAG='1'	  AND SUPPLIER_ID IS NOT NULL	  WITH UR;   SELECT * FROM bass2.DIM_NEWBUSI_SPINFOWITH UR;  SELECT * FROM bass2.DIM_PROD_UP_PRODUCT_ITEMwhere name like '全球通88%'  a a.PRODUCT_ITEM_ID ;    select * from bass2.DW_PRODUCT_INS_OFF_INS_PROD_201103 b b.OFFER_ID    select a.*,b.*  --select count(0),count(distinct PRODUCT_ITEM_ID)  from (select OFFER_ID,count(0) cntfrom bass2.DW_PRODUCT_INS_OFF_INS_PROD_201103where valid_date < '2011-03-01' and expire_date >  '2011-03-01'group by OFFER_ID) a ,(  SELECT PRODUCT_ITEM_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEMWHERE ITEM_TYPE='OFFER_PLAN'	  AND DEL_FLAG='1'	  AND SUPPLIER_ID IS NOT NULL) b where a.OFFER_ID = b.PRODUCT_ITEM_IDselect b.EXTEND_ID2,a.cnt
from 
(
select OFFER_ID,count(0) cnt
from bass2.DW_PRODUCT_INS_OFF_INS_PROD_201103
where valid_date < '2011-03-01' and expire_date >  '2011-03-01'
group by OFFER_ID
) a ,(
SELECT PRODUCT_ITEM_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
        WHERE ITEM_TYPE='OFFER_PLAN'
  AND DEL_FLAG='1'
  AND SUPPLIER_ID IS NOT NULL
) b 
where a.OFFER_ID = b.PRODUCT_ITEM_ID
   

drop table BASS1.G_I_77780_DAY_DOWN20110422
CREATE TABLE BASS1.G_I_77780_DAY_DOWN20110422
 (TIME_ID                char(1)         ,
  ENTERPRISE_ID          CHARACTER(20),
  ID                     CHARACTER(9),
  ENTERPRISE_NAME        CHARACTER(60),
  ORG_TYPE               CHARACTER(5),
  ADDR_CODE              CHARACTER(6),
  CITY                   CHARACTER(20),
  REGION                 CHARACTER(20),
  COUNTY                 CHARACTER(20),
  DOOR_NO                CHARACTER(60),
  AREA_CODE              CHARACTER(5),
  PHONE_NO1              CHARACTER(11),
  PHONE_NO2              CHARACTER(10),
  POST_CODE              CHARACTER(6),
  INDUSTRY_TYPE          CHARACTER(4),
  EMPLOYEE_CNT           CHARACTER(8),
  INDUSTRY_UNIT_CNT      CHARACTER(5),
  ECONOMIC_TYPE          CHARACTER(3),
  OPEN_YEAR              CHARACTER(4),
  OPEN_MONTH             CHARACTER(2),
  SHAREHOLDER            CHARACTER(1),
  GROUP_TYPE             CHARACTER(1),
  MANAGE_STYLE           CHARACTER(1),
  OPERATE_REVENUE_CLASS  CHARACTER(2),
  CAPITAL_CLASS          CHARACTER(2),
  INDUSTRY_CLASS_CODE    CHARACTER(2)    ,
  CUST_STATUS            CHARACTER(2),
  CUST_INFO_SRC_ID       CHARACTER(1),
  UPLOAD_TYPE_ID         CHARACTER(1)   
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    ID
   ) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY_DOWN20110422
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE   select POINT_FEEDBACK_IDselect POINT_FEEDBACK_ID , count(0) 
--,  count(distinct POINT_FEEDBACK_ID ) 
from G_S_02007_month 
group by  POINT_FEEDBACK_ID 
order by 1     select user_id,ord_code,max(offer_name) offer_name,count(*) cnt from  bass2.dw_product_ord_so_log_dm_201103  a where  (a.offer_name like '%充值卡%' or a.offer_name like '%送%话费%') 
	         group by user_id,ord_code select * from   G_S_04003_DAYG_S_04003_DAYFLOWUP                         SYSIBM    CHARACTER                10     0 No    
FLOWDOWN                       SYSIBM    CHARACTER                10     0 No    select b.*from 
(
select BASE_PROD_ID , count(0) 
--,  count(distinct BASE_PROD_ID ) 
from g_i_02018_month 
group by  BASE_PROD_ID 
) a ,bass2.dim_prod_up_product_item  b where a.BASE_PROD_ID = char(b.product_item_id)
select BASE_PROD_TYPE , count(0) 
--,  count(distinct BASE_PROD_TYPE ) 
from g_i_02018_month 
group by  BASE_PROD_TYPE 
order by 1 
select * from   bass2.dim_prod_up_product_item              where name like '%全球通%88%'and item_type = 'OFFER_PLAN'and BUSINESS_DOMAIN_ID = 'Y'select substr(control_code , 11,5) unit_code,substr(b.CONTROL_CODE,7,13) from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'd'and b.control_code like '%DAY%'
select * from BASS1.MON_ALL_INTERFACE where upper(INTERFACE_name) like  '%GPRS%'        aidb_runstats $objectTable 1	free_res_val1：空的就是收费的，非空就是免费is null 收费is not null 免费不占用套餐流量的免费流量， free_res_val1 is null 套餐内:免费is not null 套餐外:select /**                  sum(bigint(data_flow_up1+data_flow_up2))*1.0/1024/1024/1024   as up_flows
                  ,sum(bigint(data_flow_down1+data_flow_down2))*1.0/1024/1024/1024 as down_flows                  --                  ,sum(case when free_res_val1 is not null and (charge1+charge2+charge3+charge4) = 0 then              bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else 0 end     ) free_is_pkg                  ,sum(case when free_res_val1 is  null and (charge1+charge2+charge3+charge4) = 0 then              bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else 0 end     ) free_not_pkg             **/                  sum( case when (charge1+charge2+charge3+charge4) > 0 then              bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else  0 end             ) not_free_not_pkgfrom  bass2.CDR_GPRS_LOCAL_20110423select                   sum(bigint(data_flow_up1+data_flow_up2))*1.0/1024/1024/1024   as up_flows
                  ,sum(bigint(data_flow_down1+data_flow_down2))*1.0/1024/1024/1024 as down_flows            from bass2.CDR_GPRS_LOCAL_20110423
            where drtype_id not in (8307)
              and bigint(product_no) not between 14734500000 
              and 14734999999 and apn_ni <> 'JF.XZ.IP.MOBILE.LAN.CHINAMOBILE'
              and service_code not in ('1010000001','1010000002','2000000000')              select count(0) from                 bass2.CDR_GPRS_LOCAL_20110423where  free_res_val1 is null and free_res_val2 is not null                     select * from bass2.DIM_GPRS_SERVICE_CODE          select * from syscat.tables where tabname LIKE '%DIM%GPRS%'            \select count(0), sum(charge1+charge2+charge3+charge4),sum( bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024 )from   bass2.CDR_GPRS_LOCAL_20110423where SERVICE_CODE = '1040000001'Dw_newbusi_gprs_dsselect BASE_PROD_NAME,BASE_PROD_TYPE,count(0) from   bass1.g_i_02018_monthwhere BASE_PROD_NAME like '%全球通%'and time_id = 201103group by BASE_PROD_NAME,BASE_PROD_TYPEselect * from    bass1.dim_base_prod_map
select BASE_PROD_TYPE , count(0) 
--,  count(distinct BASE_PROD_TYPE ) 
from bass1.g_i_02018_month 
group by  BASE_PROD_TYPE 
order by 1 
select * from    bass2.Dim_prod_up_offerselect * from   BASS1.MON_ALL_INTERFACE where unit_code = '04002'select cast(row_number()over() as char(8)) from  bass1.g_i_06032_day where time_id = 20110415
select \n\r from bass2.dual
VALUES 'Hello everyone' || CHR(10) || CHR(13) || 'i''m wave'  

select * from  bass2.dim_product_item b where  b.prod_id in (90004024,90004025,90004026,90004027,90004028,99001676,99001677,90004050,90008961,90004047,90004048,90004049,90004052,90004053,90004054,90004055,90004023,90004305,73900001,73900002,73900003,73900004,73900005,73900006)    drop table BASS1.dim_gprs_pkg_flow;
CREATE TABLE BASS1.dim_gprs_pkg_flow
 (
	prod_id integer,
	flow decimal(12,2)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (prod_id
   ) USING HASHING;

ALTER TABLE BASS1.dim_gprs_pkg_flow
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
    BASS1.dim_gprs_pkg_flow values
insert into bass1.dim_gprs_pkg_flow values (90004013,10);
insert into bass1.dim_gprs_pkg_flow values (90001311,10);
insert into bass1.dim_gprs_pkg_flow values (90004011,10);
insert into bass1.dim_gprs_pkg_flow values (90009610,10);
insert into bass1.dim_gprs_pkg_flow values (99001694,10);
insert into bass1.dim_gprs_pkg_flow values (90009401,10);
insert into bass1.dim_gprs_pkg_flow values (90004012,10);
insert into bass1.dim_gprs_pkg_flow values (90004303,10);
insert into bass1.dim_gprs_pkg_flow values (90009334,20);
insert into bass1.dim_gprs_pkg_flow values (99001642,20);
insert into bass1.dim_gprs_pkg_flow values (90004304,20);
insert into bass1.dim_gprs_pkg_flow values (99001612,20);
insert into bass1.dim_gprs_pkg_flow values (99001613,20);
insert into bass1.dim_gprs_pkg_flow values (99001643,20);
insert into bass1.dim_gprs_pkg_flow values (90600006,20);
insert into bass1.dim_gprs_pkg_flow values (90001214,20);
insert into bass1.dim_gprs_pkg_flow values (90400006,20);
insert into bass1.dim_gprs_pkg_flow values (90300006,20);
insert into bass1.dim_gprs_pkg_flow values (90001324,30);
insert into bass1.dim_gprs_pkg_flow values (90001330,30);
insert into bass1.dim_gprs_pkg_flow values (90001329,30);
insert into bass1.dim_gprs_pkg_flow values (90001323,30);
insert into bass1.dim_gprs_pkg_flow values (90001326,30);
insert into bass1.dim_gprs_pkg_flow values (90001327,30);
insert into bass1.dim_gprs_pkg_flow values (90001321,30);
insert into bass1.dim_gprs_pkg_flow values (90001313,30);
insert into bass1.dim_gprs_pkg_flow values (90009392,30);
insert into bass1.dim_gprs_pkg_flow values (90004024,30);
insert into bass1.dim_gprs_pkg_flow values (90001212,30);
insert into bass1.dim_gprs_pkg_flow values (90004040,30);
insert into bass1.dim_gprs_pkg_flow values (90004015,30);
insert into bass1.dim_gprs_pkg_flow values (90008109,30);
insert into bass1.dim_gprs_pkg_flow values (90001213,30);
insert into bass1.dim_gprs_pkg_flow values (90009614,30);
insert into bass1.dim_gprs_pkg_flow values (90001320,30);
insert into bass1.dim_gprs_pkg_flow values (90009722,30);
insert into bass1.dim_gprs_pkg_flow values (99001136,30);
insert into bass1.dim_gprs_pkg_flow values (90001312,30);
insert into bass1.dim_gprs_pkg_flow values (90001322,30);
insert into bass1.dim_gprs_pkg_flow values (90001325,30);
insert into bass1.dim_gprs_pkg_flow values (99001670,30);
insert into bass1.dim_gprs_pkg_flow values (90001319,30);
insert into bass1.dim_gprs_pkg_flow values (90001328,30);
insert into bass1.dim_gprs_pkg_flow values (90004302,30);
insert into bass1.dim_gprs_pkg_flow values (90009424,30);
insert into bass1.dim_gprs_pkg_flow values (90003312,50);
insert into bass1.dim_gprs_pkg_flow values (90003306,50);
insert into bass1.dim_gprs_pkg_flow values (90001315,50);
insert into bass1.dim_gprs_pkg_flow values (90003311,50);
insert into bass1.dim_gprs_pkg_flow values (90003305,50);
insert into bass1.dim_gprs_pkg_flow values (90003308,50);
insert into bass1.dim_gprs_pkg_flow values (99001614,50);
insert into bass1.dim_gprs_pkg_flow values (90003315,50);
insert into bass1.dim_gprs_pkg_flow values (90003307,50);
insert into bass1.dim_gprs_pkg_flow values (90001314,50);
insert into bass1.dim_gprs_pkg_flow values (90003316,50);
insert into bass1.dim_gprs_pkg_flow values (99001660,60);
insert into bass1.dim_gprs_pkg_flow values (99001644,60);
insert into bass1.dim_gprs_pkg_flow values (90009292,70);
insert into bass1.dim_gprs_pkg_flow values (90009721,70);
insert into bass1.dim_gprs_pkg_flow values (90004305,70);
insert into bass1.dim_gprs_pkg_flow values (90004023,70);
insert into bass1.dim_gprs_pkg_flow values (99001002,77);
insert into bass1.dim_gprs_pkg_flow values (99001335,77);
insert into bass1.dim_gprs_pkg_flow values (99001504,77);
insert into bass1.dim_gprs_pkg_flow values (99001316,77);
insert into bass1.dim_gprs_pkg_flow values (99001502,77);
insert into bass1.dim_gprs_pkg_flow values (99001320,77);
insert into bass1.dim_gprs_pkg_flow values (99001009,77);
insert into bass1.dim_gprs_pkg_flow values (99001311,77);
insert into bass1.dim_gprs_pkg_flow values (99001308,77);
insert into bass1.dim_gprs_pkg_flow values (99001001,77);
insert into bass1.dim_gprs_pkg_flow values (99001317,77);
insert into bass1.dim_gprs_pkg_flow values (99001663,85);
insert into bass1.dim_gprs_pkg_flow values (99001676,100);
insert into bass1.dim_gprs_pkg_flow values (90003213,100);
insert into bass1.dim_gprs_pkg_flow values (99001661,100.50);
insert into bass1.dim_gprs_pkg_flow values (99001645,100.50);
insert into bass1.dim_gprs_pkg_flow values (90009133,140);
insert into bass1.dim_gprs_pkg_flow values (99001685,150);
insert into bass1.dim_gprs_pkg_flow values (90001316,150);
insert into bass1.dim_gprs_pkg_flow values (90008110,150);
insert into bass1.dim_gprs_pkg_flow values (99001634,150);
insert into bass1.dim_gprs_pkg_flow values (90009393,150);
insert into bass1.dim_gprs_pkg_flow values (99001137,150);
insert into bass1.dim_gprs_pkg_flow values (90009720,150);
insert into bass1.dim_gprs_pkg_flow values (90009723,150);
insert into bass1.dim_gprs_pkg_flow values (90009615,150);
insert into bass1.dim_gprs_pkg_flow values (99001671,150);
insert into bass1.dim_gprs_pkg_flow values (90009431,150);
insert into bass1.dim_gprs_pkg_flow values (92001002,150);
insert into bass1.dim_gprs_pkg_flow values (90004025,150);
insert into bass1.dim_gprs_pkg_flow values (90003019,154);
insert into bass1.dim_gprs_pkg_flow values (99001677,200);
insert into bass1.dim_gprs_pkg_flow values (90009328,210);
insert into bass1.dim_gprs_pkg_flow values (90009329,210);
insert into bass1.dim_gprs_pkg_flow values (90001317,300);
insert into bass1.dim_gprs_pkg_flow values (90003314,400);
insert into bass1.dim_gprs_pkg_flow values (90003310,400);
insert into bass1.dim_gprs_pkg_flow values (90003303,400);
insert into bass1.dim_gprs_pkg_flow values (90003301,400);
insert into bass1.dim_gprs_pkg_flow values (90003304,400);
insert into bass1.dim_gprs_pkg_flow values (90003302,400);
insert into bass1.dim_gprs_pkg_flow values (90003313,400);
insert into bass1.dim_gprs_pkg_flow values (90003309,400);
insert into bass1.dim_gprs_pkg_flow values (73700001,500);
insert into bass1.dim_gprs_pkg_flow values (90009616,500);
insert into bass1.dim_gprs_pkg_flow values (90009724,500);
insert into bass1.dim_gprs_pkg_flow values (99001672,500);
insert into bass1.dim_gprs_pkg_flow values (99001690,500);
insert into bass1.dim_gprs_pkg_flow values (99001687,500);
insert into bass1.dim_gprs_pkg_flow values (90004014,500);
insert into bass1.dim_gprs_pkg_flow values (99001689,500);
insert into bass1.dim_gprs_pkg_flow values (90004026,500);
insert into bass1.dim_gprs_pkg_flow values (90009437,500);
insert into bass1.dim_gprs_pkg_flow values (90004052,500);
insert into bass1.dim_gprs_pkg_flow values (90009490,500);
insert into bass1.dim_gprs_pkg_flow values (99001686,500);
insert into bass1.dim_gprs_pkg_flow values (90001318,500);
insert into bass1.dim_gprs_pkg_flow values (99001606,501);
insert into bass1.dim_gprs_pkg_flow values (99001662,501);
insert into bass1.dim_gprs_pkg_flow values (99001646,525);
insert into bass1.dim_gprs_pkg_flow values (90004047,600);
insert into bass1.dim_gprs_pkg_flow values (90004017,800);
insert into bass1.dim_gprs_pkg_flow values (90004018,800);
insert into bass1.dim_gprs_pkg_flow values (90009293,800);
insert into bass1.dim_gprs_pkg_flow values (90004020,800);
insert into bass1.dim_gprs_pkg_flow values (90004019,800);
insert into bass1.dim_gprs_pkg_flow values (90009114,980);
insert into bass1.dim_gprs_pkg_flow values (90009617,1024);
insert into bass1.dim_gprs_pkg_flow values (99001673,1024);
insert into bass1.dim_gprs_pkg_flow values (99001692,1024);
insert into bass1.dim_gprs_pkg_flow values (99001691,1024);
insert into bass1.dim_gprs_pkg_flow values (99001664,1026);
insert into bass1.dim_gprs_pkg_flow values (99001605,1026);
insert into bass1.dim_gprs_pkg_flow values (90004053,2048);
insert into bass1.dim_gprs_pkg_flow values (99001693,2048);
insert into bass1.dim_gprs_pkg_flow values (90004016,2048);
insert into bass1.dim_gprs_pkg_flow values (99001138,2048);
insert into bass1.dim_gprs_pkg_flow values (90004027,2048);
insert into bass1.dim_gprs_pkg_flow values (73700002,2048);
insert into bass1.dim_gprs_pkg_flow values (90009438,2048);
insert into bass1.dim_gprs_pkg_flow values (90009491,2048);
insert into bass1.dim_gprs_pkg_flow values (90004048,2248);
insert into bass1.dim_gprs_pkg_flow values (90004049,3372);
insert into bass1.dim_gprs_pkg_flow values (90004028,5120);
insert into bass1.dim_gprs_pkg_flow values (90004054,5120);
insert into bass1.dim_gprs_pkg_flow values (99001139,5120);
insert into bass1.dim_gprs_pkg_flow values (90009439,5120);
insert into bass1.dim_gprs_pkg_flow values (73700003,5120);
insert into bass1.dim_gprs_pkg_flow values (99001517,10000);
insert into bass1.dim_gprs_pkg_flow values (99001518,10000);
insert into bass1.dim_gprs_pkg_flow values (73700004,10240);
insert into bass1.dim_gprs_pkg_flow values (90004050,10240);
insert into bass1.dim_gprs_pkg_flow values (90004055,10240);
insert into bass1.dim_gprs_pkg_flow values (90004007,13800);
insert into bass1.dim_gprs_pkg_flow values (90004001,21150);
insert into bass1.dim_gprs_pkg_flow values (90004010,23200);
insert into bass1.dim_gprs_pkg_flow values (90004006,184000);
insert into bass1.dim_gprs_pkg_flow values (90004008,188416);
insert into bass1.dim_gprs_pkg_flow values (90004002,288768);
insert into bass1.dim_gprs_pkg_flow values (90004009,471040);
insert into bass1.dim_gprs_pkg_flow values (90004003,721920);
  select count(0),count(distinct user_id)
		from bass2.dwd_product_sprom_active_20110301 a,
			 BASS1.dim_gprs_pkg_flow  b
		where a.sprom_id=b.prod_id
		  and replace(char(date(a.valid_date)),'-','')<='20110301'
		  and replace(char(date(a.expire_date)),'-','')>'20110301'
		  
    select * from BASS1.dim_gprs_pkg_flow   runstats on table bass1.dim_gprs_pkg_flow with distribution and detailed indexes all               drop table BASS1.t_gprs_prod_user;
CREATE TABLE BASS1.t_gprs_prod_user
 (
user_id VARCHAR(20),
prod_id integer,
valid_date TIMESTAMP,
expire_date TIMESTAMP
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING;

ALTER TABLE BASS1.t_gprs_prod_user
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
insert into BASS1.t_gprs_prod_user
  select user_id,b.prod_id,a.valid_date,a.expire_date
		from bass2.dwd_product_sprom_active_20110301 a,
			 BASS1.dim_gprs_pkg_flow  b
		where a.sprom_id=b.prod_id
		  and replace(char(date(a.valid_date)),'-','')<='20110301'
		  and replace(char(date(a.expire_date)),'-','')>'20110301'		                select * from BASS1.t_gprs_prod_user  where valid_date is null      insert into  BASS1.t_gprs_prod_user2
select user_id,prod_id,valid_date
from  
(select a.*,row_number()over(PARTITION by user_id order by expire_date desc , valid_date desc  ) rn from 
BASS1.t_gprs_prod_user a
) t where t.rn = 1

  drop table BASS1.t_gprs_prod_user2;
CREATE TABLE BASS1.t_gprs_prod_user2
 (
user_id VARCHAR(20),
prod_id integer,
valid_date TIMESTAMP
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING;

ALTER TABLE BASS1.t_gprs_prod_user2
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
  
drop table BASS1.t_gprs_prod_user;
CREATE TABLE BASS1.t_gprs_prod_user
 (
user_id VARCHAR(20),
prod_id integer,
	flow decimal(12,2),
	valid_date TIMESTAMP,
expire_date TIMESTAMP
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING;

ALTER TABLE BASS1.t_gprs_prod_user
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;      
insert into BASS1.t_gprs_prod_user
  select user_id,b.prod_id,b.flow,a.valid_date,a.expire_date
		from bass2.dwd_product_sprom_active_20110301 a,
			 BASS1.dim_gprs_pkg_flow  b
		where a.sprom_id=b.prod_id
		  and replace(char(date(a.valid_date)),'-','')<='20110301'
		  and replace(char(date(a.expire_date)),'-','')>'20110301'		  

  
  drop table BASS1.t_gprs_prod_user2;
CREATE TABLE BASS1.t_gprs_prod_user2
 (
user_id VARCHAR(20),
prod_id integer,
	flow decimal(12,2),
valid_date TIMESTAMP
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING;

ALTER TABLE BASS1.t_gprs_prod_user2
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
  
insert into  BASS1.t_gprs_prod_user2
select user_id,prod_id,valid_date
from  
(select a.*,row_number()over(PARTITION by user_id order by expire_date|| valid_date desc  ) rn 
BASS1.t_gprs_prod_user a
) t where t.rn = 1

 

insert into  BASS1.t_gprs_prod_user2
select user_id,prod_id,flow,valid_date
from  
(select a.*,row_number()over(PARTITION by user_id order by expire_date desc , valid_date desc   ) rn from 
BASS1.t_gprs_prod_user a
) t where t.rn = 1

  select sum(flow)/1024
from  BASS1.t_gprs_prod_user2
select * from    BASS1.t_gprs_prod_user2  delete from BASS1.t_gprs_sum;  drop table BASS1.t_gprs_sum;
CREATE TABLE BASS1.t_gprs_sum
 (
        UP_FLOWS                DECIMAL(12,2)         
        ,DOWN_FLOWS              DECIMAL(12,2)            
        ,FREE_IS_PKG             DECIMAL(12,2)              
        ,FREE_NOT_PKG             DECIMAL(12,2)              
        ,NOT_FREE_NOT_PKG        DECIMAL(12,2)         
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (UP_FLOWS
   ) USING HASHING;

ALTER TABLE BASS1.t_gprs_sum
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
        select * from  BASS1.t_gprs_sum       select 
                  sum(bigint(data_flow_up1+data_flow_up2))*1.0/1024/1024/1024   as up_flows
                  ,sum(bigint(data_flow_down1+data_flow_down2))*1.0/1024/1024/1024 as down_flows
                  ,sum(case when free_res_val1 is not null and (charge1+charge2+charge3+charge4) = 0 then 
             bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else 0 end     ) free_is_pkg
                  ,sum(case when free_res_val1 is  null and (charge1+charge2+charge3+charge4) = 0 then 
             bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else 0 end     ) free_not_pkg
                  ,sum( case when (charge1+charge2+charge3+charge4) > 0 then 
             bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else  0 end
             ) not_free_not_pkg 
from  bass2.CDR_GPRS_LOCAL_20110322 a
left join BASS1.t_gprs_prod_user2 b on  a.user_id = b.user_id 
where  b.user_id is null select min(flow) from BASS1.t_gprs_prod_user2
 drop table BASS1.t_gprs_sum2;
CREATE TABLE BASS1.t_gprs_sum2
 (
        UP_FLOWS                DECIMAL(12,5)         
        ,DOWN_FLOWS              DECIMAL(12,5)            
        ,FREE_IS_PKG             DECIMAL(12,5)              
        ,FREE_NOT_PKG             DECIMAL(12,5)              
        ,NOT_FREE_NOT_PKG        DECIMAL(12,5)         
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (UP_FLOWS
   ) USING HASHING;

ALTER TABLE BASS1.t_gprs_sum2
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
      select * from t_gprs_sum      select sum(UP_FLOWS+DOWN_FLOWS) all_flow,sum(FREE_IS_PKG) FREE_IS_PKG,sum(FREE_NOT_PKG) FREE_NOT_PKG,sum(NOT_FREE_NOT_PKG) NOT_FREE_NOT_PKG  from t_gprs_sum  select sum(flow)/1024 from BASS1.t_gprs_prod_user2      select flow,count(0)  from  BASS1.t_gprs_prod_user2  group by flow  order by 1  select * from   BASS1.t_gprs_sum2            select sum(UP_FLOWS+DOWN_FLOWS) all_flow,sum(FREE_IS_PKG) FREE_IS_PKG,sum(FREE_NOT_PKG) FREE_NOT_PKG,sum(NOT_FREE_NOT_PKG) NOT_FREE_NOT_PKG  from t_gprs_sum2     select * from  bass2.DW_PRODUCT_BASS1_20110323   dim_up_product_   select * from  bass2.DIM_PROD_UP_PRODUCT_ITEM where name like '%全球通%' and name not like '%存%' and name not like '%积分%' and item_type = 'OFFER_PLAN' and extend_id is not null     select * from  bass2.DIM_PROD_UP_PRODUCT_ITEM where name like '%尊享%' and item_type = 'OFFER_PLAN'      select * from  bass2.DIM_PROD_UP_PRODUCT_ITEM where name like '%凤凰资讯%'     and item_type = 'OFFER_PLAN'SELECT * FROM G_S_04002_DAY WHERE TIME_ID = 20110321 bass1.fn_get_all_dim  select * from syscat.functions where funcname = 'FN_GET_ALL_DIM'select * from BODY
CREATE FUNCTION BASS1.FN_GET_ALL_DIM(GID VARCHAR(20),DID VARCHAR(20)) RETURNS VARCHAR(10) DETERMINISTIC NO EXTERNAL ACTION LANGUAGE SQL BEGIN ATOMIC RETURN SELECT BASS1_VALUE FROM BASS1.ALL_DIM_LKP WHERE BASS1_TBID = GID AND XZBAS_VALUE = DID; END
select * from  bass1.g_s_03004_monthwhere time_id = 201103and ACCT_ITEM_ID in ('0626','0627')dim_acct_item	随e行G3上网卡包月不限流量不封顶	80000508	帐目科目	BASS_STD1_0074	0626	GPRS套餐费
dim_acct_item	数据时长月基本费 	80000512	帐目科目	BASS_STD1_0074	0626	GPRS套餐费
dim_acct_item	超出套餐数据时长费 	80000513	帐目科目	BASS_STD1_0074	0627	GPRS通信费
select * from BASS1.ALL_DIM_LKPwhere BASS1_TBID = 'BASS_STD1_0074'select c.*from (select user_idfrom  bass1.g_s_03004_monthwhere time_id = 201103and ACCT_ITEM_ID in ('0626','0627')) a ,bass2.dw_product_201103 b , bass2.dwd_cust_msg_20110331 c where a.user_id = b.user_id and b.cust_id = c.cust_idt_int_check_user_statusselect * from    bass1.g_s_03004_monthselect count(0)from  bass1.td_check_user_mobile a , t_int_check_user_status c  where a.PRODUCT_NO = c.PRODUCT_NOselect count(0) from   G_S_03005_MONTHwhere time_id = 201103and ITEM_ID in ('0626','0627')select b.*from  bass1.td_check_user_mobile a , t_int_check_user_status c , (select user_idfrom  bass1.g_s_03004_monthwhere time_id = 201103and ACCT_ITEM_ID in ('0626','0627')) b , bass2.dwd_cust_msg_20110331 dwhere a.product_no = c.product_no and c.user_id = b.user_id select * from  bass2.dwd_cust_msg_20110331 fetch first 10 rows only  select count(0) from    t_int_check_user_statusselect b.*from  t_int_check_user_status c , (select user_idfrom  bass1.g_s_03004_monthwhere time_id = 201103and ACCT_ITEM_ID in ('0626','0627')) bwhere c.user_id = b.user_id 
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

--session临时表跑数慢，建实体表
drop table t_int_check_user_status
create table t_int_check_user_status like     session.int_check_user_status
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHINGdrop table t_int_check_user_status
create table t_int_check_user_status like     session.int_check_user_status
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING



--抓取用户资料入表
insert into t_int_check_user_status (
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
where time_id<=20110331 and SIM_CODE = '0') e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where time_id<=20110331 ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1

select count(0) from   bass1.td_check_user_mobile
select count(0)
from 
(
select product_no from t_int_check_user_status where usertype_id NOT IN ('2010','2020','2030','9000')
except 
select product_no from  bass1.td_check_user_mobile
)t
select  b.ITEM_ID, UPPER(item_name) ,count(0)                             from   bass2.dw_acct_shoulditem_201103 a , bass2.dim_acct_item b
                        where a.ITEM_ID = b.item_id                         and  UPPER(item_name) like '%GPRS%' group by    b.ITEM_ID, UPPER(item_name) order by 1,2ITEM_ID	2	3
80000103	优惠GPRS费	30
80000634	随E行-GPRS费	4011
80000663	M2M GPRS功能费	18
80000664	M2M_GPRS功能费(5元30M/月)	18
                      select  b.ITEM_ID, UPPER(item_name) ,count(0)                             from   bass2.dw_acct_shoulditem_201103 a , bass2.dim_acct_item b
                        where a.ITEM_ID = b.item_id                         AND  UPPER(item_name) like '%流量%'group by    b.ITEM_ID, UPPER(item_name) order by 1,2ITEM_ID	2	3
80000078	超出套餐数据流量费	677714
80000104	数据流量月基本费	187057
80000462	G3上网本超出套餐流量费	206
80000531	G3上网卡超出套餐流量费	140
                        
                          a.item_id in (80000508,80000512,80000513) 0626	GPRS套餐费	包含相应数据流量的GPRS月使用费
0627	GPRS通信费	套餐用户超出部分的GPRS通信费，以及非套餐用户的GPRS通信费
                                                    dim_acct_item	随e行G3上网卡包月不限流量不封顶	80000508	帐目科目	BASS_STD1_0074	0626	GPRS套餐费
dim_acct_item	数据时长月基本费 	80000512	帐目科目	BASS_STD1_0074	0626	GPRS套餐费
dim_acct_item	超出套餐数据时长费 	80000513	帐目科目	BASS_STD1_0074	0627	GPRS通信费
select * from   bass2.dim_acct_item where UPPER(item_name) like '%GPRS%'select count(0),count(distinct item_id ) from    bass2.dim_acct_item 
drop view t_gprs_03

create view t_gprs_03
as
select 
                  sum(bigint(data_flow_up1+data_flow_up2))*1.0/1024/1024/1024   as up_flows
                  ,sum(bigint(data_flow_down1+data_flow_down2))*1.0/1024/1024/1024 as down_flows
                  --
                  ,sum(case when free_res_val1 is not null and (charge1+charge2+charge3+charge4) = 0 then 
             bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else 0 end     ) free_is_pkg
                  ,sum(case when free_res_val1 is  null and (charge1+charge2+charge3+charge4) = 0 then 
             bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else 0 end     ) free_not_pkg
             
                  ,sum( case when (charge1+charge2+charge3+charge4) > 0 then 
             bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else  0 end
             ) not_free_not_pkg 
from  bass2.CDR_GPRS_LOCAL_20110423 a,
BASS1.t_gprs_prod_user2 b 
where a.user_id = b.user_id 
 
 UP_FLOWS	DOWN_FLOWS	FREE_IS_PKG	FREE_NOT_PKG
40.15598522219	226.62631536461	128.06665484731	122.77105686100


 40.15598522219	226.62631536461	128.06665484731	122.77105686100
not_free_not_pkg
 15.94457482177
 
 
   select * from  bass1.g_i_02019_month  where OVER_PROD_AREA = '1' and OVER_PROD_NAME like '%阅读%'  select * from  bass1.g_i_02018_month where base_prod_type = '113'    select * from  bass2.dim_prod_up_plan_plan_rel    WITH  tmp1 as ( select 
		    a.product_item_id                         base_prod_id,
		    b.trademark
		from bass2.dim_prod_up_product_item a,
		  bass2.Dim_prod_up_offer b
		where a.item_type = 'OFFER_PLAN' 
		and b.offer_type in ('OFFER_PLAN')
		and a.product_item_id = b.offer_id
		and a.platform_id in (1,2)) t        select count(0),count(distinct PEER_SEQ) from    bass2.DW_ACCT_PAYMENT_DM_201103 select * from   bass2.DW_ACCT_PAYMENT_DM_201103 where remarks like '%SP退费%'select * from   bass2.DW_ACCT_PAYMENT_DM_201103 where upper(remarks) like '%SP%'select replace(char(date(a.OP_TIME)),'-','')  time_id
,count(0) op_time
,sum(amount) back_cnt
from bass2.DW_ACCT_PAYMENT_DM_201103 a
where a.remarks like '%SP退费%'
group by replace(char(date(a.OP_TIME)),'-','')select replace(char(date(a.OP_TIME)),'-','')  time_id
,count(0) back_cnt
,sum(amount) back_fee
from bass2.DW_ACCT_PAYMENT_DM_201103 a
where a.remarks like '%SP退费%'
and replace(char(date(a.OP_TIME)),'-','') = '20110330'
group by replace(char(date(a.OP_TIME)),'-','')select replace(char(date(a.OP_TIME)),'-','')  time_id
,char(count(0)) back_cnt
,char(bigint(sum(amount))) back_fee
from bass2.DW_ACCT_PAYMENT_DM_201103 a
where a.remarks like '%SP退费%'
and replace(char(date(a.OP_TIME)),'-','') = '20110330'
group by replace(char(date(a.OP_TIME)),'-','')
select count(0),count(distinct sp_code ) from   bass2.DIM_NEWBUSI_SPINFOselect count(0),count(distinct serv_code ) from   bass2.DIM_NEWBUSI_SPINFOselect tabname from syscat.tables where tabname like  'VGOP%20110407%' select * from   bass2.VGOP_11201_20110407VGOP_11201_20110407select sum(bigint(free_point)) from   g_i_02006_month where time_id = 201103select case 
     when a.crm_brand_id2=70 then '1' 
     else '0' 
    end    ,count(0) cnt from  bass2.dw_product_bass1_20110401 a,BASS1.t_gprs_prod_user2 b where a.user_id = b.user_id group by case 
     when a.crm_brand_id2=70 then '1' 
     else '0' 
    end              select product_no,b.flowfrom  bass2.dw_product_bass1_20110401 a,BASS1.t_gprs_prod_user2 b where a.user_id = b.user_id and  a.crm_brand_id2=70group by case 
     when a.crm_brand_id2=70 then '1' 
     else '0' 
    end                            db2 runstats on table bass1.t_gprs_prod_user2 with distribution and detailed indexes all    db2 runstats on table  bass2.dw_product_bass1_20110401 with distribution and detailed indexes all    CREATE TABLE BASS1.mon_interface_not_empty
 (
interface_code  char(5)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (interface_code) USING HASHING
   
  ALTER TABLE BASS1.mon_interface_not_empty
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE    insert into BASS1.mon_interface_not_empty values 
 ('01002')
,('02004')
,('02008')
,('02011')
,('02014')
,('02015')
,('02016')
,('02018')
,('02019')
,('02020')
,('02021')
,('02047')
,('02049')
,('02050')
,('02052')
,('02053')
,('02063')
,('03001')
,('03002')
,('03004')
,('03005')
,('03013')
,('04002')
,('04004')
,('04005')
,('04006')
,('04007')
,('04008')
,('04011')
,('05001')
,('05002')
,('05003')
,('06021')
,('06022')
,('06023')
,('06011')
,('06012')
,('06029')
,('06031')
,('06032')
,('21001')
,('21002')
,('21007')
,('21003')
,('21008')
,('21011')
,('21012')
,('21013')
,('21014')
,('21015')
,('21020')
,('22012')
,('22013')
,('22038')
,('22039')
,('22032')
,('22033')
,('22035')
,('22052')
,('22070')
,('22073')
,('22061')
,('22062')
,('22063')
,('22064')
,('22065')
,('22049')
,('22050')
,('22055')
,('22056')
,('22101')
,('22102')
,('22103')
,('22104')
,('22105')
,('22201')
,('22202')
,('22203')
,('22080')
,('22081')
,('22082')
,('22083')
,('22084')
,('22085')
;
    select * from bass1.MON_ALL_INTERFACEwhere interface_code not in (select  interface_code  from     BASS1.mon_interface_not_empty  )