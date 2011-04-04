/**标志位	FLAG	INTEGER		0   运行完成 1   正在运行-1  运行出错 -2  重新运行 
**/--int(replace(char(current date - 1 days),'-',''))select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mbfrom syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspaceand a.tabname = 'G_S_04002_DAY_BAK'G_S_04002_DAY	95829.84 635661807G_S_04002_DAY	49864.68 330794180    rename bass1.G_S_04002_DAY to bass1.G_S_04002_DAY_BAKrename BASS1.G_S_04002_DAY to BASS1.G_S_04002_DAY_BAKselect * fromBASS1.G_S_04002_DAYrename BASS1.G_S_04002_DAY to G_S_04002_DAY_BAK create table BASS1.G_S_04002_DAY like BASS1.G_S_04002_DAY_BAK      DISTRIBUTE BY HASH(TIME_ID,                   PRODUCT_NO,                   ROAM_LOCN,                   ROAM_TYPE_ID,                   APNNI,                   START_TIME)        IN TBS_APP_BASS1 INDEX IN TBS_INDEX 	  insert into BASS1.G_S_04002_DAY 	  select * from  BASS1.G_S_04002_DAY_BAK	  where time_id >= 20101101       	  --RUNSTATS ON table G_S_04002_DAY	with distribution and detailed indexes all  --runstats on table bass1.G_S_04002_DAY with distribution and detailed indexes all            select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mbfrom syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspaceand a.tabname = 'G_S_21003_MONTH'select * from bass1.G_S_04002_DAY_bakselect tabname , card from syscat.tables where tabname in ('G_S_04002_DAY','G_S_04002_DAY_BAK')            AND tabschema = 'BASS1'SELECT COUNT(0) FROM BASS1.G_S_04002_DAY_BAK--告警 alarmselect * from  app.sch_control_alarm where alarmtime >=  timestamp('20110311'||'000000') --and flag = -1and control_code like 'BASS1%'order by alarmtime desc select * from syscat.functions where funcname = 'FN_GET_ALL_DIM'select * from   BASS1.ALL_DIM_LKP select count(0),count(distinct user_id)  from   bass1.g_a_02004_daybass1.fn_get_all_dimBODYCREATE FUNCTION BASS1.FN_GET_ALL_DIM(GID VARCHAR(20),DID VARCHAR(20)) RETURNS VARCHAR(10) DETERMINISTIC NO EXTERNAL ACTION LANGUAGE SQL BEGIN ATOMIC RETURN SELECT BASS1_VALUE FROM BASS1.ALL_DIM_LKP WHERE BASS1_TBID = GID AND XZBAS_VALUE = DID ENDselect * from syscat.tablesselect * from app.sch_control_runlog where control_code = 'TR1_VGOP_D_14303' select * from app.sch_control_runlog  select * from app.sch_control_alarm select min(deal_time) from APP.G_FILE_REPORT where length(deal_time)=14 select substr(deal_time,1,6) , count(0) from app.g_file_report  group by substr(deal_time,1,6)  select tabname from syscat.tables where tabschema = 'BASS1'and tabname like 'G_%'  select * from app.sch_control_map where module = 2    select * from BASS1.G_USER_LST  select * from BASS2.ETL_TASK_LOG where task_id in ( 'I11101')and cycle_id='20110228'select * from APP.G_RUNLOGselect * fromAPP.G_FILE_REPORT_ERRselect * fromAPP.G_REC_REPORT_ERRselect time_id,count(0) cnt from BASS1.G_USER_LSTgroup by time_id select * fromapp.g_file_report where deal_time like '%20110301%'and err_code = '00'select * from  bass1.G_S_22073_DAYselect  * --distinct left(right(filename,16),5),err_code from APP.G_FILE_REPORTwhere filename like '%_201103_%' and err_code='00'select * from app.g_runlog where time_id=20110117and return_flag=1select count(0)select min(deal_time)from app.g_file_reportwhere deal_time > '19990412211032'select  * --distinct left(right(filename,16),5),err_code from APP.G_FILE_REPORTwhere filename like '%_201103_%' and err_code='00'and length(filename)=length('s_13100_201002_03014_01_001.dat')order by deal_time descselect * from bass1.g_rule_check where rule_code = 'R161_15'where time_id = 20110317select  time_id, case when rule_code='R159_1' then '新增客户数'      when rule_code='R159_2' then '客户到达数'      when rule_code='R159_3' then '上网本客户数'      when rule_code='R159_4' then '离网客户数' end, target1, target2, target3from bass1.g_rule_checkwhere rule_code in ('R159_1','R159_2','R159_3','R159_4')  and time_id=20110301  select count(0) frombass1.G_S_21003_TO_DAY  select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mbfrom syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspaceand a.tabname = 'G_S_21003_TO_DAY'select tbsp_id,substr(tbsp_name,1,30) tbsp_name,TBSP_UTILIZATION_PERCENT from SYSIBMADM.TBSP_UTILIZATION order by tbsp_id,dbpartitionnumselect * from syscat.tables where tabname like '%04005%'and tabschema = 'BASS1'G_S_04005_DAYselect * from  BASS1.G_S_04005_DAY select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mbfrom syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspaceand a.tabname = 'G_S_04005_DAY'    rename BASS1.G_S_04005_DAY to G_S_04005_DAY_BAK    create table BASS1.G_S_04005_DAY like BASS1.G_S_04005_DAY_BAK                  DISTRIBUTE BY HASH(TIME_ID,                   PRODUCT_NO,                   SP_CODE,                   OPPOSITE_NO)                      IN TBS_APP_BASS1 INDEX IN TBS_INDEX NOT LOGGED INITIALLY	  insert into BASS1.G_S_04005_DAY 	  select * from  BASS1.G_S_04005_DAY_BAK	  where time_id >= 20101001select tabname , card from syscat.tables where tabname in ('G_S_04005_DAY','G_S_04005_DAY_BAK')            AND tabschema = 'BASS1'select time_id,count(0) cnt from BASS1.G_S_04005_DAYgroup by  time_id                 select tabname , card from syscat.tables where tabname in ('G_S_04005_DAY','G_S_04005_DAY_BAK')            AND tabschema = 'BASS1'select * from BASS1.G_S_04005_DAY                 drop table BASS1.G_S_04005_DAY_BAKselect a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mbfrom syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspaceand a.tabname = 'G_S_21003_MONTH'select * from  bass1.G_S_21003_MONTH select TIME_ID , count(0) --, count(distinct TIME_ID ) from bass1.G_S_21003_MONTH group by  TIME_ID order by 1 SQL0911Nselect * fromg_s_04003_dayselect count(distinct WLAN_ATTESTATION_CODE)from BASS1.G_S_04005_DAYwhere time_id between 20101201 and 20101231 andselect * from bass2.dim_roam_type			select count(0),count(distinct WLAN_ATTESTATION_CODE)from BASS1.G_S_04003_DAYwhere time_id between 20101201 and 20101231 and ROAM_TYPE_ID in ('500','110')select *  from BASS1.G_S_04003_DAYwhere time_id between 20101201 and 20101231 and ROAM_TYPE_ID in ('500','110')select * from app.sch_control_before where control_code like '%21007%'BASS1_EXP_G_I_03007_MONTH	BASS1_G_I_03007_MONTH.tclBASS1_EXP_G_I_03007_MONTH	BASS1_INT_CHECK_F7_MONTH.tclBASS1_EXP_G_I_03007_MONTH	BASS1_INT_CHECK_R028_MONTH.tclBASS1_EXP_G_I_03007_MONTH	BASS1_INT_CHECK_R029_MONTH.tclselect * from app.sch_control_before where control_code = 'BASS1_INT_CHECK_R029_MONTH.tcl'select * from app.sch_control_task where control_code = '%bak%'select * fromAPP.G_UNIT_INFO select * from APP.G_RUNLOG  order by 1 desc select * from bass1.g_bus_check_all_dayselect * from bass1.g_bus_check_bill_monthselect count(distinct HOTSPOT_AREA_ID)from BASS1.G_S_04003_DAYwhere time_id between 20101201 and 20101231 and ROAM_TYPE_ID in ('500','110')select distinct HOTSPOT_AREA_IDfrom BASS1.G_S_04003_DAYwhere time_id between 20101201 and 20101231 and ROAM_TYPE_ID in ('500','110')select * from   bass2.DW_ENTERPRISE_MEMBER_201102 where enterprise_id = '89103000041929'select * from   bass2.DW_ENTERPRISE_MEMBER_HIS_201102 where enterprise_id = '89103000041929'select * from   bass2.DW_ENTERPRISE_MEMBER_MID_20110323 where enterprise_id = '89103000041929'select * from    bass2.DW_ENTERPRISE_SUB_DSwhere ENTERPRISE_ID = '89103000041929'select count(0),count(distinct enterprise_id) from   bass2.DW_ENTERPRISE_MEMBER_201012select count(0),count(distinct enterprise_id) from   bass2.DW_ENTERPRISE_MEMBER_HIS_201012select count(0),count(distinct enterprise_id) from   bass2.DW_ENTERPRISE_MEMBER_MID_201012select * from bass2.CDR_WLAN_20100304select * from  BASS1.G_S_04003_DAYselect count(distinct HOTSPOT_AREA_ID)from BASS1.G_S_04003_DAYwhere --time_id between 20101201 and 20101231 --and ROAM_TYPE_ID in ('500','110')select tabname,card  from syscat.tables where tabname like '%WLAN%'SELECT * FROM app.sch_control_task where function_desc like '%WLAN%'select * from G_I_02016_MONTHselect * from bass2.Dw_newbusi_wlan_20110302select distinct HOTSPOT_AREA_IDselect *from BASS1.G_S_04003_DAYwhere ROAM_TYPE_ID in ('500','110')select min(time_id) from BASS1.G_S_04003_DAYselect * from BASS1.G_S_04003_DAYwhere product_no not like '1%'select  count(product_no),count(distinct product_no),count(distinct enterprise_id),count(distinct product_no||enterprise_id)from  bass2.DW_ENTERPRISE_MEMBER_MID_201012select sum(int(flowup)+int(flowdown))/1024 from (select a.product_no,a.HOTSPOT_AREA_ID,b.enterprise_id,c.ENTERPRISE_NAME,a.flowup,a.flowdown,a.call_fee,a.info_fee,a.CALL_DURATIONfrom (select * from BASS1.G_S_04003_DAY         where time_id between 20101201 and 20101231         and ROAM_TYPE_ID in ('500','110')	) aleft join  (select distinct enterprise_id,product_no             from bass2.DW_ENTERPRISE_MEMBER_MID_201012            ) b             on a.product_no = b.product_noleft join   bass2.dw_enterprise_msg_201012 c             on b.enterprise_id = c.enterprise_id) t where enterprise_id is not nullselect max(time_id) from g_s_05001_monthselect * from g_s_05001_monthselect time_id,count(0) from g_s_05002_monthgroup by time_idselect tabname from syscat.tables where tabname like '%05002%'select time_id,count(0)from G_S_05001_MONTHgroup by time_id select * from app.sch_control_task where control_code = 'BASS1_EXP_G_S_03005_MONTH'select count(0) from G_A_02052_MONTH where time_id = 201101select * from APP.SCH_CONTROL_ALARM  where flag in(1,-1) and control_code like 'BASS1_INT_CHECK_INDEX_WAVE_DAY.tcl%'and date(alarmtime) = '2011-03-05'CONTENTR161_1 波动性检查新增客户数超出15%R161_7 波动性检查当月累计使用TD网络的手机客户数超出5%R161_9 波动性检查当月累计使用TD网络的数据卡客户数超出5%R161_10 波动性检查当月累计使用TD网络的上网本客户数超出5%R161_13 波动性检查联通移动新增客户数超出8%R161_14 波动性检查电信移动新增客户数超出8%select * from bass1.g_rule_check where rule_code in ('R161_1') order by time_id descselect * from g_file_reportselect * from app.g_runlog select time_id,count(0) from bass1.g_user_lstgroup by time_id select * from syscat.tables where tabname like '%22012%'select * from G_S_22012_DAYselect count(0) from APP.SMS_SEND_INFOselect * from    bass1.T_GS05001M select * from      bass1.T_GS05001M  select * from bass1.T_GS05001M select * from   bass1.g_s_05001_month where time_id = 201012exceptselect * from bass1.T_GS05001MCREATE TABLE "BASS1   "."T_GS05002M"  (                  "TIME_ID" INTEGER ,                   "BILL_MONTH" CHAR(6) ,                   "SELF_CMCC_CODE" CHAR(5) ,                   "SELF_SVC_BRND_ID" CHAR(1) ,                   "OTHER_CMCC_CODE" CHAR(6) ,                   "OTHER_SVC_BRND_ID" CHAR(6) ,                   "IN_COUNT" CHAR(12) ,                   "OUT_COUNT" CHAR(12) ,                   "STLMNT_FEE" CHAR(12) ,                   "PAY_STLMNT_FEE" CHAR(12) )                    DISTRIBUTE BY HASH("TIME_ID")                      IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  select * from   g_s_05002_month where time_id = 201012exceptselect * from T_GS05002Mselect count(0) from app.sch_control_alarmwhere control_code like 'BASS1%'select * from app.sch_control_task where upper(cmd_line) like '%BASS1%SH%'SCH_CONTROL_TASKselect CONTROL_CODE,'bass1_lst.sh',1,'${ALARM_CONTENT}',current timestamp,-1 from app.SCH_CONTROL_TASK where upper(replace(CMD_LINE,' ',''))=upper('bass1_lst.sh') select * from app.sch_control_groupinfo  dmk	BI-M01-01-MARKDB	2011-03-08 16:53:42.240912olap_load	BI-M02-01-OLAP	2011-03-08 16:53:41.055375app	BI-M02-01-OLAP	2011-03-08 16:53:42.407528select mobile_num,count(0) from app.sms_send_info group by mobile_num select * from SYS_TASK_RUNLOGselect * from syscat.tables where tabname like '%RUNLOG%'select * from SYS_TASK_RUNLOGselect a.custstatus_id,coalesce(bass1.fn_get_all_dim('BASS_STD1_0028',char(a.custstatus_id)),'20') as custstatus_id from bass2.dwd_cust_msg_20110310 aselect * from select * from BASS1.ALL_DIM_LKP select time_id,count(0)from G_A_02004_DAYwhere time_id >= 20110101group by time_id order by 1 desc select * from G_A_02004_DAYselect count(distinct user_id) from G_A_02004_DAY with ur-- =2088474select count(distinct user_id) from G_A_02008_DAY with ur2088474select * from   product_xhxselect * from select * from bass1.G_REPORT_CHECKSELECT sum(bigint(BILLING_CAT_OF_WAP_INFO))                  FROM BASS1.G_S_04006_DAY                 WHERE TIME_ID/100=2011021118057                 SELECT sum(bigint(INFO_FEE))                  FROM BASS1.G_S_04006_DAY                 WHERE TIME_ID/100=2011025621188select * from   app.sch_control_task where control_code like '%CHECK%MONTH%'select * from   BASS1.G_S_04006_DAYselect * from    bass2.tmp_zcgselect * from app.SYS_TASK_RUNLOGselect * from bass2.dw_product_ins_off_ins_prod_201102where product_instance_id='89157333132742'  and offer_id=111099001926                select * from   bass1.g_s_22012_day                SELECT * FROM BASS2.ETL_TASK_LOG                select * from   app.sch_control_task where control_code like '%00000%'                select * from   bass1.G_BUS_CHECK_BILL_MONTHselect * from   app.sch_control_taskselect * from   app.sch_control_task where function_desc like '%作废%'and control_code like 'BASS1%'select * from   app.sch_control_task where cmd_type = 1select cmd_type , count(0) , count(distinct cmd_type ) from app.sch_control_task group by  cmd_type order by 1 select * from   APP.SCH_PERSON_PHONEselect userid , count(0) , count(distinct userid ) from APP.SCH_PERSON_PHONE group by  userid order by 1 select * from   app.sch_control_groupinfoselect * from   app.sch_control_mogrpinfoBASS2.ETL_SEND_MESSAGEselect * from BASS2.ETL_SEND_MESSAGE where phone_id = '13989094821'select       c.MO_GROUP_DESC,--模块名count( case when date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 then a.control_code end ) not_run_cnt,--未完成数count( case when   b.flag=1  then a.control_code end ) running_cnt,--执行数count( case when   b.flag=-1 then a.control_code end ) run_err_cnt, --执行出错count( case when date(current timestamp)=date(b.begintime)    and  b.flag=0 then a.control_code end ) done_cnt	--完成数from APP.SCH_CONTROL_TASK a,      APP.SCH_CONTROL_RUNLOG  b,      APP.SCH_CONTROL_MOGRPINFO  cwhere  a.CONTROL_CODE=b.CONTROL_CODE     and a.MO_GROUP_CODE = c.MO_GROUP_CODE    and a.deal_time in (2)group by c.MO_GROUP_DESC,c.sort_idorder by c.sort_idwith ur	select deal_time , count(0) , count(distinct deal_time ) from app.sch_control_task group by  deal_time order by 1 select deal_time , count(0) --,  count(distinct deal_time ) from app.sch_control_task group by  deal_time order by 1 select * from   app.sch_control_task  where deal_time = 3select a.*,b.*from APP.SCH_CONTROL_TASK a,      APP.SCH_CONTROL_RUNLOG  b,      APP.SCH_CONTROL_MOGRPINFO  cwhere  a.CONTROL_CODE=b.CONTROL_CODE     and a.MO_GROUP_CODE = c.MO_GROUP_CODE    and a.deal_time in (2)   and (date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2)   and c.MO_GROUP_DESC = '一经程序'order by c.sort_idwith ur	select       a.MO_GROUP_CODE,c.MO_GROUP_DESC,a.CONTROL_CODE,a.CMD_LINE,a.FUNCTION_DESC,case  when date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 then '未完成'       when   b.flag=1   then '执行中'      when   b.flag=-1  then '执行出错'      when   date(current timestamp)=date(b.begintime) and  b.flag=0 then '完成'      else  '未知'end,b.*from  APP.SCH_CONTROL_TASK a,      APP.SCH_CONTROL_RUNLOG  b,      APP.SCH_CONTROL_MOGRPINFO  cwhere  a.CONTROL_CODE=b.CONTROL_CODE     and a.MO_GROUP_CODE = c.MO_GROUP_CODE    and a.deal_time in (1)   and a.MO_GROUP_CODE = 'BASS1'order by c.sort_idwith urselect count(0),count(distinct ENTERPRISE_ID) select t2.*from (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from G_A_02055_DAY where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340') t ) t2 where rn = 1 select count(0)from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from g_a_02059_day awhere TIME_ID < 20110301and MANAGE_MODE = '2'and STATUS_ID ='1'and ENTERPRISE_BUSI_TYPE ='1340'and length(trim(user_id)) = 14) t) t2where rn = 1 select * from g_a_02059_day awhere TIME_ID < 20110301and MANAGE_MODE = '2'and STATUS_ID ='1'and ENTERPRISE_BUSI_TYPE ='1340'select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'select t2.*from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'and length(trim(user_id)) = 14) t) t2where rn = 1 select * from g_a_02054_dayselect count(0),count(distinct ENTERPRISE_ID) select count(0)from (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220') t ) t2 where rn = 1 select * from   g_a_02059_dayselect tabname from   syscat.tables where tabname like '%ENTERPRISE_MEMBER_MID%'select '20110313',a.ENTERPRISE_ID,a.USER_ID,'1340','2','20110313','1' from   bass2.DW_ENTERPRISE_MEMBER_MID_20110313select * from   bass2.DW_ENTERPRISE_msg_20110313where ENTERPRISE_ID in (select distinct ENTERPRISE_IDfrom (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from G_A_02055_DAY where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340') t ) t2 where rn = 1 )select * from   bass2.DW_ENTERPRISE_msg_20110313where ENTERPRISE_ID in (select distinct ENTERPRISE_IDfrom (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220') t ) t2 where rn = 1 select * from   bass2.DW_ENTERPRISE_msg_20110313where ENTERPRISE_ID in (select  distinct ENTERPRISE_IDfrom (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220') t ) t2 where rn = 1 )select * from   app.sch_control_task where control_code like '%02031%'--1040	无线商务电话	select * from    g_a_02059_day  where ENTERPRISE_BUSI_TYPE = '1040'--02054	集团客户业务订购关系select * from   g_a_02054_day where  ENTERPRISE_BUSI_TYPE = '1040'select * from  bass2.dw_enterprise_msg_201102 where enterprise_id = '89100000000705'select * from   select * from bass1.ALL_DIM_LKP_160 c  where bass1_tbid='BASS_STD1_0108'and C.BASS1_VALUE  in ('1230','1241','1249','1220','1320','1040')select * from bass2.dim_enterprise_productwhere service_name like  '%话%'select * from app.sch_control_beforewhere control_code = 'BASS1_G_A_02059_DAY.tcl'select * from app.sch_control_beforewhere control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl'select * from app.sch_control_beforewhere control_code in (select BEFORE_CONTROL_CODE from app.sch_control_beforewhere control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl')select * from app.sch_control_beforewhere control_codein (select BEFORE_CONTROL_CODE from app.sch_control_beforewhere control_code in (select BEFORE_CONTROL_CODE from app.sch_control_beforewhere control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl'))select * from app.sch_control_beforewhere control_codein(select BEFORE_CONTROL_CODE from app.sch_control_beforewhere control_codein (select BEFORE_CONTROL_CODE from app.sch_control_beforewhere control_code in (select BEFORE_CONTROL_CODE from app.sch_control_beforewhere control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl')))select * from app.sch_control_beforewhere control_code like '%enterprise%'select * from app.sch_control_beforewhere before_control_code like '%BASS2_Dw_product_ds.tcl%'BASS1_G_A_02059_DAY.tcl	BASS2_Dw_enterprise_extsub_rela_ds.tclselect * from app.sch_control_taskwhere control_code like '%w_product%'insert into app.sch_control_beforevalues('BASS1_G_A_02059_DAY.tcl','BASS2_Dw_product_regsp_ds.tcl')insert into app.sch_control_beforevalues('BASS1_G_A_02059_DAY.tcl','BASS1_G_A_02054_DAY.tcl')insert into app.sch_control_beforevalues('BASS1_G_A_02059_DAY.tcl','BASS2_Dw_product_ds.tcl')insert into app.sch_control_beforevalues('BASS1_G_A_02059_DAY.tcl','BASS2_Dw_enterprise_member_mid_ds.tcl')insert into app.sch_control_beforevalues('BASS1_G_A_02059_DAY.tcl','BASS2_Dw_enterprise_msg_ds.tcl')commitselect * from   app.sch_control_task where upper(control_code) like '%02054%'BASS2_Dw_enterprise_member_mid_ds.tcldw_enterprise_member_mid_yyyymmddBEFORE_CONTROL_CODEBASS2_Dw_enterprise_sub_ds.tclBASS2_Dw_enterprise_membersub_ds.tclBASS2_Dw_product_ds.tclselect * from app.sch_control_beforewhere control_code = 'BASS2_Dw_enterprise_extsub_rela_ds.tcl'select * from   app.sch_control_task where upper(control_code) like '%DW_PRODUCT_REGSP_DS%'DW_PRODUCT_REGSP_DSBASS1_G_A_02059_DAY.tcl	BASS2_Dw_enterprise_extsub_rela_ds.tclselect * from G_A_02059_DAY_0315modifyexceptselect * from   G_A_02059_DAYselect '20110314'											 ,a.enterprise_id											 ,b.user_id											 ,'1040'											 ,'3'											 ,'20110314'											 ,a.status_id								from 	bass1.g_a_02054_day a,								      bass2.dw_enterprise_member_mid_20110314 b,								      bass2.dw_enterprise_msg_20110314 c,								      bass2.dw_product_20110314 d								where a.enterprise_id=b.enterprise_id								  and a.enterprise_id=c.enterprise_id								  and b.user_id=d.user_id								  and d.usertype_id in (1,2,3,6)								  and a.enterprise_busi_type='1040'								  and a.time_id=20110314								  and a.status_id='1'select 20110314											 ,a.enterprise_id											 ,b.user_id											 ,'1040'											 ,'3'											 ,'20110314'											 ,a.status_id								from 	bass1.g_a_02054_day a,								      (select distinct enterprise_id,user_id 								        from BASS1.G_A_02059_DAY 								       where enterprise_busi_type='1040' 								         and time_id<20110314) b								where a.enterprise_id=b.enterprise_id								  and a.time_id=20110314								  and a.enterprise_busi_type='1040'								  and a.status_id='2'                                  select A.*,char(a.RUNTIME/60)||'min',char(a.RUNTIME/60/60)||'hr' from   app.sch_control_runlog Awhere control_code like 'BASS1%'and a.RUNTIME/60 > 10ORDER BY RUNTIME DESC select * from   g_s_02059_day                                                                    select * from g_a_02059_daywhere time_id = 20110315and ENTERPRISE_BUSI_TYPE = '1040'select * from   g_a_01004_dayselect * from   BASS2.DW_ENTERPRISE_ACCOUNT_20110315select * from   bass2.dwd_cust_msg_20110315select a.CUSTTYPE_ID, a.* from   bass2.dwd_cust_msg_20110315 aselect * from   app.sch_control_runlog where control_code like '%01004%'                                  select count(0)from ( select distinct a.enterprise_id from   (select time_id,enterprise_id,cust_statu_typ_id from bass1.G_A_01004_DAY where time_id <= 20110301 ) a,   (select enterprise_id,max(time_id) as time_id  from bass1.G_A_01004_DAY                                                  where time_id<=20110301                                              group by enterprise_id) bwhere a.time_id=b.time_id and a.enterprise_id=b.enterprise_id and a.cust_statu_typ_id = '20') t10:04 9075select count(0)from (select distinct t.enterprise_idfrom (select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn from bass1.G_A_01004_DAY where time_id <= 20110301 ) t where t.rn = 1 and  cust_statu_typ_id = '20') tt 10537select enterprise_id,max(time_id) as time_id  from bass1.G_A_01004_DAY                                                  where time_id<=20110301                                              group by enterprise_id                                              select  from   G_A_02004_DAYselect count(0) from   G_A_02062_DAY where char(time_id) like '201102%'select count(0),count(distinct user_id) from   G_A_02062_DAY where time_id < 20110301 and time_id >= 2011020102004select time_id,count(0) from    G_A_02051_DAYgroup by time_idorder by 1 desc select count(0),count(distinct user_id ) from    G_A_02004_DAYselect a.brand_id, count(0),count(distinct a.user_id) from  (select user_id,BRAND_ID from G_A_02004_DAY  where SIM_CODE = '1' and USERTYPE_ID = '1') a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where a.user_id = b.user_id group by a.brand_id2	51902	6438	4663(select user_id,BRAND_ID ,row_number()over(partition by )from G_A_02004_DAY  where SIM_CODE = '1' and USERTYPE_ID = '1') a 46226438time_id <20110301select * from   bass1.G_A_02062_DAYchar(time_id) like '201102%'select SIM_CODE ,BRAND_ID, count(0) ,  count(distinct USER_ID ) from bass1.G_A_02004_DAY group by  SIM_CODE ,BRAND_IDorder by 1 declare global temporary table session.int_check_user_status    (   user_id        CHARACTER(15),   product_no     CHARACTER(15),   test_flag      CHARACTER(1),   sim_code       CHARACTER(15),   usertype_id    CHARACTER(4),   create_date    CHARACTER(15),   brand_id       CHARACTER(4),   time_id        int    )                            partitioning key            (   user_id     ) using hashing           with replace on commit preserve rows not logged in tbs_user_temp--抓取用户资料入表insert into session.int_check_user_status (     user_id        ,product_no     ,test_flag      ,sim_code       ,usertype_id      ,create_date    ,brand_id    ,time_id )select e.user_id    ,e.product_no      ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag    ,e.sim_code    ,f.usertype_id      ,e.create_date      ,e.brand_id    ,f.time_id       from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id                ,row_number() over(partition by user_id order by time_id desc ) row_id   from bass1.g_a_02004_daywhere time_id<=20110324 ) einner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id              from bass1.g_a_02008_day           where time_id<=20110324 ) f on f.user_id=e.user_idwhere e.row_id=1 and f.row_id=1commitselect a.sim_code,a.brand_id, count(0),count(distinct a.user_id) from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0'group by a.sim_code,a.brand_idorder by 1,246222   	4622	4622西藏	7610	2658	193	7	2858		4622		4622	7480	130select a.sim_code,a.brand_id, count(0),count(distinct a.user_id) from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0'group by a.sim_code,a.brand_idorder by 1,2select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1'  select count(0),count(distinct user_id ) from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1'  7610  select distinct user_id from  G_A_02062_DAY  where time_id <20110301 and STATUS_ID = '1' except select distinct a.user_id from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0'  select * from  G_A_02062_DAY where user_id = '89460000193920' select * from  session.int_check_user_status where user_id = '89460000193920'   select * from  session.int_check_user_status where user_id  in (select distinct user_id from  G_A_02062_DAY  where time_id <20110301 and STATUS_ID = '1' except select distinct a.user_id from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0')   select * from  G_A_02062_DAY where user_id = '89657334067011' select OP_TIME,count(0) from   bass2.DW_ENTERPRISE_SUB_ds group by  OP_TIME  select count(0) from  bass2.DW_ENTERPRISE_SUB_ds bass2.DW_ENTERPRISE_SUB_201011 select * from  bass2.DW_ENTERPRISE_SUB_201011where user_id = '89657334067011' select count(0) from  bass2.DW_ENTERPRISE_SUB_201102 select * from   bass2.dw_product_20110314where user_id = '89657334067011'1363896839589657334067011	89603001340010	89601002853991	100	6	0	0	1	0	0	[NULL]	13638968395	460008961120700	[NULL]	[NULL]	896	896	1059	10590005	96011006	4	[NULL]	300001911090	0	279	10	1	1	10	101	89610012	5	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	2010-02-24 	2010-05-25 	2010-11-29 	2010-11-29 	0	0	0	0	0.00	0.00	0.00	0.00	0	0	0	0	0.00	0.00	0.00	0.00	0.00	0.00 89657334067011 	13638968395    	0	0              	2020	20100224       	1   	2010112989657334006049 	13638960960    	0	0              	2020	20100118       	1   	20101130 select * from  session.int_check_user_status where user_id  in   ('89157333603327','89257332979496','89257332980422','89457332326899','89460000193947','89560000363718','89560000363941','89657334006210','89157334021842','89257332979460','89257332980702','89257333374671','89457333600524','89460000193798','89460000339034','89560000363916','89560000363965','89657333802412','89157332844445','89257332979510','89360000266593','89457332319392','89457333729869','89460000193999','89560000363735','89560000363905','89657333043867','89657333685357','89657333695270','89157332768972','89157333603386','89257332980520','89257332980594','89360000266440','89360000266468','89457333386157','89460000193789','89460000270137','89560000363897','89657333689064','89157333346941','89157333942045','89257332979480','89360000266538','89360000266610','89360000266686','89460000193802','89560000363758','89657333802374','89657334006049','89657334067011','89360000266456','89360000266532','89457332240376','89457333035055','89460000193749','89657333688890','89701170013122','89457333038292','89460000193714','89560000363747','89560000363894','89657333685472','89157332834451','89157333603404','89360000266628','89460000193679','89460000193777','89460000193932','89460000221143','89657333695116','89157332768979','89157332993008','89157332994577','89357333334596','89457332341379','89460000193825','89657333693001','89157334002261','89157334021848','89360000266437','89360000266524','89457332341392','89457333038299','89460000194023','89560000363859','89657333043881','89657334037819','89157332826412','89360000266462','89360000266559','89460000193616','89460000193810','89657333562860','89757334249044','89157332993069','89157333346957','89157333942188','89460000193858','89657333696722','89657333832637','89157332895961','89157333603323','89157333942125','89360000266273','89457333338562','89560000363911','89657333295749','89657333736478','89157333346922','89157333872113','89360000266286','89360000266674','89360000266703','89460000193774','89460000193920','89460000193937','89460000193954','89560000363769','89560000363789','89657334006083','89757334252690','89157333942117','89157334021855','89357333334573','89360000266167','89360000266306','89360000266516','89560000363889','89657333800018')order by 3 select count(0) from  bass2.DW_ENTERPRISE_SUB_dsdeclare global temporary table session.int_check_user_status    (   user_id        CHARACTER(15),   product_no     CHARACTER(15),   test_flag      CHARACTER(1),   sim_code       CHARACTER(15),   usertype_id    CHARACTER(4),   create_date    CHARACTER(15),   brand_id       CHARACTER(4),   time_id        int    )                            partitioning key            (   user_id     ) using hashing           with replace on commit preserve rows not logged in tbs_user_temp--抓取用户资料入表insert into session.int_check_user_status (     user_id        ,product_no     ,test_flag      ,sim_code       ,usertype_id      ,create_date    ,brand_id    ,time_id )select e.user_id    ,e.product_no      ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag    ,e.sim_code    ,f.usertype_id      ,e.create_date      ,e.brand_id    ,f.time_id       from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id                ,row_number() over(partition by user_id order by time_id desc ) row_id   from bass1.g_a_02004_daywhere time_id<=20110231 ) einner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id              from bass1.g_a_02008_day           where time_id<=20110231 ) f on f.user_id=e.user_idwhere e.row_id=1 and f.row_id=1commitselect a.sim_code,a.brand_id, count(distinct a.user_id) user_cnt from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0'group by a.sim_code,a.brand_idorder by 1,2select case when a.sim_code = '1' then '数据SIM卡用户' else '非数据SIM卡用户' end ,a.brand_id, count(distinct a.user_id) user_cnt from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0'group by case when a.sim_code = '1' then '数据SIM卡用户' else '非数据SIM卡用户' end ,a.brand_idorder by 1,2 select distinct user_id from  G_A_02062_DAY  where time_id <20110301 and STATUS_ID = '1' except select distinct a.user_id from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and usertype_id NOT IN ('2010','2020','2030','9000')        and test_flag='0'select count(0)from (        select distinct user_id from  G_A_02062_DAY  where time_id <20110301 and STATUS_ID = '1' except select distinct a.user_id from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and a.usertype_id NOT IN ('2010','2020','2030','9000')        and a.test_flag='0'     ) t         select test_flag,usertype_id,a.user_id from  session.int_check_user_status a where user_id  in (select distinct user_id from  G_A_02062_DAY  where time_id <20110301 and STATUS_ID = '1' except select distinct a.user_id from  (select * from session.int_check_user_status   ) a ,(select distinct user_id from   G_A_02062_DAY where time_id <20110301 and STATUS_ID = '1') b where   a.user_id = b.user_id         and a.usertype_id NOT IN ('2010','2020','2030','9000')        and a.test_flag='0'  ) order by 1,2结果:TEST_FLAG	USERTYPE_ID	30	2010	200	2020	331	1010	671	2020	10select * from bass2.dw_enterprise_membersub_ds awhere a.USER_ID in ('89157333942045','89157333603327','89257332980422','89157332993008','89157333942117','89257332980702','89460000339034','89257333374671','89157332993069','89157333942188','89157332895961','89157333942125','89157333603323','89157334002261','89157333872113','89157333603386','89257332980594','89257332980520','89157332834451','89157333603404','89460000221143','89657333695116','89157332768972','89657333689064','89460000270137','89457333386157','89657334006083','89757334252690','89657333688890','89657334037819','89457333338562','89657333736478','89657333295749','89657333696722','89657333832637','89157332826412','89757334249044','89657333562860','89457333600524','89657333802412','89657333800018','89157332768979','89657333693001','89157332994577','89157332844445','89657333695270','89657333685357','89457333729869','89657334006210','89657333685472','89657333802374','89657334067011','89657334006049')select *from  ( select a.*,row_number()over(partition by user_id order by time_id desc) rn  from  BASS1.G_A_02062_DAY a where a.USER_ID in ('89157333942045','89157333603327','89257332980422','89157332993008','89157333942117','89257332980702','89460000339034','89257333374671','89157332993069','89157333942188','89157332895961','89157333942125','89157333603323','89157334002261','89157333872113','89157333603386','89257332980594','89257332980520','89157332834451','89157333603404','89460000221143','89657333695116','89157332768972','89657333689064','89460000270137','89457333386157','89657334006083','89757334252690','89657333688890','89657334037819','89457333338562','89657333736478','89657333295749','89657333696722','89657333832637','89157332826412','89757334249044','89657333562860','89457333600524','89657333802412','89657333800018','89157332768979','89657333693001','89157332994577','89157332844445','89657333695270','89657333685357','89457333729869','89657334006210','89657333685472','89657333802374','89657334067011','89657334006049')) t where rn = 1 20110212	89401560000169      	89460000270137      	1249	3	13908940254    	4 	1	cmm.hn                                                      	20110212	1	1select * from   g_a_02008_daywhere user_id = '89460000270137'20110226	89460000270137      	202020101123	89460000270137      	102220110212	89401560000169      	89460000270137      	1249	3	13908940254    	4 	1	cmm.hn                                                      	20110212	1	12011012789157333942188      select * from   g_a_02008_daywhere user_id = '89157333942188'20110201	89157333942188      	2010201102122011012720110127201101272011012720101231select * from    bass2.dwd_enterprise_sub_20101231 awhere a.SUB_ID in ('89157333942045','89157333603327','89257332980422','89157332993008','89157333942117','89257332980702','89460000339034','89257333374671','89157332993069','89157333942188','89157332895961','89157333942125','89157333603323','89157334002261','89157333872113','89157333603386','89257332980594','89257332980520','89157332834451','89157333603404','89460000221143','89657333695116','89157332768972','89657333689064','89460000270137','89457333386157','89657334006083','89757334252690','89657333688890','89657334037819','89457333338562','89657333736478','89657333295749','89657333696722','89657333832637','89157332826412','89757334249044','89657333562860','89457333600524','89657333802412','89657333800018','89157332768979','89657333693001','89157332994577','89157332844445','89657333695270','89657333685357','89457333729869','89657334006210','89657333685472','89657333802374','89657334067011','89657334006049')bass2.ods_product_ins_prod_20110315select count(0)from (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220') t ) t2 where rn = 1 select count(0)from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'and length(trim(user_id)) = 14) t) t2where rn = 1 select * from   bass2.dw_enterprise_msg_his_20110316where enterprise_id in ('89108911013886'      ,'89100000000645'     )select         TIME_ID        ,ENTERPRISE_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,ORDER_DATE        ,2 STATUS_IDfrom          (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1220') t ) t2 where rn = 1 select *from g_a_02054_day where STATUS_ID ='1' and MANAGE_MODE = '2'and time_id >=20110301 and ENTERPRISE_BUSI_TYPE = '1220'select count(0) from G_A_02054_DAYselect * from BASS1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108'select count(0)from (select t.*,row_number()over(partition by user_id order by time_id ) rn from (select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'and length(trim(user_id)) = 14) t) t2where rn = 1 select * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'CREATE TABLE BASS1.G_A_02054_DAY_20110317BAK (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (ENTERPRISE_ID,    TIME_ID   ) USING HASHINGinsert into BASS1.G_A_02054_DAY_20110317BAKselect * from G_A_02054_DAYselect count(0) from    G_A_02054_DAY_20110317BAK54697select count(0) from    G_A_02054_DAYCREATE TABLE BASS1.G_A_02054_DAY_0317_1220repair (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (ENTERPRISE_ID,    TIME_ID   ) USING HASHING   ALTER TABLE BASS1.G_A_02054_DAY_0317_1220repair  LOCKSIZE ROW  APPEND OFF  NOT VOLATILE  insert into BASS1.G_A_02054_DAY_0317_1220repairselect         20110317        ,ENTERPRISE_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,ORDER_DATE        ,'2' STATUS_IDfrom g_a_02054_day where STATUS_ID ='1' 			and MANAGE_MODE = '2'			and time_id <20110301 			and ENTERPRISE_BUSI_TYPE = '1220'            CREATE TABLE BASS1.G_A_02061_DAY_20110317BAK (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  USER_ID               CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  EXPIRE_DATE           CHARACTER(8),  PAY_TYPE              CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (USER_ID,    TIME_ID   ) USING HASHINGALTER TABLE BASS1.G_A_02061_DAY_20110317BAK  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEinsert into   bass1.G_A_02061_DAY_20110317BAKselect * from  bass1.G_A_02061_DAYselect count(0) from     bass1.G_A_02061_DAYselect count(0) from    bass1.G_A_02061_DAY_20110317BAKCREATE TABLE BASS1.G_A_02061_DAY_0317repair (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  USER_ID               CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  EXPIRE_DATE           CHARACTER(8),  PAY_TYPE              CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (USER_ID,    TIME_ID   ) USING HASHINGALTER TABLE BASS1.G_A_02061_DAY_0317repair  LOCKSIZE ROW  APPEND OFF  NOT VOLATILE  insert into   bass1.G_A_02061_DAY_0317repairselect * from G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'delete from   bass1.G_A_02061_DAY_0317repairinsert into   bass1.G_A_02061_DAY_0317repairselect          20110317        ,ENTERPRISE_ID        ,USER_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,'20110317' EXPIRE_DATE        ,PAY_TYPE        ,ORDER_DATE        ,'2' STATUS_IDfrom G_A_02061_DAYwhere ENTERPRISE_BUSI_TYPE = '1220'and  MANAGE_MODE = '2'and STATUS_ID ='1'select * from    bass1.G_A_02061_DAY_0317repair	select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=20110316    571304    
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
                     and day_new_mark = 1 and test_mark<>1                     and a.cust_id = b.cust_idselect count(0) from session.int_check_user_status where create_date = '20110324'and test_flag = '0'                     4231 3月23日	 3月24日2880	4239select * from   g_s_22012_day20110324	20110324	`4231      	1636785     	23021010    	412022      	3873747     	3424      	335706      20110323	20110323	`2878      	1635972     	22901457    	389311      	3975018     	120       	329440      select b.cust_name,a.CRM_BRAND_ID1, count(0)                    from bass2.dw_product_20110323 a ,bass2.dwd_cust_msg_20110323 b                    where usertype_id in (1,2,9)                      and day_new_mark = 1 and test_mark<>1                     and a.cust_id = b.cust_idand  b.CHANNEL_ID in(91000047,91000048,91000049,91000046,91000050,91000035,97000019,91000012,97000023)group by    b.cust_name, a.CRM_BRAND_ID1order by 3 desc                      select b.cust_name,b.CHANNEL_ID, count(0)                    from bass2.dw_product_20110322 a ,bass2.dwd_cust_msg_20110322 b                    where usertype_id in (1,2,9)                      and day_new_mark = 1 and test_mark<>1                     and a.cust_id = b.cust_idgroup by    b.cust_name, b.CHANNEL_IDorder by 3 desc 拉萨市东城区代理店	91000046	126拉萨市南城区代理店	91000049	114拉萨市西城区代理店	91000047	113拉萨市开发区代理店	91000048	78拉萨市北城区代理店	91000050	77拉萨市邮政局	91000012	38拉萨万利文体商贸有限公司	91000035	11巴桑	97000019	1王玉萍	97000023	1索南顿珠	97000023	1普布次仁	97000019	1欧珠	97000019	1洛桑顿珠	97000019	1洪吉	97000023	1次仁桑珠	97000019	1次仁德吉	97000019	1巴桑	97000023	1declare global temporary table session.int_check_user_status    (   user_id        CHARACTER(15),   product_no     CHARACTER(15),   test_flag      CHARACTER(1),   sim_code       CHARACTER(15),   usertype_id    CHARACTER(4),   create_date    CHARACTER(15),   time_id        int)                            partitioning key            (   user_id     ) using hashing           with replace on commit preserve rows not logged in tbs_user_tempinsert into session.int_check_user_status (         user_id            ,product_no         ,test_flag          ,sim_code           ,usertype_id          ,create_date        ,time_id )select e.user_id        ,e.product_no          ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag        ,e.sim_code        ,f.usertype_id          ,e.create_date          ,f.time_id       from (select user_id,create_date,product_no,sim_code,usertype_id                    ,row_number() over(partition by user_id order by time_id desc ) row_id     from bass1.g_a_02004_day  where time_id<=20110324) einner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id                  from bass1.g_a_02008_day               where time_id<=20110324 ) f on f.user_id=e.user_idwhere e.row_id=1 and f.row_id=1commitselect * from bass2.ODS_PRODUCT_ORD_CUST_20110324where bill_id in(select product_no from bass2.dw_product_20110324where cust_id in(   select cust_id from bass2.dw_cust_20110324    where cust_id in        (        select cust_id from bass2.dw_product_20110324        where user_id in            (            select user_id from session.int_check_user_status             where create_date = '20110324'            and test_flag='0'            )        )    and cust_name like '%代理%'))select business_id,count(*) from bass2.ODS_PRODUCT_ORD_CUST_20110324where bill_id in(select product_no from bass2.dw_product_20110324where cust_id in(   select cust_id from bass2.dw_cust_20110324    where cust_id in        (        select cust_id from bass2.dw_product_20110324        where user_id in            (            select user_id from session.int_check_user_status             where create_date = '20110324'            and test_flag='0'            )        )    and cust_name like '%代理%'))group by business_idBUSINESS_ID	    2191000000007	31191000000008	2191000000012	1191000000145	2063191000001017	45191000001024	116193000000001	12select * from bass2.ODS_PROD_UP_PRODUCT_ITEM_20110324where product_item_id in (191000000145)select b.CHANNEL_ID, count(0)                    from bass2.dw_product_20110324 a ,bass2.dwd_cust_msg_20110324 b                    where usertype_id in (1,2,9)                      and day_new_mark = 1 and test_mark<>1                     and a.cust_id = b.cust_idand b.cust_name like '%代理%'group by    b.CHANNEL_IDorder by 2 desc 91000047	56891000048	53591000049	23191000046	17791000050	173select b.CHANNEL_ID, count(0)                    from bass2.dw_product_20110321 a ,bass2.dwd_cust_msg_20110321 b                    where usertype_id in (1,2,9)                      and day_new_mark = 1 and test_mark<>1                     and a.cust_id = b.cust_idand b.CHANNEL_ID in (91000047,91000048,91000049,91000046,91000050)group by    b.CHANNEL_IDorder by 1 desc select * from bass2.ODS_PROD_UP_PRODUCT_ITEM_20110324where product_item_id in (191000000145)select * from BASS2.ODS_CHANNEL_INFO_20110324 bwhere  b.CHANNEL_ID in (91000047,91000048,91000049,91000046,91000050)order by 1 desc drop table BASS1.G_A_02059_DAY_down20110321CREATE TABLE BASS1.G_A_02059_DAY_down20110321 (  ENTERPRISE_ID         CHARACTER(20),  USER_ID               CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (USER_ID   ) USING HASHINGselect count(0)from (select         ENTERPRISE_ID        ,USER_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,ORDER_DATE        ,STATUS_ID from bass1.G_A_02059_DAYexcept select         ENTERPRISE_ID        ,USER_ID        ,ENTERPRISE_BUSI_TYPE        ,MANAGE_MODE        ,ORDER_DATE        ,STATUS_ID from  bass1.G_A_02059_DAY_down20110321        ) tselect ENTERPRISE_IDfrom (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from BASS1.G_A_02055_DAY_down20110321 where MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340') t ) t2 where rn = 1  and  STATUS_ID ='1'  exceptselect ENTERPRISE_IDfrom (select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn from (select *from BASS1.G_A_02055_DAY where MANAGE_MODE = '2'and time_id <20110301 and ENTERPRISE_BUSI_TYPE = '1340') t ) t2 where rn = 1  and  STATUS_ID ='1'        select * from   G_A_02055_DAY where ENTERPRISE_ID = '89103000041929'select * from   G_A_02055_DAY_down20110321 where ENTERPRISE_ID = '89103000041929'select * from   bass2.dw_enterprise_member_mid_20110327 where enterprise_id = '89103000041929'select count(0)from (select distinct trim(ENTERPRISE_ID) ENTERPRISE_ID,trim(USER_ID) from bass1.G_A_02059_DAY_down20110321 a exceptselect distinct trim(ENTERPRISE_ID) ENTERPRISE_ID,trim(USER_ID) from bass1.G_A_02059_DAY b) tselect count(0) , count(distinct trim(ENTERPRISE_ID)) ENTERPRISE_ID from bass1.G_A_02059_DAY_down20110321bass1.G_A_02059_DAY_down20110321select ascii(ENTERPRISE_ID),ENTERPRISE_IDfrom  bass1.G_A_02059_DAY_down20110321 where ENTERPRISE_ID like  '%89403000904884%'fetch first 1 rows only     select ascii(ENTERPRISE_ID)from  bass1.G_A_02059_DAY where ENTERPRISE_ID = '89403000904884'fetch first 1 rows onlydrop table BASS1.G_A_02059_DAY_down20110321CREATE TABLE BASS1.G_A_02059_DAY_down20110321 (  ENTERPRISE_ID         CHARACTER(20),  USER_ID               CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (USER_ID   ) USING HASHING           select * from BASS1.G_A_02055_DAY where enterprise_id = '89202999392356'        CREATE TABLE BASS1.G_A_02055_DAY_down20110321 (TIME_ID               INTEGER,  ENTERPRISE_ID         CHARACTER(20),  ENTERPRISE_BUSI_TYPE  CHARACTER(4),  MANAGE_MODE           CHARACTER(1),  PAY_TYPE              CHARACTER(1),  CREATE_MODE           CHARACTER(1),  ORDER_DATE            CHARACTER(8),  STATUS_ID             CHARACTER(1) )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (ENTERPRISE_ID,    TIME_ID   ) USING HASHINGALTER TABLE BASS1.G_A_02055_DAY_down20110321  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEselect * from   BASS1.G_A_02055_DAY_down20110321select * from   bass2.dw_enterbass2.DWD_GROUP_ORDER_FEATUR_${timestamp}select tabname from syscat.tables where tabname like '%DWD_ENTERPRISE_SUB%'select * from   G_A_02055_DAY_down20110321 where ENTERPRISE_ID = '89103000041929'select date(DONE_DATE) from   bass2.DWD_ENTERPRISE_SUB_20101001 WHERE GROUP_ID = '89103000041929'AND DONE_DATE LIKE '2009-11%'select 20110328,a.ENTERPRISE_ID,a.ENTERPRISE_BUSI_TYPE,a.MANAGE_MODE,a.PAY_TYPE,a.CREATE_MODE,'20110328' ORDER_DATE,'2' STATUS_IDfrom bass1.G_A_02055_DAY_down20110321 awhere  ENTERPRISE_ID = '89103000041929'select * from G_A_02055_DAYwhere ENTERPRISE_ID = '89103000041929'and ENTERPRISE_BUSI_TYPE = '1340'delete from   G_A_02055_DAY where ENTERPRISE_ID = '89103000041929'select * from app.sch_control_alarm select * from   G_A_02055_DAYselect * from app.sch_control_alarm where alarmtime >= timestamp('20110329'||'000000') --and flag = -1 and control_code like 'BASS1%'select * from bass2.dim_test_enterpriseselect * from syscat.tables where tabname like 'DIM%ENTERPRISE%'select * from    BASS1.t_grp_id_old_new_map--目标表drop table BASS1.t_grp_id_old_new_mapCREATE TABLE BASS1.t_grp_id_old_new_map (	 area_id            		INTEGER              ----数据日期        	,OLD_ENTERPRISE_ID      		CHAR(20)             ----old集团客户标识    	,NEW_ENTERPRISE_ID      		CHAR(20)            ----new集团客户标识    	,ENTERPRISE_NAME    		CHAR(60)       								----集团客户名称*     )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (OLD_ENTERPRISE_ID,NEW_ENTERPRISE_ID) USING HASHINGALTER TABLE BASS1.t_grp_id_old_new_map  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEselect distinct length(trim(NEW_ENTERPRISE_ID)) from   BASS1.t_grp_id_old_new_mapwhere  length(trim(NEW_ENTERPRISE_ID)) <> 14select * from   BASS1.t_grp_id_old_new_mapwhere  length(trim(NEW_ENTERPRISE_ID)) = 14select * from   BASS1.t_grp_id_old_new_mapwhere  length(trim(NEW_ENTERPRISE_ID)) = 14and NEW_ENTERPRISE_ID like '8%'and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_ID  CREATE TABLE BASS1.grp_id_old_new_map_20110330 like BASS1.t_grp_id_old_new_map  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (OLD_ENTERPRISE_ID,NEW_ENTERPRISE_ID) USING HASHINGALTER TABLE BASS1.grp_id_old_new_map_20110330  LOCKSIZE ROW  APPEND OFF  NOT VOLATILE      insert into   BASS1.grp_id_old_new_map_20110330select * from   BASS1.t_grp_id_old_new_mapwhere  length(trim(NEW_ENTERPRISE_ID)) = 14and NEW_ENTERPRISE_ID like '8%'and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_IDselect * from   BASS1.grp_id_old_new_map_20110330select count(0),count(distinct OLD_ENTERPRISE_ID) from   BASS1.grp_id_old_new_map_20110330select count(0),count(distinct NEW_ENTERPRISE_ID) from   BASS1.grp_id_old_new_map_20110330select *from (        select a.*,row_number()over(partition by OLD_ENTERPRISE_ID order by ENTERPRISE_NAME desc ) rn  from   BASS1.t_grp_id_old_new_map awhere  length(trim(NEW_ENTERPRISE_ID)) = 14and NEW_ENTERPRISE_ID like '8%'and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_ID) torder by 2,5delete from  BASS1.grp_id_old_new_map_20110330insert into   BASS1.grp_id_old_new_map_20110330select          AREA_ID        ,OLD_ENTERPRISE_ID        ,NEW_ENTERPRISE_ID        ,ENTERPRISE_NAMEfrom (        select a.*,row_number()over(partition by OLD_ENTERPRISE_ID order by ENTERPRISE_NAME desc ) rn  from   BASS1.t_grp_id_old_new_map awhere  length(trim(NEW_ENTERPRISE_ID)) = 14and NEW_ENTERPRISE_ID like '8%'and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_ID) t where rn = 1select * from    BASS1.grp_id_old_new_map_20110330select count(0) from app.g_unit_info where unit_code='77780'select * from app.g_unit_info where unit_code='77780'delete from app.g_unit_info where unit_code='77780'insert into app.g_unit_info values ('77780',0,'重要集团客户拍照置换清单','BASS1.G_I_77780_DAY',1,0,0)CREATE TABLE BASS1.dim_bass1_std_map (	 interface_code       INTEGER            	,dim_table_id      		CHAR(20)       NOT NULL      	,code                 CHAR(9)        NOT NULL				       	,code_name    		    CHAR(60)       NOT NULL											 )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (dim_table_id,code) USING HASHINGALTER TABLE BASS1.dim_bass1_std_map  LOCKSIZE ROW  APPEND OFF  NOT VOLATILE      select * from   BASS1.dim_bass1_std_map where dim_table_id = 'BASS_STD1_0001'    select ENTER_TYPE_ID from   G_I_77778_DAYgroup by ENTER_TYPE_IDselect * from   G_I_77778_DAYselect count(0)from (select distinct ENTER_TYPE_ID ENTER_TYPE_ID from   G_I_77778_DAY) a where ENTER_TYPE_ID  not in (select code from BASS1.dim_bass1_std_map where interface_code = '77780' and dim_table_id ='BASS_STD1_0001')group by ENTER_TYPE_IDdrop table BASS1.dim_bass1_std_mapCREATE TABLE BASS1.dim_bass1_std_map (	 interface_code       CHAR(5)            	,dim_table_id      		CHAR(20)       NOT NULL      	,code                 CHAR(9)        NOT NULL				       	,code_name    		    CHAR(60)       NOT NULL											 )  DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX  PARTITIONING KEY   (dim_table_id,code) USING HASHINGALTER TABLE BASS1.dim_bass1_std_map  LOCKSIZE ROW  APPEND OFF  NOT VOLATILEselect * from  bass1.G_I_77778_DAY create nickname xzbass1.G_I_77778_DAY 
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
;insert into BASS1.G_I_77780_DAY
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
   (ENTERPRISE_ID,ID) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;          select count(0) from BASS1.G_I_77780_DAY    select * from app.g_unit_info where unit_code='77780';
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
delete from  app.sch_control_task  where control_code = 'BASS1_EXP_G_I_77780_DAY'select * from   app.sch_control_task delete from app.sch_control_before where control_code = 'BASS1_EXP_G_I_77780_DAY'insert into app.sch_control_before values ('BASS1_EXP_G_I_77780_DAY','BASS1_EXP_G_S_04008_DAY')delete from  app.sch_control_task where control_code = 'BASS1_EXP_G_I_77780_DAY'insert into app.sch_control_task values ('BASS1_EXP_G_I_77780_DAY',1,2,'bass1_export bass1.g_i_77780_day YESTERDAY()',0,-1,'重要集团客户拍照置换清单','app','BASS1',1,'/bassapp/backapp/bin/bass1_export/');
update  app.sch_control_taskset cc_flag = 2where control_code = 'BASS1_EXP_G_I_77780_DAY'delete from app.g_unit_info where unit_code='77780';
insert into app.g_unit_info values ('77780',0,'重要集团客户拍照置换清单','bass1.g_i_77780_day',1,0,0);
select * from   app.sch_control_map where control_code like '%7778%'select * from   bass1.int_program_data where program_name like '%7778%'select * from   app.sch_control_before where control_code like '%7778%'select ENTERPRISE_ID||ID||ENTERPRISE_NAME||ORG_TYPE||ADDR_CODE||CITY||REGION||COUNTY||DOOR_NO||AREA_CODE||PHONE_NO1||PHONE_NO2||POST_CODE||INDUSTRY_TYPE||EMPLOYEE_CNT||INDUSTRY_UNIT_CNT||ECONOMIC_TYPE||OPEN_YEAR||OPEN_MONTH||SHAREHOLDER||GROUP_TYPE||MANAGE_STYLE||OPERATE_REVENUE_CLASS||CAPITAL_CLASS||INDUSTRY_CLASS_CODE||CUST_STATUS||CUST_INFO_SRC_ID||UPLOAD_TYPE_ID from bass1.g_i_77780_day where time_id=20101231select *  from bass1.g_i_77780_day where time_id=20101231select ENTERPRISE_ID||ID||ENTERPRISE_NAME||ORG_TYPE||ADDR_CODE||CITY||REGION||COUNTY||DOOR_NO||AREA_CODE||PHONE_NO1||PHONE_NO2||POST_CODE||INDUSTRY_TYPE||EMPLOYEE_CNT||INDUSTRY_UNIT_CNT||ECONOMIC_TYPE||OPEN_YEAR||OPEN_MONTH||SHAREHOLDER||GROUP_TYPE||MANAGE_STYLE||OPERATE_REVENUE_CLASS||CAPITAL_CLASS||INDUSTRY_CLASS_CODE||CUST_STATUS||CUST_INFO_SRC_ID||UPLOAD_TYPE_ID from bass1.g_i_77780_day where time_id=20101231select * from g_i_77778_day
update BASS1.G_I_77780_DAY
set ENTERPRISE_ID = ' '
where ENTERPRISE_ID is null 
;

update BASS1.G_I_77780_DAY
set ID = ' '
where ID is null 
;

update BASS1.G_I_77780_DAY
set ENTERPRISE_NAME = ' '
where ENTERPRISE_NAME is null 
;

update BASS1.G_I_77780_DAY
set ORG_TYPE = ' '
where ORG_TYPE is null 
;


update BASS1.G_I_77780_DAY
set CITY = ' '
where CITY is null 
;

update BASS1.G_I_77780_DAY
set REGION = ' '
where REGION is null 
;

update BASS1.G_I_77780_DAY
set COUNTY = ' '
where COUNTY is null 
;

update BASS1.G_I_77780_DAY
set DOOR_NO = ' '
where DOOR_NO is null 
;

update BASS1.G_I_77780_DAY
set AREA_CODE = ' '
where AREA_CODE is null 
;

update BASS1.G_I_77780_DAY
set PHONE_NO1 = ' '
where  PHONE_NO1 is null 
;

update BASS1.G_I_77780_DAY
set PHONE_NO2 = ' '
where PHONE_NO2 is null 
;

update BASS1.G_I_77780_DAY
set POST_CODE = ' '
where POST_CODE is null 
;


update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = ' '
where INDUSTRY_TYPE is null 
;

update BASS1.G_I_77780_DAY
set EMPLOYEE_CNT = ' '
where EMPLOYEE_CNT is null 
;

update BASS1.G_I_77780_DAY
set INDUSTRY_UNIT_CNT = ' '
where INDUSTRY_UNIT_CNT is null 
;

update BASS1.G_I_77780_DAY
set ECONOMIC_TYPE = ' '
where ECONOMIC_TYPE is null 
;

update BASS1.G_I_77780_DAY
set OPEN_YEAR = ' '
where OPEN_YEAR is null 
;

update BASS1.G_I_77780_DAY
set OPEN_MONTH = ' '
where OPEN_MONTH  is null 
;

update BASS1.G_I_77780_DAY
set SHAREHOLDER = ' '
where SHAREHOLDER is null 
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
;
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
;update BASS1.G_I_77780_DAY
set CUST_STATUS = '31'
where CUST_STATUS = '30'
;select * from  BASS1.t_bass1_std_0113where code = '9011'
 select count(0)
from 
(select distinct trim(INDUSTRY_TYPE) INDUSTRY_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_TYPE  not in 
(select code from BASS1.t_bass1_std_0113 )


update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '9010'
where INDUSTRY_TYPE = '9011'
;
8910300013519989108911013693891030012154118910156000161089100000003719
update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '3010'
where INDUSTRY_TYPE = '3000'
;

update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '2040'
where INDUSTRY_TYPE = '2050'
;

select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
--and flag = -1
and control_code not  like 'BASS1%'
order by alarmtime desc 
select * from   BASS1.G_S_04002_DAY_20100501BAKselect * from   app.sch_control_before where control_code = 'BASS1_G_S_04015_DAY.tcl'BASS1_G_S_04015_DAY.tcl	TR1_L_A98012
select * from   app.sch_control_runlog where control_code = 'TR1_L_A98012'select * from   app.sch_control_runlog where control_code = 'BASS1_G_S_04015_DAY.tcl'TR1_L_A98012	2011-03-30 6:35:13.182254	2011-03-30 6:35:50.654050	37	0
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
		group by product_no,imei                RENAME TABLE BASS2.DIM_TERM_TAC TO DIM_TERM_TAC_20110331BAK;
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
   ) USING HASHING;
ALTER TABLE BASS2.DIM_TERM_TAC
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
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
   ) USING HASHING;
ALTER TABLE BASS2.DIM_TERM_TAC_0331
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
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
where net_type <>'2';       select count(0) ,count(distinct TAC_NUM) from BASS2.DIM_TERM_TAC_20110331BAK       select count(0) ,count(distinct TAC_NUM) from  BASS2.DIM_TERM_TAC27571	27571
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
where net_type <>'2';       select count(0)from (		select product_no,imei,count(*) 
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
		   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 

ALTER TABLE "BASS1   "."G_S_04008_DAY0331" APPEND ON;

ALTER TABLE "BASS1   "."G_S_04008_DAY0331" LOCKSIZE TABLE;

insert into  bass1.G_S_04008_DAY0331select * from   bass1.G_S_04008_DAYwhere time_id = 20110331 select * from  BASS1.G_S_04008_DAY where time_id = 20110331 select time_id,count(0)from    BASS1.G_S_04008_DAY0331group by     time_id  20110331	94470
 select * from   bass1.G_RULE_CHECK where 
rule_code in ('R107','R108')   select * from  syscat.tables where tabname like 'SCH_CONTROL_RUN%'  select * from  app.SCH_CONTROL_RUNLOG_BAKNGGJ where control_code = 'BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl'  select * from  bass1.G_RULE_CHECK   WHERE TIME_ID= 20110331
  AND RULE_CODE IN ('R107','R108')  select * from   syscat.indexes where tabschema = 'BASS2'drop table "BASS1"."DUAL"CREATE TABLE "BASS1"."DUAL" like sysibm.SYSdummy1 
 IN "TBS_APP_BASS1";

ALTER TABLE "BASS1"."DUAL"
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
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
		   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 
ALTER TABLE BASS1.DIM_NOT_NULL_INTERFACE
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
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
   (INTERFACE_CODE) USING HASHING;     ALTER TABLE BASS1.MON_ALL_INTERFACE
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
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
select count(0),count(distinct user_id) from   BASS1.G_I_02005_MONTH where time_id  = 201103