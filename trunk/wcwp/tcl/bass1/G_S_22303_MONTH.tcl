######################################################################################################
#接口名称：集团产品业务量月汇总
#接口编码：22303
#接口说明：本接口上报所有集团业务的业务量月使用情况。
#程序名称: G_S_22303_MONTH.tcl
#功能描述: 生成22303的数据
#运行粒度: 月
#源    表：
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2009-07-05
#问题记录：
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        set op_time 2008-06-01
#        set optime_month 2009-05-31
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        puts $timestamp
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        #----求上月最后一天---#,格式 yyyymmdd
        puts $last_month_day
        
        set thisyear [string range $op_time 0 3]
        puts $thisyear
        
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        set objecttable "G_S_22303_MONTH"
        #set db_user $env(DB_USER)
       
       
       
        set this_month_firstday [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_firstday 

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.$objecttable where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


  #建立G_S_22303_MONTH的session表，表名session.tmp22303
	set sql_buff "declare global temporary table session.tmp22303
	        (
						  enterprise_id  		char(20)   not null,
						  ent_busi_id			  char(4)    not null,
						  manage_mod			  char(1)    not null,
							bill_duration			bigint,
							upmessage					bigint,
							downmessage				bigint,
							upflow						bigint,
							downflow					bigint,
							mms_busi_nums			bigint,
							accept_mail_nums	bigint,
							send_mail_nums		bigint,
							wap_busi_nums			bigint,
							web_busi_nums			bigint,
							ussd_counts				bigint,
							guoji_flows				bigint,
							guonei_flows			bigint,
							use_cust_nums			bigint,
							bill_cust_nums		bigint,
							leiji_cust_nums		bigint,
							nzt_phone_counts	bigint
                )
               partitioning key (ENTERPRISE_ID) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
 	
	exec_sql $sql_buff
	puts "建立G_S_22303_MONTH的session表，表名session.tmp22303"
	

  #建立集团业务临时表,表名session.tmp22303_temp1
	set sql_buff "declare global temporary table session.tmp22303_temp1
	        (
						  service_id  		char(20),
						  config_value		varchar(100),
						  bass1_value			varchar(100),
							plan_id			    bigint
                )
               partitioning key (plan_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
 	
	exec_sql $sql_buff
	puts "建立G_S_22303_MONTH的session表，表名session.tmp22303"
		


  #建立user_id关联业务临时表，表名session.tmp22303_temp2
	set sql_buff "declare global temporary table session.tmp22303_temp2
	        (
						  user_id						char(20)   not null,
						  ent_busi_id			  char(4)    not null,
						  manage_mod			  char(1)    not null,
							bill_duration			bigint,
							upmessage					bigint,
							downmessage				bigint,
							upflow						bigint,
							downflow					bigint,
							mms_busi_nums			bigint,
							accept_mail_nums	bigint,
							send_mail_nums		bigint,
							wap_busi_nums			bigint,
							web_busi_nums			bigint,
							ussd_counts				bigint,
							guoji_flows				bigint,
							guonei_flows			bigint,
							use_cust_nums			bigint,
							bill_cust_nums		bigint,
							leiji_cust_nums		bigint,
							nzt_phone_counts	bigint
                )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
 	
	exec_sql $sql_buff
	puts "建立G_S_22303_MONTH的session表，表名session.tmp22303"



  #插入主体集团业务类型结构信息数据
	set sql_buff "insert into session.tmp22303
	        (
						  enterprise_id,
						  ent_busi_id,
						  manage_mod,
							bill_duration,
							upmessage,
							downmessage,
							upflow,
							downflow,
							mms_busi_nums,
							accept_mail_nums,
							send_mail_nums,
							wap_busi_nums,
							web_busi_nums,
							ussd_counts,
							guoji_flows,
							guonei_flows,
							use_cust_nums,
							bill_cust_nums,
							leiji_cust_nums,
							nzt_phone_counts
            )
						select distinct
							a.enterprise_id   as enterprise_id,
							c.bass1_value     as ent_busi_id,
							case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end as manage_mod,
							0 as bill_duration,
							0 as upmessage,
							0 as downmessage,
							0 as upflow,
							0 as downflow,
							0 as mms_busi_nums,
							0 as accept_mail_nums,
							0 as send_mail_nums,
							0 as wap_busi_nums,
							0 as web_busi_nums,
							0 as ussd_counts,
							0 as guoji_flows,
							0 as guonei_flows,
							0 as use_cust_nums,
							0 as bill_cust_nums,
							0 as leiji_cust_nums,
							0 as nzt_phone_counts
						from bass2.dw_enterprise_extsub_rela_${op_month} a,
						(select * from bass2.dim_service_config where config_id=1000027) b,
						(select * from bass1.all_dim_lkp_160 where bass1_tbid='BASS_STD1_0108') c
						where a.service_id = b.service_id
						  and a.service_id = c.xzbas_value"
          
          
	exec_sql $sql_buff
	puts "插入到session表中的业务结构信息完成"	
	
	
  ## 917	无线PBX(WLAN)业务	1030	无线PBX
  ## 934										1130	集团E网
  ## 933	集团彩铃业务			1140	集团彩铃
  ## 912	集团专线产品服务	1170	互联网专线
  ## 925										1210	企业邮箱
  ## 926										1220	手机邮箱
  ## 717										1230	BLACK BERRY
  ## 946	车载数据卡				1240	M2M（行业应用卡）
  ## 949	电力数据卡				1240	M2M（行业应用卡）
  ## 947	气象数据卡				1240	M2M（行业应用卡）
  ## 942	全网车务通				1240	M2M（行业应用卡）
  ## 944	无线POS						1240	M2M（行业应用卡）
  ## 939	MAS服务(包括移动MAS,手机邮箱(MAS)，无线网站(MAS))	1250	移动OA
  ## 903	移动OA（ADC）			1250	移动OA
  ## 904	移动进销存（ADC）	1260	移动进销存
  ## 906	移动CRM（ADC）		1280	移动CRM
  ## 142	移动CRM（MAS）		1280	移动CRM
  ## 936	商信通						1290	商信通
  ## 951										1300	财信通
  ## 911	校信通业务				1310	校讯通
  ## 953	农信通(中央ADC)		1320	农信通
  ## 910	企信通(ADC)				1330	企信通
  ## 924										1340	企业建站
  ## 945	警务通						1360	警务通



  #生成集团业务临时表
	set sql_buff "insert into session.tmp22303_temp1
	        (
						service_id,
						config_value,
						bass1_value,
					  plan_id
            )
            select 
              a.service_id,
              a.config_value,
              b.bass1_value,
              c.plan_id 
            from 
						(select * from bass2.dim_service_config where config_id=1000027) a,
						(select * from bass1.all_dim_lkp_160 where bass1_tbid='BASS_STD1_0108') b,
						(SELECT service_id,int(plan_id) plan_id FROM bass2.DIM_ENT_GROUP_PLAN) c
						where a.service_id = b.xzbas_value
						  and a.service_id = c.service_id"
          
          
	exec_sql $sql_buff
	puts "插入到session.tmp22303_temp1集团业务类型临时表完成"	


  #统计计费时长
	set sql_buff "insert into session.tmp22303_temp2
	        (
						  user_id,
						  ent_busi_id,
						  manage_mod,
							bill_duration,
							upmessage,
							downmessage,
							upflow,
							downflow,
							mms_busi_nums,
							accept_mail_nums,
							send_mail_nums,
							wap_busi_nums,
							web_busi_nums,
							ussd_counts,
							guoji_flows,
							guonei_flows,
							use_cust_nums,
							bill_cust_nums,
							leiji_cust_nums,
							nzt_phone_counts
            )
						select 
							a.user_id      as user_id,
							b.bass1_value  as ent_busi_id,
							case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end as manage_mod,
							sum(a.call_duration_m) as bill_duration,
							0 as upmessage,
							0 as downmessage,
							0 as upflow,
							0 as downflow,
							0 as mms_busi_nums,
							0 as accept_mail_nums,
							0 as send_mail_nums,
							0 as wap_busi_nums,
							0 as web_busi_nums,
							0 as ussd_counts,
							0 as guoji_flows,
							0 as guonei_flows,
							0 as use_cust_nums,
							0 as bill_cust_nums,
							0 as leiji_cust_nums,
							0 as nzt_phone_counts
						from bass2.dw_call_${op_month} a,
						     session.tmp22303_temp1 b
						where a.plan_id = b.plan_id
					 group by a.user_id,b.bass1_value,
					 		case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end"
          
          
	exec_sql $sql_buff
	puts "插入到session.tmp22303_temp2表中的计费时长指标信息完成"	


  ##~   #统计上行、下行短信条数
	##~   set sql_buff "insert into session.tmp22303_temp2
	        ##~   (
						  ##~   user_id,
						  ##~   ent_busi_id,
						  ##~   manage_mod,
							##~   bill_duration,
							##~   upmessage,
							##~   downmessage,
							##~   upflow,
							##~   downflow,
							##~   mms_busi_nums,
							##~   accept_mail_nums,
							##~   send_mail_nums,
							##~   wap_busi_nums,
							##~   web_busi_nums,
							##~   ussd_counts,
							##~   guoji_flows,
							##~   guonei_flows,
							##~   use_cust_nums,
							##~   bill_cust_nums,
							##~   leiji_cust_nums,
							##~   nzt_phone_counts
            ##~   )
						##~   select 
							##~   a.user_id      as user_id,
							##~   b.bass1_value  as ent_busi_id,
							##~   case
								##~   when upper(b.config_value)='MAS' then '1'
								##~   when upper(b.config_value)='ADC' then '2'
								##~   else '3'
							##~   end as manage_mod,
							##~   0 as bill_duration,
							##~   sum(case when calltype_id=0 then counts else 0 end) as upmessage,
							##~   sum(case when calltype_id=1 then counts else 0 end) as downmessage,
							##~   0 as upflow,
							##~   0 as downflow,
							##~   0 as mms_busi_nums,
							##~   0 as accept_mail_nums,
							##~   0 as send_mail_nums,
							##~   0 as wap_busi_nums,
							##~   0 as web_busi_nums,
							##~   0 as ussd_counts,
							##~   0 as guoji_flows,
							##~   0 as guonei_flows,
							##~   0 as use_cust_nums,
							##~   0 as bill_cust_nums,
							##~   0 as leiji_cust_nums,
							##~   0 as nzt_phone_counts
						##~   from bass2.dw_newbusi_ismg_${op_month} a,
						     ##~   session.tmp22303_temp1 b
						##~   where a.plan_id = b.plan_id
					 ##~   group by a.user_id,b.bass1_value,
					 		##~   case
								##~   when upper(b.config_value)='MAS' then '1'
								##~   when upper(b.config_value)='ADC' then '2'
								##~   else '3'
							##~   end"
          
  ##~           
	##~   exec_sql $sql_buff
	##~   puts "插入到session.tmp22303_temp2表中的上行、下行短信条数指标信息完成"	
		

  #统计上行、下行短信条数
	set sql_buff "insert into session.tmp22303_temp2
	        (
						  user_id,
						  ent_busi_id,
						  manage_mod,
							bill_duration,
							upmessage,
							downmessage,
							upflow,
							downflow,
							mms_busi_nums,
							accept_mail_nums,
							send_mail_nums,
							wap_busi_nums,
							web_busi_nums,
							ussd_counts,
							guoji_flows,
							guonei_flows,
							use_cust_nums,
							bill_cust_nums,
							leiji_cust_nums,
							nzt_phone_counts
            )
						select 
							a.user_id      as user_id,
							b.BASS1_VALUE ent_busi_id,
					 		b.TYPE manage_mod,
							0 as bill_duration,
							sum(case when calltype_id=0 then counts else 0 end) as upmessage,
							sum(case when calltype_id=1 then counts else 0 end) as downmessage,
							0 as upflow,
							0 as downflow,
							0 as mms_busi_nums,
							0 as accept_mail_nums,
							0 as send_mail_nums,
							0 as wap_busi_nums,
							0 as web_busi_nums,
							0 as ussd_counts,
							0 as guoji_flows,
							0 as guonei_flows,
							0 as use_cust_nums,
							0 as bill_cust_nums,
							0 as leiji_cust_nums,
							0 as nzt_phone_counts
						from bass2.dw_newbusi_ismg_${op_month} a,DIM_GRP_PLANID b
						where a.plan_id = b.plan_id
					 group by a.user_id
							,b.BASS1_VALUE
					 		,b.TYPE
						with ur
						"
          
          
	exec_sql $sql_buff
	puts "插入到session.tmp22303_temp2表中的上行、下行短信条数指标信息完成"	
		


  #统计上行、下行流量
	set sql_buff "insert into session.tmp22303_temp2
	        (
						  user_id,
						  ent_busi_id,
						  manage_mod,
							bill_duration,
							upmessage,
							downmessage,
							upflow,
							downflow,
							mms_busi_nums,
							accept_mail_nums,
							send_mail_nums,
							wap_busi_nums,
							web_busi_nums,
							ussd_counts,
							guoji_flows,
							guonei_flows,
							use_cust_nums,
							bill_cust_nums,
							leiji_cust_nums,
							nzt_phone_counts
            )
						select 
							a.user_id      as user_id,
							b.bass1_value  as ent_busi_id,
							case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end as manage_mod,
							0 as bill_duration,
							0 as upmessage,
							0 as downmessage,
							sum(upflow1+upflow2) as upflow,
							sum(downflow1+downflow1) as downflow,
							0 as mms_busi_nums,
							0 as accept_mail_nums,
							0 as send_mail_nums,
							0 as wap_busi_nums,
							0 as web_busi_nums,
							0 as ussd_counts,
							0 as guoji_flows,
							0 as guonei_flows,
							0 as use_cust_nums,
							0 as bill_cust_nums,
							0 as leiji_cust_nums,
							0 as nzt_phone_counts
						from bass2.dw_newbusi_gprs_${op_month} a,
						     session.tmp22303_temp1 b
						where a.plan_id = b.plan_id
						  and a.bill_mark = 1
					 group by a.user_id,b.bass1_value,
					 		case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end"
          
          
	exec_sql $sql_buff
	puts "插入到session.tmp22303_temp2表中的上行、下行流量指标信息完成"	
	

  #统计彩信业务量
	set sql_buff "insert into session.tmp22303_temp2
	        (
						  user_id,
						  ent_busi_id,
						  manage_mod,
							bill_duration,
							upmessage,
							downmessage,
							upflow,
							downflow,
							mms_busi_nums,
							accept_mail_nums,
							send_mail_nums,
							wap_busi_nums,
							web_busi_nums,
							ussd_counts,
							guoji_flows,
							guonei_flows,
							use_cust_nums,
							bill_cust_nums,
							leiji_cust_nums,
							nzt_phone_counts
            )
						select 
							a.user_id      as user_id,
							b.bass1_value  as ent_busi_id,
							case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end as manage_mod,
							0 as bill_duration,
							0 as upmessage,
							0 as downmessage,
							0 as upflow,
							0 as downflow,
							sum(counts) as mms_busi_nums,
							0 as accept_mail_nums,
							0 as send_mail_nums,
							0 as wap_busi_nums,
							0 as web_busi_nums,
							0 as ussd_counts,
							0 as guoji_flows,
							0 as guonei_flows,
							0 as use_cust_nums,
							0 as bill_cust_nums,
							0 as leiji_cust_nums,
							0 as nzt_phone_counts
						from bass2.dw_newbusi_mms_${op_month} a,
						     session.tmp22303_temp1 b
						where a.plan_id = b.plan_id
					 group by a.user_id,b.bass1_value,
					 		case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end"
          
          
	exec_sql $sql_buff
	puts "插入到session.tmp22303_temp2表中的彩信业务量指标信息完成"	


  #统计WAP业务量
	set sql_buff "insert into session.tmp22303_temp2
	        (
						  user_id,
						  ent_busi_id,
						  manage_mod,
							bill_duration,
							upmessage,
							downmessage,
							upflow,
							downflow,
							mms_busi_nums,
							accept_mail_nums,
							send_mail_nums,
							wap_busi_nums,
							web_busi_nums,
							ussd_counts,
							guoji_flows,
							guonei_flows,
							use_cust_nums,
							bill_cust_nums,
							leiji_cust_nums,
							nzt_phone_counts
            )
						select 
							a.user_id      as user_id,
							b.bass1_value  as ent_busi_id,
							case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end as manage_mod,
							0 as bill_duration,
							0 as upmessage,
							0 as downmessage,
							0 as upflow,
							0 as downflow,
							0 as mms_busi_nums,
							0 as accept_mail_nums,
							0 as send_mail_nums,
							sum(counts) as wap_busi_nums,
							0 as web_busi_nums,
							0 as ussd_counts,
							0 as guoji_flows,
							0 as guonei_flows,
							0 as use_cust_nums,
							0 as bill_cust_nums,
							0 as leiji_cust_nums,
							0 as nzt_phone_counts
						from bass2.dw_newbusi_wap_${op_month} a,
						     session.tmp22303_temp1 b
						where a.plan_id = b.plan_id
					 group by a.user_id,b.bass1_value,
					 		case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end"
          
          
	exec_sql $sql_buff
	puts "插入到session.tmp22303_temp2表中的WAP业务量指标信息完成"	



  #统计黑莓、手机邮箱的国际、国内流量
  #717  1230	BLACK BERRY
  #926	1220	手机邮箱
	set sql_buff "insert into session.tmp22303_temp2
	        (
						  user_id,
						  ent_busi_id,
						  manage_mod,
							bill_duration,
							upmessage,
							downmessage,
							upflow,
							downflow,
							mms_busi_nums,
							accept_mail_nums,
							send_mail_nums,
							wap_busi_nums,
							web_busi_nums,
							ussd_counts,
							guoji_flows,
							guonei_flows,
							use_cust_nums,
							bill_cust_nums,
							leiji_cust_nums,
							nzt_phone_counts
            )
						select 
							a.user_id      as user_id,
							b.bass1_value  as ent_busi_id,
							case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end as manage_mod,
							0 as bill_duration,
							0 as upmessage,
							0 as downmessage,
							0 as upflow,
							0 as downflow,
							0 as mms_busi_nums,
							0 as accept_mail_nums,
							0 as send_mail_nums,
							0 as wap_busi_nums,
							0 as web_busi_nums,
							0 as ussd_counts,
							sum(case when roamtype_id in (3,5,9) then rating_res else 0 end) as guoji_flows,
							sum(case when roamtype_id in (0,1,2,4,6,7,8) then rating_res else 0 end) as guonei_flows,
							0 as use_cust_nums,
							0 as bill_cust_nums,
							0 as leiji_cust_nums,
							0 as nzt_phone_counts
						from bass2.dw_newbusi_gprs_${op_month} a,
						     session.tmp22303_temp1 b
						where a.plan_id = b.plan_id
						  and a.bill_mark = 1
						  and b.bass1_value in ('1220','1230')
					 group by a.user_id,b.bass1_value,
					 		case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end"
          
          
	exec_sql $sql_buff
	puts "插入到session.tmp22303_temp2表中的黑莓、手机邮箱的国际/国内流量指标信息完成"	

  #关联enterprise_id，汇总指标数据插入临时表
	set sql_buff "insert into session.tmp22303
	        (
						  enterprise_id,
						  ent_busi_id,
						  manage_mod,
							bill_duration,
							upmessage,
							downmessage,
							upflow,
							downflow,
							mms_busi_nums,
							accept_mail_nums,
							send_mail_nums,
							wap_busi_nums,
							web_busi_nums,
							ussd_counts,
							guoji_flows,
							guonei_flows,
							use_cust_nums,
							bill_cust_nums,
							leiji_cust_nums,
							nzt_phone_counts
            )
						select 
							a.enterprise_id       as enterprise_id,
							b.ent_busi_id         as ent_busi_id,
							b.manage_mod          as manage_mod,
							sum(bill_duration) 	  as bill_duration,
							sum(upmessage) 			  as upmessage,
							sum(downmessage) 		  as downmessage,
							sum(upflow) 				  as upflow,
							sum(downflow) 			  as downflow,
							sum(mms_busi_nums)    as mms_busi_nums,
							sum(accept_mail_nums) as accept_mail_nums,
							sum(send_mail_nums)   as send_mail_nums,
							sum(wap_busi_nums)    as wap_busi_nums,
							sum(web_busi_nums) 		as web_busi_nums,
							sum(ussd_counts)  		as ussd_counts,
							sum(guoji_flows) 			as guoji_flows,
							sum(guonei_flows)     as guonei_flows,
							0 as use_cust_nums,
							0 as bill_cust_nums,
							0 as leiji_cust_nums,
							0 as nzt_phone_counts
						from bass2.dw_enterprise_member_mid_${op_month} a,
						     session.tmp22303_temp2 b
						where a.user_id = b.user_id
					 group by a.enterprise_id,b.ent_busi_id,b.manage_mod"
          
          
	exec_sql $sql_buff
	puts "插入到session.tmp22303表中指标信息基本汇总完成"	



  #统计各行业类型当月使用客户数
  ##1  校讯通  1310   '3'  其它
  ##2  银信通  1380   '3'  其它
  ##3  农信通  1320   '2'  ADC
  ##4  财信通  1300   '1'  MAS
  ##5  警物通  1360   '3'  其它
  ##8  交警通
	set sql_buff "insert into session.tmp22303
	        (
						  enterprise_id,
						  ent_busi_id,
						  manage_mod,
							bill_duration,
							upmessage,
							downmessage,
							upflow,
							downflow,
							mms_busi_nums,
							accept_mail_nums,
							send_mail_nums,
							wap_busi_nums,
							web_busi_nums,
							ussd_counts,
							guoji_flows,
							guonei_flows,
							use_cust_nums,
							bill_cust_nums,
							leiji_cust_nums,
							nzt_phone_counts
            )
						select 
							a.enterprise_id    as enterprise_id,
							case a.apptype_id
                  when 1 then '1310'
                  when 2 then '1380'
                  when 3 then '1320' 
                  when 4 then '1300'
                  when 5 then '1360'
              end as ent_busi_id,
							case a.apptype_id
                  when 1 then '3'
                  when 2 then '3'
                  when 3 then '2' 
                  when 4 then '1'
                  when 5 then '3'
              end as manage_mod,
							0 as bill_duration,
							0 as upmessage,
							0 as downmessage,
							0 as upflow,
							0 as downflow,
							0 as mms_busi_nums,
							0 as accept_mail_nums,
							0 as send_mail_nums,
							0 as wap_busi_nums,
							0 as web_busi_nums,
							0 as ussd_counts,
							0 as guoji_flows,
							0 as guonei_flows,
							count(distinct a.user_id) as use_cust_nums,
							0 as bill_cust_nums,
							0 as leiji_cust_nums,
							0 as nzt_phone_counts
						from bass2.dw_enterprise_industry_apply a
						where a.enterprise_id<>''
						  and a.op_time = '$this_month_firstday'
              and a.apptype_id in (1,2,3,4,5)
					 group by a.enterprise_id,
					 case a.apptype_id
                  when 1 then '1310'
                  when 2 then '1380'
                  when 3 then '1320' 
                  when 4 then '1300'
                  when 5 then '1360'
              end,
							case a.apptype_id
                  when 1 then '3'
                  when 2 then '3'
                  when 3 then '2' 
                  when 4 then '1'
                  when 5 then '3'
              end"
          
          
	exec_sql $sql_buff
	puts "插入到session.tmp22303表中的当月使用客户数指标信息完成"	



  #统计各行业当月计费客户数
	set sql_buff "insert into session.tmp22303
	        (
						  enterprise_id,
						  ent_busi_id,
						  manage_mod,
							bill_duration,
							upmessage,
							downmessage,
							upflow,
							downflow,
							mms_busi_nums,
							accept_mail_nums,
							send_mail_nums,
							wap_busi_nums,
							web_busi_nums,
							ussd_counts,
							guoji_flows,
							guonei_flows,
							use_cust_nums,
							bill_cust_nums,
							leiji_cust_nums,
							nzt_phone_counts
            )
						select 
							a.enterprise_id    as enterprise_id,
							case a.apptype_id
                  when 1 then '1310'
                  when 2 then '1380'
                  when 3 then '1320' 
                  when 4 then '1300'
                  when 5 then '1360'
              end as ent_busi_id,
							case a.apptype_id
                  when 1 then '3'
                  when 2 then '3'
                  when 3 then '2' 
                  when 4 then '1'
                  when 5 then '3'
              end as manage_mod,
							0 as bill_duration,
							0 as upmessage,
							0 as downmessage,
							0 as upflow,
							0 as downflow,
							0 as mms_busi_nums,
							0 as accept_mail_nums,
							0 as send_mail_nums,
							0 as wap_busi_nums,
							0 as web_busi_nums,
							0 as ussd_counts,
							0 as guoji_flows,
							0 as guonei_flows,
							0 as use_cust_nums,
							count(distinct a.user_id) as bill_cust_nums,
							0 as leiji_cust_nums,
							0 as nzt_phone_counts
						from bass2.dw_enterprise_industry_apply a
						where a.op_time = '$this_month_firstday'
						  and a.enterprise_id<>''
              and a.apptype_id in (1,2,3,4,5)
              and a.fee > 0
					 group by a.enterprise_id,
					 case a.apptype_id
                  when 1 then '1310'
                  when 2 then '1380'
                  when 3 then '1320' 
                  when 4 then '1300'
                  when 5 then '1360'
              end,
							case a.apptype_id
                  when 1 then '3'
                  when 2 then '3'
                  when 3 then '2' 
                  when 4 then '1'
                  when 5 then '3'
              end"
          
          
	exec_sql $sql_buff
	puts "插入到session.tmp22303表中的当月计费客户数指标信息完成"	



  #统计各行业累计使用客户数-自然年度
	set sql_buff "insert into session.tmp22303
	        (
						  enterprise_id,
						  ent_busi_id,
						  manage_mod,
							bill_duration,
							upmessage,
							downmessage,
							upflow,
							downflow,
							mms_busi_nums,
							accept_mail_nums,
							send_mail_nums,
							wap_busi_nums,
							web_busi_nums,
							ussd_counts,
							guoji_flows,
							guonei_flows,
							use_cust_nums,
							bill_cust_nums,
							leiji_cust_nums,
							nzt_phone_counts
            )
						select 
							a.enterprise_id    as enterprise_id,
							case a.apptype_id
                  when 1 then '1310'
                  when 2 then '1380'
                  when 3 then '1320' 
                  when 4 then '1300'
                  when 5 then '1360'
              end as ent_busi_id,
							case a.apptype_id
                  when 1 then '3'
                  when 2 then '3'
                  when 3 then '2' 
                  when 4 then '1'
                  when 5 then '3'
              end as manage_mod,
							0 as bill_duration,
							0 as upmessage,
							0 as downmessage,
							0 as upflow,
							0 as downflow,
							0 as mms_busi_nums,
							0 as accept_mail_nums,
							0 as send_mail_nums,
							0 as wap_busi_nums,
							0 as web_busi_nums,
							0 as ussd_counts,
							0 as guoji_flows,
							0 as guonei_flows,
							0 as use_cust_nums,
							0 as bill_cust_nums,
							count(distinct a.user_id) as leiji_cust_nums,
							0 as nzt_phone_counts
						from (
								select enterprise_id,user_id,city_id,apptype_id
                 from bass2.dw_enterprise_industry_apply
                 where op_time between '$thisyear-01-01' and '$this_month_firstday'
                   and apptype_id in (1,2,3,4,5)
                 group by enterprise_id,user_id,city_id,apptype_id
                 having count(distinct op_time) >= 3
                 union all
                 select enterprise_id,a.user_id,a.city_id,a.apptype_id
                 from bass2.dw_enterprise_industry_apply a left outer join bass2.dw_product_$op_month b
                   on a.user_id = b.user_id
                 where a.op_time between '$thisyear-01-01' and '$this_month_firstday'
                   and a.apptype_id in (1,2,3,4,5)
                 group by enterprise_id,a.user_id,a.city_id,a.apptype_id
						    ) a
						 where a.enterprise_id<>''
					 group by a.enterprise_id,
					 case a.apptype_id
                  when 1 then '1310'
                  when 2 then '1380'
                  when 3 then '1320'
                  when 4 then '1300'
                  when 5 then '1360'
              end,
							case a.apptype_id
                  when 1 then '3'
                  when 2 then '3'
                  when 3 then '2'
                  when 4 then '1'
                  when 5 then '3'
              end"
          
          
	exec_sql $sql_buff
	puts "插入到session.tmp22303表中的累计使用客户数指标信息完成"	


	#插入目标表
	set sql_buff "insert into bass1.G_S_22303_MONTH
	        (
	            time_id,
						  enterprise_id,
						  ent_busi_id,
						  manage_mod,
							bill_duration,
							upmessage,
							downmessage,
							upflow,
							downflow,
							mms_busi_nums,
							accept_mail_nums,
							send_mail_nums,
							wap_busi_nums,
							web_busi_nums,
							ussd_counts,
							guoji_flows,
							guonei_flows,
							use_cust_nums,
							bill_cust_nums,
							leiji_cust_nums,
							nzt_phone_counts
            )
						select 
						  $op_month,
						  enterprise_id,
						  ent_busi_id,
						  manage_mod,
							char(sum(bill_duration)) 	as bill_duration,
							char(sum(upmessage)) 			as upmessage,
							char(sum(downmessage)) 		as downmessage,
							char(sum(upflow)) 				as upflow,
							char(sum(downflow)) 			as downflow,
							char(sum(mms_busi_nums)) 	as mms_busi_nums,
							char(sum(accept_mail_nums)) as accept_mail_nums,
							char(sum(send_mail_nums)) 	as send_mail_nums,
							char(sum(wap_busi_nums)) 		as wap_busi_nums,
							char(sum(web_busi_nums)) 		as web_busi_nums,
							char(sum(ussd_counts)) 			as ussd_counts,
							char(sum(guoji_flows)) 			as guoji_flows,
							char(sum(guonei_flows)) 		as guonei_flows,
							char(sum(use_cust_nums)) 		as use_cust_nums,
							char(sum(bill_cust_nums)) 	as bill_cust_nums,
							char(sum(leiji_cust_nums)) 	as leiji_cust_nums,
							char(sum(nzt_phone_counts)) as nzt_phone_counts
						from session.tmp22303
					group by enterprise_id,ent_busi_id,manage_mod"
          
	          
		exec_sql $sql_buff
		puts "插入到目标表中bass1.G_S_22303_MONTH 各信息完成"	

   
    #剔除测试集团
    set sql_buff " delete from  BASS1.G_S_22303_MONTH where time_id = $op_month and enterprise_id in ('891910006274')"
          
    exec_sql $sql_buff      



	aidb_close $handle

	return 0
}

