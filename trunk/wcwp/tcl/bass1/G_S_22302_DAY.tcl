#纳入ADC平台管理的集团产品业务量日汇总
#接口编码：22302
#接口说明：
#程序名称: G_S_22302_DAY.tcl
#功能描述: 生成22302的数据
#运行粒度: 日
#源    表：
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhaolp2
#编写时间：2009-07-06
#问题记录：1.
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #当天 yyyy-mm-dd
        set optime $op_time
        
        set year [string range $op_time 0 3]


        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_S_22302_DAY where TIME_ID=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn	
	
  #01:创建临时表1(装载数据)
  set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.G_S_22302_DAY_tmp1
	              (
									enterprise_id			char(20) not null,
									ent_busi_type		  char(4)  not null,
									bill_duration_m		bigint,
									mo_sms_counts			bigint,
									mt_sms_counts			bigint,
									mms_counts				bigint,
									gprs_flow					bigint,
									wap_counts				bigint,
									web_counts				bigint,
									ussd_counts				bigint,
									email_counts			bigint,
									use_numbers				bigint,
									bill_numbers			bigint,
									use_total_numbers	bigint
	              )
	              partitioning key
	              (enterprise_id,ent_busi_type)
	              using hashing	              
	              with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle   	

	
  #02:创建临时表2
  set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.G_S_22302_DAY_tmp2
	              (
									user_id						char(20) not null,
									ent_busi_type		  char(4)  not null,
									bill_duration_m		bigint,
									mo_sms_counts			bigint,
									mt_sms_counts			bigint,
									mms_counts				bigint,
									gprs_flow					bigint,
									wap_counts				bigint,
									web_counts				bigint,
									ussd_counts				bigint,
									email_counts			bigint,
									use_numbers				bigint,
									bill_numbers			bigint,
									use_total_numbers	bigint
	              )
	              partitioning key
	              (user_id,ent_busi_type)
	              using hashing	              
	              with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle  

set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22302_DAY_tmp1
			(
				 ENTERPRISE_ID
				,ENT_BUSI_TYPE
				,BILL_DURATION_M
				,MO_SMS_COUNTS
				,MT_SMS_COUNTS
				,MMS_COUNTS
				,GPRS_FLOW
				,WAP_COUNTS
				,WEB_COUNTS
				,USSD_COUNTS
				,EMAIL_COUNTS
				,USE_NUMBERS
				,BILL_NUMBERS
				,USE_TOTAL_NUMBERS
			)
			select distinct
				A.ENTERPRISE_ID,
				C.BASS1_VALUE as ENT_BUSI_TYPE,
				0 as BILL_DURATION_M,
				0 as MO_SMS_COUNTS,
				0 as MT_SMS_COUNTS,
				0 as MMS_COUNTS,
				0 as GPRS_FLOW,
				0 as WAP_COUNTS,
				0 as WEB_COUNTS,
				0 as USSD_COUNTS,
				0 as EMAIL_COUNTS,
				0 as USE_NUMBERS,
				0 as BILL_NUMBERS,
				0 as USE_TOTAL_NUMBERS
			from
				bass2.DW_ENTERPRISE_EXTSUB_RELA_DS a,
				(select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027 and upper(CONFIG_VALUE)='ADC')  b,
				(select * from bass1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c
			where
				A.SERVICE_ID=B.SERVICE_ID
				and A.SERVICE_ID=C.XZBAS_VALUE"

        puts $sql_buff
        
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

###
###903	移动OA（ADC）     1250
###904	移动进销存（ADC） 1260
###906	移动CRM（ADC）    1280
###909	ADC服务器
###910	企信通(ADC)       1330
###924	企业建站（ADC）   1340
###925	企业邮箱（ADC）   1210
###926	手机邮箱（ADC）   1220
###952	医信通(中央ADC)
###953	农信通(中央ADC)   1320
	
	#统计计费时长
	set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22302_DAY_tmp2
			(
				 user_id
				,ENT_BUSI_TYPE
				,BILL_DURATION_M
				,MO_SMS_COUNTS
				,MT_SMS_COUNTS
				,MMS_COUNTS
				,GPRS_FLOW
				,WAP_COUNTS
				,WEB_COUNTS
				,USSD_COUNTS
				,EMAIL_COUNTS
				,USE_NUMBERS
				,BILL_NUMBERS
				,USE_TOTAL_NUMBERS
			)
			select
				b.user_id,
				case when c.service_id='903' then '1250' 
				     when c.service_id='904' then '1260'
				     when c.service_id='906' then '1280'
				     when c.service_id='910' then '1330'
				     when c.service_id='924' then '1340'
				     when c.service_id='925' then '1210'
				     when c.service_id='926' then '1220'
				     when c.service_id='953' then '1320'
				end as ENT_BUSI_TYPE,
				sum(call_duration_m) as BILL_DURATION_M,
				0 as MO_SMS_COUNTS,
				0 as MT_SMS_COUNTS,
				0 as MMS_COUNTS,
				0 as GPRS_FLOW,
				0 as WAP_COUNTS,
				0 as WEB_COUNTS,
				0 as USSD_COUNTS,
				0 as EMAIL_COUNTS,
				0 as USE_NUMBERS,
				0 as BILL_NUMBERS,
				0 as USE_TOTAL_NUMBERS
			from bass2.dw_call_$timestamp b,
					 (SELECT service_id,int(plan_id) plan_id FROM bass2.DIM_ENT_GROUP_PLAN
							where service_id in ('903','904','906','910','924','925','926','953')
						) c
			where b.plan_id = c.plan_id
		group by b.user_id,
		case when c.service_id='903' then '1250' 
				     when c.service_id='904' then '1260'
				     when c.service_id='906' then '1280'
				     when c.service_id='910' then '1330'
				     when c.service_id='924' then '1340'
				     when c.service_id='925' then '1210'
				     when c.service_id='926' then '1220'
				     when c.service_id='953' then '1320'
				end"

    puts $sql_buff
        

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	



	
	#统计上行、下行短信条数
	set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22302_DAY_tmp2
			(
				 user_id
				,ENT_BUSI_TYPE
				,BILL_DURATION_M
				,MO_SMS_COUNTS
				,MT_SMS_COUNTS
				,MMS_COUNTS
				,GPRS_FLOW
				,WAP_COUNTS
				,WEB_COUNTS
				,USSD_COUNTS
				,EMAIL_COUNTS
				,USE_NUMBERS
				,BILL_NUMBERS
				,USE_TOTAL_NUMBERS
			)
			select
				b.user_id,
				case when c.service_id='903' then '1250' 
				     when c.service_id='904' then '1260'
				     when c.service_id='906' then '1280'
				     when c.service_id='910' then '1330'
				     when c.service_id='924' then '1340'
				     when c.service_id='925' then '1210'
				     when c.service_id='926' then '1220'
				     when c.service_id='953' then '1320'
				end  as ENT_BUSI_TYPE,
				0 as BILL_DURATION_M,
				sum(case when calltype_id=0 then counts else 0 end) as MO_SMS_COUNTS,
				sum(case when calltype_id=1 then counts else 0 end) as MT_SMS_COUNTS,
				0 as MMS_COUNTS,
				0 as GPRS_FLOW,
				0 as WAP_COUNTS,
				0 as WEB_COUNTS,
				0 as USSD_COUNTS,
				0 as EMAIL_COUNTS,
				0 as USE_NUMBERS,
				0 as BILL_NUMBERS,
				0 as USE_TOTAL_NUMBERS
			from bass2.dw_newbusi_ismg_$timestamp b,
					 (SELECT service_id,int(plan_id) plan_id FROM bass2.DIM_ENT_GROUP_PLAN
							where service_id in ('903','904','906','910','924','925','926','953')
						) c
			where b.plan_id = c.plan_id
		group by b.user_id,
		case when c.service_id='903' then '1250' 
				     when c.service_id='904' then '1260'
				     when c.service_id='906' then '1280'
				     when c.service_id='910' then '1330'
				     when c.service_id='924' then '1340'
				     when c.service_id='925' then '1210'
				     when c.service_id='926' then '1220'
				     when c.service_id='953' then '1320'
				end"

    puts $sql_buff
        

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	
	
	#统计彩信信息条数
	set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22302_DAY_tmp2
			(
				 user_id
				,ENT_BUSI_TYPE
				,BILL_DURATION_M
				,MO_SMS_COUNTS
				,MT_SMS_COUNTS
				,MMS_COUNTS
				,GPRS_FLOW
				,WAP_COUNTS
				,WEB_COUNTS
				,USSD_COUNTS
				,EMAIL_COUNTS
				,USE_NUMBERS
				,BILL_NUMBERS
				,USE_TOTAL_NUMBERS
			)
			select
				b.user_id,
				case when c.service_id='903' then '1250' 
				     when c.service_id='904' then '1260'
				     when c.service_id='906' then '1280'
				     when c.service_id='910' then '1330'
				     when c.service_id='924' then '1340'
				     when c.service_id='925' then '1210'
				     when c.service_id='926' then '1220'
				     when c.service_id='953' then '1320'
				end as ENT_BUSI_TYPE,
				0 as BILL_DURATION_M,
				0 as MO_SMS_COUNTS,
				0 as MT_SMS_COUNTS,
				sum(counts) as MMS_COUNTS,
				0 as GPRS_FLOW,
				0 as WAP_COUNTS,
				0 as WEB_COUNTS,
				0 as USSD_COUNTS,
				0 as EMAIL_COUNTS,
				0 as USE_NUMBERS,
				0 as BILL_NUMBERS,
				0 as USE_TOTAL_NUMBERS
			from bass2.dw_newbusi_mms_$timestamp b,
					 (SELECT service_id,int(plan_id) plan_id FROM bass2.DIM_ENT_GROUP_PLAN
							where service_id in ('903','904','906','910','924','925','926','953')
						) c
			where b.plan_id = c.plan_id
		group by b.user_id,
		case when c.service_id='903' then '1250' 
				     when c.service_id='904' then '1260'
				     when c.service_id='906' then '1280'
				     when c.service_id='910' then '1330'
				     when c.service_id='924' then '1340'
				     when c.service_id='925' then '1210'
				     when c.service_id='926' then '1220'
				     when c.service_id='953' then '1320'
				end"

    puts $sql_buff
        

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	

	#统计GPRS流量
	set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22302_DAY_tmp2
			(
				 user_id
				,ENT_BUSI_TYPE
				,BILL_DURATION_M
				,MO_SMS_COUNTS
				,MT_SMS_COUNTS
				,MMS_COUNTS
				,GPRS_FLOW
				,WAP_COUNTS
				,WEB_COUNTS
				,USSD_COUNTS
				,EMAIL_COUNTS
				,USE_NUMBERS
				,BILL_NUMBERS
				,USE_TOTAL_NUMBERS
			)
			select
				b.user_id,
				case when c.service_id='903' then '1250' 
				     when c.service_id='904' then '1260'
				     when c.service_id='906' then '1280'
				     when c.service_id='910' then '1330'
				     when c.service_id='924' then '1340'
				     when c.service_id='925' then '1210'
				     when c.service_id='926' then '1220'
				     when c.service_id='953' then '1320'
				end as ENT_BUSI_TYPE,
				0 as BILL_DURATION_M,
				0 as MO_SMS_COUNTS,
				0 as MT_SMS_COUNTS,
				0 as MMS_COUNTS,
				sum(rating_res) as GPRS_FLOW,
				0 as WAP_COUNTS,
				0 as WEB_COUNTS,
				0 as USSD_COUNTS,
				0 as EMAIL_COUNTS,
				0 as USE_NUMBERS,
				0 as BILL_NUMBERS,
				0 as USE_TOTAL_NUMBERS
			from bass2.dw_newbusi_gprs_$timestamp b,
					 (SELECT service_id,int(plan_id) plan_id FROM bass2.DIM_ENT_GROUP_PLAN
							where service_id in ('903','904','906','910','924','925','926','953')
						) c
			where b.plan_id = c.plan_id
		group by b.user_id,
		case when c.service_id='903' then '1250' 
				     when c.service_id='904' then '1260'
				     when c.service_id='906' then '1280'
				     when c.service_id='910' then '1330'
				     when c.service_id='924' then '1340'
				     when c.service_id='925' then '1210'
				     when c.service_id='926' then '1220'
				     when c.service_id='953' then '1320'
				end"

    puts $sql_buff
        

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	


	#统计WAP业务量
	set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22302_DAY_tmp2
			(
				 user_id
				,ENT_BUSI_TYPE
				,BILL_DURATION_M
				,MO_SMS_COUNTS
				,MT_SMS_COUNTS
				,MMS_COUNTS
				,GPRS_FLOW
				,WAP_COUNTS
				,WEB_COUNTS
				,USSD_COUNTS
				,EMAIL_COUNTS
				,USE_NUMBERS
				,BILL_NUMBERS
				,USE_TOTAL_NUMBERS
			)
			select
				b.user_id,
				case when c.service_id='903' then '1250' 
				     when c.service_id='904' then '1260'
				     when c.service_id='906' then '1280'
				     when c.service_id='910' then '1330'
				     when c.service_id='924' then '1340'
				     when c.service_id='925' then '1210'
				     when c.service_id='926' then '1220'
				     when c.service_id='953' then '1320'
				end as ENT_BUSI_TYPE,
				0 as BILL_DURATION_M,
				0 as MO_SMS_COUNTS,
				0 as MT_SMS_COUNTS,
				0 as MMS_COUNTS,
				0 as GPRS_FLOW,
				sum(counts) as WAP_COUNTS,
				0 as WEB_COUNTS,
				0 as USSD_COUNTS,
				0 as EMAIL_COUNTS,
				0 as USE_NUMBERS,
				0 as BILL_NUMBERS,
				0 as USE_TOTAL_NUMBERS
			from bass2.dw_newbusi_wap_$timestamp b,
					 (SELECT service_id,int(plan_id) plan_id FROM bass2.DIM_ENT_GROUP_PLAN
							where service_id in ('903','904','906','910','924','925','926','953')
						) c
			where b.plan_id = c.plan_id
		group by b.user_id,
		case when c.service_id='903' then '1250' 
				     when c.service_id='904' then '1260'
				     when c.service_id='906' then '1280'
				     when c.service_id='910' then '1330'
				     when c.service_id='924' then '1340'
				     when c.service_id='925' then '1210'
				     when c.service_id='926' then '1220'
				     when c.service_id='953' then '1320'
				end"

    puts $sql_buff
        

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
		

 #通过临时表2关联集团资料插入临时表1
 set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22302_DAY_tmp1
			(
				 ENTERPRISE_ID
				,ENT_BUSI_TYPE
				,BILL_DURATION_M
				,MO_SMS_COUNTS
				,MT_SMS_COUNTS
				,MMS_COUNTS
				,GPRS_FLOW
				,WAP_COUNTS
				,WEB_COUNTS
				,USSD_COUNTS
				,EMAIL_COUNTS
				,USE_NUMBERS
				,BILL_NUMBERS
				,USE_TOTAL_NUMBERS
			)
			select
				A.ENTERPRISE_ID,
				b.ent_busi_type as ENT_BUSI_TYPE,
				sum(BILL_DURATION_M) as BILL_DURATION_M,
				sum(MO_SMS_COUNTS) as MO_SMS_COUNTS,
				sum(MT_SMS_COUNTS) as MT_SMS_COUNTS,
				sum(MMS_COUNTS) as MMS_COUNTS,
				sum(GPRS_FLOW) as GPRS_FLOW,
				sum(WAP_COUNTS) as WAP_COUNTS,
				sum(WEB_COUNTS) as WEB_COUNTS,
				sum(USSD_COUNTS) as USSD_COUNTS,
				sum(EMAIL_COUNTS) as EMAIL_COUNTS,
				0 as USE_NUMBERS,
				0 as BILL_NUMBERS,
				0 as USE_TOTAL_NUMBERS
			from
				bass2.dw_enterprise_member_mid_$timestamp a,
				session.G_S_22302_DAY_tmp2 b
			where a.user_id = b.user_id
			group by a.enterprise_id,b.ent_busi_type
			"

      puts $sql_buff
        

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle




  #911 校迅通
  #953 农讯通
	#统计当日使用用户数
	set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22302_DAY_tmp1
			(
				 ENTERPRISE_ID
				,ENT_BUSI_TYPE
				,BILL_DURATION_M
				,MO_SMS_COUNTS
				,MT_SMS_COUNTS
				,MMS_COUNTS
				,GPRS_FLOW
				,WAP_COUNTS
				,WEB_COUNTS
				,USSD_COUNTS
				,EMAIL_COUNTS
				,USE_NUMBERS
				,BILL_NUMBERS
				,USE_TOTAL_NUMBERS
			)
			select
				a.ENTERPRISE_ID,
				case when c.service_id='953' then '1320'
				end as ENT_BUSI_TYPE,
				0 as BILL_DURATION_M,
				0 as MO_SMS_COUNTS,
				0 as MT_SMS_COUNTS,
				0 as MMS_COUNTS,
				0 as GPRS_FLOW,
				0 as WAP_COUNTS,
				0 as WEB_COUNTS,
				0 as USSD_COUNTS,
				0 as EMAIL_COUNTS,
				count(distinct b.user_id) as USE_NUMBERS,
				0 as BILL_NUMBERS,
				0 as USE_TOTAL_NUMBERS
			from bass2.dw_enterprise_member_mid_$timestamp a,
			     bass2.dw_newbusi_ismg_$timestamp b,
					 (SELECT service_id,int(plan_id) plan_id FROM bass2.DIM_ENT_GROUP_PLAN
							where service_id in ('953')
						) c
			where a.user_id = b.user_id
			  and b.plan_id = c.plan_id
		group by a.enterprise_id,
		case when c.service_id='953' then '1320'
				end"

    puts $sql_buff
        

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
		

	#统计当月计费用户数
	set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22302_DAY_tmp1
			(
				 ENTERPRISE_ID
				,ENT_BUSI_TYPE
				,BILL_DURATION_M
				,MO_SMS_COUNTS
				,MT_SMS_COUNTS
				,MMS_COUNTS
				,GPRS_FLOW
				,WAP_COUNTS
				,WEB_COUNTS
				,USSD_COUNTS
				,EMAIL_COUNTS
				,USE_NUMBERS
				,BILL_NUMBERS
				,USE_TOTAL_NUMBERS
			)
			select
				a.ENTERPRISE_ID,
				case when c.service_id='953' then '1320'
				end as ENT_BUSI_TYPE,
				0 as BILL_DURATION_M,
				0 as MO_SMS_COUNTS,
				0 as MT_SMS_COUNTS,
				0 as MMS_COUNTS,
				0 as GPRS_FLOW,
				0 as WAP_COUNTS,
				0 as WEB_COUNTS,
				0 as USSD_COUNTS,
				0 as EMAIL_COUNTS,
				0 as USE_NUMBERS,
				count(distinct b.user_id) as BILL_NUMBERS,
				0 as USE_TOTAL_NUMBERS
			from bass2.dw_enterprise_member_mid_$timestamp a,
			     bass2.dw_newbusi_ismg_dt b,
					 (SELECT service_id,int(plan_id) plan_id FROM bass2.DIM_ENT_GROUP_PLAN
							where service_id in ('953')
						) c
			where a.user_id = b.user_id
			  and b.plan_id = c.plan_id
			  and b.bill_counts > 0
		group by a.enterprise_id,
		case when c.service_id='953' then '1320'
				end"

    puts $sql_buff
        

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	#统计累计使用用户数，从自然年度开始统计
	set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22302_DAY_tmp1
			(
				 ENTERPRISE_ID
				,ENT_BUSI_TYPE
				,BILL_DURATION_M
				,MO_SMS_COUNTS
				,MT_SMS_COUNTS
				,MMS_COUNTS
				,GPRS_FLOW
				,WAP_COUNTS
				,WEB_COUNTS
				,USSD_COUNTS
				,EMAIL_COUNTS
				,USE_NUMBERS
				,BILL_NUMBERS
				,USE_TOTAL_NUMBERS
			)
			select
				a.ENTERPRISE_ID,
				case when a.apptype_id=1 then '1310' 
             when a.APPTYPE_ID=3 then '1320' end as ENT_BUSI_TYPE,
				0 as BILL_DURATION_M,
				0 as MO_SMS_COUNTS,
				0 as MT_SMS_COUNTS,
				0 as MMS_COUNTS,
				0 as GPRS_FLOW,
				0 as WAP_COUNTS,
				0 as WEB_COUNTS,
				0 as USSD_COUNTS,
				0 as EMAIL_COUNTS,
				0 as USE_NUMBERS,
				0 as BILL_NUMBERS,
				count(distinct a.user_id) as USE_TOTAL_NUMBERS
			from 
			(
				 select enterprise_id,user_id,city_id,apptype_id
         from bass2.dw_enterprise_industry_apply
         where op_time between '$year-01-01' and '$optime'
           and apptype_id in (1,3)
         group by enterprise_id,user_id,city_id,apptype_id
         having count(distinct op_time) >= 3
         union all
         select enterprise_id,a.user_id,a.city_id,a.apptype_id
         from bass2.dw_enterprise_industry_apply a left outer join bass2.dw_product_$timestamp b
           on a.user_id = b.user_id
         where a.op_time between '$year-01-01' and '$optime'
           and a.apptype_id in (1,3)
         group by enterprise_id,a.user_id,a.city_id,a.apptype_id
       ) a 
      where a.enterprise_id<>''
		group by a.enterprise_id,
				case when a.apptype_id=1 then '1310' 
         when a.APPTYPE_ID=3 then '1320' end "

    puts $sql_buff
        

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



	#插入目标表
	set handle [aidb_open $conn]
	set sql_buff "insert into BASS1.G_S_22302_DAY
			( 
			   TIME_ID
				,ENTERPRISE_ID
				,ENT_BUSI_TYPE
				,BILL_DURATION_M
				,MO_SMS_COUNTS
				,MT_SMS_COUNTS
				,MMS_COUNTS
				,GPRS_FLOW
				,WAP_COUNTS
				,WEB_COUNTS
				,USSD_COUNTS
				,EMAIL_COUNTS
				,USE_NUMBERS
				,BILL_NUMBERS
				,USE_TOTAL_NUMBERS
			)
			select
			  $timestamp,
				a.ENTERPRISE_ID,
				a.ENT_BUSI_TYPE,
				char(sum(a.BILL_DURATION_M)),
				char(sum(a.MO_SMS_COUNTS)),
				char(sum(a.MT_SMS_COUNTS)),
				char(sum(a.MMS_COUNTS)),
				char(sum(a.GPRS_FLOW)),
				char(sum(a.WAP_COUNTS)),
				char(sum(a.WEB_COUNTS)),
				char(sum(a.USSD_COUNTS)),
				char(sum(a.EMAIL_COUNTS)),
				char(sum(a.USE_NUMBERS)),
				char(sum(a.BILL_NUMBERS)),
				char(sum(a.USE_TOTAL_NUMBERS))
			from session.G_S_22302_DAY_tmp1 a
		group by a.ENTERPRISE_ID,a.ENT_BUSI_TYPE"

    puts $sql_buff
    puts "sucess!"
        

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	

	return 0
}
