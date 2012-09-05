######################################################################################################
#接口名称：移动400业务订购情况
#接口编码：02064
#接口说明：
#程序名称: G_A_02064_DAY.tcl
#功能描述: 生成02064的数据
#运行粒度: 日
#源    表：
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2009-11-23
#问题记录：1.
#修改历史: 1.modify 具体业务逻辑 20100423
#修改历史: 2011-06-30 12:23:04 修改管理模式为3
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #当天 yyyy-mm-dd
        set optime $op_time


	set sql_buff "delete from BASS1.G_A_02064_DAY where TIME_ID < 20120801"
	exec_sql $sql_buff


        #删除本期数据
	set sql_buff "delete from BASS1.G_A_02064_DAY where TIME_ID=$timestamp"
	exec_sql $sql_buff


#	set sql_buff "insert into BASS1.G_A_02064_DAY
#			(
#				TIME_ID,
#				ENTERPRISE_ID,
#				ENTERPRISE_BUSI_TYPE,
#				MANAGE_MODE,
#				CDR_NUM_CNT,
#				SMS_NUM_CNT,
#				NUM_ALL_CNT,
#				NUM_MOBILE_CNT,
#				NUM_OTHER_CNT,
#				ORDER_DATE,
#				STATUS_ID
#			)
#			select distinct
#				${timestamp},
#				A.enterprise_id,
#				'1520' as enterprise_busi_type,
#				case
#					when upper(B.config_value)='MAS' then '1'
#					when upper(B.config_value)='ADC' then '2'
#					else '3'
#				end as manage_mode,
#				char(value(c.cdr_num_cnt,0)) as cdr_num_cnt,
#				char(value(d.sms_num_cnt,0)) as sms_num_cnt,
#				char(value(e.num_all_cnt,0)) as num_all_cnt,
#				char(value(e.num_all_cnt,0)) as num_mobile_cnt,
#				'0',
#				replace(char(date(A.done_date)),'-','') as order_date,
#				case
#					when rec_status=1 then '1'
#					when rec_status=0 then '2'
#				else '2'
#				end as status_id
#			from (select * from bass2.dw_enterprise_sub_ds where service_id in ('931')) a
#			inner join (select * from bass2.dim_service_config where config_id=1000027)  b on a.service_id=b.service_id
#			left join (select group_id,count(*) cdr_num_cnt from bass2.dwd_group_order_featur_${timestamp} where feature_id='803601002' and date(done_date)='${op_time}' group by group_id) C on a.enterprise_id=c.group_id
#			left join (select group_id,count(*) sms_num_cnt from bass2.dwd_group_order_featur_${timestamp} where feature_id='803606001' and date(done_date)='${op_time}' group by group_id) D on a.enterprise_id=D.group_id
#			left join (select group_id,sum((length(feature_value)-length(replace(feature_value,';','')))/2*length(';')) num_all_cnt from bass2.dwd_group_order_featur_${timestamp} where feature_id='803600014' and date(done_date)='${op_time}' group by group_id) e on a.enterprise_id=e.group_id
#		   where DATE(A.done_date)='${op_time}'
#			 and a.enterprise_id not in ('891910006274','891910006688','891910006714','891910006932')
#			"
#

	##~   set sql_buff "insert into BASS1.G_A_02064_DAY
			##~   (
				##~   TIME_ID,
				##~   ENTERPRISE_ID,
				##~   ENTERPRISE_BUSI_TYPE,
				##~   MANAGE_MODE,
				##~   CDR_NUM_CNT,
				##~   SMS_NUM_CNT,
				##~   NUM_ALL_CNT,
				##~   NUM_MOBILE_CNT,
				##~   NUM_OTHER_CNT,
				##~   ORDER_DATE,
				##~   STATUS_ID
			##~   )
			##~   select distinct
				##~   ${timestamp},
				##~   A.enterprise_id,
				##~   '1520' as enterprise_busi_type,
				##~   '3' manage_mode,
				##~   char(value(c.cdr_num_cnt,0)) as cdr_num_cnt,
				##~   char(value(d.sms_num_cnt,0)) as sms_num_cnt,
				##~   char(value(e.num_all_cnt,0)) as num_all_cnt,
				##~   char(value(e.num_all_cnt,0)) as num_mobile_cnt,
				##~   '0',
				##~   replace(char(date(A.done_date)),'-','') as order_date,
				##~   case
					##~   when rec_status=1 then '1'
					##~   when rec_status=0 then '2'
				##~   else '2'
				##~   end as status_id
			##~   from (select * from bass2.dw_enterprise_sub_ds where service_id in ('931')) a
			##~   inner join (select * from bass2.dim_service_config where config_id=1000027)  b on a.service_id=b.service_id
			##~   left join (select group_id,count(*) cdr_num_cnt from bass2.dwd_group_order_featur_${timestamp} where feature_id='803601002' and date(done_date)='${op_time}' group by group_id) C on a.enterprise_id=c.group_id
			##~   left join (select group_id,count(*) sms_num_cnt from bass2.dwd_group_order_featur_${timestamp} where feature_id='803606001' and date(done_date)='${op_time}' group by group_id) D on a.enterprise_id=D.group_id
			##~   left join (select group_id,sum((length(feature_value)-length(replace(feature_value,';','')))/2*length(';')) num_all_cnt from bass2.dwd_group_order_featur_${timestamp} where feature_id='803600014' and date(done_date)='${op_time}' group by group_id) e on a.enterprise_id=e.group_id
		   ##~   where DATE(A.done_date)='${op_time}'
			 ##~   and a.enterprise_id not in ('891910006274','891910006688','891910006714','891910006932')
			##~   "
			
  ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2006
		##~   puts $errmsg
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle
	
	
  ##~   #保证主键唯一性，删除同一天订购又退订的数据
  ##~   set handle [aidb_open $conn]
	##~   set sql_buff "delete  from bass1.G_A_02064_DAY a where exists 
								 ##~   (
								##~   select * from 
									##~   (
								##~   select time_id,enterprise_id,enterprise_busi_type,manage_mode,order_date,count(*)
								##~   from bass1.G_A_02064_DAY
								##~   group by time_id,enterprise_id,enterprise_busi_type,manage_mode,order_date
								##~   having count(*)>1
								##~   ) as b
								##~   where a.time_id = b.time_id
								  ##~   and a.enterprise_id = b.enterprise_id
								  ##~   and a.enterprise_busi_type = b.enterprise_busi_type
								  ##~   and a.manage_mode = b.manage_mode
								  ##~   and a.order_date = b.order_date
								  ##~   and a.time_id=$timestamp
								 ##~   )"
	##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2020
		##~   puts $errmsg
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle		

#2011-06-30 11:28:55
#一次性修复
##移动400
#
#       set sql_buff "
#       insert into G_A_02064_DAY select * from G_A_02064_DAY_FIX
#       "
#       exec_sql $sql_buff
#


# 2011-08-12 
#剔除 1520 测试集团
#
#     set sql_buff "
#     insert into G_A_02064_DAY select * from G_A_02064_DAY_FIX20110812
#     "
#     exec_sql $sql_buff
#     

##~   1.8.2 重新定口径！

 ##~   set sql_buff "
##~   insert into G_A_02064_DAY
##~   select 
         ##~   $timestamp TIME_ID
		##~   ,CUST_ID ENTERPRISE_ID
		##~   ,PRODUCT_NO NUMBER400
        ##~   ,'1' BIND_CNT
        ##~   ,'1' BIND_CMC_CNT
        ##~   ,'0' BIND_OTH_CNT
        ##~   ,replace(char(date(CREATE_DATE)),'-','') OPEN_DT
        ##~   ,'0' IF_ORD_CALLSHIELD
        ##~   ,'0' IF_ORD_CALLLIMIT
        ##~   ,'0' IF_ORD_PSWDACCESS
        ##~   ,'0' IF_ORD_BLACKLIST
        ##~   ,'0' IF_ORD_SMS
		##~   ,case when date(EXPIRE_DATE) <= '$op_time'  then '2' else '1' end ORD_STS
##~   from bass2.dw_product_$timestamp
##~   where USERTYPE_ID = 8 and product_no like '4001%'
##~   with ur
##~   "


##~   exec_sql $sql_buff


source /bassapp/bass1/tcl/INT_FIX_TMP.tcl
Deal_fix02064 $op_time $optime_month

	return 0
}