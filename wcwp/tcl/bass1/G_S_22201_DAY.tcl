######################################################################################################
#接口名称：使用TD网络的客户日汇总
#接口编码：22201
#接口说明：记录每日使用TD网络的客户汇总信息，以及TD专用号段的客户汇总信息
#程序名称: G_S_22201_DAY.tcl
#功能描述: 生成22201的数据
#运行粒度: 日
#源    表：1.bass2.dw_product_td_yyyymmdd
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-04-28
#问题记录：MTL_TD_USAGE_MARK char(10), MTL_TD_DATACARD_MARK char(10)
#修改历史: liuzhilong  20090629 根据1.6.0修改"使用TD网络的手机客户数","使用TD网络的数据卡客户数","两网融合的数据卡客户数"字段出数逻辑 去掉上网本客户
#          增加 6个字段 "使用TD网络的上网本客户数","使用TD网络的信息机客户数","当月累计使用TD网络的信息机客户数","当日新增的信息机客户数","当月累计新增的信息机客户数","信息机客户到达数"
#           liuzhilong 20090702 去掉最后三个字段
#          2009-09-18 修改“使用TD网络的手机客户数”业务口径，满足TD日校验
#          2009-12-22 修改：TD专用号段客户数-统计周期内专用号段的在网客户。目前专用号段为188号段。陈阳邮件通知。
#          2010-01-13 当月累计使用TD网络的手机客户数 口径修改，通过核查集团的调查发现此问题,累计指标和当日指标口径一致
#          2010-01-20 修改客户到达数据、在网客户数的口径 userstatus_id in (1,2,3,6,8)
#          1.6.5规范 剔除 共模终端的专用号段147345—147349数据
#          2011.12.27 由于  取 188 没有排除 2G数据卡，导致R111：bigint(TD_MOBILE_CUSTOMER_CNT)-bigint(TD_188_CNT)-bigint(TD_157_CNT)-bigint(TD_GSM_CNT)不通过；补剔数据卡。 LINE：88   and td_2gcard_mark=0
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        
        #删除本期数据
	set sql_buff "delete from bass1.g_s_22201_day where time_id=$timestamp"
	puts $sql_buff
  exec_sql $sql_buff
       

  #插入 新增客户数  
	set sql_buff "insert into BASS1.G_S_22201_DAY values
                 ($timestamp,           
                 '$timestamp',          
                (
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  
),
(  
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8) 
  and usertype_id in (1,2,9)
  and td_3gcard_mark=0
  and td_2gcard_mark=0
  and test_mark=0
  and not ((td_3gbook_mark = 1 and td_gprs_mark=1) and td_call_mark = 0 and td_addon_mark = 0)
  
),  
( 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and td_video_mark =1
  and td_3gcard_mark=0
  and td_2gcard_mark=0
  and test_mark=0
  and td_3gbook_mark=0  
  
 ),  
(  
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8) 
  and usertype_id in (1,2,9)
  and td_3gcard_mark=0
  and td_2gcard_mark=0
  and product_no like '188%'
  and test_mark=0
  and not ((td_3gbook_mark = 1 and td_gprs_mark=1) and td_call_mark = 0 and td_addon_mark = 0) 
  
 ) ,  
( 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and td_3gcard_mark=0
  and product_no like '157%'
  and test_mark=0
  and not ((td_3gbook_mark = 1 and td_gprs_mark=1) and td_call_mark = 0 and td_addon_mark = 0)  
  
) ,  
( 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8) 
  and usertype_id in (1,2,9)   
  and td_3gcard_mark =0
  and td_2gcard_mark=0
  and product_no not like '157%'
  and product_no not like '188%'
  and test_mark=0
  and not ((td_3gbook_mark = 1 and td_gprs_mark=1) and td_call_mark = 0 and td_addon_mark = 0)  
  
) ,  
( 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (((td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and td_3gcard_mark=1)
  or (td_gprs_mark=1 and td_2gcard_mark=1)) 
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and td_3gbook_mark=0
  
),  
(  
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and td_3gcard_mark=1
  and product_no like '157%'
  and test_mark=0
  and td_3gbook_mark=0  
  
),  
(  
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and td_3gcard_mark=1
  and product_no like '147%'  
  and test_mark=0
  and td_3gbook_mark=0  

),  
( 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where td_gprs_mark =1
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and td_3gcard_mark=0
  and td_2gcard_mark=1
  and test_mark=0
  and td_3gbook_mark = 0

),  
(select value(char(count(distinct user_id)),'0') 
from bass2.dw_product_td_$timestamp
where td_gprs_mark =1
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and td_3gbook_mark=1
  and bigint(product_no) not between 14734500000 and 14734999999
),
'0',
( 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and product_no like '188%'
  and test_mark=0
  
 ),  
(  

select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and product_no like '188%'
  and test_mark=0
  
),  
(   
 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and product_no like '147%'  
  and test_mark=0
  
  ),
 (select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where mtl_td_usage_mark=1
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and td_3gcard_mark=0
  and td_2gcard_mark=0
  and test_mark=0
  and not ((td_3gbook_mark = 1 and td_gprs_mark=1) and td_call_mark = 0 and td_addon_mark = 0)
  ) ,
  (select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where mtl_td_datacard_mark=1
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and td_3gbook_mark=0
  ) ,
  '0',
(select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and mtl_td_3gbook_mark=1
  and bigint(product_no) not between 14734500000 and 14734999999
)
  
)
with ur
;"
                        
  puts $sql_buff
  exec_sql $sql_buff
  
	#	#R111：  自动调R111校验
	#	set sql_buff "select bigint(TD_MOBILE_CUSTOMER_CNT)-bigint(TD_188_CNT)-bigint(TD_157_CNT)-bigint(TD_GSM_CNT)
	#	from BASS1.G_S_22201_DAY
	#	where time_id=$timestamp
	#	with ur"
	#	
	#	#	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]
	#	set DEC_RESULT_VAL1 [get_single $sql_buff]
	#	puts $DEC_RESULT_VAL1
	#	
	#	
	#	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]!=0 } {
	#	set sql_buff "
				#update BASS1.G_S_22201_DAY
				#set TD_188_CNT =char( bigint(TD_188_CNT)+(${DEC_RESULT_VAL1} ))
				#where time_id=$timestamp
				#"
	#	exec_sql $sql_buff
	#	}
	#	
	
	return 0
}


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
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------



