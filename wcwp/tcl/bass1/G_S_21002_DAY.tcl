######################################################################################################
#接口名称：GSM普通语音业务日时段使用
#接口编码：21002
#接口说明：记录批价后的语音业务日时段服务使用信息（智能网客户服务使用记录除外），
#          其中包括中国移动客户通过手机拨打中国移动IP17951、17950服务使用记录，IP拨号上网卡，
#          和利用其他运营商IP系统产生的服务使用记录，还包括语音增值业务服务使用记录，但不包括漫游来访话单。
#程序名称:  G_S_21002_DAY.tcl
#功能描述: 生成2100的数据
#运行粒度: 日
#源    表：1.bass1.int_210012916_yyyymm
#          
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.增加区域标志字段@20070801 By tym
#修改历史: liuzhilong 20090911 去除where 条件  and DRTYPE_ID  not in (1700,9901,9902,9903)
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyy-mm-dd
	set optime $op_time
	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
	#上月  yyyymm
	set last_month [GetLastMonth [string range $op_month 0 5]]
	#上上月 yyyymm
	set last_last_month [GetLastMonth [string range $last_month 0 5]]
	#今天的日期，格式dd(例：输入20070411 返回11,输入20070708，返回8)
	set today_dd [format "%.0f" [string range $timestamp 6 7]]
	       
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_21002_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
       #增加区域标志字段@20070801 By tym
       if {$today_dd > 12} {
           set RegDatFrmMis "bass2.stat_zd_village_users_$last_month" 
       } else {
	   set RegDatFrmMis "bass2.stat_zd_village_users_$last_last_month" 
       }           
       
       
  puts $RegDatFrmMis     
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_21002_DAY
                      (
                       TIME_ID
                       ,BILL_DATE
                       ,BRAND_ID
                       ,CALL_MOMENT_ID
                       ,CALL_COUNTS
                       ,BASE_BILL_DURATION
                       ,TOLL_BILL_DURATION
                       ,CALL_DURATION
                       ,FAVOURED_BASECALL_FEE
                       ,FAVOURED_TOLLCALL_FEE
                       ,FAVOURED_CALL_FEE      
                      )
                      select
                         $timestamp
                      	,'$timestamp'
                      	,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(a.BRAND_ID)),'2') 
                      	,a.callmoment_id
                      	,char(bigint(sum(a.call_counts	          )))
                      	,char(bigint(sum(a.base_bill_duration     )))
                      	,char(bigint(sum(a.toll_bill_duration     )))
                      	,char(bigint(sum(a.call_duration	  )))
                      	,char(bigint(sum(a.favoured_basecall_fee  )))
                      	,char(bigint(sum(a.favoured_tollcall_fee  )))
                      	,char(bigint(sum(a.favoured_call_fee	  )))                      	
                      FROM
                        (
                         select 
                           * 
                         from 
                           bass1.int_210012916_$op_month
                         where 
                           op_time=$timestamp

                      	) a
                      left join 
                          $RegDatFrmMis  b
                      on 
                       a.user_id=b.user_id
                      GROUP BY
                        COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(a.brand_id)),'2') 
                      	,a.CALLMOMENT_ID "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}      
	aidb_commit $conn
	aidb_close $handle

	return 0
}
#####################################
#and svcitem_id not in ('1700','9901','9902','9903') 
#--屏蔽1700:娱音在线、音信互动业务 others:VPMN数据--

#原来程序备份 20070801
#       #当天 yyyy-mm-dd
#	set optime $op_time
#	#当天 yyyymmdd
#        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
#        #本月 yyyymm
#        set op_month [string range $op_time 0 3][string range $op_time 5 6]
#	       
#        #删除本期数据
#        set handle [aidb_open $conn]
#	set sql_buff "delete from bass1.g_s_21002_day where time_id=$timestamp"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#       
#       
#       
#       
#        set handle [aidb_open $conn]
#	set sql_buff "insert into bass1.G_S_21002_DAY
#                      (
#                       TIME_ID
#                       ,BILL_DATE
#                       ,BRAND_ID
#                       ,CALL_MOMENT_ID
#                       ,REGION_FLAG
#                       ,CALL_COUNTS
#                       ,BASE_BILL_DURATION
#                       ,TOLL_BILL_DURATION
#                       ,CALL_DURATION
#                       ,FAVOURED_BASECALL_FEE
#                       ,FAVOURED_TOLLCALL_FEE
#                       ,FAVOURED_CALL_FEE      
#                      )
#                      select
#                         $timestamp
#                      	,'$timestamp'
#                      	,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(a.BRAND_ID)),'2') 
#                      	,a.callmoment_id
#                      	,'2'
#                      	,char(bigint(sum(a.call_counts	          )))
#                      	,char(bigint(sum(a.base_bill_duration     )))
#                      	,char(bigint(sum(a.toll_bill_duration     )))
#                      	,char(bigint(sum(a.call_duration	  )))
#                      	,char(bigint(sum(a.favoured_basecall_fee  )))
#                      	,char(bigint(sum(a.favoured_tollcall_fee  )))
#                      	,char(bigint(sum(a.favoured_call_fee	  )))                      	
#                      FROM
#                         bass1.int_210012916_$op_month
#                      where 
#                         op_time=$timestamp
#                         and svcitem_id not in ('1700','9901','9902','9903')
#                      GROUP BY
#                        COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(a.brand_id)),'2') 
#                      	,a.CALLMOMENT_ID "
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}      
#	aidb_commit $conn
#	aidb_close $handle
#
#	return 0
####################################