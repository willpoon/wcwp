######################################################################################################
#接口名称：智能网VPMN业务日时段使用
#接口编码：21016
#接口说明：记录中国移动公司智能网VPMN用户日时段使用信息
#程序名称:  BASS1.g_s_21016_day_tmp.tcl
#功能描述: 生成21016的数据
#运行粒度: 日
#源    表：1.bass1.int_210012916_yyyymm
#          
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	        #当天 yyyy-mm-dd
	set optime $op_time
	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
        
  #上月字符串
	set last_month [GetLastMonth [string range $op_month 0 5]]
	#上上月字符串
	set last_last_month [GetLastMonth [string range $last_month 0 5]]
	#今天字符串
  set today_dd [format "%.0f" [string range $timestamp 6 7]]


        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.g_s_21016_day_tmp where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
       
       
    if {$today_dd > 12} {
      set TableName "bass2.stat_zd_village_users_$last_month" 
    }   else {
      set TableName "bass2.stat_zd_village_users_$last_last_month" 
    }    
      
       
        set handle [aidb_open $conn]

	set sql_buff "insert into BASS1.g_s_21016_day_tmp
                       (
                       time_id
                       ,bill_date
                       ,brand_id
                       ,callmoment_id
                       ,call_counts
                       ,base_bill_duration
                       ,toll_bill_duration
                       ,call_duration
                       ,favoured_basecall_fee
                       ,favoured_tollcall_fee
                       ,favoured_call_fee
                       ,region_flag
                       )
                      SELECT
	                $timestamp
	                ,'$timestamp'
	                ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2') 
	                ,CALLMOMENT_ID
	                ,char(bigint(sum(call_counts		)))     as call_counts
	                ,char(bigint(sum(base_bill_duration	)))     as base_bill_duration
	                ,char(bigint(sum(toll_bill_duration	)))     as toll_bill_duration
	                ,char(bigint(sum(call_duration		)))     as call_duration
	                ,char(bigint(sum(favoured_basecall_fee	)))     as favoured_basecall_fee
	                ,char(bigint(sum(favoured_tollcall_fee	)))     as favoured_tollcall_fee
	                ,char(bigint(sum(favoured_call_fee      )))     as favoured_call_fee
	                ,value(char(b.LOCNTYPE_ID),'2')
	                from
	                	bass1.int_210012916_${op_month} a,
                    $TableName b 
	                	
	                WHERE
	                	a.op_time=$timestamp         AND
                    a.user_id=b.user_id
	                	and svcitem_id in ('9901','9902','9903')
	                GROUP BY 
	                        COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2'),
	                        CALLMOMENT_ID,value(char(b.LOCNTYPE_ID),'2') "
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