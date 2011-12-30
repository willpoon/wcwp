######################################################################################################
#接口名称：普通短信业务日使用
#接口编码：21007
#接口说明：记录中国移动用户普通短信业务日使用记录，
#          包括全球通、神州行、动感地带用户产生的普通短信。
#          根据自身情况，尽量传下行话单
#程序名称: G_S_21007_DAY.tcl
#功能描述: 生成21007的数据
#运行粒度: 日
#源    表：1.bass1.INT_21007_YYYYMM
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
  #set op_time 2008-06-01    
	set optime $op_time
	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]

        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_21007_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
            
        set handle [aidb_open $conn]

	set sql_buff "insert into bass1.g_s_21007_day
            (
               TIME_ID                         
              ,BILL_DATE                       
              ,BRND_ID                         
              ,SVC_TYPE_ID                     
              ,CDR_TYPE_ID                     
              ,END_STATUS                      
              ,ADVERSARY_ID                    
              ,SMS_COUNTS                      
              ,SMS_BASE_FEE                    
              ,SMS_INFO_FEE                    
              ,SMS_MONTH_FEE  
              )                 
              select
		$timestamp
		,'$timestamp'
		,brand_id
		,svc_type_id
		,cdr_type_id
		,end_status
		,adversary_id
		,char(int(sum(sms_counts )  ))
		,char(int(sum(sms_base_fee) ))
		,char(int(sum(sms_info_fee) ))
		,char(int(sum(sms_month_fee)))
              from
	        bass1.int_21007_$op_month
	      where	
	        op_time=$timestamp
		 and  SVC_TYPE_ID in ('11','12','13','70')
	      group by	        
	        brand_id
	        ,svc_type_id
	        ,cdr_type_id
	        ,end_status
	        ,adversary_id"
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
