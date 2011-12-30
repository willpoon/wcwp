######################################################################################################
#接口名称：GSM/TD普通语音业务月使用
#接口编码：21003
#接口说明:
#程序名称: G_S_21003_MONTH.tcl
#功能描述: 生成21003的数据
#运行粒度: 月
#源    表：1.bass1.g_s_21003_to_day(21003日接口)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
       
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_21003_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "Error: SQL query failed! The error message is:\n	$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
       
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21003_month
                    select
                      ${op_month}
                      ,'${op_month}'
		      ,product_no
		      ,brand_id
		      ,svc_type_id
		      ,toll_type_id
		      ,ip_type_id
		      ,adversary_id
		      ,roam_type_id
		      ,call_type_id
                      ,char(sum(bigint(call_counts            )))
                      ,char(sum(bigint(base_bill_duration     )))
                      ,char(sum(bigint(toll_bill_duration     )))
                      ,char(sum(bigint(call_duration          )))
                      ,char(sum(bigint(base_call_fee          )))
                      ,char(sum(bigint(toll_call_fee          )))
                      ,char(sum(bigint(callfw_toll_fee        )))
                      ,char(sum(bigint(call_fee               )))
                      ,char(sum(bigint(favoured_basecall_fee  )))
                      ,char(sum(bigint(favoured_tollcall_fee  )))
                      ,char(sum(bigint(favoured_callfw_tollfee)))
                      ,char(sum(bigint(favoured_call_fee      )))
                      ,char(sum(bigint(free_duration          )))
                      ,char(sum(bigint(favour_duration        )))   
                      ,MNS_TYPE
	            from
	              bass1.g_s_21003_to_day
		    where
                      time_id/100=$op_month
                    group by
		      product_no
		      ,brand_id
		      ,svc_type_id
		      ,toll_type_id
		      ,ip_type_id
		      ,adversary_id
		      ,roam_type_id
		      ,call_type_id
		      ,MNS_TYPE"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "Error: SQL query failed! The error message is:\n	$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}      
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "
  update bass1.G_S_21003_MONTH
  set ip_type_id='9000'
     where ip_type_id not in ('1000', '2000', '2100', '2101', '2102', '2199', '2200', '2201', '2202', 
   '2203', '2204', '2299', '3000', '3100', '3101', '3102', '3103', '3104', 
   '3105', '3106', '3107', '3108', '3109', '3110', '3111', '3112', '3113', 
   '3114', '3199', '3200', '4000', '4100', '4101', '4102', '4103', '4104', 
   '4105', '4106', '4107', '4108', '4109', '4110', '4111', '4199', '4200', 
   '6000', '6100', '6101', '6102', '6103', '6104', '6105', '6106', '6107', 
   '6199', '6200', '9000' )
    and time_id=$op_month "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "Error: SQL query failed! The error message is:\n	$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}      
	aidb_commit $conn
	aidb_close $handle





	return 0
}