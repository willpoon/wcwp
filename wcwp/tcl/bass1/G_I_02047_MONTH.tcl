######################################################################################################
#接口名称：用户使用终端通话情况
#接口编码：02047
#接口说明：记录每月的TD客户的汇总信息
#程序名称: G_I_02047_MONTH.tcl
#功能描述: 生成02047的数据
#运行粒度: 月
#源    表：1.BASS1.CDR_CALL_DM_MID
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-05-05
#问题记录：
#修改历史: 
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	      #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        
        #当天 yyyy-mm-dd
        set optime $op_time
        
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        puts $op_month
              
        #本月第一天 yyyy-mm-dd
        set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_first_day

        #本月最后一天 yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        puts $this_month_last_day
        
        #删除正式表本月数据
        set sql_buff "delete from bass1.g_i_02047_month where time_id=$op_month"
        puts $sql_buff
        exec_sql $sql_buff
       
        #清空中间表
        set sql_buff "ALTER TABLE bass1.CDR_CALL_DM_MID ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        puts $sql_buff
        exec_sql $sql_buff
          

        #插入中间表
        set sql_buff "insert into bass1.CDR_CALL_DM_MID
                       select user_id,product_no,imei,count(*) from bass1.CDR_CALL_DM
                        where time_id/100=$op_month
                       group by user_id,product_no,imei"
        puts $sql_buff
        exec_sql $sql_buff
          
          
        #清洗中间表
        set sql_buff "delete from bass1.CDR_CALL_DM_MID where imei is null"
        puts $sql_buff
        exec_sql $sql_buff
        
        
        #清洗中间表
        set sql_buff "delete from bass1.CDR_CALL_DM_MID
          where  (
                  (ascii(substr(imei,1 ,1)) between  65 and 90 or ascii(substr(imei,1 ,1)) between 97  and 122) or 
                  (ascii(substr(imei,2 ,1)) between  65 and 90 or ascii(substr(imei,2 ,1)) between 97  and 122) or 
                  (ascii(substr(imei,3 ,1)) between  65 and 90 or ascii(substr(imei,3 ,1)) between 97  and 122) or 
                  (ascii(substr(imei,4 ,1)) between  65 and 90 or ascii(substr(imei,4 ,1)) between 97  and 122) or 
                  (ascii(substr(imei,5 ,1)) between  65 and 90 or ascii(substr(imei,5 ,1)) between 97  and 122) or 
                  (ascii(substr(imei,6 ,1)) between  65 and 90 or ascii(substr(imei,6 ,1)) between 97  and 122) or 
                  (ascii(substr(imei,7 ,1)) between  65 and 90 or ascii(substr(imei,7 ,1)) between 97  and 122) or 
                  (ascii(substr(imei,8 ,1)) between  65 and 90 or ascii(substr(imei,8 ,1)) between 97  and 122) or 
                  (ascii(substr(imei,9 ,1)) between  65 and 90 or ascii(substr(imei,9 ,1)) between 97  and 122) or 
                  (ascii(substr(imei,10,1)) between  65 and 90 or ascii(substr(imei,10,1)) between 97  and 122) or 
                  (ascii(substr(imei,11,1)) between  65 and 90 or ascii(substr(imei,11,1)) between 97  and 122) or 
                  (ascii(substr(imei,12,1)) between  65 and 90 or ascii(substr(imei,12,1)) between 97  and 122) or 
                  (ascii(substr(imei,13,1)) between  65 and 90 or ascii(substr(imei,13,1)) between 97  and 122) or 
                  (ascii(substr(imei,14,1)) between  65 and 90 or ascii(substr(imei,14,1)) between 97  and 122) or 
                  (ascii(substr(imei,15,1)) between  65 and 90 or ascii(substr(imei,15,1)) between 97  and 122) 
                  )"
        puts $sql_buff
        exec_sql $sql_buff

        set sql_buff "delete from bass1.CDR_CALL_DM_MID 
          where 
           (   substr(imei,1,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9') 
           or substr(imei,2,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9')
           or substr(imei,3,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9')
           or substr(imei,4,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9')
           or substr(imei,5,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9')
           or substr(imei,6,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9')
           or substr(imei,7,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9')
           or substr(imei,8,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9')
           or substr(imei,9,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9')
           or substr(imei,10,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9')
           or substr(imei,11,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9')
           or substr(imei,12,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9')
           or substr(imei,13,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9')
           or substr(imei,14,1) not in ('0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9')        
           )
          and time_id=$op_month"
        puts $sql_buff
        exec_sql $sql_buff
        
        
        #往正式表插入本月数据
        set sql_buff "insert into bass1.G_I_02047_MONTH
                      select $op_month,user_id,product_no,imei,cnt from bass1.CDR_CALL_DM_MID"
        puts $sql_buff
        exec_sql $sql_buff


        #清空中间表
        set sql_buff "ALTER TABLE bass1.CDR_CALL_DM_MID ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        puts $sql_buff
        exec_sql $sql_buff
         
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



