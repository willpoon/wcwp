######################################################################################################
#接口名称：
#接口编码：
#接口说明：
#程序名称: INT_02011_SNAPSHOT.tcl
#功能描述: 生成02011的快照数据(在每天的下午18:00定时运行)
#运行粒度: 日
#源    表：1.BASS1.G_A_02011_DAY
#          
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 修改生成快照的代码
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #set op_time 2009-02-05
        puts $op_time
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        
        puts $timestamp
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        puts $last_day
        
  #删除本期数据
  set handle [aidb_open $conn]
	set sql_buff "\
	 alter table bass1.int_02011_snapshot activate not logged initially with empty table"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#从bass.g_a_02011_day表提取快照数据插入
        set handle [aidb_open $conn]
##	set sql_buff "insert into bass1.int_02011_snapshot
##                           select
##                             a.busi_code
##                             ,a.user_id
##                             ,a.valid_date
##                             ,a.invalid_date
##                           from 
##                             bass1.g_a_02011_day a,
##                             (select max(time_id) as time_id,busi_code,user_id from bass1.g_a_02011_day
##                              where time_id<=$timestamp and bigint(invalid_date)>$timestamp
##                              group by busi_code,user_id
##                             )b                           
##                           where a.time_id=b.time_id and a.user_id=b.user_id and a.busi_code=b.busi_code "
	
	set sql_buff "insert into bass1.int_02011_snapshot
                           select
                              a.busi_code
                             ,a.user_id
                             ,a.valid_date
                             ,a.invalid_date
from ( select   busi_code    
	,user_id      
	,valid_date   
	,invalid_date 
,row_number() over(partition by busi_code,user_id order by time_id desc ) row_id
                                  from bass1.g_a_02011_day 
                                  where time_id<=$timestamp ) a  
                           where a.row_id=1 
                            "

        puts $sql_buff      
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn 
	aidb_close $handle


  #-------------------------------------------------------
  #下午6点执行，对波动性调度报错进行恢复
  #更新告警状态
  set handle [aidb_open $conn]
	set sql_buff "update app.sch_control_alarm set flag=1 where control_code='BASS1_INT_CHECK_INDEX_WAVE_DAY.tcl'"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  #更新运行日志状态
  set handle [aidb_open $conn]
	set sql_buff "update app.sch_control_runlog set flag=0 where control_code='BASS1_INT_CHECK_INDEX_WAVE_DAY.tcl'"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



	
	return 0
}	