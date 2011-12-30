######################################################################################################
#接口名称：地市公司运营机构
#接口编码：06032
#接口说明：记录中国移动地市运营公司的代码和描述信息，仅包括截止当日有效的地市运营机构。
#程序名称: G_I_06032_DAY.tcl
#功能描述: 生成06032的数据
#运行粒度: 日
#源    表：1.bass2.DWD_PS_NET_NUMBER_YYYYMMDD

#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2009-11-23
#问题记录：
#修改历史: 
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #当天 yyyy-mm-dd
  set optime $op_time
        
  #删除本期数据
	set sql_buff "delete from bass1.g_i_06032_day where time_id=$timestamp"
	exec_sql $sql_buff
       
	set sql_buff "insert into bass1.g_i_06032_day
                     select
                        distinct
                        $timestamp
                        ,case when region_code = '891' then '13101'
                              when region_code = '892' then '13102'
                              when region_code = '893' then '13103'
                              when region_code = '894' then '13104'
                              when region_code = '895' then '13105'
                              when region_code = '896' then '13106'
                              when region_code = '897' then '13107'
                        end
                        ,case when region_code = '891' then '西藏移动通信有限责任公司拉萨分公司'
                              when region_code = '892' then '西藏移动通信有限责任公司日喀则分公司'
                              when region_code = '893' then '西藏移动通信有限责任公司山南分公司'
                              when region_code = '894' then '西藏移动通信有限责任公司林芝分公司'
                              when region_code = '895' then '西藏移动通信有限责任公司昌都分公司'
                              when region_code = '896' then '西藏移动通信有限责任公司那曲分公司'
                              when region_code = '897' then '西藏移动通信有限责任公司阿里分公司'
                        end
                        ,case when region_code = '891' then '拉萨'
                              when region_code = '892' then '日喀则'
                              when region_code = '893' then '山南'
                              when region_code = '894' then '林芝'
                              when region_code = '895' then '昌都'
                              when region_code = '896' then '那曲'
                              when region_code = '897' then '阿里'
                        end
                      from
                        bass2.DWD_PS_NET_NUMBER_$timestamp
                      where
                        region_code in ('891','892','893','894','895','896','897') and length(number_segment) = 7 "
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
	
	puts $sql_buff 
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
	
	
	puts $sql_buff 
	return $result
}
#--------------------------------------------------------------------------------------------------------------

