######################################################################################################
#接口名称：地市公司与MSISDN号段的对应关系
#接口编码：06031
#接口说明：记录中国移动地市运营公司与MSISDN号段的对应关系。该接口只报当前有效地市分公司和号段的对应关系，已取消的地市公司则不再上报。
#程序名称: G_I_06031_DAY.tcl
#功能描述: 生成06031的数据
#运行粒度: 日
#源    表：1.bass2.DWD_PS_NET_NUMBER_YYYYMMDD

#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：夏华学
#编写时间：2008-12-16
#问题记录：
#修改历史: 20091123,1.6.4规范修改(只记录中国移动地市运营公司与MSISDN号段的对应关系)
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #当天 yyyy-mm-dd
  set optime $op_time

		set app_name "G_I_06031_DAY.tcl"        

        
  #删除本期数据
	set sql_buff "delete from bass1.g_i_06031_day where time_id=$timestamp"
	exec_sql $sql_buff
       
	set sql_buff "insert into bass1.g_i_06031_day
                     select
                        distinct
                        $timestamp
                        ,number_segment
                        ,case when region_code = '891' then '13101'
                              when region_code = '892' then '13102'
                              when region_code = '893' then '13103'
                              when region_code = '894' then '13104'
                              when region_code = '895' then '13105'
                              when region_code = '896' then '13106'
                              when region_code = '897' then '13107'
                        end
                      from
                        bass2.DWD_PS_NET_NUMBER_$timestamp
                      where
                        region_code in ('891','892','893','894','895','896','897') and length(trim(number_segment)) = 7 "
	exec_sql $sql_buff



  #进行06031主键唯一性检查
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select msisdn,count(*) cnt from bass1.g_i_06031_day
	              where time_id =$timestamp
	             group by msisdn
	             having count(*)>1
	            ) as a
	            "
	puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "06031接口主键唯一性校验未通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	   }




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

