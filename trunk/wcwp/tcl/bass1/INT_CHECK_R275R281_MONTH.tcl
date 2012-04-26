######################################################################################################
#程序名称：	INT_CHECK_R275R281_MONTH.tcl
#校验接口：	渠道相关借口
#运行粒度: MONTH
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2012-04-19
#问题记录：
#修改历史:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        #set op_time 2011-06-01
        #set optime_month 2011-05
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #自然月
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #程序名
        global app_name
        set app_name "INT_CHECK_R275R281_MONTH.tcl"



###########################################################

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R275' ,'R276' ,'R277' ,'R278' ,'R279' ,'R280' ,'R281')
 	  "        
	  exec_sql $sqlbuf

##~   R276	月	09_渠道运营	自营厅的前台营业面积	06023 实体渠道资源配置信息	自营厅的前台营业面积不能为0，且前台营业面积/使用面积 ≤ 60%	0.05	
##~   R277	月	09_渠道运营	自营厅的台席数量和营业人员数量	06023 实体渠道资源配置信息	自营厅的台席数量≠0，且营业人员数量≠0，且1.5≤营业人员数量/台席数量≤2.5	0.05	
##~   R278	月	09_渠道运营	实体渠道基础信息日月关系	"06021 实体渠道基础信息
##~   06035 实体渠道基础信息（日增量）"	06021中的实体渠道个数应等于06035月末快照中的实体渠道个数	0.05	
##~   R279	月	09_渠道运营	实体渠道购建或租赁信息日月关系	"06022 实体渠道购建或租赁信息
##~   06036 实体渠道购建或租赁信息（日增量）"	06022中的实体渠道个数应等于06036月末快照中的实体渠道个数	0.05	
##~   R280	月	09_渠道运营	实体渠道资源配置信息日月关系	"06023 实体渠道资源配置信息
##~   06037 实体渠道资源配置信息（日增量）"	06023中的实体渠道个数应等于06037月末快照中的实体渠道个数	0.05	
##~   R281	日	09_渠道运营	短信营业厅的放号量和定制终端销售量	22066 电子渠道关键数据日汇总	短信营业厅的放号量=0，且定制终端销售量=0	0.05	


   ##~   set RESULT_VAL1 0
   ##~   set RESULT_VAL2 0
   ##~   set RESULT_VAL3 0

##~   R275	月	09_渠道运营	自营厅的经纬度	06021 实体渠道基础信息	不同自营厅的经纬度不能完全相同	0.05	

set sql_buff "
			select count(0) cnt
			from (
				select LONGITUDE,LATITUDE,count(0)
				from bass1.g_i_06021_month  
				where time_id =$op_month and channel_type='1'	
				group by LONGITUDE,LATITUDE
				having count(0) > 1	
			) t
			with ur
"
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   ##~   set RESULT_VAL2 [lindex $p_row 1]
   ##~   set RESULT_VAL3 [lindex $p_row 2]

        #--将校验值插入校验结果表
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R275',$RESULT_VAL1,0,0,0) 
                "
                exec_sql $sql_buff
		
 # 校验值超标时告警	
	if { $RESULT_VAL3 < 0.05 } {
		set grade 2
	  set alarmcontent "R275 校验不通过"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

      	
	return 0
}
