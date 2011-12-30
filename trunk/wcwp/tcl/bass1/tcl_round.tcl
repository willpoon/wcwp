#################################################################
# 程序名称: tcl_round.tcl
# 功能描述:
# 输入参数:
#	    op_time		数据时间（批次）,“yyyy-mm-dd”
#	    province_id	        省份编码
#	    redo_number	        重传序号。只有最终的一经编码定长数据文件需要重传序号
#	    trace_fd	        trace文件句柄
#	    temp_data_dir	临时数据文件的存放位置，$BASS1FILEDIR/tempdata/，用于保存e_transform生成的数据文件
#	    semi_data_dir	中间数据文件的存放位置，$BASS1FILEDIR/semidata/，用于保存.bass1、.bass2、.sample等文件
#	    final_data_dir	最终数据文件的存放位置，$BASS1FILEDIR/finaldata/，用于保存.dat，即一经编码定长数据文件
#	    conn		数据库连接
#	    src_data	        数据源
#	    obj_data	        数据目的
#输出参数:
# 返回值:   0 成功; -1 失败
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env
	set p_optime $op_time
	set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set this_month [string range $op_time 0 3][string range $op_time 5 6]
	set db_user $env(DB_USER)

        set dNum 0.43542
        set iBit 3
        
        set iZero 0
        set iLen [string length $dNum]
        puts $iLen
        
        set iPos [string first "." $dNum ]
        puts $iPos
        
        set  x1 [format "%.02f" [expr ($dNum)]] 
        puts $x1

        set  x2 [format "%.03f" [expr ($dNum)]] 
        puts $x2
        
#        #输入值为整数
#        if { $iPos==-1} {
#          set dResult $dNum.
#          puts $dResult
#
#          for { set iStep 0 } {$iStep <= [expr $iBit -1 ]} {incr iStep} {
#          	set dResult $dResult$iZero
#          	}
#          }
#          puts $dResult      
#        }
#        if { $iLen <= [expr ($iPos+$iBit) ]} {
#           set dResult $dNum
#        } else {
#            if { [string range $dNum [expr ($iPos+$iBit) ] [[expr ($iPos+$iBit+1) ]] >4 } {
#               
#            	set [expr ]
#            }
#        }
#        
        
	return 0
}