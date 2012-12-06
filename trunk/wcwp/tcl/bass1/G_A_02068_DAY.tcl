
######################################################################################################		
#接口名称: 商户管家终端用户绑定关系                                                               
#接口编码：02068                                                                                          
#接口说明：本接口为日增量接口，首次上报订购状态为正常的全量订购关系。
#程序名称: G_A_02068_DAY.tcl                                                                            
#功能描述: 生成02068的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120801
#问题记录：
#修改历史: 1. panzw 20120801	中国移动一级经营分析系统省级数据接口规范 (V1.8.2) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #当天 yyyy-mm-dd
        set optime $op_time

	set sql_buff "delete from BASS1.G_A_02068_DAY where TIME_ID = $timestamp"
	exec_sql $sql_buff


	return 0
}