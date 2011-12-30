######################################################################################################
#接口名称：电信运营商经营网络类型
#接口编码：06014
#接口说明：记录电信运营商经营的电信网络类型。
#程序名称: G_I_06014_MONTH.tcl
#功能描述: 生成06014的数据
#运行粒度: 月
#源    表：1.
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.因为西藏电信运营商很少，目前也没有专门的表来维护这些信息，所以该接口参照
#          老系统的做法，直接插入几条数据。
#修改历史: 1.
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]            
            
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06014_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06014_month values
                       ($op_month,'020000','01'),
                       ($op_month,'020000','02'),
                       ($op_month,'020000','03'),
                       ($op_month,'020000','04'),
                       ($op_month,'020000','05'),
                       ($op_month,'020000','06'),
                       ($op_month,'020000','10'),
                       ($op_month,'030000','01'),
                       ($op_month,'030000','05'),
                       ($op_month,'030000','11'),
                       ($op_month,'040000','01'),
                       ($op_month,'040000','05'),
                       ($op_month,'040000','11'),
                       ($op_month,'010000','02'),
                       ($op_month,'010000','04'),
                       ($op_month,'010000','05'),
                       ($op_month,'050000','01'),
                       ($op_month,'050000','05') "
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
#####################参考##############################
#联通：020000
#01	固定
#02	GSM
#03	CDMA
#04	GSM智能网
#05	IP网
#06	GPRS
#10	CDMA-1X
#电信030000、网通040000
#01	固定
#05	IP网
#11	PHS（小灵通）
#移动：010000
#02	GSM
#04	GSM智能网
#05	IP网
#铁通050000
#01	固定
#05	IP网
#######################################################