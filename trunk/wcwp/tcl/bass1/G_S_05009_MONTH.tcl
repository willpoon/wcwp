######################################################################################################
#接口名称：分省结算短信业务稽核情况
#接口编码：05009
#接口说明：记录各省分省结算的短信业务的稽核情况。
#程序名称: G_S_05009_MONTH.tcl
#功能描述: 生成05009的数据
#运行粒度: 月
#源    表：1. bass2.dwd_js_ismgjh_sp_yyyymm(分省结算短信业务稽核)
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
	set sql_buff "delete from bass1.g_s_05009_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
              
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_05009_month
                      select
                        ${op_month}
                        ,'${op_month}'
                        ,sp_code
                        ,char(int(bal_fee      ))
                        ,char(int(error_fee    ))
                        ,char(int(error_user   ))
                        ,char(int(tj_fee       ))
                        ,char(int(tj_sheet_cnt ))
                        ,char(int(tj_user      ))
                        ,char(int(cj_fee       ))
                        ,char(int(cj_sheet_cnt ))
                        ,char(int(cj_user      ))
                        ,char(int(dg_fee       ))
                        ,char(int(dg_sheet_cnt ))
                        ,char(int(dg_user      ))
                        ,char(int(cm_fee       ))
                        ,char(int(cm_user      ))
                        ,char(int(dt_fee       ))
                        ,char(int(dt_sheet_cnt ))
                        ,char(int(dt_user      ))
                        ,char(int(hj_user      ))
                        ,char(int(other_fee    ))
                        ,char(int(other_user   ))
                      from 
                        bass2.dwd_js_ismgjh_sp_$op_month"
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