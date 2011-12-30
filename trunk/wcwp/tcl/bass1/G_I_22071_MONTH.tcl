######################################################################################################
#接口名称：国际长途收入情况月汇总
#接口编码：22071
#接口说明：国际长途业务收入情况月汇总 ，包括中国移动用户和国际漫游来访用户利用中国移动的IDD和
#          IP网络资源（主叫）拨打国际长途从用户（中国移动用户）和对方运营商（漫游来访用户）取得的
#          实际收入。
#程序名称: G_I_22071_MONTH.tcl
#功能描述: 生成22071的数据
#运行粒度: 月
#源    表：1.bass2.dw_call_yyyymm(业务量月表)

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
        #上月  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_22071_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

       #直接汇总到结果表
        set handle [aidb_open $conn]      
	set sql_buff "insert into bass1.g_i_22071_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0007',char(roam_city_id)),'900')
                        ,case
                          when ip_mark=1 then '2'
                          else '1'
                         end
                        ,char(bigint(sum(basecall_fee+toll_fee+info_fee+other_fee)*100))                       
                      from 
                        bass2.dw_call_$op_month 
                      where 
                        tolltype_id in (3,4,5,6,7,8,9,10,11,12,13,99,103,104,105,106,107,108,109,110,111,112,113,999)
                      group by
                        coalesce(bass1.fn_get_all_dim('BASS_STD1_0007',char(roam_city_id)),'900')
                        ,case
                           when ip_mark=1 then '2'
                           else '1'
                         end                        
                        "
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
	return 0
}