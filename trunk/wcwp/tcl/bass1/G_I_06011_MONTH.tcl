######################################################################################################
#接口名称：SP服务代码
#接口编码：06011
#接口说明：服务代码是在使用短信/彩信方式的上传类业务时，需要输入的发送号码，
#          或者是在下行类业务中用户终端显示的发送方号码。各省只上报提供本地服务
#         （本地/全网业务本地接入）的sp局数据服务代码以数字表示。1.全国业务服务代码长度统一为 4 位，
#          即"1000"－"9999"；2.本地业务服务代码长度统一为5 位，即 "01000"－"09999"。
#          对于"移动沙龙"和"语音杂志"业务，这里是SP业务接入号码：12590XYZZ（语音杂志），
#          12586XY（移动沙龙）。
#程序名称: G_I_06011_MONTH.tcl
#功能描述: 生成06011的数据
#运行粒度: 月
#源    表：1.BASS2.dim_newbusi_spinfo(SP维表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.集团公司规定该接口是全量，所以要传已经失效的SP信息。这样就会造成该接口
#            违反SP企业代码和SP服务代码做联合主键的校验。
#            为了满足校验，取valid_date的max
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6] 
        #本月最后一天 yyyymmdd
       set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]         

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06011_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06011_month
	                select a.time_id,
	                       a.sp_code,
	                       a.serv_code,
	                       a.valid_date,
	                       a.expire_date
	                from       
                  (       select
                           $op_month time_id
                           ,substr(sp_code,1,12) sp_code
                           ,case when value(substr(serv_code ,1,21),' ') = '#' then '0' else value(substr(serv_code ,1,21),' ') end  serv_code
                           ,max(valid_date) valid_date
                           ,expire_date
                           ,row_number()over(partition by substr(sp_code,1,12) order by $op_month ) row_id
                         from
                           bass2.dim_newbusi_spinfo 
                         where
                           bigint(expire_date)>$this_month_last_day and
                           (sp_region <> 1 or
                           sp_name like '%西藏移动%')
                         group by 
                           substr(sp_code,1,12)
                          ,substr(serv_code ,1,21)
                          ,expire_date
                   ) a
                  where a.row_id=1         "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06011_month where time_id=$op_month and length(ltrim(rtrim(sp_code))) <> 6;"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2030
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06011_month where time_id=$op_month and substr(ltrim(sp_code),1,1) not in ('4','7','9')"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2040
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle   

	return 0
}