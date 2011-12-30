######################################################################################################
#接口名称：SP企业代码
#接口编码：06012
#接口说明：企业代码为网络中SP地址和身份的标识，地址翻译、计费、结算等均以企业代码为依据。
#          企业代码以数字表示，共6位，从"AXY000"至"AXY999"，其中"XY"对应于各移动公司,
#          对于梦网短信业务，SP企业代码均以9开头，彩信以8开头，PDA业务以6开头。
#          各省只上报提供本地服务（本地/全网业务本地接入）的sp局数据根据目前的业务规则，
#          同一个SP运营商，例如新浪，根据其提供的业务类型不同（例如：梦网短信，百宝箱），
#          会分配多个SP企业代码。
#程序名称: G_I_06012_MONTH.tcl
#功能描述: 生成06012的数据
#运行粒度: 月
#源    表：1.BASS2.dim_newbusi_spinfo(SP维表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人: tym
#编写时间：2007-03-22
#问题记录：1.统计自有业务标志时,只排除了SP_NAME为"西藏移动"的两个SP。
#          2.集团公司规定该接口是全量，所以要传已经失效的SP信息。这样就会造成该接口
#            违反SP企业代码，SP业务类型编码和生效日期做联合主键的校验。
#            为了满足校验，取valid_date和expire_date的max
#          3.但是存在同一个SP企业有几种SP业务类型编码，这样就造成不符合集团的主键校验。
#           这个情况违反了集团规定的“根据其提供的业务类型不同(例如：梦网短信，百宝箱) 
#           会分配多个SP企业代码”的业务要求。
#修改历史: 1.SP只送本地的 20081117 夏华学
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]       
        #set op_month 200809 
        puts $op_month
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
        #删除本期数据
	set sql_buff "delete from bass1.g_i_06012_month where time_id=$op_month"
  puts $sql_buff
  exec_sql $sql_buff       

	set sql_buff "insert into bass1.g_i_06012_month
                        select
                          $op_month
                          ,substr(sp_code,1,12)
                          ,substr(sp_name,1,50)
                          ,case
                             when sp_type=1 then '01'
                             when sp_type=2 then '02'
                             when sp_type=3 then '03'
                             when sp_type=5 then '05'
                             when sp_type=6 then '04'
                             else '99'
                           end
                          ,case 
                             when sp_region=1 then '3'
                             else '2'
                           end 
                          ,max(valid_date)
                          ,max(expire_date)
                          ,case 
                             when sp_name like '%西藏移动%' then '1'
                             else '0'
                           end                                         
                       from  
                         bass2.dim_newbusi_spinfo
                       where 
                         bigint(expire_date)>$this_month_last_day  and (sp_region <> 1 or sp_name like '%西藏移动%')
                       group by 
                            substr(sp_code,1,12)
                           ,substr(sp_name,1,50)
                           ,case
                              when sp_type=1 then '01'
                              when sp_type=2 then '02'
                              when sp_type=3 then '03'
                              when sp_type=5 then '05'
                              when sp_type=6 then '04'
                              else '99'
                            end
                           ,case 
                              when sp_region=1 then '3'
                              else '2'
                            end 
                           ,case 
                              when sp_name like '%西藏移动%' then '1'
                              else '0'
                            end "
  puts $sql_buff
  exec_sql $sql_buff       

	set sql_buff "delete from bass1.g_i_06012_month where time_id=$op_month and length(ltrim(rtrim(sp_code))) <> 6;"
  puts $sql_buff
  exec_sql $sql_buff       

	set sql_buff "delete from bass1.g_i_06012_month where time_id=$op_month and substr(ltrim(sp_code),1,1) not in ('4','7','9');"
  puts $sql_buff
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
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------

