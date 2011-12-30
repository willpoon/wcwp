######################################################################################################
#接口名称：代销代办商渠道
#接口编码：06013
#接口说明：代销代办商是为中国移动提供代理业务的企业或商业机构。
#程序名称: G_I_06013_MONTH.tcl
#功能描述: 生成06013的数据
#运行粒度: 月
#源    表：1.bass2.dim_pub_channel(渠道维表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.目前BOSS渠道无法区分代理类型，所以统一填"03:合作型代理"
#          2.目前渠道表在BOSS没有任何时间，所以启用日期统一填20000101，截止日期统一填20300101 
#          3.目前BOSS渠道无法区分代销渠道类型，所以统一填"210102:非排他型"。
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06013_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06013_month
                         select
                           $op_month
                           ,char(channel_id)
                           ,substr(channel_name,1,20)
		           ,case 
		              when city_id ='891' then '540100'
		              when city_id ='892' then '542300'
		              when city_id ='893' then '542200'
		              when city_id ='894' then '542600'
		              when city_id ='895' then '542100'
		              when city_id ='896' then '542400'
		              when city_id ='897' then '542500'		
		              else '540000' 
		           end  
		           ,'03'
		           ,char(int(city_id)+12210)
		           ,'20000101'
		           ,'20300101'
		           ,'210102'
                         from 
                           bass2.dim_pub_channel
                         where sts=1 
                           and channeltype_id in (6,7,8)"
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