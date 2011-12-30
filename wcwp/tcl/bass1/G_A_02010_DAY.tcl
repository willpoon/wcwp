######################################################################################################
#接口名称：用户选择资费营销案历史
#接口编码：02010
#接口说明：用户选择的资费营销案/业务包。
#程序名称: G_A_02010_DAY.tcl
#功能描述: 生成02010的数据
#运行粒度: 月
#源    表：1.bass2.dwd_product_sprom_history_yyyymmdd(用户套餐关系(历史))
#          2.bass2.dwd_product_sprom_active_yyyymmdd(用户套餐关系(在用))
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_a_02010_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
   
        set handle [aidb_open $conn]

	set sql_buff "insert into bass1.g_a_02010_day
                   (
                   time_id
                   ,plan_id
                   ,user_id
                   ,valid_date
                   ,invalid_date
                   )
                  select  
                    $timestamp
                    ,t.plan_id
                    ,t.user_id
                    ,char($timestamp) as  vaild_date
                    ,max(t.invaild_date)
                  from 
                  (
                     select                   
                       char(sprom_id) as plan_id
                       ,char(user_id) as user_id
                       ,replace(char(date(expire_date)),'-','') as invaild_date
                     from 
                       bass2.dwd_product_sprom_history_$timestamp
                     where 
                       date(valid_date)='$optime' 
                     union all 
                     select
                       char(sprom_id) as plan_id
                       ,char(user_id) as user_id
                       ,replace(char(date(expire_date)),'-','') as invaild_date
                     from 
                       bass2.dwd_product_sprom_active_$timestamp
                     where 
                       date(valid_date)='$optime'
                  )t
                  group by 
                    t.plan_id
                    ,t.user_id "
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