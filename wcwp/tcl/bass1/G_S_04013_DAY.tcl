######################################################################################################
#接口名称：充值卡充值话单
#接口编码：04013
#接口说明：包括IP充值卡的充值话单，神州行充值话单
#程序名称: G_S_04013_DAY.tcl
#功能描述: 生成04013的数据
#运行粒度: 日
#源    表：1.bass2.cdr_vcard_yyyymmdd
#          2.bass2.dw_product_yyyymmdd
#          3.bass1.g_user_lst
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyy-mm-dd
	set optime $op_time
	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]

        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_04013_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
        #创建临时表
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_s_04013_day_tmp
                     (
                       PRODUCT_NO       VARCHAR(15)
                      )
                      partitioning key
	              (PRODUCT_NO)
	              using hashing
                      with replace on commit preserve rows not logged in tbs_user_temp"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       #抽样表和用户表关联取出product_no插入临时表
        set handle [aidb_open $conn]
	set sql_buff "insert into  session.g_s_04013_day_tmp
                    select
                      b.product_no
                    from 
                      bass1.g_user_lst a,
                      bass2.dw_product_$timestamp b
                    where
                      a.time_id=$op_month
                      and a.user_id=b.user_id "
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
	set sql_buff "insert into bass1.g_s_04013_day
                  (
                  time_id
                  ,card_id
                  ,recharge_date
                  ,recharge_time
                  ,card_price
                  ,product_no
                  ,vc_id
                  ,vc_locn
                  ,home_locn
                  ,card_status
                  ,card_pay_mode_id
                  ,card_crd_type
                  )
                select
                  $timestamp
                  ,card_id
                  ,replace(char(date(start_time)),'-','')
                  ,replace(char(time(start_time)),'.','')
                  ,char(int(card_charge)/100)
                  ,a.product_no
                  ,' '
                  ,char(int(COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0008',char(card_city_id)),'891')))
                  ,char(int(COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0008',char(city_id)),'891')))
                  ,case 
                     when trade_state=1 then '0' 
                     else '1' 
                   end
                  ,'0'
                  ,'VCD'
                from 
                  bass2.cdr_vcard_$timestamp a,
                  session.g_s_04013_day_tmp b
                where 
                  a.product_no=b.product_no  "
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