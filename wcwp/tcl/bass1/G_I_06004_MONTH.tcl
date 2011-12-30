######################################################################################################
#接口名称：SIM卡
#接口编码：06004
#接口说明：SIM卡（Subscriber Identity Module）是指内置含有CPU的集成电路芯片的智能卡，
#          其作为GSM移动通信网络用户的身份识别卡，担负着用户身份鉴别、通讯信息加密、数据存储等任务。
#          本实体记录CMCC各级运营公司拥有的全部SIM卡信息。
#程序名称: G_I_06004_MONTH.tcl
#功能描述: 生成06004的数据
#运行粒度: 月
#源    表：1.bass2.dwd_res_sim_yyyymmdd(卡资源)
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：tym
#编写时间：2007-03-22
#问题记录：1.SIM卡容量类型编码目前不能出，送空
#          2.因为目前源表的STS目前不能确定是否符合集团要求，所以程序用CASE来写
#          3.做中间表剔重
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06004_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

        #建立临时表
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_i_06004_month_tmp
	              (
                        sim_iccid         character(20)  ,
                        sim_bus_srv_id    character(2)   ,
                        cmcc_id           character(5)   ,
                        sim_card_status   character(2)   ,
                        chnl_id           character(20)
	              )
	              partitioning key
	              (sim_iccid)
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

	#想临时表插入剔重后的数据
        set handle [aidb_open $conn]
	set sql_buff "insert into  session.g_i_06004_month_tmp
                       select
                         case
                           when a.sim_id is null then '89860'||imsi
                           else a.sim_id
                         end
                         ,case
                           when a.main_flag=1 then '07'
                           else '01'
                          end
                         ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13100')
                         ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0001',CHAR(a.sts)),'39')
                         ,char(a.channel_id)
                       from
                         bass2.dwd_res_sim_$this_month_last_day a,
                         (select sim_id,max(res_date) as  res_date,max(channel_id) as channel_id 
                          from bass2.dwd_res_sim_$this_month_last_day
                          group by sim_id
                         ) b
                       where 
                         a.sim_id=b.sim_id 
                         and a.res_date=b.res_date and a.channel_id=b.channel_id 
                       group by 
                         case
                           when a.sim_id is null then '89860'||imsi
                           else a.sim_id
                         end
                         ,case
                           when a.main_flag=1 then '07'
                           else '01'
                          end
                         ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13100')
                         ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0001',CHAR(a.sts)),'39')
                         ,char(a.channel_id) "                      
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

        #汇总到结果表
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06004_month
                        select
                          $op_month
                          ,sim_iccid
                          ,sim_bus_srv_id
                          ,' '
                          ,cmcc_id
                          ,sim_card_status
                          ,chnl_id
                        from
                          session.g_i_06004_month_tmp"
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