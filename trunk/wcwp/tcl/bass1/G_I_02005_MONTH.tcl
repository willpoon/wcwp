######################################################################################################
#接口名称：大客户/大用户
#接口编码：02005
#接口说明：记录当用户成为大客户时对应的级别信息和客户经理信息。（注：包括当月底最后一天所有在网个人
#          大客户、以及当月内离网的个人大客户）。
#程序名称: G_I_02005_MONTH.tcl
#功能描述: 生成02005的数据
#运行粒度: 月
#源    表：1.bass2.dw_product_yyyymm(用户资料月表)
#          2.bass2.dwd_vipcust_manager_rela_yyyymmdd(大客户和客户经理的对应关系)
#          3.bass2.dwd_cust_vip_card_yyyymm(大客户卡信息)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.该接口的逻辑不能够体现当月内离网的个人大客户
#          2."成为大用户的日期"的日期，目前没有专门的字段，所以只好用卡的生效日期替代，稍微有些偏差。
#          3.目前存在一个大客户有2个或2个以上的大客户经理，这样违反该接口大客户标识唯一的校验。
#            所以只送大客户和客户经理的对应关系表中生效日期最小的记录对应的客户经理
#修改历史: 1.20100120 在网客户口径变动(将原先不属于在网的主动预销、进入保留期和数据卡客户调整为在网客户)
#            原有代码没剔除测试，现加上。
#
#修改历史: 2.2011-04-04 去掉 vip_source=0  条件，把为null(批量新增) 的也包含。
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02005_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
              
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02005_month
                      select 
                        $op_month
                        ,a.user_id
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0013',char(a.custclass_id)),'4')
                        ,char(max(bigint(b.manager_id)))
                        ,replace(char(date(c.valid_date)),'-','')
                        ,case 
                           when a.custclass_id in (1,2,3,5) then '0' 
                           else '1' 
                          end
                      from 
                        (
                         select 
                           user_id,
                           cust_id,
                           custclass_id
                         from 
                           bass2.dw_product_$op_month 
                         where 
                           custclass_id in (1,2,3,5,7,8,9)
                           and userstatus_id in (1,2,3,6,8) 
                           and usertype_id in (1,2,9)
                           and vip_mark=1
                           and crm_brand_id1=1
                           and test_mark<>1
                        )a,
                        (
                         select 
                           cust_id,
                           manager_id,
                           min(valid_date)
                         from bass2.dwd_vipcust_manager_rela_$this_month_last_day 
                         where rec_status=1 
                         group by cust_id,manager_id
                        )b,
                        (
                          select 
                            user_id,
                            min(card_valid_date) as valid_date
                          from bass2.dwd_cust_vip_card_$op_month 
                          where 
                              rec_status=1
                          group by user_id
                        )c
                     where  a.cust_id=b.cust_id
                        and a.user_id=c.user_id 
                     group by 
                        a.user_id
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0013',char(a.custclass_id)),'4')
                        ,replace(char(date(c.valid_date)),'-','')
                        ,case 
                           when a.custclass_id in (1,2,3,5) then '0' 
                           else '1' 
                          end "
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

#################参考
#                         select 
#                           user_id,
#                           cust_id,
#                           custclass_id
#                         from 
#                           bass2.dw_product_$op_month 
#                         where 
#                           custclass_id in (1,2,3,5,7,8,9) --钻石卡、金卡、银卡、贵宾卡、TS钻石卡、TS金卡、TS贵宾卡
#                           and userstatus_id in (1,2,3,6)  --在网用户
#                           and usertype_id in (1,2,9) --非虚拟用户
#                           and vip_mark=1
#                           and crm_brand_id1=1
#                        )a,
#                        (
#                         select 
#                           cust_id,
#                           manager_id,
#                           min(valid_date)
#                         from bass2.dwd_vipcust_manager_rela_$this_month_last_day 
#                         where rec_status=1 --在网
#                         group by cust_id,manager_id
#                        )b,
#                        (
#                          select 
#                            user_id,
#                            min(card_valid_date) as valid_date
#                          from bass2.dwd_cust_vip_card_$op_month 
#                          where vip_source=0 --个人大客户
#                             and rec_status=1 --当前用户
#                             --and bigint(replace(char(date(CARD_EXPIRE_DATE)),'-',''))<=$this_month_last_day
#                          group by user_id
#                        )c
#                     where  a.cust_id=b.cust_id
#                        and a.user_id=c.user_id
#################################################                     