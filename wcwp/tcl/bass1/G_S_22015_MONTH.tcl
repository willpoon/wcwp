######################################################################################################
#接口名称：手机补贴月汇总1 
#接口编码：22015
#接口说明：记录手机补贴用户的相关信息, 所有指标特指手机补贴客户。
#程序名称: G_S_22015_MONTH.tcl
#功能描述: 生成22015的数据
#运行粒度: 月
#源    表：1.
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        puts $timestamp
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        #----求上月最后一天---#,格式 yyyymmdd
        puts $last_month_day
        
        
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]     
        puts $op_month    
       
        #本月1号 yyyy-mm-dd
        set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $ThisMonthFirstDay 
       
        set ThisMonthYear [string range $ThisMonthFirstDay 0 3]
        puts $ThisMonthYear
        set ThisYearFirstDay [string range $ThisMonthYear 0 3][string range $op_time 4 4]01[string range $op_time 4 4]01
        puts $ThisYearFirstDay


        #得到下个月的1号日期
      	set sql_buff "select date('$ThisMonthFirstDay')+1 month from bass2.dual"
	      puts $sql_buff
	      set NextMonthFirstDay [get_single $sql_buff]
	      puts $NextMonthFirstDay


        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.G_S_22015_MONTH where time_id=$op_month"
	puts $sql_buff
  exec_sql $sql_buff

	set sql_buff "delete from bass1.G_S_22015_MONTH where time_id=888888"
	puts $sql_buff
  exec_sql $sql_buff
  
  
  


#01	月份	格式：YYYYMM	char(6)
#02	客户品牌编码	参见维度指标说明中的BASS_STD1_0055	char(1)
#03	月末客户到达数	单位：户	Number(9)
#04	本月发展客户数	单位：户	Number(9)
#05	本月离网客户数	单位：户	Number(9)
#06	本年累计发展客户数	单位：户	Number(9)
#07	本年累计离网客户数	单位：户	Number(9)
#08	本月计费时长	单位：分钟	Number(14)
#09	本年累计计费时长	单位：分钟	Number(14)
#10	本月总收入	单位：分	Number(12)
#11	本年累计总收入	单位：分	Number(15)
#12	本月新业务收入	单位：分	Number(12)
#13	本年累计新业务收入	单位：分	Number(15)

#CREATE TABLE BASS1.G_S_22015_MONTH
# (TIME_ID                 INTEGER,
#  Bill_Month              CHARACTER(6),
#  Brand_ID                CHARACTER(1),
#  ThisMonthArrives        CHARACTER(9),  
#  ThisMonthCount          CHARACTER(9), 
#  ThisMonthLostCount      CHARACTER(9), 
#  ThisYearCount           CHARACTER(9), 
#  ThisYearLostCount       CHARACTER(9), 
#  ThisMonthDuration       CHARACTER(14),
#  ThisYearDuration        CHARACTER(14),
#  ThisMonthIncome         CHARACTER(12),
#  ThisYearIncome          CHARACTER(15),
#  ThisMonthNewbusiIncome  CHARACTER(12),
#  ThisYearNewbusiIncome   CHARACTER(15) 
#           
# )
#  DATA CAPTURE NONE
#  IN TBS_APP_BASS1
#  INDEX IN TBS_INDEX
#  PARTITIONING KEY
#   (TIME_ID
#   ) USING HASHING
#  NOT LOGGED INITIALLY;
#
#ALTER TABLE BASS1.G_S_22015_MONTH
#  LOCKSIZE ROW
#  APPEND OFF
#  NOT VOLATILE;

#月末客户到达数
  set sql_buff "insert into G_S_22015_MONTH (time_id,Brand_ID,ThisMonthArrives)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            char(count(distinct b.user_id))
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b
                      where cond_name like '%手机%'    and 
                            a.user_id = b.user_id      and 
	                          userstatus_id in (1,2,3,6) and 
	                          usertype_id in (1,2,9)     and 
	                          free_mark = 0
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') ;"
	puts $sql_buff
  exec_sql $sql_buff

#04	本月发展客户数	
  set sql_buff "insert into G_S_22015_MONTH (time_id,Brand_ID,ThisMonthCount)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            char(count(distinct b.user_id)) 
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b
                      where cond_name like '%手机%'    and 
                            a.user_id = b.user_id      and 
	                          userstatus_id in (1,2,3,6) and 
	                          usertype_id in (1,2,9)     and 
	                          free_mark = 0              and
	                          a.create_date >= date('$ThisMonthFirstDay') and a.create_date < date('$NextMonthFirstDay')
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') ;"
	puts $sql_buff
  exec_sql $sql_buff


#05	本月离网客户数
  set sql_buff "insert into G_S_22015_MONTH (time_id,Brand_ID,ThisMonthLostCount)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            char(count(distinct b.user_id)) 
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b
                      where cond_name like '%手机%'    and 
                            a.user_id = b.user_id      and 
	                          userstatus_id in (0)       and 
	                          free_mark = 0              and
	                          b.valid_date >= date('$ThisMonthFirstDay') and b.valid_date < date('$NextMonthFirstDay')
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') ;"
	puts $sql_buff
  exec_sql $sql_buff


#06	本年累计发展客户数
  set sql_buff "insert into G_S_22015_MONTH (time_id,Brand_ID,ThisYearCount)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            char(count(distinct b.user_id)) 
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b
                      where cond_name like '%手机%'    and 
                            a.user_id = b.user_id      and 
	                          userstatus_id in (1,2,3,6) and 
	                          usertype_id in (1,2,9)     and 
	                          free_mark = 0              and
	                          a.create_date >= date('$ThisYearFirstDay') 
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') ;"
	puts $sql_buff
  exec_sql $sql_buff


#07	本年累计离网客户数
  set sql_buff "insert into G_S_22015_MONTH (time_id,Brand_ID,ThisYearLostCount)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            char(count(distinct b.user_id)) 
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b
                      where cond_name like '%手机%'    and 
                            a.user_id = b.user_id      and 
	                          userstatus_id in (0)       and 
	                          free_mark = 0              and
	                          b.valid_date >= date('$ThisYearFirstDay') 
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') ;"
	puts $sql_buff
  exec_sql $sql_buff


#08	本月计费时长
  set sql_buff "insert into G_S_22015_MONTH (time_id,Brand_ID,ThisMonthDuration)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2') as brand_id,
                            char(sum(bigint(base_bill_Duration)+bigint(toll_bill_Duration)))
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b,
                            bass1.g_s_21003_MONTH c
                      where cond_name like '%手机%'     and 
                            a.user_id = b.user_id       and 
                            b.product_no = c.product_no and
	                          free_mark = 0               and
	                          c.time_id = $op_month   and 
	                          b.valid_date >= date('$ThisYearFirstDay') 
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2') ;"
	puts $sql_buff
  exec_sql $sql_buff


#09	本年累计计费时长
  set sql_buff "insert into G_S_22015_MONTH (time_id,Brand_ID,ThisYearDuration)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2') as brand_id,
                            char(sum(bigint(base_bill_Duration)+bigint(toll_bill_Duration)))
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b,
                            bass1.g_s_21003_MONTH c
                      where cond_name like '%手机%'     and 
                            a.user_id = b.user_id       and 
                            b.product_no = c.product_no and
	                          free_mark = 0               and
	                          c.time_id/100 = $ThisMonthYear   and 
	                          b.valid_date >= date('$ThisYearFirstDay') 
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2') ;"
	puts $sql_buff
  exec_sql $sql_buff


#10	本月总收入
  set sql_buff "insert into G_S_22015_MONTH (time_id,Brand_ID,ThisMonthIncome)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            char(sum(bigint(should_fee)))
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b,
                            bass1.g_s_03005_month c
                      where cond_name like '%手机%'     and 
                            a.user_id = b.user_id       and 
                            b.user_id = c.user_id       and
	                          free_mark = 0               and
	                          c.time_id = $op_month       and 
	                          b.valid_date >= date('$ThisYearFirstDay') 
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') ;"
	puts $sql_buff
  exec_sql $sql_buff


#11	本年累计总收入
  set sql_buff "insert into G_S_22015_MONTH (time_id,Brand_ID,ThisYearIncome)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            char(sum(bigint(should_fee)))
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b,
                            bass1.g_s_03005_month c
                      where cond_name like '%手机%'     and 
                            a.user_id = b.user_id       and 
                            b.user_id = c.user_id       and
	                          free_mark = 0               and
	                          c.time_id/100 = $ThisMonthYear   and 
	                          b.valid_date >= date('$ThisYearFirstDay') 
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') ;"
 puts $sql_buff
 exec_sql $sql_buff



#12	本月新业务收入
  set sql_buff "insert into G_S_22015_MONTH (time_id,Brand_ID,ThisMonthNewbusiIncome)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            char(sum(bigint(should_fee)))
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b,
                            bass1.g_s_03005_month c
                      where cond_name like '%手机%'     and 
                            a.user_id = b.user_id       and 
                            b.user_id = c.user_id       and
                            substr(c.item_id,1,2) not in ('01','02','03') and
	                          free_mark = 0               and
	                          c.time_id = $op_month       and 
	                          b.valid_date >= date('$ThisYearFirstDay') 
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') ;"
	puts $sql_buff
  exec_sql $sql_buff


#13	本年累计新业务收入
  set sql_buff "insert into G_S_22015_MONTH (time_id,Brand_ID,ThisYearNewbusiIncome)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            char(sum(bigint(should_fee)))
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b,
                            bass1.g_s_03005_month c
                      where cond_name like '%手机%'     and 
                            a.user_id = b.user_id       and 
                            b.user_id = c.user_id       and
                            substr(c.item_id,1,2) not in ('01','02','03') and 
	                          free_mark = 0               and
	                          c.time_id/100 = $ThisMonthYear   and 
	                          b.valid_date >= date('$ThisYearFirstDay') 
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') ;"
	puts $sql_buff
  exec_sql $sql_buff



  set sql_buff "insert into G_S_22015_MONTH    
                     select $op_month,'$op_month',Brand_ID,
                            char(sum(bigint(value(ThisMonthArrives,'0')      ))), 
                            char(sum(bigint(value(ThisMonthCount    ,'0')    ))),
                            char(sum(bigint(value(ThisMonthLostCount,'0')    ))),
                            char(sum(bigint(value(ThisYearCount     ,'0')    ))),
                            char(sum(bigint(value(ThisYearLostCount ,'0')    ))),
                            char(sum(bigint(value(ThisMonthDuration ,'0')    ))),
                            char(sum(bigint(value(ThisYearDuration  ,'0')    ))),
                            char(sum(bigint(value(ThisMonthIncome   ,'0')    ))),
                            char(sum(bigint(value(ThisYearIncome    ,'0')    ))),
                            char(sum(bigint(value(ThisMonthNewbusiIncome,'0')))),
                            char(sum(bigint(value(ThisYearNewbusiIncome,'0') )))
                       from G_S_22015_MONTH
                      where time_id = 888888 
                      group by Brand_ID;"
	puts $sql_buff
  exec_sql $sql_buff


  set sql_buff "delete from G_S_22015_MONTH where time_id = 888888 ;"
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
