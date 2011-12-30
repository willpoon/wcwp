######################################################################################################
#接口名称：手机补贴月汇总2
#接口编码：22016
#接口说明：记录手机补贴用户的相关信息。
#程序名称: G_S_22016_MONTH.tcl
#功能描述: 生成22016的数据
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
	set sql_buff "delete from bass1.G_S_22016_MONTH where time_id=$op_month"
	puts $sql_buff
  exec_sql $sql_buff

	set sql_buff "delete from bass1.G_S_22016_MONTH where time_id=888888"
	puts $sql_buff
  exec_sql $sql_buff
  



#01	月份	格式：YYYYMM	char(6)
#02	客户品牌编码	参见维度指标说明中的BASS_STD1_0055	char(1)
#03	手机补贴方式	参见维度指标说明中的BASS_STD1_0095	Char(1)
#04	本月手机补贴成本	单位：分	Number(12)
#05	本年累计手机补贴成本	单位：分	Number(15)

#06	本月手机补贴数量		Number(9)
#07	本年累计手机补贴数量		Number(9)

#08	手机补贴客户到达数	单位：户	Number(9)
#09	本月手机补贴客户收入	单位：分	Number(12)
#10	本年累计手机补贴客户收入	单位：分	Number(15)
#11	本月手机补贴客户离网数	单位：户	Number(9)
#12	本年累计手机补贴客户离网数	单位：户	Number(9)

#CREATE TABLE BASS1.G_S_22016_MONTH
# (TIME_ID  INTEGER,
#  BillMonth                CHARACTER(6),
#  Brand_ID                 CHARACTER(1),
#  SubsidyType              CHARACTER(1),
#  SubsidyCost              CHARACTER(12),
#  ThisYearSubsidyCost      CHARACTER(15),
#  SubsidyCount             CHARACTER(9),
#  ThisYearSubsidyCount     CHARACTER(9),
#  ThisMonthArrives         CHARACTER(9),
#  ThisMonthIncome          CHARACTER(12),
#  ThisYearIncome           CHARACTER(15),
#  ThisMonthLostCount       CHARACTER(9),
#  ThisYearLostCount         CHARACTER(9)
# )
#  DATA CAPTURE NONE
#  IN TBS_APP_BASS1
#  INDEX IN TBS_INDEX
#  PARTITIONING KEY
#   (TIME_ID
#   ) USING HASHING
#  NOT LOGGED INITIALLY;
#
#ALTER TABLE BASS1.G_S_22016_MONTH
#  LOCKSIZE ROW
#  APPEND OFF
#  NOT VOLATILE;


#06	本月手机补贴数量	
  set sql_buff "insert into G_S_22016_MONTH (time_id,Brand_ID,SubsidyType,SubsidyCount)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                            end,
                                 
                            char(count(*)) 
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b
                      where cond_name like '%手机%'    and 
                            a.user_id = b.user_id      and 
	                          userstatus_id in (1,2,3,6) and 
	                          usertype_id in (1,2,9)     and 
	                          free_mark = 0              and
	                          a.create_date >= date('$ThisMonthFirstDay') and a.create_date < date('$NextMonthFirstDay')
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') ,                           
                               case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                                end ;"
	puts $sql_buff
  exec_sql $sql_buff


#07	本年手机补贴数量
  set sql_buff "insert into G_S_22016_MONTH (time_id,Brand_ID,SubsidyType,ThisYearSubsidyCount)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                            end,
                            char(count(*)) 
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b
                      where cond_name like '%手机%'    and 
                            a.user_id = b.user_id      and 
	                          userstatus_id in (1,2,3,6) and 
	                          usertype_id in (1,2,9)     and 
	                          free_mark = 0              and
	                          a.create_date >= date('$ThisYearFirstDay') 
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') ,                           
                               case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                                end ;"
	puts $sql_buff
  exec_sql $sql_buff



#08	手机补贴客户到达数
  set sql_buff "insert into G_S_22016_MONTH (time_id,Brand_ID,SubsidyType,ThisMonthArrives)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                            end,
                            char(count(distinct b.user_id)) 
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b
                      where cond_name like '%手机%'    and 
                            a.user_id = b.user_id      and 
	                          userstatus_id in (1,2,3,6) and 
	                          usertype_id in (1,2,9)     and 
	                          free_mark = 0             
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')  ,                           
                               case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                                end ;"
	puts $sql_buff
  exec_sql $sql_buff


#09	本月总收入
  set sql_buff "insert into G_S_22016_MONTH (time_id,Brand_ID,SubsidyType,ThisMonthIncome)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                            end,
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
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')  ,                           
                               case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                                end ;"
	puts $sql_buff
  exec_sql $sql_buff


#10	本年累计总收入
  set sql_buff "insert into G_S_22016_MONTH (time_id,Brand_ID,SubsidyType,ThisYearIncome)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                            end,
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
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')  ,                           
                               case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                                end ;"
 puts $sql_buff
 exec_sql $sql_buff

#11	本月离网客户数
  set sql_buff "insert into G_S_22016_MONTH (time_id,Brand_ID,SubsidyType,ThisMonthLostCount)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                            end,
                            char(count(distinct b.user_id)) 
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b
                      where cond_name like '%手机%'    and 
                            a.user_id = b.user_id      and 
	                          userstatus_id in (0)       and 
	                          free_mark = 0              and
	                          b.valid_date >= date('$ThisMonthFirstDay') and b.valid_date < date('$NextMonthFirstDay')
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') ,                           
                               case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                                end ;"
	puts $sql_buff
  exec_sql $sql_buff




#12	本年累计离网客户数
  set sql_buff "insert into G_S_22016_MONTH (time_id,Brand_ID,SubsidyType,ThisYearLostCount)   
                     select 888888,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id,
                            case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                            end,
                            char(count(distinct b.user_id)) 
                       from bass2.dw_product_user_promo_$op_month a,
                            bass2.dw_product_$op_month b
                      where cond_name like '%手机%'    and 
                            a.user_id = b.user_id      and 
	                          userstatus_id in (0)       and 
	                          free_mark = 0              and
	                          b.valid_date >= date('$ThisYearFirstDay') 
                      group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')  ,                           
                               case when cond_name like '%积分%' then '1'
                                 when cond_name like '%话费%' then '2'
                                 else '9'
                                end ;"
	puts $sql_buff
  exec_sql $sql_buff
  
  
  
  
  set sql_buff "insert into G_S_22016_MONTH    
                     select $op_month,'$op_month',Brand_ID,SubsidyType,
                            char(sum(bigint(value(SubsidyCost         ,'0')    ))),
                            char(sum(bigint(value(ThisYearSubsidyCost ,'0')    ))),
                            char(sum(bigint(value(SubsidyCount        ,'0')    ))),
                            char(sum(bigint(value(ThisYearSubsidyCount,'0')    ))),
                            char(sum(bigint(value(ThisMonthArrives    ,'0')    ))),
                            char(sum(bigint(value(ThisMonthIncome     ,'0')    ))),
                            char(sum(bigint(value(ThisYearIncome      ,'0')    ))),
                            char(sum(bigint(value(ThisMonthLostCount  ,'0')    ))),
                            char(sum(bigint(value(ThisYearLostCount,   '0'))))
                       from G_S_22016_MONTH
                      where time_id = 888888 
                      group by Brand_ID,SubsidyType;"
	puts $sql_buff
  exec_sql $sql_buff


  set sql_buff "delete from G_S_22016_MONTH where time_id = 888888 ;"
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


